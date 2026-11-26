package redisinfra

import (
	"context"
	"errors"
	"fillbrunn.ai/kube-agent/proto/pkg/common"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// result is what a fake worker reports for one run.
type result struct {
	worked bool
	err    error
}

// fakeRunner is an ArcRunner that answers from a script of results and
// records what it was asked to work.
type fakeRunner struct {
	mu    sync.Mutex
	calls []common.ArcRef
	ids   []string
	seqs  []int
	// script answers each run in turn, and repeats its last answer once it
	// is exhausted.
	script []result
	// hold parks each run until the test releases it, so that a consumer
	// can be seen holding an entry it has not settled.
	hold chan struct{}
	// entered reports that a run has been parked, so that a test can let
	// the consumer claim its entry before moving the story on.
	entered     chan struct{}
	enteredOnce sync.Once
}

func (f *fakeRunner) Work(_ context.Context, ref common.ArcRef, agentID string, triggerSeq int) (bool, error) {
	f.mu.Lock()
	if len(f.calls) == 0 && agentID == "" {
		f.mu.Unlock()
		return false, errors.New("a work item names no agent")
	}
	f.calls = append(f.calls, ref)
	f.ids = append(f.ids, agentID)
	f.seqs = append(f.seqs, triggerSeq)
	i := len(f.calls) - 1
	res := result{worked: true}
	if i < len(f.script) {
		res = f.script[i]
	} else if len(f.script) > 0 {
		res = f.script[len(f.script)-1]
	}
	f.mu.Unlock()

	if f.hold != nil {
		f.enteredOnce.Do(func() { close(f.entered) })
		<-f.hold
	}
	return res.worked, res.err
}

func (f *fakeRunner) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

// newQueue builds a queue on a fresh Redis with timings a test can wait
// through.
func newQueue(t *testing.T, options ...QueueOption) (*RedisWorkQueue, *redis.Client) {
	t.Helper()
	client, _ := newRedis(t)
	defaults := []QueueOption{
		WithContentionDelay(20 * time.Millisecond),
		WithClaimIdle(75 * time.Millisecond),
		WithBlockTimeout(20 * time.Millisecond),
		WithMaxAttempts(2),
	}
	queue, err := NewRedisWorkQueue(client, append(defaults, options...)...)
	if err != nil {
		t.Fatalf("could not build the queue: %v", err)
	}
	return queue, client
}

// consume runs the queue until the test is done with it.
func consume(t *testing.T, queue *RedisWorkQueue, runner ArcRunner) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = queue.Consume(ctx, runner, nil)
	}()
	return func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("the consumer did not stop")
		}
	}
}

// waitUntil polls until the condition holds or the wait runs out.
func waitUntil(wait time.Duration, condition func() bool) bool {
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		if condition() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return condition()
}

// pendingCount reports how many entries the group holds unsettled. A group
// that does not exist yet holds nothing.
func pendingCount(t *testing.T, client *redis.Client, queue *RedisWorkQueue) int64 {
	t.Helper()
	summary, err := client.XPending(context.Background(), queue.stream, queue.group).Result()
	if err != nil {
		if strings.Contains(err.Error(), "NOGROUP") {
			return 0
		}
		t.Fatalf("could not read the pending entries: %v", err)
	}
	return summary.Count
}

func TestEnqueueRefusesAnItemNamingNoAgent(t *testing.T) {
	queue, _ := newQueue(t)

	err := queue.Enqueue(context.Background(), common.ArcRef{SessionID: "s", ArcID: "a"}, "", 0)
	if err == nil {
		t.Fatal("an item naming no agent was enqueued")
	}
}

func TestEnqueueThenConsumeRunsTheItem(t *testing.T) {
	queue, client := newQueue(t)
	ref := common.ArcRef{SessionID: "s", ArcID: "a"}

	if err := queue.Enqueue(context.Background(), ref, "agent-1", 42); err != nil {
		t.Fatalf("could not enqueue the item: %v", err)
	}

	runner := &fakeRunner{}
	defer consume(t, queue, runner)()

	if !waitUntil(2*time.Second, func() bool { return runner.callCount() == 1 }) {
		t.Fatal("the enqueued item was never worked")
	}
	runner.mu.Lock()
	defer runner.mu.Unlock()
	if len(runner.calls) != 1 || runner.calls[0] != ref || runner.ids[0] != "agent-1" {
		t.Fatalf("the item was worked as %v under %v", runner.calls, runner.ids)
	}
	if len(runner.seqs) != 1 || runner.seqs[0] != 42 {
		t.Fatalf("the item was worked with trigger seqs %v, want [42]", runner.seqs)
	}
	if !waitUntil(2*time.Second, func() bool { return pendingCount(t, client, queue) == 0 }) {
		t.Fatal("a worked item was left unsettled")
	}
}

func TestContentionPutsTheItemBackAndWorksItAgain(t *testing.T) {
	queue, client := newQueue(t)
	ref := common.ArcRef{SessionID: "s", ArcID: "a"}

	if err := queue.Enqueue(context.Background(), ref, "agent-1", 0); err != nil {
		t.Fatalf("could not enqueue the item: %v", err)
	}

	// The first run finds the arc held, and the second works it.
	runner := &fakeRunner{script: []result{{worked: false}, {worked: true}}}
	defer consume(t, queue, runner)()

	if !waitUntil(2*time.Second, func() bool { return runner.callCount() >= 2 }) {
		t.Fatal("an item that found its arc held was never worked again")
	}
	if !waitUntil(2*time.Second, func() bool { return pendingCount(t, client, queue) == 0 }) {
		t.Fatal("a contended item was left unsettled")
	}
}

func TestAFailingItemIsRetriedThenMovedToTheDeadStream(t *testing.T) {
	queue, client := newQueue(t)
	ref := common.ArcRef{SessionID: "s", ArcID: "a"}

	if err := queue.Enqueue(context.Background(), ref, "agent-1", 0); err != nil {
		t.Fatalf("could not enqueue the item: %v", err)
	}

	// Every run fails, so the item exhausts its two attempts.
	runner := &fakeRunner{script: []result{{err: errors.New("the agent is not there")}}}
	defer consume(t, queue, runner)()

	if !waitUntil(2*time.Second, func() bool { return runner.callCount() == 2 }) {
		t.Fatalf("a failing item was worked %d times, not its two attempts", runner.callCount())
	}
	if !waitUntil(2*time.Second, func() bool { return pendingCount(t, client, queue) == 0 }) {
		t.Fatal("a dead item was left unsettled")
	}
	length, err := client.XLen(context.Background(), queue.dead).Result()
	if err != nil {
		t.Fatalf("could not read the dead stream: %v", err)
	}
	if length != 1 {
		t.Fatalf("a dead item arrived %d times in the dead stream, not once", length)
	}

	// The dead entry says what failed and why, so a poison item is visible.
	entries, err := client.XRange(context.Background(), queue.dead, "-", "+").Result()
	if err != nil || len(entries) != 1 {
		t.Fatalf("could not read the dead entry: %v", err)
	}
	if got := entries[0].Values["payload"]; got == nil {
		t.Fatal("the dead entry carries no payload")
	}
}

func TestADeadConsumersPendingEntryIsRecovered(t *testing.T) {
	queue, client := newQueue(t)
	ref := common.ArcRef{SessionID: "s", ArcID: "a"}
	if err := queue.Enqueue(context.Background(), ref, "agent-1", 0); err != nil {
		t.Fatalf("could not enqueue the item: %v", err)
	}

	// The first consumer claims the item and parks mid-run, then dies
	// without settling it: the entry stays pending under its name.
	held := make(chan struct{})
	blocked := &fakeRunner{hold: held, entered: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = queue.Consume(ctx, blocked, nil)
	}()
	<-blocked.entered
	// The consumer dies here: its context is cancelled first, so that the
	// parked run's attempt to settle the entry fails and leaves it
	// pending, and only then is the run let go.
	cancel()
	close(held)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the dead consumer did not stop when its context ended")
	}
	if !waitUntil(2*time.Second, func() bool { return pendingCount(t, client, queue) == 1 }) {
		t.Fatal("the dead consumer's entry was not left pending")
	}

	// A second consumer takes the entry over once it has been pending
	// long enough, and works it.
	runner := &fakeRunner{}
	second, err := NewRedisWorkQueue(client,
		WithGroup(queue.group),
		WithClaimIdle(50*time.Millisecond),
		WithBlockTimeout(20*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("could not build the second consumer: %v", err)
	}
	defer consume(t, second, runner)()

	if !waitUntil(2*time.Second, func() bool { return runner.callCount() == 1 }) {
		t.Fatal("a dead consumer's pending entry was never recovered")
	}
	if !waitUntil(2*time.Second, func() bool { return pendingCount(t, client, queue) == 0 }) {
		t.Fatal("the recovered entry was left unsettled")
	}
}

// fakeCompactionRunner answers compaction items from a script of results,
// like fakeRunner, and records what it was asked to compute.
type fakeCompactionRunner struct {
	mu       sync.Mutex
	calls    int
	sessions []string
	requests []string
	ids      []string
	script   []result
	// hold parks each run until the test releases it, so that a consumer
	// can be seen holding an entry it has not settled.
	hold chan struct{}
	// entered reports that a run has been parked.
	entered     chan struct{}
	enteredOnce sync.Once
}

func (f *fakeCompactionRunner) Work(_ context.Context, sessionID, requestID, agentID string) (bool, error) {
	f.mu.Lock()
	f.calls++
	f.sessions = append(f.sessions, sessionID)
	f.requests = append(f.requests, requestID)
	f.ids = append(f.ids, agentID)
	i := f.calls - 1
	res := result{worked: true}
	if i < len(f.script) {
		res = f.script[i]
	} else if len(f.script) > 0 {
		res = f.script[len(f.script)-1]
	}
	f.mu.Unlock()

	if f.hold != nil {
		f.enteredOnce.Do(func() { close(f.entered) })
		<-f.hold
	}
	return res.worked, res.err
}

func (f *fakeCompactionRunner) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// consumeBoth runs the queue with both workers until the test is done with it.
func consumeBoth(t *testing.T, queue *RedisWorkQueue, arcs ArcRunner, compactions CompactionRunner) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = queue.Consume(ctx, arcs, compactions)
	}()
	return func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("the consumer did not stop")
		}
	}
}

func TestEnqueueCompactionRefusesAnIncompleteItem(t *testing.T) {
	queue, _ := newQueue(t)

	if err := queue.EnqueueCompaction(context.Background(), "s", "req-1", ""); err == nil {
		t.Fatal("a compaction item naming no agent was enqueued")
	}
	if err := queue.EnqueueCompaction(context.Background(), "s", "", "agent-1"); err == nil {
		t.Fatal("a compaction item naming no request was enqueued")
	}
}

func TestCompactionItemIsRoutedToTheCompactionRunner(t *testing.T) {
	queue, client := newQueue(t)
	if err := queue.EnqueueCompaction(context.Background(), "s", "req-1", "agent-1"); err != nil {
		t.Fatalf("could not enqueue the compaction item: %v", err)
	}

	arcs := &fakeRunner{}
	compactions := &fakeCompactionRunner{}
	defer consumeBoth(t, queue, arcs, compactions)()

	if !waitUntil(2*time.Second, func() bool { return compactions.callCount() == 1 }) {
		t.Fatal("the compaction item was never worked")
	}
	compactions.mu.Lock()
	if compactions.sessions[0] != "s" || compactions.requests[0] != "req-1" || compactions.ids[0] != "agent-1" {
		t.Fatalf("the item was worked as %s/%s under %s",
			compactions.sessions[0], compactions.requests[0], compactions.ids[0])
	}
	compactions.mu.Unlock()
	if arcs.callCount() != 0 {
		t.Fatal("a compaction item reached the arc runner")
	}
	if !waitUntil(2*time.Second, func() bool { return pendingCount(t, client, queue) == 0 }) {
		t.Fatal("a worked compaction item was left unsettled")
	}
}

func TestCompactionItemWithNoCompactionRunnerStaysPending(t *testing.T) {
	queue, client := newQueue(t)
	if err := queue.EnqueueCompaction(context.Background(), "s", "req-1", "agent-1"); err != nil {
		t.Fatalf("could not enqueue the compaction item: %v", err)
	}

	arcs := &fakeRunner{}
	defer consume(t, queue, arcs)()

	// The consumer has no compaction worker, so the item is left for one
	// that does: it stays pending and no run is spent on it.
	if !waitUntil(2*time.Second, func() bool { return pendingCount(t, client, queue) == 1 }) {
		t.Fatal("the compaction item was not left pending")
	}
	time.Sleep(100 * time.Millisecond)
	if arcs.callCount() != 0 {
		t.Fatal("a compaction item reached the arc runner")
	}
	if pendingCount(t, client, queue) != 1 {
		t.Fatal("the pending compaction item was settled without a compaction runner")
	}
}

func TestHeldCompactionLeaseReoffersTheItem(t *testing.T) {
	queue, client := newQueue(t)
	if err := queue.EnqueueCompaction(context.Background(), "s", "req-1", "agent-1"); err != nil {
		t.Fatalf("could not enqueue the compaction item: %v", err)
	}

	// The first run finds the session lease held; the re-offer works it.
	compactions := &fakeCompactionRunner{script: []result{{worked: false}, {worked: true}}}
	defer consumeBoth(t, queue, &fakeRunner{}, compactions)()

	if !waitUntil(2*time.Second, func() bool { return compactions.callCount() >= 2 }) {
		t.Fatal("an item that found its session lease held was never worked again")
	}
	if !waitUntil(2*time.Second, func() bool { return pendingCount(t, client, queue) == 0 }) {
		t.Fatal("a contended compaction item was left unsettled")
	}
}

func TestADeadConsumersCompactionItemIsRecovered(t *testing.T) {
	queue, client := newQueue(t)
	if err := queue.EnqueueCompaction(context.Background(), "s", "req-1", "agent-1"); err != nil {
		t.Fatalf("could not enqueue the compaction item: %v", err)
	}

	// The first consumer claims the compaction item and parks mid-run, then
	// dies without settling it: the entry stays pending under its name.
	held := make(chan struct{})
	blocked := &fakeCompactionRunner{hold: held, entered: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = queue.Consume(ctx, &fakeRunner{}, blocked)
	}()
	<-blocked.entered
	cancel()
	close(held)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the dead consumer did not stop when its context ended")
	}
	if !waitUntil(2*time.Second, func() bool { return pendingCount(t, client, queue) == 1 }) {
		t.Fatal("the dead consumer's compaction entry was not left pending")
	}

	// A second consumer takes the entry over once it has been pending long
	// enough, and computes the compaction.
	compactions := &fakeCompactionRunner{}
	second, err := NewRedisWorkQueue(client,
		WithGroup(queue.group),
		WithClaimIdle(50*time.Millisecond),
		WithBlockTimeout(20*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("could not build the second consumer: %v", err)
	}
	defer consumeBoth(t, second, &fakeRunner{}, compactions)()

	if !waitUntil(2*time.Second, func() bool { return compactions.callCount() == 1 }) {
		t.Fatal("a dead consumer's pending compaction entry was never recovered")
	}
	if !waitUntil(2*time.Second, func() bool { return pendingCount(t, client, queue) == 0 }) {
		t.Fatal("the recovered compaction entry was left unsettled")
	}
}
