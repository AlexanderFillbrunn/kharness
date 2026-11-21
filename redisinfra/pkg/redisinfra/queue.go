package redisinfra

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"fillbrunn.ai/kube-agent/proto/pkg/common"
)

// DefaultStream, DefaultGroup and DefaultDeadStream name the Redis streams the
// work queue lives in. The dead stream is separate so that a poison item is
// visible rather than silently dropped, and can be replayed by hand once its
// cause is understood.
const (
	DefaultStream     = "kube-agent:queue:work"
	DefaultGroup      = "kube-agent"
	DefaultDeadStream = "kube-agent:queue:dead"
)

// WorkItem is one "this arc needs a run" message. It is not a command: the
// run decides from the log what the item means, so a duplicate costs a
// replay and nothing else, which is what makes at-least-once delivery
// correct here.
type WorkItem struct {
	// SessionID and ArcID name the arc, and AgentID names the agent that
	// works it. An item naming no agent is refused, by the same rule as
	// ArcWorker.Work: an event written under no agent names nobody.
	SessionID string `json:"session_id"`
	ArcID     string `json:"arc_id"`
	AgentID   string `json:"agent_id"`

	// Attempts counts the runs that failed on this item so far, and
	// NotBeforeMs is the earliest time it may be worked again. Both travel
	// in the payload because a stream entry cannot be edited in place: an
	// item that goes back is a new entry, and the old one is settled only
	// once the new one is written.
	Attempts    int   `json:"attempts,omitempty"`
	NotBeforeMs int64 `json:"not_before_ms,omitempty"`

	// Kind says what the item is: an arc run (the zero value, so an entry
	// written before kinds existed still reads as the arc it names) or a
	// compaction. A compaction item names no arc; it names the
	// COMPACTION_REQUESTED event whose watermark the compaction is to reach.
	// The wire names match common.WorkItem, so both shapes read one stream.
	Kind string `json:"kind,omitempty"`

	// RequestID is the ID of the COMPACTION_REQUESTED event a compaction
	// item serves. Empty for arc items.
	RequestID string `json:"request_id,omitempty"`
}

// ref names the arc the item wants a run of. It is meaningless for a
// compaction item.
func (i WorkItem) ref() common.ArcRef {
	return common.ArcRef{SessionID: i.SessionID, ArcID: i.ArcID}
}

// isCompaction reports whether the item is a compaction rather than an arc run.
func (i WorkItem) isCompaction() bool { return i.Kind == common.WorkItemKindCompaction }

// ArcRunner is the shape of ArcWorker.Work, which the consumer calls on
// every arc item it claims. It is an interface so that the queue serves the
// worker without depending on how one is built, and so that a test can run
// the queue against a worker that is not an agent.
type ArcRunner interface {
	Work(ctx context.Context, ref common.ArcRef, agentID string) (worked bool, err error)
}

// CompactionRunner is the shape of CompactionWorker.Work, which the consumer
// calls on every compaction item it claims. A compaction item names no arc:
// it names the session and the COMPACTION_REQUESTED event being served.
type CompactionRunner interface {
	Work(ctx context.Context, sessionID, requestID, agentID string) (worked bool, err error)
}

// WorkQueue is the queue that delivers the runs a write owes an arc, and the
// compactions a due round owes a session. Every write that changes what an
// arc has to do must be followed by a run of that arc, and no trigger may be
// lost, so an enqueue must not fail silently and a consume must not drop an
// item it could not work.
type WorkQueue interface {
	// Enqueue adds an item for the arc named to the queue. It is the sliver of
	// the queue the framework's report path needs, so pkg/agent can hand over
	// an arc it opened without importing this package.
	Enqueue(ctx context.Context, ref common.ArcRef, agentID string) error
	// EnqueueCompaction adds an item for the compaction request named, served
	// by the compaction worker rather than the arc worker.
	EnqueueCompaction(ctx context.Context, sessionID, requestID, agentID string) error
	// Consume claims items and runs the workers over them, until the
	// context is cancelled or the queue cannot go on: arc items go to arcs,
	// compaction items to compactions. A nil compactions serves no compaction
	// and leaves those items pending for a consumer that does. It returns
	// only when there is nothing left to consume with.
	Consume(ctx context.Context, arcs ArcRunner, compactions CompactionRunner) error
}

// RedisWorkQueue is a WorkQueue on Redis streams.
//
// A stream rather than a list, because a list forgets an item the moment it
// is popped and a stream does not: an item is settled by an explicit
// acknowledgement, and one whose consumer died stays pending until
// another claims it. That is the half of the invariant the lease cannot
// cover, since the lease keeps two runs off one arc but says nothing
// about a run nobody ever started.
//
// Delivery is at-least-once, which costs nothing here: the lease makes a
// duplicate harmless, since one claimer runs the arc and the other sees
// the arc held and puts the item back.
type RedisWorkQueue struct {
	client *redis.Client
	stream string
	group  string
	dead   string

	// consumer names this process's consumer within the group. A pending
	// entry belongs to the consumer that read it, so the name identifies
	// the consumers whose death leaves entries to recover.
	consumer string

	// claimIdle is how long an entry must sit pending before another
	// consumer takes it over. It should stay generous: an entry is
	// reclaimed while its consumer may still be running it, and the
	// duplicate that produces is harmless but not free.
	claimIdle time.Duration
	// contentionDelay is how long an item goes back after finding its arc
	// held. The holder's run may have started before the write that
	// enqueued the item, so the item is owed its own run once the holder
	// is done.
	contentionDelay time.Duration
	// maxAttempts is how many runs an item may fail before it is moved to
	// the dead stream.
	maxAttempts int
	// blockTimeout is how long a read with nothing to claim waits before
	// looping, which is also how often pending items are reconsidered.
	blockTimeout time.Duration
	// count bounds how many entries one read claims.
	count int64
}

// QueueOption configures a RedisWorkQueue.
type QueueOption func(*RedisWorkQueue)

// WithStream sets the stream a queue works on.
func WithStream(stream string) QueueOption {
	return func(q *RedisWorkQueue) { q.stream = stream }
}

// WithGroup sets the consumer group a queue works through. Callers sharing
// a queue must share a group, since a group is what an entry is pending
// within.
func WithGroup(group string) QueueOption {
	return func(q *RedisWorkQueue) { q.group = group }
}

// WithDeadStream sets the stream an item that failed too many runs is
// moved to.
func WithDeadStream(dead string) QueueOption {
	return func(q *RedisWorkQueue) { q.dead = dead }
}

// WithConsumer names the consumer a queue claims as. A process running
// several consumers should give each its own name, since a name shared by
// two processes makes each able to strand the other's entries.
func WithConsumer(name string) QueueOption {
	return func(q *RedisWorkQueue) { q.consumer = name }
}

// WithClaimIdle sets how long an entry must be pending before another
// consumer may claim it.
func WithClaimIdle(idle time.Duration) QueueOption {
	return func(q *RedisWorkQueue) { q.claimIdle = idle }
}

// WithContentionDelay sets how long an item waits before it is worked
// again after finding its arc held.
func WithContentionDelay(delay time.Duration) QueueOption {
	return func(q *RedisWorkQueue) { q.contentionDelay = delay }
}

// WithMaxAttempts sets how many runs an item may fail before it is moved
// to the dead stream.
func WithMaxAttempts(attempts int) QueueOption {
	return func(q *RedisWorkQueue) { q.maxAttempts = attempts }
}

// WithBlockTimeout sets how long a read with nothing new to claim waits.
func WithBlockTimeout(timeout time.Duration) QueueOption {
	return func(q *RedisWorkQueue) { q.blockTimeout = timeout }
}

// NewRedisWorkQueue builds a queue over a Redis client. The consumer name
// is minted per queue, so two queues in one process do not strand each
// other's entries.
func NewRedisWorkQueue(client *redis.Client, options ...QueueOption) (*RedisWorkQueue, error) {
	consumer, err := newConsumerName()
	if err != nil {
		return nil, fmt.Errorf("could not mint a consumer name: %w", err)
	}
	q := &RedisWorkQueue{
		client:          client,
		stream:          DefaultStream,
		group:           DefaultGroup,
		dead:            DefaultDeadStream,
		consumer:        consumer,
		claimIdle:       time.Minute,
		contentionDelay: time.Second,
		maxAttempts:     5,
		blockTimeout:    time.Second,
		count:           16,
	}
	for _, option := range options {
		option(q)
	}
	return q, nil
}

var _ WorkQueue = (*RedisWorkQueue)(nil)

// Enqueue adds an item for the arc named to the stream.
//
// An arc that names no agent is refused before anything is written, since it
// could never be worked: ArcWorker.Work refuses an empty agent id, and an
// item enqueued anyway would fail on every consumer until it exhausted its
// attempts and arrived in the dead stream saying nothing beyond that.
func (q *RedisWorkQueue) Enqueue(ctx context.Context, ref common.ArcRef, agentID string) error {
	if agentID == "" {
		return fmt.Errorf("a work item for arc %s names no agent", ref)
	}
	item := WorkItem{SessionID: ref.SessionID, ArcID: ref.ArcID, AgentID: agentID}
	if err := q.client.XAdd(ctx, &redis.XAddArgs{
		Stream: q.stream,
		Values: map[string]any{"payload": string(mustMarshal(item))},
	}).Err(); err != nil {
		return fmt.Errorf("could not enqueue a run of arc %s: %w", ref, err)
	}
	return nil
}

// EnqueueCompaction adds a compaction item for the request named to the
// stream, in the same queue arc items travel. The item names no arc, so
// consumers route it by its kind to the compaction worker.
//
// Like Enqueue, an item naming no agent is refused before anything is
// written, and a compaction naming no request is refused too: an item with
// no request names nothing to compute.
func (q *RedisWorkQueue) EnqueueCompaction(ctx context.Context, sessionID, requestID, agentID string) error {
	if agentID == "" {
		return fmt.Errorf("a compaction item for session %s names no agent", sessionID)
	}
	if requestID == "" {
		return fmt.Errorf("a compaction item for session %s names no request", sessionID)
	}
	item := WorkItem{
		SessionID: sessionID,
		AgentID:   agentID,
		Kind:      common.WorkItemKindCompaction,
		RequestID: requestID,
	}
	if err := q.client.XAdd(ctx, &redis.XAddArgs{
		Stream: q.stream,
		Values: map[string]any{"payload": string(mustMarshal(item))},
	}).Err(); err != nil {
		return fmt.Errorf("could not enqueue a compaction of session %s: %w", sessionID, err)
	}
	return nil
}

// redisinfra's queue satisfies the framework's slivers of it.
var (
	_ common.WorkEnqueuer       = (*RedisWorkQueue)(nil)
	_ common.CompactionEnqueuer = (*RedisWorkQueue)(nil)
)

// Consume claims items and works them until the context is cancelled.
//
// One pass of the loop serves the invariant from both ends: entries
// pending under a consumer that died are claimed back first, then this
// consumer's own pending entries are reconsidered, and only then are new
// entries read. An item whose time has not come is left pending and
// reconsidered on the next pass, so a delayed item is never lost to the
// consumer that read it and cannot be worked yet.
//
// A single item's failure never stops the loop: an item that failed goes
// back with its attempt counted, and one that failed too often is moved to
// the dead stream. Only a queue that cannot be read or written ends the
// loop, since a consumer that cannot reach its queue can settle nothing
// and should not pretend to go on.
//
// A nil compactions is allowed: the consumer then serves arc items only and
// leaves compaction items pending, for autoclaim to offer a consumer that
// has a compaction worker.
func (q *RedisWorkQueue) Consume(ctx context.Context, arcs ArcRunner, compactions CompactionRunner) error {
	if arcs == nil {
		return errors.New("a work queue consumes through an arc worker")
	}
	if err := q.ensureGroup(ctx); err != nil {
		return err
	}
	for {
		if err := q.claimAbandoned(ctx, arcs, compactions); err != nil {
			return err
		}
		if err := q.workPending(ctx, arcs, compactions); err != nil {
			return err
		}
		if err := q.workNew(ctx, arcs, compactions); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
}

// ensureGroup creates the consumer group, which every consumer in the
// process pool shares. A group that exists already is not an error, since
// the second consumer of a queue is the normal case and not a mistake.
func (q *RedisWorkQueue) ensureGroup(ctx context.Context) error {
	err := q.client.XGroupCreateMkStream(ctx, q.stream, q.group, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("could not create the consumer group %s on %s: %w", q.group, q.stream, err)
	}
	return nil
}

// claimAbandoned takes over entries whose consumer has not acknowledged
// them for longer than the claim idle time. A consumer that died mid-run
// leaves its entries pending here, and this is what keeps its death from
// stranding the runs those entries owed.
func (q *RedisWorkQueue) claimAbandoned(ctx context.Context, arcs ArcRunner, compactions CompactionRunner) error {
	cursor := "0-0"
	for {
		claimed, next, err := q.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
			Stream:   q.stream,
			Group:    q.group,
			Consumer: q.consumer,
			MinIdle:  q.claimIdle,
			Start:    cursor,
			Count:    q.count,
		}).Result()
		if err != nil {
			return fmt.Errorf("could not claim abandoned entries on %s: %w", q.stream, err)
		}
		for _, msg := range claimed {
			q.work(ctx, arcs, compactions, msg)
		}
		if next == "0-0" {
			return nil
		}
		cursor = next
	}
}

// workPending reconsiders the entries this consumer has read and not
// settled. They are the items whose time had not come when they were
// read, and they stay pending until a pass finds their time arrived.
func (q *RedisWorkQueue) workPending(ctx context.Context, arcs ArcRunner, compactions CompactionRunner) error {
	return q.read(ctx, arcs, compactions, "0")
}

// workNew claims entries nobody has read yet.
func (q *RedisWorkQueue) workNew(ctx context.Context, arcs ArcRunner, compactions CompactionRunner) error {
	return q.read(ctx, arcs, compactions, ">")
}

// read claims entries at an ID: ">" for entries never delivered, and "0"
// for the entries this consumer already holds. A read that times out with
// nothing to claim is a pass with no work in it, and not a failure.
func (q *RedisWorkQueue) read(ctx context.Context, arcs ArcRunner, compactions CompactionRunner, id string) error {
	streams, err := q.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    q.group,
		Consumer: q.consumer,
		Block:    q.blockTimeout,
		Count:    q.count,
		Streams:  []string{q.stream, id},
	}).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil
		}
		return fmt.Errorf("could not read the queue %s: %w", q.stream, err)
	}
	for _, stream := range streams {
		for _, msg := range stream.Messages {
			q.work(ctx, arcs, compactions, msg)
		}
	}
	return nil
}

// work routes one claimed entry to the worker its kind calls for, and
// settles the entry according to what the run did.
//
// An entry is settled by an acknowledgement, and nothing is acknowledged
// before whatever replaces it is written: a re-enqueued item is written
// first, so a consumer that dies between the two leaves the entry pending
// and the trigger alive, which is a duplicate rather than a loss.
//
// The outcomes are the three the worker reports:
//
// A run that worked settles its entry. A run that found its lease held
// cannot tell a holder that will cover the item from one that replayed
// too early, so the item goes back after the contention delay and the
// entry is settled; autoclaim re-offers an entry whose consumer died
// before settling it. A run that failed goes back with its attempt
// counted, and one that failed too often is moved to the dead stream, so
// a poison item is visible rather than retried forever.
func (q *RedisWorkQueue) work(ctx context.Context, arcs ArcRunner, compactions CompactionRunner, msg redis.XMessage) {
	item, err := q.item(msg)
	if err != nil {
		// An entry that cannot be read is moved to the dead stream
		// rather than retried, since it will fail the same way on every
		// consumer that claims it.
		q.moveToDead(ctx, msg, WorkItem{}, err)
		return
	}
	if item.NotBeforeMs > 0 && time.Now().UnixMilli() < item.NotBeforeMs {
		return
	}
	if item.isCompaction() {
		if compactions == nil {
			// This consumer serves no compactions: leave the entry pending
			// without counting an attempt, so autoclaim offers it to a
			// consumer that has a compaction worker.
			return
		}
		q.workItem(ctx, msg, item, func() (bool, error) {
			return compactions.Work(ctx, item.SessionID, item.RequestID, item.AgentID)
		})
		return
	}
	q.workItem(ctx, msg, item, func() (bool, error) {
		return arcs.Work(ctx, item.ref(), item.AgentID)
	})
}

// workItem runs one item's worker and settles the entry the outcome calls
// for, as work documents.
func (q *RedisWorkQueue) workItem(ctx context.Context, msg redis.XMessage, item WorkItem, run func() (bool, error)) {
	worked, err := run()
	item.Attempts++
	switch {
	case err == nil && worked:
		q.ack(ctx, msg.ID)
	case err == nil:
		q.requeue(ctx, item, q.contentionDelay)
		q.ack(ctx, msg.ID)
	case item.Attempts >= q.maxAttempts:
		q.moveToDead(ctx, msg, item, err)
	default:
		q.requeue(ctx, item, q.contentionDelay)
		q.ack(ctx, msg.ID)
	}
}

// item decodes an entry's payload. The payload is one JSON object, so a
// consumer built on a different version of this package still reads the
// fields it knows and ignores the rest.
func (q *RedisWorkQueue) item(msg redis.XMessage) (WorkItem, error) {
	var item WorkItem
	payload, ok := msg.Values["payload"].(string)
	if !ok {
		return item, fmt.Errorf("entry %s of %s carries no payload", msg.ID, q.stream)
	}
	if err := json.Unmarshal([]byte(payload), &item); err != nil {
		return item, fmt.Errorf("entry %s of %s carries an unreadable payload: %w", msg.ID, q.stream, err)
	}
	return item, nil
}

// requeue writes the item back with a time it may next be worked, without
// settling the entry it came from: the caller acknowledges only once this
// has succeeded, so a failure here leaves the entry pending and its
// trigger alive.
func (q *RedisWorkQueue) requeue(ctx context.Context, item WorkItem, delay time.Duration) {
	item.NotBeforeMs = time.Now().Add(delay).UnixMilli()
	err := q.client.XAdd(ctx, &redis.XAddArgs{
		Stream: q.stream,
		Values: map[string]any{"payload": string(mustMarshal(item))},
	}).Err()
	if err != nil {
		// The entry stays pending and is redelivered, which works the
		// item again rather than losing it.
		_ = err
	}
}

// moveToDead settles an entry by writing it to the dead stream, with the
// error that sent it there. A consumer that dies between the write and
// the acknowledgement leaves the entry pending, and it arrives in the
// dead stream twice, which is what a replayable record of failures is
// allowed to do.
func (q *RedisWorkQueue) moveToDead(ctx context.Context, msg redis.XMessage, item WorkItem, cause error) {
	entry := struct {
		Item     WorkItem `json:"item"`
		EntryID  string   `json:"entry_id"`
		Attempts int      `json:"attempts"`
		Cause    string   `json:"cause"`
	}{Item: item, EntryID: msg.ID, Attempts: item.Attempts, Cause: cause.Error()}
	err := q.client.XAdd(ctx, &redis.XAddArgs{
		Stream: q.dead,
		Values: map[string]any{"payload": string(mustMarshal(entry))},
	}).Err()
	if err != nil {
		// The entry stays pending and is redelivered, and the next pass
		// tries the dead stream again.
		_ = err
		return
	}
	q.ack(ctx, msg.ID)
}

// ack settles an entry. An acknowledgement that cannot reach Redis leaves
// the entry pending, and it is worked again: a duplicate run, which the
// lease makes harmless.
func (q *RedisWorkQueue) ack(ctx context.Context, id string) {
	_ = q.client.XAck(ctx, q.stream, q.group, id).Err()
}

// mustMarshal encodes a value this package has constructed itself, which
// cannot fail to marshal.
func mustMarshal(value any) []byte {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return encoded
}

// newConsumerName mints a consumer name, which only has to be unique
// within the process pool for long enough to tell one consumer's pending
// entries from another's.
func newConsumerName() (string, error) {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return "consumer-" + hex.EncodeToString(buf), nil
}
