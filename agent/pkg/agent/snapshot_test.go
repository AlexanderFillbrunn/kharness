package agent

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"fillbrunn.ai/kube-agent/proto/pkg/common"
	"fillbrunn.ai/kube-agent/proto/pkg/events"
	"fillbrunn.ai/kube-agent/proto/pkg/history"
)

// countingSource wraps a logStore and counts the arc reads that asked for
// snapshots, so a test can tell which way the worker read.
type countingSource struct {
	*logStore
	snapshotReads int
}

func (s *countingSource) GetEventsForArc(ctx context.Context, ref common.ArcRef, opts common.ReadOptions) (*common.Snapshot, []*events.Event, error) {
	if opts.UseSnapshots {
		s.snapshotReads++
	}
	return s.logStore.GetEventsForArc(ctx, ref, opts)
}

func normalized(t *testing.T, v any) any {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out any
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("normalize: %v", err)
	}
	return out
}

func lastSeq(t *testing.T, store *logStore) int {
	t.Helper()
	_, log, err := store.GetEventsForSession(context.Background(), testSessionID, common.ReadOptions{})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(log) == 0 {
		return -1
	}
	return log[len(log)-1].Seq
}

// The save point of a work item: with the policy enabled, the worker stores
// the fold at the log's head once the run is over, and a later read through
// that snapshot folds the same history the whole log folds.
func TestArcWorkerSavesASnapshotAtTheEndOfAWorkItem(t *testing.T) {
	store := &logStore{}
	arcHistoryWith(t, store, userEvent(0, testArcID, "how many pods?"))

	llm := &scriptedLLM{replies: []*LLMResponse{{ModelID: "model-1", Content: "there are two"}}}
	source := &countingSource{logStore: store}
	worker, err := NewArcWorker(store, source, NewFixedResolver(mustAgent(t, llm)), nil,
		WithSnapshotPolicy(SnapshotPolicy{Enabled: true}))
	if err != nil {
		t.Fatalf("NewArcWorker: %v", err)
	}

	if worked, err := worker.Work(context.Background(), testRef(), testAgentID, 0); err != nil || !worked {
		t.Fatalf("Work = %v, %v", worked, err)
	}

	if source.snapshotReads != 1 {
		t.Fatalf("the worker made %d snapshot reads, want 1", source.snapshotReads)
	}
	if store.snap == nil {
		t.Fatal("no snapshot was saved at the end of the work item")
	}
	if want := lastSeq(t, store); store.snap.Seq() != want {
		t.Fatalf("snapshot seq %d, want the log's head %d", store.snap.Seq(), want)
	}

	snap, tail, err := store.GetEventsForSession(context.Background(), testSessionID, common.ReadOptions{UseSnapshots: true})
	if err != nil {
		t.Fatalf("snapshot read: %v", err)
	}
	resumed, err := common.FoldEvents(snap, tail)
	if err != nil {
		t.Fatalf("FoldEvents: %v", err)
	}
	_, whole, err := store.GetEventsForSession(context.Background(), testSessionID, common.ReadOptions{})
	if err != nil {
		t.Fatalf("full read: %v", err)
	}
	full, err := common.FoldEvents(nil, whole)
	if err != nil {
		t.Fatalf("FoldEvents full: %v", err)
	}
	if !reflect.DeepEqual(normalized(t, full), normalized(t, resumed)) {
		t.Fatal("the snapshot read folded a different history than the whole log")
	}
}

// Without the policy the worker neither reads through snapshots nor saves
// one, which is how a deployment that has not opted in behaves.
func TestArcWorkerWithoutThePolicyNeverTouchesSnapshots(t *testing.T) {
	store := &logStore{}
	arcHistoryWith(t, store, userEvent(0, testArcID, "how many pods?"))

	llm := &scriptedLLM{replies: []*LLMResponse{{ModelID: "model-1", Content: "there are two"}}}
	source := &countingSource{logStore: store}
	worker, err := NewArcWorker(store, source, NewFixedResolver(mustAgent(t, llm)), nil)
	if err != nil {
		t.Fatalf("NewArcWorker: %v", err)
	}
	if worked, err := worker.Work(context.Background(), testRef(), testAgentID, 0); err != nil || !worked {
		t.Fatalf("Work = %v, %v", worked, err)
	}
	if source.snapshotReads != 0 {
		t.Fatalf("the worker made %d snapshot reads with the policy off, want 0", source.snapshotReads)
	}
	if store.snap != nil {
		t.Fatal("a snapshot was saved with the policy off")
	}
}

// MinEvents makes saving less often than every save point: the first work
// item has no snapshot to compare against and saves, the next one has grown
// the log by less than MinEvents and leaves the stored snapshot alone.
func TestSnapshotPolicyMinEventsThrottlesSaving(t *testing.T) {
	store := &logStore{}
	arcHistoryWith(t, store, userEvent(0, testArcID, "first question"))

	llm := &scriptedLLM{replies: []*LLMResponse{
		{ModelID: "model-1", Content: "first answer"},
		{ModelID: "model-1", Content: "second answer"},
	}}
	worker, err := NewArcWorker(store, store, NewFixedResolver(mustAgent(t, llm)), nil,
		WithSnapshotPolicy(SnapshotPolicy{Enabled: true, MinEvents: 1000}))
	if err != nil {
		t.Fatalf("NewArcWorker: %v", err)
	}

	if worked, err := worker.Work(context.Background(), testRef(), testAgentID, 0); err != nil || !worked {
		t.Fatalf("Work = %v, %v", worked, err)
	}
	if store.snap == nil {
		t.Fatal("the first work item saved no snapshot")
	}
	firstSeq := store.snap.Seq()

	// A second arc, worked by the same worker.
	arcHistoryWith(t, store, userEvent(10, "arc-2", "second question"))
	if worked, err := worker.Work(context.Background(), common.ArcRef{SessionID: testSessionID, ArcID: "arc-2"}, testAgentID, 0); err != nil || !worked {
		t.Fatalf("Work arc-2 = %v, %v", worked, err)
	}
	if store.snap.Seq() != firstSeq {
		t.Fatalf("snapshot moved from %d to %d under a MinEvents of 1000", firstSeq, store.snap.Seq())
	}
	if last := lastSeq(t, store); last <= firstSeq {
		t.Fatalf("the log's head %d did not move past the snapshot %d", last, firstSeq)
	}
}

// The compaction worker's save point: once the COMPACTED is folded, the fold
// is stored at the log's head.
func TestCompactionWorkerSavesASnapshotAfterTheCompacted(t *testing.T) {
	store := &logStore{}
	arcHistoryWith(t, store,
		userEvent(0, "a0", strings.Repeat("x", 200)),
		assistantEvent(1, "a0", "r0", "done", &events.TokenUsage{Input: 10}),
		events.Event{SessionID: testSessionID, ID: "close-a0", Seq: 2, Timestamp: time.Unix(2, 0),
			Typ: events.ArcClosedEventType, CorrelationID: ptr("a0"), Payload: events.ArcClosedPayload{CausationID: "r0"}},
		userEvent(3, "a1", strings.Repeat("y", 200)),
		assistantEvent(4, "a1", "r1", "working", &events.TokenUsage{Input: 10}),
		requestEvent(5, "a1", "req-1", -1),
		suspendEvent(6, "a1", "req-1"),
	)
	worker, err := NewCompactionWorker(store, store, NewFixedCompactionResolver(prunePlan()),
		NewLocalSessionLease(), &fakeArcEnqueuer{}, history.DefaultModes(),
		WithCompactionSnapshotPolicy(SnapshotPolicy{Enabled: true}))
	if err != nil {
		t.Fatalf("NewCompactionWorker: %v", err)
	}

	if worked, err := worker.Work(context.Background(), testSessionID, "req-1", testAgentID); err != nil || !worked {
		t.Fatalf("Work = %v, %v", worked, err)
	}

	var compacted *events.Event
	for _, ev := range store.events {
		if ev.Typ == events.CompactedEventType {
			compacted = ev
		}
	}
	if compacted == nil {
		t.Fatal("no COMPACTED in the log")
	}
	if store.snap == nil {
		t.Fatal("no snapshot was saved after the compaction")
	}
	if store.snap.Seq() != compacted.Seq {
		t.Fatalf("snapshot seq %d, want the COMPACTED's seq %d", store.snap.Seq(), compacted.Seq)
	}
}

// The save decision reads only the stored snapshot's info: its seq and when
// it was saved, nil where nothing is stored.
func TestSnapshotPolicyDueReadsTheInfo(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	stored := &common.SnapshotInfo{Seq: 10, SavedAt: now.Add(-time.Minute)}
	hourOld := &common.SnapshotInfo{Seq: 10, SavedAt: now.Add(-time.Hour)}

	if !(SnapshotPolicy{Enabled: true}).due(nil, 12, now) {
		t.Error("nothing stored: a save is always due")
	}
	if (SnapshotPolicy{}).due(nil, 12, now) {
		t.Error("policy off: never due")
	}
	if !(SnapshotPolicy{Enabled: true, MinEvents: 5}).due(stored, 15, now) {
		t.Error("5 new events under MinEvents 5: due")
	}
	if (SnapshotPolicy{Enabled: true, MinEvents: 5}).due(stored, 14, now) {
		t.Error("4 new events under MinEvents 5: not due")
	}
	if !(SnapshotPolicy{Enabled: true, MinInterval: time.Hour}).due(hourOld, 100, now) {
		t.Error("an hour-old snapshot under MinInterval an hour: due")
	}
	if (SnapshotPolicy{Enabled: true, MinInterval: time.Hour}).due(stored, 100, now.Add(-time.Minute)) {
		t.Error("a minute-old snapshot under MinInterval an hour: not due")
	}
}

func mustAgent(t *testing.T, llm LLM) *Agent {
	t.Helper()
	agent, err := NewAgent(nil, nil, llm)
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	return agent
}
