package filestore

import (
	"os"
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"fillbrunn.ai/kube-agent/proto/pkg/common"
	"fillbrunn.ai/kube-agent/proto/pkg/events"
	"fillbrunn.ai/kube-agent/proto/pkg/history"
	"fillbrunn.ai/kube-agent/proto/pkg/users"
)

// writeSnapshotLog writes a session whose first arc started a task, then
// opened a second arc, and returns the stored events in write order.
func writeSnapshotLog(t *testing.T, store *Store) []*events.Event {
	t.Helper()
	ctx := context.Background()
	arc1, arc2, taskID := "arc-1", "arc-2", "task-1"
	actor := users.Actor{UserID: "dev", TenantID: "dev"}
	invoke := &events.Event{
		SessionID: "session", ID: taskID, Timestamp: time.Now().UTC(),
		Typ: events.AsyncTaskInvokeEventType, CorrelationID: &arc1, Actor: actor,
		Payload: events.AsyncTaskInvokePayload{TaskName: "auditor", Actor: actor},
	}
	noise := &events.Event{
		SessionID: "session", ID: "n-1", Timestamp: time.Now().UTC(),
		Typ: events.AsyncTaskEventEventType, CorrelationID: &taskID,
		Payload: events.AsyncTaskEventPayload{
			CausationID: taskID, Class: "detail", Payload: "noise",
			LLMVisibility: events.LLMVisibilityNone},
	}

	var stored []*events.Event
	for _, ev := range []*events.Event{
		userMessage(arc1, "audit the cluster"),      // seq 0
		assistantMessage(arc1, "starting the audit"), // seq 1
		invoke,                                       // seq 2
		noise,                                        // seq 3, correlated to the task
		userMessage(arc2, "what happened?"),          // seq 4
	} {
		persisted, err := store.Persist(ctx, ev)
		if err != nil {
			t.Fatalf("Persist: %v", err)
		}
		stored = append(stored, persisted[0])
	}
	return stored
}

func foldThrough(t *testing.T, log []*events.Event, through int) *history.History {
	t.Helper()
	h, err := history.Replay(log[:through+1])
	if err != nil {
		t.Fatalf("replay through %d: %v", through, err)
	}
	return h
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

func seqsOf(evs []*events.Event) []int {
	var out []int
	for _, ev := range evs {
		out = append(out, ev.Seq)
	}
	return out
}

// A saved snapshot lands in the sidecar file and is returned with only the
// events above its seq; the opted-out read is unchanged.
func TestStoreReadsTheSessionThroughASnapshot(t *testing.T) {
	store := openTemp(t)
	ctx := context.Background()
	log := writeSnapshotLog(t, store)

	if err := store.SaveSnapshot(ctx, "session", foldThrough(t, log, 2)); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	snap, tail, err := store.GetEventsForSession(ctx, "session", common.ReadOptions{UseSnapshots: true})
	if err != nil {
		t.Fatalf("GetEventsForSession: %v", err)
	}
	if snap == nil || snap.Seq() != 2 {
		t.Fatalf("snapshot seq %v, want 2", snap)
	}
	if got, want := seqsOf(tail), []int{3, 4}; !reflect.DeepEqual(got, want) {
		t.Fatalf("tail seqs %v, want %v", got, want)
	}

	resumed, err := common.FoldEvents(snap, tail)
	if err != nil {
		t.Fatalf("FoldEvents: %v", err)
	}
	_, whole, err := store.GetEventsForSession(ctx, "session", common.ReadOptions{})
	if err != nil {
		t.Fatalf("GetEventsForSession whole: %v", err)
	}
	full, err := common.FoldEvents(nil, whole)
	if err != nil {
		t.Fatalf("FoldEvents full: %v", err)
	}
	if !reflect.DeepEqual(normalized(t, full), normalized(t, resumed)) {
		t.Fatal("the snapshot read folded a different history than the whole log")
	}

	snap2, all, err := store.GetEventsForSession(ctx, "session", common.ReadOptions{})
	if err != nil {
		t.Fatalf("GetEventsForSession: %v", err)
	}
	if snap2 != nil || len(all) != 5 {
		t.Fatalf("opted-out read returned snapshot %v and %d events, want nil and 5", snap2, len(all))
	}
}

// The sidecar survives a reopen, the store keeps the newest snapshot, and a
// save beyond the log is refused.
func TestStoreSnapshotSurvivesReopenAndKeepsTheNewest(t *testing.T) {
	path := t.TempDir() + "/log.jsonl"
	store, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	ctx := context.Background()
	log := writeSnapshotLog(t, store)
	if err := store.SaveSnapshot(ctx, "session", foldThrough(t, log, 1)); err != nil {
		t.Fatalf("SaveSnapshot at 1: %v", err)
	}
	if err := store.SaveSnapshot(ctx, "session", foldThrough(t, log, 3)); err != nil {
		t.Fatalf("SaveSnapshot at 3: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { reopened.Close() })
	snap, tail, err := reopened.GetEventsForSession(ctx, "session", common.ReadOptions{UseSnapshots: true})
	if err != nil {
		t.Fatalf("GetEventsForSession: %v", err)
	}
	if snap == nil || snap.Seq() != 3 {
		t.Fatalf("snapshot after reopen seq %v, want 3", snap)
	}
	if got, want := seqsOf(tail), []int{4}; !reflect.DeepEqual(got, want) {
		t.Fatalf("tail seqs %v, want %v", got, want)
	}

	if err := reopened.SaveSnapshot(ctx, "session", foldThrough(t, log, 1)); err != nil {
		t.Fatalf("older SaveSnapshot: %v", err)
	}
	snap, _, err = reopened.GetEventsForSession(ctx, "session", common.ReadOptions{UseSnapshots: true})
	if err != nil {
		t.Fatalf("GetEventsForSession: %v", err)
	}
	if snap.Seq() != 3 {
		t.Fatalf("snapshot seq %d after the older save, want 3", snap.Seq())
	}

	bogus := history.NewHistory()
	bogus.LastSeq = 42
	if err := reopened.SaveSnapshot(ctx, "session", bogus); err == nil {
		t.Fatal("a snapshot beyond the log was accepted")
	}
}

// The arc read through a snapshot takes the opening and the task IDs from the
// snapshot's fold: the second arc's opening stays out of the first arc's
// tail, while the late task event stays in.
func TestStoreReadsAnArcThroughASnapshot(t *testing.T) {
	store := openTemp(t)
	ctx := context.Background()
	log := writeSnapshotLog(t, store)

	if err := store.SaveSnapshot(ctx, "session", foldThrough(t, log, 2)); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	ref1 := ref("arc-1")
	snap, tail, err := store.GetEventsForArc(ctx, ref1, common.ReadOptions{UseSnapshots: true})
	if err != nil {
		t.Fatalf("GetEventsForArc: %v", err)
	}
	if snap == nil || snap.Seq() != 2 {
		t.Fatalf("snapshot seq %v, want 2", snap)
	}
	if got, want := seqsOf(tail), []int{3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("arc-1 tail seqs %v, want %v (arc-2's opening excluded, the task event kept)", got, want)
	}

	resumed, err := common.FoldEvents(snap, tail)
	if err != nil {
		t.Fatalf("FoldEvents: %v", err)
	}
	_, whole, err := store.GetEventsForArc(ctx, ref1, common.ReadOptions{})
	if err != nil {
		t.Fatalf("GetEventsForArc whole: %v", err)
	}
	full, err := common.FoldEvents(nil, whole)
	if err != nil {
		t.Fatalf("FoldEvents full: %v", err)
	}
	if !reflect.DeepEqual(normalized(t, full), normalized(t, resumed)) {
		t.Fatal("the snapshot arc read folded a different history than the whole-log read")
	}

	// An arc opened after the snapshot is found in the tail, and its read
	// still takes everything up to its opening — here the task event at 3.
	_, tail2, err := store.GetEventsForArc(ctx, ref("arc-2"), common.ReadOptions{UseSnapshots: true})
	if err != nil {
		t.Fatalf("GetEventsForArc arc-2: %v", err)
	}
	if got, want := seqsOf(tail2), []int{3, 4}; !reflect.DeepEqual(got, want) {
		t.Fatalf("arc-2 tail seqs %v, want %v", got, want)
	}
}

// Rotating the log moves the snapshot aside with it. Left behind, the stale
// snapshot would claim a seq range the new, empty log never reaches, and a
// snapshot-backed read of the first arc written after the rotate would report
// "no arc" — which is what /clear followed by a message looked like.
func TestRotateMovesTheSnapshotAside(t *testing.T) {
	store := openTemp(t)
	ctx := context.Background()
	log := writeSnapshotLog(t, store)
	if err := store.SaveSnapshot(ctx, "session", foldThrough(t, log, 4)); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	archived, err := store.Rotate()
	if err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	if archived == "" {
		t.Fatal("Rotate archived nothing, want the old log's path")
	}
	if _, err := os.Stat(archived + ".snapshot"); err != nil {
		t.Errorf("the snapshot did not move with the log: %v", err)
	}

	// The first arc of the fresh log is found by a snapshot-backed read.
	if _, err := store.Persist(ctx, userMessage("arc-3", "a fresh question")); err != nil {
		t.Fatalf("Persist after rotate: %v", err)
	}
	snap, tail, err := store.GetEventsForArc(ctx, ref("arc-3"), common.ReadOptions{UseSnapshots: true})
	if err != nil {
		t.Fatalf("GetEventsForArc after rotate: %v", err)
	}
	if snap != nil {
		t.Errorf("the fresh log is served through a snapshot of %v, want none", snap.Seq())
	}
	if got, want := seqsOf(tail), []int{0}; !reflect.DeepEqual(got, want) {
		t.Errorf("arc-3 tail seqs %v, want %v (just its opening)", got, want)
	}
}
