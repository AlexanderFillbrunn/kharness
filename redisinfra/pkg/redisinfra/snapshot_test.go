package redisinfra

import (
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

// snapshotLog writes a session whose first arc started a task, then opened a
// second arc, then took a COMPACTED and a late task event, and returns the
// stored events in write order.
func snapshotLog(t *testing.T, store *RedisEventStore) []*events.Event {
	t.Helper()
	ctx := context.Background()
	arc1, arc2, taskID := "arc-1", "arc-2", "task-1"
	actor := users.Actor{UserID: "dev", TenantID: "dev"}
	invoke := &events.Event{
		SessionID: "session", ID: taskID, Timestamp: time.Now().UTC(),
		Typ: events.AsyncTaskInvokeEventType, CorrelationID: &arc1, Actor: actor,
		Payload: events.AsyncTaskInvokePayload{TaskName: "auditor", Actor: actor},
	}
	noise := func(id string) *events.Event {
		return &events.Event{
			SessionID: "session", ID: id, Timestamp: time.Now().UTC(),
			Typ: events.AsyncTaskEventEventType, CorrelationID: &taskID,
			Payload: events.AsyncTaskEventPayload{
				CausationID: taskID, Class: "detail", Payload: "noise",
				LLMVisibility: events.LLMVisibilityNone},
		}
	}
	compacted := &events.Event{
		SessionID: "session", ID: "compacted-1", Timestamp: time.Now().UTC(),
		Typ: events.CompactedEventType, // no correlation ID: a fact about the session
		Payload: events.CompactedPayload{ThroughSeq: 1, Summary: "summary", Method: "prune"},
	}

	var stored []*events.Event
	for _, ev := range []*events.Event{
		userMessage("session", arc1, "audit the cluster"),       // seq 0
		assistantMessage("session", arc1, "starting the audit"),  // seq 1
		invoke,                                                   // seq 2
		noise("n-1"),                                             // seq 3, correlated to the task
		userMessage("session", arc2, "what happened?"),           // seq 4
		compacted,                                                // seq 5
		noise("n-2"),                                             // seq 6, correlated to the task
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

// A saved snapshot is returned with only the events above its seq, and
// folding the two yields the same history the full read yields. Asking for
// no snapshots returns the whole log and no snapshot, even with one stored.
func TestStoreReadsTheSessionThroughASnapshot(t *testing.T) {
	store := openStore(t)
	ctx := context.Background()
	log := snapshotLog(t, store)

	if err := store.SaveSnapshot(ctx, "session", foldThrough(t, log, 3)); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	snap, tail, err := store.GetEventsForSession(ctx, "session", common.ReadOptions{UseSnapshots: true})
	if err != nil {
		t.Fatalf("GetEventsForSession: %v", err)
	}
	if snap == nil || snap.Seq() != 3 {
		t.Fatalf("snapshot seq %v, want 3", snap)
	}
	if got, want := seqsOf(tail), []int{4, 5, 6}; !reflect.DeepEqual(got, want) {
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

	// The caller that does not ask for snapshots gets the old read, even with
	// one stored.
	snap2, all, err := store.GetEventsForSession(ctx, "session", common.ReadOptions{})
	if err != nil {
		t.Fatalf("GetEventsForSession: %v", err)
	}
	if snap2 != nil || len(all) != 7 {
		t.Fatalf("opted-out read returned snapshot %v and %d events, want nil and 7", snap2, len(all))
	}
}

// The store keeps the snapshot with the highest seq and refuses one beyond
// the log, so a snapshot never claims more than the log holds and a stale
// writer cannot regress it.
func TestStoreSaveSnapshotKeepsTheNewestAndRefusesBeyondTheLog(t *testing.T) {
	store := openStore(t)
	ctx := context.Background()
	log := snapshotLog(t, store)

	if err := store.SaveSnapshot(ctx, "session", foldThrough(t, log, 1)); err != nil {
		t.Fatalf("SaveSnapshot at 1: %v", err)
	}
	if err := store.SaveSnapshot(ctx, "session", foldThrough(t, log, 3)); err != nil {
		t.Fatalf("SaveSnapshot at 3: %v", err)
	}
	snap, _, err := store.GetEventsForSession(ctx, "session", common.ReadOptions{UseSnapshots: true})
	if err != nil {
		t.Fatalf("GetEventsForSession: %v", err)
	}
	if snap.Seq() != 3 {
		t.Fatalf("stored snapshot seq %d, want the newest 3", snap.Seq())
	}

	// An older save is a no-op, not an error: the newer snapshot is already
	// there.
	if err := store.SaveSnapshot(ctx, "session", foldThrough(t, log, 1)); err != nil {
		t.Fatalf("SaveSnapshot at 1 again: %v", err)
	}
	snap, _, err = store.GetEventsForSession(ctx, "session", common.ReadOptions{UseSnapshots: true})
	if err != nil {
		t.Fatalf("GetEventsForSession: %v", err)
	}
	if snap.Seq() != 3 {
		t.Fatalf("stored snapshot seq %d after the older save, want 3", snap.Seq())
	}

	// A save claiming a seq beyond the log is refused.
	bogus := history.NewHistory()
	bogus.LastSeq = 99
	if err := store.SaveSnapshot(ctx, "session", bogus); err == nil {
		t.Fatal("a snapshot beyond the log was accepted")
	}
}

// The arc read through a snapshot replaces the prefix up to the snapshot's
// seq with the snapshot itself: the tail keeps the cutoff rule — the second
// arc's events stay out of the first arc's read, while the session-level
// COMPACTED and the task-correlated event whose task is in the snapshot stay
// in.
func TestStoreReadsAnArcThroughASnapshot(t *testing.T) {
	store := openStore(t)
	ctx := context.Background()
	log := snapshotLog(t, store)

	if err := store.SaveSnapshot(ctx, "session", foldThrough(t, log, 3)); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	ref := common.ArcRef{SessionID: "session", ArcID: "arc-1"}
	snap, tail, err := store.GetEventsForArc(ctx, ref, common.ReadOptions{UseSnapshots: true})
	if err != nil {
		t.Fatalf("GetEventsForArc: %v", err)
	}
	if snap == nil || snap.Seq() != 3 {
		t.Fatalf("snapshot seq %v, want 3", snap)
	}
	if got, want := seqsOf(tail), []int{5, 6}; !reflect.DeepEqual(got, want) {
		t.Fatalf("arc tail seqs %v, want %v", got, want)
	}

	// Folding the snapshot and the tail gives the same arc history the full
	// read gives.
	resumed, err := common.FoldEvents(snap, tail)
	if err != nil {
		t.Fatalf("FoldEvents: %v", err)
	}
	_, whole, err := store.GetEventsForArc(ctx, ref, common.ReadOptions{})
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

	// An arc opened after the snapshot is found in the tail.
	_, tail2, err := store.GetEventsForArc(ctx, common.ArcRef{SessionID: "session", ArcID: "arc-2"}, common.ReadOptions{UseSnapshots: true})
	if err != nil {
		t.Fatalf("GetEventsForArc arc-2: %v", err)
	}
	if got, want := seqsOf(tail2), []int{4, 5, 6}; !reflect.DeepEqual(got, want) {
		t.Fatalf("arc-2 tail seqs %v, want %v", got, want)
	}

	// An arc nowhere in the log or the snapshot is still refused.
	if _, _, err := store.GetEventsForArc(ctx, common.ArcRef{SessionID: "session", ArcID: "absent"}, common.ReadOptions{UseSnapshots: true}); err == nil {
		t.Fatal("an unknown arc was accepted through a snapshot")
	}
}
