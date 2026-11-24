package common

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"fillbrunn.ai/kube-agent/proto/pkg/events"
	"fillbrunn.ai/kube-agent/proto/pkg/history"
	"fillbrunn.ai/kube-agent/proto/pkg/users"
)

// snapshotLog builds a small session log as a store would hold it, seq equal
// to index: an arc with a task, a second arc, a compaction record and a late
// task event.
func snapshotLog() []*events.Event {
	at := time.Unix(0, 0)
	actor := users.Actor{UserID: "alice", TenantID: "acme"}
	arc1, arc2, task := testArcID, "arc-2", "task-1"
	ev := func(seq int, id string, corr *string, typ events.EventType, payload events.EventPayload) *events.Event {
		return &events.Event{
			SessionID: testSessionID, ID: id, Seq: seq, Timestamp: at,
			Typ: typ, CorrelationID: corr, Actor: actor, Payload: payload,
		}
	}
	return []*events.Event{
		ev(0, "ev-0", &arc1, events.UserMessageEventType, events.UserMessagePayload{Content: "first", Actor: actor}),
		ev(1, "ev-1", &arc1, events.AssistantMessageEventType, events.AssistantMessagePayload{ModelID: "m", Content: "answer"}),
		ev(2, task, &arc1, events.AsyncTaskInvokeEventType, events.AsyncTaskInvokePayload{TaskName: "audit", Actor: actor}),
		ev(3, "ev-3", &task, events.AsyncTaskEventEventType, events.AsyncTaskEventPayload{CausationID: task, LLMVisibility: events.LLMVisibilityNone}),
		ev(4, "ev-4", &arc2, events.UserMessageEventType, events.UserMessagePayload{Content: "second", Actor: actor}),
		ev(5, "ev-5", nil, events.CompactedEventType, events.CompactedPayload{ThroughSeq: 1, Summary: "summary", Method: "prune"}),
		ev(6, "ev-6", &task, events.AsyncTaskEventEventType, events.AsyncTaskEventPayload{CausationID: task, LLMVisibility: events.LLMVisibilityNone}),
	}
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

// Without a snapshot, FoldEvents is an ordinary full replay.
func TestFoldEventsWithoutSnapshotReplaysTheWholeLog(t *testing.T) {
	h, err := FoldEvents(nil, snapshotLog())
	if err != nil {
		t.Fatalf("FoldEvents: %v", err)
	}
	if h.LastSeq != 6 || len(h.Arcs) != 2 || len(h.Tasks) != 1 {
		t.Fatalf("folded last seq %d, %d arcs, %d tasks", h.LastSeq, len(h.Arcs), len(h.Tasks))
	}
}

// With a snapshot, FoldEvents resumes from the snapshot's fold and the
// result is the same history the full replay produces.
func TestFoldEventsResumesFromTheSnapshot(t *testing.T) {
	log := snapshotLog()
	prefix := append([]*events.Event(nil), log[:4]...)
	fold, err := history.Replay(prefix)
	if err != nil {
		t.Fatalf("replay prefix: %v", err)
	}
	snap := &Snapshot{SessionID: testSessionID, SavedAt: time.Unix(100, 0), Fold: fold}

	resumed, err := FoldEvents(snap, log[4:])
	if err != nil {
		t.Fatalf("FoldEvents: %v", err)
	}
	full, err := FoldEvents(nil, log)
	if err != nil {
		t.Fatalf("FoldEvents full: %v", err)
	}
	if !reflect.DeepEqual(normalized(t, full), normalized(t, resumed)) {
		t.Fatal("resumed fold disagrees with the full replay")
	}
}

// The arc cutoff over a tail takes the opening and the task IDs from the
// snapshot's fold where they are already in it: the second arc's opening is
// not the first arc's, and the late task event still belongs to arc-1's read
// because its task is in the snapshot.
func TestSelectArcEventsReadsOpeningAndTasksFromTheSnapshot(t *testing.T) {
	log := snapshotLog()
	prefix := append([]*events.Event(nil), log[:4]...)
	fold, err := history.Replay(prefix)
	if err != nil {
		t.Fatalf("replay prefix: %v", err)
	}
	snap := &Snapshot{SessionID: testSessionID, Fold: fold}

	selected, err := SelectArcEvents(log[4:], ArcRef{SessionID: testSessionID, ArcID: testArcID}, snap)
	if err != nil {
		t.Fatalf("SelectArcEvents: %v", err)
	}
	// arc-2's opening (4) belongs to arc-2; the COMPACTED (5) carries no
	// correlation and the task event (6) correlates to a task in the snapshot.
	if got, want := seqsOf(selected), []int{5, 6}; !reflect.DeepEqual(got, want) {
		t.Fatalf("selected seqs %v, want %v", got, want)
	}

	// The arc opened after the snapshot is found in the tail instead.
	selected, err = SelectArcEvents(log[4:], ArcRef{SessionID: testSessionID, ArcID: "arc-2"}, snap)
	if err != nil {
		t.Fatalf("SelectArcEvents: %v", err)
	}
	if got, want := seqsOf(selected), []int{4, 5, 6}; !reflect.DeepEqual(got, want) {
		t.Fatalf("selected seqs %v, want %v", got, want)
	}
}

// Without a snapshot the same rule scans the whole log, as the stores always
// have.
func TestSelectArcEventsScansTheWholeLogWithoutASnapshot(t *testing.T) {
	selected, err := SelectArcEvents(snapshotLog(), ArcRef{SessionID: testSessionID, ArcID: testArcID}, nil)
	if err != nil {
		t.Fatalf("SelectArcEvents: %v", err)
	}
	if got, want := seqsOf(selected), []int{0, 1, 2, 3, 5, 6}; !reflect.DeepEqual(got, want) {
		t.Fatalf("selected seqs %v, want %v", got, want)
	}
	if _, err := SelectArcEvents(snapshotLog(), ArcRef{SessionID: testSessionID, ArcID: "absent"}, nil); err == nil {
		t.Fatal("an unknown arc was accepted")
	}
}

func TestSnapshotInfoIsNilSafe(t *testing.T) {
	var nilSnap *Snapshot
	if got := nilSnap.Info(); got != nil {
		t.Fatalf("Info() on a nil snapshot = %+v, want nil", got)
	}
	if got := (&Snapshot{}).Info(); got != nil {
		t.Fatalf("Info() on a snapshot with no fold = %+v, want nil", got)
	}
	fold := history.NewHistory()
	fold.LastSeq = 7
	savedAt := time.Unix(50, 0).UTC()
	info := (&Snapshot{SessionID: testSessionID, SavedAt: savedAt, Fold: fold}).Info()
	if info == nil || info.Seq != 7 || !info.SavedAt.Equal(savedAt) {
		t.Fatalf("Info() = %+v, want seq 7 saved at %s", info, savedAt)
	}
}

func TestSnapshotEncodeDecodeRoundTrip(t *testing.T) {
	log := snapshotLog()
	prefix := append([]*events.Event(nil), log[:5]...)
	fold, err := history.Replay(prefix)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	savedAt := time.Unix(123, 0).UTC()
	data, err := EncodeSnapshot(&Snapshot{SessionID: testSessionID, SavedAt: savedAt, Fold: fold})
	if err != nil {
		t.Fatalf("EncodeSnapshot: %v", err)
	}
	snap, err := DecodeSnapshot(data, testSessionID)
	if err != nil {
		t.Fatalf("DecodeSnapshot: %v", err)
	}
	if snap.Seq() != 4 || snap.SessionID != testSessionID || !snap.SavedAt.Equal(savedAt) {
		t.Fatalf("decoded snapshot seq %d session %q saved at %s", snap.Seq(), snap.SessionID, snap.SavedAt)
	}
	if !reflect.DeepEqual(normalized(t, fold), normalized(t, snap.Fold)) {
		t.Fatal("decoded fold disagrees with the encoded one")
	}
}

// recordingPersistor remembers the snapshots it was asked to save.
type recordingPersistor struct {
	saved []*history.History
}

func (r *recordingPersistor) Persist(_ context.Context, evs ...*events.Event) ([]*events.Event, error) {
	return evs, nil
}

func (r *recordingPersistor) SaveSnapshot(_ context.Context, _ string, fold *history.History) error {
	r.saved = append(r.saved, fold)
	return nil
}

// nopSink swallows every batch published to it.
type nopSink struct{}

func (nopSink) Consume(context.Context, ...*events.Event) {}

// Every wrapper on the persistor chain passes a snapshot save through to the
// store, untouched: a snapshot is not an event, so there is nothing to mint,
// stamp or publish on it.
func TestPersistorChainForwardsSaveSnapshot(t *testing.T) {
	rec := &recordingPersistor{}
	chain := AsAgent("agent-1", MintIdentity(PublishEvents(rec, nopSink{})))
	fold := history.NewHistory()
	if err := chain.SaveSnapshot(context.Background(), testSessionID, fold); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	if len(rec.saved) != 1 || rec.saved[0] != fold {
		t.Fatalf("the store saw %d snapshot saves, want the one fold", len(rec.saved))
	}
}
