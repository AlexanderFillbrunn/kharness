package history

import (
	"encoding/json"
	"reflect"
	"testing"

	"fillbrunn.ai/kube-agent/proto/pkg/events"
)

// snapshotLog builds a small session log: one arc with a tool call and a
// task, a second arc, and a compaction record. Seq equals index, as in a
// store's log.
func snapshotLog(t *testing.T) []*events.Event {
	t.Helper()
	l := newCompactionLogger()
	l.user(0, "arc-1", "first question")
	l.assistant(1, "arc-1", "msg-1", "first answer", nil)
	l.toolCall(2, "arc-1", "msg-1", "call-1", "restart_deployment", map[string]any{"name": "web"})
	l.toolResult(3, "arc-1", "call-1", "restarted")
	l.invoke(4, "arc-1", "task-1", "audit", map[string]any{"scope": "cluster"})
	l.add(5, "task-ev-1", "task-1", events.AsyncTaskEventEventType, events.AsyncTaskEventPayload{
		CausationID: "task-1", Class: "progress", LLMVisibility: events.LLMVisibilityNone,
		Payload: map[string]any{"step": 1},
	})
	l.user(6, "arc-2", "second question")
	l.assistant(7, "arc-2", "msg-2", "second answer", nil)
	l.compacted(8, "compacted-1", 3, "summary of the first arc")
	l.close(9, "arc-2", "msg-2")
	return l.events
}

// normalized renders a history through a JSON round trip, so two folds
// compare by content: the fold's any-typed payload fields become maps on a
// snapshot round trip, and this erases that difference — key order in a
// rendered value is not content.
func normalized(t *testing.T, h *History) any {
	t.Helper()
	data, err := json.Marshal(h)
	if err != nil {
		t.Fatalf("marshal history: %v", err)
	}
	var v any
	if err := json.Unmarshal(data, &v); err != nil {
		t.Fatalf("normalize history: %v", err)
	}
	return v
}

// Resuming from an encoded snapshot must yield the same history the full
// replay yields, at every cut point.
func TestResumeFromSnapshotEqualsFullReplay(t *testing.T) {
	log := snapshotLog(t)
	full, err := Replay(log)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}

	for cut := 0; cut < len(log); cut++ {
		prefix := append([]*events.Event(nil), log[:cut+1]...)
		snap, err := Replay(prefix)
		if err != nil {
			t.Fatalf("replay prefix through %d: %v", cut, err)
		}
		data, err := EncodeFold(snap)
		if err != nil {
			t.Fatalf("encode fold at %d: %v", cut, err)
		}
		decoded, err := DecodeFold(data)
		if err != nil {
			t.Fatalf("decode fold at %d: %v", cut, err)
		}
		if decoded.LastSeq != cut {
			t.Fatalf("decoded fold at %d has last seq %d", cut, decoded.LastSeq)
		}

		tail := append([]*events.Event(nil), log[cut+1:]...)
		resumed, err := Resume(decoded, tail)
		if err != nil {
			t.Fatalf("resume from %d: %v", cut, err)
		}
		if !reflect.DeepEqual(normalized(t, full), normalized(t, resumed)) {
			t.Fatalf("resume from seq %d disagrees with the full replay", cut)
		}
	}
}

// A tail that does not start above the snapshot's seq is refused, so a
// snapshot and a log that disagree stop the resume rather than fold a wrong
// history.
func TestResumeRefusesATailAtOrBelowTheSnapshotSeq(t *testing.T) {
	log := snapshotLog(t)
	snap, err := Replay(log[:4])
	if err != nil {
		t.Fatalf("replay prefix: %v", err)
	}
	// The tail starts at the snapshot's own seq, not above it.
	stale := log[3]
	if _, err := Resume(snap, []*events.Event{stale}); err == nil {
		t.Fatal("resume accepted a tail at the snapshot's seq")
	}
}

func TestEncodeFoldRefusesANilHistory(t *testing.T) {
	if _, err := EncodeFold(nil); err == nil {
		t.Fatal("encoding a nil history succeeded")
	}
}

func TestDecodeFoldRefusesAnInvalidLastSeq(t *testing.T) {
	if _, err := DecodeFold([]byte(`{"session_id":"s","last_seq":-5,"tasks":{}}`)); err == nil {
		t.Fatal("decoding a fold with an invalid last seq succeeded")
	}
}
