package history

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"fillbrunn.ai/kube-agent/proto/pkg/events"
)

// arcInterruptLog is a session log with one user arc, to which tests append
// arc-wide interrupt records.
func arcInterruptLog(arc string) []*events.Event {
	start := time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC)
	return []*events.Event{
		{ID: arc, Seq: 0, Timestamp: start, Typ: events.UserMessageEventType, CorrelationID: &arc, Actor: testActor, Payload: events.UserMessagePayload{Content: "deploy it", Actor: testActor}},
	}
}

func arcInterruptEvent(id, arc, reason string, seq int) *events.Event {
	return &events.Event{
		ID: id, Seq: seq, Timestamp: time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC).Add(time.Duration(seq) * time.Second),
		Typ: events.InterruptEventType, CorrelationID: &arc, Actor: testActor,
		Payload: events.InterruptPayload{Reason: reason},
	}
}

func arcInterruptResponseEvent(id, arc, interrupt, response string, seq int, rejected bool) *events.Event {
	return &events.Event{
		ID: id, Seq: seq, Timestamp: time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC).Add(time.Duration(seq) * time.Second),
		Typ: events.InterruptResponseEventType, CorrelationID: &arc, Actor: testActor,
		Payload: events.InterruptResponsePayload{CausationID: interrupt, Response: response, IsRejected: rejected, Actor: testActor},
	}
}

// An arc-wide interrupt — one with no causation ID — holds the whole arc: the
// fold attaches it to the arc, and the arc reads INTERRUPTED while it is
// pending.
func TestArcWideInterruptHoldsTheArc(t *testing.T) {
	arc := "arc-1"
	log := append(arcInterruptLog(arc), arcInterruptEvent("int-1", arc, "which cluster?", 1))

	h := foldEvents(t, log)
	if len(h.Arcs) != 1 {
		t.Fatalf("arcs = %d, want 1", len(h.Arcs))
	}
	got := h.Arcs[0]
	if len(got.Interrupts) != 1 || got.Interrupts[0].ID != "int-1" {
		t.Fatalf("arc interrupts = %#v, want int-1", got.Interrupts)
	}
	if got.Interrupts[0].CausationID != "" {
		t.Errorf("causation = %q, want empty for an arc-wide interrupt", got.Interrupts[0].CausationID)
	}
	if got.Interrupts[0].Status != InterruptPending {
		t.Errorf("interrupt status = %s, want pending", got.Interrupts[0].Status)
	}
	if got.Status() != ArcStatusInterrupted {
		t.Errorf("arc status = %s, want INTERRUPTED while an arc-wide interrupt is pending", got.Status())
	}

	// The index rebuilt from a folded history resolves the interrupt, so a
	// response folded into a history loaded from a snapshot lands too.
	state := FoldStateFromHistory(h)
	if _, ok := state.interrupts["int-1"]; !ok {
		t.Error("FoldStateFromHistory does not index the arc-wide interrupt")
	}
	res := arcInterruptResponseEvent("res-1", arc, "int-1", "production", 2, false)
	if err := h.Update(res, state); err != nil {
		t.Fatalf("Update with the response: %v", err)
	}
	if got.Interrupts[0].Status != InterruptAnswered {
		t.Errorf("interrupt status after the answer = %s, want answered", got.Interrupts[0].Status)
	}
	if got.Status() != ArcStatusPending {
		t.Errorf("arc status after the answer = %s, want PENDING again", got.Status())
	}
}

// The answer to an arc-wide interrupt is information: it settles the interrupt
// and nothing else, and carries the responder and the response.
func TestArcWideInterruptAnswerCarriesTheResponse(t *testing.T) {
	arc := "arc-1"
	log := append(arcInterruptLog(arc),
		arcInterruptEvent("int-1", arc, "which cluster?", 1),
		arcInterruptResponseEvent("res-1", arc, "int-1", "production", 2, false),
	)
	h := foldEvents(t, log)
	interrupt := h.Arcs[0].Interrupts[0]
	if interrupt.Status != InterruptAnswered || interrupt.Response != "production" {
		t.Errorf("interrupt = %#v, want answered with response production", interrupt)
	}
	if interrupt.Responder == nil || interrupt.Responder.UserID != testActor.UserID {
		t.Errorf("responder = %#v, want the answering actor", interrupt.Responder)
	}
	if interrupt.ResolvedSeq == nil || *interrupt.ResolvedSeq != 2 {
		t.Errorf("resolved seq = %v, want 2", interrupt.ResolvedSeq)
	}
}

// A rejection is a verdict on a call. An arc-wide interrupt names no call, so
// the fold refuses a rejected response to one rather than settling nothing.
func TestArcWideInterruptRefusesARejection(t *testing.T) {
	arc := "arc-1"
	log := append(arcInterruptLog(arc),
		arcInterruptEvent("int-1", arc, "which cluster?", 1),
		arcInterruptResponseEvent("res-1", arc, "int-1", "no", 2, true),
	)
	if _, err := Replay(log); err == nil {
		t.Fatal("replay accepted a rejected response to an arc-wide interrupt")
	} else if !strings.Contains(err.Error(), "holds no call") {
		t.Fatalf("replay error = %v, want it to say the interrupt holds no call", err)
	}
}

// A decision on an interrupt that is no longer pending is dropped, so a
// second answer changes nothing.
func TestArcWideInterruptDropsADuplicateAnswer(t *testing.T) {
	arc := "arc-1"
	log := append(arcInterruptLog(arc),
		arcInterruptEvent("int-1", arc, "which cluster?", 1),
		arcInterruptResponseEvent("res-1", arc, "int-1", "production", 2, false),
		arcInterruptResponseEvent("res-2", arc, "int-1", "staging", 3, false),
	)
	h := foldEvents(t, log)
	interrupt := h.Arcs[0].Interrupts[0]
	if interrupt.Response != "production" || interrupt.ResolvedSeq == nil || *interrupt.ResolvedSeq != 2 {
		t.Errorf("interrupt after the duplicate = %#v, want the first answer to stick", interrupt)
	}
}

// The invalid shapes an interrupt can arrive in are all refused by the fold:
// no correlation, an unknown arc, an arc that is already closed or cancelled,
// and a response naming no interrupt.
func TestArcWideInterruptRefusesInvalidShapes(t *testing.T) {
	arc := "arc-1"
	base := arcInterruptLog(arc)

	cases := []struct {
		name string
		log  []*events.Event
		want string
	}{
		{
			name: "no correlation",
			log: []*events.Event{{
				ID: "int-1", Seq: 1, Typ: events.InterruptEventType, Actor: testActor,
				Payload: events.InterruptPayload{Reason: "why"},
			}},
			want: "no correlation ID",
		},
		{
			name: "unknown arc",
			log:  append(arcInterruptLog(arc), arcInterruptEvent("int-1", "no-such-arc", "why", 1)),
			want: "no arc with ID=no-such-arc",
		},
		{
			name: "closed arc",
			log: append(append(arcInterruptLog(arc),
				&events.Event{ID: "msg-1", Seq: 1, Typ: events.AssistantMessageEventType, CorrelationID: &arc, Actor: testActor, Payload: events.AssistantMessagePayload{ModelID: "m", Content: "done"}},
				&events.Event{ID: "closed-1", Seq: 2, Typ: events.ArcClosedEventType, CorrelationID: &arc, Payload: events.ArcClosedPayload{CausationID: "msg-1"}},
			), arcInterruptEvent("int-1", arc, "why", 3)),
			want: "cannot be interrupted",
		},
		{
			name: "cancelled arc",
			log: append(append(arcInterruptLog(arc),
				&events.Event{ID: "cancel-1", Seq: 1, Typ: events.CancelEventType, CorrelationID: &arc, Actor: testActor, Payload: events.CancelPayload{}},
			), arcInterruptEvent("int-1", arc, "why", 2)),
			want: "cannot be interrupted",
		},
		{
			name: "response naming no interrupt",
			log: append(arcInterruptLog(arc),
				arcInterruptResponseEvent("res-1", arc, "no-such-interrupt", "x", 1, false)),
			want: "no interrupt with ID=no-such-interrupt",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Replay(tc.log); err == nil {
				t.Fatalf("replay accepted %s", tc.name)
			} else if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("replay error = %v, want it to contain %q", err, tc.want)
			}
			_ = base
		})
	}
}

// A cancel settles a pending arc-wide interrupt along with the arc's calls,
// so a later answer finds it no longer pending and is dropped.
func TestCancelSettlesArcWideInterrupt(t *testing.T) {
	arc := "arc-1"
	log := append(arcInterruptLog(arc),
		arcInterruptEvent("int-1", arc, "which cluster?", 1),
		&events.Event{ID: "cancel-1", Seq: 2, Typ: events.CancelEventType, CorrelationID: &arc, Actor: testActor, Payload: events.CancelPayload{}},
		arcInterruptResponseEvent("res-1", arc, "int-1", "production", 3, false),
	)
	h := foldEvents(t, log)
	interrupt := h.Arcs[0].Interrupts[0]
	if interrupt.Status != InterruptCancelled {
		t.Errorf("interrupt status after the cancel = %s, want cancelled", interrupt.Status)
	}
	if interrupt.Response != nil {
		t.Errorf("interrupt response after the cancel = %#v, want nil: the late answer was dropped", interrupt.Response)
	}
	if h.Arcs[0].Status() != ArcStatusCancelled {
		t.Errorf("arc status = %s, want CANCELLED", h.Arcs[0].Status())
	}
}

// Several arc-wide interrupts may be pending at once, one per event, and the
// arc stays INTERRUPTED until the last is answered.
func TestSeveralArcWideInterruptsGateTogether(t *testing.T) {
	arc := "arc-1"
	log := append(arcInterruptLog(arc),
		arcInterruptEvent("int-1", arc, "which cluster?", 1),
		arcInterruptEvent("int-2", arc, "which namespace?", 2),
		arcInterruptResponseEvent("res-1", arc, "int-1", "production", 3, false),
	)
	h := foldEvents(t, log)
	if h.Arcs[0].Status() != ArcStatusInterrupted {
		t.Errorf("arc status with one of two asks answered = %s, want INTERRUPTED", h.Arcs[0].Status())
	}
	if err := h.Update(arcInterruptResponseEvent("res-2", arc, "int-2", "kube-system", 4, false), FoldStateFromHistory(h)); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if h.Arcs[0].Status() != ArcStatusPending {
		t.Errorf("arc status with both asks answered = %s, want PENDING", h.Arcs[0].Status())
	}
}

// A fold snapshot stores the folded history as JSON, so the interrupt's
// causation_id has to read back, and an arc-wide interrupt reads as empty.
func TestInterruptJSONCausation(t *testing.T) {
	var renamed Interrupt
	if err := json.Unmarshal([]byte(`{"id":"int-1","seq":3,"correlation_id":"arc-1","causation_id":"call-2","reason":"approve","status":"pending"}`), &renamed); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if renamed.CausationID != "call-2" {
		t.Errorf("causation = %q, want call-2", renamed.CausationID)
	}

	var arcWide Interrupt
	if err := json.Unmarshal([]byte(`{"id":"int-1","seq":3,"correlation_id":"arc-1","reason":"which cluster?","status":"pending"}`), &arcWide); err != nil {
		t.Fatalf("unmarshal arc-wide: %v", err)
	}
	if arcWide.CausationID != "" {
		t.Errorf("causation = %q, want empty for an arc-wide interrupt", arcWide.CausationID)
	}
}

// The projection discloses an arc-wide interrupt as notes under the interrupt
// mode: the ask where it was raised, the answer where it arrived. The default
// mode elides both.
func TestProjectionShowsArcWideInterruptNotes(t *testing.T) {
	arc := "arc-1"
	log := append(arcInterruptLog(arc),
		arcInterruptEvent("int-1", arc, "which cluster?", 1),
		arcInterruptResponseEvent("res-1", arc, "int-1", "production", 2, false),
	)
	h := foldEvents(t, log)

	modes := DefaultModes()
	modes.Interrupt = TreatmentNote
	transcript, err := h.Project(arc, modes)
	if err != nil {
		t.Fatalf("Project: %v", err)
	}
	var ask, answer bool
	for _, text := range allTexts(transcript) {
		if strings.Contains(text, "held for a human") && strings.Contains(text, "which cluster?") {
			ask = true
		}
		if strings.Contains(text, "a human answered:") && strings.Contains(text, "production") {
			answer = true
		}
	}
	if !ask {
		t.Errorf("projection has no ask note for the arc-wide interrupt: %#v", transcript.Messages)
	}
	if !answer {
		t.Errorf("projection has no answer note for the arc-wide interrupt: %#v", transcript.Messages)
	}

	elided, err := h.Project(arc, DefaultModes())
	if err != nil {
		t.Fatalf("Project with default modes: %v", err)
	}
	for _, text := range allTexts(elided) {
		if strings.Contains(text, "held for a human") || strings.Contains(text, "a human answered") {
			t.Errorf("default modes disclose the interrupt: %q", text)
		}
	}
}

// A pending arc-wide interrupt projects as the ask alone, which is what tells
// the resumed run it is waiting on a human.
func TestProjectionShowsPendingArcWideInterrupt(t *testing.T) {
	arc := "arc-1"
	log := append(arcInterruptLog(arc), arcInterruptEvent("int-1", arc, "which cluster?", 1))
	h := foldEvents(t, log)

	modes := DefaultModes()
	modes.Interrupt = TreatmentNote
	transcript, err := h.Project(arc, modes)
	if err != nil {
		t.Fatalf("Project: %v", err)
	}
	found := false
	for _, text := range allTexts(transcript) {
		if strings.Contains(text, "int-1") && strings.Contains(text, "which cluster?") {
			found = true
		}
	}
	if !found {
		t.Errorf("projection of a pending arc-wide interrupt has no ask note: %#v", transcript.Messages)
	}
}
