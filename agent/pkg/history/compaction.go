package history

import (
	"errors"
	"fmt"

	"fillbrunn.ai/kube-agent/proto/pkg/events"
	"fillbrunn.ai/kube-agent/proto/pkg/users"
)

// This file holds the fold's half of compaction: the three records
// (COMPACTION_REQUESTED, ARC_SUSPENDED, COMPACTED) as the fold holds them, the
// predicates the agent loop consults (CompactionDue, the suspension states),
// and the watermark rule that decides which compaction is in force. The fold
// itself reads the whole log, exactly as before — compaction changes what the
// model is told, never what the fold loads, so a reference to a record below
// the watermark still resolves here.

// Compacted is one COMPACTED record as the fold holds it.
type Compacted struct {
	ID         string
	Seq        int
	ThroughSeq int
	Summary    string
	Method     string
	Modes      events.ProjectionModes
	ModelID    string
	Usage      *events.TokenUsage
}

// CompactionRequest is one COMPACTION_REQUESTED record as the fold holds it.
type CompactionRequest struct {
	ID        string
	Seq       int
	Watermark int
	Actor     *users.Actor
}

// ArcSuspension is one ARC_SUSPENDED record against an arc. Its state is
// derived from the log, never stored: outstanding until a covering COMPACTED
// lands, released from then until the arc is worked again, resolved once a
// response follows it.
type ArcSuspension struct {
	ID        string
	Seq       int
	RequestID string
}

// SuspensionState is the derived lifecycle state of one arc suspension.
type SuspensionState string

const (
	// SuspensionOutstanding is a suspension no covering COMPACTED has landed
	// for yet: the arc is waiting on the compaction its request asked for.
	SuspensionOutstanding SuspensionState = "OUTSTANDING"
	// SuspensionReleased is a suspension a covering COMPACTED has landed for,
	// whose arc has not been worked since: the completion scan requeues it.
	SuspensionReleased SuspensionState = "RELEASED"
	// SuspensionResolved is a suspension a response has followed: the arc was
	// resumed and answered, so the wait is over.
	SuspensionResolved SuspensionState = "RESOLVED"
)

// applyCompactionRequested records the want. The event correlates to the arc
// it was produced in, when it was produced in one, so that arc's cutoff picks
// it up; a request written outside any run carries no correlation ID and is
// a fact about the session alone.
func (h *History) applyCompactionRequested(ev *events.Event, state *FoldState) error {
	pl, ok := ev.Payload.(events.CompactionRequestedPayload)
	if !ok {
		return errors.New("expected payload type to be CompactionRequestedPayload")
	}
	if ev.CorrelationID != nil {
		if _, ok := state.arcs[*ev.CorrelationID]; !ok {
			return fmt.Errorf("compaction request %q correlates to no arc %s", ev.ID, *ev.CorrelationID)
		}
	}
	req := &CompactionRequest{ID: ev.ID, Seq: ev.Seq, Watermark: pl.Watermark}
	if pl.Actor != nil {
		actor := *pl.Actor
		req.Actor = &actor
	}
	h.CompactionRequests = append(h.CompactionRequests, req)
	state.compactionRequests[req.ID] = req
	return nil
}

// applyArcSuspended pauses an arc until the compaction its causation names has
// landed. The suspension names the request, never a run: one request in the
// log serves every suspension this way.
func (h *History) applyArcSuspended(ev *events.Event, state *FoldState) error {
	pl, ok := ev.Payload.(events.ArcSuspendedPayload)
	if !ok {
		return errors.New("expected payload type to be ArcSuspendedPayload")
	}
	if ev.CorrelationID == nil {
		return fmt.Errorf("event %q of type %q has no correlation ID", ev.ID, ev.Typ)
	}
	arc, ok := state.arcs[*ev.CorrelationID]
	if !ok {
		return fmt.Errorf("no arc with ID=%s found for arc suspension", *ev.CorrelationID)
	}
	if _, ok := state.compactionRequests[pl.CausationID]; !ok {
		return fmt.Errorf("arc suspension %q names no compaction request %s", ev.ID, pl.CausationID)
	}
	arc.Suspensions = append(arc.Suspensions, &ArcSuspension{ID: ev.ID, Seq: ev.Seq, RequestID: pl.CausationID})
	return nil
}

// applyCompacted records the result. The event carries no correlation ID —
// the cutoff rule gives it its scope — and it becomes the compaction in force
// only when its watermark is greater than the one held, never merely because
// it arrived later: coverage, not recency, decides.
func (h *History) applyCompacted(ev *events.Event, state *FoldState) error {
	pl, ok := ev.Payload.(events.CompactedPayload)
	if !ok {
		return errors.New("expected payload type to be CompactedPayload")
	}
	if ev.CorrelationID != nil {
		return fmt.Errorf("compacted event %q carries correlation ID %s; a COMPACTED carries none", ev.ID, *ev.CorrelationID)
	}
	compacted := &Compacted{
		ID:         ev.ID,
		Seq:        ev.Seq,
		ThroughSeq: pl.ThroughSeq,
		Summary:    pl.Summary,
		Method:     pl.Method,
		Modes:      pl.Modes,
		ModelID:    pl.ModelID,
		Usage:      pl.Usage,
	}
	if h.LastCompaction == nil || compacted.ThroughSeq > h.LastCompaction.ThroughSeq {
		h.LastCompaction = compacted
	}
	return nil
}

// Wire carries the modes as the events package holds them, so a COMPACTED
// payload can record the settings its summary was made under. The proto
// package cannot name this package's types, so the conversion lives here,
// on the side that can see both.
func (m Modes) Wire() events.ProjectionModes {
	return events.ProjectionModes{
		ForeignArc: m.ForeignArc.String(),
		Pending:    m.Pending.String(),
		ToolEvents: m.ToolEvents.String(),
		Error:      m.Error.String(),
		Interrupt:  m.Interrupt.String(),
		QuoteLimit: m.QuoteLimit,
	}
}

// ModesFromWire reads the projection settings a COMPACTED payload recorded
// back into modes, so a reader can tell what a summary was made under.
func ModesFromWire(w events.ProjectionModes) (Modes, error) {
	modes := Modes{QuoteLimit: w.QuoteLimit}
	var err error
	parse := func(name, value string) (Treatment, error) {
		switch value {
		case "":
			return 0, nil
		case "elide":
			return TreatmentElide, nil
		case "note":
			return TreatmentNote, nil
		case "stand_in":
			return TreatmentStandIn, nil
		case "keep":
			return TreatmentKeep, nil
		}
		return 0, fmt.Errorf("mode %s is %q, not a treatment", name, value)
	}
	if modes.ForeignArc, err = parse("foreign_arc", w.ForeignArc); err != nil {
		return Modes{}, err
	}
	if modes.Pending, err = parse("pending", w.Pending); err != nil {
		return Modes{}, err
	}
	if modes.ToolEvents, err = parse("tool_events", w.ToolEvents); err != nil {
		return Modes{}, err
	}
	if modes.Error, err = parse("error", w.Error); err != nil {
		return Modes{}, err
	}
	if modes.Interrupt, err = parse("interrupt", w.Interrupt); err != nil {
		return Modes{}, err
	}
	if err := modes.Validate(); err != nil {
		return Modes{}, err
	}
	return modes, nil
}

// Watermark returns the watermark of the compaction in force: the greatest
// ThroughSeq among the COMPACTED records, or -1 where the session has never
// compacted.
func (h *History) Watermark() int {
	if h.LastCompaction == nil {
		return -1
	}
	return h.LastCompaction.ThroughSeq
}

// CompactionRequestByID returns the compaction request an ID names, or nil.
func (h *History) CompactionRequestByID(id string) *CompactionRequest {
	for _, req := range h.CompactionRequests {
		if req.ID == id {
			return req
		}
	}
	return nil
}

// CompactionRequestSatisfied reports whether a COMPACTED covering the request's
// watermark has landed. Coverage by watermark, not arrival, so an
// out-of-order or duplicated compaction satisfies it just the same.
func (h *History) CompactionRequestSatisfied(req *CompactionRequest) bool {
	return h.LastCompaction != nil && h.LastCompaction.ThroughSeq >= req.Watermark
}

// UnsatisfiedRequest returns the oldest COMPACTION_REQUESTED no covering
// COMPACTED has landed for, or nil when every request stands satisfied.
func (h *History) UnsatisfiedRequest() *CompactionRequest {
	for _, req := range h.CompactionRequests {
		if !h.CompactionRequestSatisfied(req) {
			return req
		}
	}
	return nil
}

// SuspensionStateOf derives the state of one suspension from the log: resolved
// once a response follows it, released once a COMPACTED covering its request
// has landed, outstanding until then.
func (h *History) SuspensionStateOf(arc *Arc, s *ArcSuspension) SuspensionState {
	for _, response := range arc.Responses {
		if response.Seq > s.Seq {
			return SuspensionResolved
		}
	}
	if req := h.CompactionRequestByID(s.RequestID); req != nil && h.CompactionRequestSatisfied(req) {
		return SuspensionReleased
	}
	return SuspensionOutstanding
}

// ArcSuspended reports whether the arc has a suspension that is not resolved —
// one the arc is still waiting out, whether or not its compaction has landed.
func (h *History) ArcSuspended(arc *Arc) bool {
	for _, s := range arc.Suspensions {
		if h.SuspensionStateOf(arc, s) != SuspensionResolved {
			return true
		}
	}
	return false
}

// LastReply returns the last assistant message in the log by sequence, or nil
// where the session holds none.
func (h *History) LastReply() *AssistantMessage {
	var last *AssistantMessage
	for _, arc := range h.Arcs {
		for _, response := range arc.Responses {
			if last == nil || response.Seq > last.Seq {
				last = response
			}
		}
	}
	return last
}

// CompactionDue reports whether the log calls for a compaction at the given
// lower budget. It is due when the last reply's input tokens exceed the
// budget — the reported usage where the provider gave one, the transcript
// estimate where it did not, nil never counting as "not due" — that reply
// sits above the watermark of the compaction in force, and no
// COMPACTION_REQUESTED stands unsatisfied.
func (h *History) CompactionDue(budget int) bool {
	if budget <= 0 {
		return false
	}
	last := h.LastReply()
	if last == nil {
		return false
	}
	if last.Seq <= h.Watermark() {
		return false
	}
	if h.UnsatisfiedRequest() != nil {
		return false
	}
	input := h.EstimateTranscript()
	if last.Usage != nil {
		input = last.Usage.Input
	}
	return input > budget
}
