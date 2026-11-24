package harness

import (
	"strings"
	"testing"

	"fillbrunn.ai/kube-agent/proto/pkg/history"
	"fillbrunn.ai/kube-agent/proto/pkg/events"
)

func arcEvent(id string, typ events.EventType, payload events.EventPayload) *events.Event {
	arc := "arc-1"
	return &events.Event{ID: id, Typ: typ, CorrelationID: &arc, Payload: payload}
}

// Tokens are what the reply looks like while it arrives; the recorded message
// is what it was. The second settles the first.
func TestRecordedReplySettlesTheStreamedOne(t *testing.T) {
	shown := newTranscript()
	shown.token("look")
	shown.token("ing")
	if got := shown.blocks[0].text; got != "looking" {
		t.Fatalf("streamed text = %q", got)
	}

	shown.event(arcEvent("msg-1", events.AssistantMessageEventType,
		events.AssistantMessagePayload{ModelID: "gpt-5", Content: "looking at kube-system"}))

	if len(shown.blocks) != 1 {
		t.Fatalf("transcript holds %d blocks, want the one reply", len(shown.blocks))
	}
	if shown.blocks[0].text != "looking at kube-system" {
		t.Errorf("reply = %q, want what was recorded", shown.blocks[0].text)
	}
	if shown.streaming != nil {
		t.Error("the finished reply is still being streamed into")
	}
}

// A reply that only asked for tools said nothing, and leaves no empty block
// sitting in the transcript.
func TestAReplyWithNoTextLeavesNoBlock(t *testing.T) {
	shown := newTranscript()
	shown.event(arcEvent("msg-1", events.AssistantMessageEventType,
		events.AssistantMessagePayload{ModelID: "gpt-5"}))

	if len(shown.blocks) != 0 {
		t.Errorf("transcript holds %+v, want nothing", shown.blocks)
	}
}

// A call moves through queued, running and done, and each step has to find the
// line the call created.
func TestToolCallTracksItsOwnLine(t *testing.T) {
	shown := newTranscript()
	shown.event(arcEvent("tc-1", events.ToolCallEventType, events.ToolCallPayload{
		MessageID: "msg-1", VendorID: "call_abc", ToolName: "list_pods",
		Args: map[string]any{"namespace": "kube-system"},
	}))

	called := shown.blocks[0]
	if called.state != toolQueued || called.args != `{"namespace":"kube-system"}` {
		t.Fatalf("call block = %+v, want it queued with its arguments", called)
	}

	shown.event(arcEvent("td-1", events.ToolDispatchedEventType, events.ToolDispatchedPayload{CausationID: "tc-1"}))
	if called.state != toolRunning {
		t.Errorf("a dispatched call is %s, want running", called.state)
	}

	shown.event(arcEvent("tr-1", events.ToolResultEventType, events.ToolResultPayload{
		CausationID: "tc-1", Result: map[string]any{"pods": 2},
	}))
	if called.state != toolDone || called.result != `{"pods":2}` {
		t.Errorf("a settled call is %+v, want it done with its result", called)
	}
	if len(shown.blocks) != 1 {
		t.Errorf("the call grew to %d blocks", len(shown.blocks))
	}
}

func TestAFailedResultShowsAsFailed(t *testing.T) {
	shown := newTranscript()
	shown.event(arcEvent("tc-1", events.ToolCallEventType,
		events.ToolCallPayload{MessageID: "msg-1", ToolName: "restart_deployment"}))
	shown.event(arcEvent("tr-1", events.ToolResultEventType,
		events.ToolResultPayload{CausationID: "tc-1", Result: "no such deployment", IsError: true}))

	if shown.blocks[0].state != toolFailed {
		t.Errorf("call is %s, want failed", shown.blocks[0].state)
	}
}

// The three compaction records reach the screen: the request and the
// suspension as notes, the result as the summary block.
func TestCompactionEventsReachTheTranscript(t *testing.T) {
	shown := newTranscript()
	shown.event(arcEvent("req-1", events.CompactionRequestedEventType,
		events.CompactionRequestedPayload{Watermark: 40}))
	shown.event(arcEvent("sus-1", events.ArcSuspendedEventType,
		events.ArcSuspendedPayload{CausationID: "req-1"}))
	shown.event(&events.Event{ID: "c1", Typ: events.CompactedEventType,
		Payload: events.CompactedPayload{ThroughSeq: 40, Method: "prune", Summary: "earlier conversation pruned"}})

	if len(shown.blocks) != 3 {
		t.Fatalf("transcript holds %d blocks, want request, suspension and summary", len(shown.blocks))
	}
	if shown.blocks[0].kind != blockNote || shown.blocks[1].kind != blockNote {
		t.Errorf("request/suspension blocks are %v/%v, want notes", shown.blocks[0].kind, shown.blocks[1].kind)
	}
	summary := shown.blocks[2]
	if summary.kind != blockSummary ||
		!strings.Contains(summary.text, "seq 40") ||
		!strings.Contains(summary.text, "earlier conversation pruned") {
		t.Errorf("summary block = %+v", summary)
	}
}

// A recorded session with a compaction in force opens with the summary block
// ahead of its arcs.
func TestFromHistoryOpensWithTheSummaryBlock(t *testing.T) {
	arc1, arc2 := "arc-1", "arc-2"
	log := []*events.Event{
		{SessionID: "session", ID: "u1", Seq: 1, Typ: events.UserMessageEventType,
			CorrelationID: &arc1, Payload: events.UserMessagePayload{Content: "first question"}},
		{SessionID: "session", ID: "c1", Seq: 2, Typ: events.CompactedEventType,
			Payload: events.CompactedPayload{ThroughSeq: 1, Method: "prune", Summary: "earlier conversation pruned"}},
		{SessionID: "session", ID: "u2", Seq: 3, Typ: events.UserMessageEventType,
			CorrelationID: &arc2, Payload: events.UserMessagePayload{Content: "second question"}},
	}
	h, err := history.Replay(log)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}

	shown := newTranscript()
	shown.fromHistory(h)

	if len(shown.blocks) < 2 {
		t.Fatalf("transcript holds %d blocks, want the summary plus the arcs", len(shown.blocks))
	}
	head := shown.blocks[0]
	if head.kind != blockSummary || !strings.Contains(head.text, "earlier conversation pruned") {
		t.Errorf("head block = %+v, want the summary block", head)
	}
	// The log is never rewritten, so the screen still shows every arc below
	// the watermark; the summary sits ahead of them, not instead of them.
	if shown.blocks[1].kind != blockUser || shown.blocks[1].text != "first question" {
		t.Errorf("block after the summary = %+v, want the first arc's opening", shown.blocks[1])
	}
	if shown.blocks[2].kind != blockUser || shown.blocks[2].text != "second question" {
		t.Errorf("third block = %+v, want the second arc's opening", shown.blocks[2])
	}
}
