package agent

import (
	"context"
	"testing"
	"time"

	"fillbrunn.ai/kube-agent/proto/pkg/events"
	"fillbrunn.ai/kube-agent/proto/pkg/history"
	"fillbrunn.ai/kube-agent/proto/pkg/users"
)

// An arc with a pending arc-wide interrupt is worked by no run: Run replays
// it, reads the INTERRUPTED status, and returns without asking the model. The
// answer in the log is what lets the next run proceed.
func TestRunWaitsOutAnArcWideInterrupt(t *testing.T) {
	llm := &scriptedLLM{replies: []*LLMResponse{{ModelID: "gpt-5", Content: "deployed to production"}}}
	persistor := &fakePersistor{next: 3}
	agent, err := NewAgent(&AgentConfig{}, NewToolRegistry(), llm)
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}

	arc := testArcID
	actor := users.Actor{UserID: "alice", TenantID: "acme"}
	base := []*events.Event{
		{SessionID: testSessionID, ID: arc, Seq: 0, Timestamp: time.Unix(0, 0), Typ: events.UserMessageEventType, CorrelationID: &arc, Actor: actor, Payload: events.UserMessagePayload{Content: "deploy it", Actor: actor}},
		{SessionID: testSessionID, ID: "int-1", Seq: 1, Timestamp: time.Unix(1, 0), Typ: events.InterruptEventType, CorrelationID: &arc, Actor: actor, Payload: events.InterruptPayload{Reason: "which cluster?"}},
	}

	h, err := history.Replay(base)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if h.Arcs[0].Status() != history.ArcStatusInterrupted {
		t.Fatalf("arc status = %s, want INTERRUPTED before the run", h.Arcs[0].Status())
	}
	if err := agent.Run(context.Background(), RunOptions{Persistor: persistor, History: h, ArcID: arc}); err != nil {
		t.Fatalf("Run on an interrupted arc: %v", err)
	}
	if llm.calls != 0 {
		t.Errorf("the model was asked %d times while the arc was interrupted, want 0: no run waits on a human", llm.calls)
	}
	if len(persistor.persisted) != 0 {
		t.Errorf("the interrupted run wrote %d events, want none", len(persistor.persisted))
	}

	// With the answer in the log the same run works the arc to its close.
	answered := append(append([]*events.Event{}, base...),
		&events.Event{SessionID: testSessionID, ID: "res-1", Seq: 2, Timestamp: time.Unix(2, 0), Typ: events.InterruptResponseEventType, CorrelationID: &arc, Actor: actor, Payload: events.InterruptResponsePayload{CausationID: "int-1", Response: "production", Actor: actor}},
	)
	h2, err := history.Replay(answered)
	if err != nil {
		t.Fatalf("replay with the answer: %v", err)
	}
	if h2.Arcs[0].Status() != history.ArcStatusPending {
		t.Fatalf("arc status = %s, want PENDING once the interrupt is answered", h2.Arcs[0].Status())
	}
	if err := agent.Run(context.Background(), RunOptions{Persistor: persistor, History: h2, ArcID: arc}); err != nil {
		t.Fatalf("Run after the answer: %v", err)
	}
	if llm.calls != 1 {
		t.Errorf("the model was asked %d times after the answer, want 1", llm.calls)
	}
	if h2.Arcs[0].Status() != history.ArcStatusDone {
		t.Errorf("arc status after the resumed run = %s, want DONE", h2.Arcs[0].Status())
	}
}
