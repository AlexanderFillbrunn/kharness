package agent

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"fillbrunn.ai/kube-agent/proto/pkg/common"
	"fillbrunn.ai/kube-agent/proto/pkg/events"
	"fillbrunn.ai/kube-agent/proto/pkg/history"
	"fillbrunn.ai/kube-agent/proto/pkg/users"
)

// questionsSchema is the canonical schema a question-asking interrupt is
// configured with.
var questionsSchema = map[string]any{
	"type": "object",
	"properties": map[string]any{
		"questions": map[string]any{
			"type":     "array",
			"minItems": 1,
			"items": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"id":   map[string]any{"type": "string"},
					"text": map[string]any{"type": "string"},
					"kind": map[string]any{"type": "string", "enum": []any{"text", "single_choice", "multi_choice"}},
				},
				"required": []any{"id", "text", "kind"},
			},
		},
	},
	"required": []any{"questions"},
}

var askUserSpec = InterruptSpec{
	Name:          "ask_user",
	Description:   "Ask the user one or more questions",
	RequestSchema: questionsSchema,
}

// offeredLLM records the tool names each request offered.
type offeredLLM struct {
	*scriptedLLM
	offered [][]string
}

func (l *offeredLLM) StreamResponse(ctx context.Context, t *history.ModelTranscript, opts *LLMOptions, sink common.Sink[RawToken]) (*LLMResponse, error) {
	if opts != nil && opts.Tools != nil {
		var names []string
		for _, tool := range opts.Tools.All() {
			names = append(names, tool.Name)
		}
		l.offered = append(l.offered, names)
	}
	return l.scriptedLLM.StreamResponse(ctx, t, opts, sink)
}

// A configured interrupt is offered to the model as a tool with the request
// schema as its input schema.
func TestInterruptSpecBecomesATool(t *testing.T) {
	llm := &offeredLLM{scriptedLLM: &scriptedLLM{replies: []*LLMResponse{{ModelID: "gpt-5", Content: "hi"}}}}
	agent, err := NewAgent(&AgentConfig{}, NewToolRegistry(), llm, WithInterrupts([]InterruptSpec{askUserSpec}))
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	if err := agent.Run(context.Background(), RunOptions{Persistor: &fakePersistor{next: 1}, History: userArcHistory(t), ArcID: testArcID}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(llm.offered) != 1 {
		t.Fatalf("the model was asked %d times, want 1", len(llm.offered))
	}
	found := false
	for _, name := range llm.offered[0] {
		if name == "ask_user" {
			found = true
		}
	}
	if !found {
		t.Errorf("offered tools = %v, want ask_user among them", llm.offered[0])
	}

	tools := interruptToolSet{[]InterruptSpec{askUserSpec}}.All()
	if len(tools) != 1 || tools[0].Name != "ask_user" || tools[0].Description != askUserSpec.Description {
		t.Fatalf("tool = %+v", tools)
	}
	schema, ok := tools[0].InputSchema.(map[string]any)
	if !ok || schema["type"] != "object" {
		t.Errorf("input schema = %#v, want the request schema verbatim", tools[0].InputSchema)
	}
}

// An interrupt name collides with a registered tool and with a duplicate
// declaration, the way tool and task names do.
func TestInterruptNameCollisions(t *testing.T) {
	registry := registryWith(t, "ask_user", func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return &mcp.CallToolResult{}, nil
	})
	llm := &scriptedLLM{}
	if _, err := NewAgent(&AgentConfig{}, registry, llm, WithInterrupts([]InterruptSpec{askUserSpec})); err == nil {
		t.Error("NewAgent accepted an interrupt named like a registered tool")
	}
	if _, err := NewAgent(&AgentConfig{}, NewToolRegistry(), llm, WithInterrupts([]InterruptSpec{askUserSpec, askUserSpec})); err == nil {
		t.Error("NewAgent accepted the same interrupt twice")
	}
	if _, err := NewAgent(&AgentConfig{}, NewToolRegistry(), llm, WithInterrupts([]InterruptSpec{{Name: "", Description: "x"}})); err == nil {
		t.Error("NewAgent accepted an interrupt with no name")
	}
}

// A call to a configured interrupt raises an arc-wide interrupt and settles
// the call in the same write; the run then ends without asking the model
// again, and a run after the answer works the arc to its close.
func TestRunRaisesInterruptFromToolCall(t *testing.T) {
	llm := &scriptedLLM{replies: []*LLMResponse{
		{
			ModelID: "gpt-5",
			Content: "let me ask",
			ToolCalls: []LLMToolCall{{
				VendorID: "call_ask",
				ToolName: "ask_user",
				Args: map[string]any{"questions": []any{
					map[string]any{"id": "cluster", "text": "Which cluster?", "kind": "single_choice", "options": []any{"prod", "staging"}},
				}},
			}},
		},
		{ModelID: "gpt-5", Content: "deploying to prod"},
	}}
	persistor := &fakePersistor{next: 1}
	agent, err := NewAgent(&AgentConfig{}, NewToolRegistry(), llm, WithInterrupts([]InterruptSpec{askUserSpec}))
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}

	arc := testArcID
	actor := users.Actor{UserID: "alice", TenantID: "acme"}
	base := []*events.Event{
		{SessionID: testSessionID, ID: arc, Seq: 0, Timestamp: time.Unix(0, 0), Typ: events.UserMessageEventType, CorrelationID: &arc, Actor: actor, Payload: events.UserMessagePayload{Content: "deploy it", Actor: actor}},
	}
	h, err := history.Replay(base)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}

	modes := history.DefaultModes()
	modes.Interrupt = history.TreatmentNote
	if err := agent.Run(context.Background(), RunOptions{Persistor: persistor, History: h, ArcID: arc, Modes: modes}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if llm.calls != 1 {
		t.Fatalf("the model was asked %d times, want 1: the run ended at the interrupt", llm.calls)
	}
	if h.Arcs[0].Status() != history.ArcStatusInterrupted {
		t.Fatalf("arc status = %s, want INTERRUPTED", h.Arcs[0].Status())
	}

	// The log holds the interrupt and the call's result, written together:
	// the interrupt carries the description as its reason and the call's
	// arguments as its context, with no causation — it is arc-wide.
	var interrupt, result *events.Event
	for _, ev := range persistor.persisted {
		switch ev.Typ {
		case events.InterruptEventType:
			interrupt = ev
		case events.ToolResultEventType:
			result = ev
		}
	}
	if interrupt == nil || result == nil {
		t.Fatalf("persisted = %v, want an INTERRUPT and a TOOL_RESULT", typesOfEvents(persistor.persisted))
	}
	payload := interrupt.Payload.(events.InterruptPayload)
	if payload.CausationID != "" {
		t.Errorf("interrupt causation = %q, want empty for an arc-wide interrupt", payload.CausationID)
	}
	if payload.Reason != askUserSpec.Description {
		t.Errorf("interrupt reason = %q, want the spec description", payload.Reason)
	}
	if payload.Context == nil {
		t.Error("interrupt context = nil, want the call's arguments")
	}
	if interrupt.Seq != result.Seq-1 {
		t.Errorf("interrupt seq %d is not right before the result seq %d, want one batch", interrupt.Seq, result.Seq)
	}
	if len(h.Arcs[0].Interrupts) != 1 || h.Arcs[0].Interrupts[0].ID != interrupt.ID {
		t.Errorf("arc interrupts = %#v, want the raised interrupt", h.Arcs[0].Interrupts)
	}
	call := h.Arcs[0].Responses[0].ToolCalls[0]
	if call.Status != history.ToolCallStatusSuccess || !strings.Contains(call.Result.(string), interrupt.ID) {
		t.Errorf("call = %s result %#v, want it settled naming the interrupt", call.Status, call.Result)
	}

	// The answer in the log lets the next run finish the arc, and the
	// resumed run's transcript carries the answer as a note.
	answered := append(append([]*events.Event{}, base...), persistor.persisted...)
	answered = append(answered, &events.Event{
		SessionID: testSessionID, ID: "res-1", Seq: result.Seq + 1, Timestamp: time.Unix(int64(result.Seq+1), 0),
		Typ: events.InterruptResponseEventType, CorrelationID: &arc, Actor: actor,
		Payload: events.InterruptResponsePayload{CausationID: interrupt.ID, Response: map[string]any{"answers": map[string]any{"cluster": "prod"}}, Actor: actor},
	})
	h2, err := history.Replay(answered)
	if err != nil {
		t.Fatalf("replay with the answer: %v", err)
	}
	persistor.next = result.Seq + 2
	if err := agent.Run(context.Background(), RunOptions{Persistor: persistor, History: h2, ArcID: arc, Modes: modes}); err != nil {
		t.Fatalf("Run after the answer: %v", err)
	}
	if llm.calls != 2 {
		t.Errorf("the model was asked %d times in total, want 2", llm.calls)
	}
	if h2.Arcs[0].Status() != history.ArcStatusDone {
		t.Errorf("arc status after the resumed run = %s, want DONE", h2.Arcs[0].Status())
	}
	transcript, err := h2.Project(arc, modes)
	if err != nil {
		t.Fatalf("Project: %v", err)
	}
	var sawAnswer bool
	for _, msg := range transcript.Messages {
		if note, ok := msg.(history.ModelNoteMessage); ok && strings.Contains(note.Content, "a human answered") && strings.Contains(note.Content, "prod") {
			sawAnswer = true
		}
	}
	if !sawAnswer {
		t.Errorf("the resumed run's transcript has no answer note: %#v", transcript.Messages)
	}
}

// An interrupt runs after the other calls of its step, whatever the emission
// order: no call in one message can depend on the user's answer, since the
// model emitted the batch before the answer existed. So [ask, list_pods]
// runs list_pods first — its result is in the log before the INTERRUPT — and
// then raises the interrupt, which holds the arc. A second ask in the same
// message stays pending for the run after the first is answered.
func TestInterruptRunsAfterTheOtherCallsOfTheStep(t *testing.T) {
	invoked := false
	registry := registryWith(t, "list_pods", func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		invoked = true
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "pods"}}}, nil
	})
	llm := &scriptedLLM{replies: []*LLMResponse{{
		ModelID: "gpt-5",
		ToolCalls: []LLMToolCall{
			{VendorID: "call_ask", ToolName: "ask_user", Args: map[string]any{"questions": []any{map[string]any{"id": "q", "text": "?", "kind": "text"}}}},
			{VendorID: "call_pods", ToolName: "list_pods", Args: map[string]any{}},
			{VendorID: "call_ask2", ToolName: "ask_user", Args: map[string]any{"questions": []any{map[string]any{"id": "q2", "text": "??", "kind": "text"}}}},
		},
	}}}
	persistor := &fakePersistor{next: 1}
	agent, err := NewAgent(&AgentConfig{}, registry, llm, WithInterrupts([]InterruptSpec{askUserSpec}))
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	h := userArcHistory(t)
	if err := agent.Run(context.Background(), RunOptions{Persistor: persistor, History: h, ArcID: testArcID}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !invoked {
		t.Error("list_pods did not run although the ask was emitted first, want the interrupt to run last in its step")
	}
	if llm.calls != 1 {
		t.Errorf("the model was asked %d times, want 1: the run ended at the interrupt", llm.calls)
	}
	if h.Arcs[0].Status() != history.ArcStatusInterrupted {
		t.Fatalf("arc status = %s, want INTERRUPTED", h.Arcs[0].Status())
	}

	// The ordinary call's result is written before the interrupt, so the
	// resumed run sees it together with the answer.
	podsCallID := h.Arcs[0].Responses[0].ToolCalls[1].ID
	var podsResult, interrupt *events.Event
	for _, ev := range persistor.persisted {
		switch {
		case ev.Typ == events.ToolResultEventType && ev.Payload.(events.ToolResultPayload).CausationID == podsCallID:
			podsResult = ev
		case ev.Typ == events.InterruptEventType:
			interrupt = ev
		}
	}
	if podsResult == nil || interrupt == nil {
		t.Fatalf("persisted = %v, want a TOOL_RESULT for call_pods and an INTERRUPT", typesOfEvents(persistor.persisted))
	}
	if podsResult.Seq >= interrupt.Seq {
		t.Errorf("call_pods result seq %d is not before the interrupt seq %d, want the interrupt last", podsResult.Seq, interrupt.Seq)
	}

	calls := h.Arcs[0].Responses[0].ToolCalls
	if calls[0].Status != history.ToolCallStatusSuccess {
		t.Errorf("the ask call = %s, want it settled", calls[0].Status)
	}
	if calls[2].Status != history.ToolCallStatusPending {
		t.Errorf("the second ask = %s, want it left pending until the first is answered", calls[2].Status)
	}
}

func typesOfEvents(evs []*events.Event) []events.EventType {
	types := make([]events.EventType, 0, len(evs))
	for _, ev := range evs {
		types = append(types, ev.Typ)
	}
	return types
}

// A settings file's interrupts reach the agent: each becomes a configured
// interrupt, classified as one and offered as a tool with the request schema
// from the file.
func TestFileResolverBuildsInterruptsFromSettings(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agents.json")
	settings := `{"system_prompt":"p","model":{"source":"test","name":"gpt-5"},` +
		`"interrupts":[{"name":"ask_user","description":"Ask the user one or more questions",` +
		`"request_schema":{"type":"object","properties":{"questions":{"type":"array","minItems":1}}}}]}`
	if err := os.WriteFile(path, []byte(settings), 0o644); err != nil {
		t.Fatalf("write settings: %v", err)
	}
	resolver := NewFileResolver(path, nil, nil, nil, scriptedModels)
	agent, err := resolver.ResolveAgent(context.Background(), "anybody")
	if err != nil {
		t.Fatalf("ResolveAgent: %v", err)
	}
	if len(agent.interruptSpecs) != 1 || agent.interruptSpecs[0].Name != "ask_user" {
		t.Fatalf("specs = %+v, want ask_user configured", agent.interruptSpecs)
	}
	if got := agent.operation("ask_user"); got != opInterrupt {
		t.Errorf("operation(ask_user) = %v, want opInterrupt", got)
	}
	if !agent.knows("ask_user") {
		t.Error("knows(ask_user) = false, want the interrupt call to have somewhere to go")
	}
	tools := interruptToolSet{agent.interruptSpecs}.All()
	if len(tools) != 1 || tools[0].Name != "ask_user" {
		t.Fatalf("tools = %+v", tools)
	}
	schema, ok := tools[0].InputSchema.(map[string]any)
	if !ok {
		t.Fatalf("input schema = %#v, want the request schema from the file as JSON", tools[0].InputSchema)
	}
	if fmt.Sprintf("%v", schema["type"]) != "object" {
		t.Errorf("schema type = %v, want object", schema["type"])
	}
}
