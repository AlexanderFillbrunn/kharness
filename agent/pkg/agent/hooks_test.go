package agent

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"fillbrunn.ai/kube-agent/proto/pkg/common"
	"fillbrunn.ai/kube-agent/proto/pkg/events"
	"fillbrunn.ai/kube-agent/proto/pkg/history"
	"fillbrunn.ai/kube-agent/proto/pkg/users"
)

// toolCallReply is one scripted model turn that requests a single tool call.
func toolCallReply(vendorID, toolName string, args any) *LLMResponse {
	return &LLMResponse{
		ModelID:   "gpt-5",
		Content:   "checking",
		ToolCalls: []LLMToolCall{{VendorID: vendorID, ToolName: toolName, Args: args}},
	}
}

func finalReply(content string) *LLMResponse {
	return &LLMResponse{ModelID: "gpt-5", Content: content}
}

// persistedOfType returns the persisted events of one type.
func persistedOfType(p *fakePersistor, typ events.EventType) []*events.Event {
	var out []*events.Event
	for _, ev := range p.persisted {
		if ev.Typ == typ {
			out = append(out, ev)
		}
	}
	return out
}

// A BeforeToolCall Block refuses the call without asking anyone: a
// GUARDRAIL_BLOCKED is written, the call settles BLOCKED, and the tool never
// runs.
func TestBeforeToolCallBlock(t *testing.T) {
	invoked := false
	registry := registryWith(t, "delete_pod", func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		invoked = true
		return textResult("deleted"), nil
	})
	llm := &scriptedLLM{replies: []*LLMResponse{
		toolCallReply("call_1", "delete_pod", map[string]any{"name": "web-1"}),
		finalReply("not deleting anything"),
	}}
	agent, err := NewAgent(&AgentConfig{}, registry, llm, WithHooks(Hooks{
		BeforeToolCall: func(_ context.Context, call ToolCallView) (ToolCallEffects, error) {
			if call.ToolName != "delete_pod" {
				t.Errorf("hook saw tool %q", call.ToolName)
			}
			if call.CallID == "" || call.ArcID != testArcID || call.MessageID == "" {
				t.Errorf("hook saw call %+v", call)
			}
			return ToolCallEffects{HookEffects: HookEffects{Block: &BlockRequest{Message: "destructive calls are not allowed"}}}, nil
		},
	}))
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}

	persistor := &fakePersistor{next: 1}
	h := userArcHistory(t)
	if err := agent.Run(context.Background(), RunOptions{Persistor: persistor, History: h, ArcID: testArcID}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if invoked {
		t.Error("the blocked tool was invoked")
	}
	blocked := persistedOfType(persistor, events.GuardrailBlockedEventType)
	if len(blocked) != 1 {
		t.Fatalf("persisted %d GUARDRAIL_BLOCKED events, want 1", len(blocked))
	}
	call := h.Arcs[0].Responses[0].ToolCalls[0]
	if call.Status != history.ToolCallStatusBlocked {
		t.Errorf("call status = %s, want BLOCKED", call.Status)
	}
	if got := blocked[0].Payload.(events.GuardrailBlockedPayload); got.CausationID != call.ID || got.Message != "destructive calls are not allowed" {
		t.Errorf("blocked payload = %+v, want causation %s and the block message", got, call.ID)
	}
	if h.Arcs[0].Status() != history.ArcStatusDone || llm.calls != 2 {
		t.Errorf("arc = %s after %d model calls, want DONE after 2", h.Arcs[0].Status(), llm.calls)
	}
}

// A BeforeToolCall Ask raises a call-scoped INTERRUPT, which holds the arc:
// the run ends without running the call, and an approval lets a later run
// execute it.
func TestBeforeToolCallAskThenApprovalRunsTheCall(t *testing.T) {
	invoked := 0
	registry := registryWith(t, "deploy", func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		invoked++
		return textResult("deployed"), nil
	})
	llm := &scriptedLLM{replies: []*LLMResponse{
		toolCallReply("call_1", "deploy", map[string]any{"env": "prod"}),
		finalReply("done"),
	}}
	agent, err := NewAgent(&AgentConfig{}, registry, llm, WithHooks(Hooks{
		BeforeToolCall: func(_ context.Context, call ToolCallView) (ToolCallEffects, error) {
			return ToolCallEffects{HookEffects: HookEffects{Ask: &AskRequest{Reason: "deploy to prod?", Context: map[string]any{"env": "prod"}}}}, nil
		},
	}))
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}

	ctx := context.Background()
	persistor := &fakePersistor{next: 1}
	h := userArcHistory(t)
	if err := agent.Run(ctx, RunOptions{Persistor: persistor, History: h, ArcID: testArcID}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if invoked != 0 {
		t.Fatal("the tool ran before a human approved it")
	}
	interrupts := persistedOfType(persistor, events.InterruptEventType)
	if len(interrupts) != 1 {
		t.Fatalf("persisted %d INTERRUPT events, want 1", len(interrupts))
	}
	call := h.Arcs[0].Responses[0].ToolCalls[0]
	pl := interrupts[0].Payload.(events.InterruptPayload)
	if pl.CausationID != call.ID {
		t.Errorf("interrupt causation = %q, want the call %q", pl.CausationID, call.ID)
	}
	if pl.Reason != "deploy to prod?" {
		t.Errorf("interrupt reason = %q", pl.Reason)
	}
	if h.Arcs[0].Status() != history.ArcStatusInterrupted {
		t.Fatalf("arc = %s, want INTERRUPTED", h.Arcs[0].Status())
	}
	if llm.calls != 1 {
		t.Errorf("model calls = %d, want 1 (the run ended at the ask)", llm.calls)
	}

	// The human approves: the response settles the interrupt, and the resumed
	// run executes the approved call.
	actor := users.Actor{UserID: "alice", TenantID: "acme"}
	arc := testArcID
	response := &events.Event{
		Typ:           events.InterruptResponseEventType,
		CorrelationID: &arc,
		Actor:         actor,
		Payload:       events.InterruptResponsePayload{CausationID: interrupts[0].ID, Response: "yes", Actor: actor},
	}
	stored, err := persistor.Persist(ctx, response)
	if err != nil {
		t.Fatalf("persist response: %v", err)
	}
	if err := h.Update(stored[0], history.FoldStateFromHistory(h)); err != nil {
		t.Fatalf("fold response: %v", err)
	}
	if err := agent.Run(ctx, RunOptions{Persistor: persistor, History: h, ArcID: testArcID}); err != nil {
		t.Fatalf("resumed Run: %v", err)
	}
	if invoked != 1 {
		t.Errorf("tool invoked %d times after approval, want 1", invoked)
	}
	if call.Status != history.ToolCallStatusSuccess {
		t.Errorf("call status = %s, want SUCCESS after approval", call.Status)
	}
	if h.Arcs[0].Status() != history.ArcStatusDone {
		t.Errorf("arc = %s after the resumed run, want DONE", h.Arcs[0].Status())
	}
}

// A rejection answers the call itself: it settles REFUSED with the human's
// words as the result, and the tool never runs.
func TestBeforeToolCallAskThenRejectionRefusesTheCall(t *testing.T) {
	invoked := false
	registry := registryWith(t, "deploy", func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		invoked = true
		return textResult("deployed"), nil
	})
	llm := &scriptedLLM{replies: []*LLMResponse{
		toolCallReply("call_1", "deploy", nil),
		finalReply("aborted"),
	}}
	agent, err := NewAgent(&AgentConfig{}, registry, llm, WithHooks(Hooks{
		BeforeToolCall: func(context.Context, ToolCallView) (ToolCallEffects, error) {
			return ToolCallEffects{HookEffects: HookEffects{Ask: &AskRequest{Reason: "deploy?"}}}, nil
		},
	}))
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}

	ctx := context.Background()
	persistor := &fakePersistor{next: 1}
	h := userArcHistory(t)
	if err := agent.Run(ctx, RunOptions{Persistor: persistor, History: h, ArcID: testArcID}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	interrupts := persistedOfType(persistor, events.InterruptEventType)
	if len(interrupts) != 1 {
		t.Fatalf("persisted %d INTERRUPT events, want 1", len(interrupts))
	}

	actor := users.Actor{UserID: "alice", TenantID: "acme"}
	arc := testArcID
	stored, err := persistor.Persist(ctx, &events.Event{
		Typ:           events.InterruptResponseEventType,
		CorrelationID: &arc,
		Actor:         actor,
		Payload:       events.InterruptResponsePayload{CausationID: interrupts[0].ID, Response: "no, staging only", IsRejected: true, Actor: actor},
	})
	if err != nil {
		t.Fatalf("persist response: %v", err)
	}
	if err := h.Update(stored[0], history.FoldStateFromHistory(h)); err != nil {
		t.Fatalf("fold response: %v", err)
	}
	if err := agent.Run(ctx, RunOptions{Persistor: persistor, History: h, ArcID: testArcID}); err != nil {
		t.Fatalf("resumed Run: %v", err)
	}

	if invoked {
		t.Error("the refused tool was invoked")
	}
	call := h.Arcs[0].Responses[0].ToolCalls[0]
	if call.Status != history.ToolCallStatusRefused {
		t.Errorf("call status = %s, want REFUSED", call.Status)
	}
	if call.Result != "no, staging only" {
		t.Errorf("call result = %#v, want the human's words", call.Result)
	}
	if h.Arcs[0].Status() != history.ArcStatusDone {
		t.Errorf("arc = %s, want DONE", h.Arcs[0].Status())
	}
}

// Several asks in one step are raised together, in one batch.
func TestBeforeToolCallAsksAreRaisedTogether(t *testing.T) {
	registry := NewToolRegistry()
	for _, name := range []string{"deploy", "scale"} {
		if err := registry.RegisterTool(&mcp.Tool{Name: name}, func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return textResult("ok"), nil
		}); err != nil {
			t.Fatalf("register: %v", err)
		}
	}
	llm := &scriptedLLM{replies: []*LLMResponse{{
		ModelID: "gpt-5",
		Content: "two changes",
		ToolCalls: []LLMToolCall{
			{VendorID: "call_1", ToolName: "deploy", Args: nil},
			{VendorID: "call_2", ToolName: "scale", Args: nil},
		},
	}}}
	agent, err := NewAgent(&AgentConfig{}, registry, llm, WithHooks(Hooks{
		BeforeToolCall: func(context.Context, ToolCallView) (ToolCallEffects, error) {
			return ToolCallEffects{HookEffects: HookEffects{Ask: &AskRequest{Reason: "allow?"}}}, nil
		},
	}))
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}

	persistor := &fakePersistor{next: 1}
	h := userArcHistory(t)
	if err := agent.Run(context.Background(), RunOptions{Persistor: persistor, History: h, ArcID: testArcID}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	interrupts := persistedOfType(persistor, events.InterruptEventType)
	if len(interrupts) != 2 {
		t.Fatalf("persisted %d INTERRUPT events, want both asks raised together", len(interrupts))
	}
	if interrupts[1].Seq != interrupts[0].Seq+1 {
		t.Errorf("interrupts at seq %d and %d, want one batch", interrupts[0].Seq, interrupts[1].Seq)
	}
	if h.Arcs[0].Status() != history.ArcStatusInterrupted {
		t.Errorf("arc = %s, want INTERRUPTED", h.Arcs[0].Status())
	}
}

// An Answer settles the call without dispatching it: the tool never runs, and
// the answer is the call's result.
func TestBeforeToolCallAnswer(t *testing.T) {
	invoked := false
	registry := registryWith(t, "get_config", func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		invoked = true
		return textResult("live config"), nil
	})
	llm := &scriptedLLM{replies: []*LLMResponse{
		toolCallReply("call_1", "get_config", nil),
		finalReply("answered from cache"),
	}}
	agent, err := NewAgent(&AgentConfig{}, registry, llm, WithHooks(Hooks{
		BeforeToolCall: func(context.Context, ToolCallView) (ToolCallEffects, error) {
			return ToolCallEffects{HookEffects: HookEffects{Answer: &Answer{Result: "cached config"}}}, nil
		},
	}))
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}

	persistor := &fakePersistor{next: 1}
	h := userArcHistory(t)
	if err := agent.Run(context.Background(), RunOptions{Persistor: persistor, History: h, ArcID: testArcID}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if invoked {
		t.Error("the answered tool was invoked")
	}
	results := persistedOfType(persistor, events.ToolResultEventType)
	if len(results) != 1 {
		t.Fatalf("persisted %d TOOL_RESULT events, want 1", len(results))
	}
	pl := results[0].Payload.(events.ToolResultPayload)
	if pl.Result != "cached config" || pl.IsError {
		t.Errorf("result payload = %+v, want the answer, not an error", pl)
	}
	if dispatched := persistedOfType(persistor, events.ToolDispatchedEventType); len(dispatched) != 0 {
		t.Errorf("persisted %d TOOL_DISPATCHED events, want none for an answered call", len(dispatched))
	}
	if h.Arcs[0].Status() != history.ArcStatusDone {
		t.Errorf("arc = %s, want DONE", h.Arcs[0].Status())
	}
}

// An Args override changes what the tool executes while the log keeps the
// arguments the model asked for.
func TestBeforeToolCallArgsOverride(t *testing.T) {
	var gotArgs json.RawMessage
	registry := registryWith(t, "list_pods", func(_ context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		gotArgs = req.Params.Arguments
		return textResult("pods in default"), nil
	})
	llm := &scriptedLLM{replies: []*LLMResponse{
		toolCallReply("call_1", "list_pods", map[string]any{"namespace": "kube-system"}),
		finalReply("done"),
	}}
	agent, err := NewAgent(&AgentConfig{}, registry, llm, WithHooks(Hooks{
		BeforeToolCall: func(context.Context, ToolCallView) (ToolCallEffects, error) {
			override := any(map[string]any{"namespace": "default"})
			return ToolCallEffects{HookEffects: HookEffects{Run: true}, Args: &override}, nil
		},
	}))
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}

	persistor := &fakePersistor{next: 1}
	h := userArcHistory(t)
	if err := agent.Run(context.Background(), RunOptions{Persistor: persistor, History: h, ArcID: testArcID}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	var got map[string]any
	if err := json.Unmarshal(gotArgs, &got); err != nil {
		t.Fatalf("unmarshal executed args: %v", err)
	}
	if got["namespace"] != "default" {
		t.Errorf("tool executed with %v, want the override", got)
	}
	logged := persistedOfType(persistor, events.ToolCallEventType)
	if len(logged) != 1 {
		t.Fatalf("persisted %d TOOL_CALL events, want 1", len(logged))
	}
	loggedArgs := logged[0].Payload.(events.ToolCallPayload).Args.(map[string]any)
	if loggedArgs["namespace"] != "kube-system" {
		t.Errorf("logged args = %v, want the model's original", loggedArgs)
	}
}

// A hook that fails fails closed: the call is settled as an error and never
// run, and the run continues.
func TestBeforeToolCallErrorFailsClosed(t *testing.T) {
	invoked := false
	registry := registryWith(t, "delete_pod", func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		invoked = true
		return textResult("deleted"), nil
	})
	llm := &scriptedLLM{replies: []*LLMResponse{
		toolCallReply("call_1", "delete_pod", nil),
		finalReply("skipped"),
	}}
	agent, err := NewAgent(&AgentConfig{}, registry, llm, WithHooks(Hooks{
		BeforeToolCall: func(context.Context, ToolCallView) (ToolCallEffects, error) {
			return ToolCallEffects{}, errors.New("policy store unreachable")
		},
	}))
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}

	persistor := &fakePersistor{next: 1}
	h := userArcHistory(t)
	if err := agent.Run(context.Background(), RunOptions{Persistor: persistor, History: h, ArcID: testArcID}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if invoked {
		t.Error("the tool ran although the hook failed")
	}
	results := persistedOfType(persistor, events.ToolResultEventType)
	if len(results) != 1 {
		t.Fatalf("persisted %d TOOL_RESULT events, want 1", len(results))
	}
	pl := results[0].Payload.(events.ToolResultPayload)
	if !pl.IsError || !strings.Contains(pl.Result.(string), "policy store unreachable") {
		t.Errorf("result payload = %+v, want an error naming the hook failure", pl)
	}
	if h.Arcs[0].Status() != history.ArcStatusDone {
		t.Errorf("arc = %s, want DONE (the run continues after a failed hook)", h.Arcs[0].Status())
	}
}

// A decision setting more than one action is refused the same way: fail
// closed.
func TestBeforeToolCallInvalidDecisionFailsClosed(t *testing.T) {
	invoked := false
	registry := registryWith(t, "delete_pod", func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		invoked = true
		return textResult("deleted"), nil
	})
	llm := &scriptedLLM{replies: []*LLMResponse{
		toolCallReply("call_1", "delete_pod", nil),
		finalReply("skipped"),
	}}
	agent, err := NewAgent(&AgentConfig{}, registry, llm, WithHooks(Hooks{
		BeforeToolCall: func(context.Context, ToolCallView) (ToolCallEffects, error) {
			return ToolCallEffects{HookEffects: HookEffects{Run: true, Block: &BlockRequest{Message: "both"}}}, nil
		},
	}))
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	persistor := &fakePersistor{next: 1}
	h := userArcHistory(t)
	if err := agent.Run(context.Background(), RunOptions{Persistor: persistor, History: h, ArcID: testArcID}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if invoked {
		t.Error("the tool ran on an invalid decision")
	}
	call := h.Arcs[0].Responses[0].ToolCalls[0]
	if call.Status != history.ToolCallStatusFailed {
		t.Errorf("call status = %s, want FAILED", call.Status)
	}
}

// AfterToolCall rewrites the recorded result — redaction happens before
// anything lands in the log — and can fail the call or attach notes.
func TestAfterToolCallRedactsFailsAndNotes(t *testing.T) {
	registry := registryWith(t, "get_secret", func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return textResult("token=hunter2"), nil
	})
	llm := &scriptedLLM{replies: []*LLMResponse{
		toolCallReply("call_1", "get_secret", nil),
		finalReply("done"),
	}}
	redacted := any("token=[redacted]")
	isErr := true
	agent, err := NewAgent(&AgentConfig{}, registry, llm, WithHooks(Hooks{
		AfterToolCall: func(_ context.Context, res ToolCallResult) (ToolCallChanges, error) {
			if res.ToolName != "get_secret" || res.Result != "token=hunter2" || res.CallID == "" {
				t.Errorf("hook saw result %+v", res)
			}
			return ToolCallChanges{Result: &redacted, IsError: &isErr, Notes: []string{"secret was fetched"}}, nil
		},
	}))
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}

	persistor := &fakePersistor{next: 1}
	h := userArcHistory(t)
	if err := agent.Run(context.Background(), RunOptions{Persistor: persistor, History: h, ArcID: testArcID}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	results := persistedOfType(persistor, events.ToolResultEventType)
	if len(results) != 1 {
		t.Fatalf("persisted %d TOOL_RESULT events, want 1", len(results))
	}
	pl := results[0].Payload.(events.ToolResultPayload)
	if pl.Result != "token=[redacted]" || !pl.IsError {
		t.Errorf("recorded result = %+v, want the redacted, failed one", pl)
	}
	notes := persistedOfType(persistor, events.ToolEventEventType)
	if len(notes) != 1 || notes[0].Payload.(events.ToolEventPayload).Payload != "secret was fetched" {
		t.Errorf("notes = %+v, want the one note", notes)
	}
	if h.Arcs[0].Status() != history.ArcStatusDone {
		t.Errorf("arc = %s, want DONE", h.Arcs[0].Status())
	}
}

// A call a hook marked sequential never joins a parallel wave.
func TestWavesSoloCallsRunAlone(t *testing.T) {
	registry := parallelRegistry(t, true, map[string]mcp.ToolHandler{
		"read": func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) { return textResult("x"), nil },
	})
	agent := Agent{config: &AgentConfig{}, tools: registry}
	calls := []*history.ToolCall{{ToolName: "read"}, {ToolName: "read"}, {ToolName: "read"}}
	waves := agent.waves(calls, callDecisions{calls[1]: {solo: true}})
	want := [][]string{{"read"}, {"read"}, {"read"}}
	if len(waves) != len(want) {
		t.Fatalf("waves = %d, want %d", len(waves), len(want))
	}
	for i, wave := range waves {
		if len(wave) != 1 || wave[0] != calls[i] {
			t.Errorf("wave %d = %v, want the single call %d", i, wave, i)
		}
	}
	waves = agent.waves(calls, nil)
	if len(waves) != 1 || len(waves[0]) != 3 {
		t.Errorf("waves without solo = %d waves, want one wave of 3", len(waves))
	}
}

// A BeforeTaskStart Block refuses the start without asking anyone: the
// invoke is recorded, the meta event records the launch as failed, no task is
// started, and the run continues.
func TestBeforeTaskStartBlock(t *testing.T) {
	controller := &fakeController{}
	tasks := taskSourceWith(t, controller, "deploy_task")
	llm := &scriptedLLM{replies: []*LLMResponse{
		toolCallReply("call_1", "deploy_task", map[string]any{"env": "prod"}),
		finalReply("not started"),
	}}
	agent, err := NewAgent(&AgentConfig{}, NewToolRegistry(), llm, WithTasks(tasks), WithHooks(Hooks{
		BeforeTaskStart: func(_ context.Context, v TaskStartView) (TaskStartEffects, error) {
			if v.TaskName != "deploy_task" {
				t.Errorf("hook saw task %q, want deploy_task", v.TaskName)
			}
			if v.Args["env"] != "prod" || v.CallID == "" || v.ArcID != testArcID {
				t.Errorf("hook saw args %v, call %q, arc %q", v.Args, v.CallID, v.ArcID)
			}
			return TaskStartEffects{HookEffects: HookEffects{Block: &BlockRequest{Message: "task starts need a ticket"}}}, nil
		},
	}))
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}

	persistor := &fakePersistor{next: 1}
	h := userArcHistory(t)
	if err := agent.Run(context.Background(), RunOptions{Persistor: persistor, History: h, ArcID: testArcID, Controller: tasks}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(controller.started) != 0 {
		t.Errorf("started tasks = %v, want none", controller.started)
	}
	invokes := persistedOfType(persistor, events.AsyncTaskInvokeEventType)
	if len(invokes) != 1 {
		t.Fatalf("persisted %d ASYNC_TASK_INVOKE events, want 1", len(invokes))
	}
	metas := persistedOfType(persistor, events.AsyncTaskMetaEventType)
	if len(metas) != 1 {
		t.Fatalf("persisted %d ASYNC_TASK_META events, want 1", len(metas))
	}
	meta := metas[0].Payload.(events.AsyncTaskMetaPayload)
	if meta.Launched || meta.CausationID != invokes[0].ID || meta.ExternalRef != invokes[0].ID {
		t.Errorf("meta = %+v, want Launched=false with the invoke's ID as causation and external ref", meta)
	}
	if !strings.Contains(meta.Error, "task starts need a ticket") {
		t.Errorf("meta error = %q, want the block message", meta.Error)
	}
	if h.Arcs[0].Status() != history.ArcStatusDone {
		t.Errorf("arc = %s, want DONE", h.Arcs[0].Status())
	}
}

// A BeforeTaskStart Ask holds the start for a human; once approved, the
// resumed run starts the task.
func TestBeforeTaskStartAskThenApprovalStartsTask(t *testing.T) {
	controller := &fakeController{}
	tasks := taskSourceWith(t, controller, "deploy_task")
	llm := &scriptedLLM{replies: []*LLMResponse{
		toolCallReply("call_1", "deploy_task", map[string]any{"env": "prod"}),
		finalReply("started"),
	}}
	agent, err := NewAgent(&AgentConfig{}, NewToolRegistry(), llm, WithTasks(tasks), WithHooks(Hooks{
		BeforeTaskStart: func(context.Context, TaskStartView) (TaskStartEffects, error) {
			return TaskStartEffects{HookEffects: HookEffects{Ask: &AskRequest{Reason: "start deploy_task?"}}}, nil
		},
	}))
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}

	ctx := context.Background()
	persistor := &fakePersistor{next: 1}
	h := userArcHistory(t)
	if err := agent.Run(ctx, RunOptions{Persistor: persistor, History: h, ArcID: testArcID, Controller: tasks}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(controller.started) != 0 {
		t.Fatal("the task started before approval")
	}
	interrupts := persistedOfType(persistor, events.InterruptEventType)
	if len(interrupts) != 1 {
		t.Fatalf("persisted %d INTERRUPT events, want 1", len(interrupts))
	}
	invokes := persistedOfType(persistor, events.AsyncTaskInvokeEventType)
	if len(invokes) != 1 {
		t.Fatalf("persisted %d ASYNC_TASK_INVOKE events, want 1", len(invokes))
	}
	if pl := interrupts[0].Payload.(events.InterruptPayload); pl.CausationID != invokes[0].ID {
		t.Errorf("interrupt causation = %q, want task %q", pl.CausationID, invokes[0].ID)
	}
	if h.Arcs[0].Status() != history.ArcStatusInterrupted {
		t.Fatalf("arc = %s, want INTERRUPTED", h.Arcs[0].Status())
	}

	actor := users.Actor{UserID: "alice", TenantID: "acme"}
	arc := testArcID
	stored, err := persistor.Persist(ctx, &events.Event{
		Typ:           events.InterruptResponseEventType,
		CorrelationID: &arc,
		Actor:         actor,
		Payload:       events.InterruptResponsePayload{CausationID: interrupts[0].ID, Response: "go ahead", Actor: actor},
	})
	if err != nil {
		t.Fatalf("persist response: %v", err)
	}
	if err := h.Update(stored[0], history.FoldStateFromHistory(h)); err != nil {
		t.Fatalf("fold response: %v", err)
	}
	if err := agent.Run(ctx, RunOptions{Persistor: persistor, History: h, ArcID: testArcID, Controller: tasks}); err != nil {
		t.Fatalf("resumed Run: %v", err)
	}
	if len(controller.started) != 1 || controller.started[0] != "deploy_task" {
		t.Errorf("started tasks = %v, want [deploy_task]", controller.started)
	}
	if invokes := persistedOfType(persistor, events.AsyncTaskInvokeEventType); len(invokes) != 1 {
		t.Errorf("persisted %d ASYNC_TASK_INVOKE events, want 1", len(invokes))
	}
	metas := persistedOfType(persistor, events.AsyncTaskMetaEventType)
	if len(metas) != 1 {
		t.Fatalf("persisted %d ASYNC_TASK_META events, want 1", len(metas))
	}
	if meta := metas[0].Payload.(events.AsyncTaskMetaPayload); !meta.Launched || meta.CausationID != invokes[0].ID {
		t.Errorf("meta = %+v, want Launched=true causated to the invoke", meta)
	}
	if h.Arcs[0].Status() != history.ArcStatusDone {
		t.Errorf("arc = %s, want DONE", h.Arcs[0].Status())
	}
}

// The skill tool is governed by BeforeSkillLoad, not BeforeToolCall: an
// Answer serves the content without loading the skill.
func TestBeforeSkillLoadAnswer(t *testing.T) {
	llm := &scriptedLLM{replies: []*LLMResponse{
		toolCallReply("call_1", LoadSkillToolName, map[string]any{"name": "deploy"}),
		finalReply("answered"),
	}}
	agent, err := NewAgent(&AgentConfig{}, NewToolRegistry(), llm,
		WithSkills(testSkillSet()),
		WithHooks(Hooks{
			BeforeToolCall: func(context.Context, ToolCallView) (ToolCallEffects, error) {
				t.Error("BeforeToolCall fired for the skill tool; BeforeSkillLoad governs it")
				return ToolCallEffects{HookEffects: HookEffects{Run: true}}, nil
			},
			BeforeSkillLoad: func(_ context.Context, call ToolCallView) (ToolCallEffects, error) {
				if call.ToolName != LoadSkillToolName {
					t.Errorf("hook saw tool %q", call.ToolName)
				}
				return ToolCallEffects{HookEffects: HookEffects{Answer: &Answer{Result: "skill content from policy"}}}, nil
			},
		}))
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}

	persistor := &fakePersistor{next: 1}
	h := userArcHistory(t)
	if err := agent.Run(context.Background(), RunOptions{Persistor: persistor, History: h, ArcID: testArcID}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	results := persistedOfType(persistor, events.ToolResultEventType)
	if len(results) != 1 {
		t.Fatalf("persisted %d TOOL_RESULT events, want 1", len(results))
	}
	if pl := results[0].Payload.(events.ToolResultPayload); pl.Result != "skill content from policy" {
		t.Errorf("result = %#v, want the policy answer", pl.Result)
	}
	if h.Arcs[0].Status() != history.ArcStatusDone {
		t.Errorf("arc = %s, want DONE", h.Arcs[0].Status())
	}
}

// AfterSkillLoad rewrites what a loaded skill returns before it is recorded.
func TestAfterSkillLoadRedacts(t *testing.T) {
	llm := &scriptedLLM{replies: []*LLMResponse{
		toolCallReply("call_1", LoadSkillToolName, map[string]any{"name": "git-workflow"}),
		finalReply("done"),
	}}
	redacted := any("[skill redacted]")
	agent, err := NewAgent(&AgentConfig{}, NewToolRegistry(), llm,
		WithSkills(testSkillSet()),
		WithHooks(Hooks{
			AfterSkillLoad: func(_ context.Context, res ToolCallResult) (ToolCallChanges, error) {
				if res.Result != "git-workflow body" {
					t.Errorf("hook saw result %#v, want the loaded body", res.Result)
				}
				return ToolCallChanges{Result: &redacted}, nil
			},
		}))
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	persistor := &fakePersistor{next: 1}
	h := userArcHistory(t)
	if err := agent.Run(context.Background(), RunOptions{Persistor: persistor, History: h, ArcID: testArcID}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	results := persistedOfType(persistor, events.ToolResultEventType)
	if len(results) != 1 || results[0].Payload.(events.ToolResultPayload).Result != "[skill redacted]" {
		t.Errorf("results = %+v, want the redacted one", results)
	}
}

// BeforeInterrupt can block an arc-wide interrupt from a configured tool: no
// INTERRUPT is written and the call settles as a tool error.
func TestBeforeInterruptBlock(t *testing.T) {
	llm := &scriptedLLM{replies: []*LLMResponse{
		toolCallReply("call_1", "ask_user", map[string]any{"questions": []any{}}),
		finalReply("skipped asking"),
	}}
	agent, err := NewAgent(&AgentConfig{}, NewToolRegistry(), llm,
		WithInterrupts([]InterruptSpec{askUserSpec}),
		WithHooks(Hooks{
			BeforeInterrupt: func(_ context.Context, req InterruptRequest) (InterruptChanges, error) {
				if req.ToolName != "ask_user" || req.CallID != "" || req.ArcID != testArcID {
					t.Errorf("hook saw ask %+v", req)
				}
				return InterruptChanges{Block: &BlockRequest{Message: "this agent does not ask"}}, nil
			},
		}))
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	persistor := &fakePersistor{next: 1}
	h := userArcHistory(t)
	if err := agent.Run(context.Background(), RunOptions{Persistor: persistor, History: h, ArcID: testArcID}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if interrupts := persistedOfType(persistor, events.InterruptEventType); len(interrupts) != 0 {
		t.Errorf("persisted %d INTERRUPT events, want none (blocked)", len(interrupts))
	}
	call := h.Arcs[0].Responses[0].ToolCalls[0]
	if call.Status != history.ToolCallStatusFailed || call.Result != "this agent does not ask" {
		t.Errorf("call = %s %#v, want FAILED with the block message", call.Status, call.Result)
	}
	if h.Arcs[0].Status() != history.ArcStatusDone {
		t.Errorf("arc = %s, want DONE (the run continues past a blocked interrupt)", h.Arcs[0].Status())
	}
}

// BeforeInterrupt can rewrite the ask, and AfterInterrupt observes the raised
// interrupt after it is persisted.
func TestBeforeInterruptRewritesAndAfterInterruptObserves(t *testing.T) {
	llm := &scriptedLLM{replies: []*LLMResponse{
		toolCallReply("call_1", "ask_user", map[string]any{"questions": []any{}}),
	}}
	var seen []InterruptView
	newReason := "policy-approved phrasing"
	agent, err := NewAgent(&AgentConfig{}, NewToolRegistry(), llm,
		WithInterrupts([]InterruptSpec{askUserSpec}),
		WithHooks(Hooks{
			BeforeInterrupt: func(_ context.Context, req InterruptRequest) (InterruptChanges, error) {
				return InterruptChanges{Reason: &newReason}, nil
			},
			AfterInterrupt: func(_ context.Context, view InterruptView) {
				seen = append(seen, view)
			},
		}))
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	persistor := &fakePersistor{next: 1}
	h := userArcHistory(t)
	if err := agent.Run(context.Background(), RunOptions{Persistor: persistor, History: h, ArcID: testArcID}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	interrupts := persistedOfType(persistor, events.InterruptEventType)
	if len(interrupts) != 1 {
		t.Fatalf("persisted %d INTERRUPT events, want 1", len(interrupts))
	}
	if pl := interrupts[0].Payload.(events.InterruptPayload); pl.Reason != "policy-approved phrasing" {
		t.Errorf("interrupt reason = %q, want the rewritten one", pl.Reason)
	}
	if len(seen) != 1 || seen[0].ID != interrupts[0].ID || seen[0].Scope != ScopeArc || seen[0].ArcID != testArcID {
		t.Errorf("AfterInterrupt saw %+v, want the stored arc-wide interrupt", seen)
	}
	if h.Arcs[0].Status() != history.ArcStatusInterrupted {
		t.Errorf("arc = %s, want INTERRUPTED", h.Arcs[0].Status())
	}
}

// BeforeInterrupt can auto-answer a call-scoped ask: approval runs the call
// in the same run, rejection refuses it, and no human is involved.
func TestBeforeInterruptAutoAnswer(t *testing.T) {
	invoked := 0
	registry := registryWith(t, "deploy", func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		invoked++
		return textResult("deployed"), nil
	})
	llm := &scriptedLLM{replies: []*LLMResponse{
		toolCallReply("call_1", "deploy", nil),
		finalReply("done"),
	}}
	agent, err := NewAgent(&AgentConfig{}, registry, llm, WithHooks(Hooks{
		BeforeToolCall: func(context.Context, ToolCallView) (ToolCallEffects, error) {
			return ToolCallEffects{HookEffects: HookEffects{Ask: &AskRequest{Reason: "deploy?"}}}, nil
		},
		BeforeInterrupt: func(_ context.Context, req InterruptRequest) (InterruptChanges, error) {
			if req.CallID == "" {
				t.Errorf("expected a call-scoped ask, got %+v", req)
			}
			return InterruptChanges{AutoAnswer: &InterruptAnswer{Response: "auto-approved"}}, nil
		},
	}))
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	persistor := &fakePersistor{next: 1}
	h := userArcHistory(t)
	if err := agent.Run(context.Background(), RunOptions{Persistor: persistor, History: h, ArcID: testArcID}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	responses := persistedOfType(persistor, events.InterruptResponseEventType)
	if len(responses) != 1 {
		t.Fatalf("persisted %d INTERRUPT_RESPONSE events, want the auto-answer", len(responses))
	}
	if pl := responses[0].Payload.(events.InterruptResponsePayload); pl.IsRejected || pl.Response != "auto-approved" {
		t.Errorf("response = %+v, want an approval with the policy answer", pl)
	}
	if invoked != 1 {
		t.Errorf("tool invoked %d times, want 1 (auto-approved calls run in the same run)", invoked)
	}
	if h.Arcs[0].Status() != history.ArcStatusDone {
		t.Errorf("arc = %s, want DONE without waiting for a human", h.Arcs[0].Status())
	}
}

// An auto-answer rejection refuses the call outright.
func TestBeforeInterruptAutoAnswerRejection(t *testing.T) {
	invoked := false
	registry := registryWith(t, "deploy", func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		invoked = true
		return textResult("deployed"), nil
	})
	llm := &scriptedLLM{replies: []*LLMResponse{
		toolCallReply("call_1", "deploy", nil),
		finalReply("refused"),
	}}
	agent, err := NewAgent(&AgentConfig{}, registry, llm, WithHooks(Hooks{
		BeforeToolCall: func(context.Context, ToolCallView) (ToolCallEffects, error) {
			return ToolCallEffects{HookEffects: HookEffects{Ask: &AskRequest{Reason: "deploy?"}}}, nil
		},
		BeforeInterrupt: func(context.Context, InterruptRequest) (InterruptChanges, error) {
			return InterruptChanges{AutoAnswer: &InterruptAnswer{Response: "denied by policy", IsRejected: true}}, nil
		},
	}))
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	persistor := &fakePersistor{next: 1}
	h := userArcHistory(t)
	if err := agent.Run(context.Background(), RunOptions{Persistor: persistor, History: h, ArcID: testArcID}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if invoked {
		t.Error("the auto-rejected tool was invoked")
	}
	call := h.Arcs[0].Responses[0].ToolCalls[0]
	if call.Status != history.ToolCallStatusRefused || call.Result != "denied by policy" {
		t.Errorf("call = %s %#v, want REFUSED with the policy answer", call.Status, call.Result)
	}
	if h.Arcs[0].Status() != history.ArcStatusDone {
		t.Errorf("arc = %s, want DONE", h.Arcs[0].Status())
	}
}

// An arc-wide interrupt carries no verdict, so a rejection in BeforeInterrupt's
// auto-answer is ignored rather than failing the call: the interrupt is raised
// and answered as a plain answer.
func TestBeforeInterruptAutoRejectArcWideIsIgnored(t *testing.T) {
	llm := &scriptedLLM{replies: []*LLMResponse{
		toolCallReply("call_1", "ask_user", map[string]any{"questions": []any{}}),
		finalReply("ok"),
	}}
	agent, err := NewAgent(&AgentConfig{}, NewToolRegistry(), llm,
		WithInterrupts([]InterruptSpec{askUserSpec}),
		WithHooks(Hooks{
			BeforeInterrupt: func(context.Context, InterruptRequest) (InterruptChanges, error) {
				return InterruptChanges{AutoAnswer: &InterruptAnswer{IsRejected: true}}, nil
			},
		}))
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	persistor := &fakePersistor{next: 1}
	h := userArcHistory(t)
	if err := agent.Run(context.Background(), RunOptions{Persistor: persistor, History: h, ArcID: testArcID}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if interrupts := persistedOfType(persistor, events.InterruptEventType); len(interrupts) != 1 {
		t.Errorf("persisted %d INTERRUPT events, want 1 (the verdict is ignored, not failed)", len(interrupts))
	}
	responses := persistedOfType(persistor, events.InterruptResponseEventType)
	if len(responses) != 1 || responses[0].Payload.(events.InterruptResponsePayload).IsRejected {
		t.Errorf("responses = %+v, want one auto-answer with IsRejected=false", responses)
	}
	call := h.Arcs[0].Responses[0].ToolCalls[0]
	if call.Status != history.ToolCallStatusSuccess {
		t.Errorf("call = %s, want SUCCESS (the interrupt was raised and answered)", call.Status)
	}
}

// capturingLLM records the prompt, tools and transcript of each request on
// top of the scripted replies.
type capturingLLM struct {
	*scriptedLLM
	prompts     []string
	tools       [][]string
	transcripts []*history.ModelTranscript
}

func (l *capturingLLM) StreamResponse(ctx context.Context, t *history.ModelTranscript, opts *LLMOptions, sink common.Sink[RawToken]) (*LLMResponse, error) {
	if opts != nil {
		l.prompts = append(l.prompts, opts.SystemPrompt)
		var names []string
		for _, tool := range opts.Tools.All() {
			names = append(names, tool.Name)
		}
		l.tools = append(l.tools, names)
	}
	l.transcripts = append(l.transcripts, t)
	return l.scriptedLLM.StreamResponse(ctx, t, opts, sink)
}

// BeforeAssistant sees the request and may reshape it: the prompt and the
// offered tool list are what the hook changed, and the change never reaches
// the log.
func TestBeforeAssistantReshapesRequest(t *testing.T) {
	registry := parallelRegistry(t, true, map[string]mcp.ToolHandler{
		"read_a": func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) { return textResult("a"), nil },
		"write_b": func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) { return textResult("b"), nil },
	})
	llm := &capturingLLM{scriptedLLM: &scriptedLLM{replies: []*LLMResponse{finalReply("done")}}}
	newPrompt := "policy prompt"
	agent, err := NewAgent(&AgentConfig{SystemPrompt: "base prompt"}, registry, llm, WithHooks(Hooks{
		BeforeAssistant: func(_ context.Context, req AssistantRequest) (AssistantRequestChanges, error) {
			if req.ArcID != testArcID || req.EstimatedTokens <= 0 || len(req.Transcript.Messages) == 0 {
				t.Errorf("hook saw request %+v (transcript %d messages)", req, len(req.Transcript.Messages))
			}
			var kept []*mcp.Tool
			for _, tool := range req.Tools {
				if tool.Name != "write_b" {
					kept = append(kept, tool)
				}
			}
			return AssistantRequestChanges{SystemPrompt: &newPrompt, Tools: &kept}, nil
		},
	}))
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	persistor := &fakePersistor{next: 1}
	h := userArcHistory(t)
	if err := agent.Run(context.Background(), RunOptions{Persistor: persistor, History: h, ArcID: testArcID}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(llm.prompts) != 1 || llm.prompts[0] != "policy prompt" {
		t.Errorf("prompts = %v, want the overridden prompt", llm.prompts)
	}
	if len(llm.tools) != 1 || len(llm.tools[0]) != 1 || llm.tools[0][0] != "read_a" {
		t.Errorf("offered tools = %v, want only read_a", llm.tools)
	}
	// The log holds the user's message and the answer, nothing about prompts.
	if messages := persistedOfType(persistor, events.AssistantMessageEventType); len(messages) != 1 {
		t.Errorf("persisted %d ASSISTANT_MESSAGE events, want 1", len(messages))
	}
}

// A BeforeAssistant Reply short-circuits the model: the hook's reply is
// recorded as the answer and the model is never called.
func TestBeforeAssistantShortCircuits(t *testing.T) {
	llm := &scriptedLLM{}
	agent, err := NewAgent(&AgentConfig{}, NewToolRegistry(), llm, WithHooks(Hooks{
		BeforeAssistant: func(context.Context, AssistantRequest) (AssistantRequestChanges, error) {
			return AssistantRequestChanges{Reply: finalReply("answered by policy")}, nil
		},
	}))
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	persistor := &fakePersistor{next: 1}
	h := userArcHistory(t)
	if err := agent.Run(context.Background(), RunOptions{Persistor: persistor, History: h, ArcID: testArcID}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if llm.calls != 0 {
		t.Errorf("the model was called %d times, want 0", llm.calls)
	}
	messages := persistedOfType(persistor, events.AssistantMessageEventType)
	if len(messages) != 1 || messages[0].Payload.(events.AssistantMessagePayload).Content != "answered by policy" {
		t.Errorf("messages = %+v, want the hook's reply recorded", messages)
	}
	if h.Arcs[0].Status() != history.ArcStatusDone {
		t.Errorf("arc = %s, want DONE", h.Arcs[0].Status())
	}
}

// AfterAssistant rewrites the reply before it is persisted: content changes,
// and a tool call list set to empty drops the calls so the arc closes.
func TestAfterAssistantRewritesReply(t *testing.T) {
	registry := registryWith(t, "list_pods", func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return textResult("2 pending"), nil
	})
	llm := &scriptedLLM{replies: []*LLMResponse{
		toolCallReply("call_1", "list_pods", nil),
	}}
	newContent := "sanitized answer"
	empty := []LLMToolCall{}
	agent, err := NewAgent(&AgentConfig{}, registry, llm, WithHooks(Hooks{
		AfterAssistant: func(_ context.Context, reply AssistantReply) (AssistantReplyChanges, error) {
			if len(reply.ToolCalls) != 1 || reply.MessageID == "" || reply.ArcID != testArcID {
				t.Errorf("hook saw reply %+v", reply)
			}
			return AssistantReplyChanges{Content: &newContent, ToolCalls: &empty}, nil
		},
	}))
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	persistor := &fakePersistor{next: 1}
	h := userArcHistory(t)
	if err := agent.Run(context.Background(), RunOptions{Persistor: persistor, History: h, ArcID: testArcID}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	messages := persistedOfType(persistor, events.AssistantMessageEventType)
	if len(messages) != 1 || messages[0].Payload.(events.AssistantMessagePayload).Content != "sanitized answer" {
		t.Errorf("messages = %+v, want the rewritten content", messages)
	}
	if calls := persistedOfType(persistor, events.ToolCallEventType); len(calls) != 0 {
		t.Errorf("persisted %d TOOL_CALL events, want none (dropped by the hook)", len(calls))
	}
	if h.Arcs[0].Status() != history.ArcStatusDone {
		t.Errorf("arc = %s, want DONE (no calls left to run)", h.Arcs[0].Status())
	}
}

// An AfterAssistant Abort vetoes the reply: the run fails and records a fatal
// error.
func TestAfterAssistantAbort(t *testing.T) {
	llm := &scriptedLLM{replies: []*LLMResponse{finalReply("hi")}}
	agent, err := NewAgent(&AgentConfig{}, NewToolRegistry(), llm, WithHooks(Hooks{
		AfterAssistant: func(context.Context, AssistantReply) (AssistantReplyChanges, error) {
			return AssistantReplyChanges{Abort: errors.New("reply not allowed")}, nil
		},
	}))
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	persistor := &fakePersistor{next: 1}
	h := userArcHistory(t)
	runErr := agent.Run(context.Background(), RunOptions{Persistor: persistor, History: h, ArcID: testArcID})
	if runErr == nil || !strings.Contains(runErr.Error(), "reply not allowed") {
		t.Fatalf("Run error = %v, want the abort", runErr)
	}
	if messages := persistedOfType(persistor, events.AssistantMessageEventType); len(messages) != 0 {
		t.Errorf("persisted %d ASSISTANT_MESSAGE events, want none (vetoed)", len(messages))
	}
	fatal := persistedOfType(persistor, events.ErrorEventType)
	if len(fatal) != 1 || !fatal[0].Payload.(events.ErrorPayload).IsFatal {
		t.Errorf("errors = %+v, want one fatal ERROR", fatal)
	}
}

// BeforeRun can refuse the run outright: a fatal ERROR is written, the model
// is never called, and Run returns nil the way the admission refusal does.
func TestBeforeRunRefuse(t *testing.T) {
	llm := &scriptedLLM{replies: []*LLMResponse{finalReply("never")}}
	agent, err := NewAgent(&AgentConfig{}, NewToolRegistry(), llm, WithHooks(Hooks{
		BeforeRun: func(_ context.Context, view RunView) (RunDecision, error) {
			if view.ArcID != testArcID || view.Content == "" || view.EstimatedTokens <= 0 {
				t.Errorf("hook saw run %+v", view)
			}
			return RunDecision{Refuse: &Refusal{Message: "this session is read-only"}}, nil
		},
	}))
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	persistor := &fakePersistor{next: 1}
	h := userArcHistory(t)
	if err := agent.Run(context.Background(), RunOptions{Persistor: persistor, History: h, ArcID: testArcID}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if llm.calls != 0 {
		t.Errorf("the model was called %d times, want 0", llm.calls)
	}
	fatal := persistedOfType(persistor, events.ErrorEventType)
	if len(fatal) != 1 || fatal[0].Payload.(events.ErrorPayload).Message != "this session is read-only" {
		t.Errorf("errors = %+v, want the refusal", fatal)
	}
}

// AfterRun observes how the run ended, and AfterArcClosed the closed arc.
func TestAfterRunAndAfterArcClosed(t *testing.T) {
	registry := registryWith(t, "list_pods", func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return textResult("2 pending"), nil
	})
	llm := &scriptedLLM{replies: []*LLMResponse{
		toolCallReply("call_1", "list_pods", nil),
		finalReply("two pending"),
	}}
	var runSummaries []RunSummary
	var arcSummaries []ArcSummary
	agent, err := NewAgent(&AgentConfig{}, registry, llm, WithHooks(Hooks{
		AfterRun:       func(_ context.Context, sum RunSummary) { runSummaries = append(runSummaries, sum) },
		AfterArcClosed: func(_ context.Context, sum ArcSummary) { arcSummaries = append(arcSummaries, sum) },
	}))
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	persistor := &fakePersistor{next: 1}
	h := userArcHistory(t)
	if err := agent.Run(context.Background(), RunOptions{Persistor: persistor, History: h, ArcID: testArcID}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(runSummaries) != 1 {
		t.Fatalf("AfterRun fired %d times, want 1", len(runSummaries))
	}
	sum := runSummaries[0]
	if sum.Outcome != RunFinished || sum.Err != nil || sum.Rounds < 3 || sum.Written == 0 || sum.ArcID != testArcID {
		t.Errorf("run summary = %+v, want finished, at least 3 rounds, some writes", sum)
	}
	if len(arcSummaries) != 1 {
		t.Fatalf("AfterArcClosed fired %d times, want 1", len(arcSummaries))
	}
	closed := arcSummaries[0]
	if closed.ArcID != testArcID || closed.Messages != 2 || closed.ToolCalls != 1 || closed.ConclusionMessageID == "" {
		t.Errorf("arc summary = %+v", closed)
	}
}

// AfterRun reports an interrupted run when the run ends holding the arc for a
// human.
func TestAfterRunReportsInterrupted(t *testing.T) {
	registry := registryWith(t, "deploy", func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return textResult("ok"), nil
	})
	llm := &scriptedLLM{replies: []*LLMResponse{toolCallReply("call_1", "deploy", nil)}}
	var outcomes []RunOutcome
	agent, err := NewAgent(&AgentConfig{}, registry, llm, WithHooks(Hooks{
		BeforeToolCall: func(context.Context, ToolCallView) (ToolCallEffects, error) {
			return ToolCallEffects{HookEffects: HookEffects{Ask: &AskRequest{Reason: "deploy?"}}}, nil
		},
		AfterRun: func(_ context.Context, sum RunSummary) { outcomes = append(outcomes, sum.Outcome) },
	}))
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	persistor := &fakePersistor{next: 1}
	h := userArcHistory(t)
	if err := agent.Run(context.Background(), RunOptions{Persistor: persistor, History: h, ArcID: testArcID}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(outcomes) != 1 || outcomes[0] != RunInterrupted {
		t.Errorf("outcomes = %v, want [interrupted]", outcomes)
	}
}

// A fake conversation carrying every event type in the vocabulary, worked
// by the ArcWorker with every hook installed: each hook fires the expected
// number of times — the run's own hooks once per occurrence in the run, the
// continuation hooks once per external event named as a work item's trigger.
func TestEveryHookFiresTheExpectedNumberOfTimes(t *testing.T) {
	ctx := context.Background()
	store := &logStore{}
	actor := users.Actor{UserID: "alice", TenantID: "acme"}
	arc1, arc2 := testArcID, "arc-2"
	taskID := "task-1"
	msg1, msg2 := "msg-1", "msg-2"
	call1, call2 := "call-1", "call-2"
	intID, reqID := "int-1", "req-1"
	cancelReason := "done anyway"

	seed := []*events.Event{
		// A closed arc before the worked one, so its records are in the read.
		{SessionID: testSessionID, ID: arc2, Typ: events.UserMessageEventType, CorrelationID: &arc2, Actor: actor, Payload: events.UserMessagePayload{Content: "old", Actor: actor}},
		{SessionID: testSessionID, ID: msg2, Typ: events.AssistantMessageEventType, CorrelationID: &arc2, Payload: events.AssistantMessagePayload{ModelID: "gpt-5", Content: "old answer"}},
		{SessionID: testSessionID, ID: "close-2", Typ: events.ArcClosedEventType, CorrelationID: &arc2, Payload: events.ArcClosedPayload{CausationID: msg2}},
		// The arc under test, opened here.
		{SessionID: testSessionID, ID: arc1, Typ: events.UserMessageEventType, CorrelationID: &arc1, Actor: actor, Payload: events.UserMessagePayload{Content: "audit the cluster", Actor: actor}},
		{SessionID: testSessionID, ID: msg1, Typ: events.AssistantMessageEventType, CorrelationID: &arc1, Payload: events.AssistantMessagePayload{ModelID: "gpt-5", Content: "starting"}},
		{SessionID: testSessionID, ID: call1, Typ: events.ToolCallEventType, CorrelationID: &arc1, Payload: events.ToolCallPayload{MessageID: msg1, VendorID: "v1", ToolName: "read_a", Args: map[string]any{"x": 1}}},
		{SessionID: testSessionID, ID: "disp-1", Typ: events.ToolDispatchedEventType, CorrelationID: &arc1, Payload: events.ToolDispatchedPayload{CausationID: call1}},
		{SessionID: testSessionID, ID: "res-1", Typ: events.ToolResultEventType, CorrelationID: &arc1, Payload: events.ToolResultPayload{CausationID: call1, Result: "ok"}},
		{SessionID: testSessionID, ID: "tev-1", Typ: events.ToolEventEventType, CorrelationID: &arc1, Payload: events.ToolEventPayload{CausationID: call1, Payload: "halfway"}},
		{SessionID: testSessionID, ID: call2, Typ: events.ToolCallEventType, CorrelationID: &arc1, Payload: events.ToolCallPayload{MessageID: msg1, VendorID: "v2", ToolName: "delete_pod"}},
		{SessionID: testSessionID, ID: "grb-1", Typ: events.GuardrailBlockedEventType, CorrelationID: &arc1, Payload: events.GuardrailBlockedPayload{CausationID: call2, Message: "blocked"}},
		{SessionID: testSessionID, ID: intID, Typ: events.InterruptEventType, CorrelationID: &arc1, Actor: actor, Payload: events.InterruptPayload{Reason: "which cluster?"}},
		{SessionID: testSessionID, ID: "ir-1", Typ: events.InterruptResponseEventType, CorrelationID: &arc1, Actor: actor, Payload: events.InterruptResponsePayload{CausationID: intID, Response: "prod", Actor: actor}},
		{SessionID: testSessionID, ID: taskID, Typ: events.AsyncTaskInvokeEventType, CorrelationID: &arc1, Actor: actor, Payload: events.AsyncTaskInvokePayload{TaskName: "auditor", Actor: actor}},
		{SessionID: testSessionID, ID: "tae-1", Typ: events.AsyncTaskEventEventType, CorrelationID: &taskID, Actor: actor, Payload: events.AsyncTaskEventPayload{CausationID: taskID, Class: "progress", Payload: "50%", LLMVisibility: events.LLMVisibilityInResult}},
		{SessionID: testSessionID, ID: "tir-1", Typ: events.AsyncTaskInputRequestedEventType, CorrelationID: &taskID, Actor: actor, Payload: events.AsyncTaskInputRequestedPayload{CausationID: taskID, Message: "namespace?", LLMVisibility: events.LLMVisibilityNone}},
		{SessionID: testSessionID, ID: "tip-1", Typ: events.AsyncTaskInputProvidedEventType, CorrelationID: &arc1, Actor: actor, Payload: events.AsyncTaskInputProvidedPayload{CausationID: taskID, Message: "kube-system"}},
		{SessionID: testSessionID, ID: "tar-1", Typ: events.AsyncTaskResultEventType, CorrelationID: &taskID, Actor: actor, Payload: events.AsyncTaskResultPayload{CausationID: taskID, Result: "clean", Outcome: events.AsyncTaskOutcomeSuccess, LLMVisibility: events.LLMVisibilityNone}},
		{SessionID: testSessionID, ID: "can-1", Typ: events.CancelEventType, CorrelationID: &taskID, Actor: actor, Payload: events.CancelPayload{TaskID: &taskID, Reason: &cancelReason}},
		{SessionID: testSessionID, ID: "err-1", Typ: events.ErrorEventType, CorrelationID: &arc1, Payload: events.ErrorPayload{Message: "transient"}},
		{SessionID: testSessionID, ID: reqID, Typ: events.CompactionRequestedEventType, Payload: events.CompactionRequestedPayload{Watermark: 0}},
		{SessionID: testSessionID, ID: "sus-1", Typ: events.ArcSuspendedEventType, CorrelationID: &arc1, Payload: events.ArcSuspendedPayload{CausationID: reqID}},
		{SessionID: testSessionID, ID: "cmp-1", Typ: events.CompactedEventType, Payload: events.CompactedPayload{ThroughSeq: 100, Summary: "s", Method: "test"}},
	}
	stored, err := store.Persist(ctx, seed...)
	if err != nil {
		t.Fatalf("seed the log: %v", err)
	}
	seqOf := map[string]int{}
	for _, ev := range stored {
		seqOf[ev.ID] = ev.Seq
	}

	counts := map[string]int{}
	hooks := Hooks{
		BeforeRun: func(context.Context, RunView) (RunDecision, error) {
			counts["BeforeRun"]++
			return RunDecision{Pass: true}, nil
		},
		AfterRun: func(_ context.Context, sum RunSummary) { counts["AfterRun:"+string(sum.Outcome)]++ },
		BeforeAssistant: func(context.Context, AssistantRequest) (AssistantRequestChanges, error) {
			counts["BeforeAssistant"]++
			return AssistantRequestChanges{}, nil
		},
		AfterAssistant: func(context.Context, AssistantReply) (AssistantReplyChanges, error) {
			counts["AfterAssistant"]++
			return AssistantReplyChanges{}, nil
		},
		BeforeToolCall: func(context.Context, ToolCallView) (ToolCallEffects, error) {
			counts["BeforeToolCall"]++
			return ToolCallEffects{HookEffects: HookEffects{Run: true}}, nil
		},
		AfterToolCall: func(context.Context, ToolCallResult) (ToolCallChanges, error) {
			counts["AfterToolCall"]++
			return ToolCallChanges{}, nil
		},
		BeforeTaskStart:  func(context.Context, TaskStartView) (TaskStartEffects, error) { counts["BeforeTaskStart"]++; return TaskStartEffects{HookEffects: HookEffects{Run: true}}, nil },
		BeforeTaskCancel: func(context.Context, TaskCancelView) (TaskCancelEffects, error) { counts["BeforeTaskCancel"]++; return HookEffects{Run: true}, nil },
		BeforeTaskInput:  func(context.Context, TaskInputView) (TaskInputEffects, error) { counts["BeforeTaskInput"]++; return HookEffects{Run: true}, nil },
		AfterTaskResult:  func(context.Context, TaskResultView) { counts["AfterTaskResult"]++ },
		AfterTaskEvent:   func(context.Context, TaskEventView) { counts["AfterTaskEvent"]++ },
		BeforeSkillLoad:  func(context.Context, ToolCallView) (ToolCallEffects, error) { counts["BeforeSkillLoad"]++; return ToolCallEffects{HookEffects: HookEffects{Run: true}}, nil },
		AfterSkillLoad:   func(context.Context, ToolCallResult) (ToolCallChanges, error) { counts["AfterSkillLoad"]++; return ToolCallChanges{}, nil },
		BeforeInterrupt:  func(context.Context, InterruptRequest) (InterruptChanges, error) { counts["BeforeInterrupt"]++; return InterruptChanges{}, nil },
		AfterInterrupt:   func(context.Context, InterruptView) { counts["AfterInterrupt"]++ },
		AfterInterruptResponse: func(context.Context, InterruptResponseView) { counts["AfterInterruptResponse"]++ },
		AfterArcClosed:         func(context.Context, ArcSummary) { counts["AfterArcClosed"]++ },
	}

	registry := registryWith(t, "read_a", func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return textResult("a"), nil
	})
	llm := &scriptedLLM{replies: []*LLMResponse{
		toolCallReply("call_x", "read_a", map[string]any{"x": 2}),
		finalReply("done"),
	}}
	agent, err := NewAgent(nil, registry, llm, WithHooks(hooks))
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	worker, err := NewArcWorker(store, store, NewFixedResolver(agent), nil)
	if err != nil {
		t.Fatalf("NewArcWorker: %v", err)
	}
	if _, err := worker.Work(ctx, testRef(), testAgentID, 0); err != nil {
		t.Fatalf("Work: %v", err)
	}

	// The run asked the model twice (call, then answer) and closed the arc.
	// The item named no trigger, so no continuation hook fired.
	want := map[string]int{
		"BeforeRun":         1,
		"AfterRun:finished": 1,
		"BeforeAssistant":   2,
		"AfterAssistant":    2,
		"BeforeToolCall":    1,
		"AfterToolCall":     1,
		"AfterArcClosed":    1,
	}
	if !reflect.DeepEqual(counts, want) {
		t.Errorf("hook counts = %v, want %v", counts, want)
	}

	// Each seeded external event fires its continuation hook when a work
	// item names it as its trigger — one item per event.
	for _, id := range []string{"tae-1", "tir-1", "tip-1", "tar-1", "ir-1"} {
		if _, err := worker.Work(ctx, testRef(), testAgentID, seqOf[id]); err != nil {
			t.Fatalf("Work with trigger %s: %v", id, err)
		}
	}
	for hook, n := range map[string]int{
		"AfterTaskEvent":         3, // notification, input requested, input provided
		"AfterTaskResult":        1,
		"AfterInterruptResponse": 1,
	} {
		if counts[hook] != n {
			t.Errorf("counts[%s] = %d, want %d", hook, counts[hook], n)
		}
	}
}

// A work item names the one event that caused it, and the continuation hook
// fires for exactly that event — not for the latest of its type in the arc:
// with two responses and two notifications in the log, the item naming the
// first response fires for the first, and the one naming the first
// notification for the first.
func TestTriggerHookFiresForTheNamedEventNotTheTail(t *testing.T) {
	ctx := context.Background()
	store := &logStore{}
	actor := users.Actor{UserID: "alice", TenantID: "acme"}
	arc1 := testArcID
	taskID := "task-1"

	seed := []*events.Event{
		{SessionID: testSessionID, ID: arc1, Typ: events.UserMessageEventType, CorrelationID: &arc1, Actor: actor, Payload: events.UserMessagePayload{Content: "go", Actor: actor}},
		{SessionID: testSessionID, ID: "int-1", Typ: events.InterruptEventType, CorrelationID: &arc1, Actor: actor, Payload: events.InterruptPayload{Reason: "first?"}},
		{SessionID: testSessionID, ID: "ir-1", Typ: events.InterruptResponseEventType, CorrelationID: &arc1, Actor: actor, Payload: events.InterruptResponsePayload{CausationID: "int-1", Response: "one", Actor: actor}},
		{SessionID: testSessionID, ID: "int-2", Typ: events.InterruptEventType, CorrelationID: &arc1, Actor: actor, Payload: events.InterruptPayload{Reason: "second?"}},
		{SessionID: testSessionID, ID: "ir-2", Typ: events.InterruptResponseEventType, CorrelationID: &arc1, Actor: actor, Payload: events.InterruptResponsePayload{CausationID: "int-2", Response: "two", Actor: actor}},
		{SessionID: testSessionID, ID: taskID, Typ: events.AsyncTaskInvokeEventType, CorrelationID: &arc1, Actor: actor, Payload: events.AsyncTaskInvokePayload{TaskName: "auditor", Actor: actor}},
		{SessionID: testSessionID, ID: "tae-1", Typ: events.AsyncTaskEventEventType, CorrelationID: &taskID, Actor: actor, Payload: events.AsyncTaskEventPayload{CausationID: taskID, Class: "progress", Payload: "10%", LLMVisibility: events.LLMVisibilityInResult}},
		{SessionID: testSessionID, ID: "tae-2", Typ: events.AsyncTaskEventEventType, CorrelationID: &taskID, Actor: actor, Payload: events.AsyncTaskEventPayload{CausationID: taskID, Class: "progress", Payload: "90%", LLMVisibility: events.LLMVisibilityInResult}},
	}
	stored, err := store.Persist(ctx, seed...)
	if err != nil {
		t.Fatalf("seed the log: %v", err)
	}
	seqOf := map[string]int{}
	for _, ev := range stored {
		seqOf[ev.ID] = ev.Seq
	}

	var responses []InterruptResponseView
	var taskEvents []TaskEventView
	hooks := Hooks{
		AfterInterruptResponse: func(_ context.Context, v InterruptResponseView) { responses = append(responses, v) },
		AfterTaskEvent:         func(_ context.Context, v TaskEventView) { taskEvents = append(taskEvents, v) },
	}
	llm := &scriptedLLM{replies: []*LLMResponse{finalReply("done"), finalReply("done")}}
	agent, err := NewAgent(nil, NewToolRegistry(), llm, WithHooks(hooks))
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	worker, err := NewArcWorker(store, store, NewFixedResolver(agent), nil)
	if err != nil {
		t.Fatalf("NewArcWorker: %v", err)
	}
	// The item naming the FIRST notification fires for it, not for tae-2.
	if _, err := worker.Work(ctx, testRef(), testAgentID, seqOf["tae-1"]); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if len(taskEvents) != 1 || taskEvents[0].Seq != seqOf["tae-1"] || taskEvents[0].Payload != "10%" {
		t.Errorf("AfterTaskEvent views = %+v, want one for tae-1 (10%%)", taskEvents)
	}
	// The item naming the FIRST response fires for it, not for ir-2.
	if _, err := worker.Work(ctx, testRef(), testAgentID, seqOf["ir-1"]); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if len(responses) != 1 || responses[0].InterruptID != "int-1" || responses[0].Seq != seqOf["ir-1"] {
		t.Errorf("AfterInterruptResponse views = %+v, want one for int-1 at seq %d", responses, seqOf["ir-1"])
	}
}

// A trigger at or below a snapshot's seq is resolved from the fold itself —
// the snapshot keeps the hooked events by seq (a task's FinishedSeq and
// Events entries, an interrupt's ResolvedSeq) — so the worker does not
// re-read the whole log for it. A trigger above the snapshot fires from the
// batch.
func TestTriggerHookResolvesFromTheFoldUnderASnapshot(t *testing.T) {
	ctx := context.Background()
	store := &logStore{}
	actor := users.Actor{UserID: "alice", TenantID: "acme"}
	arc1 := testArcID
	taskID := "task-1"

	seed := []*events.Event{
		{SessionID: testSessionID, ID: arc1, Typ: events.UserMessageEventType, CorrelationID: &arc1, Actor: actor, Payload: events.UserMessagePayload{Content: "go", Actor: actor}},
		{SessionID: testSessionID, ID: "int-1", Typ: events.InterruptEventType, CorrelationID: &arc1, Actor: actor, Payload: events.InterruptPayload{Reason: "which?"}},
		{SessionID: testSessionID, ID: "ir-1", Typ: events.InterruptResponseEventType, CorrelationID: &arc1, Actor: actor, Payload: events.InterruptResponsePayload{CausationID: "int-1", Response: "prod", Actor: actor}},
		{SessionID: testSessionID, ID: taskID, Typ: events.AsyncTaskInvokeEventType, CorrelationID: &arc1, Actor: actor, Payload: events.AsyncTaskInvokePayload{TaskName: "auditor", Actor: actor}},
		{SessionID: testSessionID, ID: "tae-1", Typ: events.AsyncTaskEventEventType, CorrelationID: &taskID, Actor: actor, Payload: events.AsyncTaskEventPayload{CausationID: taskID, Class: "progress", Payload: "10%", LLMVisibility: events.LLMVisibilityInResult}},
		{SessionID: testSessionID, ID: "tar-1", Typ: events.AsyncTaskResultEventType, CorrelationID: &taskID, Actor: actor, Payload: events.AsyncTaskResultPayload{CausationID: taskID, Result: "clean", Outcome: events.AsyncTaskOutcomeSuccess, LLMVisibility: events.LLMVisibilityNone}},
	}
	stored, err := store.Persist(ctx, seed...)
	if err != nil {
		t.Fatalf("seed the log: %v", err)
	}
	seqOf := map[string]int{}
	for _, ev := range stored {
		seqOf[ev.ID] = ev.Seq
	}

	var taskEvents []TaskEventView
	var results []TaskResultView
	var responses []InterruptResponseView
	hooks := Hooks{
		AfterTaskEvent:         func(_ context.Context, v TaskEventView) { taskEvents = append(taskEvents, v) },
		AfterTaskResult:        func(_ context.Context, v TaskResultView) { results = append(results, v) },
		AfterInterruptResponse: func(_ context.Context, v InterruptResponseView) { responses = append(responses, v) },
	}
	llm := &scriptedLLM{replies: []*LLMResponse{finalReply("done")}}
	agent, err := NewAgent(nil, NewToolRegistry(), llm, WithHooks(hooks))
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	worker, err := NewArcWorker(store, store, NewFixedResolver(agent), nil, WithSnapshotPolicy(SnapshotPolicy{Enabled: true}))
	if err != nil {
		t.Fatalf("NewArcWorker: %v", err)
	}

	// The first item names no trigger: the run closes the arc and the save
	// point stores a snapshot at the log's head, above every seeded event.
	if _, err := worker.Work(ctx, testRef(), testAgentID, 0); err != nil {
		t.Fatalf("first Work: %v", err)
	}
	if store.snap == nil {
		t.Fatal("no snapshot saved at the first work item's save point")
	}
	if store.snap.Seq() < seqOf["tar-1"] {
		t.Fatalf("snapshot at seq %d, want above tar-1 at %d", store.snap.Seq(), seqOf["tar-1"])
	}

	// Each seeded trigger is below the snapshot, so each fires from the fold.
	for _, id := range []string{"tae-1", "tar-1", "ir-1"} {
		if _, err := worker.Work(ctx, testRef(), testAgentID, seqOf[id]); err != nil {
			t.Fatalf("Work with trigger %s: %v", id, err)
		}
	}
	if len(taskEvents) != 1 || taskEvents[0].Seq != seqOf["tae-1"] || taskEvents[0].Class != "progress" {
		t.Errorf("AfterTaskEvent views = %+v, want one for tae-1 from the fold", taskEvents)
	}
	if len(results) != 1 || results[0].Seq != seqOf["tar-1"] || results[0].TaskID != taskID || results[0].Result != "clean" {
		t.Errorf("AfterTaskResult views = %+v, want one for tar-1 from the fold", results)
	}
	if len(responses) != 1 || responses[0].InterruptID != "int-1" || responses[0].Seq != seqOf["ir-1"] || responses[0].Response != "prod" {
		t.Errorf("AfterInterruptResponse views = %+v, want one for ir-1 from the fold", responses)
	}

	// A notification landing above the snapshot fires from the batch.
	tae2, err := store.Persist(ctx, &events.Event{
		SessionID: testSessionID, ID: "tae-2", Typ: events.AsyncTaskEventEventType, CorrelationID: &taskID, Actor: actor,
		Payload: events.AsyncTaskEventPayload{CausationID: taskID, Class: "progress", Payload: "90%", LLMVisibility: events.LLMVisibilityInResult},
	})
	if err != nil {
		t.Fatalf("persist tae-2: %v", err)
	}
	if _, err := worker.Work(ctx, testRef(), testAgentID, tae2[0].Seq); err != nil {
		t.Fatalf("Work with trigger tae-2: %v", err)
	}
	if len(taskEvents) != 2 || taskEvents[1].Seq != tae2[0].Seq {
		t.Errorf("AfterTaskEvent views = %+v, want tae-2 from the batch second", taskEvents)
	}
}

// A trigger a snapshot swallowed without its fold carrying it — here an
// interrupt response on one arc, while the snapshot was saved working
// another — gets one full read, and fires from the batch then.
func TestTriggerSwallowedByAnotherArcsSnapshotGetsAFullRead(t *testing.T) {
	ctx := context.Background()
	store := &logStore{}
	actor := users.Actor{UserID: "alice", TenantID: "acme"}
	arc1, arc2 := testArcID, "arc-2"

	seed := []*events.Event{
		{SessionID: testSessionID, ID: arc1, Typ: events.UserMessageEventType, CorrelationID: &arc1, Actor: actor, Payload: events.UserMessagePayload{Content: "first", Actor: actor}},
		{SessionID: testSessionID, ID: arc2, Typ: events.UserMessageEventType, CorrelationID: &arc2, Actor: actor, Payload: events.UserMessagePayload{Content: "second", Actor: actor}},
		{SessionID: testSessionID, ID: "int-1", Typ: events.InterruptEventType, CorrelationID: &arc1, Actor: actor, Payload: events.InterruptPayload{Reason: "which?"}},
		{SessionID: testSessionID, ID: "ir-1", Typ: events.InterruptResponseEventType, CorrelationID: &arc1, Actor: actor, Payload: events.InterruptResponsePayload{CausationID: "int-1", Response: "prod", Actor: actor}},
	}
	stored, err := store.Persist(ctx, seed...)
	if err != nil {
		t.Fatalf("seed the log: %v", err)
	}
	irSeq := 0
	for _, ev := range stored {
		if ev.ID == "ir-1" {
			irSeq = ev.Seq
		}
	}

	var responses []InterruptResponseView
	hooks := Hooks{AfterInterruptResponse: func(_ context.Context, v InterruptResponseView) { responses = append(responses, v) }}
	llm := &scriptedLLM{replies: []*LLMResponse{finalReply("two done"), finalReply("one done")}}
	agent, err := NewAgent(nil, NewToolRegistry(), llm, WithHooks(hooks))
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	worker, err := NewArcWorker(store, store, NewFixedResolver(agent), nil, WithSnapshotPolicy(SnapshotPolicy{Enabled: true}))
	if err != nil {
		t.Fatalf("NewArcWorker: %v", err)
	}

	// Work the OTHER arc: its run closes it and its save point stores a
	// snapshot whose fold does not carry arc-1's interrupt response, since
	// the response correlates to arc-1, above arc-2's opening.
	arc2Ref := common.ArcRef{SessionID: testSessionID, ArcID: arc2}
	if _, err := worker.Work(ctx, arc2Ref, testAgentID, 0); err != nil {
		t.Fatalf("Work on arc-2: %v", err)
	}
	if store.snap == nil || store.snap.Seq() < irSeq {
		t.Fatalf("no snapshot above the response (seq %d)", irSeq)
	}

	// The item for arc-1 naming the response: the snapshot read finds
	// neither the event nor its fold record, so the worker re-reads the
	// whole log once and fires from the batch.
	if _, err := worker.Work(ctx, testRef(), testAgentID, irSeq); err != nil {
		t.Fatalf("Work with trigger ir-1: %v", err)
	}
	if len(responses) != 1 || responses[0].InterruptID != "int-1" || responses[0].Seq != irSeq || responses[0].Response != "prod" {
		t.Errorf("AfterInterruptResponse views = %+v, want one for ir-1 after the full read", responses)
	}
}

// DecideInterruptResponse is what an answering side calls before persisting:
