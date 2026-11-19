package agent

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"fillbrunn.ai/kube-agent/proto/pkg/events"
	"fillbrunn.ai/kube-agent/agent/pkg/history"
	"fillbrunn.ai/kube-agent/proto/pkg/users"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// fakeController records what it was asked to do and answers with whatever it
// was told to. It holds no idea of what is running, which is the point: a
// controller is asked to do things and never asked how a task is doing.
type fakeController struct {
	cancelled []TaskRef
	inputs    []string
	requests  []string
	refs      []TaskRef
	failure   error
}

func (c *fakeController) Cancel(_ context.Context, ref TaskRef, _ string) error {
	if c.failure != nil {
		return c.failure
	}
	c.cancelled = append(c.cancelled, ref)
	return nil
}

func (c *fakeController) ProvideInput(_ context.Context, ref TaskRef, requestID, input string) error {
	if c.failure != nil {
		return c.failure
	}
	c.refs = append(c.refs, ref)
	c.requests = append(c.requests, requestID)
	c.inputs = append(c.inputs, input)
	return nil
}

// taskRegistryWith returns a registry holding one kind of task.
func taskRegistryWith(t *testing.T, name string, start Starter) *TaskRegistry {
	t.Helper()
	registry := NewTaskRegistry()
	if err := registry.RegisterTask(TaskMetadata{Name: name}, start); err != nil {
		t.Fatalf("register task: %v", err)
	}
	return registry
}

func toolNames(tools []*mcp.Tool) []string {
	names := make([]string, 0, len(tools))
	for _, tool := range tools {
		names = append(names, tool.Name)
	}
	return names
}

// N kinds of task are N+2 tools: one to start each, and one each to stop and to
// answer whichever is running. The two are addressed by ID, so there is no
// reason to have one per kind.
func TestARegistryOffersOneToolPerTaskPlusTwo(t *testing.T) {
	registry := NewTaskRegistry()
	for _, name := range []string{"auditor", "migrator"} {
		if err := registry.RegisterTask(TaskMetadata{Name: name}, func(context.Context, TaskRef, any) error { return nil }); err != nil {
			t.Fatalf("register task: %v", err)
		}
	}

	want := []string{"auditor", "migrator", CancelTaskTool, ProvideTaskInputTool}
	if got := toolNames(taskToolSet{registry}.All()); !slices.Equal(got, want) {
		t.Errorf("tools = %v, want %v", got, want)
	}
}

// With nothing to start there is nothing to stop or answer, so an empty
// registry costs the model no tools at all.
func TestAnEmptyRegistryOffersNothing(t *testing.T) {
	if tools := (taskToolSet{NewTaskRegistry()}).All(); len(tools) != 0 {
		t.Errorf("tools = %v, want none", toolNames(tools))
	}
	if tools := (taskToolSet{(*TaskRegistry)(nil)}).All(); len(tools) != 0 {
		t.Errorf("nil registry tools = %v, want none", toolNames(tools))
	}
}

func TestATaskCannotTakeTheNameOfTheToolsThatAddressTasks(t *testing.T) {
	registry := NewTaskRegistry()
	err := registry.RegisterTask(TaskMetadata{Name: CancelTaskTool}, func(context.Context, TaskRef, any) error { return nil })
	if err == nil {
		t.Fatal("a task was registered under the name of the tool that cancels tasks")
	}
}

func TestATaskNeedsAWayToStart(t *testing.T) {
	if err := NewTaskRegistry().RegisterTask(TaskMetadata{Name: "auditor"}, nil); err == nil {
		t.Fatal("a task with no starter was registered")
	}
}

// The model picks a tool by name and the agent routes by the same name, so one
// name meaning two things is a call that reaches whichever is checked first.
func TestANameCannotBeBothAToolAndATask(t *testing.T) {
	tools := registryWith(t, "auditor", nil)
	tasks := taskRegistryWith(t, "auditor", func(context.Context, TaskRef, any) error { return nil })

	_, err := NewAgent(nil, tools, &scriptedLLM{}, WithTasks(tasks, &fakeController{}))
	if err == nil {
		t.Fatal("an agent was built with one name registered as both")
	}
}

// A registry with nobody to reach the tasks it starts is half an agent.
func TestTasksNeedAController(t *testing.T) {
	tasks := taskRegistryWith(t, "auditor", func(context.Context, TaskRef, any) error { return nil })
	if _, err := NewAgent(nil, nil, &scriptedLLM{}, WithTasks(tasks, nil)); err == nil {
		t.Fatal("an agent was built with tasks it could never stop or answer")
	}
}

// The model is given one list, and it holds both what it can call and what it
// can start.
func TestTheModelIsOfferedToolsAndTasksTogether(t *testing.T) {
	tools := registryWith(t, "list_pods", nil)
	tasks := taskRegistryWith(t, "auditor", func(context.Context, TaskRef, any) error { return nil })

	agent, err := NewAgent(nil, tools, &scriptedLLM{}, WithTasks(tasks, &fakeController{}))
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}

	want := []string{"list_pods", "auditor", CancelTaskTool, ProvideTaskInputTool}
	if got := toolNames(CombineTools(agent.tools, taskToolSet{agent.tasks}).All()); !slices.Equal(got, want) {
		t.Errorf("tools = %v, want %v", got, want)
	}
}

// An agent with no tasks offers what it always offered, and every name is an
// ordinary tool's.
func TestAnAgentWithoutTasksIsUnchanged(t *testing.T) {
	tools := registryWith(t, "list_pods", nil)
	agent, err := NewAgent(nil, tools, &scriptedLLM{})
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}

	if got := toolNames(CombineTools(agent.tools, taskToolSet{agent.tasks}).All()); !slices.Equal(got, []string{"list_pods"}) {
		t.Errorf("tools = %v, want the one registered tool", got)
	}
	if classify(agent.tasks, CancelTaskTool) != opTool {
		t.Error("a name that addresses tasks routed somewhere with no tasks to address")
	}
}

func TestClassifyNamesTheOperation(t *testing.T) {
	tasks := taskRegistryWith(t, "auditor", func(context.Context, TaskRef, any) error { return nil })

	for name, want := range map[string]taskOperation{
		"auditor":            opStart,
		CancelTaskTool:       opCancel,
		ProvideTaskInputTool: opProvideInput,
		"list_pods":          opTool,
	} {
		if got := classify(tasks, name); got != want {
			t.Errorf("classify(%s) = %v, want %v", name, got, want)
		}
	}
}

var errUnreachable = errors.New("the runner is not answering")

// taskConversation is a log in which an arc started a task and a later arc is
// about to address it: the shape every cancel and every answer arrives in,
// since the task's own arc has already had its turn.
//
// waiting says whether the task stopped to ask for something.
func taskConversation(t *testing.T, waiting bool) *history.History {
	t.Helper()
	first, second := testArcID, "arc-2"
	actor := users.Actor{UserID: "alice", TenantID: "acme"}
	at := time.Unix(0, 0)

	log := []events.Event{
		{ID: first, Seq: 0, Timestamp: at, Typ: events.UserMessageEventType, CorrelationID: &first, Actor: actor,
			Payload: events.UserMessagePayload{Content: "audit the cluster", Actor: actor}},
		{ID: "msg-1", Seq: 1, Timestamp: at, Typ: events.AssistantMessageEventType, CorrelationID: &first,
			Payload: events.AssistantMessagePayload{ModelID: "gpt-5", Content: "starting the audit"}},
		{ID: "task-1", Seq: 2, Timestamp: at, Typ: events.AsyncTaskInvokeEventType, CorrelationID: &first, Actor: actor,
			Payload: events.AsyncTaskInvokePayload{TaskName: "auditor", Actor: actor}},
	}
	if waiting {
		// An ask is always AsMessage, so it opens an arc of its own, which is
		// where the answer is later recorded.
		askArc := "arc-ask"
		log = append(log, events.Event{ID: "ask-1", Seq: 3, Timestamp: at, Typ: events.AsyncTaskInputRequestedEventType, CorrelationID: &askArc, Actor: actor,
			Payload: events.AsyncTaskInputRequestedPayload{CausationID: "task-1", Message: "which namespace?", LLMVisibility: events.LLMVisibilityAsMessage}})
	}
	log = append(log, events.Event{ID: second, Seq: 4, Timestamp: at, Typ: events.UserMessageEventType, CorrelationID: &second, Actor: actor,
		Payload: events.UserMessagePayload{Content: "kube-system", Actor: actor}})

	h, err := history.Replay("session", log)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	return h
}

// runCallInArc drives one turn of the named arc in which the model makes the
// given call, then says it is done.
func runCallInArc(t *testing.T, h *history.History, arcID string, tasks *TaskRegistry, controller TaskController, call LLMToolCall) *fakePersistor {
	t.Helper()
	persistor := &fakePersistor{next: 10}
	agent, err := NewAgent(nil, nil, &scriptedLLM{replies: []*LLMResponse{
		{ModelID: "gpt-5", ToolCalls: []LLMToolCall{call}},
		{ModelID: "gpt-5", Content: "done"},
	}}, WithTasks(tasks, controller))
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	if err := agent.Run(context.Background(), RunOptions{Persistor: persistor, History: h, ArcID: arcID}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	return persistor
}

// lastCallIn returns the last tool call the named arc requested.
func lastCallIn(t *testing.T, h *history.History, arcID string) *history.ToolCall {
	t.Helper()
	for _, arc := range h.Arcs {
		if arc.ID != arcID {
			continue
		}
		for _, response := range slices.Backward(arc.Responses) {
			if len(response.ToolCalls) > 0 {
				return response.ToolCalls[len(response.ToolCalls)-1]
			}
		}
	}
	t.Fatalf("no tool call in arc %s", arcID)
	return nil
}

func resultText(t *testing.T, call *history.ToolCall) string {
	t.Helper()
	text, ok := call.Result.(string)
	if !ok {
		t.Fatalf("result = %#v, want text", call.Result)
	}
	return text
}

// The invocation is written before the task is started, because the record is
// what names the task and that name is what the task reports back under.
func TestStartingATaskNamesItBeforeItRuns(t *testing.T) {
	var started TaskRef
	tasks := taskRegistryWith(t, "auditor", func(_ context.Context, ref TaskRef, _ any) error {
		started = ref
		return nil
	})

	h := userArcHistory(t)
	persistor := runCallInArc(t, h, testArcID, tasks, &fakeController{}, LLMToolCall{
		VendorID: "call_1", ToolName: "auditor", Args: map[string]any{"namespace": "kube-system"}})

	want := "ASSISTANT_MESSAGE, TOOL_CALL, TOOL_DISPATCHED, ASYNC_TASK_INVOKE, TOOL_RESULT, ASSISTANT_MESSAGE, ARC_CLOSED"
	if got := typesOf(persistor.persisted); got != want {
		t.Fatalf("wrote %s, want %s", got, want)
	}

	task := h.Tasks[started.ID]
	if task == nil {
		t.Fatalf("no task %q in the session's task table", started.ID)
	}
	if started.SessionID != testSessionID {
		t.Errorf("ref = %#v, want it to name the session, not an arc", started)
	}
	// The invoke records the actor that started the task, and the task table
	// carries it forward for every event the task later emits.
	if task.Actor == nil || task.Actor.UserID != "alice" {
		t.Errorf("task actor = %v, want alice, whose message opened the arc", task.Actor)
	}
	for _, ev := range persistor.persisted {
		if ev.Typ == events.AsyncTaskInvokeEventType && ev.Actor.UserID != "alice" {
			t.Errorf("invoke event actor = %v, want alice", ev.Actor)
		}
	}

	// The model is told the id, because it is the only way it can answer the
	// task or stop it later.
	if text := resultText(t, lastCallIn(t, h, testArcID)); !strings.Contains(text, task.ID) {
		t.Errorf("result = %q, want it to carry the task id", text)
	}
}

// The start result tells the model both ways to answer a task's question:
// directly through the input tool, or by putting the question to the user
// first and then passing the answer on.
func TestTheStartResultOffersBothWaysToAnswerAnAsk(t *testing.T) {
	tasks := taskRegistryWith(t, "auditor", func(context.Context, TaskRef, any) error { return nil })
	h := userArcHistory(t)
	runCallInArc(t, h, testArcID, tasks, &fakeController{}, LLMToolCall{VendorID: "call_1", ToolName: "auditor"})

	text := resultText(t, lastCallIn(t, h, testArcID))
	for _, want := range []string{"put the question to the user first", ProvideTaskInputTool, CancelTaskTool} {
		if !strings.Contains(text, want) {
			t.Errorf("start result = %q, want it to mention %q", text, want)
		}
	}
}

// AC3: a running task does not hold its arc open. The arc closes on the last
// response that requested no tool calls, while the task runs on as an object
// of the session.
func TestAStartedTaskDoesNotHoldTheArcOpen(t *testing.T) {
	tasks := taskRegistryWith(t, "auditor", func(context.Context, TaskRef, any) error { return nil })
	h := userArcHistory(t)
	persistor := runCallInArc(t, h, testArcID, tasks, &fakeController{}, LLMToolCall{VendorID: "call_1", ToolName: "auditor"})

	if status := h.Arcs[0].Status(); status != history.ArcStatusDone {
		t.Errorf("arc is %s, want it closed while its task runs on", status)
	}
	if got := typesOf(persistor.persisted); !strings.Contains(got, "ARC_CLOSED") {
		t.Errorf("wrote %s, want the close (KX) recorded", got)
	}
	tasks2 := h.TasksInOrder()
	if len(tasks2) != 1 || tasks2[0].Status != history.AsyncTaskPending {
		t.Errorf("tasks = %+v, want the one started task still pending", tasks2)
	}
}

// A launch that fails may still have taken hold, so the task is settled and the
// text says the effect is unknown rather than claiming nothing happened.
func TestATaskThatCouldNotBeStartedIsSettledAsUnknown(t *testing.T) {
	tasks := taskRegistryWith(t, "auditor", func(context.Context, TaskRef, any) error { return errUnreachable })
	h := userArcHistory(t)
	persistor := runCallInArc(t, h, testArcID, tasks, &fakeController{}, LLMToolCall{VendorID: "call_1", ToolName: "auditor"})

	want := "ASSISTANT_MESSAGE, TOOL_CALL, TOOL_DISPATCHED, ASYNC_TASK_INVOKE, ASYNC_TASK_RESULT, TOOL_RESULT, ASSISTANT_MESSAGE, ARC_CLOSED"
	if got := typesOf(persistor.persisted); got != want {
		t.Fatalf("wrote %s, want %s", got, want)
	}
	if task := h.TasksInOrder()[0]; task.Status != history.AsyncTaskFailed {
		t.Errorf("task is %s, want it settled", task.Status)
	}
	if text := resultText(t, lastCallIn(t, h, testArcID)); !strings.Contains(text, "not known") {
		t.Errorf("result = %q, want it to say the effect is unknown", text)
	}
}

// A cancel naming a task is written as a CANCEL event whose correlation is the
// task ID — which is what settles the task in the log, with who and when — and
// then reaches the runner through the controller. What became of work already
// under way is the runner's to report.
func TestCancellingATaskWritesACancelAgainstTheTask(t *testing.T) {
	tasks := taskRegistryWith(t, "auditor", func(context.Context, TaskRef, any) error { return nil })
	controller := &fakeController{}
	h := taskConversation(t, false)

	persistor := runCallInArc(t, h, "arc-2", tasks, controller, LLMToolCall{
		VendorID: "call_2", ToolName: CancelTaskTool,
		Args: map[string]any{"task_id": "task-1", "reason": "no longer needed"}})

	want := "ASSISTANT_MESSAGE, TOOL_CALL, TOOL_DISPATCHED, CANCEL, TOOL_RESULT, ASSISTANT_MESSAGE, ARC_CLOSED"
	if got := typesOf(persistor.persisted); got != want {
		t.Fatalf("wrote %s, want %s", got, want)
	}
	if len(controller.cancelled) != 1 || controller.cancelled[0].ID != "task-1" {
		t.Fatalf("cancelled = %#v, want the one task named", controller.cancelled)
	}
	if got := controller.cancelled[0].SessionID; got != testSessionID {
		t.Errorf("cancelled session = %q, want %q", got, testSessionID)
	}

	// The CANCEL correlates to the task and names it in the payload, and the
	// fold settles the task from it — actor and seq included.
	cancel := persistor.persisted[3]
	if cancel.Typ != events.CancelEventType || cancel.CorrelationID == nil || *cancel.CorrelationID != "task-1" {
		t.Fatalf("cancel event = %+v, want correlation task-1", cancel)
	}
	if payload, ok := cancel.Payload.(events.CancelPayload); !ok || payload.TaskID == nil || *payload.TaskID != "task-1" {
		t.Errorf("cancel payload = %#v, want TaskID task-1", cancel.Payload)
	}
	if cancel.Actor.UserID != "alice" {
		t.Errorf("cancel actor = %v, want the arc's actor", cancel.Actor)
	}
	task := h.Tasks["task-1"]
	if task == nil || task.Status != history.AsyncTaskCancelled {
		t.Fatalf("task = %+v, want it settled cancelled", task)
	}
	if task.CancelledBy == nil || task.CancelledBy.UserID != "alice" {
		t.Errorf("cancelled by %v, want alice", task.CancelledBy)
	}
	if task.FinishedSeq == nil || *task.FinishedSeq != cancel.Seq {
		t.Errorf("finished seq = %v, want the cancel's seq %d", task.FinishedSeq, cancel.Seq)
	}
}

// Input reaches the task before the log says it did, and the record lands on
// the arc the task's question opened rather than the one the call was made in —
// that arc is the one waiting on the answer, and the cutoff includes the record
// there at any point.
func TestInputIsGivenToTheTaskBeforeItIsRecorded(t *testing.T) {
	tasks := taskRegistryWith(t, "auditor", func(context.Context, TaskRef, any) error { return nil })
	controller := &fakeController{}
	h := taskConversation(t, true)

	persistor := runCallInArc(t, h, "arc-2", tasks, controller, LLMToolCall{
		VendorID: "call_2", ToolName: ProvideTaskInputTool,
		Args: map[string]any{"task_id": "task-1", "input": "kube-system"}})

	want := "ASSISTANT_MESSAGE, TOOL_CALL, TOOL_DISPATCHED, ASYNC_TASK_INPUT_PROVIDED, TOOL_RESULT, ASSISTANT_MESSAGE, ARC_CLOSED"
	if got := typesOf(persistor.persisted); got != want {
		t.Fatalf("wrote %s, want %s", got, want)
	}
	if len(controller.inputs) != 1 || controller.inputs[0] != "kube-system" {
		t.Fatalf("inputs = %#v, want the one answer", controller.inputs)
	}
	// The answer names the question, so a task that has moved on can refuse it.
	if len(controller.requests) != 1 || controller.requests[0] != "ask-1" {
		t.Errorf("requests = %#v, want the question the task is stopped at", controller.requests)
	}

	recorded := persistor.persisted[3]
	if recorded.CorrelationID == nil || *recorded.CorrelationID != "arc-ask" {
		t.Errorf("the record landed on %v, want the arc the ask opened", recorded.CorrelationID)
	}
	// The answer is given under the actor the invoke recorded.
	if recorded.Actor.UserID != "alice" {
		t.Errorf("record actor = %v, want the task's actor alice", recorded.Actor)
	}
	if status := h.Tasks["task-1"].Status; status != history.AsyncTaskPending {
		t.Errorf("task is %s, want it running again", status)
	}
}

// A controller that could not reach the task must not leave the log saying the
// input arrived: the task would be waiting on an answer everyone believes it
// has.
func TestInputThatNeverReachedTheTaskIsNotRecorded(t *testing.T) {
	tasks := taskRegistryWith(t, "auditor", func(context.Context, TaskRef, any) error { return nil })
	controller := &fakeController{failure: errUnreachable}
	h := taskConversation(t, true)

	persistor := runCallInArc(t, h, "arc-2", tasks, controller, LLMToolCall{
		VendorID: "call_2", ToolName: ProvideTaskInputTool,
		Args: map[string]any{"task_id": "task-1", "input": "kube-system"}})

	if got := typesOf(persistor.persisted); strings.Contains(got, "ASYNC_TASK_INPUT_PROVIDED") {
		t.Fatalf("wrote %s, want no record of input the task never got", got)
	}
	if status := h.Tasks["task-1"].Status; status != history.AsyncTaskWaiting {
		t.Errorf("task is %s, want it still waiting", status)
	}
	if text := resultText(t, lastCallIn(t, h, "arc-2")); !strings.Contains(text, "did not take the input") {
		t.Errorf("result = %q, want it to say the input did not land", text)
	}
}

// A call naming a task this conversation cannot see is answered rather than
// failing the run. The model made a mistake it can correct.
func TestAnsweringAnUnknownTaskIsAnErrorTheModelCanRead(t *testing.T) {
	tasks := taskRegistryWith(t, "auditor", func(context.Context, TaskRef, any) error { return nil })
	controller := &fakeController{}
	h := taskConversation(t, true)

	runCallInArc(t, h, "arc-2", tasks, controller, LLMToolCall{
		VendorID: "call_2", ToolName: ProvideTaskInputTool,
		Args: map[string]any{"task_id": "made-up", "input": "kube-system"}})

	call := lastCallIn(t, h, "arc-2")
	if call.Status != history.ToolCallStatusFailed {
		t.Errorf("call is %s, want it settled as failed", call.Status)
	}
	if text := resultText(t, call); !strings.Contains(text, "made-up") {
		t.Errorf("result = %q, want it to name the task that was not found", text)
	}
	if len(controller.inputs) != 0 {
		t.Errorf("inputs = %#v, want the controller left alone", controller.inputs)
	}
}

// A question the run has not been given is one it cannot answer, whether the
// task has yet to ask it or asked it after this run read the log.
func TestAnsweringATaskThatIsNotWaitingIsRefused(t *testing.T) {
	tasks := taskRegistryWith(t, "auditor", func(context.Context, TaskRef, any) error { return nil })
	controller := &fakeController{}
	h := taskConversation(t, false)

	runCallInArc(t, h, "arc-2", tasks, controller, LLMToolCall{
		VendorID: "call_2", ToolName: ProvideTaskInputTool,
		Args: map[string]any{"task_id": "task-1", "input": "kube-system"}})

	if text := resultText(t, lastCallIn(t, h, "arc-2")); !strings.Contains(text, "asked nothing you have been given") {
		t.Errorf("result = %q, want it to say there was no question to answer", text)
	}
	if len(controller.inputs) != 0 {
		t.Errorf("inputs = %#v, want the controller left alone", controller.inputs)
	}
}

// Stopping something that already stopped is not a stop, and saying so as an
// error is what stops the model reporting that it cancelled the work.
func TestCancellingAFinishedTaskSaysSo(t *testing.T) {
	tasks := taskRegistryWith(t, "auditor", func(context.Context, TaskRef, any) error { return nil })
	controller := &fakeController{}
	h := taskConversation(t, false)
	settleTask(t, h, "task-1")

	runCallInArc(t, h, "arc-2", tasks, controller, LLMToolCall{
		VendorID: "call_2", ToolName: CancelTaskTool, Args: map[string]any{"task_id": "task-1"}})

	call := lastCallIn(t, h, "arc-2")
	if call.Status != history.ToolCallStatusFailed {
		t.Errorf("call is %s, want it settled as failed", call.Status)
	}
	if text := resultText(t, call); !strings.Contains(text, "nothing to stop") {
		t.Errorf("result = %q, want it to say there was nothing to stop", text)
	}
	if len(controller.cancelled) != 0 {
		t.Errorf("cancelled = %#v, want the controller left alone", controller.cancelled)
	}
}

// settleTask folds a completion into a history that is already built.
func settleTask(t *testing.T, h *history.History, taskID string) {
	t.Helper()
	ev := &events.Event{
		ID: "result-" + taskID, Seq: 5, Timestamp: time.Unix(0, 0),
		Typ: events.AsyncTaskResultEventType, CorrelationID: ptr(taskID),
		Payload: events.AsyncTaskResultPayload{
			CausationID: taskID, Result: "14 findings", Outcome: events.AsyncTaskOutcomeSuccess,
			LLMVisibility: events.LLMVisibilityNone},
	}
	if err := h.Update(ev, history.FoldStateFromHistory(h)); err != nil {
		t.Fatalf("fold: %v", err)
	}
}
