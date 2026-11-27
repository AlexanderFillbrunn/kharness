package agent

import (
	"context"
	"errors"
	"fillbrunn.ai/kube-agent/proto/pkg/common"
	"fmt"
	"sync"
	"testing"
	"time"

	"fillbrunn.ai/kube-agent/proto/pkg/events"
	"fillbrunn.ai/kube-agent/proto/pkg/history"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// cancelledArcs reports a cancel for the arcs it was told about, and for the
// tasks it was told about.
type cancelledArcs struct {
	mu    sync.Mutex
	arcs  map[string]*events.Event
	tasks map[string]*events.Event
	err   error
	asks  int
}

func (c *cancelledArcs) Cancelled(_ context.Context, ref common.ArcRef) (*events.Event, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.asks++
	if c.err != nil {
		return nil, c.err
	}
	return c.arcs[ref.ArcID], nil
}

func (c *cancelledArcs) CancelledTask(_ context.Context, _ string, taskID string) (*events.Event, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return nil, c.err
	}
	return c.tasks[taskID], nil
}

// cancelOf is the record a cancel leaves, as a store would hand it back.
func cancelOf(arcID string) *events.Event {
	reason := "the user stopped it"
	return &events.Event{
		ID: "cancel-1", Typ: events.CancelEventType, CorrelationID: &arcID,
		Payload: events.CancelPayload{Reason: &reason},
	}
}

// runGuarded runs one arc whose model asks for a tool, against a guard that
// already knows the arc was cancelled.
func runGuarded(t *testing.T, cancels *cancelledArcs) (*history.History, *fakePersistor, error) {
	t.Helper()
	var ran bool
	registry := registryWith(t, "list_pods", func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		ran = true
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "2 pending"}}}, nil
	})

	persistor := &fakePersistor{next: 1}
	guarded := common.GuardCancellation(persistor, cancels)
	agent, err := NewAgent(nil, registry, &scriptedLLM{replies: []*LLMResponse{
		{ModelID: "gpt-5", ToolCalls: []LLMToolCall{{VendorID: "call_abc", ToolName: "list_pods"}}},
		{ModelID: "gpt-5", Content: "two are pending"},
	}})
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}

	h := userArcHistory(t)
	runErr := agent.Run(context.Background(), RunOptions{Persistor: guarded, History: h, ArcID: testArcID})
	if ran && cancels.arcs[testArcID] != nil {
		t.Error("the tool ran for a cancelled arc")
	}
	return h, persistor, runErr
}

// The guard stops the run before the tool, and hands back the cancel so the
// arc the run is reading is settled rather than left looking open.
func TestTheGuardStopsARunOnACancelledArc(t *testing.T) {
	h, persistor, err := runGuarded(t, &cancelledArcs{arcs: map[string]*events.Event{testArcID: cancelOf(testArcID)}})
	if !errors.Is(err, common.ErrCancelled) {
		t.Fatalf("Run returned %v, want it to report the cancellation", err)
	}

	// The cancel is not folded into the running history: it was recorded
	// before the events this run has written since, and the arc reads it in
	// the right place the next time it is loaded.
	if typesOf(persistor.persisted) != "ASSISTANT_MESSAGE, TOOL_CALL" {
		t.Errorf("wrote %s, want the reply and its request and nothing after", typesOf(persistor.persisted))
	}

	// A cancelled run is not a failed one, and the cancel already says how the
	// arc ended.
	if len(h.Arcs[0].Errors) != 0 {
		t.Errorf("a cancelled run recorded %d failures", len(h.Arcs[0].Errors))
	}
}

// An uncancelled arc is untouched: every write goes through and the run
// finishes.
func TestTheGuardLetsALiveArcThrough(t *testing.T) {
	h, persistor, err := runGuarded(t, &cancelledArcs{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if h.Arcs[0].Status() != history.ArcStatusDone {
		t.Errorf("arc is %s, want it closed", h.Arcs[0].Status())
	}
	if want := "ASSISTANT_MESSAGE, TOOL_CALL, TOOL_DISPATCHED, TOOL_RESULT, ASSISTANT_MESSAGE, ARC_CLOSED"; typesOf(persistor.persisted) != want {
		t.Errorf("wrote %s, want %s", typesOf(persistor.persisted), want)
	}
}

// A guard that cannot answer must not halt work that is otherwise fine. The
// cancel is still in the log and the run stops at a later step.
func TestAGuardThatCannotAnswerFailsOpen(t *testing.T) {
	_, _, err := runGuarded(t, &cancelledArcs{err: errors.New("the cancel store is down")})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
}

// Only a write that is about to start work is worth stopping. A reply and the
// calls it requests cost nothing further, so they are never asked about.
func TestTheGuardOnlyAsksAboutWritesThatStartWork(t *testing.T) {
	cancels := &cancelledArcs{}
	guarded := common.GuardCancellation(&fakePersistor{next: 1}, cancels)
	arc := testArcID

	for _, typ := range []events.EventType{
		events.AssistantMessageEventType,
		events.ToolCallEventType,
		events.ToolResultEventType,
		events.ArcClosedEventType,
	} {
		if _, err := guarded.Persist(context.Background(), &events.Event{
			SessionID: testSessionID, Typ: typ, CorrelationID: &arc,
		}); err != nil {
			t.Fatalf("Persist(%s): %v", typ, err)
		}
	}
	if cancels.asks != 0 {
		t.Errorf("the guard asked about %d writes that start nothing", cancels.asks)
	}

	if _, err := guarded.Persist(context.Background(), &events.Event{
		SessionID: testSessionID, Typ: events.ToolDispatchedEventType, CorrelationID: &arc,
	}); err != nil {
		t.Fatalf("Persist(dispatch): %v", err)
	}
	if cancels.asks != 1 {
		t.Errorf("the guard asked %d times about a dispatch, want once", cancels.asks)
	}
}

// The guard holds nothing between writes. It used to remember the arcs it had
// refused, which raced between the runs sharing one persistor and saved about
// one read, since a refusal ends the run at the first dispatch it reaches.
func TestTheGuardRemembersNothingBetweenWrites(t *testing.T) {
	cancels := &cancelledArcs{arcs: map[string]*events.Event{testArcID: cancelOf(testArcID)}}
	guarded := common.GuardCancellation(&fakePersistor{next: 1}, cancels)
	arc := testArcID

	for range 3 {
		_, err := guarded.Persist(context.Background(), &events.Event{
			SessionID: testSessionID, Typ: events.ToolDispatchedEventType, CorrelationID: &arc,
		})
		if !errors.Is(err, common.ErrCancelled) {
			t.Fatalf("Persist returned %v, want every dispatch refused", err)
		}
	}
	if cancels.asks != 3 {
		t.Errorf("the guard asked %d times, want it to read the cancel afresh each time", cancels.asks)
	}
}

// One persistor serves every run in a process, so arcs being worked at once
// write through the same guard. Half of these are cancelled and half are not,
// which is what makes the guard take both paths at once. Run with -race, which
// is what this is here for.
func TestTheGuardServesConcurrentRuns(t *testing.T) {
	const arcs = 8
	cancels := &cancelledArcs{arcs: map[string]*events.Event{}}
	for i := range arcs {
		if i%2 == 0 {
			arc := fmt.Sprintf("arc-%d", i)
			cancels.arcs[arc] = cancelOf(arc)
		}
	}
	guarded := common.GuardCancellation(&fakePersistor{next: 1}, cancels)

	var wg sync.WaitGroup
	for i := range arcs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			arc := fmt.Sprintf("arc-%d", i)
			for range 8 {
				_, err := guarded.Persist(context.Background(), &events.Event{
					SessionID: testSessionID, Typ: events.ToolDispatchedEventType, CorrelationID: &arc,
				})
				if refused := errors.Is(err, common.ErrCancelled); refused != (i%2 == 0) {
					t.Errorf("arc %s: Persist returned %v, want refused=%v", arc, err, i%2 == 0)
					return
				}
			}
		}()
	}
	wg.Wait()
}

// recordingController is a common.TaskController that remembers what it was asked to
// stop, and can refuse.
type recordingController struct {
	mu      sync.Mutex
	stopped []common.TaskRef
	reasons []string
	err     error
}

func (c *recordingController) Cancel(_ context.Context, ref common.TaskRef, reason string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return c.err
	}
	c.stopped = append(c.stopped, ref)
	c.reasons = append(c.reasons, reason)
	return nil
}

func (c *recordingController) ProvideInput(context.Context, common.TaskRef, string, string) error {
	return nil
}

func (c *recordingController) Start(context.Context, string, common.TaskRef, any) (*common.TaskLaunch, error) {
	return nil, nil
}

// cancelledTaskArc is an arc with a task still running and a cancel recorded
// against it: what the log holds when somebody withdrew an arc whose task is
// off doing something.
func cancelledTaskArc(t *testing.T, reason string) *logStore {
	t.Helper()
	store := runningTaskStore(t)
	arc := testArcID
	if _, err := store.Persist(context.Background(), &events.Event{
		SessionID: testSessionID, Typ: events.CancelEventType, CorrelationID: &arc,
		Payload: events.CancelPayload{Reason: &reason},
	}); err != nil {
		t.Fatalf("persist the cancel: %v", err)
	}
	return store
}

// workerOver builds a worker whose agent can start and reach tasks.
func workerOver(t *testing.T, store *logStore, controller common.TaskController, options ...ArcWorkerOption) *ArcWorker {
	t.Helper()
	tasks := taskSourceWith(t, controller, "auditor")
	agent, err := NewAgent(nil, nil, &scriptedLLM{}, WithTasks(tasks))
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	worker, err := NewArcWorker(store, store, NewFixedResolver(agent), nil, append(options, WithTaskController(tasks))...)
	if err != nil {
		t.Fatalf("NewArcWorker: %v", err)
	}
	return worker
}

// Cancelling an arc does not reach its tasks from the worker: "cancel every
// task this arc started" is the harness's loop over the task table, not a
// mechanism in the run. Working the cancelled arc writes nothing and stops
// nothing.
func TestWorkingACancelledArcLeavesItsTasksToTheHarness(t *testing.T) {
	store := cancelledTaskArc(t, "changed my mind")
	controller := &recordingController{}

	worked, err := workerOver(t, store, controller).Work(context.Background(), testRef(), testAgentID, 0)
	if err != nil {
		t.Fatalf("Work: %v", err)
	}
	if !worked {
		t.Fatal("the arc was not worked")
	}

	if len(controller.stopped) != 0 {
		t.Errorf("stopped %+v, want the worker to leave the task to the harness", controller.stopped)
	}
	want := "USER_MESSAGE, ASSISTANT_MESSAGE, ASYNC_TASK_INVOKE, CANCEL"
	if got := store.types(); got != want {
		t.Errorf("log = %s, want %s", got, want)
	}
}

// taskCancelOf is the record a task-addressed cancel leaves: correlation ID
// the task ID, and the payload naming it too.
func taskCancelOf(taskID, reason string) *events.Event {
	corr := taskID
	payload := events.CancelPayload{TaskID: &taskID}
	if reason != "" {
		payload.Reason = &reason
	}
	return &events.Event{
		ID: "cancel-" + taskID, Typ: events.CancelEventType, CorrelationID: &corr,
		Payload: payload,
	}
}

// A write whose causation reaches a cancelled task is refused like a write that
// starts work on a cancelled arc: the whole batch, common.ErrCancelled, nothing
// written. A late report from a cancelled task's runner is exactly this.
func TestTheGuardRefusesAWritingAboutACancelledTask(t *testing.T) {
	cancels := &cancelledArcs{tasks: map[string]*events.Event{"task-1": taskCancelOf("task-1", "not needed")}}
	inner := &fakePersistor{next: 1}
	guarded := common.GuardCancellation(inner, cancels)
	arc := testArcID

	batch := []*events.Event{
		{SessionID: testSessionID, Typ: events.AssistantMessageEventType, CorrelationID: &arc,
			Payload: events.AssistantMessagePayload{ModelID: "gpt-5", Content: "wrapping up"}},
		{SessionID: testSessionID, Typ: events.AsyncTaskResultEventType, CorrelationID: ptr("task-1"),
			Payload: events.AsyncTaskResultPayload{
				CausationID: "task-1", Result: "too late", Outcome: events.AsyncTaskOutcomeSuccess,
				LLMVisibility: events.LLMVisibilityNone}},
	}
	if _, err := guarded.Persist(context.Background(), batch...); !errors.Is(err, common.ErrCancelled) {
		t.Fatalf("Persist returned %v, want the whole batch refused", err)
	}
	if len(inner.persisted) != 0 {
		t.Errorf("wrote %s, want nothing", typesOf(inner.persisted))
	}

	// A batch that reaches no task is untouched by the task axis.
	if _, err := guarded.Persist(context.Background(), &events.Event{
		SessionID: testSessionID, Typ: events.AssistantMessageEventType, CorrelationID: &arc,
		Payload: events.AssistantMessagePayload{ModelID: "gpt-5", Content: "all good"},
	}); err != nil {
		t.Fatalf("Persist: %v", err)
	}
}

// An arc cancelled with nothing running is the cheap no-op the queue deals in.
func TestWorkingACancelledArcWithNoTasksDoesNothing(t *testing.T) {
	store := cancelledTaskArc(t, "")
	// Settle the task with None visibility, correlated to the task, so the arc
	// is cancelled with nothing left to reach.
	if _, err := store.Persist(context.Background(), &events.Event{
		SessionID: testSessionID, Typ: events.AsyncTaskResultEventType, CorrelationID: ptr("task-1"),
		Payload: events.AsyncTaskResultPayload{
			CausationID: "task-1", Result: "done", Outcome: events.AsyncTaskOutcomeSuccess,
			LLMVisibility: events.LLMVisibilityNone},
	}); err != nil {
		t.Fatalf("persist: %v", err)
	}
	controller := &recordingController{}

	if _, err := workerOver(t, store, controller).Work(context.Background(), testRef(), testAgentID, 0); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if len(controller.stopped) != 0 {
		t.Errorf("stopped %+v, want nothing", controller.stopped)
	}
}

// openArcStore is an arc with nothing but the user's message in it.
func openArcStore(t *testing.T) *logStore {
	t.Helper()
	store := &logStore{}
	if _, err := store.Persist(context.Background(), &events.Event{
		SessionID: testSessionID, Typ: events.UserMessageEventType, CorrelationID: ptr(testArcID),
		Payload: events.UserMessagePayload{Content: "wait for the rollout"},
	}); err != nil {
		t.Fatalf("persist: %v", err)
	}
	store.events[0].ID = testArcID
	return store
}

// The guard cannot stop a tool that is already running: it refuses the next
// dispatch, which is one tool too late. The watcher is the half that reaches
// inside, and the tool's own account of what it did still reaches the log.
func TestTheWatcherStopsAToolAlreadyRunning(t *testing.T) {
	cancels := NewLocalCancels()
	running := make(chan struct{})
	registry := registryWith(t, "wait", func(ctx context.Context, _ *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		close(running)
		<-ctx.Done()
		return &mcp.CallToolResult{
			IsError: true,
			Content: []mcp.Content{&mcp.TextContent{Text: "stopped before the rollout finished"}},
		}, nil
	})

	store := openArcStore(t)
	chat, err := NewAgent(nil, registry, &scriptedLLM{replies: []*LLMResponse{
		{ModelID: "gpt-5", ToolCalls: []LLMToolCall{{VendorID: "call_abc", ToolName: "wait"}}},
	}})
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	worker, err := NewArcWorker(store, store, NewFixedResolver(chat), nil, WithCancelWatcher(cancels))
	if err != nil {
		t.Fatalf("NewArcWorker: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := worker.Work(context.Background(), testRef(), testAgentID, 0)
		done <- err
	}()

	<-running
	if err := cancels.NotifyCancel(context.Background(), testRef()); err != nil {
		t.Fatalf("NotifyCancel: %v", err)
	}

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Work returned %v, want the cancellation", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the tool was never stopped")
	}

	want := "USER_MESSAGE, ASSISTANT_MESSAGE, TOOL_CALL, TOOL_DISPATCHED, TOOL_RESULT"
	if got := store.types(); got != want {
		t.Fatalf("log = %s, want %s", got, want)
	}
}

// Watching an arc that is cancelled later and one that is not must not be
// confused, or a cancel stops a run on a different arc.
func TestNotifyingOneArcLeavesAnotherAlone(t *testing.T) {
	cancels := NewLocalCancels()
	mine, stopMine := cancels.Watch(context.Background(), testRef())
	defer stopMine()
	theirs, stopTheirs := cancels.Watch(context.Background(), common.ArcRef{SessionID: testSessionID, ArcID: "arc-2"})
	defer stopTheirs()

	if err := cancels.NotifyCancel(context.Background(), testRef()); err != nil {
		t.Fatalf("NotifyCancel: %v", err)
	}

	select {
	case <-mine:
	case <-time.After(time.Second):
		t.Error("the cancelled arc's watcher was not told")
	}
	select {
	case <-theirs:
		t.Error("another arc's watcher was told")
	default:
	}
}

// A watcher that stops after the cancel must not close a channel twice, and
// one that stops before it must not be told at all.
func TestStoppingAWatchIsSafeEitherSideOfTheCancel(t *testing.T) {
	cancels := NewLocalCancels()

	_, stopFirst := cancels.Watch(context.Background(), testRef())
	if err := cancels.NotifyCancel(context.Background(), testRef()); err != nil {
		t.Fatalf("NotifyCancel: %v", err)
	}
	stopFirst()

	left, stopSecond := cancels.Watch(context.Background(), testRef())
	stopSecond()
	if err := cancels.NotifyCancel(context.Background(), testRef()); err != nil {
		t.Fatalf("NotifyCancel: %v", err)
	}
	select {
	case <-left:
		t.Error("a watch that was stopped was still told")
	default:
	}
}
