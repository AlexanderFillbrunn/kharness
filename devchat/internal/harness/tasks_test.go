package harness

import (
	"fillbrunn.ai/kube-agent/proto/pkg/common"
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fillbrunn.ai/kube-agent/devchat/internal/filestore"
	"fillbrunn.ai/kube-agent/agent/pkg/agent"
	"fillbrunn.ai/kube-agent/ingress/pkg/ingress"
	"fillbrunn.ai/kube-agent/proto/pkg/events"
	"fillbrunn.ai/kube-agent/proto/pkg/history"
	"fillbrunn.ai/kube-agent/proto/pkg/users"
)

// scriptLLM answers with a canned reply per turn. The order is the order the
// agent asks in, which is what the test is checking as much as anything.
type scriptLLM struct {
	replies []*agent.LLMResponse
	calls   int
}

func (l *scriptLLM) StreamResponse(_ context.Context, _ *history.ModelTranscript, _ *agent.LLMOptions, _ common.Sink[agent.RawToken]) (*agent.LLMResponse, error) {
	if l.calls >= len(l.replies) {
		return &agent.LLMResponse{ModelID: "test", Content: "(nothing scripted)"}, nil
	}
	reply := l.replies[l.calls]
	l.calls++
	return reply, nil
}

func (l *scriptLLM) Response(ctx context.Context, t *history.ModelTranscript, opts *agent.LLMOptions) (*agent.LLMResponse, error) {
	return l.StreamResponse(ctx, t, opts, nil)
}

// harness is devchat's wiring with a scripted model in place of a real one.
type harness struct {
	store   *filestore.Store
	session *session
	reports *ingress.TaskReports
	runner  *localTasks
	llm     *scriptLLM
}

func newHarness(t *testing.T, replies ...*agent.LLMResponse) *harness {
	t.Helper()
	// Long enough that nothing finishes before the test looks, short enough
	// that a whole audit fits in a test run.
	auditDelay = 5 * time.Millisecond
	t.Cleanup(func() { auditDelay = 4 * time.Second })

	path := filepath.Join(t.TempDir(), "session.jsonl")
	store, err := filestore.Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	runner, err := newLocalTasks()
	if err != nil {
		t.Fatalf("newLocalTasks: %v", err)
	}

	llm := &scriptLLM{replies: replies}
	chat, err := agent.NewAgent(nil, agent.NewToolRegistry(), llm, agent.WithTasks(runner))
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}

	persistor := common.MintIdentity(common.GuardCancellation(store, store))
	cancels := agent.NewLocalCancels()
	worker, err := agent.NewArcWorker(persistor, store, agent.NewFixedResolver(chat), agent.NewLocalLease(),
		agent.WithCancelWatcher(cancels),
		agent.WithTaskController(runner))
	if err != nil {
		t.Fatalf("NewArcWorker: %v", err)
	}
	reports, err := ingress.NewTaskReports(persistor, store, runner, workEnqueuer{worker})
	if err != nil {
		t.Fatalf("NewTaskReports: %v", err)
	}
	runner.attach(reports, localAgentID, nil)

	return &harness{
		store:   store,
		reports: reports,
		runner:  runner,
		llm:     llm,
		session: &session{
			id:         path,
			store:      store,
			persistor:  persistor,
			worker:     worker,
			cancels:    cancels,
			controller: runner,
			actor:      users.Actor{UserID: "dev", TenantID: "dev"},
		},
	}
}

// workEnqueuer is the work queue in a test: an arc a report opened is worked
// straight away, the way the queue serves it.
type workEnqueuer struct{ worker *agent.ArcWorker }

func (e workEnqueuer) Enqueue(ctx context.Context, ref common.ArcRef, agentID string, triggerSeq int) error {
	_, _ = e.worker.Work(ctx, ref, agentID, triggerSeq)
	return nil
}

// waitFor polls the log until it satisfies the condition, because a task runs
// on its own and reports when it is ready.
func (h *harness) waitFor(t *testing.T, what string, done func(*history.History) bool) *history.History {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		folded, err := h.session.fullHistory(context.Background())
		if err != nil {
			t.Fatalf("read log: %v", err)
		}
		if done(folded) {
			return folded
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s; log holds %s", what, logTypes(t, h))
		}
		time.Sleep(time.Millisecond)
	}
}

func logTypes(t *testing.T, h *harness) string {
	t.Helper()
	stored, err := h.store.Events(context.Background())
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	var names []string
	for _, ev := range stored {
		names = append(names, string(ev.Typ))
	}
	return strings.Join(names, ", ")
}

func firstTask(h *history.History) *history.AsyncTask {
	tasks := h.TasksInOrder()
	if len(tasks) == 0 {
		return nil
	}
	return tasks[0]
}

// The whole round trip under the session-task model: the model starts a task
// and its arc closes on the reply that started it; the task asks a question,
// which opens an arc of its own that the model works; the user answers in
// another arc, and the answer is recorded in the ask's arc; the task's result
// opens a third arc, which the model works to report what came of the work.
//
// Every report goes through the ingress rather than into the log directly, so
// what this exercises is the path a task running anywhere else would take.
func TestATaskAsksTheUserAndFinishes(t *testing.T) {
	h := newHarness(t,
		// The first turn starts the audit and says so.
		&agent.LLMResponse{ModelID: "test", ToolCalls: []agent.LLMToolCall{
			{VendorID: "call_1", ToolName: "audit_namespaces", Args: map[string]any{"depth": "full"}}}},
		&agent.LLMResponse{ModelID: "test", Content: "the audit is running"},
		// Woken by the task's question, it puts that question to the user.
		&agent.LLMResponse{ModelID: "test", Content: "which namespace should I look at?"},
		// The user answers in an arc of their own, and it is handed on.
		&agent.LLMResponse{ModelID: "test", ToolCalls: []agent.LLMToolCall{
			{VendorID: "call_2", ToolName: events.ProvideTaskInputToolName,
				Args: map[string]any{"task_id": "", "input": "kube-system"}}}},
		&agent.LLMResponse{ModelID: "test", Content: "passed it on"},
		// Woken by the result, it reports what came of the work.
		&agent.LLMResponse{ModelID: "test", Content: "the audit found two problems"},
	)

	ctx := context.Background()
	first := h.session.newArcID()
	if err := h.session.turn(ctx, first, "audit the cluster"); err != nil {
		t.Fatalf("first turn: %v", err)
	}

	// D1: the arc closed on its last reply even though the task it started is
	// still running — the task is an object of the session, not of the arc.
	folded, err := h.session.fullHistory(ctx)
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	if status := folded.Arcs[0].Status(); status != history.ArcStatusDone {
		t.Fatalf("arc is %s, want it closed while its task runs on", status)
	}
	// The task may already have stopped to ask — the fake audit pauses only
	// milliseconds here — so anything unfinished proves it outlived the arc.
	if task := firstTask(folded); task == nil || task.FinishedSeq != nil {
		t.Fatalf("task = %+v, want the one started task unfinished", firstTask(folded))
	}

	// The task asks a question. The ask is AsMessage, so it opens an arc of its
	// own and that arc is served as a work item — the origin arc is not
	// re-worked, and nothing is owed.
	folded = h.waitFor(t, "the task to stop and ask", func(folded *history.History) bool {
		task := firstTask(folded)
		return task != nil && task.Status == history.AsyncTaskWaiting && task.WaitingArc != nil
	})
	task := firstTask(folded)
	askArc := *task.WaitingArc
	var ask *history.Arc
	for _, arc := range folded.Arcs {
		if arc.ID == askArc {
			ask = arc
		}
	}
	if ask == nil {
		t.Fatalf("no arc %s for the ask; arcs are %d", askArc, len(folded.Arcs))
	}
	if !strings.Contains(ask.Content, "asks a question") || !strings.Contains(ask.Content, "which of kube-system") {
		t.Errorf("ask arc content = %q, want the task note and the question", ask.Content)
	}
	if ask.OpenedByTask != task.ID {
		t.Errorf("ask arc opened by %q, want the task %s", ask.OpenedByTask, task.ID)
	}
	// The arc was worked: the model put the question to the user in it.
	folded = h.waitFor(t, "the question to be put to the user", func(folded *history.History) bool {
		for _, arc := range folded.Arcs {
			if arc.ID == askArc && len(arc.Responses) > 0 {
				return true
			}
		}
		return false
	})
	for _, arc := range folded.Arcs {
		if arc.ID == askArc && len(arc.Responses) > 0 {
			if last := arc.Responses[len(arc.Responses)-1].Content; !strings.Contains(last, "which namespace") {
				t.Errorf("the ask arc's last reply is %q, want the task's question put to the user", last)
			}
		}
	}

	// The user answers. It opens an arc of its own, which is why the task's ID
	// has to reach the model as data rather than as position.
	h.scriptAnswer(task.ID)
	second := h.session.newArcID()
	if err := h.session.turn(ctx, second, "kube-system"); err != nil {
		t.Fatalf("second turn: %v", err)
	}

	// AC4: the answer is recorded in the ask's own arc, so the cutoff puts it
	// in that arc's read at any point.
	h.waitFor(t, "the answer to be recorded", func(folded *history.History) bool {
		return strings.Contains(logTypes(t, h), string(events.AsyncTaskInputProvidedEventType))
	})
	_, askEvents, err := h.store.GetEventsForArc(ctx, h.session.ref(askArc), common.ReadOptions{})
	if err != nil {
		t.Fatalf("read the ask arc: %v", err)
	}
	var sawAnswer bool
	for _, ev := range askEvents {
		if ev.Typ == events.AsyncTaskInputProvidedEventType {
			sawAnswer = true
			if ev.CorrelationID == nil || *ev.CorrelationID != askArc {
				t.Errorf("the answer is correlated to %v, want the ask's arc %s", ev.CorrelationID, askArc)
			}
		}
	}
	if !sawAnswer {
		t.Errorf("the ask arc's read holds no ASYNC_TASK_INPUT_PROVIDED; got %s", logTypes(t, h))
	}

	// The task finishes with the answer in it, and its result opens another
	// arc, which is worked to report the findings.
	folded = h.waitFor(t, "the audit to finish", func(folded *history.History) bool {
		task := firstTask(folded)
		return task != nil && task.FinishedSeq != nil
	})
	task = firstTask(folded)
	if task.Status != history.AsyncTaskSuccess {
		t.Fatalf("task is %s, want it finished", task.Status)
	}
	if result, _ := task.Result.(string); !strings.Contains(result, "kube-system") {
		t.Errorf("result = %q, want the audit to have used the answer it was given", result)
	}
	if task.ResultArc == "" {
		t.Fatal("the result opened no arc")
	}
	folded = h.waitFor(t, "the result arc to be worked", func(folded *history.History) bool {
		for _, arc := range folded.Arcs {
			if arc.ID == task.ResultArc && len(arc.Responses) > 0 {
				return true
			}
		}
		return false
	})
	for _, arc := range folded.Arcs {
		if arc.ID != task.ResultArc {
			continue
		}
		if !strings.Contains(arc.Content, "returned results") {
			t.Errorf("result arc content = %q, want the task note and the result", arc.Content)
		}
		if result, _ := task.Result.(string); !strings.Contains(arc.Content, result) {
			t.Errorf("result arc content = %q, want the task's result %q carried whole", arc.Content, result)
		}
		if last := arc.Responses[len(arc.Responses)-1].Content; !strings.Contains(last, "two problems") {
			t.Errorf("the result arc's last reply is %q, want it to report what the task found", last)
		}
	}
}

// scriptAnswer fills in the task ID the model could not know when the replies
// were written, which is the same thing the real model does from the notes.
func (h *harness) scriptAnswer(taskID string) {
	for _, reply := range h.llm.replies {
		for i := range reply.ToolCalls {
			if reply.ToolCalls[i].ToolName != events.ProvideTaskInputToolName {
				continue
			}
			args := reply.ToolCalls[i].Args.(map[string]any)
			args["task_id"] = taskID
		}
	}
}

// Cancelling an arc stops every task it started: the harness loops over the
// fold's task table and writes a task-addressed CANCEL for each, which is what
// settles the task in the log and reaches the runner. The runner's later
// report about a settled task is refused, so the cancel's outcome sticks.
func TestCancellingAnArcStopsItsTask(t *testing.T) {
	h := newHarness(t,
		&agent.LLMResponse{ModelID: "test", ToolCalls: []agent.LLMToolCall{
			{VendorID: "call_1", ToolName: "audit_namespaces"}}},
		&agent.LLMResponse{ModelID: "test", Content: "the audit is running"},
	)

	ctx := context.Background()
	arc := h.session.newArcID()
	if err := h.session.turn(ctx, arc, "audit the cluster"); err != nil {
		t.Fatalf("turn: %v", err)
	}

	if err := h.session.cancel(ctx, arc, "changed my mind"); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if err := h.session.cancelArcTasks(ctx, arc, "changed my mind"); err != nil {
		t.Fatalf("cancel the arc's tasks: %v", err)
	}

	folded := h.waitFor(t, "the task to be settled cancelled", func(folded *history.History) bool {
		task := firstTask(folded)
		return task != nil && task.Status == history.AsyncTaskCancelled
	})
	task := firstTask(folded)
	if task.CancelledBy == nil || task.CancelledBy.UserID != "dev" {
		t.Errorf("cancelled by %v, want the session's actor", task.CancelledBy)
	}
	if task.FinishedSeq == nil {
		t.Error("the cancel set no finish seq")
	}

	// The CANCEL is correlated to the task ID and names it in the payload.
	stored, err := h.store.Events(ctx)
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	var sawCancel, sawResult bool
	for _, ev := range stored {
		if ev.Typ == events.CancelEventType && ev.CorrelationID != nil && *ev.CorrelationID == task.ID {
			sawCancel = true
			if payload, ok := ev.Payload.(events.CancelPayload); !ok || payload.TaskID == nil || *payload.TaskID != task.ID {
				t.Errorf("cancel payload = %#v, want TaskID %s", ev.Payload, task.ID)
			}
		}
		if ev.Typ == events.AsyncTaskResultEventType {
			sawResult = true
		}
	}
	if !sawCancel {
		t.Errorf("no CANCEL correlated to the task in %s", logTypes(t, h))
	}
	// The runner's report that it stopped is refused: the task is already
	// settled, and the cancel's outcome is what the log keeps.
	if sawResult {
		t.Errorf("a result was written for the cancelled task; log = %s", logTypes(t, h))
	}
}

// A task whose process died is settled on the way back in, or its arc is open
// for ever and every later turn is told work is in flight that will never
// report.
func TestReopeningALogSettlesAbandonedTasks(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	arc := "arc-1"
	for _, ev := range []*events.Event{
		{SessionID: h.session.id, AgentID: localAgentID, Typ: events.UserMessageEventType, ID: arc, CorrelationID: &arc, Actor: h.session.actor,
			Payload: events.UserMessagePayload{Content: "audit the cluster", Actor: h.session.actor}},
		{SessionID: h.session.id, AgentID: localAgentID, Typ: events.AssistantMessageEventType, ID: "msg-1", CorrelationID: &arc,
			Payload: events.AssistantMessagePayload{ModelID: "test", Content: "running"}},
		{SessionID: h.session.id, AgentID: localAgentID, Typ: events.AsyncTaskInvokeEventType, ID: "task-1", CorrelationID: &arc, Actor: h.session.actor,
			Payload: events.AsyncTaskInvokePayload{TaskName: "audit_namespaces", Actor: h.session.actor}},
	} {
		if _, err := h.session.persistor.Persist(ctx, ev); err != nil {
			t.Fatalf("persist: %v", err)
		}
	}

	folded, err := h.session.fullHistory(ctx)
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	settled, err := ingress.SettleAbandonedTasks(ctx, common.AsAgent(localAgentID, h.session.persistor), h.session.id, folded)
	if err != nil {
		t.Fatalf("SettleAbandonedTasks: %v", err)
	}
	if settled != 1 {
		t.Fatalf("settled %d, want the one task nothing is behind", settled)
	}

	folded, err = h.session.fullHistory(ctx)
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	task := firstTask(folded)
	if task.Status != history.AsyncTaskFailed {
		t.Errorf("task is %s, want it settled", task.Status)
	}
	if result, _ := task.Result.(string); !strings.Contains(result, "not known") {
		t.Errorf("result = %q, want it to say whether the work happened is not known", result)
	}
	// The reaper's settle is None visibility, correlated to the task: it opens
	// no arc and reaches no model.
	if len(folded.Arcs) != 1 {
		t.Errorf("the settle opened %d arcs, want none (arcs: %d)", len(folded.Arcs)-1, len(folded.Arcs))
	}
	stored, err := h.store.Events(ctx)
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	last := stored[len(stored)-1]
	if last.Typ != events.AsyncTaskResultEventType || last.CorrelationID == nil || *last.CorrelationID != "task-1" {
		t.Fatalf("the settle is %s correlated to %v, want a result correlated to the task", last.Typ, last.CorrelationID)
	}
	if payload, ok := last.Payload.(events.AsyncTaskResultPayload); !ok || payload.LLMVisibility != events.LLMVisibilityNone {
		t.Errorf("the settle's visibility = %#v, want NONE", last.Payload)
	}
}

// Two answers to one question, which is what two arcs holding the same answer
// produce. Only the runner can tell them apart: each caller decided from a
// replay taken before it called, and both read that the task was waiting.
func TestOnlyTheFirstAnswerToAQuestionReachesTheTask(t *testing.T) {
	tasks, err := newLocalTasks()
	if err != nil {
		t.Fatalf("newLocalTasks: %v", err)
	}
	ref := common.TaskRef{ID: "task-1"}
	tasks.running[ref.ID] = &localTask{input: make(chan string, 1)}
	tasks.await(ref.ID, "ask-1")

	if err := tasks.ProvideInput(context.Background(), ref, "ask-1", "kube-system"); err != nil {
		t.Fatalf("first answer: %v", err)
	}
	if err := tasks.ProvideInput(context.Background(), ref, "ask-1", "ingress"); err == nil {
		t.Fatal("the second answer to one question was accepted")
	}

	if got := <-tasks.running[ref.ID].input; got != "kube-system" {
		t.Errorf("the task was given %q, want the answer that arrived first", got)
	}
}

// An answer to a question the task has already moved past is refused, rather
// than taken for the one it is waiting on now.
func TestAnAnswerToASupersededQuestionIsRefused(t *testing.T) {
	tasks, err := newLocalTasks()
	if err != nil {
		t.Fatalf("newLocalTasks: %v", err)
	}
	ref := common.TaskRef{ID: "task-1"}
	tasks.running[ref.ID] = &localTask{input: make(chan string, 1)}
	tasks.await(ref.ID, "ask-2")

	if err := tasks.ProvideInput(context.Background(), ref, "ask-1", "kube-system"); err == nil {
		t.Fatal("an answer to the wrong question was accepted")
	}
	if len(tasks.running[ref.ID].input) != 0 {
		t.Error("the task was handed input it had not asked for")
	}
}
