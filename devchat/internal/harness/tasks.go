package harness

import (
	"context"
	"fmt"
	"sync"
	"time"

	"fillbrunn.ai/kube-agent/ingress/pkg/ingress"
	"fillbrunn.ai/kube-agent/proto/pkg/common"
	"fillbrunn.ai/kube-agent/proto/pkg/events"
)

// The task here is a goroutine standing in for work that runs somewhere else.
// It reports the way a real one would, by calling the ingress rather than
// writing to the log.
//
// Reaching a goroutine needs a table of handles. That table lives here, in the
// implementation, and is never consulted for status: what a task is doing is
// read from the log.

// auditDelay is how long the fake audit spends on each of its steps. It is a
// variable so a test does not have to wait out a demo.
var auditDelay = 4 * time.Second

// localTasks is the one task source this harness holds: it runs tasks as
// goroutines, and is the controller that starts them and reaches them. The
// halves are one object because they share the handle table.
type localTasks struct {
	mu      sync.Mutex
	running map[string]*localTask

	// meta holds the kinds this source holds, and starters the launch of
	// each; the two are registered together in newLocalTasks, so a kind is
	// never held without a way to start it.
	meta     *common.TaskRegistry
	starters map[string]func(ref common.TaskRef, args any) error

	// reports is how a task says what it is doing. It is set after
	// construction because the ingress is built from an agent, and the agent
	// is built from the source these tasks are selected from.
	reports *ingress.TaskReports

	// defs holds the metadata of each running task's kind, keyed by task
	// ID, so the coalescer can resolve the class a notification names.
	defs map[string]common.TaskMetadata

	// coalescer is this controller's buffer: notifications of each AsMessage
	// class are coalesced per task, and past the kind's event limit demoted.
	coalescer *ingress.Coalescer

	// agentID names the agent that works the arc a report hands over. Devchat
	// runs one agent and knows which, so it says so when the reports are
	// attached rather than leaving the framework to recover it: a tier on a
	// queue would name the agent on each queue item instead.
	agentID string

	// unfiled is told about a report that could not be written. Everything a
	// task says that does reach the log reaches the screen through the event
	// sink; this is the one case there is nothing in the log to publish.
	unfiled func(ref common.TaskRef, err error)
}

// localTask is one running goroutine and the two ways to reach it.
type localTask struct {
	input  chan string
	cancel context.CancelFunc

	// awaiting is the request the task has stopped at, and is empty whenever
	// it is not waiting for anything. It is read and cleared under
	// localTasks.mu, so of two answers to one question only the first finds
	// it there.
	awaiting string
}

// auditTaskName is the one canned task kind this source holds, and the one the
// default settings name.
const auditTaskName = "audit_namespaces"

func newLocalTasks(options ...ingress.CoalesceOption) (*localTasks, error) {
	l := &localTasks{
		running:   map[string]*localTask{},
		defs:      map[string]common.TaskMetadata{},
		coalescer: ingress.NewCoalescer(options...),
		meta:      common.NewTaskRegistry(),
		starters:  map[string]func(common.TaskRef, any) error{},
	}
	// The declaration is plain metadata — the agent generates the model-facing
	// tool from it — and the launch is the harness's own, since the task runs
	// in this process.
	if err := l.meta.RegisterTask(common.TaskMetadata{
		Name: auditTaskName,
		Description: "Audit every namespace for problems. This takes a while, runs on its own, " +
			"and will ask which namespace to look at in detail.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"depth": map[string]any{
					"type":        "string",
					"description": "How thorough to be: quick or full.",
				},
			},
		},
	},
		// Progress reports are arc-opening and coalesced in a two-second
		// window; detail notes are invisible to the model.
		common.WithNotificationClasses(map[string]common.NotificationClass{
			"progress": {Visibility: events.LLMVisibilityAsMessage, GroupWindow: 2 * time.Second},
			"detail":   {Visibility: events.LLMVisibilityNone},
		}),
	); err != nil {
		return nil, err
	}
	l.starters[auditTaskName] = func(ref common.TaskRef, args any) error {
		return l.start(auditTaskName, ref, args)
	}
	return l, nil
}

// All returns every kind this source holds, in a fixed order.
func (l *localTasks) All() []common.TaskMetadata {
	names := l.meta.Names()
	kinds := make([]common.TaskMetadata, 0, len(names))
	for _, name := range names {
		if meta, ok := l.meta.Get(name); ok {
			kinds = append(kinds, meta)
		}
	}
	return kinds
}

// Get returns one kind of task by name.
func (l *localTasks) Get(name string) (common.TaskMetadata, bool) {
	return l.meta.Get(name)
}

// Start launches one kind of task and returns as soon as the work is under
// way, the way a call to a job runner would. In this process the controller
// runs the task directly and asynchronously.
func (l *localTasks) Start(_ context.Context, name string, ref common.TaskRef, args any) error {
	start, ok := l.starters[name]
	if !ok {
		return fmt.Errorf("task %s is not held by this source", name)
	}
	return start(ref, args)
}

// attach gives the tasks somewhere to report to, once there is one, and names
// the agent their reports are worked under.
func (l *localTasks) attach(reports *ingress.TaskReports, agentID string, unfiled func(common.TaskRef, error)) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.reports, l.agentID, l.unfiled = reports, agentID, unfiled
}

// start launches an audit and returns, the way a call to a job runner would.
// The context is not the run's, since a task outlives the run that started
// it.
func (l *localTasks) start(name string, ref common.TaskRef, _ any) error {
	ctx, cancel := context.WithCancel(context.Background())
	task := &localTask{input: make(chan string, 1), cancel: cancel}

	// The handle is registered before the goroutine starts, so a cancel or an
	// answer arriving the instant after the start always finds the task.
	metadata, ok := l.meta.Get(name)
	if !ok {
		cancel()
		return fmt.Errorf("task %s is not held by this source", name)
	}
	l.mu.Lock()
	l.running[ref.ID] = task
	l.defs[ref.ID] = metadata
	l.mu.Unlock()

	go l.audit(ctx, ref, task)
	return nil
}

// Cancel asks a running audit to stop. It settles nothing: the goroutine
// reports what became of it, which is what a job runner would do too.
func (l *localTasks) Cancel(_ context.Context, ref common.TaskRef, _ string) error {
	task, ok := l.lookup(ref.ID)
	if !ok {
		return fmt.Errorf("task %s is not running here", ref.ID)
	}
	task.cancel()
	return nil
}

// ProvideInput hands a waiting audit its answer, and refuses an answer to a
// question it has moved on from. This is the only place that can: whoever
// calls decided from a replay taken before the call.
func (l *localTasks) ProvideInput(_ context.Context, ref common.TaskRef, requestID, input string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	task, ok := l.running[ref.ID]
	if !ok {
		return fmt.Errorf("task %s is not running here", ref.ID)
	}
	if task.awaiting != requestID {
		return fmt.Errorf("task %s is not waiting on request %s", ref.ID, requestID)
	}

	// Clearing the request first is what makes the refusal exact. The channel
	// is buffered and a task asks again only once it has taken the last
	// answer, so the send cannot block under the lock.
	task.awaiting = ""
	task.input <- input
	return nil
}

// await records which question a task has stopped at, so an answer to it can
// be told from an answer to one it has already taken.
func (l *localTasks) await(id, requestID string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if task, ok := l.running[id]; ok {
		task.awaiting = requestID
	}
}

func (l *localTasks) lookup(id string) (*localTask, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	task, ok := l.running[id]
	return task, ok
}

func (l *localTasks) forget(id string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.running, id)
	delete(l.defs, id)
}

// audit is the work. It reports twice, stops to ask a question, and finishes.
// Every report goes through the ingress: write the event, and where the event
// opens an arc, hand it to the work queue.
func (l *localTasks) audit(ctx context.Context, ref common.TaskRef, task *localTask) {
	defer l.forget(ref.ID)

	if !l.pause(ctx, ref, "scanning namespaces") {
		return
	}
	l.report(ref, ingress.TaskReport{
		Kind: ingress.TaskReportNotification, Class: "progress",
		Message: "scanned 41 namespaces, 3 look busy"})
	l.report(ref, ingress.TaskReport{
		Kind: ingress.TaskReportNotification, Class: "detail",
		Message: "audit log follows each namespace as it is checked"})

	// The request is named and taken up before it is filed, because filing it
	// opens the arc, and the answer can be back before report returns.
	requestID := common.NewEventID()
	l.await(ref.ID, requestID)
	l.report(ref, ingress.TaskReport{
		Kind: ingress.TaskReportInputRequested, RequestID: requestID,
		Message: "which of kube-system, ingress or data should I look at in detail?"})

	var chosen string
	select {
	case chosen = <-task.input:
	case <-ctx.Done():
		l.stopped(ref)
		return
	}

	if !l.pause(ctx, ref, "auditing "+chosen) {
		return
	}
	l.report(ref, ingress.TaskReport{
		Kind:    ingress.TaskReportCompleted,
		Outcome: events.AsyncTaskOutcomeSuccess,
		Result:  fmt.Sprintf("%s: 2 pods without resource limits, 1 deployment with a single replica", chosen),
	})
}

// pause waits, and says whether the task should carry on. A task that was
// stopped reports that it was, because nothing else can say so.
func (l *localTasks) pause(ctx context.Context, ref common.TaskRef, _ string) bool {
	select {
	case <-time.After(auditDelay):
		return true
	case <-ctx.Done():
		l.stopped(ref)
		return false
	}
}

func (l *localTasks) stopped(ref common.TaskRef) {
	l.report(ref, ingress.TaskReport{
		Kind:    ingress.TaskReportCompleted,
		Outcome: events.AsyncTaskOutcomeCancelled,
		Result:  "stopped before the audit finished",
	})
}

// report calls the ingress, which writes the report and hands any arc it
// opened to the work queue. Nothing here tells the screen what was said: the
// report is published from the persistor that wrote it, like everything else
// in the log.
//
// A notification of an AsMessage class goes through the coalescer first, which
// holds it for its class's group window and files one merged report when the
// window closes; a report that cannot be filed at all is the exception, since
// there is nothing to publish and nobody above a task to hand the failure to.
func (l *localTasks) report(ref common.TaskRef, report ingress.TaskReport) {
	l.mu.Lock()
	reports, agentID, unfiled := l.reports, l.agentID, l.unfiled
	metadata, hasDef := l.defs[ref.ID]
	coalescer := l.coalescer
	l.mu.Unlock()

	if reports == nil {
		return
	}

	file := func(r ingress.TaskReport) {
		if err := reports.Report(context.Background(), ref, agentID, r); err != nil && unfiled != nil {
			unfiled(ref, err)
		}
	}

	if report.Kind == ingress.TaskReportNotification && hasDef && coalescer != nil {
		coalescer.Offer(ref, metadata, report, file)
		return
	}
	file(report)
}
