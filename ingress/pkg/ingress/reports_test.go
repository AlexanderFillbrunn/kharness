package ingress_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"fillbrunn.ai/kube-agent/agent/pkg/agent"
	"fillbrunn.ai/kube-agent/proto/pkg/history"
	"fillbrunn.ai/kube-agent/ingress/pkg/ingress"
	"fillbrunn.ai/kube-agent/proto/pkg/common"
	"fillbrunn.ai/kube-agent/proto/pkg/events"
	"fillbrunn.ai/kube-agent/proto/pkg/users"
)

// logStore is a persistor that is also readable, which is what the inbound
// half needs: it writes a report and then reads the session back to validate
// and work it.
type logStore struct {
	mu     sync.Mutex
	events []*events.Event
	next   int
}

func (s *logStore) Persist(_ context.Context, evs ...*events.Event) ([]*events.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	stored := make([]*events.Event, 0, len(evs))
	for _, ev := range evs {
		one := *ev
		one.Seq = s.next
		if one.ID == "" {
			one.ID = "ev-" + string(rune('a'+s.next))
		}
		if one.Timestamp.IsZero() {
			one.Timestamp = time.Unix(int64(s.next), 0)
		}
		s.next++
		s.events = append(s.events, &one)
		stored = append(stored, &one)
	}
	return stored, nil
}

// SaveSnapshot accepts and drops the fold; these tests do not read through
// snapshots.
func (s *logStore) SaveSnapshot(_ context.Context, _ string, _ *history.History) error {
	return nil
}

// GetEventsForSession returns the whole log, the way a store that holds many
// sessions returns one session's slice.
func (s *logStore) GetEventsForSession(_ context.Context, sessionID string, _ common.ReadOptions) (*common.Snapshot, []*events.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*events.Event
	for _, ev := range s.events {
		if ev.SessionID == sessionID {
			out = append(out, ev)
		}
	}
	return nil, out, nil
}

// GetEventsForArc applies the projection's cutoff the way the real store does:
// everything up to the arc's own opening event, plus everything correlated to
// it or to a task afterwards.
func (s *logStore) GetEventsForArc(_ context.Context, ref common.ArcRef, _ common.ReadOptions) (*common.Snapshot, []*events.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// A store holds many sessions and partitions on the session in the ref,
	// the way the Redis key and the filestore's filter do.
	var mine []*events.Event
	for _, ev := range s.events {
		if ev.SessionID == ref.SessionID {
			mine = append(mine, ev)
		}
	}
	selected, err := common.SelectArcEvents(mine, ref, nil)
	if err != nil {
		return nil, nil, err
	}
	return nil, selected, nil
}

func (s *logStore) Cancelled(_ context.Context, ref common.ArcRef) (*events.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ev := range s.events {
		if ev.Typ == events.CancelEventType && ev.CorrelationID != nil && *ev.CorrelationID == ref.ArcID {
			return ev, nil
		}
	}
	return nil, nil
}

func (s *logStore) CancelledTask(_ context.Context, _, taskID string) (*events.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ev := range s.events {
		if ev.Typ == events.CancelEventType && ev.CorrelationID != nil && *ev.CorrelationID == taskID {
			return ev, nil
		}
	}
	return nil, nil
}

// common.TaskState scans the in-memory log, the way a store scans its file.
func (s *logStore) TaskState(_ context.Context, sessionID, taskID string) (*common.TaskState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return common.TaskStateOf(sessionID, taskID, s.events), nil
}

func (s *logStore) types() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return typesOf(s.events)
}

// recordingEnqueuer is the work queue the report path hands arcs to: it records
// them, and works them through the worker when the test wants that.
type recordingEnqueuer struct {
	mu       sync.Mutex
	enqueued []common.ArcRef
	work     func(ctx context.Context, ref common.ArcRef, agentID string)
}

func (e *recordingEnqueuer) Enqueue(ctx context.Context, ref common.ArcRef, agentID string) error {
	e.mu.Lock()
	e.enqueued = append(e.enqueued, ref)
	work := e.work
	e.mu.Unlock()
	if work != nil {
		work(ctx, ref, agentID)
	}
	return nil
}

func (e *recordingEnqueuer) refs() []common.ArcRef {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]common.ArcRef(nil), e.enqueued...)
}

// taskRegistry declares the one kind of task the fixtures run, with a
// coalescible AsMessage class and a None class.
func taskRegistry(t *testing.T) *common.TaskRegistry {
	t.Helper()
	registry := common.NewTaskRegistry()
	err := registry.RegisterTask(common.TaskMetadata{Name: "auditor"},
		common.WithNotificationClasses(map[string]common.NotificationClass{
			"progress": {Visibility: events.LLMVisibilityAsMessage, GroupWindow: time.Hour},
			"detail":   {Visibility: events.LLMVisibilityNone},
		}))
	if err != nil {
		t.Fatalf("RegisterTask: %v", err)
	}
	return registry
}

// runningTaskStore is a log holding one arc that started a task and answered
// without waiting for it, which is where every report arrives.
func runningTaskStore(t *testing.T) *logStore {
	t.Helper()
	arc := testArcID
	actor := users.Actor{UserID: "alice", TenantID: "acme"}
	store := &logStore{}
	for _, ev := range []*events.Event{
		{SessionID: testSessionID, AgentID: testAgentID, Typ: events.UserMessageEventType, CorrelationID: &arc, Actor: actor,
			Payload: events.UserMessagePayload{Content: "audit the cluster", Actor: actor}},
		{SessionID: testSessionID, AgentID: testAgentID, ID: "msg-1", Typ: events.AssistantMessageEventType, CorrelationID: &arc,
			Payload: events.AssistantMessagePayload{ModelID: "gpt-5", Content: "starting the audit"}},
		{SessionID: testSessionID, AgentID: testAgentID, ID: "task-1", Typ: events.AsyncTaskInvokeEventType, CorrelationID: &arc,
			Actor: actor, Payload: events.AsyncTaskInvokePayload{TaskName: "auditor", Actor: actor}},
	} {
		if _, err := store.Persist(context.Background(), ev); err != nil {
			t.Fatalf("persist: %v", err)
		}
	}
	// The arc's opening has to carry the arc's own ID, which the store only
	// assigns to events that arrive without one.
	store.events[0].ID = arc
	return store
}

// reportsFor builds the ingress over the store, with an enqueuer that works
// whatever it is handed through the worker, so a report's arc is served the
// way the queue would serve it.
func reportsFor(t *testing.T, store *logStore, enqueuer *recordingEnqueuer, replies ...*agent.LLMResponse) *ingress.TaskReports {
	t.Helper()
	if enqueuer == nil {
		enqueuer = &recordingEnqueuer{}
	}
	ag, err := agent.NewAgent(nil, nil, &fakeLLM{replies: replies})
	if err != nil {
		t.Fatalf("agent.NewAgent: %v", err)
	}
	worker, err := agent.NewArcWorker(store, store, agent.NewFixedResolver(ag), nil)
	if err != nil {
		t.Fatalf("agent.NewArcWorker: %v", err)
	}
	enqueuer.work = func(ctx context.Context, ref common.ArcRef, agentID string) {
		_, _ = worker.Work(ctx, ref, agentID)
	}
	reports, err := ingress.NewTaskReports(store, store, taskRegistry(t), enqueuer)
	if err != nil {
		t.Fatalf("ingress.NewTaskReports: %v", err)
	}
	return reports
}

func taskRef() common.TaskRef {
	return common.TaskRef{SessionID: testSessionID, ID: "task-1"}
}

// AC2: a completion reported after the origin arc closed is persisted with a
// fresh arc ID as its correlation, the fold mints that arc, and the new arc is
// handed to the work queue as a work item — the owed list is not involved.
func TestACompletionIsRecordedAndOpensANewArc(t *testing.T) {
	store := runningTaskStore(t)
	enqueuer := &recordingEnqueuer{}
	reports := reportsFor(t, store, enqueuer, &agent.LLMResponse{ModelID: "gpt-5", Content: "the audit found 3 things"})

	if err := reports.Report(context.Background(), taskRef(), testAgentID, ingress.TaskReport{
		Kind: ingress.TaskReportCompleted, Result: "3 findings", Outcome: events.AsyncTaskOutcomeSuccess}); err != nil {
		t.Fatalf("Report: %v", err)
	}

	want := "USER_MESSAGE, ASSISTANT_MESSAGE, ASYNC_TASK_INVOKE, ASYNC_TASK_RESULT, ASSISTANT_MESSAGE, ARC_CLOSED"
	if got := store.types(); got != want {
		t.Fatalf("log = %s, want %s", got, want)
	}

	var result *events.Event
	for _, ev := range store.events {
		if ev.Typ == events.AsyncTaskResultEventType {
			result = ev
		}
	}
	if result == nil {
		t.Fatal("no result was written")
	}
	payload := result.Payload.(events.AsyncTaskResultPayload)
	if payload.LLMVisibility != events.LLMVisibilityAsMessage {
		t.Errorf("visibility = %q, want AS_MESSAGE", payload.LLMVisibility)
	}
	if result.CorrelationID == nil || *result.CorrelationID == testArcID {
		t.Errorf("the result correlates to %v, want a fresh arc ID", result.CorrelationID)
	}
	// The actor is copied forward from the invoke.
	if result.Actor.UserID != "alice" {
		t.Errorf("the result's actor = %q, want the invoke's actor", result.Actor.UserID)
	}
	// The new arc went to the queue as a work item.
	refs := enqueuer.refs()
	if len(refs) != 1 || refs[0].ArcID != *result.CorrelationID || refs[0].SessionID != testSessionID {
		t.Errorf("enqueued = %v, want the one new arc", refs)
	}
}

// An ask is always written AsMessage under the name the task gave it, and the
// arc it opens is worked: putting the question to a person is the only thing
// that can move the task on.
func TestAnInputRequestOpensAnArcUnderTheNameTheTaskGaveIt(t *testing.T) {
	store := runningTaskStore(t)
	enqueuer := &recordingEnqueuer{}
	reports := reportsFor(t, store, enqueuer, &agent.LLMResponse{ModelID: "gpt-5", Content: "which namespace should I audit?"})

	if err := reports.Report(context.Background(), taskRef(), testAgentID, ingress.TaskReport{
		Kind: ingress.TaskReportInputRequested, RequestID: "ask-1", Message: "which namespace?"}); err != nil {
		t.Fatalf("Report: %v", err)
	}

	want := "USER_MESSAGE, ASSISTANT_MESSAGE, ASYNC_TASK_INVOKE, ASYNC_TASK_INPUT_REQUESTED, ASSISTANT_MESSAGE, ARC_CLOSED"
	if got := store.types(); got != want {
		t.Fatalf("log = %s, want %s", got, want)
	}

	var ask *events.Event
	for _, ev := range store.events {
		if ev.Typ == events.AsyncTaskInputRequestedEventType {
			ask = ev
		}
	}
	if ask == nil {
		t.Fatal("no question was written")
	}
	if ask.ID != "ask-1" {
		t.Errorf("the question is %q, want the name the task minted", ask.ID)
	}
	payload := ask.Payload.(events.AsyncTaskInputRequestedPayload)
	if payload.LLMVisibility != events.LLMVisibilityAsMessage {
		t.Errorf("visibility = %q, want AS_MESSAGE: an ask that opened no arc could never be answered", payload.LLMVisibility)
	}
	if ask.CorrelationID == nil || *ask.CorrelationID == testArcID {
		t.Errorf("the ask correlates to %v, want a fresh arc ID", ask.CorrelationID)
	}
}

// A task that asks without naming the request is refused. Nothing but the task
// can mint that name, since it has to recognise the answer when it comes back.
func TestAnInputRequestWithoutARequestIDIsRefused(t *testing.T) {
	store := runningTaskStore(t)
	reports := reportsFor(t, store, nil)

	if err := reports.Report(context.Background(), taskRef(), testAgentID, ingress.TaskReport{
		Kind: ingress.TaskReportInputRequested, Message: "which namespace?"}); err == nil {
		t.Fatal("an unnamed request was accepted")
	}

	want := "USER_MESSAGE, ASSISTANT_MESSAGE, ASYNC_TASK_INVOKE"
	if got := store.types(); got != want {
		t.Errorf("wrote %s, want nothing beyond %s", got, want)
	}
}

// A notification of a None class is written correlated to the task and opens
// no arc: it is in the log for the frontend and replay, and the queue is not
// involved.
func TestANoneNotificationIsRecordedAgainstTheTaskAndOpensNoArc(t *testing.T) {
	store := runningTaskStore(t)
	enqueuer := &recordingEnqueuer{}
	reports := reportsFor(t, store, enqueuer)

	if err := reports.Report(context.Background(), taskRef(), testAgentID, ingress.TaskReport{
		Kind: ingress.TaskReportNotification, Class: "detail", Message: "scanned 40 namespaces"}); err != nil {
		t.Fatalf("Report: %v", err)
	}

	want := "USER_MESSAGE, ASSISTANT_MESSAGE, ASYNC_TASK_INVOKE, ASYNC_TASK_EVENT"
	if got := store.types(); got != want {
		t.Fatalf("log = %s, want %s", got, want)
	}

	var notification *events.Event
	for _, ev := range store.events {
		if ev.Typ == events.AsyncTaskEventEventType {
			notification = ev
		}
	}
	payload := notification.Payload.(events.AsyncTaskEventPayload)
	if payload.Class != "detail" || payload.LLMVisibility != events.LLMVisibilityNone {
		t.Errorf("payload = %#v, want class detail and NONE visibility", payload)
	}
	if notification.CorrelationID == nil || *notification.CorrelationID != "task-1" {
		t.Errorf("the notification correlates to %v, want the task", notification.CorrelationID)
	}
	if refs := enqueuer.refs(); len(refs) != 0 {
		t.Errorf("enqueued = %v, want nothing: a None event opens no arc", refs)
	}
}

// A notification of an AsMessage class is written with a fresh arc ID as its
// correlation and the new arc is enqueued.
func TestAnAsMessageNotificationOpensAnArc(t *testing.T) {
	store := runningTaskStore(t)
	enqueuer := &recordingEnqueuer{}
	reports := reportsFor(t, store, enqueuer, &agent.LLMResponse{ModelID: "gpt-5", Content: "noted"})

	if err := reports.Report(context.Background(), taskRef(), testAgentID, ingress.TaskReport{
		Kind: ingress.TaskReportNotification, Class: "progress", Message: "3 namespaces look busy"}); err != nil {
		t.Fatalf("Report: %v", err)
	}

	var notification *events.Event
	for _, ev := range store.events {
		if ev.Typ == events.AsyncTaskEventEventType {
			notification = ev
		}
	}
	payload := notification.Payload.(events.AsyncTaskEventPayload)
	if payload.LLMVisibility != events.LLMVisibilityAsMessage {
		t.Errorf("visibility = %q, want AS_MESSAGE", payload.LLMVisibility)
	}
	if notification.CorrelationID == nil || *notification.CorrelationID == testArcID || *notification.CorrelationID == "task-1" {
		t.Errorf("the notification correlates to %v, want a fresh arc ID", notification.CorrelationID)
	}
	refs := enqueuer.refs()
	if len(refs) != 1 || refs[0].ArcID != *notification.CorrelationID {
		t.Errorf("enqueued = %v, want the arc the notification opened", refs)
	}
}

// AC11: a notification naming a class the task kind has not declared is
// refused at the write path and nothing is logged.
func TestANotificationNamingAnUndeclaredClassIsRefused(t *testing.T) {
	store := runningTaskStore(t)
	reports := reportsFor(t, store, nil)

	if err := reports.Report(context.Background(), taskRef(), testAgentID, ingress.TaskReport{
		Kind: ingress.TaskReportNotification, Class: "undeclared", Message: "news"}); err == nil {
		t.Fatal("a notification of an undeclared class was accepted")
	}

	want := "USER_MESSAGE, ASSISTANT_MESSAGE, ASYNC_TASK_INVOKE"
	if got := store.types(); got != want {
		t.Errorf("log = %s, want nothing written", got)
	}
}

// A report is validated by whether the task exists in the session, not by
// whether an arc holds it: a report naming an unknown task is refused before
// anything is written.
func TestAReportNamingAnUnknownTaskIsRefused(t *testing.T) {
	store := runningTaskStore(t)
	reports := reportsFor(t, store, nil)

	err := reports.Report(context.Background(), common.TaskRef{SessionID: testSessionID, ID: "not-this-one"}, testAgentID, ingress.TaskReport{
		Kind: ingress.TaskReportCompleted, Result: "done", Outcome: events.AsyncTaskOutcomeSuccess})
	if err == nil {
		t.Fatal("a report about a task the session does not hold was accepted")
	}
	if got := store.types(); strings.Contains(got, "ASYNC_TASK_RESULT") {
		t.Errorf("log = %s, want nothing written", got)
	}
}

// A completion that does not say how it ended is not a completion.
func TestACompletionWithoutAnOutcomeIsRefused(t *testing.T) {
	store := runningTaskStore(t)
	reports := reportsFor(t, store, nil)

	if err := reports.Report(context.Background(), taskRef(), testAgentID, ingress.TaskReport{Kind: ingress.TaskReportCompleted, Result: "done"}); err == nil {
		t.Fatal("a completion with no outcome was accepted")
	}
}

// AC5/R15: a report arriving after the task was settled — cancelled, say — is
// refused at the write path.
func TestALateReportOnASettledTaskIsRefused(t *testing.T) {
	store := runningTaskStore(t)
	taskID := "task-1"
	if _, err := store.Persist(context.Background(), &events.Event{
		SessionID: testSessionID, Typ: events.CancelEventType, CorrelationID: &taskID,
		Payload: events.CancelPayload{TaskID: &taskID},
	}); err != nil {
		t.Fatalf("persist the cancel: %v", err)
	}
	reports := reportsFor(t, store, nil)

	err := reports.Report(context.Background(), taskRef(), testAgentID, ingress.TaskReport{
		Kind: ingress.TaskReportCompleted, Result: "done", Outcome: events.AsyncTaskOutcomeSuccess})
	if err == nil {
		t.Fatal("a report on a cancelled task was accepted")
	}
	if got := store.types(); strings.Contains(got, "ASYNC_TASK_RESULT") {
		t.Errorf("log = %s, want no result after the cancel", got)
	}
}

func TestALeaseIsReleased(t *testing.T) {
	ctx := context.Background()
	lease := agent.NewLocalLease()
	first := common.ArcRef{SessionID: testSessionID, ArcID: "arc-1"}
	second := common.ArcRef{SessionID: testSessionID, ArcID: "arc-2"}

	release, ok, err := lease.Acquire(ctx, first)
	if err != nil || !ok {
		t.Fatalf("Acquire: held=%v err=%v, want the arc free", !ok, err)
	}
	defer release()

	if _, again, _ := lease.Acquire(ctx, first); again {
		t.Error("a held arc was taken twice")
	}
	if _, other, _ := lease.Acquire(ctx, second); !other {
		t.Error("holding one arc blocked another")
	}
	release()
	if _, after, _ := lease.Acquire(ctx, first); !after {
		t.Error("a released arc could not be taken again")
	}
}

// The same arc ID in two sessions is two arcs. Nothing in this harness
// produces one, but a lease that keyed on the ID alone would block the second
// session on the first.
func TestALeaseKeysOnTheSessionToo(t *testing.T) {
	ctx := context.Background()
	lease := agent.NewLocalLease()

	if _, ok, err := lease.Acquire(ctx, common.ArcRef{SessionID: "one", ArcID: testArcID}); err != nil || !ok {
		t.Fatalf("a free arc could not be taken: held=%v err=%v", !ok, err)
	}
	if _, ok, err := lease.Acquire(ctx, common.ArcRef{SessionID: "two", ArcID: testArcID}); err != nil || !ok {
		t.Errorf("the same arc ID in another session was blocked: held=%v err=%v", !ok, err)
	}
}

// refusingLease stands in for a lock service that cannot be reached. A caller
// that read that as "nobody holds it" would run the arc twice.
type refusingLease struct{ err error }

func (l refusingLease) Acquire(context.Context, common.ArcRef) (func(), bool, error) {
	return nil, false, l.err
}

func TestAWorkerReportsALeaseItCouldNotReach(t *testing.T) {
	store := runningTaskStore(t)
	ag, err := agent.NewAgent(nil, nil, &fakeLLM{})
	if err != nil {
		t.Fatalf("agent.NewAgent: %v", err)
	}
	worker, err := agent.NewArcWorker(store, store, agent.NewFixedResolver(ag), refusingLease{err: errors.New("no route to the lock service")})
	if err != nil {
		t.Fatalf("agent.NewArcWorker: %v", err)
	}

	worked, err := worker.Work(context.Background(), common.ArcRef{SessionID: testSessionID, ArcID: testArcID}, testAgentID)
	if err == nil {
		t.Fatal("an unreachable lease was reported as an arc somebody else held")
	}
	if worked {
		t.Error("the arc was worked without the lease")
	}
}

// A ref naming the wrong session would replay one session's arc under
// another's name, and everything the run then wrote would be filed there.
func TestAWorkerRefusesAnArcFromAnotherSession(t *testing.T) {
	store := runningTaskStore(t)
	ag, err := agent.NewAgent(nil, nil, &fakeLLM{})
	if err != nil {
		t.Fatalf("agent.NewAgent: %v", err)
	}
	worker, err := agent.NewArcWorker(store, store, agent.NewFixedResolver(ag), nil)
	if err != nil {
		t.Fatalf("agent.NewArcWorker: %v", err)
	}

	if _, err := worker.Work(context.Background(), common.ArcRef{SessionID: "somebody-else", ArcID: testArcID}, testAgentID); err == nil {
		t.Error("an arc was worked under a session it is not in")
	}
}

// A task whose runner died with its process is settled, or nobody ever reports
// on it. The settle is written with None visibility, correlated to the task,
// which is the reaper's exception to results always opening an arc.
func TestAbandonedTasksAreSettledAsUnknown(t *testing.T) {
	store := runningTaskStore(t)
	h, err := history.Replay(store.events)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}

	settled, err := ingress.SettleAbandonedTasks(context.Background(), store, testSessionID, h)
	if err != nil {
		t.Fatalf("ingress.SettleAbandonedTasks: %v", err)
	}
	if settled != 1 {
		t.Fatalf("settled %d tasks, want the one that was running", settled)
	}

	recorded := store.events[len(store.events)-1]
	payload, ok := recorded.Payload.(events.AsyncTaskResultPayload)
	if !ok {
		t.Fatalf("payload = %#v, want a task result", recorded.Payload)
	}
	if payload.Outcome != events.AsyncTaskOutcomeFailed {
		t.Errorf("outcome = %s, want it settled as failed rather than cancelled: nobody stopped it", payload.Outcome)
	}
	if payload.LLMVisibility != events.LLMVisibilityNone {
		t.Errorf("visibility = %q, want NONE: the reaper's settle opens no arc", payload.LLMVisibility)
	}
	if recorded.CorrelationID == nil || *recorded.CorrelationID != "task-1" {
		t.Errorf("the settle correlates to %v, want the task", recorded.CorrelationID)
	}
	if text, _ := payload.Result.(string); !strings.Contains(text, "not known") {
		t.Errorf("result = %q, want it to say whether the work happened is not known", text)
	}
}

// A task that already reported is not settled a second time.
func TestSettlingAbandonedTasksLeavesFinishedOnesAlone(t *testing.T) {
	store := runningTaskStore(t)
	taskID := "task-1"
	if _, err := store.Persist(context.Background(), &events.Event{
		Typ: events.AsyncTaskResultEventType, CorrelationID: &taskID,
		Payload: events.AsyncTaskResultPayload{
			CausationID: "task-1", Result: "3 findings", Outcome: events.AsyncTaskOutcomeSuccess,
			LLMVisibility: events.LLMVisibilityNone},
	}); err != nil {
		t.Fatalf("persist: %v", err)
	}
	h, err := history.Replay(store.events)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}

	settled, err := ingress.SettleAbandonedTasks(context.Background(), store, testSessionID, h)
	if err != nil {
		t.Fatalf("ingress.SettleAbandonedTasks: %v", err)
	}
	if settled != 0 {
		t.Errorf("settled %d tasks, want none: it had already reported", settled)
	}
}

// A report that names no agent has nobody to hand the arc it opens to, and it
// is refused rather than worked by a guess: the caller that hands a report
// over is where the agent is known, so one that cannot name it has nothing to
// hand the queue.
func TestAReportNamingNoAgentIsRefused(t *testing.T) {
	store := runningTaskStore(t)
	reports := reportsFor(t, store, nil)

	if err := reports.Report(context.Background(), taskRef(), "", ingress.TaskReport{
		Kind: ingress.TaskReportNotification, Class: "detail", Message: "half through",
	}); err == nil {
		t.Error("a report naming no agent was accepted")
	}
}
