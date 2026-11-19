package agent

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"fillbrunn.ai/kube-agent/pkg/events"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// coalesceRegistry declares one task kind with an AsMessage class coalesced in
// a short window, a None class and an InResult class, so a test can watch the
// windows close without waiting.
func coalesceRegistry(t *testing.T, window time.Duration, options ...TaskOption) *TaskRegistry {
	t.Helper()
	registry := NewTaskRegistry()
	classes := map[string]NotificationClass{
		"progress": {Visibility: events.LLMVisibilityAsMessage, GroupWindow: window},
		"detail":   {Visibility: events.LLMVisibilityNone},
		"fold":     {Visibility: events.LLMVisibilityInResult},
	}
	options = append([]TaskOption{WithNotificationClasses(classes)}, options...)
	err := registry.RegisterTask(&mcp.Tool{Name: "auditor"}, func(context.Context, TaskRef, any) error {
		return nil
	}, options...)
	if err != nil {
		t.Fatalf("RegisterTask: %v", err)
	}
	return registry
}

// taskEvents returns the notification events in the log.
func taskEvents(t *testing.T, store *logStore) []*events.Event {
	t.Helper()
	all, err := store.GetEventsForSession(context.Background(), testSessionID)
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	var out []*events.Event
	for _, ev := range all {
		if ev.Typ == events.AsyncTaskEventEventType {
			out = append(out, ev)
		}
	}
	return out
}

// waitFor polls until the condition holds or the wait runs out.
func waitFor(condition func() bool) bool {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return condition()
}

// reportLog is a thread-safe record of the reports a coalescer files, which
// may happen on its timer goroutine.
type reportLog struct {
	mu    sync.Mutex
	filed []TaskReport
}

func (l *reportLog) file(r TaskReport) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.filed = append(l.filed, r)
}

func (l *reportLog) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.filed)
}

func (l *reportLog) last() TaskReport {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.filed[len(l.filed)-1]
}

// AC8: three notifications of one AsMessage class inside one group window
// produce one log record, whose payload is the window's notifications under
// "events" and whose text says they were merged. Notifications of a None or
// InResult class are each logged as they arrive. An ask and a result are
// always written whole.
func TestCoalescingMergesOneClassPerWindow(t *testing.T) {
	const window = 60 * time.Millisecond
	registry := coalesceRegistry(t, window)
	store := runningTaskStore(t)
	enqueuer := &recordingEnqueuer{}
	reports, err := NewTaskReports(store, store, registry, enqueuer)
	if err != nil {
		t.Fatalf("NewTaskReports: %v", err)
	}
	coalescer := NewCoalescer()
	def, ok := registry.Get("auditor")
	if !ok {
		t.Fatal("auditor is not registered")
	}
	file := func(r TaskReport) {
		if err := reports.Report(context.Background(), taskRef(), testAgentID, r); err != nil {
			t.Errorf("Report: %v", err)
		}
	}

	for i := range 3 {
		coalescer.Offer(taskRef(), def, TaskReport{
			Kind: TaskReportNotification, Class: "progress", Message: fmt.Sprintf("step %d done", i),
		}, file)
	}
	// The window is still open, so nothing of the class has been logged yet.
	if got := len(taskEvents(t, store)); got != 0 {
		t.Fatalf("%d notification events logged inside the window, want none yet", got)
	}

	// None and InResult classes flow as they are.
	coalescer.Offer(taskRef(), def, TaskReport{Kind: TaskReportNotification, Class: "detail", Message: "noise"}, file)
	coalescer.Offer(taskRef(), def, TaskReport{Kind: TaskReportNotification, Class: "fold", Message: "for the result"}, file)
	if got := len(taskEvents(t, store)); got != 2 {
		t.Fatalf("%d notification events logged, want the two that flow as they are", got)
	}
	for _, ev := range taskEvents(t, store) {
		payload := ev.Payload.(events.AsyncTaskEventPayload)
		if payload.LLMVisibility != events.LLMVisibilityNone && payload.LLMVisibility != events.LLMVisibilityInResult {
			t.Errorf("event of class %s has visibility %s, want None or InResult", payload.Class, payload.LLMVisibility)
		}
		if ev.CorrelationID == nil || *ev.CorrelationID != taskRef().ID {
			t.Errorf("event of class %s correlates to %v, want the task", payload.Class, ev.CorrelationID)
		}
	}

	// The window closes and one merged report is written.
	if !waitFor(func() bool { return len(taskEvents(t, store)) == 3 }) {
		t.Fatalf("the window never flushed; log = %s", store.types())
	}
	var merged *events.Event
	for _, ev := range taskEvents(t, store) {
		if payload := ev.Payload.(events.AsyncTaskEventPayload); payload.Class == "progress" {
			merged = ev
		}
	}
	if merged == nil {
		t.Fatal("no merged progress report in the log")
	}
	payload := merged.Payload.(events.AsyncTaskEventPayload)
	if payload.LLMVisibility != events.LLMVisibilityAsMessage {
		t.Errorf("merged report visibility = %s, want AS_MESSAGE", payload.LLMVisibility)
	}
	// The merged report opens an arc, so it correlates to a fresh arc ID and
	// was enqueued; the None and InResult events were not.
	if merged.CorrelationID == nil || *merged.CorrelationID == taskRef().ID {
		t.Errorf("merged report correlates to %v, want a fresh arc ID", merged.CorrelationID)
	}
	if refs := enqueuer.refs(); len(refs) != 1 || refs[0].ArcID != *merged.CorrelationID {
		t.Errorf("enqueued %v, want the one arc the merged report opened", refs)
	}
	eventsList, ok := payload.Payload.(map[string]any)["events"].([]any)
	if !ok || len(eventsList) != 3 {
		t.Fatalf("merged payload = %#v, want an events list of the window's three notifications", payload.Payload)
	}
	for i, item := range eventsList {
		if item != fmt.Sprintf("step %d done", i) {
			t.Errorf("events[%d] = %v, want the window's notifications in order", i, item)
		}
	}

	// An ask and a result are always written whole, never coalesced.
	if err := reports.Report(context.Background(), taskRef(), testAgentID, TaskReport{
		Kind: TaskReportInputRequested, RequestID: "ask-1", Message: "which namespace?"}); err != nil {
		t.Fatalf("Report(ask): %v", err)
	}
	if err := reports.Report(context.Background(), taskRef(), testAgentID, TaskReport{
		Kind: TaskReportCompleted, Outcome: events.AsyncTaskOutcomeSuccess, Result: "3 findings"}); err != nil {
		t.Fatalf("Report(result): %v", err)
	}
	all, err := store.GetEventsForSession(context.Background(), testSessionID)
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	var asks, results int
	for _, ev := range all {
		switch ev.Typ {
		case events.AsyncTaskInputRequestedEventType:
			asks++
			if ev.CorrelationID == nil || *ev.CorrelationID == taskRef().ID {
				t.Error("the ask correlates to the task, want a fresh arc")
			}
		case events.AsyncTaskResultEventType:
			results++
			if ev.CorrelationID == nil || *ev.CorrelationID == taskRef().ID {
				t.Error("the result correlates to the task, want a fresh arc")
			}
		}
	}
	if asks != 1 || results != 1 {
		t.Errorf("asks=%d results=%d, want one of each, written whole", asks, results)
	}
	if refs := enqueuer.refs(); len(refs) != 3 {
		t.Errorf("enqueued %d arcs, want the merged report's, the ask's and the result's", len(refs))
	}
}

// AC13: past the kind's event limit, a further AsMessage-class notification
// opens no arc and is logged demoted per the overflow policy; asks and
// results are always written whole and are not demoted.
func TestPastTheEventLimitNotificationsAreDemoted(t *testing.T) {
	const window = 40 * time.Millisecond
	registry := coalesceRegistry(t, window, WithEventLimit(2), WithOverflowPolicy(OverflowFold))
	store := runningTaskStore(t)
	enqueuer := &recordingEnqueuer{}
	reports, err := NewTaskReports(store, store, registry, enqueuer)
	if err != nil {
		t.Fatalf("NewTaskReports: %v", err)
	}
	coalescer := NewCoalescer()
	def, _ := registry.Get("auditor")
	file := func(r TaskReport) {
		if err := reports.Report(context.Background(), taskRef(), testAgentID, r); err != nil {
			t.Errorf("Report: %v", err)
		}
	}
	progress := func(msg string) TaskReport {
		return TaskReport{Kind: TaskReportNotification, Class: "progress", Message: msg}
	}

	// Two windows flush, each admitting one arc-opening event: the limit of
	// two is now spent.
	coalescer.Offer(taskRef(), def, progress("first"), file)
	if !waitFor(func() bool { return len(taskEvents(t, store)) == 1 }) {
		t.Fatalf("the first window never flushed; log = %s", store.types())
	}
	coalescer.Offer(taskRef(), def, progress("second"), file)
	if !waitFor(func() bool { return len(taskEvents(t, store)) == 2 }) {
		t.Fatalf("the second window never flushed; log = %s", store.types())
	}
	if refs := enqueuer.refs(); len(refs) != 2 {
		t.Fatalf("enqueued %d arcs, want the two admitted events'", len(refs))
	}

	// The third is past the limit: demoted to InResult by the fold policy, so
	// it correlates to the task and opens no arc.
	coalescer.Offer(taskRef(), def, progress("third"), file)
	evs := taskEvents(t, store)
	if len(evs) != 3 {
		t.Fatalf("%d notification events logged, want the demoted third one too", len(evs))
	}
	demoted := evs[2]
	payload := demoted.Payload.(events.AsyncTaskEventPayload)
	if payload.LLMVisibility != events.LLMVisibilityInResult {
		t.Errorf("demoted event visibility = %s, want IN_RESULT under the fold policy", payload.LLMVisibility)
	}
	if demoted.CorrelationID == nil || *demoted.CorrelationID != taskRef().ID {
		t.Errorf("demoted event correlates to %v, want the task — it must open no arc", demoted.CorrelationID)
	}
	if refs := enqueuer.refs(); len(refs) != 2 {
		t.Errorf("enqueued %d arcs after the demotion, want still two", len(refs))
	}

	// An ask and a result past the limit are still written whole.
	if err := reports.Report(context.Background(), taskRef(), testAgentID, TaskReport{
		Kind: TaskReportInputRequested, RequestID: "ask-1", Message: "which namespace?"}); err != nil {
		t.Fatalf("Report(ask): %v", err)
	}
	if refs := enqueuer.refs(); len(refs) != 3 {
		t.Errorf("the ask was demoted: enqueued %d arcs, want three", len(refs))
	}
	if err := reports.Report(context.Background(), taskRef(), testAgentID, TaskReport{
		Kind: TaskReportCompleted, Outcome: events.AsyncTaskOutcomeSuccess, Result: "done"}); err != nil {
		t.Fatalf("Report(result): %v", err)
	}
	if refs := enqueuer.refs(); len(refs) != 4 {
		t.Errorf("the result was demoted: enqueued %d arcs, want four", len(refs))
	}
}

// The default overflow policy is to ignore: a past-limit notification is
// demoted to None, not folded.
func TestTheDefaultOverflowPolicyIgnores(t *testing.T) {
	const window = 40 * time.Millisecond
	registry := coalesceRegistry(t, window, WithEventLimit(1))
	def, _ := registry.Get("auditor")
	coalescer := NewCoalescer()
	log := &reportLog{}

	coalescer.Offer(taskRef(), def, TaskReport{Kind: TaskReportNotification, Class: "progress", Message: "one"}, log.file)
	if !waitFor(func() bool { return log.count() == 1 }) {
		t.Fatal("the first window never flushed")
	}
	coalescer.Offer(taskRef(), def, TaskReport{Kind: TaskReportNotification, Class: "progress", Message: "two"}, log.file)
	if log.count() != 2 {
		t.Fatalf("filed %d reports, want the demoted second one immediately", log.count())
	}
	if last := log.last(); last.Visibility != events.LLMVisibilityNone {
		t.Errorf("demoted visibility = %s, want NONE under the default ignore policy", last.Visibility)
	}
}

// The global override in the agent config wins over the kind's declared limit.
func TestTheGlobalEventLimitOverrideWins(t *testing.T) {
	const window = 40 * time.Millisecond
	registry := coalesceRegistry(t, window, WithEventLimit(5))
	def, _ := registry.Get("auditor")
	coalescer := NewCoalescer(WithEventLimitOverride(1))
	log := &reportLog{}

	coalescer.Offer(taskRef(), def, TaskReport{Kind: TaskReportNotification, Class: "progress", Message: "one"}, log.file)
	if !waitFor(func() bool { return log.count() == 1 }) {
		t.Fatal("the first window never flushed")
	}
	coalescer.Offer(taskRef(), def, TaskReport{Kind: TaskReportNotification, Class: "progress", Message: "two"}, log.file)
	if log.count() != 2 || log.last().Visibility != events.LLMVisibilityNone {
		t.Fatalf("filed %d reports, last visibility %s, want the second demoted to NONE by the override of 1", log.count(), log.last().Visibility)
	}
}
