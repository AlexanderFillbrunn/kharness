package agent

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"fillbrunn.ai/kube-agent/pkg/events"
	"fillbrunn.ai/kube-agent/pkg/history"
)

// This file is the inbound half of async work. A task says what it is doing by
// calling in here: the report is written to the log, and where it opens an arc
// of its own, that arc is handed to the work queue like any other work item.

// TaskReport is one thing a task has to say.
type TaskReport struct {
	// Kind says which of the three it is.
	Kind TaskReportKind

	// Message is what a notification or an input request says.
	Message string

	// Class names the notification's class, as the task kind declared it. Only
	// notifications carry one.
	Class string

	// RequestID names an input request, and is the ID its event is written
	// under. A task mints it before it asks, because the answer comes back
	// naming it and the task has to recognise which question it answers.
	RequestID string

	// Payload is what a notification carries, where it carries structure
	// rather than a sentence.
	Payload any

	// Visibility overrides the class declaration, for a notification the
	// controller demoted past the task's event limit. Empty means the
	// declaration decides.
	Visibility events.LLMVisibility

	// Result and Outcome settle the task, and are read for a completion.
	Result  any
	Outcome events.AsyncTaskOutcome
}

// TaskReportKind is what a task is calling in to say.
type TaskReportKind string

const (
	// TaskReportNotification is progress. The task keeps running.
	TaskReportNotification TaskReportKind = "NOTIFICATION"
	// TaskReportInputRequested is the task stopping to ask for something.
	TaskReportInputRequested TaskReportKind = "INPUT_REQUESTED"
	// TaskReportCompleted settles the task, however it ended.
	TaskReportCompleted TaskReportKind = "COMPLETED"
)

// TaskReports takes what running tasks have to say and turns it into log
// records and work items.
//
// A report names the session and the task ID. The origin arc and the task kind
// are read from the session's log, and the report is validated by whether the
// task exists in the session — not by whether an arc holds it.
type TaskReports struct {
	persistor EventPersistor
	source    EventSource
	tasks     *TaskRegistry
	enqueue   WorkEnqueuer
}

// NewTaskReports builds the inbound half over the parts it needs: where it
// writes, where it reads the session's log, what task kinds are declared, and
// where it hands an arc it opened to the work queue.
func NewTaskReports(persistor EventPersistor, source EventSource, tasks *TaskRegistry, enqueue WorkEnqueuer) (*TaskReports, error) {
	if persistor == nil || source == nil || tasks == nil || enqueue == nil {
		return nil, errors.New("reports need a persistor, a source, a task registry and a work queue")
	}
	return &TaskReports{persistor: persistor, source: source, tasks: tasks, enqueue: enqueue}, nil
}

// Report records what a task said, and where the record opens an arc of its
// own, hands that arc to the work queue as a work item. The origin arc is not
// re-worked, and the caller does not retry when the new arc's lease is held:
// the queue re-offers the item, and the per-arc lease serializes the runs of
// one arc.
//
// The event's LLMVisibility is resolved here, on the write path, from the
// declaration on the task's kind, and written into the payload; the
// correlation follows it. An ask, a result, or a notification of an AsMessage
// class gets a fresh arc ID as its correlation; a None or InResult event
// correlates to the task. A notification naming a class the kind has not
// declared is refused, and nothing is logged.
func (r *TaskReports) Report(ctx context.Context, ref TaskRef, agentID string, report TaskReport) error {
	if agentID == "" {
		return fmt.Errorf("a report on task %s names no agent to work the arc it opens", ref.ID)
	}

	h, err := r.sessionHistory(ctx, ref.SessionID)
	if err != nil {
		return err
	}
	task, ok := h.Tasks[ref.ID]
	if !ok {
		return fmt.Errorf("session %s holds no task %s", ref.SessionID, ref.ID)
	}
	if task.FinishedSeq != nil {
		// The task is settled — cancelled or finished — so a later report from
		// its runner is refused rather than written over the outcome.
		return fmt.Errorf("task %s is already %s, so its report is refused", ref.ID, task.Status)
	}
	definition, ok := r.tasks.Get(task.TaskName)
	if !ok {
		return fmt.Errorf("task %s is of kind %s, which is not registered", ref.ID, task.TaskName)
	}

	ev, opensArc, err := reportEvent(ref, task, definition, report)
	if err != nil {
		return err
	}
	if _, err := AsAgent(agentID, r.persistor).Persist(ctx, ev); err != nil {
		return err
	}
	if opensArc {
		if err := r.enqueue.Enqueue(ctx, ArcRef{SessionID: ref.SessionID, ArcID: *ev.CorrelationID}, agentID); err != nil {
			return fmt.Errorf("enqueue the arc the report opened: %w", err)
		}
	}
	return nil
}

// sessionHistory folds the session's whole log, which is where the task table
// is built from.
func (r *TaskReports) sessionHistory(ctx context.Context, sessionID string) (*history.History, error) {
	stored, err := r.source.GetEventsForSession(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	folded := make([]events.Event, 0, len(stored))
	for _, ev := range stored {
		if ev.SessionID != sessionID {
			return nil, fmt.Errorf("event %s is in session %s, not %s", ev.ID, ev.SessionID, sessionID)
		}
		folded = append(folded, *ev)
	}
	return history.Replay(sessionID, folded)
}

// reportEvent turns a report into the record of it. It returns the event and
// whether the event opens an arc, in which case its correlation ID is the
// fresh arc ID the caller enqueues.
func reportEvent(ref TaskRef, task *history.AsyncTask, definition TaskDefinition, report TaskReport) (*events.Event, bool, error) {
	ev := &events.Event{SessionID: ref.SessionID}
	if task.Actor != nil {
		// The actor is copied forward: every event the task emits carries the
		// one the invoke recorded, so every arc one of them opens inherits it.
		ev.Actor = *task.Actor
	}

	switch report.Kind {
	case TaskReportNotification:
		class, ok := definition.Notifications[report.Class]
		if !ok {
			return nil, false, fmt.Errorf("task kind %s has declared no notification class %q", definition.Tool.Name, report.Class)
		}
		visibility := class.Visibility
		if report.Visibility != "" {
			// A demoted notification: the controller's decision, already made
			// against the kind's event limit.
			visibility = report.Visibility
		}
		payload := report.Payload
		if payload == nil {
			payload = report.Message
		}
		ev.Typ = events.AsyncTaskEventEventType
		ev.Payload = events.AsyncTaskEventPayload{
			CausationID:   ref.ID,
			Class:         report.Class,
			Payload:       payload,
			LLMVisibility: visibility,
		}

	case TaskReportInputRequested:
		if report.RequestID == "" {
			return nil, false, fmt.Errorf("task %s asked for input without naming the request", ref.ID)
		}
		// The task named the request, so the event is written under that name
		// rather than one the store mints. An ask is always AsMessage: an ask
		// that opened no arc could never be answered.
		ev.ID = report.RequestID
		ev.Typ = events.AsyncTaskInputRequestedEventType
		ev.Payload = events.AsyncTaskInputRequestedPayload{
			CausationID:   ref.ID,
			Message:       report.Message,
			LLMVisibility: events.LLMVisibilityAsMessage,
		}

	case TaskReportCompleted:
		if !report.Outcome.Valid() {
			return nil, false, fmt.Errorf("task %s reported completion without saying how it ended", ref.ID)
		}
		// A result is always AsMessage: a result that opened no arc would
		// never be worked on.
		ev.Typ = events.AsyncTaskResultEventType
		ev.Payload = events.AsyncTaskResultPayload{
			CausationID:   ref.ID,
			Result:        report.Result,
			Outcome:       report.Outcome,
			LLMVisibility: events.LLMVisibilityAsMessage,
		}

	default:
		return nil, false, fmt.Errorf("task %s reported something of no known kind: %q", ref.ID, report.Kind)
	}

	visibility := events.LLMVisibilityOf(ev.Payload).Effective()
	if visibility == events.LLMVisibilityAsMessage {
		arcID := NewEventID()
		ev.CorrelationID = &arcID
		return ev, true, nil
	}
	ev.CorrelationID = &ref.ID
	return ev, false, nil
}

// SettleAbandonedTasks closes off tasks whose runners died with the process
// that started them, and reports how many it settled. Left alone, each one
// would be a task nothing ever reports on.
//
// They are settled as failed rather than cancelled, since nobody stopped them.
// The settle is written with None visibility, correlated to the task — the
// reaper's exception to results always opening an arc — so it opens no arc
// and reaches no model.
func SettleAbandonedTasks(ctx context.Context, persistor EventPersistor, h *history.History) (int, error) {
	settled := 0
	for _, task := range h.TasksInOrder() {
		if task.FinishedSeq != nil {
			continue
		}
		taskID := task.ID
		_, err := persistor.Persist(ctx, &events.Event{
			SessionID:     h.SessionID,
			Typ:           events.AsyncTaskResultEventType,
			CorrelationID: &taskID,
			Payload: events.AsyncTaskResultPayload{
				CausationID:   task.ID,
				Outcome:       events.AsyncTaskOutcomeFailed,
				LLMVisibility: events.LLMVisibilityNone,
				Result: fmt.Sprintf(
					"task %s was started by a process that is no longer running, and nothing "+
						"has reported on it since. Whether it did its work is not known.",
					task.TaskName),
			},
		})
		if err != nil {
			return settled, err
		}
		settled++
	}
	return settled, nil
}

// Coalescer is the buffer a controller keeps for the notifications of each
// AsMessage class: per task and class, at most one report per group window.
// The buffer is the controller's memory, so coalescing is per controller
// instance. Coalescing happens before the log; nothing about it is recorded.
//
// It also enforces the task's event limit: past it, an AsMessage-class
// notification is demoted per the kind's overflow policy and opens no arc.
// Asks and results are always written whole and never pass through here.
type Coalescer struct {
	mu       sync.Mutex
	windows  map[coalesceKey]*coalesceWindow
	admitted map[string]int

	// limitOverride is the agent config's global override of the per-kind
	// event limit, where one is set.
	limitOverride *int

	now func() time.Time
}

// CoalesceOption configures a Coalescer.
type CoalesceOption func(*Coalescer)

// WithEventLimitOverride sets the global override of the per-kind event limit.
func WithEventLimitOverride(limit int) CoalesceOption {
	return func(c *Coalescer) { c.limitOverride = &limit }
}

// NewCoalescer returns an empty coalescer.
func NewCoalescer(options ...CoalesceOption) *Coalescer {
	c := &Coalescer{
		windows:  map[coalesceKey]*coalesceWindow{},
		admitted: map[string]int{},
		now:      time.Now,
	}
	for _, option := range options {
		option(c)
	}
	return c
}

type coalesceKey struct {
	task  string
	class string
}

type coalesceWindow struct {
	ref      TaskRef
	def      TaskDefinition
	class    string
	file     func(TaskReport)
	openedAt time.Time
	window   time.Duration
	payloads []any
	messages []string
	timer    *time.Timer
}

// Offer decides what happens to one notification report for a task of the
// given definition. file is called with the report to write to the log now:
// a None or InResult notification flows as it is, a past-limit AsMessage
// notification is filed demoted, and an AsMessage notification inside its
// limit is held for coalescing, with one merged report flushed when its window
// closes. A report naming a class the kind has not declared is passed on
// unchanged, for the write path to refuse.
func (c *Coalescer) Offer(ref TaskRef, definition TaskDefinition, report TaskReport, file func(TaskReport)) {
	class, ok := definition.Notifications[report.Class]
	if !ok || class.Visibility.Effective() != events.LLMVisibilityAsMessage {
		// None and InResult notifications flow as they are.
		file(report)
		return
	}

	c.mu.Lock()
	limit := definition.EventLimit
	if c.limitOverride != nil {
		limit = *c.limitOverride
	}
	if limit > 0 && c.admitted[ref.ID] >= limit {
		c.mu.Unlock()
		// Past the limit the event is demoted instead of opening an arc: the
		// overflow policy says ignored (None) or folded (InResult).
		demoted := report
		switch definition.OverflowPolicy {
		case OverflowFold:
			demoted.Visibility = events.LLMVisibilityInResult
		default:
			demoted.Visibility = events.LLMVisibilityNone
		}
		file(demoted)
		return
	}

	key := coalesceKey{task: ref.ID, class: report.Class}
	now := c.now()
	window := c.windows[key]
	if window != nil && now.Sub(window.openedAt) >= window.window {
		// The window has closed without its timer having fired yet: flush it
		// and open a new one with this notification.
		c.flushLocked(key)
		window = nil
	}
	if window == nil {
		window = &coalesceWindow{
			ref:      ref,
			def:      definition,
			class:    report.Class,
			file:     file,
			openedAt: now,
			window:   class.GroupWindow,
		}
		window.timer = time.AfterFunc(class.GroupWindow, func() {
			c.mu.Lock()
			defer c.mu.Unlock()
			c.flushLocked(key)
		})
		c.windows[key] = window
	}
	window.payloads = append(window.payloads, notificationPayload(report))
	if report.Message != "" {
		window.messages = append(window.messages, report.Message)
	}
	c.mu.Unlock()
}

// flushLocked flushes one window as a single merged report. The window is
// fixed from its first notification, so the merged report carries the whole
// window's payloads in order.
func (c *Coalescer) flushLocked(key coalesceKey) {
	window, ok := c.windows[key]
	if !ok {
		return
	}
	delete(c.windows, key)
	if window.timer != nil {
		window.timer.Stop()
	}
	c.admitted[window.ref.ID]++

	merged := TaskReport{
		Kind:    TaskReportNotification,
		Class:   window.class,
		Payload: map[string]any{"events": window.payloads},
		Message: fmt.Sprintf("[%d notifications of class %s were merged into this one report]", len(window.payloads), window.class),
	}
	if len(window.messages) > 0 {
		merged.Message += "\n" + strings.Join(window.messages, "\n")
	}
	window.file(merged)
}

// notificationPayload is what a notification carries: its structured payload
// where it has one, its text otherwise.
func notificationPayload(report TaskReport) any {
	if report.Payload != nil {
		return report.Payload
	}
	return report.Message
}
