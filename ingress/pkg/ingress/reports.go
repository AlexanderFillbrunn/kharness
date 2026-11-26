// Package ingress is the producer side of async work: it turns what running
// tasks have to say into log records and work items. A controller service
// imports this package; the agent never does — the agent worker only takes
// work items off the queue, and the enqueue happens here, on the producer's
// side of the line, right after the event that opened an arc is persisted.
package ingress

import (
	"context"
	"errors"
	"fmt"

	"fillbrunn.ai/kube-agent/proto/pkg/history"
	"fillbrunn.ai/kube-agent/proto/pkg/common"
	"fillbrunn.ai/kube-agent/proto/pkg/events"
)

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
// A report names the session and the task ID. What the log says about the task
// — whether the session holds it, its kind, its actor, whether it has settled —
// is read with a targeted TaskState query, not by folding the session, and the
// report is validated on that.
type TaskReports struct {
	persistor common.EventPersistor
	source    common.EventSource
	tasks     common.MetadataSource
	enqueue   common.WorkEnqueuer
}

// NewTaskReports builds the inbound half over the parts it needs: where it
// writes, where it reads the session's log, where the task kinds' metadata is
// declared, and where it hands an arc it opened to the work queue.
func NewTaskReports(persistor common.EventPersistor, source common.EventSource, tasks common.MetadataSource, enqueue common.WorkEnqueuer) (*TaskReports, error) {
	if persistor == nil || source == nil || tasks == nil || enqueue == nil {
		return nil, errors.New("reports need a persistor, a source, a task registry and a work queue")
	}
	return &TaskReports{persistor: persistor, source: source, tasks: tasks, enqueue: enqueue}, nil
}

// Report records what a task said, and where the record opens an arc of its
// own, hands that arc to the work queue as a work item. The enqueue is the
// producer's: the agent worker is never told about it here, it only ever takes
// the item off the queue. The origin arc is not re-worked, and the caller does
// not retry when the new arc's lease is held: the queue re-offers the item, and
// the per-arc lease serializes the runs of one arc.
//
// The event's LLMVisibility is resolved here, on the write path, from the
// declaration on the task's kind, and written into the payload; the
// correlation follows it. An ask, a result, or a notification of an AsMessage
// class gets a fresh arc ID as its correlation; a None or InResult event
// correlates to the task. A notification naming a class the kind has not
// declared is refused, and nothing is logged.
func (r *TaskReports) Report(ctx context.Context, ref common.TaskRef, agentID string, report TaskReport) error {
	if agentID == "" {
		return fmt.Errorf("a report on task %s names no agent to work the arc it opens", ref.ID)
	}

	state, err := r.source.TaskState(ctx, ref.SessionID, ref.ID)
	if err != nil {
		return err
	}
	if state == nil || !state.Exists {
		return fmt.Errorf("session %s holds no task %s", ref.SessionID, ref.ID)
	}
	if state.Settled {
		// The task is settled — cancelled or finished — so a later report from
		// its runner is refused rather than written over the outcome.
		return fmt.Errorf("task %s is already %s, so its report is refused", ref.ID, state.Status)
	}
	metadata, ok := r.tasks.Get(state.TaskName)
	if !ok {
		return fmt.Errorf("task %s is of kind %s, which is not registered", ref.ID, state.TaskName)
	}

	ev, opensArc, err := reportEvent(ref, state, metadata, report)
	if err != nil {
		return err
	}
	stored, err := common.AsAgent(agentID, r.persistor).Persist(ctx, ev)
	if err != nil {
		return err
	}
	if opensArc {
		// The item carries the event's seq as its trigger, so the worker
		// fires the continuation hook for exactly this report.
		if err := r.enqueue.Enqueue(ctx, common.ArcRef{SessionID: ref.SessionID, ArcID: *ev.CorrelationID}, agentID, stored[0].Seq); err != nil {
			return fmt.Errorf("enqueue the arc the report opened: %w", err)
		}
	}
	return nil
}

// reportEvent turns a report into the record of it. It returns the event and
// whether the event opens an arc, in which case its correlation ID is the
// fresh arc ID the caller enqueues.
func reportEvent(ref common.TaskRef, state *common.TaskState, metadata common.TaskMetadata, report TaskReport) (*events.Event, bool, error) {
	ev := &events.Event{SessionID: ref.SessionID}
	if state.Actor != nil {
		// The actor is copied forward: every event the task emits carries the
		// one the invoke recorded, so every arc one of them opens inherits it.
		ev.Actor = *state.Actor
	}

	switch report.Kind {
	case TaskReportNotification:
		class, ok := metadata.Notifications[report.Class]
		if !ok {
			return nil, false, fmt.Errorf("task kind %s has declared no notification class %q", metadata.Name, report.Class)
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
		arcID := common.NewEventID()
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
func SettleAbandonedTasks(ctx context.Context, persistor common.EventPersistor, sessionID string, h *history.History) (int, error) {
	// The session is the caller's to name; the fold does not carry it.
	persistor = common.AsSession(sessionID, persistor)
	settled := 0
	for _, task := range h.TasksInOrder() {
		if task.FinishedSeq != nil {
			continue
		}
		taskID := task.ID
		_, err := persistor.Persist(ctx, &events.Event{
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
