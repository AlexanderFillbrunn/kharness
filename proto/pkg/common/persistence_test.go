package common

import (
	"testing"
	"time"

	"fillbrunn.ai/kube-agent/proto/pkg/events"
	"fillbrunn.ai/kube-agent/proto/pkg/users"
)

const (
	testSessionID = "session"
	testArcID     = "arc-1"
)

// TaskStateOf reads one task's state out of a log without folding it: an
// unknown task does not exist, a running one carries its kind and actor, and
// a settled one carries its outcome — by result or by task-addressed cancel.
func TestTaskStateOfReadsOneTaskOutOfALog(t *testing.T) {
	at := time.Unix(0, 0)
	actor := users.Actor{UserID: "alice", TenantID: "acme"}
	originArc := testArcID
	invoke := func(id, name string) *events.Event {
		return &events.Event{
			SessionID: testSessionID, ID: id, Seq: 0, Timestamp: at,
			Typ: events.AsyncTaskInvokeEventType, CorrelationID: &originArc, Actor: actor,
			Payload: events.AsyncTaskInvokePayload{TaskName: name, Actor: actor},
		}
	}
	result := func(taskID string, outcome events.AsyncTaskOutcome) *events.Event {
		return &events.Event{
			SessionID: testSessionID, ID: "res-" + taskID, Seq: 1, Timestamp: at,
			Typ: events.AsyncTaskResultEventType, CorrelationID: &taskID,
			Payload: events.AsyncTaskResultPayload{CausationID: taskID, Outcome: outcome, LLMVisibility: events.LLMVisibilityNone},
		}
	}
	cancel := func(taskID string) *events.Event {
		reason := "stopped"
		task := taskID
		return &events.Event{
			SessionID: testSessionID, ID: "cx-" + taskID, Seq: 2, Timestamp: at,
			Typ: events.CancelEventType, CorrelationID: &task,
			Payload: events.CancelPayload{Reason: &reason, TaskID: &task},
		}
	}
	metaFailed := func(taskID string) *events.Event {
		return &events.Event{
			SessionID: testSessionID, ID: "launchres-" + taskID, Seq: 3, Timestamp: at,
			Typ: events.AsyncTaskLaunchResultEventType, CorrelationID: &taskID,
			Payload: events.AsyncTaskLaunchResultPayload{CausationID: taskID, ExternalRef: taskID, Launched: false, Error: "no runner", LLMVisibility: events.LLMVisibilityNone},
		}
	}
	otherSession := invoke("task-x", "auditor")
	otherSession.SessionID = "another-session"

	log := []*events.Event{
		invoke("task-a", "auditor"),
		invoke("task-b", "upgrader"),
		result("task-b", events.AsyncTaskOutcomeSuccess),
		invoke("task-c", "auditor"),
		cancel("task-c"),
		invoke("task-d", "auditor"),
		result("task-d", events.AsyncTaskOutcomeFailed),
		invoke("task-e", "auditor"),
		metaFailed("task-e"),
		otherSession,
	}

	if state := TaskStateOf(testSessionID, "task-unknown", log); state.Exists {
		t.Errorf("unknown task exists = true, want false")
	}

	state := TaskStateOf(testSessionID, "task-a", log)
	if !state.Exists || state.TaskName != "auditor" || state.Settled || state.Status != events.TaskStatusPending {
		t.Errorf("running task state = %+v, want exists, kind auditor, unsettled, PENDING", state)
	}
	if state.Actor == nil || state.Actor.UserID != "alice" {
		t.Errorf("actor = %v, want alice from the invoke", state.Actor)
	}

	if state := TaskStateOf(testSessionID, "task-b", log); !state.Settled || state.Status != events.TaskStatusSuccess {
		t.Errorf("result-settled task = %+v, want SETTLED SUCCESS", state)
	}
	if state := TaskStateOf(testSessionID, "task-c", log); !state.Settled || state.Status != events.TaskStatusCancelled {
		t.Errorf("cancel-settled task = %+v, want SETTLED CANCELLED", state)
	}
	if state := TaskStateOf(testSessionID, "task-d", log); !state.Settled || state.Status != events.TaskStatusFailed {
		t.Errorf("failed task = %+v, want SETTLED FAILED", state)
	}
	if state := TaskStateOf(testSessionID, "task-e", log); !state.Settled || state.Status != events.TaskStatusFailed {
		t.Errorf("launch-failed task = %+v, want SETTLED FAILED from its launch-result event", state)
	}
	if state := TaskStateOf("another-session", "task-x", log); !state.Exists {
		t.Error("the task of another session was not found in its own session")
	}
	if state := TaskStateOf(testSessionID, "task-x", log); state.Exists {
		t.Error("a task of another session was found in this one")
	}
}
