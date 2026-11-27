package history

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"fillbrunn.ai/kube-agent/proto/pkg/events"
	"fillbrunn.ai/kube-agent/proto/pkg/users"
)

// ToolCallStatus is the lifecycle state of a ToolCall. Every state other than
// PENDING is terminal.
type ToolCallStatus string

const (
	// ToolCallStatusPending is a call that has been requested but has not
	// produced an outcome yet.
	ToolCallStatusPending ToolCallStatus = "PENDING"
	// ToolCallStatusFailed is a call that ran and reported an error.
	ToolCallStatusFailed ToolCallStatus = "FAILED"
	// ToolCallStatusSuccess is a call that ran and returned a result.
	ToolCallStatusSuccess ToolCallStatus = "SUCCESS"
	// ToolCallStatusBlocked is a call an automatic policy refused before it
	// ran. No human was asked.
	ToolCallStatusBlocked ToolCallStatus = "BLOCKED"
	// ToolCallStatusRefused is a call a human refused when asked to approve
	// it. A policy would have blocked it without asking.
	ToolCallStatusRefused ToolCallStatus = "REFUSED"
	// ToolCallStatusCancelled is a call that was still pending, and had never
	// been dispatched, when its arc was cancelled. It provably never ran.
	//
	// A cancel does not settle a call already handed to its tool. That call
	// may still report, so it stays PENDING in the DISPATCHED phase.
	ToolCallStatusCancelled ToolCallStatus = "CANCELLED"
)

// MessageRole identifies who authored a piece of the conversation.
type MessageRole string

const (
	MessageRoleUser      MessageRole = "USER"
	MessageRoleAssistant MessageRole = "ASSISTANT"
	// MessageRoleInstruction marks a system or developer prompt rather than
	// anything a participant said.
	MessageRoleInstruction MessageRole = "INSTRUCTION"
)

// ToolEvent is a progress notification emitted by a tool while it runs. Events
// are informational; they never change the call's status.
type ToolEvent struct {
	ID string

	// Seq is the sequence number of the event that emitted this notification.
	Seq int

	Payload any
}

// ToolCall is one tool invocation requested by an assistant message.
type ToolCall struct {
	ID string

	// Seq is the sequence number of the event that requested the call. A
	// projection orders messages by the log, so every record keeps the
	// position it was written at.
	Seq int

	// MessageID is the assistant message that requested the call, so a
	// consumer can correlate the call with the message's content without
	// walking the arc.
	MessageID string

	// VendorID is the model provider's own identifier for the call, used to
	// match a result back to the request when replying to the provider.
	VendorID string

	ToolName string
	Args     any
	Result   any
	Status   ToolCallStatus

	// DispatchedAt and DispatchedSeq record the first time the call was handed
	// to its tool, and are nil for a call that provably never ran. A worker
	// that died mid-call leaves them set with no result behind them.
	DispatchedAt  *time.Time
	DispatchedSeq *int

	// Dispatches counts how often the call was handed to its tool. More than
	// once means a worker picked the call up again after another had already
	// started it, so the tool may have run twice.
	Dispatches int

	// FinishedAt is set when the call reaches a terminal status, and is nil
	// while it is still pending.
	FinishedAt *time.Time

	// FinishedSeq is the sequence number of the event that settled the call,
	// and is nil while it is still pending. A projection cut off before this
	// number treats the call as unanswered.
	FinishedSeq *int

	// Interrupt is the human decision this call is waiting on, or waited on,
	// and is nil for a call nobody was asked about. A call can only be put to
	// a human once.
	Interrupt *Interrupt

	Events []*ToolEvent
}

// ToolCallPhase is where a call stands, derived rather than stored. Status
// alone cannot say it: a requested call, one held for a human and one already
// handed to its tool are all PENDING.
type ToolCallPhase string

const (
	// ToolCallPhaseRequested is a call the model asked for that has not been
	// dispatched and is not waiting on anyone. Nothing has run.
	ToolCallPhaseRequested ToolCallPhase = "REQUESTED"
	// ToolCallPhaseAwaitingApproval is a call held until a human decides on
	// it.
	ToolCallPhaseAwaitingApproval ToolCallPhase = "AWAITING_APPROVAL"
	// ToolCallPhaseDispatched is a call that was handed to its tool and has no
	// outcome. Whether it is still running or was abandoned by a dead worker
	// cannot be told from the log, and neither can whether it took effect.
	ToolCallPhaseDispatched ToolCallPhase = "DISPATCHED"
	// ToolCallPhaseSettled is a call that reached a terminal status.
	ToolCallPhaseSettled ToolCallPhase = "SETTLED"
)

// Phase reports where the call stands. It is computed, so it cannot drift from
// the fold.
func (c *ToolCall) Phase() ToolCallPhase {
	switch {
	case c.Status != ToolCallStatusPending:
		return ToolCallPhaseSettled
	case c.DispatchedSeq != nil:
		return ToolCallPhaseDispatched
	case c.Interrupt != nil && c.Interrupt.Status == InterruptPending:
		return ToolCallPhaseAwaitingApproval
	default:
		return ToolCallPhaseRequested
	}
}

// AsyncTaskStatus is the lifecycle state of an asynchronous task. The
// vocabulary is defined in the events package, next to the outcome it is
// derived from, so a store query and the fold read the same one; history
// re-exports it under its own name.
type AsyncTaskStatus = events.TaskStatus

const (
	AsyncTaskPending   = events.TaskStatusPending
	AsyncTaskSuccess   = events.TaskStatusSuccess
	AsyncTaskFailed    = events.TaskStatusFailed
	AsyncTaskWaiting   = events.TaskStatusWaiting
	AsyncTaskCancelled = events.TaskStatusCancelled
)

// AsyncTaskEventType distinguishes task notifications from blocking input
// interactions. Every one is retained in AsyncTask.Events.
type AsyncTaskEventType string

const (
	AsyncTaskEventNotification   AsyncTaskEventType = "NOTIFICATION"
	AsyncTaskEventInputRequested AsyncTaskEventType = "INPUT_REQUESTED"
	AsyncTaskEventInputProvided  AsyncTaskEventType = "INPUT_PROVIDED"
)

// AsyncTaskEvent is one entry in a task's transcript, in the order it occurred.
type AsyncTaskEvent struct {
	ID        string             `json:"id"`
	Seq       int                `json:"seq"`
	Timestamp time.Time          `json:"timestamp"`
	Type      AsyncTaskEventType `json:"type"`

	// Class is the notification's own name for NOTIFICATION entries, and
	// Visibility is the LLMVisibility the event was written with.
	Class      string               `json:"class,omitempty"`
	Visibility events.LLMVisibility `json:"visibility,omitempty"`

	Payload any `json:"payload"`
}

// AsyncTask is a unit of work that runs outside the agent's turn and reports
// back through events. A task belongs to the session, not to its origin arc:
// the session-level task table is its only home, and the invoke event's place
// in the origin arc is its correlation.
type AsyncTask struct {
	ID string `json:"id"`

	// Seq is the sequence number of the event that invoked the task.
	Seq int `json:"seq"`

	// CorrelationID is the arc that started this task: the task's origin arc.
	CorrelationID string `json:"correlation_id"`

	// MessageID names the assistant message that requested the start, so the
	// projection can render the start as the tool call it was to the model.
	MessageID string `json:"message_id,omitempty"`

	TaskName string          `json:"task_name"`
	Status   AsyncTaskStatus `json:"status"`
	Args     any             `json:"args,omitempty"`
	Result   any             `json:"result,omitempty"`

	// ExternalRef is the ID the task's runner assigned it, recorded on the
	// task's ASYNC_TASK_LAUNCH_RESULT event; empty until that event is folded. A
	// report or callback naming it routes to this task.
	ExternalRef string `json:"external_ref,omitempty"`

	// Launch is the runner's raw response to the invocation, from the same
	// event. Nil when the launch failed or the event is not folded yet.
	Launch any `json:"launch,omitempty"`

	// Launched says whether the launch succeeded; LaunchSeq is the launch-result event's
	// seq. LaunchSeq nil means no launch was recorded yet: the task is starting
	// or its launch record was lost with a stopped run.
	Launched  bool `json:"launched,omitempty"`
	LaunchSeq *int `json:"meta_seq,omitempty"`

	// DispatchedSeq is set when a run handed the launch to the controller,
	// before the launch-result event came back. An invoke with a dispatch but no launch result
	// is a launch whose fate a stopped run never recorded: it is not retried.
	DispatchedSeq *int `json:"dispatched_seq,omitempty"`

	// Interrupt is the human decision this task's start is held up for, if
	// one was asked and is pending or resolved.
	Interrupt *Interrupt `json:"-"`

	// BlockedMessage is the guardrail's refusal, recorded when a policy
	// blocked the launch; the launch-result event beside it settles the task.
	BlockedMessage string `json:"blocked_message,omitempty"`

	// LaunchAnswer is the outcome a policy hook supplied when it intercepted
	// the start without the task running; the projection renders it as the
	// start call's result. LaunchResultText overrides the text the model is
	// told, so a hook can hide that the task never launched.
	LaunchAnswer     *events.TaskResult `json:"launch_answer,omitempty"`
	LaunchResultText string             `json:"launch_result_text,omitempty"`

	// Actor is who started the task, recorded on the invoke and copied
	// forward onto every event the task emits. It records who started the
	// work; it is not a live grant of authority.
	Actor *users.Actor `json:"actor,omitempty"`

	// CancelledBy is who cancelled the task, set by a task-addressed cancel.
	CancelledBy *users.Actor `json:"cancelled_by,omitempty"`

	// WaitingArc names the arc the task's outstanding question opened, and is
	// nil while the task is not waiting. An answer is written correlated to
	// that arc.
	WaitingArc *string `json:"waiting_arc,omitempty"`

	// ResultArc names the arc the task's result opened, where the result was
	// written AsMessage. The completion of such a task is that arc of its own
	// rather than a note in the origin arc.
	ResultArc string `json:"result_arc,omitempty"`

	// InResultTrail is how many InResult events the task's result message
	// carries, or -1 for the whole trail. It is stamped on the invoke event
	// from the task kind's declaration, so it is log content and resumes with
	// the rest of the task table. Zero is normalized to -1 at fold.
	InResultTrail int `json:"in_result_trail,omitempty"`

	// InResultTrailChars caps how many characters the InResult trail in the
	// task's result message carries; zero or negative means no cap. Like
	// InResultTrail it is stamped on the invoke event.
	InResultTrailChars int `json:"in_result_trail_chars,omitempty"`

	// InResultTrailHeader says whether each InResult trail entry in the
	// task's result message carries a header line with its 1-based index in
	// the task's trail and its timestamp. Like the caps above it is stamped
	// on the invoke event.
	InResultTrailHeader bool `json:"in_result_trail_header,omitempty"`

	// Events is the full transcript of the task: notifications it emitted and
	// every input interaction it went through.
	Events []AsyncTaskEvent `json:"events"`

	// FinishedAt is set when the task reaches a terminal status, and is nil
	// while it is still running or waiting.
	FinishedAt *time.Time `json:"finished_at,omitempty"`

	// FinishedSeq is the sequence number of the event that settled the task,
	// and is nil while it is still running or waiting.
	FinishedSeq *int `json:"finished_seq,omitempty"`
}

type ArcStatus string

const (
	// ArcStatusPending represents an arc that is currently being worked on.
	ArcStatusPending ArcStatus = "PENDING"
	// ArcStatusInterrupted represents an arc whose tool call is waiting for a human to approve it.
	ArcStatusInterrupted ArcStatus = "INTERRUPTED"
	// ArcStatusDone represents an arc that has completed through an agent message with no tool calls.
	ArcStatusDone ArcStatus = "DONE"
	// ArcStatusCancelled represents an arc that is closed due to user cancellation.
	ArcStatusCancelled ArcStatus = "CANCELLED"
	// ArcStatusError represents an arc that is closed due to an error.
	ArcStatusError ArcStatus = "ERROR"
)

// Arc represents one opening record in the conversation — a user message or a
// task event written AsMessage — including its responses and other events.
// Every other record carries the arc's ID as its correlation ID, which is what
// nests it here.
type Arc struct {
	// ID is the correlation ID every record in this arc is tagged with.
	ID string `json:"id"`

	Seq int `json:"seq"`

	// Content is what opened the arc: the user's words, or for a task-opened
	// arc the note naming the task followed by the event's own content.
	Content string `json:"content"`

	Timestamp time.Time `json:"timestamp"`

	// Actor is who opened the arc: the user who sent the message, or the
	// actor the task event carried.
	Actor *users.Actor `json:"actor,omitempty"`

	// OpenedByTask names the task whose event opened this arc, and is empty
	// for an arc a user message opened.
	OpenedByTask string `json:"opened_by_task,omitempty"`

	// Cancel is set once the arc has been cancelled, and is nil otherwise. An
	// arc can only be cancelled once.
	Cancel *Cancel `json:"cancel"`

	// Closed is set once the agent closed the arc, and is nil while it is
	// open. An arc can only be closed once.
	Closed *ArcClosed `json:"closed,omitempty"`

	// Responses are the agent's replies within this arc, in order. One arc can
	// produce several, since each round of tool calls yields another message.
	Responses []*AssistantMessage `json:"responses"`

	Errors []*Error `json:"errors"`

	// Suspensions are the ARC_SUSPENDED records against this arc, in order:
	// each one pauses the arc until the compaction named by its causation
	// lands. The derived state of each is read off the log, never stored.
	Suspensions []*ArcSuspension `json:"suspensions,omitempty"`

	// Interrupts are the arc-wide interrupts raised against this arc, in
	// order: each one holds the whole arc until a human answers it. A call
	// interrupted individually carries its own interrupt on the call.
	Interrupts []*Interrupt `json:"interrupts,omitempty"`
}

// Status computes the status of an arc. It is not a field lookup and should be
// used sparingly.
//
// Three records can close an arc: the close the agent recorded, a fatal error,
// and a cancel. A reply that requested no calls closes nothing on its own.
func (a Arc) Status() ArcStatus {
	status := ArcStatusPending
	closedAt := 0
	closedBy := func(seq int, closed ArcStatus) {
		if status == ArcStatusPending || seq < closedAt {
			status, closedAt = closed, seq
		}
	}

	if a.Closed != nil {
		closedBy(a.Closed.Seq, ArcStatusDone)
	}
	for _, e := range a.Errors {
		if e.IsFatal {
			closedBy(e.Seq, ArcStatusError)
		}
	}
	if a.Cancel != nil {
		closedBy(a.Cancel.Seq, ArcStatusCancelled)
	}
	if status != ArcStatusPending {
		return status
	}

	// An arc waiting on a human is waiting on one of its calls or on an
	// arc-wide interrupt.
	for _, response := range a.Responses {
		for _, call := range response.ToolCalls {
			if call.Interrupt != nil && call.Interrupt.Status == InterruptPending {
				return ArcStatusInterrupted
			}
		}
	}
	for _, interrupt := range a.Interrupts {
		if interrupt.Status == InterruptPending {
			return ArcStatusInterrupted
		}
	}
	return ArcStatusPending
}

// AssistantMessage represents the response of the agent to a user message.
type AssistantMessage struct {
	ID  string `json:"id"`
	Seq int    `json:"seq"`

	// CorrelationID is the arc this response belongs to.
	CorrelationID string `json:"correlation_id"`

	// VendorID is the model provider's own identifier for this message.
	VendorID *string `json:"vendor_id,omitempty"`

	Timestamp time.Time `json:"timestamp"`
	Content   string    `json:"content"`

	// Reasoning is the provider's own record of how the model arrived at this
	// message, kept as the opaque bytes it arrived as. Providers that take it
	// back require it verbatim, so nothing here reads into it.
	Reasoning []byte `json:"reasoning,omitempty"`

	// Usage is what this message cost, and is nil where the provider reported
	// nothing. Nil is not zero, so a total over an arc is only as complete as
	// the messages that carry one.
	Usage *events.TokenUsage `json:"usage,omitempty"`

	// ToolCalls are the calls this message requested. A message with none is
	// the agent's answer rather than a step towards one.
	ToolCalls []*ToolCall `json:"tool_calls"`
}

// InterruptStatus is the lifecycle state of an Interrupt.
type InterruptStatus string

const (
	InterruptPending  InterruptStatus = "pending"
	InterruptApproved InterruptStatus = "approved"
	InterruptRejected InterruptStatus = "rejected"
	// InterruptAnswered is an arc-wide interrupt a human answered with
	// information rather than a verdict on a call.
	InterruptAnswered  InterruptStatus = "answered"
	InterruptCancelled InterruptStatus = "cancelled"
)

// Interrupt is a record held up until a human answers it: the
// human-in-the-loop gate. With CausationID set it holds one tool call up for
// a verdict; without it it holds the whole arc up for an answer. It is not a
// general pause. A policy that refuses a call on its own blocks it instead,
// without asking anyone.
type Interrupt struct {
	ID string `json:"id"`

	// Seq is the sequence number of the event that raised the interrupt.
	Seq int `json:"seq"`

	// ResolvedSeq is the sequence number of the event that answered or
	// cancelled it, and is nil while it is still pending.
	ResolvedSeq *int `json:"resolved_seq,omitempty"`

	// CorrelationID is the arc the held-up call belongs to. A resumed run picks
	// up the arc from here, and cancelling that arc cancels the interrupt.
	CorrelationID string `json:"correlation_id"`

	Timestamp time.Time `json:"timestamp"`

	// CausationID is the call this decision holds up, empty for an arc-wide
	// interrupt that holds the arc in CorrelationID rather than one call.
	// The call is nested under the assistant message that requested it and
	// carries this interrupt.
	CausationID string `json:"causation_id,omitempty"`

	// Reason is what the human was asked to decide about, in words.
	Reason string `json:"reason"`

	// Context is what the decision is about beyond the call's own arguments.
	// It is written when the interrupt is raised and never changes, so
	// anything derived from it is stable for the life of the interrupt.
	Context any `json:"context,omitempty"`

	// Response and Responder are set once a human answers, and are nil while
	// the interrupt is pending or if it was cancelled unanswered.
	Response  any          `json:"response,omitempty"`
	Responder *users.Actor `json:"responder,omitempty"`

	Status InterruptStatus `json:"status"`
}

// ArcClosed records the close of one arc. The agent closes an arc once a
// response has requested no tool calls and no async task under it is still
// running.
type ArcClosed struct {
	ID        string    `json:"id"`
	Seq       int       `json:"seq"`
	Timestamp time.Time `json:"timestamp"`

	// CausationID is the assistant message that concluded the arc. Nothing
	// reads it to project; it is here so a closed arc can be traced back to
	// the message that closed it.
	CausationID string `json:"causation_id"`
}

// Cancel records cancellation of one arc. Cancelling stops the arc's pending
// tool calls, along with any decision they were waiting on, and its unfinished
// async tasks.
type Cancel struct {
	ID        string    `json:"id"`
	Seq       int       `json:"seq"`
	Arc       string    `json:"arc"`
	Timestamp time.Time `json:"timestamp"`

	// Actor is who cancelled, when a human did. Nil means the arc was
	// abandoned rather than withdrawn, such as a crashed run swept up later.
	Actor  *users.Actor `json:"actor,omitempty"`
	Reason *string      `json:"reason,omitempty"`

	// TerminatedWork says whether this cancel stopped anything at all. A
	// cancel that arrives after the arc has already finished is still
	// recorded, but leaves the completed exchange rendered as completed.
	TerminatedWork bool `json:"terminated_work"`
}

// Error is a failure recorded against the log rather than a message, since
// nobody said it. A fatal error ends its arc, and a non-fatal one is recorded
// while the arc carries on.
type Error struct {
	ID            string    `json:"id"`
	Seq           int       `json:"seq"`
	CorrelationID string    `json:"correlation_id"`
	Timestamp     time.Time `json:"timestamp"`
	Message       string    `json:"message"`

	// IsFatal marks a failure that ended the arc rather than one it survived.
	IsFatal bool `json:"is_fatal"`

	// CausationID is what the failure was about, when it was about one thing.
	CausationID *string `json:"causation_id,omitempty"`
}

// History is the folded state of one session's event log. It names no session:
// the session lives at the edges — the store keys, the work items, the
// persistor decorators — and every event in the log carries its own
// SessionID. It is also the snapshot format: EncodeFold serializes it as
// JSON, so the field tags here are the stored shape.
type History struct {
	// Arcs holds one arc per opening record, in the order they were opened.
	Arcs []*Arc `json:"arcs"`

	// Tasks is the session-level task table: every task in the session, keyed
	// by task ID. A task is an object of the session, not of its origin arc;
	// the invoke event's place in the origin arc is its correlation.
	Tasks map[string]*AsyncTask `json:"tasks"`

	// LastSeq is the sequence number of the last event applied. Events arrive
	// in increasing sequence order and may skip numbers, since a history holds
	// one arc's slice of the log.
	LastSeq int `json:"last_seq"`

	// LastCompaction is the compaction in force: the COMPACTED with the
	// greatest ThroughSeq, regardless of when it arrived. Coverage, not
	// recency, decides, so a duplicated or out-of-order compaction is
	// harmless. Nil where the session has never compacted.
	LastCompaction *Compacted `json:"last_compaction,omitempty"`

	// CompactionRequests holds every COMPACTION_REQUESTED in the log, keyed by
	// event ID, in the order they were written. A request is satisfied once a
	// COMPACTED covering its watermark has landed.
	CompactionRequests []*CompactionRequest `json:"compaction_requests,omitempty"`
}

// NewHistory returns an empty history. LastSeq starts at -1 so that the
// log's first event, numbered 0, is accepted.
func NewHistory() *History {
	return &History{
		Tasks:   map[string]*AsyncTask{},
		LastSeq: -1,
	}
}

// TasksInOrder returns the session's tasks in log order.
func (h *History) TasksInOrder() []*AsyncTask {
	tasks := make([]*AsyncTask, 0, len(h.Tasks))
	for _, task := range h.Tasks {
		tasks = append(tasks, task)
	}
	sort.Slice(tasks, func(i, j int) bool { return tasks[i].Seq < tasks[j].Seq })
	return tasks
}

// TasksOfArc returns the tasks an arc started, in log order.
func (h *History) TasksOfArc(arcID string) []*AsyncTask {
	var tasks []*AsyncTask
	for _, task := range h.TasksInOrder() {
		if task.CorrelationID == arcID {
			tasks = append(tasks, task)
		}
	}
	return tasks
}

// OpeningContent returns what an arc's opening record says: its Content, with
// the InResult trail of its task folded in where the arc was opened by a
// task's result. The trail carries the task's last InResultTrail InResult
// events, or all of them at -1, capped further at InResultTrailChars
// characters where that is positive. Both caps keep the newest entries. Where
// InResultTrailHeader is set, each entry carries a header line with its
// 1-based index in the task's whole trail and its timestamp, so the caps do
// not renumber the surviving entries.
func (h *History) OpeningContent(arc *Arc) string {
	if arc.OpenedByTask == "" {
		return arc.Content
	}
	task, ok := h.Tasks[arc.OpenedByTask]
	if !ok || task.ResultArc != arc.ID {
		return arc.Content
	}

	var trail []string
	index := 0
	for _, event := range task.Events {
		if event.Type == AsyncTaskEventNotification && event.Visibility.Effective() == events.LLMVisibilityInResult {
			index++
			entry := resultText(event.Payload)
			if task.InResultTrailHeader {
				entry = fmt.Sprintf("Event %d · %s\n%s", index, formatTime(event.Timestamp), entry)
			}
			trail = append(trail, entry)
		}
	}
	if task.InResultTrail >= 0 && len(trail) > task.InResultTrail {
		trail = trail[len(trail)-task.InResultTrail:]
	}
	if cap := task.InResultTrailChars; cap > 0 {
		// The character cap drops the oldest entries until the joined trail
		// fits; a single entry that alone exceeds the cap is truncated to it,
		// so the trail never vanishes entirely.
		for len(trail) > 1 && trailChars(trail) > cap {
			trail = trail[1:]
		}
		if len(trail) == 1 {
			if runes := []rune(trail[0]); len(runes) > cap {
				trail[0] = string(runes[:cap]) + fmt.Sprintf(
					"\n… [truncated: the event exceeded the %d-character trail cap; the log holds the whole event]", cap)
			}
		}
	}
	if len(trail) == 0 {
		return arc.Content
	}
	return arc.Content + "\n" + strings.Join(trail, "\n")
}

// trailChars is the length of a trail's entries joined with newlines, in
// characters.
func trailChars(trail []string) int {
	n := len(trail) - 1 // the newlines between entries
	for _, line := range trail {
		n += len([]rune(line))
	}
	return n
}

// FoldStateFromHistory rebuilds the lookup index for an already-folded
// history, so a history loaded from storage can go on accepting events without
// replaying the log that produced it.
func FoldStateFromHistory(h *History) *FoldState {
	state := NewFoldState()
	for _, arc := range h.Arcs {
		state.arcs[arc.ID] = arc

		for _, msg := range arc.Responses {
			state.responses[msg.ID] = msg
			for _, tc := range msg.ToolCalls {
				state.toolCalls[tc.ID] = tc
				if tc.Interrupt != nil {
					state.interrupts[tc.Interrupt.ID] = tc.Interrupt
				}
			}
		}
		for _, interrupt := range arc.Interrupts {
			state.interrupts[interrupt.ID] = interrupt
		}
	}
	for _, task := range h.Tasks {
		state.asyncTasks[task.ID] = task
	}
	for _, req := range h.CompactionRequests {
		state.compactionRequests[req.ID] = req
	}
	return state
}

// Replay left-folds the event log into a history: a resume on an empty fold,
// which is all it is. The events must be in increasing sequence order, and
// may skip numbers. The session is not the fold's business: the events carry
// their own SessionID, and a store hands out one session's slice, so nothing
// here checks it.
//
// An event the fold rejects stops the replay, so a history cannot disagree
// with the log it was built from.
func Replay(evs []*events.Event) (*History, error) {
	return Resume(NewHistory(), evs)
}

// Update folds one event into the history, using state to find the records the
// event refers to. The event must come after the last one applied, and its
// type must match its payload. Neither the history nor LastSeq changes if it
// does not.
//
// Order matters because an event names the record it acts on by ID, so that
// record has to be folded already. Sequence numbers may skip.
func (h *History) Update(ev *events.Event, state *FoldState) error {
	if ev.Seq <= h.LastSeq {
		return fmt.Errorf("expected event sequence number above %d, got %d", h.LastSeq, ev.Seq)
	}
	if ev.Typ != ev.Payload.EventType() {
		return fmt.Errorf("event type and payload disagree: %s != %s", ev.Typ, ev.Payload.EventType())
	}
	err := h.applyEvent(ev, state)
	if err != nil {
		return err
	}
	h.LastSeq = ev.Seq
	return nil
}

// applyFunc folds a single event of one type into the history.
type applyFunc func(h *History, ev *events.Event, state *FoldState) error

// applyFuncs dispatches each event type to the function that folds it. An
// event type missing from this table is rejected rather than ignored.
var applyFuncs = map[events.EventType]applyFunc{
	events.UserMessageEventType:             (*History).applyUserMessage,
	events.AssistantMessageEventType:        (*History).applyAssistantMessage,
	events.ToolCallEventType:                (*History).applyToolCall,
	events.ToolDispatchedEventType:          (*History).applyToolDispatched,
	events.ToolResultEventType:              (*History).applyToolResult,
	events.ToolEventEventType:               (*History).applyToolEvent,
	events.InterruptEventType:               (*History).applyInterrupt,
	events.InterruptResponseEventType:       (*History).applyInterruptResponse,
	events.AsyncTaskInvokeEventType:         (*History).applyAsyncTaskInvoke,
	events.AsyncTaskResultEventType:         (*History).applyAsyncTaskResult,
	events.AsyncTaskEventEventType:          (*History).applyAsyncTaskEvent,
	events.AsyncTaskInputRequestedEventType: (*History).applyAsyncTaskInputRequested,
	events.AsyncTaskInputProvidedEventType:  (*History).applyAsyncTaskInputProvided,
	events.AsyncTaskLaunchResultEventType:   (*History).applyAsyncTaskLaunchResult,
	events.ErrorEventType:                   (*History).applyError,
	events.GuardrailBlockedEventType:        (*History).applyGuardrailBlocked,
	events.CancelEventType:                  (*History).applyCancel,
	events.ArcClosedEventType:               (*History).applyArcClosed,
	events.CompactionRequestedEventType:     (*History).applyCompactionRequested,
	events.ArcSuspendedEventType:            (*History).applyArcSuspended,
	events.CompactedEventType:               (*History).applyCompacted,
}

// applyEvent routes an event to its fold function.
func (h *History) applyEvent(ev *events.Event, state *FoldState) error {
	fn, ok := applyFuncs[ev.Typ]
	if !ok {
		return fmt.Errorf("unhandled event type %q", ev.Typ)
	}
	if err := fn(h, ev, state); err != nil {
		return fmt.Errorf("apply %s: %w", ev.Typ, err)
	}
	return nil
}

// applyUserMessage opens a new arc. It is one of the two kinds of opening
// record; the other is a task event written AsMessage.
func (h *History) applyUserMessage(ev *events.Event, state *FoldState) error {
	pl, ok := ev.Payload.(events.UserMessagePayload)
	if !ok {
		return errors.New("expected payload type to be UserMessagePayload")
	}
	if ev.CorrelationID == nil {
		return fmt.Errorf("event %q of type %q has no correlation ID", ev.ID, ev.Typ)
	}

	arc := &Arc{
		ID:        *ev.CorrelationID,
		Seq:       ev.Seq,
		Timestamp: ev.Timestamp,
		Content:   pl.Content,
		Actor:     &pl.Actor,
	}
	h.Arcs = append(h.Arcs, arc)
	state.arcs[arc.ID] = arc

	return nil
}

// applyAssistantMessage appends a reply to the arc it answers.
func (h *History) applyAssistantMessage(ev *events.Event, state *FoldState) error {
	pl, ok := ev.Payload.(events.AssistantMessagePayload)
	if !ok {
		return errors.New("expected payload type to be AssistantMessagePayload")
	}
	if ev.CorrelationID == nil {
		return fmt.Errorf("event %q of type %q has no correlation ID", ev.ID, ev.Typ)
	}
	arc, ok := state.arcs[*ev.CorrelationID]
	if !ok {
		return fmt.Errorf("no arc with ID=%s found for assistant message", *ev.CorrelationID)
	}
	amsg := &AssistantMessage{
		ID:            ev.ID,
		Seq:           ev.Seq,
		CorrelationID: *ev.CorrelationID,
		Timestamp:     ev.Timestamp,
		VendorID:      pl.VendorID,
		Content:       pl.Content,
		Reasoning:     pl.Reasoning,
		Usage:         pl.Usage,
	}
	arc.Responses = append(arc.Responses, amsg)
	state.responses[amsg.ID] = amsg

	return nil
}

// applyToolCall attaches a pending call to the assistant message that
// requested it.
func (h *History) applyToolCall(ev *events.Event, state *FoldState) error {
	pl, ok := ev.Payload.(events.ToolCallPayload)
	if !ok {
		return errors.New("expected payload type to be ToolCallPayload")
	}
	tc := &ToolCall{
		ID:        ev.ID,
		Seq:       ev.Seq,
		MessageID: pl.MessageID,
		VendorID:  pl.VendorID,
		ToolName:  pl.ToolName,
		Args:      pl.Args,
		Status:    ToolCallStatusPending,
	}
	amsg, ok := state.responses[pl.MessageID]
	if !ok {
		return fmt.Errorf("no assistant message with ID=%s found for tool call", pl.MessageID)
	}
	amsg.ToolCalls = append(amsg.ToolCalls, tc)
	state.toolCalls[tc.ID] = tc
	return nil
}

// applyToolDispatched records that a call was handed to its tool. It validates
// nothing beyond the call existing, because it observes something that already
// happened rather than a transition the fold may refuse.
//
// So a dispatch is recorded even for a call that is already terminal, which is
// the case where knowing the tool may have run matters most. It leaves Status
// alone, so every status but PENDING stays terminal.
func (h *History) applyToolDispatched(ev *events.Event, state *FoldState) error {
	pl, ok := ev.Payload.(events.ToolDispatchedPayload)
	if !ok {
		return errors.New("expected payload type to be ToolDispatchedPayload")
	}
	tc, ok := state.toolCalls[pl.CausationID]
	if !ok {
		// A dispatch naming a task rather than a call marks the launch as
		// handed off: the launch-result event that follows records how it ended.
		task, isTask := state.asyncTasks[pl.CausationID]
		if !isTask {
			return fmt.Errorf("no tool call with ID=%s found for tool dispatch", pl.CausationID)
		}
		if task.DispatchedSeq == nil {
			task.DispatchedSeq = &ev.Seq
		}
		return nil
	}
	tc.Dispatches++
	if tc.DispatchedSeq == nil {
		tc.DispatchedAt = &ev.Timestamp
		tc.DispatchedSeq = &ev.Seq
	}
	return nil
}

// applyToolResult settles a pending call as successful or failed. A result for
// a call that already reached a terminal status is dropped, so a call blocked
// or cancelled mid-flight keeps that outcome.
func (h *History) applyToolResult(ev *events.Event, state *FoldState) error {
	pl, ok := ev.Payload.(events.ToolResultPayload)
	if !ok {
		return errors.New("expected payload type to be ToolResultPayload")
	}
	tc, ok := state.toolCalls[pl.CausationID]
	if !ok {
		return fmt.Errorf("no tool call with ID=%s found for tool call", pl.CausationID)
	}
	if tc.Status != ToolCallStatusPending {
		return nil
	}
	tc.Result = pl.Result
	tc.FinishedAt = &ev.Timestamp
	tc.FinishedSeq = &ev.Seq
	if pl.IsError {
		tc.Status = ToolCallStatusFailed
	} else {
		tc.Status = ToolCallStatusSuccess
	}

	return nil
}

// applyToolEvent appends a progress notification to the call that emitted it.
func (h *History) applyToolEvent(ev *events.Event, state *FoldState) error {
	pl, ok := ev.Payload.(events.ToolEventPayload)
	if !ok {
		return errors.New("expected payload type to be ToolEventPayload")
	}
	tc, ok := state.toolCalls[pl.CausationID]
	if !ok {
		return fmt.Errorf("no tool call with ID=%s found for tool call", pl.CausationID)
	}
	event := &ToolEvent{ID: ev.ID, Seq: ev.Seq, Payload: pl}
	tc.Events = append(tc.Events, event)
	return nil
}

// applyInterrupt holds a record up until a human answers it. With a
// causation ID it holds one tool call, which can only be put to a human once
// and only while it is still pending. Without one it holds the whole arc the
// correlation ID names, which has to exist and still be open: an interrupt on
// a closed, cancelled or failed arc could never be answered usefully.
func (h *History) applyInterrupt(ev *events.Event, state *FoldState) error {
	pl, ok := ev.Payload.(events.InterruptPayload)
	if !ok {
		return errors.New("expected payload type to be InterruptPayload")
	}
	if ev.CorrelationID == nil {
		return fmt.Errorf("event %q of type %q has no correlation ID", ev.ID, ev.Typ)
	}
	interrupt := &Interrupt{
		ID:            ev.ID,
		Seq:           ev.Seq,
		CorrelationID: *ev.CorrelationID,
		CausationID:   pl.CausationID,
		Timestamp:     ev.Timestamp,
		Reason:        pl.Reason,
		Context:       pl.Context,
		Status:        InterruptPending,
	}

	if pl.CausationID == "" {
		arc, ok := state.arcs[*ev.CorrelationID]
		if !ok {
			return fmt.Errorf("no arc with ID=%s found for interrupt", *ev.CorrelationID)
		}
		if arc.Closed != nil || arc.Cancel != nil || fatalError(arc) != nil {
			return fmt.Errorf("arc %s is %s and cannot be interrupted", arc.ID, arc.Status())
		}
		arc.Interrupts = append(arc.Interrupts, interrupt)
		state.interrupts[interrupt.ID] = interrupt
		return nil
	}

	if task, isTask := state.asyncTasks[pl.CausationID]; isTask {
		// An interrupt holding up a task's start: it hangs off the task and
		// off the arc, so the arc reads as interrupted while it is pending.
		arc, ok := state.arcs[*ev.CorrelationID]
		if !ok {
			return fmt.Errorf("no arc with ID=%s found for interrupt", *ev.CorrelationID)
		}
		if task.Interrupt != nil {
			return fmt.Errorf("task %s already has interrupt %s", task.ID, task.Interrupt.ID)
		}
		task.Interrupt = interrupt
		arc.Interrupts = append(arc.Interrupts, interrupt)
		state.interrupts[interrupt.ID] = interrupt
		return nil
	}
	call, ok := state.toolCalls[pl.CausationID]
	if !ok {
		return fmt.Errorf("no tool call with ID=%s found for interrupt", pl.CausationID)
	}
	if call.Status != ToolCallStatusPending {
		return fmt.Errorf("tool call %s is %s and cannot be put to a human", call.ID, call.Status)
	}
	if call.Interrupt != nil {
		return fmt.Errorf("tool call %s already has interrupt %s", call.ID, call.Interrupt.ID)
	}
	call.Interrupt = interrupt
	state.interrupts[interrupt.ID] = interrupt
	return nil
}

// applyInterruptResponse records the human's answer. An arc-wide interrupt
// carries no verdict, so its response is information and only settles the
// interrupt; a rejection of it names no call and is refused. A call interrupt
// is a verdict: a refusal settles the call, since it is never going to run,
// and an approval leaves it pending.
//
// A decision on an interrupt that is no longer pending is dropped.
func (h *History) applyInterruptResponse(ev *events.Event, state *FoldState) error {
	pl, ok := ev.Payload.(events.InterruptResponsePayload)
	if !ok {
		return errors.New("expected payload type to be InterruptResponsePayload")
	}
	interrupt, ok := state.interrupts[pl.CausationID]
	if !ok {
		return fmt.Errorf("no interrupt with ID=%s found for interrupt response", pl.CausationID)
	}
	if interrupt.Status != InterruptPending {
		return nil
	}
	if interrupt.CausationID == "" {
		if pl.IsRejected {
			return fmt.Errorf("interrupt %s holds no call, so a rejection would settle nothing", interrupt.ID)
		}
		interrupt.Responder = &pl.Actor
		interrupt.Response = pl.Response
		interrupt.ResolvedSeq = &ev.Seq
		interrupt.Status = InterruptAnswered
		return nil
	}

	if _, isTask := state.asyncTasks[interrupt.CausationID]; isTask {
		// A task start held for a human: a rejection refuses the start, which
		// the launch step records as a failed launch.
		interrupt.Responder = &pl.Actor
		interrupt.Response = pl.Response
		interrupt.ResolvedSeq = &ev.Seq
		if pl.IsRejected {
			interrupt.Status = InterruptRejected
		} else {
			interrupt.Status = InterruptApproved
		}
		return nil
	}

	call, ok := state.toolCalls[interrupt.CausationID]
	if !ok {
		return fmt.Errorf("no tool call with ID=%s found for interrupt %s", interrupt.CausationID, interrupt.ID)
	}

	interrupt.Responder = &pl.Actor
	interrupt.Response = pl.Response
	interrupt.ResolvedSeq = &ev.Seq
	if !pl.IsRejected {
		interrupt.Status = InterruptApproved
		return nil
	}

	interrupt.Status = InterruptRejected
	if call.Status == ToolCallStatusPending {
		call.Status = ToolCallStatusRefused
		call.FinishedAt = &ev.Timestamp
		call.FinishedSeq = &ev.Seq
		call.Result = pl.Response
	}
	return nil
}

// applyAsyncTaskInvoke starts a task in the session's task table, under the
// arc that launched it. The invoke is recorded inside the invoking arc; every
// later task event names the task by ID in its payload.
func (h *History) applyAsyncTaskInvoke(ev *events.Event, state *FoldState) error {
	pl, ok := ev.Payload.(events.AsyncTaskInvokePayload)
	if !ok {
		return errors.New("expected payload type to be AsyncTaskInvokePayload")
	}
	if ev.CorrelationID == nil {
		return fmt.Errorf("event %q of type %q has no correlation ID", ev.ID, ev.Typ)
	}
	arc, ok := state.arcs[*ev.CorrelationID]
	if !ok {
		return fmt.Errorf("no arc with ID=%s found for async task invoke", *ev.CorrelationID)
	}
	_ = arc

	// Zero on the invoke is unset, and folds as the whole trail, matching the
	// task kind declaration's own default.
	inResultTrail := pl.InResultTrail
	if inResultTrail == 0 {
		inResultTrail = -1
	}
	task := &AsyncTask{
		ID:                  ev.ID,
		Seq:                 ev.Seq,
		CorrelationID:       *ev.CorrelationID,
		MessageID:           pl.MessageID,
		TaskName:            pl.TaskName,
		Status:              AsyncTaskPending,
		Args:                pl.Args,
		InResultTrail:       inResultTrail,
		InResultTrailChars:  pl.InResultTrailChars,
		InResultTrailHeader: pl.InResultTrailHeader,
		Events:              make([]AsyncTaskEvent, 0),
	}
	if pl.Actor != (users.Actor{}) {
		actor := pl.Actor
		task.Actor = &actor
	}

	h.Tasks[task.ID] = task
	state.asyncTasks[task.ID] = task
	return nil
}

// routeTaskEvent decides where a task-emitted event goes from its correlation
// and its payload's LLMVisibility, and cross-checks the two: AsMessage must
// correlate to an arc, None and InResult to the task. It returns the arc the
// event attaches to or opens, which is nil for a task-correlated event.
func (h *History) routeTaskEvent(ev *events.Event, state *FoldState, causationID string) (*Arc, error) {
	if ev.CorrelationID == nil {
		return nil, fmt.Errorf("event %q of type %q has no correlation ID", ev.ID, ev.Typ)
	}
	corr := *ev.CorrelationID
	visibility := events.LLMVisibilityOf(ev.Payload).Effective()

	// The task the event is about has to be in the session table already.
	if _, ok := state.asyncTasks[causationID]; !ok {
		return nil, fmt.Errorf("no async task with ID=%s found for %s", causationID, ev.Typ)
	}

	if visibility == events.LLMVisibilityAsMessage {
		// AsMessage correlates to an arc. Naming a task is a mismatch.
		if _, isTask := state.asyncTasks[corr]; isTask {
			return nil, fmt.Errorf("%s event %q carries AsMessage visibility but correlates to task %s", ev.Typ, ev.ID, corr)
		}
		if arc, ok := state.arcs[corr]; ok {
			// The correlation names an arc that exists — the INPUT_PROVIDED
			// answer correlated to the ask's arc — so the event attaches to it
			// and opens no other.
			return arc, nil
		}
		return nil, nil // minted by the caller, which composes the content
	}

	// None and InResult correlate to the task. Naming an arc is a mismatch.
	if _, isArc := state.arcs[corr]; isArc {
		return nil, fmt.Errorf("%s event %q carries %s visibility but correlates to arc %s", ev.Typ, ev.ID, visibility, corr)
	}
	if corr != causationID {
		return nil, fmt.Errorf("%s event %q correlates to task %s but names task %s", ev.Typ, ev.ID, corr, causationID)
	}
	if _, isTask := state.asyncTasks[corr]; !isTask {
		return nil, fmt.Errorf("no async task with ID=%s found for %s", corr, ev.Typ)
	}
	return nil, nil
}

// mintTaskArc opens the arc an AsMessage task event opens, with ID, seq and
// actor taken from the event, and content composed from the task and the
// event's own payload.
func (h *History) mintTaskArc(ev *events.Event, state *FoldState, task *AsyncTask, content string) *Arc {
	arc := &Arc{
		ID:           *ev.CorrelationID,
		Seq:          ev.Seq,
		Timestamp:    ev.Timestamp,
		Content:      content,
		OpenedByTask: task.ID,
	}
	actor := ev.Actor
	arc.Actor = &actor
	h.Arcs = append(h.Arcs, arc)
	state.arcs[arc.ID] = arc
	return arc
}

// taskArcContent composes the opening content of a task-opened arc: a note
// naming the task, followed by the event's own content.
func taskArcContent(task *AsyncTask, note, body string) string {
	return fmt.Sprintf("Task %s with id %s (%s)\n%s", task.TaskName, task.ID, note, body)
}

// TaskInputToolName names the tool a waiting task's question is answered
// with. The name is protocol vocabulary defined in the events package, since
// it appears in text the fold composes for the model; the agent registers its
// input tool under the same name.
const TaskInputToolName = events.ProvideTaskInputToolName

// taskAskContent composes the opening content of the arc an ask opens: the
// usual task note, then a header line that says plainly the question comes
// from a background task and not from the user, and that it may be answered
// directly through the input tool or put to the user first; then the task's
// own message. The header is what keeps the model from reading the question
// as the user's own words.
func taskAskContent(task *AsyncTask, message string) string {
	return fmt.Sprintf("Task %s with id %s (asks a question)\n"+
		"[background task, not the user — answer it with %s, or ask the user first and then call %s with their answer]\n%s",
		task.TaskName, task.ID, TaskInputToolName, TaskInputToolName, message)
}

// applyAsyncTaskLaunchResult records the outcome of a task's launch: the external
// reference the runner assigned and the runner's raw response. A launch-result event
// saying the launch failed settles the task as failed — ASYNC_TASK_RESULT is
// reserved for the outcome of work that actually started — so no later
// report about the task is accepted. The event is written with None
// visibility and correlates to the task, so it opens no arc.
func (h *History) applyAsyncTaskLaunchResult(ev *events.Event, state *FoldState) error {
	pl, ok := ev.Payload.(events.AsyncTaskLaunchResultPayload)
	if !ok {
		return errors.New("expected payload type to be AsyncTaskLaunchResultPayload")
	}
	if !pl.LLMVisibility.Valid() {
		return fmt.Errorf("async task meta for %s carries no valid visibility", pl.CausationID)
	}
	if _, err := h.routeTaskEvent(ev, state, pl.CausationID); err != nil {
		return err
	}
	task := state.asyncTasks[pl.CausationID]
	if task.LaunchSeq != nil {
		// One launch record per invoke: the first sticks.
		return nil
	}
	task.ExternalRef = pl.ExternalRef
	task.Launch = pl.Launch
	task.Launched = pl.Launched
	task.LaunchAnswer = pl.Result
	task.LaunchResultText = pl.LaunchResult
	task.LaunchSeq = &ev.Seq
	// A launch that was not launched and not answered settles the task as
	// failed here. One answered by policy does not: the RESULT event written
	// beside it settles the task with the answer, the way a tool call's
	// Answer settles the call.
	if !pl.Launched && pl.Result == nil && task.FinishedSeq == nil {
		task.Result = pl.Error
		task.FinishedAt = &ev.Timestamp
		task.FinishedSeq = &ev.Seq
		task.Status = AsyncTaskFailed
	}
	return nil
}

// applyAsyncTaskResult settles a running task as successful or failed. A
// result for a task that already finished is dropped, so a cancelled task
// keeps that outcome. A result written AsMessage opens an arc of its own
// carrying the task's result; one written None or InResult correlates to the
// task and opens none.
func (h *History) applyAsyncTaskResult(ev *events.Event, state *FoldState) error {
	pl, ok := ev.Payload.(events.AsyncTaskResultPayload)
	if !ok {
		return errors.New("expected payload type to be AsyncTaskResultPayload")
	}
	if !pl.LLMVisibility.Valid() {
		return fmt.Errorf("async task result for %s carries no valid visibility", pl.CausationID)
	}
	arc, err := h.routeTaskEvent(ev, state, pl.CausationID)
	if err != nil {
		return err
	}
	task := state.asyncTasks[pl.CausationID]
	if task.FinishedAt != nil {
		// A later report from a runner whose task is already settled is
		// dropped, so the first report sticks.
		return nil
	}
	if !pl.Outcome.Valid() {
		return fmt.Errorf("async task result for %s carries no outcome", pl.CausationID)
	}

	visibility := pl.LLMVisibility.Effective()
	if visibility == events.LLMVisibilityAsMessage && arc == nil {
		arc = h.mintTaskArc(ev, state, task, taskArcContent(task, "returned results", resultText(pl.Result)))
	}
	if arc != nil {
		task.ResultArc = arc.ID
	}

	task.Result = pl.Result
	task.FinishedAt = &ev.Timestamp
	task.FinishedSeq = &ev.Seq
	task.Status = taskStatusOf(pl.Outcome)
	return nil
}

// taskStatusOf carries a reported outcome into the status a reader sees. The
// two are separate types because a status also covers a task that has not
// ended, which no result can report.
func taskStatusOf(outcome events.AsyncTaskOutcome) AsyncTaskStatus {
	switch outcome {
	case events.AsyncTaskOutcomeFailed:
		return AsyncTaskFailed
	case events.AsyncTaskOutcomeCancelled:
		return AsyncTaskCancelled
	default:
		return AsyncTaskSuccess
	}
}

// applyAsyncTaskEvent appends a notification to the task's transcript without
// changing its status. A notification written AsMessage opens an arc of its
// own carrying the notification's payload; one written None or InResult
// correlates to the task and opens none.
func (h *History) applyAsyncTaskEvent(ev *events.Event, state *FoldState) error {
	pl, ok := ev.Payload.(events.AsyncTaskEventPayload)
	if !ok {
		return errors.New("expected payload type to be AsyncTaskEventPayload")
	}
	if !pl.LLMVisibility.Valid() {
		return fmt.Errorf("async task event for %s carries no valid visibility", pl.CausationID)
	}
	arc, err := h.routeTaskEvent(ev, state, pl.CausationID)
	if err != nil {
		return err
	}
	task := state.asyncTasks[pl.CausationID]

	visibility := pl.LLMVisibility.Effective()
	if visibility == events.LLMVisibilityAsMessage && arc == nil {
		h.mintTaskArc(ev, state, task, taskArcContent(task, "emitted an event", resultText(pl.Payload)))
	}
	appendAsyncTaskEvent(task, ev, AsyncTaskEventNotification, pl.Payload, pl.Class, visibility)
	return nil
}

// applyAsyncTaskInputRequested parks a task on the input it is asking for. An
// ask is always written AsMessage, so it always opens an arc of its own — an
// ask that opened no arc could never be answered.
func (h *History) applyAsyncTaskInputRequested(ev *events.Event, state *FoldState) error {
	pl, ok := ev.Payload.(events.AsyncTaskInputRequestedPayload)
	if !ok {
		return errors.New("expected payload type to be AsyncTaskInputRequestedPayload")
	}
	if !pl.LLMVisibility.Valid() {
		return fmt.Errorf("async task input request for %s carries no valid visibility", pl.CausationID)
	}
	arc, err := h.routeTaskEvent(ev, state, pl.CausationID)
	if err != nil {
		return err
	}
	task := state.asyncTasks[pl.CausationID]

	visibility := pl.LLMVisibility.Effective()
	if visibility == events.LLMVisibilityAsMessage {
		if arc == nil {
			arc = h.mintTaskArc(ev, state, task, taskAskContent(task, pl.Message))
		}
		waiting := arc.ID
		task.WaitingArc = &waiting
	} else if arc != nil {
		return fmt.Errorf("async task input request %q carries %s visibility but correlates to arc %s", ev.ID, visibility, arc.ID)
	}

	task.Status = AsyncTaskWaiting
	appendAsyncTaskEvent(task, ev, AsyncTaskEventInputRequested, pl.Message, "", visibility)
	return nil
}

// applyAsyncTaskInputProvided delivers the requested input and puts the task
// back to running. The record is correlated to the arc the ask opened, which
// exists by then, so the fold attaches it there and opens no other.
func (h *History) applyAsyncTaskInputProvided(ev *events.Event, state *FoldState) error {
	pl, ok := ev.Payload.(events.AsyncTaskInputProvidedPayload)
	if !ok {
		return errors.New("expected payload type to be AsyncTaskInputProvidedPayload")
	}
	if ev.CorrelationID == nil {
		return fmt.Errorf("event %q of type %q has no correlation ID", ev.ID, ev.Typ)
	}
	if _, isTask := state.asyncTasks[*ev.CorrelationID]; isTask {
		return fmt.Errorf("async task input %q correlates to task %s, want the arc the ask opened", ev.ID, *ev.CorrelationID)
	}
	if _, ok := state.arcs[*ev.CorrelationID]; !ok {
		return fmt.Errorf("no arc with ID=%s found for async task input", *ev.CorrelationID)
	}
	task, ok := state.asyncTasks[pl.CausationID]
	if !ok {
		return fmt.Errorf("no async task with ID=%s found for async task input", pl.CausationID)
	}
	task.Status = AsyncTaskPending
	task.WaitingArc = nil
	appendAsyncTaskEvent(task, ev, AsyncTaskEventInputProvided, pl.Message, "", "")
	return nil
}

// applyError records a failure against the arc it happened in.
func (h *History) applyError(ev *events.Event, state *FoldState) error {
	pl, ok := ev.Payload.(events.ErrorPayload)
	if !ok {
		return errors.New("expected payload type to be ErrorPayload")
	}
	if ev.CorrelationID == nil {
		return fmt.Errorf("event %q of type %q has no correlation ID", ev.ID, ev.Typ)
	}
	arc, ok := state.arcs[*ev.CorrelationID]
	if !ok {
		return fmt.Errorf("no arc with ID=%s found for error", *ev.CorrelationID)
	}
	arc.Errors = append(arc.Errors, &Error{
		ID:            ev.ID,
		Seq:           ev.Seq,
		IsFatal:       pl.IsFatal,
		CorrelationID: *ev.CorrelationID,
		Timestamp:     ev.Timestamp,
		Message:       pl.Message,
		CausationID:   pl.CausationID,
	})
	return nil
}

// applyGuardrailBlocked settles a pending call that a guardrail refused,
// recording the refusal message as its result. A call that already reached a
// terminal status keeps that outcome.
func (h *History) applyGuardrailBlocked(ev *events.Event, state *FoldState) error {
	pl, ok := ev.Payload.(events.GuardrailBlockedPayload)
	if !ok {
		return errors.New("expected payload type to be GuardrailBlockedPayload")
	}
	tc, ok := state.toolCalls[pl.CausationID]
	if !ok {
		// A guardrail naming a task rather than a call refuses that task's
		// launch: the message is recorded on the task, and the launch-result
		// event written beside this one settles it.
		task, isTask := state.asyncTasks[pl.CausationID]
		if !isTask {
			return fmt.Errorf("no tool call with ID=%s found for tool call", pl.CausationID)
		}
		task.BlockedMessage = pl.Message
		return nil
	}
	if tc.Status != ToolCallStatusPending {
		return nil
	}
	tc.FinishedAt = &ev.Timestamp
	tc.FinishedSeq = &ev.Seq
	tc.Status = ToolCallStatusBlocked
	tc.Result = pl.Message
	return nil
}

// applyCancel closes an arc, stopping its pending tool calls, unanswered
// interrupts and unfinished async tasks. The first cancel an arc receives is
// the one that sticks.
//
// A cancel whose payload names a task is task-addressed instead: its
// correlation ID is the task ID, and the fold settles the task — status, who
// cancelled it and when — without touching any arc.
func (h *History) applyCancel(ev *events.Event, state *FoldState) error {
	pl, ok := ev.Payload.(events.CancelPayload)
	if !ok {
		return errors.New("expected payload type to be CancelPayload")
	}
	if ev.CorrelationID == nil {
		return fmt.Errorf("event %q of type %q has no correlation ID", ev.ID, ev.Typ)
	}

	if pl.TaskID != nil {
		if *ev.CorrelationID != *pl.TaskID {
			return fmt.Errorf("task-addressed cancel %q correlates to %s, want the task ID %s", ev.ID, *ev.CorrelationID, *pl.TaskID)
		}
		task, ok := state.asyncTasks[*pl.TaskID]
		if !ok {
			return fmt.Errorf("no async task with ID=%s found for cancel", *pl.TaskID)
		}
		if task.FinishedSeq == nil {
			task.Status = AsyncTaskCancelled
			task.FinishedAt = &ev.Timestamp
			task.FinishedSeq = &ev.Seq
			actor := ev.Actor
			task.CancelledBy = &actor
		}
		return nil
	}

	arc, ok := state.arcs[*ev.CorrelationID]
	if !ok {
		return fmt.Errorf("no arc with ID=%s found for cancel", *ev.CorrelationID)
	}

	stoppedSomething := false

	for _, amsg := range arc.Responses {
		for _, call := range amsg.ToolCalls {
			if call.Status != ToolCallStatusPending {
				continue
			}
			if call.Interrupt != nil && call.Interrupt.Status == InterruptPending {
				// Nobody will answer a decision about a call that is no
				// longer going to run.
				call.Interrupt.Status = InterruptCancelled
				call.Interrupt.ResolvedSeq = &ev.Seq
			}

			// A call already handed to its tool is not the cancel's to
			// settle. It may still report, and the cancel does not know
			// whether it took effect, so a result arriving later settles it
			// the ordinary way.
			if call.DispatchedSeq != nil {
				stoppedSomething = true
				continue
			}

			call.Status = ToolCallStatusCancelled
			call.FinishedAt = &ev.Timestamp
			call.FinishedSeq = &ev.Seq
			stoppedSomething = true
		}
	}

	for _, interrupt := range arc.Interrupts {
		if interrupt.Status == InterruptPending {
			// Nobody will answer an ask on an arc that is no longer going
			// anywhere.
			interrupt.Status = InterruptCancelled
			interrupt.ResolvedSeq = &ev.Seq
		}
	}

	for _, task := range h.TasksOfArc(arc.ID) {
		if task.Status != AsyncTaskPending && task.Status != AsyncTaskWaiting {
			continue
		}
		// A task that was invoked runs somewhere the cancel cannot reach, so
		// marking it cancelled would record an outcome nobody observed. The
		// cancel ends the waiting, and the runner reports the rest.
		stoppedSomething = true
	}

	if arc.Cancel == nil {
		// A cancel that reached an arc the agent had not closed terminated it,
		// even when there was nothing in flight to mark cancelled. Only a
		// cancel arriving after the close stopped nothing at all.
		arc.Cancel = &Cancel{
			ID:             ev.ID,
			Seq:            ev.Seq,
			Arc:            arc.ID,
			Timestamp:      ev.Timestamp,
			Actor:          &ev.Actor,
			Reason:         pl.Reason,
			TerminatedWork: stoppedSomething || arc.Closed == nil,
		}
	}
	return nil
}

// applyArcClosed records that the agent finished with an arc. A second close
// is dropped, so the first one sticks, as with a cancel.
func (h *History) applyArcClosed(ev *events.Event, state *FoldState) error {
	pl, ok := ev.Payload.(events.ArcClosedPayload)
	if !ok {
		return errors.New("expected payload type to be ArcClosedPayload")
	}
	if ev.CorrelationID == nil {
		return fmt.Errorf("event %q of type %q has no correlation ID", ev.ID, ev.Typ)
	}
	arc, ok := state.arcs[*ev.CorrelationID]
	if !ok {
		return fmt.Errorf("no arc with ID=%s found for arc close", *ev.CorrelationID)
	}
	if _, ok := state.responses[pl.CausationID]; !ok {
		return fmt.Errorf("no assistant message with ID=%s found for arc close", pl.CausationID)
	}
	if arc.Closed != nil {
		return nil
	}
	arc.Closed = &ArcClosed{
		ID:          ev.ID,
		Seq:         ev.Seq,
		Timestamp:   ev.Timestamp,
		CausationID: pl.CausationID,
	}
	return nil
}

// appendAsyncTaskEvent adds one entry to a task's transcript.
func appendAsyncTaskEvent(task *AsyncTask, ev *events.Event, typ AsyncTaskEventType, payload any, class string, visibility events.LLMVisibility) {
	task.Events = append(task.Events, AsyncTaskEvent{
		ID:         ev.ID,
		Seq:        ev.Seq,
		Timestamp:  ev.Timestamp,
		Type:       typ,
		Class:      class,
		Visibility: visibility,
		Payload:    payload,
	})
}
