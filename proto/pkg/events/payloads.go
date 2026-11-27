package events

import (
	"fillbrunn.ai/kube-agent/proto/pkg/users"
)

type UserMessagePayload struct {
	Content string      `json:"content"`
	Actor   users.Actor `json:"actor"`
}

func (UserMessagePayload) EventType() EventType {
	return UserMessageEventType
}
func (UserMessagePayload) eventPayload() {}

type AssistantMessagePayload struct {
	VendorID     *string `json:"vendor_id,omitempty"`
	ModelID      string  `json:"model_id"`
	Content      string  `json:"content"`
	Policy       *string `json:"policy,omitempty"`
	CancelPolicy *string `json:"cancel_policy,omitempty"`
	Reasoning    []byte  `json:"reasoning,omitempty"`

	// Usage is what the message cost, as the provider reported it. It is nil
	// where the provider reported nothing, which is not the same as zero.
	Usage *TokenUsage `json:"usage,omitempty"`
}

// TokenUsage is what one message cost, in tokens.
type TokenUsage struct {
	// Input counts every prompt token, CachedInput included.
	Input int `json:"input"`

	// CachedInput is the part of the prompt served from the provider's cache.
	CachedInput int `json:"cached_input"`

	// Output counts the tokens the model generated, reasoning included.
	Output int `json:"output"`
}

func (AssistantMessagePayload) EventType() EventType {
	return AssistantMessageEventType
}
func (AssistantMessagePayload) eventPayload() {}

type ToolCallPayload struct {
	MessageID string `json:"message_id"`
	VendorID  string `json:"vendor_id"`
	ToolName  string `json:"tool_name"`
	Args      any    `json:"args"`
}

func (ToolCallPayload) EventType() EventType {
	return ToolCallEventType
}
func (ToolCallPayload) eventPayload() {}

// ToolDispatchedPayload records that a tool call was handed to its tool. It is
// execution bookkeeping and is never disclosed to the model.
//
// It is written before the call is dispatched and never amended, so it says
// the call was handed over and not that it is still running. A retry writes
// another one.
type ToolDispatchedPayload struct {
	CausationID string `json:"causation_id"`
}

func (ToolDispatchedPayload) EventType() EventType {
	return ToolDispatchedEventType
}
func (ToolDispatchedPayload) eventPayload() {}

type ToolResultPayload struct {
	CausationID string `json:"causation_id"`
	Result      any    `json:"result"`
	IsError     bool   `json:"is_error"`
}

func (ToolResultPayload) EventType() EventType {
	return ToolResultEventType
}
func (ToolResultPayload) eventPayload() {}

type ToolEventPayload struct {
	CausationID string `json:"causation_id"`
	Payload     any    `json:"payload"`
}

func (ToolEventPayload) EventType() EventType {
	return ToolEventEventType
}
func (ToolEventPayload) eventPayload() {}

// InterruptPayload holds an arc up until a human answers it. With CausationID
// set it names the one call waiting on the decision, an approval gate: the
// response is a verdict on that call. Without it the interrupt is arc-wide,
// a question the agent is asking: the response is information, and no verdict
// applies to it. A policy that refuses a call without asking anyone writes a
// GuardrailBlockedPayload instead.
type InterruptPayload struct {
	// CausationID is the call waiting on the decision, following the
	// convention of the other payloads that act on a prior record. Empty
	// means the interrupt holds the whole arc named by the event's
	// correlation ID rather than one call in it.
	CausationID string `json:"causation_id,omitempty"`

	// Reason is what the human is being asked to decide about, in words.
	Reason string `json:"reason"`

	// Context is what the decision is about beyond the call's own arguments.
	// It is set when the interrupt is raised and never changes.
	Context any `json:"context,omitempty"`
}

func (InterruptPayload) EventType() EventType {
	return InterruptEventType
}
func (InterruptPayload) eventPayload() {}

type InterruptResponsePayload struct {
	CausationID string      `json:"causation_id"`
	Response    any         `json:"response"`
	IsRejected  bool        `json:"is_rejected"`
	Actor       users.Actor `json:"actor"`
}

func (InterruptResponsePayload) EventType() EventType {
	return InterruptResponseEventType
}
func (InterruptResponsePayload) eventPayload() {}

// LLMVisibility decides which stream a task-emitted event belongs to for the
// model: an event that opens an arc of its own (AsMessage), one folded into
// the task's result message (InResult), or one the model never sees (None).
// It is written once, on the write path, from the task kind's declaration, and
// the event's correlation follows it.
type LLMVisibility string

const (
	// LLMVisibilityNone is invisible to the model in every projection. It is
	// in the log for the frontend and for replay.
	LLMVisibilityNone LLMVisibility = "NONE"
	// LLMVisibilityAsMessage correlates the event to an arc it opens, the way
	// a user message opens one.
	LLMVisibilityAsMessage LLMVisibility = "AS_MESSAGE"
	// LLMVisibilityInResult correlates the event to the task and folds it into
	// the final result message of the result-opened arc.
	LLMVisibilityInResult LLMVisibility = "IN_RESULT"
)

// Effective returns the visibility an event carries, reading an empty one —
// from a log line written before visibility existed — as AsMessage.
func (v LLMVisibility) Effective() LLMVisibility {
	if v == "" {
		return LLMVisibilityAsMessage
	}
	return v
}

// Valid reports whether the visibility is one of the three, or empty.
func (v LLMVisibility) Valid() bool {
	switch v {
	case "", LLMVisibilityNone, LLMVisibilityAsMessage, LLMVisibilityInResult:
		return true
	}
	return false
}

// LLMVisibilityOf returns the visibility a task-emitted payload carries, or
// "" for a payload that carries none.
func LLMVisibilityOf(p EventPayload) LLMVisibility {
	switch typed := p.(type) {
	case AsyncTaskEventPayload:
		return typed.LLMVisibility
	case AsyncTaskInputRequestedPayload:
		return typed.LLMVisibility
	case AsyncTaskResultPayload:
		return typed.LLMVisibility
	case AsyncTaskLaunchResultPayload:
		return typed.LLMVisibility
	}
	return ""
}

// TaskCausation returns the task ID a task-emitted payload names, and whether
// the payload names one at all.
func TaskCausation(p EventPayload) (string, bool) {
	switch typed := p.(type) {
	case AsyncTaskEventPayload:
		return typed.CausationID, true
	case AsyncTaskInputRequestedPayload:
		return typed.CausationID, true
	case AsyncTaskInputProvidedPayload:
		return typed.CausationID, true
	case AsyncTaskResultPayload:
		return typed.CausationID, true
	case AsyncTaskLaunchResultPayload:
		return typed.CausationID, true
	}
	return "", false
}

// OpensArc reports whether an event is an arc's opening record: a user message,
// or a task-emitted event whose payload carries AsMessage visibility. An empty
// visibility, from a log line written before visibility existed, reads as
// AsMessage.
func OpensArc(ev *Event) bool {
	switch ev.Typ {
	case UserMessageEventType:
		return true
	case AsyncTaskEventEventType, AsyncTaskInputRequestedEventType, AsyncTaskResultEventType:
		return LLMVisibilityOf(ev.Payload).Effective() == LLMVisibilityAsMessage
	}
	return false
}

type AsyncTaskInvokePayload struct {
	TaskName string      `json:"task_name"`
	Args     any         `json:"args"`
	Actor    users.Actor `json:"actor"`

	// MessageID names the assistant message that requested the start, so the
	// projection can render the start as the call it was to the model.
	MessageID string `json:"message_id,omitempty"`

	// InResultTrail is how many InResult events the task's result message
	// carries, or -1 for the whole trail, copied from the task kind's
	// declaration by whoever starts the task. Zero is unset and folds as -1.
	InResultTrail int `json:"in_result_trail,omitempty"`

	// InResultTrailChars caps how many characters the task's InResult trail
	// carries in its result message; zero or negative means no cap.
	InResultTrailChars int `json:"in_result_trail_chars,omitempty"`

	// InResultTrailHeader says whether each InResult trail entry in the
	// task's result message carries a header line with its index in the
	// trail and its timestamp, copied from the task kind's declaration by
	// whoever starts the task.
	InResultTrailHeader bool `json:"in_result_trail_header,omitempty"`
}

func (AsyncTaskInvokePayload) EventType() EventType {
	return AsyncTaskInvokeEventType
}
func (AsyncTaskInvokePayload) eventPayload() {}

// AsyncTaskOutcome is how a task ended, as the runner reported it. A result
// can only say that a task is over, so it carries none of the states a running
// task has.
type AsyncTaskOutcome string

const (
	// AsyncTaskOutcomeSuccess is a task that finished and returned a result.
	AsyncTaskOutcomeSuccess AsyncTaskOutcome = "SUCCESS"
	// AsyncTaskOutcomeFailed is a task that ran and reported a failure.
	AsyncTaskOutcomeFailed AsyncTaskOutcome = "FAILED"
	// AsyncTaskOutcomeCancelled is a runner reporting that it stopped. Only a
	// runner can say this, since cancelling an arc asks a task to stop without
	// settling it.
	AsyncTaskOutcomeCancelled AsyncTaskOutcome = "CANCELLED"
)

// Valid reports whether the outcome is one a runner may report. The fold
// refuses a result carrying anything else.
func (o AsyncTaskOutcome) Valid() bool {
	switch o {
	case AsyncTaskOutcomeSuccess, AsyncTaskOutcomeFailed, AsyncTaskOutcomeCancelled:
		return true
	}
	return false
}

// TaskStatus is the lifecycle state of an asynchronous task. It is derivable
// from the log alone — the outcome of the task's result, or a task-addressed
// cancel — so the vocabulary lives here at the wire level, and the fold and
// any store query derive it from the same events.
type TaskStatus string

const (
	// TaskStatusPending is a task that is running.
	TaskStatusPending TaskStatus = "PENDING"
	// TaskStatusSuccess is a task that finished and returned a result.
	TaskStatusSuccess TaskStatus = "SUCCESS"
	// TaskStatusFailed is a task that finished and reported an error.
	TaskStatusFailed TaskStatus = "FAILED"
	// TaskStatusWaiting is a task that has stopped to ask for input and resumes
	// once the input arrives.
	TaskStatusWaiting TaskStatus = "WAITING"
	// TaskStatusCancelled is a task a runner reported it had stopped.
	// Cancelling an arc does not set it: the cancel ends the waiting, and what
	// became of the task is the runner's to report.
	TaskStatusCancelled TaskStatus = "CANCELLED"
)

// AsyncTaskResultPayload settles a task. Outcome is a value rather than an
// error flag, because work that was called off is not work that failed.
type AsyncTaskResultPayload struct {
	CausationID   string           `json:"causation_id"`
	Result        any              `json:"result"`
	Outcome       AsyncTaskOutcome `json:"outcome"`
	LLMVisibility LLMVisibility    `json:"llm_visibility,omitempty"`
}

func (AsyncTaskResultPayload) EventType() EventType {
	return AsyncTaskResultEventType
}
func (AsyncTaskResultPayload) eventPayload() {}

// AsyncTaskEventPayload records a non-blocking notification emitted by a task.
type AsyncTaskEventPayload struct {
	CausationID string `json:"causation_id"`
	// Class is the notification's own name, as the task kind declared it, so
	// the frontend and replay can tell notifications apart.
	Class         string        `json:"class,omitempty"`
	Payload       any           `json:"payload"`
	LLMVisibility LLMVisibility `json:"llm_visibility,omitempty"`
}

func (AsyncTaskEventPayload) EventType() EventType {
	return AsyncTaskEventEventType
}
func (AsyncTaskEventPayload) eventPayload() {}

type AsyncTaskInputRequestedPayload struct {
	CausationID   string        `json:"causation_id"`
	Message       string        `json:"message"`
	LLMVisibility LLMVisibility `json:"llm_visibility,omitempty"`
}

func (AsyncTaskInputRequestedPayload) EventType() EventType {
	return AsyncTaskInputRequestedEventType
}
func (AsyncTaskInputRequestedPayload) eventPayload() {}

type AsyncTaskInputProvidedPayload struct {
	CausationID string `json:"causation_id"`
	Message     string `json:"message"`
}

func (AsyncTaskInputProvidedPayload) EventType() EventType {
	return AsyncTaskInputProvidedEventType
}
func (AsyncTaskInputProvidedPayload) eventPayload() {}

// AsyncTaskLaunchResultPayload records what became of a task's launch: whether it was
// launched at all, and the reference the runner assigned it. It is written
// once per invoke, after the controller returned, and settles the task when
// the launch failed — ASYNC_TASK_RESULT is reserved for the outcome of work
// that actually started. It is always written with None visibility: the
// launch record is routing and replay metadata, never model-facing, and the
// model sees the start as a synthesized call and result in the projection.
type AsyncTaskLaunchResultPayload struct {
	CausationID string `json:"causation_id"`

	// ExternalRef is the ID the task's runner assigned it, guaranteed
	// non-empty: the writer falls back to CausationID — the task ID the
	// caller sent the runner — when the runner returned none. A report or
	// callback naming this ID routes to the task.
	ExternalRef string `json:"external_ref"`

	// Launched says whether the task was started. When false and no Result is
	// present, the task is settled as failed and Error carries the reason.
	Launched bool `json:"launched"`

	// Response is the runner's raw response to the invocation, kept for replay
	// and for outbound calls that need the runner's own IDs. Nil on failure.
	Response any `json:"response,omitempty"`

	// Error is set only when there is an actual error: a hook's error return,
	// a runner failure, a policy block, a launch never recorded, or a
	// LaunchResult the hook marked as an error. Empty when the task launched
	// and for a policy answer that is not an error. The log keeps it; the
	// model is never shown a raw error — the projection renders policy
	// failures as a generic message.
	Error string `json:"error,omitempty"`

	// Result is the launch answer: the outcome a policy hook supplied when it
	// intercepted the start without the task running. The log still records
	// Launched=false — nothing ran — and the answer is also written verbatim
	// as the task's ASYNC_TASK_RESULT. The projection renders it as the start
	// call's result, the way a tool call's Answer becomes its tool result.
	// Nil when the hook reported the launch itself as an error (Error carries
	// the text and no RESULT event is written) or set the launch result alone.
	Result *TaskResult `json:"result,omitempty"`

	// LaunchResult is what the model is told the launch call returned when a
	// policy hook settled the start without the task running. Empty means the
	// default: a note that the start was intercepted, with the Result
	// alongside. Set it to hide from the model that the task never launched,
	// or without a Result to leave the task result empty.
	LaunchResult string `json:"launch_result,omitempty"`

	LLMVisibility LLMVisibility `json:"llm_visibility,omitempty"`
}

// TaskResult is a policy hook's supplied outcome for a task start, as recorded
// in the log.
type TaskResult struct {
	Result  any  `json:"result"`
	IsError bool `json:"is_error,omitempty"`
}

func (AsyncTaskLaunchResultPayload) EventType() EventType {
	return AsyncTaskLaunchResultEventType
}
func (AsyncTaskLaunchResultPayload) eventPayload() {}

type ErrorPayload struct {
	Message     string  `json:"message"`
	IsFatal     bool    `json:"is_fatal"`
	CausationID *string `json:"causation_id,omitempty"`
}

func (ErrorPayload) EventType() EventType {
	return ErrorEventType
}
func (ErrorPayload) eventPayload() {}

type GuardrailBlockedPayload struct {
	Message     string `json:"message"`
	CausationID string `json:"causation_id"`
}

func (GuardrailBlockedPayload) EventType() EventType {
	return GuardrailBlockedEventType
}
func (GuardrailBlockedPayload) eventPayload() {}

// ArcClosedPayload closes an arc. The agent emits it once a response has
// requested no tool calls and no async task under the arc is still running.
type ArcClosedPayload struct {
	// CausationID is the assistant message that concluded the arc.
	CausationID string `json:"causation_id"`
}

func (ArcClosedPayload) EventType() EventType {
	return ArcClosedEventType
}
func (ArcClosedPayload) eventPayload() {}

type CancelPayload struct {
	Reason *string `json:"reason,omitempty"`

	// TaskID is set for a task-addressed cancel: the event's correlation ID is
	// then the task ID and the fold settles the task, not an arc. Unset, the
	// cancel is arc-addressed exactly as before.
	TaskID *string `json:"task_id,omitempty"`
}

func (CancelPayload) EventType() EventType {
	return CancelEventType
}
func (CancelPayload) eventPayload() {}

// CompactionRequestedPayload records that a compaction of the session was
// asked for. The event correlates to the arc the asking run was working, or
// to no arc for a request written outside any run, and is satisfied by the
// first COMPACTED whose watermark covers Watermark.
type CompactionRequestedPayload struct {
	// Watermark is the cut the compaction is to be computed against: the
	// watermark of the compaction in force when the request was written.
	Watermark int `json:"watermark"`

	// Actor is who asked, where a person did.
	Actor *users.Actor `json:"actor,omitempty"`
}

func (CompactionRequestedPayload) EventType() EventType {
	return CompactionRequestedEventType
}
func (CompactionRequestedPayload) eventPayload() {}

// ArcSuspendedPayload pauses an arc until the compaction it names has landed.
// The event correlates to the arc it pauses and carries no other payload.
type ArcSuspendedPayload struct {
	// CausationID names the COMPACTION_REQUESTED event the suspension waits
	// on: the wait is for the compaction that satisfies that request.
	CausationID string `json:"causation_id"`
}

func (ArcSuspendedPayload) EventType() EventType {
	return ArcSuspendedEventType
}
func (ArcSuspendedPayload) eventPayload() {}

// CompactedPayload is the result of one compaction. The event carries no
// correlation ID — the cutoff rule gives it its scope — and ThroughSeq is the
// watermark: the last event the summary accounts for, carried in the payload
// rather than taken from the record's own position, so a compaction computed
// through seq 100 but written at 141 still reads the events in between.
type CompactedPayload struct {
	ThroughSeq int    `json:"through_seq"`
	Summary    string `json:"summary"`

	// Method names which compactor ran: an LLM summary, pruning with a note,
	// or a provider's compaction endpoint.
	Method string `json:"method"`

	// The projection settings the summarised transcript was built under are
	// not recorded here: modes are session-level configuration, owned by the
	// session layer, which pins them for the life of the conversation.

	ModelID string      `json:"model_id,omitempty"`
	Usage   *TokenUsage `json:"usage,omitempty"`
}

func (CompactedPayload) EventType() EventType {
	return CompactedEventType
}
func (CompactedPayload) eventPayload() {}
