Event vocabulary
==========

Every event type the log can carry, what each one means, and what its payload
holds. The vocabulary is defined in `proto/pkg/events` — the types in
`events.go`, the payloads in `payloads.go`, the wire mapping in `codec.go` —
and this document describes that code as it stands. Where the meaning of an
event is only fully visible in the fold or the projection, the relevant doc
is linked: [PROJECTION.md](PROJECTION.md) for how events become what the
model sees, [ENCODING.md](ENCODING.md) for how they are written and read,
[ASYNC-TASKS.md](ASYNC-TASKS.md) and [CANCELLATION.md](CANCELLATION.md) for
the task and cancel lifecycles, [COMPACTION.md](COMPACTION.md) for the
compaction events.

The envelope
--------------------
Every event is an `events.Event` with the same record fields, whatever its
type:

    SessionID     the session the event belongs to
    AgentID       the agent that wrote it
    ID            the event's own ID; other events name it in their payloads
    Seq           the event's position in the session log, assigned on write
    Timestamp     when it was written
    Typ           the EventType, one of the twenty below
    CorrelationID the arc (or task) the event belongs to, where it has one
    Actor         who caused it, a users.Actor (UserID, TenantID, CalledBy)
    Payload       the typed payload for Typ

`Payload` is decoded from `Typ` on read by the `PayloadCodec` — `JSONCodec`
today — so the wire form of an event is the envelope plus the payload's JSON.

Message and arc events
--------------------
These are the events an arc is made of: what the user said, what the model
answered, and the record that the arc is over.

`USER_MESSAGE` — `UserMessagePayload`
- `Content string`, `Actor users.Actor`
- Opens an arc: the event's `CorrelationID` is the arc ID, and
  `events.OpensArc` reports it. Every run of the agent starts from one.

`ASSISTANT_MESSAGE` — `AssistantMessagePayload`
- `VendorID *string` (the provider's own message ID, where issued),
  `ModelID string`, `Content string`, `Policy *string`,
  `CancelPolicy *string`, `Reasoning []byte` (opaque, handed back verbatim),
  `Usage *TokenUsage` — nil where the provider reported nothing, which is
  not the same as zero. `TokenUsage` is `Input` (cached input included),
  `CachedInput`, `Output` (reasoning included).
- The model's whole reply, written once the stream completes. The streamed
  tokens are a preview of this event, never a separate record (see
  [EVENT-SINK.md](EVENT-SINK.md)).

`ARC_CLOSED` — `ArcClosedPayload`
- `CausationID string` — the assistant message that concluded the arc.
- Emitted once a response requested no tool calls and no async task under
  the arc is still running. Closes the arc.

Tool events
--------------------
A tool call the model requested, and everything that happened to it.

`TOOL_CALL` — `ToolCallPayload`
- `MessageID string` (the assistant message that requested it),
  `VendorID string` (the provider's ID for the call), `ToolName string`,
  `Args any`.

`TOOL_DISPATCHED` — `ToolDispatchedPayload`
- `CausationID string` — the `TOOL_CALL` event it belongs to.
- Execution bookkeeping, never disclosed to the model. Written before the
  call is dispatched and never amended: it says the call was handed over,
  not that it is still running. A retry writes another one.

`TOOL_RESULT` — `ToolResultPayload`
- `CausationID string`, `Result any`, `IsError bool`.

`TOOL_EVENT` — `ToolEventPayload`
- `CausationID string`, `Payload any`.
- A non-blocking notification emitted while a tool runs.

Interrupt events
--------------------
A tool call paused until a human decides on it.

`INTERRUPT` — `InterruptPayload`
- `CausationID string` — the call waiting on the decision, empty for an
  arc-wide interrupt that holds the whole arc named by the event's
  `CorrelationID`.
  `Reason string` — what the human is being asked to decide or answer, in
  words, `Context any` — set when the interrupt is raised and never changes.
- A policy that refuses a call without asking anyone writes a
  `GUARDRAIL_BLOCKED` instead.

`INTERRUPT_RESPONSE` — `InterruptResponsePayload`
- `CausationID string` — the `INTERRUPT` event, `Response any`,
  `IsRejected bool`, `Actor users.Actor`.
- On a call interrupt the response is a verdict: `IsRejected` settles the call
  `REFUSED` with `Response` as its result, an approval leaves the call
  pending. On an arc-wide interrupt the response is information only:
  `IsRejected` is refused by the fold and the response settles just the
  interrupt.

Async task events
--------------------
The lifecycle of a task that runs outside the agent loop. The task's own ID
is the `CausationID` on every task event after the invoke, and the event's
`CorrelationID` is the arc (or, per [ASYNC-TASKS.md](ASYNC-TASKS.md), the
session the task belongs to). See that doc for the full lifecycle.

`ASYNC_TASK_INVOKE` — `AsyncTaskInvokePayload`
- `TaskName string`, `Args any`, `Actor users.Actor`.
- Starts a task; the event's own ID is the task ID the later events name.

`ASYNC_TASK_RESULT` — `AsyncTaskResultPayload`
- `CausationID string`, `Result any`, `Outcome AsyncTaskOutcome`,
  `LLMVisibility LLMVisibility`.
- Settles a task. `Outcome` is `SUCCESS`, `FAILED` or `CANCELLED` — a value
  rather than an error flag, because work that was called off is not work
  that failed. Only a runner reports `CANCELLED`; cancelling an arc asks a
  task to stop without settling it.

`ASYNC_TASK_EVENT` — `AsyncTaskEventPayload`
- `CausationID string`, `Class string` — the notification's own name, as the
  task kind declared it, `Payload any`, `LLMVisibility LLMVisibility`.
- A non-blocking notification emitted by a running task.

`ASYNC_TASK_INPUT_REQUESTED` — `AsyncTaskInputRequestedPayload`
- `CausationID string`, `Message string`, `LLMVisibility LLMVisibility`.
- The task stopped to ask for input; it is `WAITING` until the input
  arrives.

`ASYNC_TASK_INPUT_PROVIDED` — `AsyncTaskInputProvidedPayload`
- `CausationID string`, `Message string`.
- The input the task was waiting for.

Two tool names are part of this vocabulary: `cancel_task` and
`provide_task_input` (`events.CancelTaskToolName`,
`events.ProvideTaskInputToolName`) are the two tools every task-carrying
agent offers the model to address a task already running. Their names appear
in log-derived text the model reads, so they live in `proto/pkg/events`
where every writer and reader of the log shares them.

Failure and control events
--------------------

`ERROR` — `ErrorPayload`
- `Message string`, `IsFatal bool`, `CausationID *string`.
- `IsFatal` ends the arc; a non-fatal error is recorded and the run
  continues.

`GUARDRAIL_BLOCKED` — `GuardrailBlockedPayload`
- `Message string`, `CausationID string` — the tool call the policy refused.

`CANCEL` — `CancelPayload`
- `Reason *string`, `TaskID *string`.
- With `TaskID` unset the cancel is arc-addressed: the event's
  `CorrelationID` is the arc. With `TaskID` set it is task-addressed: the
  `CorrelationID` is the task ID and the fold settles the task, not an arc
  (see [CANCELLATION.md](CANCELLATION.md)).

Compaction events
--------------------
The request, the pause, and the result of compacting a session. See
[COMPACTION.md](COMPACTION.md) for the lifecycle these three form.

`COMPACTION_REQUESTED` — `CompactionRequestedPayload`
- `Watermark int` — the cut the compaction is to be computed against, the
  watermark of the compaction in force when the request was written,
  `Actor *users.Actor` — who asked, where a person did.
- Correlates to the arc the asking run was working, or to no arc for a
  request written outside any run. Satisfied by the first `COMPACTED` whose
  watermark covers `Watermark`.

`ARC_SUSPENDED` — `ArcSuspendedPayload`
- `CausationID string` — the `COMPACTION_REQUESTED` event the suspension
  waits on.
- Pauses an arc until the compaction that satisfies that request has landed.
  Correlates to the arc it pauses.

`COMPACTED` — `CompactedPayload`
- `ThroughSeq int` — the watermark: the last event the summary accounts for,
  carried in the payload rather than taken from the record's own position, so
  a compaction computed through seq 100 but written at 141 still reads the
  events in between, `Summary string`, `Method string` — which compactor
  ran (an LLM summary, pruning with a note, or a provider's compaction
  endpoint), `ModelID string`, `Usage *TokenUsage`.
- Carries no `CorrelationID`; the cutoff rule gives it its scope.

Visibility and status vocabulary
--------------------
Two enums travel on the task payloads above and are part of the wire
vocabulary.

`LLMVisibility` — `NONE`, `AS_MESSAGE`, `IN_RESULT` — decides which stream a
task-emitted event belongs to for the model: invisible in every projection,
correlated to an arc it opens of its own, or folded into the task's result
message. Written once, on the write path, from the task kind's declaration.
An empty value — a log line written before visibility existed — reads as
`AS_MESSAGE` (`Effective()`).

`TaskStatus` — `PENDING`, `SUCCESS`, `FAILED`, `WAITING`, `CANCELLED` — the
lifecycle state of a task, derivable from the log alone: the outcome of the
task's `ASYNC_TASK_RESULT`, or a task-addressed `CANCEL`. Cancelling an arc
does not set `CANCELLED`; what became of the task is the runner's to report.
