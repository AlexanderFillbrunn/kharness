Async tasks
===========

Status
--------------------
A specification for implementation, not only a proposal. Current state
below is the code as it stands; desired state, requirements, acceptance
criteria and tasks define the change to make.
[PROJECTION.md](PROJECTION.md) and [ENCODING.md](ENCODING.md) stay binding
for what the code does today; this document amends them where it says so.
The interaction with [COMPACTION.md](COMPACTION.md) is noted where it
matters. No code follows this document yet.

Decision
--------------------
One decision: an async task belongs to the arc that started it (today) or
is an object of the session (this spec). This spec chooses the second.

Current state
--------------------
- An arc is opened only by `USER_MESSAGE`: `applyUserMessage`
  (agent/pkg/history/history.go). The store's cutoff, `GetEventsForArc`
  (devchat/internal/filestore/store.go), finds an arc by scanning for a
  `USER_MESSAGE` with the arc's correlation ID.
- A task is nested in its origin arc: `Arc.AsyncTasks`, keyed by the invoke
  event's ID, with `AsyncTask.CorrelationID` naming the origin arc. Every
  task event carries the task ID in its payload as `CausationID`, and the
  event itself is correlated to the arc (`reportEvent`,
  ingress/pkg/ingress/reports.go). Notifications are one undifferentiated class: the
  payload is an opaque `any`, and no task event carries an LLMVisibility
  field.
- A running task holds its arc open: `deriveArcState` (agent/pkg/agent/agent.go)
  refuses to close while any task has `FinishedSeq == nil`, and takes an
  LLM turn on `unreportedTaskNews`; `closeArc` then writes `ARC_CLOSED`
  (`KX`) naming the last response.
- The ingress re-works the origin arc: `TaskReports.Report` validates
  `holdsTask` (the named arc owns the task), persists the event correlated
  to that arc, then calls `ArcWorker.Work` on it. When the lease is held
  it returns false, and devchat queues the turn itself: `taskOwedMsg`,
  `m.owed`, `settleOwed` (devchat/internal/harness/tui.go), fed by the `deferred` callback in
  devchat/internal/harness/tasks.go.
- An ask is answered from outside the asking arc:
  `ASYNC_TASK_INPUT_REQUESTED` parks the task (`WAITING`); the human's
  answer is a new user message, which opens a new arc. The cutoff gives an
  arc everything up to its own opening plus what correlates to it, so the
  answer is never in the asking arc's transcript; only the
  `[task … was given input: …]` note reaches it (mode `async_input`).
- Projection: fixed rule 4 of PROJECTION.md — a task is always two notes,
  at invocation and completion. Notifications and input asks are governed
  by the modes `async_events` / `async_input` (note | elide) in
  `projectAsyncTasks` (agent/pkg/history/projection.go).
- Cancel is arc-scoped only: `applyCancel` settles pending calls and
  interrupts, and for tasks only records `TerminatedWork` — the task itself
  is not settled. The guard refuses writes that start work on a cancelled
  arc: `startsWork` covers `TOOL_DISPATCHED` and `ASYNC_TASK_INVOKE`
  (agent/pkg/agent/cancellation.go).
- Actor: `events.Event.Actor` exists and `UserMessagePayload` duplicates it
  into the payload; `AsyncTaskInvokePayload` carries no actor. Reaching a
  running task needs its origin arc: `TaskRef{ArcRef, ID, Name}`.
- Rendering: `RenderChat` (agent/pkg/history/chat.go) and devchat's transcript
  draw each arc as a user bubble plus replies; `Arc.Content` is the user's
  words.

Desired state
--------------------
- D1  An arc closes when the agent has nothing synchronous left to do. A
  response with no tool calls closes the arc; a running task does not hold
  it open.
- D2  A task is an object of the session. The invoke is recorded inside the
  invoking arc, as today. Every later task event names the task by ID in
  its payload, and the fold keeps a session-level task table keyed by task
  ID: status, events, actor, name, origin arc. A report names the session and the
  task ID; the origin arc is read from the task table. A report is
  validated by whether the task exists in the session, not by whether an
  arc holds it.
- D3  LLMVisibility is explicit in the payload and decides the
  correlation. Every task-emitted event carries an `LLMVisibility` field —
  `None`, `AsMessage` or `InResult` — set once, on the write path, from
  the declaration (D4). An `AsMessage` event correlates to the arc it
  opens and is that arc's opening record, carrying its own content and
  actor, exactly as a user message does; a `None` or `InResult` event
  correlates to the task. There is no separate arc-opening record and no
  new event type. Which task events get which value is D4's rule.
  Placement in a transcript is still decided from correlation and
  sequence number, as for every record; the payload field records the
  same decision explicitly for readers, replay and validation.
- D4  Asks and results reported by a runner are always `AsMessage`: an
  ask that opened no arc could never be answered, and a result that
  opened none would never be worked on. The one exception is the startup
  reaper: `SettleAbandonedTasks` writes its settle as `None`, correlated
  to the task, opening no arc. A notification's `LLMVisibility` — `None`, `AsMessage` or
  `InResult` — is declared per class: every notification carries a class
  name in its payload, and the registry declares each class of a task
  kind. `AsMessage` opens an arc. `InResult` folds the event into the
  final task result message (D9). `None` is invisible to the model in
  every projection; it is in the log for the frontend and for replay.
  The declaration is resolved once, on the write path: the resolved value
  is written into the event's payload and the correlation follows it
  (D3). A report naming a class the kind has not declared is refused, not
  defaulted.
- D5  The controller coalesces the notifications of each `AsMessage`
  class into at most one report per group window and class; asks and
  results are always written whole, since an ask merged into another
  report could never be answered. The first notification of a window
  opens it, and when the window closes one merged report is written whose
  payload is an object with an `events` property containing the window's
  notification payloads as a list, the accompanying text prepended with a
  line saying the notifications were merged (how many, which class); the
  window is fixed from the first notification and declared per class at
  registration. The buffer is the controller's memory, so coalescing is
  per controller instance. Coalescing happens before the log; nothing
  about it is recorded. `None` and `InResult` notifications flow as they
  are.
- D6  The actor is copied forward. The invoke records the actor that started
  the task; every event the task emits carries the same one, so every arc
  one of them opens inherits it. The actor is not the correlation ID and
  not a live grant of authority: it records who started the work.
- D7  An ask always opens an arc, and in that arc the model decides: it
  either puts the question to the user, or answers the ask itself through
  the input tool (`provide_task_input`), under the invocation's actor.
  Answered by the model, its words are an ordinary assistant message in
  the ask's arc and the `ASYNC_TASK_INPUT_PROVIDED` record is written
  correlated to that same arc — an arc that exists, so the fold attaches
  the event and it opens no other — which is why the cutoff includes it
  at any point. Put to the user, the ask's arc closes (D1) and the
  human's reply opens a new arc; in that arc the model calls
  `provide_task_input`, and the INPUT_PROVIDED still correlates to the
  question's arc. This is the model's decision per ask, not a setting of
  the task.
- D8  Cancellation gains a second axis addressed to the task. A cancel naming
  a task ID is written as a `CANCEL` event whose correlation ID is the task
  ID; it reaches the runner through the controller and settles the task
  in the log, with who cancelled it (the event's actor) and when (its seq
  and timestamp). The guard follows the same
  axis: a write whose causation reaches a cancelled task is refused. The
  arc-scoped cancel and guard are unchanged. "Cancel every task this arc
  started" is a loop over the task table in the harness, not a mechanism;
  there is no batch-cancel event.
- D9  A task-triggered arc has no user words. Its `Arc.Content` opens with
  a note naming the task — `Task <name> with id <id> (emitted an event |
  returned results | asks a question)` — followed by the event's own
  content: the notification's payload for an `AsMessage`-class
  notification, the ask's message for an ask-opened arc, and for a
  result-opened arc the task's result as returned in
  `AsyncTaskResultPayload.Result`, not summarized (the payload struct does
  carry the result, proto/pkg/events/payloads.go). Events declared `InResult`
  are folded into the final result message of the result-opened arc,
  rendered from the task table at render time; they are not projected as
  their own messages. How much of the trail that message carries is
  configurable: the last N `InResult` events, or all of them at -1; N is
  declared per task kind (R9) with a global override in the agent config.
  `None` events are the frontend's alone.
- D10 A running task does not pin the compaction watermark; a report is
  written when it arrives and its run is deferred like any other run. See
  [COMPACTION.md](COMPACTION.md), sections Async tasks, Compaction and the
  fold, and Where a run waits.
- D11 The work queue interleaves: work items — user arcs and task-opened
  arcs alike, from any user — are served in parallel as they arrive; there
  is no per-user or per-session serialization. The lease stays per arc
  (agent/pkg/agent/lease.go), so interleaving is across arcs, never two runs on
  one arc.
- D12 An `AsMessage` notification costs a run (it opens an arc), coalescing
  notwithstanding; the declaration is the control on that cost, and a kind
  that misuses it is a configuration fault rather than a design one. To
  bound it, an event limit caps the arc-opening notification events per
  task — only `AsMessage`-class notifications count; asks and results are
  always written whole and are not demoted — declared per task kind with a
  global default in the agent config. Past the limit the controller demotes
  the event instead of opening an arc: ignored (`None`) or folded as
  `InResult`, per the overflow policy declared on the kind.

Requirements
--------------------
Events — proto/pkg/events
- R1  `AsyncTaskInvokePayload` gains `Actor users.Actor`. Task event
  payloads keep naming the task: the `CausationID` field already holds the
  invoke event's ID, which is the task ID. The task-emitted payloads —
  `AsyncTaskEventPayload`, `AsyncTaskInputRequestedPayload`,
  `AsyncTaskResultPayload` — gain `LLMVisibility` (`None` | `AsMessage` |
  `InResult`), written by the write path (R10), and
  `AsyncTaskEventPayload`
  gains `Class string`, the notification's own name, carried from the
  report into the log so the frontend and replay can tell notifications
  apart. There are no new event types; the codec gains nothing beyond the
  new fields.
- R2  `CancelPayload` gains an optional task ID. Set, the cancel is
  task-addressed: the event's correlation ID is the task ID and the fold
  settles the task, not an arc; unset, arc-addressed exactly as today.

Fold — agent/pkg/history
- R3  `History` gains a session-level task table: every task in the session,
  keyed by task ID, in log order. `FoldStateFromHistory` rebuilds it. The
  task record keeps status, events, actor, name (from
  `AsyncTaskInvokePayload.TaskName`) and origin arc
  (`AsyncTask.CorrelationID`). `Arc.AsyncTasks` is removed: the session
  table is the only home for a task, and the invoke event's place in the
  origin arc is its correlation, not a nested record.
- R4  Arc opening becomes a kind: `applyUserMessage` and an opening task
  event are its two instances. At the write path every opening event is
  given a fresh arc ID (R10); in the fold, an opening-kind event whose
  correlation names no arc yet mints one, and one whose correlation names
  an existing arc — the INPUT_PROVIDED answer correlated to the ask's arc
  (D7) — attaches to it. Both mint an arc with ID, seq and actor taken
  from the event, and content — the user's words, or for a task-opened
  arc the note and payload composed per D9.
- R5  Task events validate against the session task table: a report naming
  an unknown task ID is rejected by the fold.
- R6  Correlation routes the fold: an arc-correlated event attaches to that
  arc; a task-correlated event attaches to the task's transcript. The fold
  cross-checks the payload's `LLMVisibility` against the correlation —
  `AsMessage` must correlate to an arc, `None` and `InResult` to a task —
  and rejects a mismatch. An empty `LLMVisibility`, from a log line written
  before this spec, reads as `AsMessage`.
- R7  The read model for a run includes the session's task-correlated
  events, so the fold builds the task table; they enter no arc's
  transcript. Implementation choice: load the whole log, or extend the arc
  query with task-correlated events.

Store — EventSource, devchat/internal/filestore
- R8  `GetEventsForArc` finds an arc by its opening event — the earliest
  event correlated to the arc ID whose kind is opening (`USER_MESSAGE`,
  or a task event whose payload carries `AsMessage`), not only
  `USER_MESSAGE`. The cutoff rule is unchanged: everything up to and
  including the opening event, plus everything correlated to the arc.

Registry — proto/pkg/common/taskdef.go, agent/pkg/agent/tasks.go
- R9  The task declaration — `TaskMetadata` in common, plain data shared
  with the ingress — gains, per named notification class: the
  `LLMVisibility` declaration (`None` | `AsMessage` | `InResult`) and the
  coalescing group window — asks and results are always `AsMessage` and
  are never coalesced. The kind also declares the `InResult` trail length
  N carried by the result message (D9, R18; -1 for the whole trail), the
  event limit (maximum arc-opening notification events per task, D12) and
  the overflow policy for events past the limit — ignored (`None`) or
  folded as `InResult`. The agent config holds the global overrides for
  N and the limit. The declaration carries no provider types: the
  model-facing tool is generated from the metadata by the agent
  (`toolFor`), and the starter is registered on the agent's side, since it
  is process-bound.

Ingress — ingress/pkg/ingress/reports.go
- R10 `TaskReports.Report` names the session and the task ID — `TaskRef`
  loses `ArcRef` and `Name` and is `{SessionID, ID}`; the origin arc and
  the kind are read from the task table — and validates the report by
  whether the task exists in the session, reading the session's log rather
  than one arc's slice. It resolves the event's `LLMVisibility` from the
  declaration on the write path and writes it into the payload: an ask, a
  result, or a notification of an `AsMessage` class gets a fresh arc ID
  as its correlation; `None` and `InResult` events correlate to the task.
  The notification's class is resolved from the registry, and a class the
  kind has not declared is refused. The event carries content, class,
  LLMVisibility, task ID and actor; that is the whole write.
- R11 After an opening event is persisted, the new arc becomes a work item
  in the same queue user arcs are served from; the origin arc is not
  re-worked, and the caller does not retry when the arc's lease is held.
  Work items interleave in arrival order across arcs and users (D11); the
  per-arc lease still serializes the runs of one arc. The enqueue goes
  through a minimal interface defined in agent/pkg/agent —
  `Enqueue(ctx, ArcRef, agentID)` — which redisinfra's WorkQueue
  satisfies, so agent/pkg/agent does not import redisinfra.
- R12 Coalescing lives in the controller: per task and notification class,
  at most one report per group window for `AsMessage` classes, using the
  window declared at registration (D5): the first notification of a
  window is held and one merged report is flushed when it closes — payload
  `{"events": [...]}` with the window's notifications in order, the text
  prepended with the merge note. Asks and results
  are written whole; `None` and `InResult` notifications are written as
  they arrive.
  The controller also enforces the event limit (D12): past it, an
  `AsMessage`-class notification is demoted per the overflow policy and
  opens no arc.

Agent loop — agent/pkg/agent/agent.go
- R13 `deriveArcState` closes the arc when the last response requested no
  tool calls, whatever tasks are unfinished. `unreportedTaskNews` and the
  finished-sequence wait go. `closeArc` is unchanged.

Guard and cancel — agent/pkg/agent/cancellation.go, agent/pkg/history
- R14 A task-addressed cancel (correlation ID = task ID, R2) is folded by
  `applyCancel`: the task is validated against the session table and
  settled — status `CANCELLED`, `FinishedAt`/`FinishedSeq` from the event,
  the actor on the event — and no arc is touched: no `Arc.Cancel`, no
  `TerminatedWork`. The runner is reached through `TaskController.Cancel`
  by whichever caller wrote the cancel (the devchat command or the model's
  `cancel_task` tool), not by the fold. A later report from that runner is
  dropped by the fold (the task is finished) and refused at the write path
  (R15).
- R15 `GuardCancellation` refuses a write whose causation reaches a
  cancelled task, in addition to the arc-scoped `startsWork` check, which
  stays. On every batch the guard checks two axes: a `startsWork` event
  correlated to a cancelled arc is refused as today, and any task-emitted
  event whose payload `CausationID` names a task the log shows settled
  `CANCELLED` is refused the same way — the whole batch, `ErrCancelled`,
  nothing written. The task-axis lookup hangs off `CancelSource`, which
  gains a method answering a task ID (filestore scans the log for the
  `CANCEL` correlated to that task ID); the arc lookup is unchanged.

Projection — agent/pkg/history/projection.go, docs/PROJECTION.md
- R16 Placement is decided from correlation and seq for every record, and
  the payload's `LLMVisibility` records the same decision explicitly (R6
  cross-checks the two). A task-correlated event appears in no arc's
  transcript as its own record — `None` never, `InResult` only folded
  into the result message (R18); the `async_events` and `async_input`
  modes go. PROJECTION.md's fixed
  rule 4 is amended: the invocation note stays in the origin arc; the
  completion is an arc of its own.
- R17 `foreign_arc` notes quote `Arc.Content`, which is set for
  task-opened arcs as D9 says.
- R18 Events declared `InResult` are folded into the final result message
  of the result-opened arc, gathered from the task table at render time.
  The message carries the last N of them, or all of them at -1 (D9; N per
  R9, with a global override in the agent config).

Rendering — agent/pkg/history/chat.go, devchat
- R19 `RenderChat` and devchat's transcript draw a session as a sequence of
  arcs; a task-triggered arc renders its `Content` as its opening bubble.
  `None` events render in the frontend only.

Harness — devchat
- R20 The owed-turn mechanism is deleted: `taskOwedMsg`, `m.owed`,
  `settleOwed`, and the `deferred` callback. `taskUnfiledMsg` stays.
- R21 `/cancel` keeps stopping the run. A command naming a task ID writes
  the `CANCEL` event correlated to that task ID (R2, D8) and calls
  `TaskController.Cancel` on it. A status line or task list is built from
  the fold's task table. `SettleAbandonedTasks` on start-up settles what
  it always settled; its result events are written with `None`
  visibility, correlated to the task (D4's reaper exception).
- R22 `localTasks` keeps start, cancel and input, minus the deferred-turn
  callback.

Acceptance criteria
--------------------
- AC1  Fold: a log with an invoke in arc A and a later notification
  correlates the notification to the task, not to arc A; replay succeeds;
  the session task table holds the task with its events and origin arc.
- AC2  Opening report after arc A closed: `TaskReports.Report` persists the
  event with a fresh arc ID as correlation, the fold mints the arc, and the
  new arc goes to the worker as a work item. The owed list is not involved.
- AC3  An arc whose last response requested no tool calls is closed (`KX`
  written, status `DONE`) while a task it started is still running.
- AC4  An ask always opens an arc, and so does a result. In the ask's arc
  the model either puts the question to the user or answers it with
  `provide_task_input` under the invoke's actor, and the answer is in the
  ask arc's own transcript at any cutoff.
- AC5  A cancel naming a task ID is written with the task ID as its
  correlation, settles the task in the log with actor and seq, reaches
  `TaskController.Cancel`, and a late report naming that task is refused.
- AC6  A notification declared `None` appears in no arc's `ModelTranscript`
  under any modes, and one declared `InResult` appears only folded into
  the result message; `Modes` has no `async_events` / `async_input`; a
  `foreign_arc` note for a task-opened arc quotes its `Content`.
- AC7  `GetEventsForArc` on a task-opened arc returns its opening event and
  everything correlated to it; replay folds the arc and `RenderChat` shows
  it.
- AC8  Coalescing is per class: three notifications of an `AsMessage` class
  inside one group window produce one log record; notifications of a
  `None` or `InResult` class are each logged. An ask and a result are
  always written whole.
- AC9  devchat: no `taskOwedMsg` path remains; a report arriving while the
  origin arc is being worked opens a new arc served by the worker; an
  unwritable report still shows `taskUnfiledMsg`.
- AC10 With compaction (COMPACTION.md): a running task does not hold the
  watermark, and a task whose invoke is below the watermark is still in the
  task table, its invocation note synthesized from the table.
- AC11 A notification naming a class its task kind has not declared is
  refused at the write path and nothing is logged; a declared `AsMessage`
  class mints an arc, while `None` and `InResult` correlate to the task.
- AC12 Every task-emitted event in the log carries its `LLMVisibility` in
  the payload, and the fold rejects an event whose payload value disagrees
  with its correlation (`AsMessage` not correlated to an arc, `None` or
  `InResult` correlated to one).
- AC13 Past the configured event limit, a further `AsMessage`-class
  notification opens no arc and is logged demoted per the overflow policy
  (`None` or `InResult`); asks and results are always written whole.

Tasks
--------------------
- T1  Events and payloads: R1, R2. proto/pkg/events. Depends: none.
- T2  Fold: session task table, arc-opening kind, correlation routing,
  validation: R3–R7. agent/pkg/history. Depends: T1.
- T3  Store cutoff generalization: R8. devchat/internal/filestore, agent/pkg/agent
  (EventSource doc). Depends: T2.
- T4  Registry declarations: R9. proto/pkg/common/taskdef.go (metadata),
  agent/pkg/agent/tasks.go (starter, model-facing tool). Depends: none.
- T5  Ingress write path and work handover: R10, R11. ingress/pkg/ingress/reports.go.
  Depends: T2, T4.
- T6  Coalescing in the controller: R12. agent/pkg/agent plus the harness
  controller. Depends: T4.
- T7  Agent loop close rule: R13. agent/pkg/agent/agent.go. Depends: T2.
- T8  In-arc asks: D7. agent/pkg/agent. Depends: T5, T7.
- T9  Cancellation task axis: R14, R15, and the devchat command. Depends:
  T1, T2.
- T10 Projection and rendering changes: R16–R19, PROJECTION.md amendments.
  agent/pkg/history, docs/PROJECTION.md. Depends: T2.
- T11 devchat harness: R20–R22. devchat. Depends: T5, T7.
- T12 Tests for AC1–AC13 across proto/pkg/events, agent/pkg/history, agent/pkg/agent,
  devchat; proofread of the amended documents. Depends: T1–T11.

