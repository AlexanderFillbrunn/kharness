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
  (pkg/history/history.go). The store's cutoff, `GetEventsForArc`
  (devchat/filestore/store.go), finds an arc by scanning for a
  `USER_MESSAGE` with the arc's correlation ID.
- A task is nested in its origin arc: `Arc.AsyncTasks`, keyed by the invoke
  event's ID, with `AsyncTask.CorrelationID` naming the origin arc. Every
  task event carries the task ID in its payload as `CausationID`, and the
  event itself is correlated to the arc (`reportEvent`,
  pkg/agent/reports.go). Notifications are one undifferentiated class: the
  payload is an opaque `any`, and no task event carries an LLMVisibility
  field.
- A running task holds its arc open: `deriveArcState` (pkg/agent/agent.go)
  refuses to close while any task has `FinishedSeq == nil`, and takes an
  LLM turn on `unreportedTaskNews`; `closeArc` then writes `ARC_CLOSED`
  (`KX`) naming the last response.
- The ingress re-works the origin arc: `TaskReports.Report` validates
  `holdsTask` (the named arc owns the task), persists the event correlated
  to that arc, then calls `ArcWorker.Work` on it. When the lease is held
  it returns false, and devchat queues the turn itself: `taskOwedMsg`,
  `m.owed`, `settleOwed` (devchat/tui.go), fed by the `deferred` callback in
  devchat/tasks.go.
- An ask is answered from outside the asking arc:
  `ASYNC_TASK_INPUT_REQUESTED` parks the task (`WAITING`); the human's
  answer is a new user message, which opens a new arc. The cutoff gives an
  arc everything up to its own opening plus what correlates to it, so the
  answer is never in the asking arc's transcript; only the
  `[task … was given input: …]` note reaches it (mode `async_input`).
- Projection: fixed rule 4 of PROJECTION.md — a task is always two notes,
  at invocation and completion. Notifications and input asks are governed
  by the modes `async_events` / `async_input` (note | elide) in
  `projectAsyncTasks` (pkg/history/projection.go).
- Cancel is arc-scoped only: `applyCancel` settles pending calls and
  interrupts, and for tasks only records `TerminatedWork` — the task itself
  is not settled. The guard refuses writes that start work on a cancelled
  arc: `startsWork` covers `TOOL_DISPATCHED` and `ASYNC_TASK_INVOKE`
  (pkg/agent/cancellation.go).
- Actor: `events.Event.Actor` exists and `UserMessagePayload` duplicates it
  into the payload; `AsyncTaskInvokePayload` carries no actor. Reaching a
  running task needs its origin arc: `TaskRef{ArcRef, ID, Name}`.
- Rendering: `RenderChat` (pkg/history/chat.go) and devchat's transcript
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
  ID: status, events, actor, origin arc. A report names the session and the
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
- D4  Asks and results are always `AsMessage`: an ask that opened no arc
  could never be answered, and a result that opened none would never be
  worked on. A notification's `LLMVisibility` — `None`, `AsMessage` or
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
  report could never be answered. The window is declared per class at
  registration. Coalescing happens before the log; nothing about it is
  recorded. `None` and `InResult` notifications flow as they are.
- D6  The actor is copied forward. The invoke records the actor that started
  the task; every event the task emits carries the same one, so every arc
  one of them opens inherits it. The actor is not the correlation ID and
  not a live grant of authority: it records who started the work.
- D7  An ask always opens an arc, and in that arc the model decides: it
  either puts the question to the user, or answers the ask itself through
  the input tool (`provide_task_input`), under the invocation's actor; the
  answer is written correlated to the ask's arc, so the cutoff includes
  it. This is the model's decision per ask, not a setting of the task. A
  loop budget bounds model-answered asks: after a fixed number of
  consecutive ones without a human turn, the next ask goes to the user or
  the task is settled.
- D8  Cancellation gains a second axis addressed to the task. A cancel naming
  a task ID reaches the runner through the controller and settles the task
  in the log, with who cancelled it and when. The guard follows the same
  axis: a write whose causation reaches a cancelled task is refused. The
  arc-scoped cancel and guard are unchanged. "Cancel every task this arc
  started" is a loop over the task table in the harness, not a mechanism;
  there is no batch-cancel event.
- D9  A task-triggered arc has no user words. `Arc.Content` is the ask's
  message for an ask-opened arc, and the task's name (and result summary)
  for a result-opened arc. Events declared `InResult` are folded into the
  final result message of the result-opened arc, rendered from the task
  table at render time; they are not projected as their own messages.
  `None` events are the frontend's alone.
- D10 A running task does not pin the compaction watermark; a report is
  written when it arrives and its run is deferred like any other run. See
  [COMPACTION.md](COMPACTION.md), sections Async tasks, Compaction and the
  fold, and Where a run waits.

Requirements
--------------------
Events — pkg/events
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
  task-addressed; unset, arc-addressed exactly as today.

Fold — pkg/history
- R3  `History` gains a session-level task table: every task in the session,
  keyed by task ID, in log order. `FoldStateFromHistory` rebuilds it. The
  task record keeps status, events, actor and origin arc
  (`AsyncTask.CorrelationID`).
- R4  Arc opening becomes a kind: `applyUserMessage` and an opening task
  event are its two instances; both mint an arc with ID, seq, content and
  actor taken from the event.
- R5  Task events validate against the session task table: a report naming
  an unknown task ID is rejected by the fold.
- R6  Correlation routes the fold: an arc-correlated event attaches to that
  arc; a task-correlated event attaches to the task's transcript. The fold
  cross-checks the payload's `LLMVisibility` against the correlation —
  `AsMessage` must correlate to an arc, `None` and `InResult` to a task —
  and rejects a mismatch.
- R7  The read model for a run includes the session's task-correlated
  events, so the fold builds the task table; they enter no arc's
  transcript. Implementation choice: load the whole log, or extend the arc
  query with task-correlated events.

Store — EventSource, devchat/filestore
- R8  `GetEventsForArc` finds an arc by its opening event of any
  arc-opening kind, not only `USER_MESSAGE`. The cutoff rule is unchanged:
  everything up to and including the opening event, plus everything
  correlated to the arc.

Registry — pkg/agent/tasks.go
- R9  `TaskDefinition` gains, per named notification class: the
  `LLMVisibility` declaration (`None` | `AsMessage` | `InResult`) and the
  coalescing group window — asks and results are always `AsMessage` and
  are never coalesced.

Ingress — pkg/agent/reports.go
- R10 `TaskReports.Report` resolves the event's `LLMVisibility` from the
  declaration on the write path and writes it into the payload: an ask, a
  result, or a notification of an `AsMessage` class gets a fresh arc ID
  as its correlation; `None` and `InResult` events correlate to the task.
  The notification's class is resolved from the registry, and a class the
  kind has not declared is refused. The event carries content, class,
  LLMVisibility, task ID and actor; that is the whole write.
- R11 After an opening event is persisted, the new arc becomes a work item
  in the same queue user arcs are served from; the origin arc is not
  re-worked, and the caller does not retry when the arc's lease is held.
- R12 Coalescing lives in the controller: per task and notification class,
  at most one report per group window for `AsMessage` classes, using the
  window declared at registration (D5). Asks and results are written
  whole; `None` and `InResult` notifications are written as they arrive.

Agent loop — pkg/agent/agent.go
- R13 `deriveArcState` closes the arc when the last response requested no
  tool calls, whatever tasks are unfinished. `unreportedTaskNews` and the
  finished-sequence wait go. `closeArc` is unchanged.
- R14 The loop budget for model-answered asks lives with the dispatcher,
  configurable on `AgentConfig` with a default; past the budget the next
  ask goes to the user or the task is settled (D7).

Guard and cancel — pkg/agent/cancellation.go, pkg/history
- R15 A task-addressed cancel is folded by `applyCancel`: the task is
  validated against the session table and settled (`CANCELLED`, actor,
  seq), and the runner is reached through `TaskController.Cancel`.
- R16 `GuardCancellation` refuses a write whose causation reaches a
  cancelled task (a report or invocation naming it), in addition to the
  arc-scoped `startsWork` check, which stays.

Projection — pkg/history/projection.go, docs/PROJECTION.md
- R17 Placement is decided from correlation and seq for every record, and
  the payload's `LLMVisibility` records the same decision explicitly (R6
  cross-checks the two). A task-correlated event appears in no arc's
  transcript as its own record — `None` never, `InResult` only folded
  into the result message (R19); the `async_events` and `async_input`
  modes go. PROJECTION.md's fixed
  rule 4 is amended: the invocation note stays in the origin arc; the
  completion is an arc of its own.
- R18 `foreign_arc` notes quote `Arc.Content`, which is set for
  task-opened arcs as D9 says.
- R19 Events declared `InResult` are folded into the final result message
  of the result-opened arc, gathered from the task table at render time.

Rendering — pkg/history/chat.go, devchat
- R20 `RenderChat` and devchat's transcript draw a session as a sequence of
  arcs; a task-triggered arc renders its `Content` as its opening bubble.
  `None` events render in the frontend only.

Harness — devchat
- R21 The owed-turn mechanism is deleted: `taskOwedMsg`, `m.owed`,
  `settleOwed`, and the `deferred` callback. `taskUnfiledMsg` stays.
- R22 `/cancel` keeps stopping the run. A command naming a task ID cancels
  that task through the task axis. A status line or task list is built from
  the fold's task table. `SettleAbandonedTasks` on start-up is unchanged.
- R23 `localTasks` keeps start, cancel and input, minus the deferred-turn
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
- AC5  Past the configured budget of consecutive model-answered asks, the
  next ask goes to the user or the task is settled.
- AC6  A cancel naming a task ID settles the task in the log with actor and
  seq, reaches `TaskController.Cancel`, and a late report naming that task
  is refused.
- AC7  A notification declared `None` appears in no arc's `ModelTranscript`
  under any modes, and one declared `InResult` appears only folded into
  the result message; `Modes` has no `async_events` / `async_input`; a
  `foreign_arc` note for a task-opened arc quotes its `Content`.
- AC8  `GetEventsForArc` on a task-opened arc returns its opening event and
  everything correlated to it; replay folds the arc and `RenderChat` shows
  it.
- AC9  Coalescing is per class: three notifications of an `AsMessage` class
  inside one group window produce one log record; notifications of a
  `None` or `InResult` class are each logged. An ask and a result are
  always written whole.
- AC10 devchat: no `taskOwedMsg` path remains; a report arriving while the
  origin arc is being worked opens a new arc served by the worker; an
  unwritable report still shows `taskUnfiledMsg`.
- AC11 With compaction (COMPACTION.md): a running task does not hold the
  watermark, and a task whose invoke is below the watermark is still in the
  task table, its invocation note synthesized from the table.
- AC12 A notification naming a class its task kind has not declared is
  refused at the write path and nothing is logged; a declared `AsMessage`
  class mints an arc, while `None` and `InResult` correlate to the task.
- AC13 Every task-emitted event in the log carries its `LLMVisibility` in
  the payload, and the fold rejects an event whose payload value disagrees
  with its correlation (`AsMessage` not correlated to an arc, `None` or
  `InResult` correlated to one).

Tasks
--------------------
- T1  Events and payloads: R1, R2. pkg/events. Depends: none.
- T2  Fold: session task table, arc-opening kind, correlation routing,
  validation: R3–R7. pkg/history. Depends: T1.
- T3  Store cutoff generalization: R8. devchat/filestore, pkg/agent
  (EventSource doc). Depends: T2.
- T4  Registry declarations: R9. pkg/agent/tasks.go. Depends: none.
- T5  Ingress write path and work handover: R10, R11. pkg/agent/reports.go.
  Depends: T2, T4.
- T6  Coalescing in the controller: R12. pkg/agent plus the harness
  controller. Depends: T4.
- T7  Agent loop close rule: R13. pkg/agent/agent.go. Depends: T2.
- T8  In-arc asks and the loop budget: D7, R14. pkg/agent. Depends: T5, T7.
- T9  Cancellation task axis: R15, R16, and the devchat command. Depends:
  T1, T2.
- T10 Projection and rendering changes: R17–R20, PROJECTION.md amendments.
  pkg/history, docs/PROJECTION.md. Depends: T2.
- T11 devchat harness: R21–R23. devchat. Depends: T5, T7.
- T12 Tests for AC1–AC13 across pkg/events, pkg/history, pkg/agent,
  devchat; proofread of the amended documents. Depends: T1–T11.

Open questions
--------------------
- The loop budget's number (D7, R14): a number that wants measuring rather
  than guessing.
- How much of the `InResult` trail the result message carries (D9, R19):
  the last N events, or a summary the controller already wrote.
- What a result-opened arc quotes in `Arc.Content`: the task's name and
  result summary, or the name alone.
- Whether the dispatcher interleaves or queues two arcs for one person; the
  work queue serving arcs is the natural place to decide.
- The cost of declaring a notification class `AsMessage`: a run per event
  of that class, coalescing notwithstanding. The declaration is the
  control on it, and a kind that misuses it is a configuration fault
  rather than a design one.
