Async tasks
===========

Status
--------------------
A proposal, not a specification, and the starting point for the
implementation work. [PROJECTION.md](PROJECTION.md) and
[ENCODING.md](ENCODING.md) describe what the code does today; nothing here is
binding until it is folded into those two. The code follows neither this
document nor [COMPACTION.md](COMPACTION.md) yet.

It concerns one decision: whether an async task is owned by an arc or is an
object of the session. Today it is the first. This document specifies the
second.

The shape today
--------------------
A task is invoked inside an arc and belongs to it. The arc stays open for as
long as the task runs, because the agent records `KX` only once nothing under
the arc is still running, and a cancel on the arc is what stops the task.
Disclosure of in-flight work is derived from the arc's status, and a task
that asks for input is disclosed as a note inside the arc that started it.

Two problems follow from that shape.

A task asks for input, the agent puts the question to the user, and the user
answers. The answer is a new user message, so it opens an arc of its own.
The projection's cutoff gives an arc everything up to its own opening plus
everything correlated to it, so the answer is never visible to the arc that
asked:

    U1, R1.1, A1.1, AQ1.1.1, R1.2, U2, R2.1, T2.1.1, AP1.1.2, K2x, AR1.1

`R1.2` asks the question. `U2` answers it. `U2` is not in arc 1's projection
at any cutoff, so arc 1 learns what the user said only from the note
`n{task auditor was given input: …}`.

Second, an arc held open by a running task is an arc the compaction
watermark cannot pass, so one background task blocks compaction of the whole
session for as long as it runs, while the task's own events accumulate in
the model's transcript. See [COMPACTION.md](COMPACTION.md), Async tasks.

The design
--------------------
An arc closes when the agent has nothing synchronous left to do. A response
with no tool calls closes the arc; a running task does not hold it open. When
the task next speaks, what happens is decided by the event's declared
visibility, specified below.

    U1, R1.1, A1.1, K1x, AQ1.1.1, R2.1, K2x, U3, R3.1, T3.1.1, AP1.1.2,
    K3x, AR1.1, R4.1, K4x

`AQ1.1.1` and `AR1.1` are the openings of arc 2 and arc 4. Arc 2 asks the
user the task's question and closes. Arc 3 is the user's answer, handed to
the task. Arc 4 reports what came of it, and its cutoff includes `U3`, so the
model reads the user's own words.

An arc is one unit of agent work, and what opens one is a kind rather than
always a user message.

*A task is an object of the session.* The invocation is recorded inside the
arc that ran the tool call, as today. Every event the task emits afterwards
names the task by an ID in its payload, and the fold keeps a table of tasks
at the level of the session, keyed by task ID, holding status, events, the
actor that started the task and the arc that invoked it. A report is
validated by whether the task exists in the session, not by whether an arc
holds it.

*Where each record lives.* Correlation names the arc or the stream a record
belongs to, by one rule: an event that opens an arc correlates to the arc it
opens and is that arc's opening record, carrying its own content, exactly as
a user message does; everything else a task says correlates to the task.
There is no separate arc-opening record. The actor rides the event's own
`Actor` field, and the task ID is in the payload, so the opening event needs
no pointer to its content and no record of who triggered it.

Visibility
--------------------
What a task event does is not the dispatcher's decision and not a projection
setting. Each kind of task declares, where it is registered, what each of its
event classes does, and the declaration is expressed by the event's
correlation, as above. There are two values.

*It opens an arc.* The event correlates to the arc it opens and the arc is
worked. This is the value an ask and a result normally carry.

*It is silent.* The event correlates to the task. It is invisible to the
model in every projection; it is in the log for the frontend and for replay,
and the model can read it only through a tool that returns a task's events.

The visibility is the correlation and not a payload field, for a concrete
reason: placing an event in a transcript is decided from correlation and
sequence number for every record the projection handles, and a visibility in
the payload would make it a per-event payload decode. The declaration is
resolved once, on the write path, and the log records its outcome.

The classes are the three a report already carries — notification, ask,
result — and the declaration is keyed by them. The invoke and the provided
input are not classes: the agent wrote both, so there is nothing to declare.
A task kind that needs finer classes extends the report with a class name the
registry can declare for; that is a naming change, not a new mechanism.

*The result may carry the trail.* A task kind declares, beside its event
visibilities, whether its result opens its arc carrying the events that led
to it, rendered in that arc's transcript as a field of its own beside the
result. The events stay silent; the projection gathers them from the fold's
task table when the result is rendered, so nothing about the carrying is
recorded and a different declaration at replay time changes nothing logged.

Coalescing
--------------------
A task that reports often must not cause a run per report. The controller
batches what its task emits and calls in at most one report per group window
for the classes that would open an arc. The window is declared in the
registry, per task kind, and handed to the controller at registration. What
the coalesced report says is the runner's decision — the latest notification,
or a summary of the ones it stands for. Events that are silent flow as they
are: they cost the model nothing and the frontend wants their resolution.

Coalescing happens before the log. Nothing about the window or the grouping
is recorded, because nothing of it reaches the log.

How a report becomes a turn
--------------------
The path from a runner calling in to an arc being worked:

*The write path is one event.* The inbound half resolves the event's
visibility from the registry: the task's kind is read from the task record,
the declaration for that class decides the correlation, and a report naming a
kind that is not registered is refused rather than defaulted, since a silent
default would hide a configuration fault. An event that opens an arc
correlates to a freshly minted arc ID; a silent event correlates to the task.
The event carries the content, the task ID and the actor, and it is the whole
write. There is no second record and no gap to recover: an event is in the
log with its arc, or not in the log at all.

*The fold mints deterministically.* An opening task event mints its arc the
way `applyUserMessage` mints one. `applyUserMessage` and an opening task
event are the two instances of "the event that opens an arc", and
`Store.GetEventsForArc`, which finds an arc by scanning for a `USER_MESSAGE`
with the correlation ID, needs the same generalisation. Two events arriving
together mint two arcs, which is correct: the controller's coalescing is what
keeps a talkative task from sending them together, and the log records what
was sent.

*The dispatcher only schedules.* Once the event is written, the arc it opened
is a work item like any other. When the run starts is not recorded in the
log; a run deferred for a compaction window defers the run, never the
event, by the same rule as a user message's. See
[COMPACTION.md](COMPACTION.md), Where a run waits.

Ownership
--------------------
A task-triggered arc has no human in it, and the agent cannot run without
knowing whose it is. Four things need the owner: who may cancel the task,
which tenant's data the run may read, who an ask is put to, and who the work
is attributed to when rendered.

The owner is not derived by walking a chain from the arc through the task to
the invocation. Ownership is read on the write path, and a walk has
unbounded length: a task started by an agent turn in a task-triggered arc is
one more link, with no limit on composition.

Instead the actor is copied forward. The invocation records the actor it was
started by, and every event the task emits carries the same one, so every arc
one of them opens inherits it. A chain of any length costs one field.
`events.Event` already carries `Actor`, and `UserMessagePayload` already
duplicates it into the payload.

Two things this actor is not. It is not the correlation ID: correlation names
the stream a record belongs to, and the link to the origin is the task ID in
the payload. And it is not a live grant of authority: it records who started
the work, a fact that never changes. Whether a task may still act on that
person's behalf an hour after they logged out is a policy question the log
does not answer.

Asks: relayed or answered
--------------------
An ask is not necessarily put to a person. A task asking for input may be
answered by the model alone: the arc opened by the ask runs, the model reads
the question with the task's context in its cutoff, and answers through the
input tool without a person involved. The ask/answer loop that the cutoff
makes impossible today — the answer living in an arc the asking arc cannot
see — closes inside one arc.

*Who may answer is declared.* An ask is either user-facing or
agent-answerable, declared where the task is registered or on the ask itself.
A user-facing ask must be relayed; an agent-answerable one may be answered in
place. Answering through the input tool executes under the actor copied
forward from the invocation, so the model answers as the person who started
the work. The log records the actor and that an agent turn answered, so an
audit can distinguish the user's words from the agent's.

*A loop budget.* A task that asks, is answered by the model, and asks again
is a loop with no human in it, running on authority granted for something
else. A budget bounds it: after a fixed number of consecutive agent-answered
asks without a human turn, the next ask is relayed rather than answered, or
the task is settled. This failure mode does not exist in the current design,
because there the answer had to come from outside.

Cancellation
--------------------
Cancel is arc-scoped today, and cancelling a task means cancelling the arc
that owns it. With the origin arc closed as soon as the task starts, that
cancel stops nothing (`TerminatedWork: false`) and the task runs on.

Cancellation therefore gains a second axis, addressed to the task. A cancel
naming a task ID reaches the runner through the controller and settles the
task in the log with who cancelled it and when. The guard follows the same
axis: a write whose causation reaches a cancelled task is refused, so a late
report does not open an arc for work the user stopped. The arc-scoped guard
keeps its job for synchronous work. Two axes, each named in the log.

*Compositions are not the agent's to build.* "Cancel every task this arc
started" is a loop, not a mechanism: read the session's task table, keep the
tasks whose origin arc is the one named and that are not settled, and cancel
each by ID. The task record carries its origin arc for this. The loop lives
in the harness that owns the process, which is also where the person is shown
what will be stopped and asked first. A task that finishes between the read
and the cancel is skipped; the controller already returns "cannot find task",
and the loop takes that as success. Cancelling a whole causation chain —
everything a request led to, not only what it started — is a walk and is not
built until the direct form proves too narrow. There is no batch-cancel
event, because N task cancels record everything it would.

What it costs
--------------------
*The dispatcher owns scheduling, visibly.* A result arriving while the user
is mid-conversation opens a second arc for the same person, and whether to
queue or interleave is a policy the log does not supply.

*`Arc.Content` loses its assumption.* `RenderChat` draws an arc as a user
bubble followed by replies, and `foreign_arc: note` quotes an arc's content to
say what the other request was about. A task-triggered arc has no user words.
The ask's message is the candidate for an arc opened by an ask; an arc opened
by a result has the task's name and nothing else.

*Delivery guarantees move into the runner.* `TaskController.ProvideInput`
refuses a second answer to one request, backed by the `WAITING` state in the
log, and `holdsTask` refuses a report naming an arc that does not own the
task. With tasks validated by existence in the session and asks answerable by
the model, duplicate answers are two records in a task's stream rather than a
refusal, and only the runner can tell them apart.

*Compaction.* A compaction may summarise the origin arc — the invocation
included — while the task still runs. The reference stays resolvable
because the fold reads the whole log, and the invocation note is
synthesised from the task table; see
[COMPACTION.md](COMPACTION.md), Compaction and the fold.

What would have to change
--------------------
*The events.* `ASYNC_TASK_INVOKE` gains the actor. `ASYNC_TASK_EVENT`,
`ASYNC_TASK_INPUT_REQUESTED`, `ASYNC_TASK_RESULT` gain the task ID in their
payloads and carry the actor. No event gains a visibility field: the
correlation is the visibility. An event that opens an arc correlates to the
arc it opens; otherwise it correlates to the task. No new event type.

*The fold.* Arc opening becomes a kind, with an opening task event the
second instance of it. The task table moves from the arc to the session,
keyed by the task ID the events carry. `Store.GetEventsForArc` finds an arc
by scanning for a `USER_MESSAGE` with the correlation ID and needs the same
generalisation.

*The registry.* `TaskDefinition` gains, per event class, the visibility
declaration — opens or silent — the flag saying whether an ask is
user-facing or agent-answerable, the flag saying whether the result carries
the task's events as a field of its own, and the group window the controller
waits before calling in with a report.

*The agent.* The hold-open rule goes, with `unreportedTaskNews` and the
settling-sequence comparison. `TaskReports` gains the write-path visibility
resolution and the minting of the arc ID where the event opens one. The
controller gains the coalescing. The dispatcher keeps the queue, and the
loop budget for agent-answered asks joins it. The projection gains the
result's events field, rendered from the task table where the task kind
declares it.

*The guard.* `startsWork` covers a dispatch and a task invocation today; it
gains the causation walk that refuses work reaching a cancelled task.

*PROJECTION.md.* "A running task holds its arc open" goes. Fixed rule 4,
that a task is always two notes, needs one amendment: the invocation note may
be synthesised from the task table when the invocation has been summarised,
and the completion is an arc of its own for the arc that reacts to it.

*Rendering.* `RenderChat` and devchat's transcript draw a session as a
sequence of arcs; a task-triggered arc needs a bubble that is not the user's
words.

The development harness
--------------------
devchat is where the change is felt first, and mostly as removal.

*`/cancel` keeps its job and gains a neighbour.* It stops the run in
progress, which is synchronous work, and needs nothing new. A command that
names a running task by ID cancels that task through the second axis, and for
a person to name a task they have to see the tasks: the fold's task table is
what a status line or a task list is built from, and "running", "waiting for
input" and "finished" are already fields on it. Cancelling every task an arc
started is the loop from Cancellation, and it lives here too.

*The owed list goes.* A report that could not get its arc a turn is the
hand-written scheduling devchat carries today: `taskOwedMsg`, the list of
arcs owed a run, and the retry when the holder lets go. Under this proposal
a report opens an arc and the worker queue serves it, so the TUI no longer
decides when a task's news is worked on and the mechanism is deleted.
`taskUnfiledMsg` stays; a report that could not be written at all is still
worth showing.

*The transcript gains a second kind of bubble.* A task-triggered arc is drawn
with the ask's question or the task's name and result where a user arc is
drawn with the user's words. Silent events are the frontend's alone: the
progress display is built from them rather than from anything the model was
shown.

*The runner barely moves.* `localTasks` starts, cancels and takes input as it
does today, minus the deferred-turn callback, and `SettleAbandonedTasks` on
start-up keeps its job. A fast task that finishes, opens an arc, and starts
another task makes the transcript busy; the coalescing that protects the
model from a batch of events wants a matching rendering that shows the batch
as one block.

Open questions
--------------------
*What a task-triggered arc quotes.* `Arc.Content` is the user's words. The
ask's message for an arc opened by an ask; for a result, the task's name and
the result's summary, or nothing.

*Whether the dispatcher interleaves or queues.* Two arcs for one person is a
problem whether or not they share an arc; it is only more visible here. The
queue the `ArcWorker` already has is the natural place to decide it.

*What the loop budget is.* How many consecutive agent-answered asks a task
may take before it is put to a person or settled is a number that wants
measuring rather than guessing.

*How much of the trail the result field carries.* A task that ran an hour and
reported every ten seconds has more events than any result wants beside it.
Carrying the last N, or carrying a summary the controller already wrote, are
both available; which reads better wants trying against real tasks.

*What a notification arc costs.* A task kind that declares every event class
opens is a run per event, coalescing notwithstanding. The declaration is the
control on it, and a task kind that misuses it is a configuration fault
rather than a design one.
