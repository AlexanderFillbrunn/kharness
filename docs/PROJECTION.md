Projection
==========

Purpose
--------------------
A session is stored as an event log. A model provider accepts a flat ordered
list of messages. Projection is the step that turns the first into the second.

The log is not a transcript with extra fields. It records interrupts, cancels,
errors, guardrail blocks and async tasks, none of which a provider has a
message shape for. It records overlapping work, which a flat list has no way to
express. Projection decides, for every record in the log, whether the model is
told about it and in what form.

Pipeline
--------------------
Three representations, two transitions.

    event log  ──fold──▶  History  ──project──▶  ModelTranscript  ──encode──▶
    provider messages

    event log           what happened, append-only, one session
    History             the log folded into arcs. `agent/pkg/history/history.go`
    ModelTranscript     provider-agnostic messages. `agent/pkg/history/projection.go`
    provider messages   one vendor's wire format

The fold is mechanical. It nests each record under the arc named by its
correlation ID and keeps everything.

Projection is where the decisions are. It is a pure function of three inputs.

    project(history, arc, modes) → ModelTranscript

Encoding is per provider: message shapes, ID formats, and the alternation and
pairing rules a given API enforces. Encoding does not decide what the model is
told. Anything it would have to invent is a projection decision that was left
open.

This document specifies projection only. Encoding is specified in
[ENCODING.md](ENCODING.md).

Output
--------------------
`ModelTranscript` is a slice of `ModelMessage`, in send order. Four shapes,
defined in `agent/pkg/history/projection.go`.

    ModelTextMessage        text with a role, USER, INSTRUCTION or TASK
    ModelAssistantMessage   the model's text, plus the calls it requested
    ModelToolMessage        one call's outcome, keyed by tool call ID
    ModelNoteMessage        synthesized status, carrying no role
    ModelToolCall           one call, carried by the assistant message

`ModelAssistantMessage` carries the model's reasoning alongside its text and
its calls, as the opaque bytes the provider sent. See Reasoning.

`ModelToolMessage.Outcome` states how the call ended as data, not only as the
text the model reads. An encoder or a reader of the transcript can tell a
policy block from a human refusal from a cancel without parsing a sentence.

The set is closed by an unexported method, so a projection cannot emit a shape
no provider understands.

Input
--------------------
A projection is made for one arc. The history it receives holds every event up
to that arc's own opening event — its user message, or a task event whose
payload carries `AS_MESSAGE` visibility, which opened the arc — plus every
later event correlated to it or to one of the session's tasks — a
`COMPACTION_REQUESTED` correlates to the arc it was produced in — plus the
session-level records that carry no correlation ID, which is `COMPACTED`
alone, so a compaction that landed after the arc opened still reaches the run
that resumes it. The read-model applies that cutoff when it builds the
history.

Four consequences:

1. Other arcs are frozen at the cutoff. Work in flight when the arc opened
   stays in flight for the whole projection. `:pending` states a fact about the
   snapshot and does not guess at timing.

2. Nothing arrives out of causal order. The projection cannot observe an event
   past the cutoff, so placement is the only ordering question. See Ordering.

3. The same record may project differently across turns. A call shown with a
   `:pending` stand-in in one arc's transcript may carry its real result in the
   next arc's. A transcript is a view from one point in the log and does not
   accumulate.

4. The query is the cutoff. The projection walks the arcs the history holds,
   once, and emits what the modes admit.

The slice has gaps. Another arc's events fall between this arc's whenever the
two overlapped, and the query keeps only one of them, so the fold requires
events in increasing sequence order.

Compacted transcript
--------------------
A session that has compacted reads differently: the transcript is the summary
of the compaction in force plus the projection of the events above its
watermark. The compaction in force is the `COMPACTED` with the greatest
`ThroughSeq` among those in the history — coverage by watermark, not arrival,
so an out-of-order or duplicated compaction is harmless. A `COMPACTED`'s
watermark is in its payload, not its position: a compaction computed through
seq 100 but written at 141 still reads the events in between.

    summary of Seq <= ThroughSeq, plus
    projection of Seq > ThroughSeq

The summary goes at the head of the transcript, before the first surviving
arc, as a note — `n{…}` — and nothing precedes it but the system prompt. It is
synthesized, so it is a note by the definition above, and it is the one note
that is not short: the "a note is one line" rule in Settings yields to it. No
mode governs it; a session that compacts has decided the material is gone. It
is not an INSTRUCTION message.

Everything at or below the watermark is replaced by the summary: arcs that end
at or below it are skipped whole, and an arc the line passes through renders
against the cut. The only arcs the line passes through are open arcs too large
for the arc threshold — an unworked or in-flight arc pins the line at its
opening or below its last turn, and a suspended arc that fits took the opening
bound — so a spanning arc renders as a skeleton: its opening request verbatim,
the tool calls of its spent prefix without their results, a note standing at
the cut, and its live part — the last response, an unanswered ask — whole below
the line. The note at the cut states that the arc's own log is verbatim and
wins over the summary where the two disagree, and where an `ARC_SUSPENDED`
stands it also says the request was paused while its context was compacted. A
spanning arc that fits the threshold renders whole against the line instead.

A tool call and its result are never separated by the cut: the watermark is
drawn on a turn boundary, which is exactly where the projection keeps the pair
contiguous.

A task whose invocation falls below the watermark is still in the fold's task
table, so its invocation note is synthesized from the table at the cut: the
name, the ID verbatim — never from the summary's prose, which will not
reliably reproduce it — and the status, including a waiting task's outstanding
question.

Tool results are thinned before every call, compacted or not: a result over
`MaxToolResultChars` is truncated to the cap with a marker naming the call and
the omitted size. The log keeps the whole result. The thinning is the
harness's own doing, so the `ModelToolMessage` keeps its outcome and never
carries `IsError` for it — the model must not read a thinned result as the tool
failing.

The compactor's own input is the same shape: the region a compaction
summarises — the previous summary at the head plus the events between its
watermark and the new one — is projected, so a summary is made from the
projected transcript, never from the log, and elided material cannot come back
through the one message no mode reaches.

Event shorthand
--------------------
IDs are parent-indexed. The prefix names the kind, the dotted suffix names the
path from the arc down to the record. `U2` is the second user message.
`T2.1.3` is the third tool call of its first response.

    UX          User message X, opens arc X
    RX.Y        Agent response Y in arc X
    TX.Y.Z      Tool call Z of response X.Y
    TRX.Y.Z     Tool result for tool call X.Y.Z
    TEX.Y.Z.W   Tool event W emitted by tool call X.Y.Z
    GX.Y.Z      Guardrail block of tool call X.Y.Z, decided by a policy
    IX.Y.Z      Interrupt on tool call X.Y.Z, put to a human
    IRX.Y.Z     Interrupt response, the human's decision on IX.Y.Z
    AX.Y        Async task Y invoked in arc X
    ARX.Y       Async task result for task X.Y
    AEX.Y.Z     Async task notification, entry Z in task X.Y's transcript
    AQX.Y.Z     Async task input requested, entry Z in task X.Y's transcript
    APX.Y.Z     Async task input provided, entry Z in task X.Y's transcript
    EX.Y        Error Y in arc X, non-fatal
    FEX.Y       Fatal error Y in arc X, closes the arc
    CX.Y        Cancellation Y of arc X
    KX          Close of arc X, recorded by the agent

AE, AQ and AP share one counter per task, because they share one transcript.
`A1.1, AE1.1.1, AQ1.1.2, AP1.1.3, AR1.1` reads in the order it happened.

An AE, AQ or AR whose payload carries `AS_MESSAGE` visibility is an arc's
opening record, exactly as a user message is: it opens an arc of its own, and
that arc's opening bubble is the event's composed content (see Async tasks).
`NONE` and `IN_RESULT` events stay correlated to the task and appear in no
arc's transcript as their own records.

TR and G are mutually exclusive for a given call. Both settle it and the first
to arrive wins.

Modifiers
--------------------
    IRX.Y.Z+     Interrupt approved
    IRX.Y.Z-     Interrupt refused
    E1.2@T1.1.1  Causation, where an error or block is about one record
    U2!          The arc being projected
    K1x          The event that closed its arc

Only `KX`, `FEX.Y` and `CX.Y` can close an arc, and each names its arc in its
own ID. An arc is open at a cutoff when no `x` event for it appears.

`KX` is the agent's own record that it is finished with the arc. It is emitted
when the last response requested no tool calls, whatever tasks are unfinished:
a running task does not hold its arc open, because the task's later events open
arcs of their own. A response is therefore not a closing event by itself, but an
arc whose last response asked for tools stays open until those calls settle.

`FEX.Y` always closes its arc. `CX.Y` closes only when the arc was not already
closed, which `Cancel.TerminatedWork` records. Where two closing records exist,
the earlier one closed the arc. The shorthand assumes one closing event per
arc.

Message shorthand
--------------------
Role is the letter. Provenance is the bracket.

    (…)     derived from the record named inside
    {…}     synthesized by the projector

    u(U1)       ModelTextMessage, USER. The user's own words.
    k(U3)       ModelTextMessage, TASK. The opening of an arc a background task
                opened; it is not the user speaking. Which wire role carries it
                is the encoder's decision, like a note's.
    a(R1.1)     ModelAssistantMessage. The model's own words.
    t(T1.1.1)   A call, carried by the assistant message that requested it.
    r(T1.1.1)   ModelToolMessage. The outcome of that call.
    s{…}        ModelTextMessage, INSTRUCTION. A real system prompt.
    n{…}        A note. Synthesized status and anything else the model must be
                told that no participant said.

A note carries no role. Whether notes reach the model as user text, as
instructions or as something else is open. See Left to the encoder.

Calls belong to the assistant message that requested them and go in brackets
after it. Everything else is one token per message, in emission order.

    a(R1.1)[t(T1.1.1) t(T1.1.2)]
    a(R1.3)                        a response with no calls. K1 follows once
                                   nothing it started is still running.

Tool results carry an outcome, `ModelToolMessage.Outcome`, the projected form
of `ToolCallStatus`.

    r(T1.1.1:ok)        SUCCESS
    r(T1.1.1:err)       FAILED. IsError is set.
    r(T1.1.1:blocked)   BLOCKED. A policy refused it, asking nobody.
    r(T1.1.1:refused)   REFUSED. A human refused it when asked.
    r(T1.1.1:cancel)    CANCELLED. Content is the cancel reason.
    r{T1.1.1:pending}   Synthesized. The call has no result at the cutoff.

A call that never ran names who stopped it. A policy is a rule to work within,
a human is someone to go back to, and a cancel is neither. The three read alike
otherwise, and the model acts differently on each.

A call's progress events become one note each, `n{event:TE1.1.1.1}`, or `∅`.

Repairs
--------------------
A message list is flat, sequential and single-threaded. A log is none of these.
Three gaps open between them.

    provider shape  the transcript must be a certain shape to be accepted
    concurrency     arcs overlap, so work is unfinished and messages interleave
    causality       which request a message belongs to

The projection closes them with synthesized messages. `{…}` and `n{…}` mark
every one. `r(T1.1.1:cancel)` is what the log says. `r{T1.1.1:pending}` is what
the projection made up. A mode gates each repair.

The gaps compound. Concurrency creates the unfinished call. Provider shape
makes it fatal. Causality decides what the stand-in has to say.

*Unanswered calls.* Most providers reject a transcript where a call has no
result. The call gets a stand-in, with a note that explains it.

    a(R1.1)[t(T1.1.1)] r{T1.1.1:pending} n{inflight:U1}

The stand-in is per call, because each call has to be answered. The note is per
arc. Several waiting calls under U1 produce one note between them.

*Attribution.* Every source ID names its own arc, so `r{T1.1.1:pending}` in the
middle of arc 2 is about arc 1. Whether the projected text carries the
attribution is a wording decision, settled in the expanded form.

Modes
--------------------
A mode is a session-wide decision about how much of one record kind to
disclose. One vocabulary covers all modes, ordered by how much it tells the
model.

    keep        project it in full
    stand_in    replace it with a synthesized message of the same shape
    note        reduce it to a synthesized note
    elide       drop it, written `∅` where a scenario needs to say so

A mode looks at one record and decides what it becomes. No mode packs several
records into one message.

`*` marks the default.

    foreign_arc    an arc other than the projected one that the log does not
                   show as closed: open, dead, or withdrawn
                   keep* | note | elide

    pending        tool calls with no result
                   stand_in* | note | elide

    tool_events    progress notifications emitted by a running call
                   note | elide*

    error          a non-fatal error the arc survived
                   note | elide*

    interrupt      the human approval a tool call was held up for, the ask
                   and an approval
                   note | elide*

Settings
--------------------
One session-wide setting is not a treatment. Every mode above decides whether
a record is disclosed; this decides how much of one is spelled out in a note
that was already going to be sent.

    quote_limit    how much of a request a note quotes, in characters
                   32* | 0 for the whole request

A note names the request it is about so the model can tell which one is meant,
and naming is not repeating. A request quoted whole is put in front of the
model twice, once as itself and once as the note about it, and under
`foreign_arc: note` the quote becomes the only trace of a request the mode
just decided to reduce. Enough to recognise it is enough.

A quote is folded onto one line. A request that was cut ends in `…`, so that a
shortened request cannot be read as one that was always that short. The one
note that is not one line is a compaction summary; see Compacted transcript.

Fixed rules
--------------------
Four cases are not modes.

1. A cancel with `TerminatedWork` false is always elided. It stopped nothing.

2. A cancel that stopped work in progress is always disclosed.

3. A refusal is always disclosed, as `r(T:refused)` on the call it refused.
   The call has to be answered and the refusal is the answer. No mode reaches
   it, because no mode reaches a tool result.

4. An async task invoked in the projected arc always gets its invocation note
   there. Its completion is an arc of its own when the result was written
   `AS_MESSAGE`; a task settled without opening an arc — a task-addressed
   cancel, or the startup reaper — gets its completion note in the origin arc.

Reasoning
--------------------
A provider may return a record of how the model reached a message, and accept
it back on a later turn so the model can pick up its own train of thought. It
is opaque: the projection carries the bytes and reads nothing in them.

Reasoning is not a record of its own and has no mode. It belongs to one
assistant message, the way that message's calls do, and travels with it.

It travels only when the message survives intact. A provider that takes
reasoning back validates it against the message it arrives with, so a message
that lost a call to `pending: note` or `pending: elide` is no longer the message
the reasoning describes, and the reasoning is dropped. Under `pending: stand_in`
every call is answered and every message keeps all of its calls, which is why
the default carries reasoning everywhere and the other two values give it up.

Dropping it costs the model its own train of thought on that turn. That is the
price of a mode that rewrites the turn, and it is the reason to prefer
`stand_in` on a provider that has reasoning to lose.

Execution records
--------------------
One record kind in the log is not about the conversation. `TOOL_DISPATCHED`
says that a tool call was handed to its tool, and exists so a worker that
inherits an abandoned arc can tell a call that may have taken effect from one
that provably never ran.

It has no mode and no projected form. There is no setting at which the model is
told a call was dispatched, because at any ordinary cutoff the answer is the
same as `pending` already gives: no result yet. What the model needs to know
arrives through the outcome of the call the marker names. A worker that refuses
to repeat an inherited call settles it, and `r(T:cancel)` carries why.

The marker is past tense. It says the call left the building, not that anything
is still running, which is what keeps it true after the worker holding it died.

It has no shorthand and appears in no scenario. A record that never projects has
nothing to write on a scenario line, and a token for it would suggest otherwise.

A further execution-plane record has to make this case again. The vocabulary in
this document is about disclosure, so a record with nothing to disclose is a
deliberate exception rather than an established second category.

Async tasks
--------------------
A task is an object of the session, not of the arc that started it. The fold
keeps a session-level task table keyed by task ID, and the invocation note is
emitted in the origin arc, where the model began the work:

    n{task:A2.1 started: …}    work the model began is now running

The invocation note is the task's only record in its origin arc. Every later
task event names the task by ID in its payload and carries an explicit
`LLMVisibility`, decided once on the write path from the task kind's
declaration, and the correlation follows it:

    AS_MESSAGE   the event opens an arc of its own, the way a user message does.
                 The arc's opening bubble is composed content: a note naming the
                 task — `Task <name> with id <id> (emitted an event | asks a
                 question | returned results)` — followed by the event's own
                 content, the notification's payload for a notification, the
                 ask's message for an ask, and for a result the task's result
                 as returned, not summarized. An ask carries one more line
                 between the note and its message, a header that says the
                 question comes from a background task and not from the user,
                 and that it may be answered directly through the input tool or
                 put to the user first: `[background task, not the user —
                 answer it with provide_task_input, or ask the user first and
                 then call provide_task_input with their answer]`. The header
                 is what keeps the model from reading the question as the
                 user's own words. That arc is a work item of its own, and the
                 model works it like any other arc. The opening message of any
                 task-opened arc carries the TASK role, not USER; which wire
                 role it is sent under is left to the encoder.
    IN_RESULT    the event stays correlated to the task and is folded into the
                 final result message of the result-opened arc, gathered from
                 the task table at render time. The message carries the last N
                 of them, or all at N = -1, with N declared per task kind and a
                 global override in the agent config. They are not projected
                 as messages of their own.
    NONE         invisible to the model in every projection. It is in the log
                 for the frontend and for replay.

An ask and a result are always `AS_MESSAGE`: an ask that opened no arc could
never be answered, and a result that opened none would never be worked on. The
one exception is the startup reaper, which settles abandoned tasks with `NONE`
so it opens no arc. A notification's visibility is declared per class on the
task kind, and a report naming a class the kind has not declared is refused at
the write path. An `AS_MESSAGE` notification costs a run, since it opens an
arc; the controller coalesces the notifications of each `AS_MESSAGE` class
into at most one report per group window, and past the kind's event limit it
demotes the event (`NONE` or `IN_RESULT`, per the kind's overflow policy)
instead of opening an arc.

Placement is decided from correlation and sequence number, as for every record.
A task-correlated event appears in no arc's transcript as its own record, and
the payload's `LLMVisibility` records the same decision explicitly: `AS_MESSAGE`
correlates to an arc, `NONE` and `IN_RESULT` to the task, and the fold rejects
an event whose payload disagrees with its correlation.

In the arc an ask opened, the model decides: it either puts the question to
the user or answers the ask itself through the input tool under the invoke's
actor. The answer is recorded as an `AP` correlated to that same arc, so it is
in the ask arc's own transcript at any cutoff. Put to the user, the ask's arc
closes and the human's reply opens a new arc; in that arc the model calls the
input tool, and the `AP` still correlates to the question's arc.

A task does not project through the tool-call protocol. A task with no result
is the normal state, and tool shape would require `pending` to invent an answer
every turn until the task finished. The invocation note is what links what the
model started to what came back, and when the completion opened an arc of its
own, that arc carries the link instead: fixed rule 4.

Every note names its task by name and ID. The ID is there because the model
answers a waiting task by naming it, and a note is the only place it is told
which task is meant. It is written whole: a shortened ID would have to be
matched by prefix, which resolves the wrong task rather than failing to find
one. Where a task's invocation falls below the compaction watermark, its
invocation note is synthesized from the task table at the cut instead of
projected from the event, with the ID verbatim; see Compacted transcript.

A task that was called off is not one that went wrong, so `cancelled` is an
outcome of its own rather than an error: a task-addressed cancel (a `CANCEL`
whose correlation ID is the task ID) settles the task in the log, with who
cancelled it and when, and the runner is reached through the controller. The
completion note for it is emitted in the origin arc, since no arc opened for
it. The startup reaper's settle is the same: a completion note in the origin
arc, written `NONE`.

Interrupts
--------------------
An interrupt is the human-in-the-loop gate and nothing else. It always names
one tool call and holds that call until a human decides. A policy that refuses
a call on its own asks nobody and blocks it instead, which is a different
record and a different outcome.

The decision reaches the model in one of two ways, depending on what it was.

    refused     the call is settled REFUSED and the outcome is `r(T:refused)`.
                No note, no mode: the call had to be answered and this is the
                answer.

    approved    the call runs as if nobody had been asked, so the log's only
                trace is the interrupt itself. A note is the only place the
                approval can be told, and the `interrupt` mode governs it.

The ask is a note of its own, at the position it happened.

    n{ask:I1.1.1}        a human was asked to approve this call before it runs
    n{approved:I1.1.1}   a human approved it

An interrupt carries `Reason`, what the human was asked in words, and
`Context`, whatever else the decision was about: what the call would touch,
which policy asked for a human, anything the person deciding was shown that the
call's own arguments do not say. Both go into the ask, so the model is told
what the human was told and can account for a refusal without guessing what was
on the screen.

Both are written when the interrupt is raised and never change. That is what
keeps the ask stable: it is built from fields that are already final.

The ask is written from the interrupt alone and says nothing about the outcome.
Every transcript that contains it contains the same bytes, so a decision
arriving later appends a message instead of rewriting one and the cached prefix
survives. An ask that reported its own status would have to be rewritten the
moment the answer came back.

The two notes can sit far apart. A human who takes an hour leaves other arcs'
turns between them, which is why the approval names the interrupt rather than
relying on sitting next to the ask.

An unanswered ask is what tells the model it is waiting on a person rather than
on a machine. The call's stand-in says the same thing from the other side:
`r{T:pending}` reads differently for a call nobody has answered and a call
nobody has approved.

Cancellation
--------------------
A cancel is three cases and only one is a mode.

    too late    `TerminatedWork` false. `KX` had already closed the arc.
                Always elided.

    withdrawn   Cancelled before the agent responded. Nothing was attempted.
                The arc is not closed, so `foreign_arc` decides.

    stopped     Cancelled while the agent was working. Always disclosed.

A cancel can also be addressed to a task instead of an arc: a `CANCEL` whose
correlation ID is the task ID. It settles the task, not an arc, and touches no
arc's records — it is not governed by `foreign_arc`, because it closed nothing
the arc's projection shows.

For the stopped case the work and the cancel are disclosed together.

    a(R1.1)[t(T1.1.1)] r(T1.1.1:cancel) n{stopped:U1}

Eliding the work and keeping the note leaves a stop with nothing to refer to.
Keeping the work and eliding the note makes an abandoned attempt read as
unfinished business to pick back up.

The note must not guess at motive. `Cancel.Reason` is optional free text, so
the log usually records that a human stopped this and not why. "The user
stopped this before it finished" is supported. "The user rejected this
approach" is not. Quote `Reason` where it is present.

What a cancel settles
--------------------
A cancel settles the calls its tools never saw, and only those. A call with no
dispatch provably never ran, nothing more will arrive for it, and it is
reported as cancelled.

A call that was already dispatched is not the cancel's to settle. The tool may
still report, and whether it took effect is not something the cancel knows. It
stays pending in the DISPATCHED phase, and a result arriving later settles it
the ordinary way — which is how a call that completed after everything stopped
still reaches the model as completed.

So a stopped arc answers its calls three ways, and the differences are the ones
that decide whether the model tries the work again.

    never dispatched    "[this call was cancelled before it ran]"
                        Definite. Nothing happened.

    dispatched, no      "[this call was handed to its tool and then stopped
    result yet          before it reported. Whether it took effect is not
                        known.]"

    dispatched, result  the result. The cancel stopped the run, not the call.

The middle one is the only thing the log cannot say, and it must not be
resolved by guessing. A model told the call did not run is a model that runs it
again.

The same three readings apply to an arc that died of a fatal error, which is
over for the same reason: nothing further is coming.

An async task is not settled by an arc's cancel at all: a running task does not
hold its arc open, and the task runs where the cancel cannot reach it. What
ends it is a task-addressed cancel — a loop over the task table in the harness,
not a batch record — which writes a `CANCEL` correlated to the task ID, settles
the task in the log with the actor and sequence of the event, and reaches the
runner through the controller. A later report from that runner is refused at
the write path and dropped by the fold, since the task is already settled.

Fixed rule 4 still holds, so a task settled without opening an arc gets its
completion note in the origin arc, placed where the settle happened:

    [task auditor (id 4f1c…) was cancelled before it finished]

A task still running when its origin arc was stopped and projected gets:

    [task auditor (id 4f1c…) was still running when this request was stopped.
     Whether it finished is not known.]

The note is placed where the arc ended, which is after everything the task
managed to report.

Errors
--------------------
`Error.IsFatal` decides which mode applies. A fatal error closes its arc, so
the arc becomes a foreign arc the log does not show as closed and `foreign_arc`
governs its material. A non-fatal error leaves the arc running, and `error`
governs the error record alone.

`Error.CausationID` names the record the failure was about, where there is
one. A note that reduces an error may use it to say what failed.

Foreign arcs
--------------------
`foreign_arc` governs every arc but the projected one that the log does not
show as closed. Three statuses land here: still open, dead of a fatal error,
and withdrawn before anything was attempted. A foreign arc that closed
normally is ordinary conversation history and is projected as such.

    keep    the arc's material is projected like any other conversation, and
            a note reports its status: inflight, dead or withdrawn
    note    the status note alone. It may quote what was asked, and it says
            that someone else is on it.
    elide   nothing

Both disclosing values report the status. They differ in whether the arc's
material comes with it, not in whether the model is told the request belongs to
another turn. Use `keep` when the model should see the request in the user's
own words, `note` when an account of it is enough.

`keep` always reports the status. A bare user turn reads as a request
addressed to the current turn, and an assistant message belongs to exactly one
arc, so a model that answers both files the answer under the wrong arc and
leaves the foreign one open with no response of its own. An arc whose material
was all elided by a finer mode reaches the transcript bare and gets the note
for the same reason.

A task-opened arc has no user words: its request is its composed `Arc.Content`,
the note naming the task followed by the event's own content. A `foreign_arc`
note about it quotes that content, so the quote names the task and carries the
question or the result whole.

One mode covers all three statuses, so an arc is governed the same way at every
cutoff as its status changes. The three share one setting: a session that
discloses open arcs discloses dead and withdrawn ones too.

Ordering
--------------------
Messages are emitted in log order. An arc's material is never gathered up and
presented contiguously.

An arc can surround another. U1 opens, U2 opens and finishes, U1 finishes, U3
opens.

    U1, R1.1, T1.1.1, U2, R2.1, K2x, TR1.1.1, R1.2, K1x, U3!

U1's material lies on both sides of U2's and stays there. R2.1 was written
while U1 was unfinished and may say so. Putting U1's closing response in front
of R2.1 would leave the model reading its own sentence about work in progress
after that work had visibly finished.

A call and its result are the one exception. Providers require the pair to be
contiguous, so it travels together and sits where the call was made. This
understates how long the call took whenever another arc's turn falls inside it.

Composition
--------------------
Modes apply outside in. `foreign_arc` decides whether a foreign arc's material
reaches the transcript at all. Only what survives is offered to `pending`,
`tool_events` and the rest.

`foreign_arc` governs an arc while it is open, once it dies and once it is
withdrawn, so which mode governs a record is fixed for the session along with
the mode's value. See Foreign arcs.

Phases
--------------------
    1  elision     apply modes
    2  attribution synthesize notes for what survived from other arcs

Elision runs first because attribution has to see what survived. An assistant
message left with no text and no surviving calls is dropped in phase 1.

Provider constraints are satisfied by mode selection, not by a repair pass.
`pending: stand_in` answers every call. A provider's requirements constrain
which modes are legal for a session.

Attribution has one placement rule with no mode. A note may not land between a
call and its result. Notes go before the assistant message that opens the block
or after the last result in it.

Left to the encoder
--------------------
Three questions about a projected transcript are answered per provider, in
[ENCODING.md](ENCODING.md).

*Note rendering.* `n{…}` carries no role. Anthropic and OpenAI have an operator
role that can carry a note at a position. Mistral and Gemini do not, so a note
is user text there.

*Task-arc rendering.* `k(…)` carries the TASK role. It is carried as `user`
text by default, which every provider accepts; a provider with an operator
role may carry it as that instead, configured per agent definition
(`task_role`, e.g. `developer` on OpenAI). The content's own header names the
provenance either way, so the role is a convenience, not the only signal.

*Role alternation.* Interleaved arcs can put two user messages in a row.
Anthropic combines them, OpenAI accepts them, Mistral and Gemini reject them.
The encoder merges them, and a merged message labels each part with its origin
so that two people's words do not read as one turn.

Merging messages is the one case where an encoder changes the message list
rather than the wire format. Everything else it does is a shape translation.

Known gaps
--------------------
*Per-task event settings.* Task notifications are governed per class by the
task kind's declaration (`AS_MESSAGE` / `IN_RESULT` / `NONE`), with coalescing
windows, an event limit and an overflow policy declared alongside. There is no
session-wide mode left to disagree with them.

Scenario format
--------------------
Compact form, one line, space-separated, in emission order.

    Messages: u(U1) a(R1.1)[t(T1.1.1)] r(T1.1.1:cancel) n{cancelled:U1} u(U2)

Expanded form, for scenarios where the synthesized text is under discussion.
Calls get an unnumbered line under their message.

    1  u(U1)              "how many pods are pending?"
    2  a(R1.1)            "let me check"
       [t(T1.1.1)]        kubectl_get{kind: pod}
    3  r(T1.1.1:cancel)   "superseded by a later request"
    4  n{cancelled:U1}    "[the previous request was cancelled before it
                            finished. Its result is unknown.]"
    5  u(U2)              "never mind, describe node n1"

Header fields:

    Modes     the session's mode settings
    Arc       the arc being projected
    Events    the events the projection was given, with `x` on any event
              that closed its arc

Scenarios
--------------------
One file each, under [projection-scenarios/](projection-scenarios/).

    01  open-arc-kept                an open foreign arc, projected in full
    02  open-arc-noted               the same, reduced to a status note
    03  open-arc-elided              the same, dropped entirely
    04  pending-call                 a call with no result at the cutoff
    05  dead-arc-elided              an arc closed by a fatal error, dropped
    08  cancel-too-late              a cancel that stopped nothing
    09  cancel-stopped-work          a cancel that stopped something
    10  cancel-withdrawn             a request taken back before it ran
    11  surrounding-arc              one arc wrapped around another
    12  interrupt-approved           a human said yes and the call ran
    13  interrupt-refused            a human said no and answered the call
    14  interrupt-unanswered         an ask still outstanding at the cutoff

01 through 03 are one log under three values of `foreign_arc`. 12 and 14 share
an ask, once answered and once not. Numbers are stable identifiers, not
positions.
