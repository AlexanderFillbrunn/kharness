Compaction
==========

Status
--------------------
A proposal, not a specification, and the starting point for the
implementation work. [PROJECTION.md](PROJECTION.md) and
[ENCODING.md](ENCODING.md) describe what the code does today; nothing here is
binding until it is folded into those two. The code follows neither this
document nor [ASYNC-TASKS.md](ASYNC-TASKS.md) yet.

Overview
--------------------
A session grows without bound and a provider's context does not. Compaction
replaces a prefix of a session with a summary so the session can continue past
the size of its own history.

Compaction appends. The log is never rewritten: a `COMPACTED` event records
that a prefix was summarised, and a reader that ignores it replays the session
whole. What is compacted is what the next projection reads, not what the
store holds.

A compaction is not an arc: it runs no tools, produces no reply, and the
model it calls is not the session's model acting as the agent. A `COMPACTED`
record carries no correlation ID — the cutoff rule gives it its scope, under
What an arc reads. A request correlates to the session's stream and a
suspension to the arc it pauses.

A compaction takes effect for every run started after it lands and changes
nothing for a run in flight, by the rules under Where the cut goes and What
an arc reads.

The record
--------------------
Compaction has three records: `COMPACTION_REQUESTED`, the want (described
under The window and admission control); `ARC_SUSPENDED`, the paused arc
(described under When compaction runs); and `COMPACTED`, the result, whose
payload is along these lines.

    type CompactedPayload struct {
        // ThroughSeq is the last event this summary accounts for.
        // Everything at or below it is replaced; everything above it
        // survives.
        ThroughSeq int

        // Summary is the replacement material, as text.
        Summary string

        // Method names which compactor ran: an LLM summary, pruning
        // with a note, or a provider's compaction endpoint.
        Method string

        // Modes records the projection settings the summarised
        // transcript was built under. A summary made under other
        // settings describes a conversation this session never had.
        Modes history.Modes

        ModelID string
        Usage   *TokenUsage
    }

`ThroughSeq` is the watermark. Which compaction is in force, what a reader
loads and whether two workers conflict are all answered by comparing
watermarks.

Compaction is recursive: the second summary is made from the first plus what
has happened since. The chain is the `COMPACTED` events ordered by
watermark, which is in the log; no field records it, and the coverage rule
discards one that arrives out of order.

The watermark is in the payload and not taken from the record's own
position: a compaction computed through sequence 100 is written at 141, and
everything written in between must be read.

Where the cut goes
--------------------
Three constraints on `ThroughSeq`.

*A tool call and its result are not separated.* Providers require the pair
to be contiguous, and the projection makes it the one exception to log order.

*A reference across the cut must resolve in what the model reads.* A tool
result names its call; an async result names its task. References that stay
inside one arc are safe on an arc boundary. A task whose invocation falls
below the watermark is still resolved — the fold reads the whole log, so the
task table has it — and the projection synthesises the invocation note from
the task table, as described under Compaction and the fold.

*An open arc's live material stays above the cut.* The live material is the
arc's last response, an ask it has made that nobody has answered, and the
request that opened it. Everything else in an open arc — the tool rounds
already accounted for in a response — is spent prefix: summarised where the
line passes through, and kept whole below the line where the opening bound
applies.

The rule that satisfies all three:

    bound(arc) = opening(arc)        where the arc's own log fits the arc threshold
              = liveBoundary(arc)   otherwise
    ThroughSeq = min(bound(arc) for arcs where arc.Status() != closed) - 1

The live boundary of an arc is its last turn boundary, or its opening where
it has taken no turn. An arc that has not been worked pins the watermark at
its opening. A worked arc takes the opening bound — the threshold under
The arc the line passes through that decides between the whole arc log and
a skeleton — where its own log so far fits it, and the line is drawn before
the arc. The reason is duplication: an arc under the threshold renders its
whole log verbatim, so a cut inside its spent prefix would summarise
material the transcript then repeats word for word; the opening bound keeps
the summary and the arc log disjoint, at the price of carrying the arc
whole in every transcript until the next compaction. Only an arc too large
to keep whole is cut at its live
boundary, with the line passing through its spent prefix. The watermark is
shared by all open arcs, so it advances only as far as the least advanced of
them permits; an arc that stays open without taking a turn holds the line
for the others.

The fit is measured when the compaction computes its watermark, against the
arc's log as it stands then. An arc kept whole that outgrows the threshold
afterwards is not re-cut by a compaction already in force: the arc renders
whole against that line, and the next compaction, finding the arc over the
threshold, draws its line at the arc's live boundary instead.

Pictures of the cut
--------------------
Five pictures are drawn as diagrams under [diagrams/](diagrams/), one
per case: [1, the ordinary case](diagrams/compaction-1-ordinary.svg),
[2, the cut inside a too-large arc](diagrams/compaction-2-cut-inside-large-arc.svg),
[3, the cut before a fitting arc](diagrams/compaction-3-cut-before-fitting-arc.svg),
[4, parallel open arcs that all fit](diagrams/compaction-4-parallel-arcs-fit.svg)
and [5, parallel open arcs with the oldest too large](diagrams/compaction-5-parallel-oldest-too-large.svg).
Each draws the log as event blocks with the arcs bracketed and the cut as a
double line, and beside the log a strip showing the context estimate as a
step function — one step per event, at the event's position — against the
soft and the hard budget as lines the curve crosses. The parallel cases 4
and 5 draw one column per run: an event both runs read spans both columns,
an event of one run's own stays in that run's column.

Four situations, drawn vertically: the log grows downward, arcs are
bracketed on the right, and the double line is the cut. Everything above
the cut is replaced by the summary; everything below it is read as it
is. An arc is open while the agent may still work it; the finished part
of an open arc is the tool work its own last reply has already covered,
and the live part is what must stay below the cut: the last reply, an
unanswered question, and the request that opened the arc.

*The ordinary case.* The cut sits just below the oldest open arc. Arc 3
has been opened but not yet worked, so the cut and arc 3's opening touch:
its first turn reads the summary plus its own events.

    user message            ┎
    agent reply             │ arc 1, closed
    arc closes              ┘

    user message            ┎
    agent reply             │
    tool call               │ arc 2, closed
    tool result             │
    agent reply             │
    arc closes              ┘

    ═══════ the cut: the summary covers everything above this line

    user message            ┎ arc 3, open,
    ...                     ┘ not yet worked

*The cut inside an arc.* How this arises: after one of arc 2's rounds the
lower budget is crossed, so the loop writes a request and the compaction
proceeds in parallel while the run continues; a later round would cross
the higher budget, so the run writes `ARC_SUSPENDED` and stops. The
compaction computes its watermark from the log as it stands: arc 2 is too
large for the line to be drawn at its opening, so its live boundary — its
last turn — decides, and the cut lands inside the arc, with the
finished part above it and the live part below it. Had arc 2 fitted under
the arc threshold, the cut would have been drawn at its opening instead and
the whole arc log would survive intact. Arc 2's next turn
reads the summary, a note about what was summarised, and its own events
below the cut.

    user message            ┎
    agent reply             │ arc 1, closed
    arc closes              ┘

    user message            ┎
    agent reply             │
    tool call               │ arc 2, open,
    tool result             │ suspended
    agent reply             │  (finished part)
    ═══════ the cut
    tool call               │
    tool result             │  (live part)
    agent reply             │
    arc suspended           ┘

    user message            ┎ arc 3, open
    ...                     ┘

*Full against skeleton.* The same arc 2 after its suspension is
released: the compaction has landed and the completion scan has requeued
it, so its next turn reads the summary plus the arc's own log. The arc's
log contains one user message — the request that opened it — followed by
its tool rounds; where the cut sat depends on the arc's size when the
compaction computed its watermark. Small, and the opening bound placed
the cut at the arc's opening: nothing of the arc was summarised, the
whole log is kept in full, and no note stands inside it. Large, and the
cut sat at the arc's live boundary: the finished part that fell above it
was summarised with everything else, and the arc log's surviving part
renders as a skeleton.

    small, the cut at the opening, the arc's whole log kept in full:

      [ summary: everything before the arc ]
      the request that opened the arc, word for word
      agent reply
      tool call
      tool result                   <- the arc's whole log,
      agent reply                      kept as it is
      tool call
      tool result
      agent reply

    large, the cut inside, a skeleton:

      [ summary ]
      the request that opened the arc, word for word
      the tool call, without its result   <- finished part, thinned:
                                          the reply and the result are
                                          dropped, the call stays

      [ note: the work above this note was summarised ]

      tool call
      tool result                   <- events below the cut,
      agent reply                      unchanged

*Two views of one prefix.* The trigger here is the lower budget alone:
an earlier round — of this run or of another arc — crossed it, so a
compaction was requested and proceeds in parallel. It lands while a run
is in flight; that run never crossed the higher budget and never
suspended. It keeps the transcript it started with; the next run reads
the summary. Time runs downward.

      a run starts and reads the transcript as it is
      ...
      the compaction lands in the log
      the run is unaffected: its transcript was fixed at its start
      the run writes its reply
      ─ ─ ─
      the next run starts here and reads the summary

What an arc reads
--------------------
The fold reads the whole log. Compaction changes what the model is told, not
what the fold loads: the task table, the arc statuses and every other piece
of folded state are built from all the events, including the ones below the
watermark. A compaction never removes a fold input, so a reference to a
record below the watermark resolves from the log itself and nothing needs to
be carried across the cut.

What a turn reads is the compaction in force at its start — the `COMPACTED`
with the greatest `ThroughSeq` among those written at or before the turn —
and the transcript it renders is the projection of the events the arc's
cutoff selects, with everything below that watermark replaced by the
summary:

    summary of Seq <= ThroughSeq, plus
    projection of ThroughSeq < Seq <= opening, plus
    projection of Seq > opening where correlatesTo(arcID)

Where there is no compaction, the first clause is empty and the transcript
is the projection of the events the cutoff selects.

A run is in flight while a worker is executing it; suspending ends the
in-flight period, and the resume is a new turn. A run in flight is
unaffected by a compaction that lands mid-run: the watermark rule keeps the
line below its own last turn, so the new compaction is not in force under
it. An arc that has been opened but not yet worked is quiescent — nothing
has been written against its prefix — so its first turn reads whatever
compaction is in force then. A suspended run is in the same position: not in
flight, so the line sits before it — at its opening where it fits the arc
threshold, at its live boundary with its earlier rounds above the cut where
it does not — and its resume reads the compaction in force then. This
is what allows a run to be deferred during a compaction window while its
trigger event is written immediately; see Where a run waits.

The fold gains one field, `History.LastCompaction`, set by the coverage rule:
a `COMPACTED` replaces the one held only when its watermark is greater, never
merely because it arrived later. Two compactions can be computed at once and
written out of order; coverage, not recency, decides which one is used, which
makes a duplicated or abandoned compaction harmless.

The apply functions check for a missing arc and return an error rather than
panicking. A correct log never produces one; the checks are where a malformed
log is detected, whatever produced it.

Snapshots
--------------------
Reading the whole log for every run is the baseline, and for a long session
it is a cost of its own. The answer is a separate mechanism, not part of
compaction: a snapshot persists the folded state, and a worker loads a
snapshot plus the events written after it instead of folding from the first
event. A snapshot is valid whatever the watermark has done, because the
fold reads everything and compaction removes nothing; the two mechanisms do
not interact. Specifying snapshots is separate work and not done here.

The arc the line passes through
--------------------
An open arc whose spent prefix falls below the watermark renders against the
cut. Under this design the only such arcs are suspended ones that do not
fit the arc threshold: an arc that has not been worked pins the watermark
at its opening, a run in flight pins the line below its last turn, and a
suspended arc that fits takes the opening bound and keeps its whole log
below the cut — so the `ARC_SUSPENDED` event and the line mark the same
arcs, the event is the projection's explicit trigger, and only the arcs
over the threshold render against the cut at all. Its transcript at a turn
is:

    the summary of everything below the watermark, plus
    the arc's own log, rendered as described below, with
    a note standing at the cut position inside it.

The note marks where the cut fell. Placed at the head it would say only
that something was summarised, and the model could not tell which of the
verbatim events that follow were part of the summarised region; placed at
the cut, the note separates the two regions on the page the way the
watermark separates them in the log. The note also states which record
wins: the arc's own log is verbatim, the summary is not, and where the
two disagree the model is told to trust the arc log. Where an
`ARC_SUSPENDED` stands in the log, the note also says the request was
paused while its context was compacted. The summary and the arc log do not
otherwise overlap: an arc small enough to render whole is never spanned,
because the opening bound placed the cut before it, and the skeleton thins
the finished part rather than repeating it — the summary covers the region
once, in prose, and the arc log carries only what the cut left below it.

*Fitting arcs never render against the cut.* A suspended arc whose own log
fits the threshold was kept whole by the opening bound at the moment the
watermark was computed, so nothing of it was summarised and its transcript
is the summary plus its own log in full, with no note inside it. Compaction
is lossless for every arc that stayed small; the lossy rendering is
reserved for arcs that do not fit, which is the only case where dropping
material is better than overflowing — and the only case the line passes
through.

*Above it, a skeleton.* The arc's log is rendered as its opening request,
verbatim; the tool calls it made, without their results; and its messages
below the line — the arc's live part, which survives whole. What is dropped
are the tool results of the spent
prefix: superseded middle results, not current state — the last response is
above the cut — and re-derivable by calling the tool again where the tool is
idempotent. A tool that is not idempotent, or whose results are too
expensive to re-derive, wants a registry-level mark, not a compaction rule.

*Derived, not recorded.* The threshold test, the note and the skeleton are
rendered from the payload the compaction already carries, so the payload
gains nothing for them. Whether an arc fits is a token count taken when
the compaction draws its line — the bound rule under Where the cut goes —
and the test decides the rendering the same way, so the projection is a
function of the log, the cutoff, the threshold and the tokenizer. If exact
checkability is ever wanted, one bit per spanning arc in the payload
restores it.

*Conditions.* The cut takes effect at the start of an arc's next turn, never
during a run. Tool calls and their results are never separated by the line.
The live material — last response, outstanding asks, the opening request —
is above the line by construction, so the skeleton only replaces material
the rule already classified as spent.

Two views of one prefix
--------------------
A run in flight keeps the transcript it started with; a run started after a
compaction lands gets the summary. Two runs under way at once therefore have
different views of the same prefix, differing in the resolution of one
region; the watermark rule bounds the divergence. Each turn works from a
consistent snapshot, and no response is written against a transcript other
than the one it was started with.

Under `foreign_arc: keep`, a later arc reads an earlier arc's responses, and
a response written against the full head may refer to detail the later arc's
head no longer holds. The exposure is bounded to material below the
watermark.

Projecting a compaction
--------------------
The summary goes at the head of the transcript, before the first surviving
arc, and nothing precedes it but the system prompt. It is synthesised, so
by PROJECTION.md's definition it is a note, and `ModelNoteMessage` is the
shape for it; PROJECTION.md's sentence saying a note is one line changes,
since a summary is the one note that is not short. It is not an INSTRUCTION
message. No mode governs it: a session that compacts has decided the
material is gone.

What is summarised
--------------------
The summary is made from the projected transcript, not from the history and
not from the log — the projection has already decided what the model is
told, and summarising the log would let elided material back in through the
one message no mode reaches. Compaction sits after projection and before
encoding:

    log ──fold──▶ History ──project──▶ ModelTranscript ──compact──▶
    ModelTranscript ──encode──▶ provider messages

The compactor's input and output are the same type: it takes a transcript
and returns a shorter one, which is what makes compaction testable without
a provider.

The compactor
--------------------
A separate interface, with three implementations.

    type Compactor interface {
        Compact(ctx context.Context, t history.ModelTranscript,
            through int) (CompactionResult, error)
    }

The method is configured on the agent: `AgentConfig` names which compactor
runs. There are three.

*An LLM summary.* A model call over the region below the watermark. The most
faithful and the most expensive method, and the source of the tens of
seconds the window is about.

*Pruning with a note.* No model call. The material below the watermark is
dropped and replaced by a note naming what was dropped: how many arcs, how
many tool rounds, over what span. It is the spanning arc's skeleton applied
to the session's closed material, and it makes compaction cheap enough to
run routinely.

*A provider's compaction endpoint.* Some providers compact a transcript
themselves; what the compactor sends and how it renders the answer is the
implementation's business.

One constraint on every implementation: what lands in the log must be text.
An opaque handle to server-held state produces a summary only that provider
can read, and the session stops being portable across models.

The method that ran is named in the `COMPACTED` payload, as `Modes` is: a
summary is a faithful account of the conversation only under the method and
settings that made it, and a reader deciding how much to trust it needs to
know which produced it.

Reasoning does not survive compaction. It belongs to one assistant message,
and a message that has been summarised away takes it with it.

When compaction runs
--------------------
Two budgets are configured on the agent: `AgentConfig` gains a lower budget
and a higher budget, as token counts, alongside the compaction method. The
lower budget triggers compaction and sits well below what the provider
accepts; the higher budget suspends runs and sits close to the limit. The
lower is checked first on any round, so by the time a run threatens the
higher budget, a compaction is already in flight or already landed. A run
reaching the higher budget is expected for long arcs.

Whether compaction is due is a pure function of the log. `AssistantMessage`
`Payload.Usage` carries the input tokens of the last reply, and distinguishes
nil from zero so that a reply whose cost went unreported is not counted as a
free one. The fold gains a predicate, `History.CompactionDue(budget)` or
similar, and compaction is due when:

    the last reply's reported input tokens exceed the lower budget, and
    that reply sits above the watermark of the compaction in force, and
    no COMPACTION_REQUESTED is unsatisfied.

The second clause prevents a loop: just after a compaction lands, the last
reply's usage still describes the replaced transcript, and a rule reading
only the usage would find the session due again. A reply counts only if its
cost was measured against the compacted transcript. A reply whose usage is
nil counts as not due.

*In the loop, after every round, the lower budget.* The loop consults the
predicate after each reply. Where due, it writes a `COMPACTION_REQUESTED`
naming the current watermark and enqueues the compaction work item, and
nothing is deferred: the compaction proceeds in parallel with the runs
still going. A run in flight is unaffected by its landing, and the next
turn reads the compaction in force then. The check after the final round
subsumes a check at arc close. The request in the log makes the decision
durable: a duplicate queue item is discarded when its request is already
covered, and a lost one leaves the request standing for any worker to
take.

*Before a run and between rounds, the higher budget.* A run that would
cross the higher budget suspends. Between rounds the request needs no
write: the lower budget is checked first on the same round, and the lower
budget is the smaller number, so the round that crossed the higher budget
already crossed the lower one and the request stands. Before a run the last
reply's usage can be stale — many events may have arrived since it was
written — so the admission check estimates the transcript the run would
read; where it is over the higher budget and no request stands, the check
writes one.

The estimate is made against the compaction in force, so a request that
stands unsatisfied leaves the estimated transcript full. A fresh arc
opened while another arc's suspension is outstanding normally finds its
estimate over the higher budget at admission, and the check writes its
`ARC_SUSPENDED` before it has run: its opening is later, so its cutoff
holds at least the material that made the other arc suspend. The cause
is the arc's own check result, not the other arc's stop — another arc's
unresolved suspension is a predictor of the crossing, not a cause of it
— and the criterion stands with no suspension anywhere in the log: a
fresh arc whose own estimate crosses the higher budget suspends the same
way. The one exemption follows projection, not suspension: an arc whose
modes elide the other arc's material entirely — whose projected
transcript contains none of what made it suspend — estimates low and
runs. Where the request stands the arrival of the newcomer needs no new
request; the one already in the log is satisfied for every arc by the
single `COMPACTED`.

And a closing point about when the watermark is drawn: a free worker
takes the item at once, and the handler reads the log and fixes the
watermark at its start — the tens of seconds of the window are the
summary computation, which runs afterwards and cannot move the line. The
compaction therefore normally reflects the open arcs as they stood at
the request: the bound rule is evaluated on the log at that read, and an
arc opened afterwards is no part of the bound; the arcs it leaves open
below the cut keep their whole logs, and the first turn it delays meets
the compaction through the admission check instead. Where the item
waits — a lease held, a busy queue — the read is later and the watermark
may reach farther, past the request's own sequence: the request names a
floor, not a snapshot, and the coverage rule asks only that the result
reach past the floor. A request that falls below the new watermark is
discarded on read once covered, like any other.

*Suspension is recorded.* The suspending run — or the admission check, for
a run that has not started — writes an `ARC_SUSPENDED` event, correlated
to the arc, with causation naming the `COMPACTION_REQUESTED` and no
payload of its own — and the request, never a run or another arc's
suspension, is the right cause: a suspension is a wait, the object of
the wait is the compaction that satisfies the request, and one request
in the log serves every suspension this way, which is what the
completion scan's link is made of. A suspension written by the
admission check, with no other arc suspended anywhere, names the same
kind of cause. The run then ends its in-flight period and the arc is
left unworked; nothing is enqueued and nothing waits on a condition. A
suspension has three derived states, all read from the log: *outstanding*
until a `COMPACTED` covering the request's watermark lands; *released*
from then until the arc is worked again; *resolved* once a response
follows it. The interface gets the state through the event sink: an arc is
suspended, waiting for compaction. The resume is a new turn reading the
compaction in force at its start, rendering as described under The arc
the line passes through.

A worker that dies before writing the event leaves the arc as any crashed
run: its item is reaped by autoclaim, the re-offering worker's admission
check finds the arc over the higher budget, and that check writes the
suspension. Nothing strands.

*The completion scan.* The compaction item's handler, having written the
`COMPACTED`, scans the fold for arcs with a released, unresolved
suspension — an `ARC_SUSPENDED` with no response after it, whose request
the `COMPACTED` just written covers — and enqueues a work item for each.
The scan is part of the handler, so a reaped item redoes it: a worker
that dies between the `COMPACTED` and the scan leaves the item to
autoclaim, and the taker still scans. An arc that suspends after a scan
has crossed the higher budget on the compacted transcript, so the lower
check on that round wrote a new request, and the next compaction's scan
picks the arc up.

*One compaction per episode.* A run that crosses the higher budget
suspends onto the compaction its own earlier rounds triggered; no second
compaction is produced.

One residual case: if even the compacted transcript is over the higher
budget, the resumed round is rejected by the provider. The budgets were
set wrong; it is a condition to alert on, not a mechanism to build.

Parallelism modes
--------------------
The two events admit four modes, differing on two axes: where the check
runs (safe points only, or after every round) and what the requesting run
does (waits for the `COMPACTED`, or proceeds on the transcript in force).
No mode needs a mechanism beyond `COMPACTION_REQUESTED`, `ARC_SUSPENDED`
and the completion scan; a mode is a choice of where the checks run and
whether the requesting run suspends.

*Never mid-run, sync.* The predicate is consulted only at safe points:
the admission check before a run, and after the final round. Where due,
the check writes `COMPACTION_REQUESTED` and an `ARC_SUSPENDED` and leaves
the arc unworked; the completion scan requeues it. Every arc is worked
entirely against one transcript.

*Never mid-run, async.* The same checks, but the requesting run is not
suspended: the request is written, the work item enqueued, and the run
starts on the transcript in force. The next run reads the compaction in
force at its start. `ARC_SUSPENDED` is never written for the lower
budget; the higher budget suspends as in every mode, at the admission
check and between rounds.

*Mid-run sync.* The predicate is consulted after every round. Where due,
the loop writes `COMPACTION_REQUESTED` and `ARC_SUSPENDED` and ends its
in-flight period; the completion scan requeues the arc, and the resume is
a new turn against the compacted transcript. This is the higher-budget
suspension applied at the lower budget: no run is in flight during a
window.

*Mid-run async.* The design of this document. The loop writes the request
after any due round and continues; a run is in flight during the window
on its fixed transcript, and `ARC_SUSPENDED` appears only where the
higher budget would otherwise produce a rejected round.

The mode is fixed by where the agent loop and the admission check call the
predicate; the sync modes pay for a simpler transcript discipline with
idle arcs during the window.

How a compaction and a run meet
--------------------
Every way the two can coincide, and whether the run waits, under the
design's mode. The cases are exhaustive: a compaction is either
requested before a run or during one, and a run either crosses the
higher budget or does not. Only cases 1 and 4 block; the rest are here
because a reader asking where a run can wait needs to see the ways it
does not.

*Case 1: compact, then run.* The admission check finds the would-be
transcript over the higher budget and suspends the arc before it starts.
Blocking: the whole window — request, queue and computation.

      before a run, the admission check estimates the transcript
      the estimate is over the higher budget
      the check writes the request and the suspension
      the arc waits, unworked
      ...
      the compaction lands
      the completion scan requeues the arc
      the run starts, reading the summary

*Case 2: compact and run in parallel.* A run starts while a request
stands and works the transcript in force. Blocking: none.

      a request already stands, from an earlier round or another arc
      the run starts, reading the compaction in force — the old one
      the compaction work item is taken; the summary is computed
      the run takes its rounds on the transcript it started with
      the compaction lands — not in force for this run
      the run writes its reply
      the next run reads the summary

*Case 3: run, lower budget crossed, run finishes.* The ordinary mid-run
case: the request is written behind the run. Blocking: none.

      the run takes a round; its reply crosses the lower budget
      the loop writes the request; the compaction proceeds in parallel
      the run takes its remaining rounds, its transcript fixed
      the compaction lands — not in force for this run
      the run writes its reply and finishes

*Case 4: compact and run in parallel, then suspension.* A later round
would cross the higher budget; the run suspends onto the compaction its
own earlier round triggered, so the wait is for a completion only.
Blocking: the tail of the window.

      the run takes a round; its reply crosses the lower budget
      the loop writes the request; the compaction proceeds in parallel
      the run takes more rounds on its fixed transcript
      the next round would cross the higher budget
      the run writes the suspension and stops; the arc waits
      ...
      the compaction lands, the cut before the arc or, where the arc grew
      past its threshold, inside the arc's finished part
      the completion scan requeues the arc
      the run resumes as a new turn, reading the summary

*Case 5: a compaction lands while a run is in flight.* The two-views
case: the landing is not in force under the run. Blocking: none.

      a run is in flight, its transcript fixed at its start
      a compaction requested earlier lands in the log
      the run is unaffected: the landing is not in force under it
      the run writes its reply
      the next run of any arc reads the summary

*Case 6: the compacted transcript is still over the higher budget.* The
residual case of When compaction runs: the budgets were set wrong.
Blocking: none built in.

      the compaction lands; the suspended arc is requeued
      the run resumes, reading the summary
      the provider rejects the round: even the compacted transcript
      is over the higher budget
      an alert, not a mechanism

The window and admission control
--------------------
The window is the time between a request and the `COMPACTED` that satisfies
it: tens of seconds for the LLM method, a model call over a long transcript;
a single write for pruning. During the window a run would read the
compaction in force at its start — the old one, since the `COMPACTED` event
is not written yet — and project the full history. The window is not a
correctness problem; it is why runs must be able to wait.

That a compaction is wanted is recorded by a `COMPACTION_REQUESTED` event:

    type CompactionRequestedPayload struct {
        // Watermark is the cut the compaction is to be computed against.
        Watermark int

        // Actor is who asked, where a person did.
        Actor *users.Actor
    }

A request is a fact about the session, not a claim on the work: it names no
holder and there is nothing to hand back. It is satisfied the moment a
`COMPACTED` with a covering watermark lands, and discarded on read once
covered — the same coverage rule that decides which compaction is in force.
No retraction event: a request that is no longer wanted is never satisfied
and costs nothing.

That a compaction is *running* is not recorded in the log, because it is a
fact about a worker. A compaction is a work item in the same queue as an
arc, and the lease it runs under is scoped to the session, not to the item:
the first handler to take it excludes every other worker from computing a
compaction for that session, whatever item they hold. A handler that finds
the lease held leaves its item unacknowledged, and the stream's autoclaim
re-offers it later — by then the request is either covered, and the taker
discards the item, or the holder died, the lease expired, and the taker
computes. Parallel computation is therefore prevented even where two
requests were written in the same instant by two workers crossing the lower
budget together; the predicate's unsatisfied-request clause narrows that
case to the instant itself. The lease's expiry bounds a worker that died
computing, and autoclaim is the whole of the reaping a compaction needs.
A taker first checks the log: an item whose request is covered by a
`COMPACTED` that has landed is discarded, and otherwise the item is computed.
A worker whose lease expires while it is still working writes a `COMPACTED`
that is accepted anyway, because the coverage rule does not care about
arrival order.

The interface reads the lease and the queue for the running state, the same
way it reads them for an arc being worked; the log carries the request and
the result, and the running is the harness's to show.

Where a run waits
--------------------
Checks defer runs; they never withhold an event. A user message is written
when it is sent; a task report when it arrives; tool results, cancels and
closes throughout the window. The only writes the checks make are the
request and, where a run suspends, the suspension. The two waiting runs
— case 1's, which has not started, and case 4's, which has suspended
mid-loop — wait the same way: the arc is left unworked, marked suspended,
and the completion scan requeues it. No queue item carries a wait
condition. The user's words are in the log for the whole window, so a
process that dies in it loses no message. The wait is bounded by the
compaction's duration, the lease's expiry, and a worker being alive to
take the item.

Async tasks
--------------------
No arc is held open by a running task: the arc that invoked a task closes
when its synchronous work is done, and the task's later events open arcs of
their own. The watermark is therefore pinned only by in-flight synchronous
work, and a long-running task never blocks compaction.

A task's events correlate to the task; an event that opens an arc is that
arc's opening record; a task report is written when it arrives and its run
is deferred by the same rule as any other run. The one place a task meets
the watermark is the transcript: a task whose invocation falls below the
line is still in the fold's task table, and the projection synthesises its
invocation note, as described under Compaction and the fold.

Compaction and the fold
--------------------
The fold reads the whole log, so an unsettled task whose invocation falls
below the watermark is still in the fold's task table with its ID, its name,
its origin arc and its status. Nothing is carried across the cut.

What the cut does affect is the transcript. PROJECTION.md's fixed rule 4
says a task is always two notes, at invocation and at completion, and the
invocation note for a task whose invoke was summarised away cannot be
projected from the event. The projection synthesises it from the task
table: the ID verbatim — never from the summary's prose, which will not
reliably reproduce it and a shortened ID resolves the wrong task — the
name, and the status, including a waiting task's outstanding question.

`SettleAbandonedTasks` settles tasks whose runners died. It is not
structural to compaction — an unsettled task pins nothing and is carried
nowhere — but it is wanted: an unsettled task stays in the task table and
the frontend's task list for ever, and the reaper is what bounds that.

What it costs
--------------------
*The cached prefix.* A compaction rewrites the front of the transcript, so
the first turn after it pays full price for the whole prompt. A lower budget
set too high in token terms can spend more on cache misses than it saves on
context; the budgets want choosing with that in mind.

*Fidelity, unrecoverably.* Once the watermark moves, no later projection can
get the material back. This is the argument for compacting the transcript
rather than the log — the log keeps everything either way.

*The carrier.* The summary is much larger than a note, which makes the
carrier question from ENCODING.md heavier: a provider with an operator role
can carry it; otherwise it is user text.

What would have to change
--------------------
`pkg/events` gains `COMPACTED` and `COMPACTION_REQUESTED` with their payloads
— `COMPACTED` carrying the method that ran — and `ARC_SUSPENDED`, which
carries no payload of its own; the codec gains their cases.

`pkg/history` gains `LastCompaction` on `History`, the apply function that
sets it by coverage rather than arrival, `CompactionDue`, and the derived
suspension states: outstanding until a covering `COMPACTED` lands, released
until a response follows the `ARC_SUSPENDED`, then resolved. The fold
itself changes nothing else: it reads the whole log, as it does today.
The missing-arc and nil-correlation checks are already in.

`pkg/history/projection.go` emits the summary at the head of the transcript,
the note and the threshold test and the skeleton for a suspended arc — the
`ARC_SUSPENDED` event being the trigger — and the invocation note
synthesised from the task table for an unsettled task whose invoke is below
the watermark. `Modes` gains nothing, because compaction has no mode.

`pkg/agent` gains the `Compactor` interface with its three implementations,
the method and the two budgets on `AgentConfig` and with them the arc
threshold the bound rule tests, the bound rule itself that computes the
watermark from the open arcs — an arc's opening or its live boundary,
whichever the threshold allows — the loop's check after
every round that writes a `COMPACTION_REQUESTED` and enqueues the compaction
work item when the fold says one is due, the compaction lease scoped to
the session and the leaving-unacknowledged of an item that finds it held,
the discard of an item whose request a landed `COMPACTED` already covers,
the write of an `ARC_SUSPENDED` by a suspending run and by the admission
check, the completion scan that requeues every arc with an outstanding
suspension, the higher-budget check before a run and between rounds, and
the autoclaim that reaps an expired item so that a dead worker's compaction
is taken up by another. `TaskReports` is unchanged, since a report is never
held.
`GuardCancellation` needs no change: `startsWork` covers dispatches and
task invocations, and a compaction is neither.

The store gains nothing: the fold reads the whole log as it does today, and
the compaction in force is answered from the folded `COMPACTED` events. A
snapshot mechanism — persisting the folded state so a worker loads a
snapshot plus the events after it — is separate work, and not specified
here.

PROJECTION.md gains a section, and loses the sentence saying a note is one
line. Rendering follows: `RenderChat` and devchat's transcript draw a
session as a sequence of arcs, and a compacted session is a summary block
followed by arcs.

Open questions
--------------------
*What the budget is measured in.* Reported input tokens are the reliable
measure and are the provider's, arriving one turn late and absent when a
provider reports nothing. An estimate from the transcript is available
immediately and is wrong by an unknown amount. The first is probably right
and the nil case needs an answer.

*Whether a summary is one block or several.* One block per compaction is
simplest and means the second compaction summarises a summary, losing a
little more each time. Keeping each generation as its own block bounds the
loss and grows the head of the transcript without bound. Nothing here
decides it.

*What the compactor is told about the tail.* The summary is better if it
knows what the conversation went on to be about, and the tail is exactly the
material that is not being summarised. Passing it costs tokens on a call
made to save tokens.

*What the lease expiry should be.* Long enough that a healthy compaction is
never taken over while it is still working, short enough that a dead worker
does not hold the request past a person's patience. A measured distribution
of compaction times decides it; a guess does not.

*What the spanning arc's threshold is.* One number does double duty: the
bound rule uses it to decide where the line is drawn — before a fitting
arc, at the live boundary of one that is not — and the projection uses it
to decide how a spanned arc renders. It wants choosing against the
duplication the opening bound avoids and the cache cost of re-cutting
rather than as a round number under the context limit.

*Whether the higher budget is reachable at all.* If the lower budget is
set well, a run rarely suspends, and the higher-budget check and the
completion scan are machinery for a case that rarely arises. They should
be built and then measured, and the measurement decides whether they
stay.
