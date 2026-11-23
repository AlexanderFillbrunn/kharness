Parallel tool calls
===================

Status
--------------------
Built. `toolStep` in `agent/pkg/agent/agent.go` partitions the pending calls
into waves and runs them as described below; the predicate is `ReadOnly`
next to `SafeToRepeat` in `agent/pkg/agent/tools.go`. The wave-size cap is
`MaxParallelToolCalls` on `AgentConfig`, resolved by `parallelCap`: zero
means the default, `DefaultMaxParallelToolCalls = 4`, negative means no
cap. `agentState.writeMu` serializes persist-and-fold, because a wave settles
its calls from several goroutines and the fold demands events in seq order.
Tests live in `agent/pkg/agent/parallel_test.go`.

The problem
--------------------
A model that wants three files read emits three calls in one response, and
today each waits for the one before it. The reads do not conflict with each
other, so the waiting buys nothing but wall-clock time. Tools that write do
conflict — with each other and with reads — so they cannot simply all run at
once. The change separates the two.

The rule: waves
--------------------
*REQ-1* The pending calls of a step are partitioned, in emission order, into
waves. A wave is a contiguous run of read-only calls, of size at most the
cap (REQ-2), and runs concurrently. Every other call — a write, a task
operation, anything not declared read-only — is a wave of its own and runs
alone, exactly as today. Waves run in order, one after another.

    r r w w r r      ->  [2r]  w  w  [2r]
    r r r w w r r    ->  [3r] w  w  [2r]
    w r r w          ->  w  [2r]  w
    r w r            ->  [1r] w  [1r]

A wave of one read is still a wave; the degenerate case is today's behavior.

*REQ-2* The cap. `AgentConfig.MaxParallelToolCalls` bounds how many calls one
wave runs at once. Zero means the default, 4. Negative means no cap. One
means fully sequential — today's behavior, useful as a kill switch. A
contiguous run of n read-only calls under cap c splits into chunks of at
most c, each chunk a wave, chunks running in emission order:

    7 reads, cap 4    ->  [4r]  [3r]
    9 reads, cap 4    ->  [4r]  [4r]  [1r]

Read-only means `readOnlyHint`, nothing else
--------------------
*REQ-3* A call joins a parallel wave only if its tool's annotations set
`ReadOnlyHint`. `IdempotentHint` alone does not qualify: an idempotent call
may still mutate (set x to 5 twice is one effect, but it is an effect), and
two such calls racing is exactly what the waves exist to prevent.

`SafeToRepeat` (`ReadOnlyHint || IdempotentHint`) is unchanged and answers a
different question: whether a call a dead worker already dispatched may be
run again on re-entry. Parallel eligibility and repeat safety are independent
properties of the same annotation fields.

*REQ-4* Task operations are never parallel. `start_task`, `cancel_task` and
`provide_task_input` are waves of one regardless of annotations, positioned
in the stream by emission order like any other non-read call.

Emission order
--------------------
*REQ-5* Emission order is the order of the `TOOL_CALL` events in the batch
`recordResponse` writes. Both persistors number a batch atomically in
in-batch order (Redis: one append script returning `start`, then
`Seq = start + i`; filestore: `seq++` per event), so the `TOOL_CALL` seqs are
the requested positions. `applyToolCall` keeps that position on the folded
`ToolCall.Seq`, and `toolStep` reads it by walking responses and their calls.

*REQ-6* Dispatch and result events are keyed by `CausationID` (the
`TOOL_CALL` event's ID), never by position. Under parallel execution their
log order is arrival order, which is nondeterministic, and nothing reads
order from them. Nothing needs to.

*REQ-7* The model always sees results in emission order. `projectResponse`
walks `message.ToolCalls` in fold order — which is emission order by REQ-5 —
and emits one tool result per call in that loop, so settlement order in the
log never reaches the wire.

Execution
--------------------
*REQ-8* A wave of one runs today's path unchanged: record the dispatch, invoke,
settle.

*REQ-9* A parallel wave writes all of its dispatch events in one
`persistAndFold` batch, in emission order — the same trick `recordResponse`
uses, so the log's dispatch ordering also reflects emission order and a
worker that dies mid-wave leaves every dispatch recorded. Then the calls run
concurrently, and each settles itself when its handler returns. The step
waits for the whole wave to settle before starting the next.

*REQ-10* One call's failure settles only that call, as an `IsError` result;
its siblings run to completion. The first error is returned only after the
wave has fully settled. `toolStep`'s contract — every pending call it finds
ends settled, because the loop re-reads the arc to decide what to do next —
is unchanged.

*REQ-11* Cancellation is unchanged. A handler that returned has said what
happened, so its outcome is recorded even under a cancelled context; each
goroutine in a wave does the same and the step still waits for all of them
before returning `ctx.Err()`.

Crash and re-entry
--------------------
Dispatch is written before the tool runs, so a worker dying mid-wave leaves
dispatches with no results behind them. On re-entry `toolStep` finds those
calls dispatched: read-only ones are `SafeToRepeat` and run again — as a wave
with their siblings — and everything else is abandoned with the existing
"may have taken effect" message. `Dispatches` already counts re-handovers.
No new states, no new events.

What does not change
--------------------
The fold, the projection, the provider wire format, compaction, and the
transcript the model sees: calls in emission order, results in emission
order. Only the wall-clock schedule of the calls themselves changes.
