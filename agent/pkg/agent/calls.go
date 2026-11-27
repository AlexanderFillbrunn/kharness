package agent

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"fillbrunn.ai/kube-agent/proto/pkg/events"
	"fillbrunn.ai/kube-agent/proto/pkg/history"
)

// toolStep runs every call the arc is still waiting on. Every pending call it
// finds has to end settled, since the loop reads the arc to decide what to do
// next.
//
// The calls run in waves, per docs/PARALLEL-TOOLS.md: a contiguous run of
// read-only calls runs concurrently (chunked to the configured cap), and
// every other call runs alone. Waves run in emission order — except interrupt
// calls, which run last: no call in one message can depend on the user's
// answer, since the model emitted the batch before the answer existed, so the
// ordinary calls run first and their results are in the log already when the
// arc is held. A call that should wait for the answer belongs in a later
// message, after the resumed run has seen it.
func (a Agent) toolStep(ctx context.Context, state *agentState) error {
	// Collected before any of them runs, because running one writes to the
	// arc being read.
	var pending []*history.ToolCall
	for _, response := range state.arc.Responses {
		for _, call := range response.ToolCalls {
			if call.Status == history.ToolCallStatusPending {
				pending = append(pending, call)
			}
		}
	}

	var toRun []*history.ToolCall
	for _, call := range pending {
		// A call another run already dispatched may have taken effect. Only a
		// tool declared safe to repeat gets a second attempt.
		tool, ok := a.tools.Get(call.ToolName)
		if call.Phase() == history.ToolCallPhaseDispatched && !(ok && SafeToRepeat(tool)) {
			if err := a.abandonToolCall(ctx, call, state); err != nil {
				return err
			}
			continue
		}
		toRun = append(toRun, call)
	}

	// The decision pass: every pending call a Before* hook governs is decided
	// on before any call of the step runs, so a step's blocks and answers
	// settle in one write and its asks are raised together.
	decisions := a.decideCalls(ctx, toRun, state)
	settles, approvals, ordinary, asks := decisions.partition(toRun, state.arc.ID)
	if len(settles) > 0 {
		if _, err := a.persistAndFold(ctx, state, settles...); err != nil {
			return err
		}
	}

	for _, wave := range a.waves(ordinary, decisions) {
		var err error
		if len(wave) == 1 {
			err = a.runCall(ctx, wave[0], execArgs(wave[0], decisions), state)
		} else {
			err = a.runWave(ctx, wave, decisions, state)
		}
		if err != nil {
			return err
		}
		// The arc may have been cancelled or otherwise settled mid-step.
		if state.arc.Status() != history.ArcStatusPending {
			return nil
		}
	}
	// Approvals are raised together, in one write, after the ordinary calls
	// have run and their results are in the log — the same ordering the
	// interrupt calls below follow.
	if err := a.raiseApprovals(ctx, approvals, state); err != nil {
		return err
	}
	if state.arc.Status() != history.ArcStatusPending {
		return nil
	}
	// An ask runs alone and holds the arc once raised, so a second ask in the
	// same message stays pending for the run after the first is answered, and
	// the run ends at its next step.
	for _, call := range asks {
		if err := a.runCall(ctx, call, execArgs(call, decisions), state); err != nil {
			return err
		}
		if state.arc.Status() != history.ArcStatusPending {
			return nil
		}
	}
	return nil
}

// waves partitions calls, in emission order, into waves: a contiguous run of
// calls that may run in parallel becomes one wave, chunked to at most the
// configured cap, and every other call is a wave of its own. A call in solo
// — one a Before* hook marked sequential — never joins a wave.
func (a Agent) waves(calls []*history.ToolCall, decisions callDecisions) [][]*history.ToolCall {
	limit := a.config.parallelCap()
	joinable := func(call *history.ToolCall) bool {
		return a.parallelizable(call) && !decisions[call].solo
	}
	var waves [][]*history.ToolCall
	for i := 0; i < len(calls); {
		if !joinable(calls[i]) {
			waves = append(waves, calls[i:i+1])
			i++
			continue
		}
		j := i
		for j < len(calls) && joinable(calls[j]) && (limit < 0 || j-i < limit) {
			j++
		}
		waves = append(waves, calls[i:j])
		i = j
	}
	return waves
}

// parallelizable reports whether a call may join a parallel wave: an ordinary
// tool declared read-only. Task operations and interrupts never do, whatever
// the tool the name resolves to says.
func (a Agent) parallelizable(call *history.ToolCall) bool {
	if a.operation(call.ToolName) != opTool {
		return false
	}
	tool, ok := a.tools.Get(call.ToolName)
	return ok && ReadOnly(tool)
}

// runWave runs one wave of read-only calls concurrently. Every dispatch is
// written in one batch, in emission order, before any of them runs — the same
// trick recordResponse uses — so a worker that dies mid-wave leaves every
// dispatch recorded. Each call settles itself when its handler returns, and
// the wave is done when all of them have; one call's failure settles only
// that call and does not stop its siblings.
func (a Agent) runWave(ctx context.Context, wave []*history.ToolCall, decisions callDecisions, state *agentState) error {
	dispatches := make([]*events.Event, 0, len(wave))
	for _, call := range wave {
		dispatches = append(dispatches, &events.Event{
			Typ:           events.ToolDispatchedEventType,
			CorrelationID: &state.arc.ID,
			Payload:       events.ToolDispatchedPayload{CausationID: call.ID},
		})
	}
	if _, err := a.persistAndFold(ctx, state, dispatches...); err != nil {
		return err
	}
	state.logger.DebugContext(ctx, "running a wave of calls", "calls", len(wave))

	errs := make([]error, len(wave))
	var wg sync.WaitGroup
	for i, call := range wave {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = a.invokeAndSettle(ctx, call, execArgs(call, decisions), state)
		}()
	}
	wg.Wait()
	return errors.Join(errs...)
}

// runCall hands one call to whatever answers calls by that name, running it
// with the args the decision pass resolved for it. The name decides what the
// call is before anything runs.
func (a Agent) runCall(ctx context.Context, call *history.ToolCall, args any, state *agentState) error {
	switch a.operation(call.ToolName) {
	case opStart:
		return a.legacyStartTask(ctx, call, args, state)
	case opCancel:
		return a.cancelTask(ctx, call, state)
	case opProvideInput:
		return a.provideTaskInput(ctx, call, args, state)
	case opInterrupt:
		return a.raiseInterrupt(ctx, call, state)
	default:
		return a.runToolCall(ctx, call, args, state)
	}
}

// abandonToolCall settles a call that an earlier run dispatched and never
// reported on, without running it again. It is recorded as a failure, and the
// text says that the tool may have done the work anyway.
func (a Agent) abandonToolCall(ctx context.Context, call *history.ToolCall, state *agentState) error {
	state.logger.WarnContext(ctx, "abandoning a dispatched call that was never reported on", "tool", call.ToolName, "call", call.ID)
	return a.settleCall(ctx, call, ToolOutcome{
		IsError: true,
		Result: fmt.Sprintf(
			"this call was handed to %s by a run that stopped before reporting. "+
				"Whether it took effect is not known, and it was not tried again "+
				"because %s is not declared safe to repeat. Check the current state "+
				"before deciding what to do.", call.ToolName, call.ToolName),
	}, state)
}

// runToolCall records that a call was handed to its tool, runs it, and records
// what came back. The dispatch is written first, so a worker that dies
// mid-call leaves that record with no result behind it.
func (a Agent) runToolCall(ctx context.Context, call *history.ToolCall, args any, state *agentState) error {
	if err := a.recordDispatch(ctx, call, state); err != nil {
		return err
	}
	return a.invokeAndSettle(ctx, call, args, state)
}

// invokeAndSettle runs a call whose dispatch is already recorded and records
// what came back. A wave calls this directly, after writing its dispatches in
// one batch.
func (a Agent) invokeAndSettle(ctx context.Context, call *history.ToolCall, args any, state *agentState) error {
	// The call runs with the args the decision pass resolved for it — the
	// ones a Before* hook overrode, where it did; the log keeps the ones the
	// model asked for.

	// A handler that returned has said what happened, whatever the context is
	// doing by then, so the outcome is recorded even for a cancelled run.
	state.logger.DebugContext(ctx, "invoking tool", "tool", call.ToolName, "call", call.ID)
	outcome, err := a.tools.Invoke(ctx, call.ToolName, args)
	if err != nil {
		outcome = ToolOutcome{Result: err.Error(), IsError: true}
	}
	if outcome.IsError {
		// The result text is the tool's own error and lands in the log; the
		// log line says the call failed, not what it said.
		state.logger.WarnContext(ctx, "tool call settled as an error", "tool", call.ToolName, "call", call.ID)
	}

	outcome, notes, herr := applyAfterToolCall(ctx, call, args, outcome, state)
	if herr != nil {
		return herr
	}
	batch := append([]*events.Event{callResult(call, outcome, state.arc.ID)}, notes...)

	if _, err := a.persistAndFold(ctx, state, batch...); err != nil {
		return err
	}

	// The call is settled either way. The run is over only if someone stopped
	// it, and both can be true at once.
	return ctx.Err()
}

// recordDispatch says that a call was handed to whatever answers it, before
// the work starts.
func (a Agent) recordDispatch(ctx context.Context, call *history.ToolCall, state *agentState) error {
	_, err := a.persistAndFold(ctx, state, &events.Event{
		Typ:           events.ToolDispatchedEventType,
		CorrelationID: &state.arc.ID,
		Payload:       events.ToolDispatchedPayload{CausationID: call.ID},
	})
	return err
}

// settleCall answers one call with what came of it.
func (a Agent) settleCall(ctx context.Context, call *history.ToolCall, outcome ToolOutcome, state *agentState) error {
	_, err := a.persistAndFold(ctx, state, callResult(call, outcome, state.arc.ID))
	return err
}

// callResult is the record of a call having been answered, for a caller that
// writes it alongside something else.
func callResult(call *history.ToolCall, outcome ToolOutcome, arcID string) *events.Event {
	return &events.Event{
		Typ:           events.ToolResultEventType,
		CorrelationID: &arcID,
		Payload: events.ToolResultPayload{
			CausationID: call.ID,
			Result:      outcome.Result,
			IsError:     outcome.IsError,
		},
	}
}

// stringArg reads one string argument out of a call's arguments, which the log
// holds as decoded JSON.
func stringArg(args any, key string) (string, bool) {
	fields, ok := args.(map[string]any)
	if !ok {
		return "", false
	}
	value, ok := fields[key].(string)
	if !ok || value == "" {
		return "", false
	}
	return value, true
}
