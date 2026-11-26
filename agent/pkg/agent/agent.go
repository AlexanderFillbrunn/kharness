package agent

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"

	"fillbrunn.ai/kube-agent/proto/pkg/common"
	"fillbrunn.ai/kube-agent/proto/pkg/events"
	"fillbrunn.ai/kube-agent/proto/pkg/history"
)

type Token struct {
	MessageID string
	Content   string
	Seq       int
}

type Agent struct {
	config *AgentConfig
	tools  ToolProvider
	llm    LLM

	// systemPrompt is the config's prompt with the skills catalog appended,
	// built once at NewAgent rather than per run. See WithSkills.
	systemPrompt string

	// skills is the set the load_skill tool answers from. Nil for an agent
	// configured with no skills, in which case the tool is not offered.
	skills SkillSet

	// tasks is the set of task kinds the agent's definition names: what the
	// tool surface is built from. Runs never start tasks through it — a
	// run's controller is RunOptions.Controller, which the worker binds to
	// the work item's session. Nil for an agent that offers no tasks.
	tasks common.TaskSet

	// interruptSpecs are the configured interrupts, in declaration order.
	// Nil and empty for an agent with no interrupts configured, in which
	// case no tool raises one.
	interruptSpecs []InterruptSpec

	// hooks are the agent's extension points, consulted at the points the
	// Hooks struct names. The zero value has no hooks and changes nothing.
	hooks Hooks
}

// RunOptions are the parts of a run that differ from one run to the next. The
// agent is built once and holds none of them, so one agent serves as many
// sessions as it is handed.
type RunOptions struct {
	// Persistor is where the run writes.
	Persistor common.EventPersistor

	// History is the arc's log, already folded. The session it names is what
	// the run's own events are filed under.
	History *history.History

	// ArcID names the arc in that history the run is to work.
	ArcID string

	// Modes is how much of the log the run's projection of that arc
	// discloses. The zero value becomes history.DefaultModes.
	Modes history.Modes

	// Tokens follows the model's text as it streams, labelled with the message
	// that will record it. It is nil for a run nobody is watching, which is
	// every run of a worker with no interface attached, and a watcher's error
	// is dropped either way.
	Tokens common.Sink[Token]

	// OnCompactionDue is called after a round writes a new
	// COMPACTION_REQUESTED: the run reports the want, and whoever is
	// listening starts the compaction outside the run, in parallel. It is
	// fire-and-forget — the request in the log is the durable trigger, so a
	// notification that fails or is never made loses nothing — and nil leaves
	// the request standing in the log for anyone to take. The agent knows
	// nothing about queues or compactors, only that compaction is due; the
	// session is the listener's to supply, since the run never sees it.
	OnCompactionDue func(ctx context.Context, requestID string)

	// Controller starts the tasks a run starts and reaches the ones already
	// running; the run addresses a task by ID alone and never sees a session
	// id. A worker hands the run the agent's task controller bound to the work
	// item's session (common.InSession). Nil means the run has no controller:
	// task tool calls are settled as errors.
	Controller common.TaskController
}

type agentState struct {
	history   *history.History
	arc       *history.Arc
	modes     history.Modes
	foldState *history.FoldState
	persistor common.EventPersistor
	tokens    common.Sink[Token]

	// tasks is the controller this run starts and reaches tasks through:
	// RunOptions.Controller, stamped by the worker. Nil where the run has
	// none, in which case task tool calls are settled as errors.
	tasks common.TaskController

	// writeMu serializes persistAndFold. A wave settles its calls from
	// several goroutines at once, and the fold demands events in seq order,
	// so the persist and the fold have to be one critical section: two
	// goroutines could otherwise persist out of order and fold the later
	// one first.
	writeMu sync.Mutex

	// onCompactionDue is who the run tells when a round writes a new
	// compaction request. Nil means nobody is told.
	onCompactionDue func(ctx context.Context, requestID string)

	// hooks are the hooks this run consults: the agent's.
	hooks Hooks

	// rounds counts the steps the run took; written counts the events it
	// persisted. Both feed the AfterRun summary.
	rounds  int
	written int

	// outcome is how the run ended, set by the admission check, the step
	// loop and the failure path as they go, and read by the AfterRun hook.
	outcome RunOutcome
}

// Run invokes the agent to complete an arc, i.e. a user message triggering the
// agent.
func (a Agent) Run(ctx context.Context, run RunOptions) (err error) {
	if run.Persistor == nil {
		return errors.New("a run needs an event persistor")
	}
	if run.History == nil {
		return errors.New("a run needs a history")
	}

	var arc *history.Arc
	for _, candidate := range slices.Backward(run.History.Arcs) {
		if candidate.ID == run.ArcID {
			arc = candidate
			break
		}
	}
	if arc == nil {
		return fmt.Errorf("arc not found in history: %s", run.ArcID)
	}

	modes := run.Modes
	if modes == (history.Modes{}) {
		modes = history.DefaultModes()
	}
	hooks := a.hooks
	state := &agentState{
		history:         run.History,
		arc:             arc,
		modes:           modes,
		foldState:       history.FoldStateFromHistory(run.History),
		persistor:       run.Persistor,
		tokens:          run.Tokens,
		tasks:           run.Controller,
		onCompactionDue: run.OnCompactionDue,
		hooks:           hooks,
		outcome:         RunFinished,
	}

	defer func() { fireAfterRun(ctx, state, run.ArcID, err) }()
	// fail records how the run ended and closes the arc the agent could not
	// finish.
	fail := func(cause error) error {
		if errors.Is(cause, context.Canceled) || errors.Is(cause, common.ErrCancelled) {
			state.outcome = RunCancelled
		} else {
			state.outcome = RunFailed
		}
		return a.recordFailure(ctx, cause, state)
	}

	proceed, err := a.admit(ctx, state)
	if err != nil {
		return fail(err)
	}
	if !proceed {
		return nil
	}

	if err := a.steps(ctx, state); err != nil {
		return fail(err)
	}
	if state.arc.Status() == history.ArcStatusInterrupted {
		state.outcome = RunInterrupted
	}
	return nil
}

// admit decides whether the run may start: the BeforeRun hook first, then the
// built-in checks as the floor. It reports whether the run may proceed. A
// refusal or a suspension ends the run — state.outcome says which — and is
// not an error; anything else returned is.
func (a Agent) admit(ctx context.Context, state *agentState) (bool, error) {
	// The BeforeRun hook runs first; its refusal ends the run before the
	// built-in checks.
	ok, err := a.applyBeforeRun(ctx, state)
	if err != nil || !ok {
		return ok, err
	}

	// The built-in floor: an arc already waiting out a suspension is left to
	// its compaction, and one whose would-be transcript estimates over the
	// higher budget is suspended before it starts — the compaction request is
	// written where none stands, then an ARC_SUSPENDED naming it, and the arc
	// is left unworked for the completion scan.
	cfg := a.config
	if cfg.compactionEnabled() {
		arc := state.arc
		if state.history.ArcSuspended(arc) {
			state.outcome = RunSuspended
			return false, nil
		}
		if cfg.HigherBudget > 0 && state.history.EstimateAbove(state.history.Watermark()) > cfg.HigherBudget {
			requestID, err := a.ensureCompactionRequest(ctx, state)
			if err != nil {
				return false, err
			}
			if err := a.suspendArc(ctx, state, requestID); err != nil {
				return false, err
			}
			state.outcome = RunSuspended
			return false, nil
		}
	}
	return true, nil
}

// recordFailure closes an arc the agent could not finish, and returns the
// error that ended it. An unrecorded failure leaves an arc the log cannot tell
// from one still being worked on.
//
// A cancelled run writes nothing, because whoever decided the cancel records
// it and the agent does not know that actor. A suspended or refused run
// likewise writes nothing: the ARC_SUSPENDED or the fatal Error the admission
// check wrote is the record, and the run ends its in-flight period with the
// queue item acknowledged.
func (a Agent) recordFailure(ctx context.Context, cause error, state *agentState) error {
	// A cancel reaches a run through its context or through a refused write,
	// and neither is a failure to record.
	if errors.Is(cause, context.Canceled) || errors.Is(cause, common.ErrCancelled) {
		return cause
	}
	if errors.Is(cause, errSuspended) {
		return nil
	}

	failure := &events.Event{
		Typ:           events.ErrorEventType,
		CorrelationID: &state.arc.ID,
		Payload:       events.ErrorPayload{Message: cause.Error(), IsFatal: true},
	}

	if _, err := a.persistAndFold(ctx, state, failure); err != nil {
		// Neither the cause nor the failure to record it is worth losing.
		return errors.Join(cause, err)
	}
	return cause
}

// steps runs the agent loop until it is the user's turn again. After every
// round the compaction checks run: the lower budget writes the request and
// notifies the run's listener, the higher suspends the run.
func (a Agent) steps(ctx context.Context, state *agentState) error {
	done := false
	for !done {
		d, err := a.step(ctx, state)
		if err != nil {
			return err
		}
		done = d
		state.rounds++
		if err := a.checkCompaction(ctx, state, done); err != nil {
			return err
		}
	}

	return nil
}

// Step performs one step in the agent loop.
func (a Agent) step(ctx context.Context, state *agentState) (bool, error) {
	s, err := a.deriveArcState(state.arc)
	if err != nil {
		return true, err
	}
	switch s {
	case ArcStateLLM:
		return false, a.llmStep(ctx, state)
	case ArcStateToolCalls:
		return false, a.toolStep(ctx, state)
	case ArcStateClose:
		return false, a.closeArc(ctx, state)
	case ArcStateNone:
		return true, ErrInvalidArcState
	}
	return true, nil
}

func (a Agent) closeArc(ctx context.Context, state *agentState) error {
	// The fold rejects a close that names no message it knows.
	if len(state.arc.Responses) == 0 {
		return fmt.Errorf("cannot close arc %s: no response to name as its conclusion", state.arc.ID)
	}
	last := state.arc.Responses[len(state.arc.Responses)-1]

	if _, err := a.persistAndFold(ctx, state, &events.Event{
		Typ:           events.ArcClosedEventType,
		CorrelationID: &state.arc.ID,
		Payload:       events.ArcClosedPayload{CausationID: last.ID},
	}); err != nil {
		return err
	}
	fireAfterArcClosed(ctx, state)
	return nil
}

// persistAndFold writes one event and folds it into the history, returning the
// events as they were persisted. The persistor assigns the sequence number,
// and the ID and timestamp where the caller left them empty, so a caller
// naming one of these events later reads it from what comes back.
//
// Events passed together are written together or not at all, which is what
// keeps two events that say one thing from becoming one event that says
// something else.
func (a Agent) persistAndFold(ctx context.Context, state *agentState, evs ...*events.Event) ([]*events.Event, error) {
	// A write that starts work honours the cancel, because not starting it is
	// the point. Every other write records something that already happened,
	// and losing one leaves the log saying less than the agent did. One event
	// that starts work governs its whole batch, as it does for the guard.
	if _, starts := common.GuardedArc(evs); !starts {
		ctx = context.WithoutCancel(ctx)
	}

	state.writeMu.Lock()
	defer state.writeMu.Unlock()
	stored, err := state.persistor.Persist(ctx, evs...)
	if err != nil {
		return nil, fmt.Errorf("error persisting %s: %w", eventTypes(evs), err)
	}
	state.written += len(stored)
	for _, ev := range stored {
		if err := state.history.Update(ev, state.foldState); err != nil {
			return nil, fmt.Errorf("error folding %s event: %w", ev.Typ, err)
		}
	}
	return stored, nil
}

// eventTypes names a batch for an error message.
func eventTypes(evs []*events.Event) string {
	names := make([]string, 0, len(evs))
	for _, ev := range evs {
		names = append(names, string(ev.Typ))
	}
	return strings.Join(names, ", ")
}
