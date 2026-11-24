package agent

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"

	"fillbrunn.ai/kube-agent/proto/pkg/common"

	"fillbrunn.ai/kube-agent/proto/pkg/history"
	"fillbrunn.ai/kube-agent/proto/pkg/events"
	"fillbrunn.ai/kube-agent/proto/pkg/users"
)

var (
	ErrInvalidArcState = errors.New("invalid arc state")
)

type arcState int

const (
	ArcStateNone      arcState = iota
	ArcStateLLM       arcState = iota
	ArcStateToolCalls arcState = iota
	ArcStateDone      arcState = iota
	ArcStateClose     arcState = iota
)

type Token struct {
	MessageID string
	Content   string
	Seq       int
}

// AgentConfig is what an agent is built with, and holds nothing that differs
// from one run to the next. Following a run is not here: notification hangs
// off the persistor, so an agent has no idea whether anybody is watching. See
// common.EventSink and docs/EVENT-SINK.md.
type AgentConfig struct {
	SystemPrompt string

	// TaskInResultTrail is the global override of how many InResult events a
	// task's result message carries, -1 for the whole trail. Nil leaves each
	// kind's own declaration standing.
	TaskInResultTrail *int

	// Compaction says whether the compaction checks run at all: no budget is
	// checked and no request is written when it is false. Which compactor
	// runs is not here — it is a property of the compaction worker's plan,
	// resolved from the settings, not of the agent.
	Compaction bool

	// LowerBudget is the token count that triggers a compaction request,
	// checked after every round. It sits well below what the provider accepts.
	LowerBudget int

	// HigherBudget is the token count that suspends runs, checked at admission
	// and between rounds. It sits just below the context window of the run's
	// model, leaving a reserve for one more tool result. Zero means the
	// default resolved at build: ContextWindow minus ContextWindowReserve.
	HigherBudget int

	// MaxToolResultChars caps what one tool result may contribute to a
	// transcript, in characters. The log keeps the whole result. Negative
	// disables the cap.
	MaxToolResultChars int

	// MaxUserMessageChars is the most a user message may carry before the
	// admission check rejects it outright. User messages are never thinned.
	// Negative disables the check.
	MaxUserMessageChars int

	// ArcThresholdTokens is the size an arc's log may reach before the bound
	// rule draws the line at its live boundary rather than its opening, and
	// before a spanning arc renders as a skeleton. Negative disables both.
	ArcThresholdTokens int

	// ContextWindow is the context window of the run's model, resolved from
	// the catalog at build.
	ContextWindow int

	// MaxParallelToolCalls caps how many read-only calls one wave of a tool
	// step runs at once. Zero means DefaultMaxParallelToolCalls, negative
	// means no cap, and one is fully sequential. See docs/PARALLEL-TOOLS.md.
	MaxParallelToolCalls int
}

// DefaultMaxParallelToolCalls is how many read-only calls run at once when
// the config says nothing.
const DefaultMaxParallelToolCalls = 4

// parallelCap resolves MaxParallelToolCalls: the configured value where it is
// positive, the default for zero, and -1 (no cap) for negative.
func (c *AgentConfig) parallelCap() int {
	switch {
	case c.MaxParallelToolCalls < 0:
		return -1
	case c.MaxParallelToolCalls == 0:
		return DefaultMaxParallelToolCalls
	default:
		return c.MaxParallelToolCalls
	}
}

// inResultTrail resolves how many InResult events one task kind's result
// message carries: the config's override where one is set, the kind's
// declaration otherwise.
func (c *AgentConfig) inResultTrail(meta common.TaskMetadata) int {
	if c.TaskInResultTrail != nil {
		return *c.TaskInResultTrail
	}
	return meta.InResultTrail
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

	// tasks is the one source of async work: the kinds that may be started and
	// the controller that starts them and reaches them once running. Nil for
	// an agent that runs no tasks.
	tasks TaskSource
}

// AgentOption configures parts of an agent that not every agent has.
type AgentOption func(*Agent) error

// WithTasks gives an agent the tasks its definition names, as one source: the
// kinds it may start and the controller that reaches them once running. A nil
// source is refused: it would start tasks nobody could stop or answer.
func WithTasks(tasks TaskSource) AgentOption {
	return func(a *Agent) error {
		if tasks == nil {
			return errors.New("tasks need a source that can start them and reach them once running")
		}
		a.tasks = tasks
		return nil
	}
}

// WithSkills gives an agent the skills its definition names. Both model-facing
// halves are derived here, once: the catalog appended to the system prompt,
// and the load_skill tool, which NewAgent folds into the tool set. A nil or
// empty set means no skills and changes nothing.
func WithSkills(skills SkillSet) AgentOption {
	return func(a *Agent) error {
		if skills == nil || len(skills.All()) == 0 {
			return nil
		}
		a.skills = skills
		a.systemPrompt = skillsCatalog(a.config.SystemPrompt, skills)
		return nil
	}
}

// NewAgent builds an agent from the parts it runs on. A nil config or tool
// provider means no system prompt and an empty registry. A model is required.
func NewAgent(config *AgentConfig, tools ToolProvider, llm LLM, options ...AgentOption) (*Agent, error) {
	if llm == nil {
		return nil, errors.New("an agent needs a model")
	}
	if config == nil {
		config = &AgentConfig{}
	}
	if tools == nil {
		tools = NewToolRegistry()
	}

	agent := &Agent{config: config, tools: tools, llm: llm, systemPrompt: config.SystemPrompt}
	for _, option := range options {
		if err := option(agent); err != nil {
			return nil, err
		}
	}

	// The skills' load_skill tool is one more tool by name, so a tool
	// registered under that name collides with it the way two tools with one
	// name do.
	if agent.skills != nil {
		if _, taken := tools.Get(LoadSkillToolName); taken {
			return nil, fmt.Errorf("%s is registered as a tool and is the tool skills are loaded with", LoadSkillToolName)
		}
		agent.tools = combineProviders(tools, loadSkillProvider{skills: agent.skills})
	}

	// The model picks by name and the agent routes by name, so one name
	// registered twice is a call that reaches whichever is checked first.
	if agent.tasks != nil {
		for _, meta := range agent.tasks.All() {
			if _, taken := agent.tools.Get(meta.Name); taken {
				return nil, fmt.Errorf("%s is registered as both a tool and a task", meta.Name)
			}
		}
		for _, name := range []string{events.CancelTaskToolName, events.ProvideTaskInputToolName} {
			if _, taken := agent.tools.Get(name); taken {
				return nil, fmt.Errorf("%s is registered as a tool and is one of the tools tasks are addressed with", name)
			}
		}
	}
	return agent, nil
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

	// Tasks reaches the tasks a run starts and the ones already running. Nil
	// falls back to the agent's own task source. A worker hands the run the
	// controller bound to the work item's session (common.InSession), so the
	// run addresses a task by ID alone and never sees a session id.
	Tasks common.TaskController
}

type agentState struct {
	history   *history.History
	arc       *history.Arc
	modes     history.Modes
	foldState *history.FoldState
	persistor common.EventPersistor
	tokens    common.Sink[Token]

	// tasks is the controller this run reaches running tasks through: the
	// run's own where RunOptions named one, else the agent's source.
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
}

// Run invokes the agent to complete an arc, i.e. a user message triggering the
// agent.
func (a Agent) Run(ctx context.Context, run RunOptions) error {
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
	tasks := run.Tasks
	if tasks == nil {
		tasks = a.tasks
	}
	state := &agentState{
		history:         run.History,
		arc:             arc,
		modes:           modes,
		foldState:       history.FoldStateFromHistory(run.History),
		persistor:       run.Persistor,
		tokens:          run.Tokens,
		tasks:           tasks,
		onCompactionDue: run.OnCompactionDue,
	}

	// The result message of an arc this run opened by a task's result carries
	// the task kind's InResult trail, under the config's override where one
	// is set.
	a.resolveTaskResultTrail(run.History, arc)

	// The admission check runs before the first step: an over-limit user
	// message is refused, an over-budget transcript suspends the arc.
	if err := a.admissionCheck(ctx, state); err != nil {
		if errors.Is(err, errSuspended) || errors.Is(err, errRefused) {
			return nil
		}
		return a.recordFailure(ctx, err, state)
	}

	if err := a.steps(ctx, state); err != nil {
		return a.recordFailure(ctx, err, state)
	}
	return nil
}

// resolveTaskResultTrail sets how many InResult task events the result message
// of the arc being run carries, when that arc was opened by a task's result.
func (a Agent) resolveTaskResultTrail(h *history.History, arc *history.Arc) {
	if a.config.TaskInResultTrail != nil {
		h.TaskResultTrail = *a.config.TaskInResultTrail
		return
	}
	if arc.OpenedByTask == "" {
		return
	}
	task, ok := h.Tasks[arc.OpenedByTask]
	if !ok || task.ResultArc != arc.ID {
		return
	}
	if a.tasks == nil {
		return
	}
	if meta, ok := a.tasks.Get(task.TaskName); ok {
		h.TaskResultTrail = a.config.inResultTrail(meta)
	}
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
	if errors.Is(cause, errSuspended) || errors.Is(cause, errRefused) {
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

	_, err := a.persistAndFold(ctx, state, &events.Event{
		Typ:           events.ArcClosedEventType,
		CorrelationID: &state.arc.ID,
		Payload:       events.ArcClosedPayload{CausationID: last.ID},
	})
	return err
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

// llmStep projects the arc so far and calls the LLM with the transcript, then
// records what it replied.
func (a Agent) llmStep(ctx context.Context, state *agentState) error {
	// The message is named before the model is called rather than when the
	// reply is written, because the text being streamed has to say which
	// message will record it.
	messageID := common.NewEventID()

	// The projection happens here rather than inside the model client: it is
	// a pure function of the log, and the LLM only encodes what it is handed.
	// The arc being worked is the projection's target.
	transcript, err := state.history.Project(state.arc.ID, state.modes, a.config.projectionOptions())
	if err != nil {
		return fmt.Errorf("project arc %s: %w", state.arc.ID, err)
	}
	response, err := a.llm.StreamResponse(ctx, transcript, &LLMOptions{
		SystemPrompt: a.systemPrompt,
		Tools:        CombineTools(a.tools, taskToolSet{a.tasks}),
	}, state.tokenSink(messageID))
	// A rejection the provider recognises as over budget is recovered, not
	// recorded as a failure: the non-fatal Error, the request and the
	// suspension say what happened. A partial reply that came with it — a
	// length stop — is recorded first, so the log holds what the model said.
	var overflow *ContextOverflowError
	if errors.As(err, &overflow) {
		if response != nil {
			if rerr := a.recordResponse(ctx, response, messageID, state); rerr != nil {
				return rerr
			}
		}
		return a.recoverOverflow(ctx, state, overflow.Error())
	}
	if err != nil {
		return fmt.Errorf("error calling LLM: %w", err)
	}
	if err := a.recordResponse(ctx, response, messageID, state); err != nil {
		return err
	}
	return nil
}

// tokenSink labels the model's text with the message that will record it. A
// run nobody is watching discards it, and the watcher's error is dropped
// rather than returned: a screen cannot fail a run.
//
// The provider is handed a plain common.Sink[string], so no LLM implementation learns
// what an arc or a message is.
func (s *agentState) tokenSink(messageID string) common.Sink[RawToken] {
	return common.SinkFromFunc(func(tok RawToken) error {
		if s.tokens != nil {
			_ = s.tokens.Consume(Token{MessageID: messageID, Content: tok.Content, Seq: tok.Seq})
		}
		return nil
	})
}

// recordResponse writes a reply to the log: the message and one event per call
// it requested, in one write.
//
// They go together because a log holding the message alone reads as an answer
// the model never gave, and the next run closes the arc on it. The message ID
// is the one llmStep minted before the call rather than one read back, since
// the calls have to name it before any of them is written and the tokens had
// to name it before that.
func (a Agent) recordResponse(ctx context.Context, response *LLMResponse, messageID string, state *agentState) error {
	batch := make([]*events.Event, 0, len(response.ToolCalls)+1)
	batch = append(batch, &events.Event{
		ID:            messageID,
		Typ:           events.AssistantMessageEventType,
		CorrelationID: &state.arc.ID,
		Payload: events.AssistantMessagePayload{
			VendorID:  response.VendorID,
			ModelID:   response.ModelID,
			Content:   response.Content,
			Reasoning: response.Reasoning,
			Usage:     recordedUsage(response.Usage),
		},
	})
	for _, call := range response.ToolCalls {
		batch = append(batch, &events.Event{
			Typ:           events.ToolCallEventType,
			CorrelationID: &state.arc.ID,
			Payload: events.ToolCallPayload{
				MessageID: messageID,
				VendorID:  call.VendorID,
				ToolName:  call.ToolName,
				Args:      call.Args,
			},
		})
	}

	_, err := a.persistAndFold(ctx, state, batch...)
	return err
}

// recordedUsage carries a reply's cost into the record of it. Nothing is
// written where the provider reported nothing, so a zero in the log is a
// measured zero.
func recordedUsage(usage *TokenUsage) *events.TokenUsage {
	if usage == nil {
		return nil
	}
	return &events.TokenUsage{
		Input:       usage.Input,
		CachedInput: usage.CachedInput,
		Output:      usage.Output,
	}
}

// toolStep runs every call the arc is still waiting on, in the order they were
// requested. Every pending call it finds has to end settled, since the loop
// reads the arc to decide what to do next.
//
// The calls run in waves, per docs/PARALLEL-TOOLS.md: a contiguous run of
// read-only calls runs concurrently (chunked to the configured cap), and
// every other call runs alone. Waves run in emission order.
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

	for _, wave := range a.waves(toRun) {
		var err error
		if len(wave) == 1 {
			err = a.runCall(ctx, wave[0], state)
		} else {
			err = a.runWave(ctx, wave, state)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// waves partitions calls, in emission order, into waves: a contiguous run of
// calls that may run in parallel becomes one wave, chunked to at most the
// configured cap, and every other call is a wave of its own.
func (a Agent) waves(calls []*history.ToolCall) [][]*history.ToolCall {
	limit := a.config.parallelCap()
	var waves [][]*history.ToolCall
	for i := 0; i < len(calls); {
		if !a.parallelizable(calls[i]) {
			waves = append(waves, calls[i:i+1])
			i++
			continue
		}
		j := i
		for j < len(calls) && a.parallelizable(calls[j]) && (limit < 0 || j-i < limit) {
			j++
		}
		waves = append(waves, calls[i:j])
		i = j
	}
	return waves
}

// parallelizable reports whether a call may join a parallel wave: an ordinary
// tool declared read-only. Task operations never do, whatever the tool the
// name resolves to says.
func (a Agent) parallelizable(call *history.ToolCall) bool {
	if classify(a.tasks, call.ToolName) != opTool {
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
func (a Agent) runWave(ctx context.Context, wave []*history.ToolCall, state *agentState) error {
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

	errs := make([]error, len(wave))
	var wg sync.WaitGroup
	for i, call := range wave {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = a.invokeAndSettle(ctx, call, state)
		}()
	}
	wg.Wait()
	return errors.Join(errs...)
}

// runCall hands one call to whatever answers calls by that name. The name
// decides what the call is before anything runs.
func (a Agent) runCall(ctx context.Context, call *history.ToolCall, state *agentState) error {
	switch classify(a.tasks, call.ToolName) {
	case opStart:
		return a.startTask(ctx, call, state)
	case opCancel:
		return a.cancelTask(ctx, call, state)
	case opProvideInput:
		return a.provideTaskInput(ctx, call, state)
	default:
		return a.runToolCall(ctx, call, state)
	}
}

// abandonToolCall settles a call that an earlier run dispatched and never
// reported on, without running it again. It is recorded as a failure, and the
// text says that the tool may have done the work anyway.
func (a Agent) abandonToolCall(ctx context.Context, call *history.ToolCall, state *agentState) error {
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
func (a Agent) runToolCall(ctx context.Context, call *history.ToolCall, state *agentState) error {
	if err := a.recordDispatch(ctx, call, state); err != nil {
		return err
	}
	return a.invokeAndSettle(ctx, call, state)
}

// invokeAndSettle runs a call whose dispatch is already recorded and records
// what came back. A wave calls this directly, after writing its dispatches in
// one batch.
func (a Agent) invokeAndSettle(ctx context.Context, call *history.ToolCall, state *agentState) error {
	// A handler that returned has said what happened, whatever the context is
	// doing by then, so the outcome is recorded even for a cancelled run.
	outcome, err := a.tools.Invoke(ctx, call.ToolName, call.Args)
	if err != nil {
		outcome = ToolOutcome{Result: err.Error(), IsError: true}
	}

	if err := a.settleCall(ctx, call, outcome, state); err != nil {
		return err
	}

	// The call is settled either way. The run is over only if someone stopped
	// it, and both can be true at once.
	return ctx.Err()
}

// deriveArcState decides what the agent does next from the arc alone, so a run
// picking up an arc another run left behind reaches the same decision.
//
// An arc closes when the agent has nothing synchronous left to do: a response
// with no tool calls closes it, whatever tasks are unfinished. A running task
// does not hold its arc open — its later events open arcs of their own.
func (a Agent) deriveArcState(arc *history.Arc) (arcState, error) {
	if arc.Status() != history.ArcStatusPending {
		return ArcStateDone, nil
	}
	if len(arc.Responses) == 0 {
		return ArcStateLLM, nil
	}

	for _, r := range arc.Responses {
		for _, tc := range r.ToolCalls {
			if tc.Status != history.ToolCallStatusPending {
				continue
			}
			if !a.knows(tc.ToolName) {
				return ArcStateNone, fmt.Errorf("tool not registered: %s", tc.ToolName)
			}
			return ArcStateToolCalls, nil
		}
	}

	last := arc.Responses[len(arc.Responses)-1]
	if len(last.ToolCalls) == 0 {
		return ArcStateClose, nil
	}
	return ArcStateLLM, nil
}

// lastInputRequest is the question the task last stopped at, or nil for a task
// that never has. A waiting task always has one, since asking is what put it
// in that state.
func lastInputRequest(task *history.AsyncTask) *history.AsyncTaskEvent {
	var last *history.AsyncTaskEvent
	for i := range task.Events {
		if task.Events[i].Type == history.AsyncTaskEventInputRequested {
			last = &task.Events[i]
		}
	}
	return last
}

// knows reports whether a call by this name has somewhere to go: a registered
// tool, a kind of task, or one of the two tools tasks are addressed with.
func (a Agent) knows(name string) bool {
	if classify(a.tasks, name) != opTool {
		return true
	}
	_, ok := a.tools.Get(name)
	return ok
}

// startTask records a task, starts it, and answers the call that asked for it.
//
// The dispatch and the invocation are written before the task starts, in one
// write, because the record is what names the task. The invocation ID is
// minted here so the task can be started under it, and the invoke records the
// actor that started the task, which every event the task later emits carries.
func (a Agent) startTask(ctx context.Context, call *history.ToolCall, state *agentState) error {
	invocationID := common.NewEventID()
	invoke := &events.Event{
		ID:            invocationID,
		Typ:           events.AsyncTaskInvokeEventType,
		CorrelationID: &state.arc.ID,
		Payload: events.AsyncTaskInvokePayload{
			TaskName: call.ToolName,
			Args:     call.Args,
			Actor:    taskActor(state.arc),
		},
	}
	invoke.Actor = taskActor(state.arc)
	if _, err := a.persistAndFold(ctx, state,
		&events.Event{
			Typ:           events.ToolDispatchedEventType,
			CorrelationID: &state.arc.ID,
			Payload:       events.ToolDispatchedPayload{CausationID: call.ID},
		},
		invoke,
	); err != nil {
		return err
	}

	// The session is filled in by the task source the run was given, which
	// the worker binds to the session it is working; the agent addresses the
	// task by ID alone.
	ref := common.TaskRef{ID: invocationID}
	if err := state.tasks.Start(ctx, call.ToolName, ref, call.Args); err != nil {
		return a.settleUnstartedTask(ctx, call, ref, err, state)
	}

	return a.settleCall(ctx, call, ToolOutcome{Result: fmt.Sprintf(
		"task %s started, id %s. It runs on its own, so say what you have and end your turn. "+
			"Anything it says reaches you as a turn of its own, a question included, so there is "+
			"nothing here to wait for and nothing to poll. When it asks a question, you may answer "+
			"it yourself with %s, or put the question to the user first and then answer with %s "+
			"once you have their reply; stop it with %s. All take that id.",
		call.ToolName, ref.ID, events.ProvideTaskInputToolName, events.ProvideTaskInputToolName, events.CancelTaskToolName)}, state)
}

// taskActor is the actor a task's events carry: the one whose message opened
// the arc the task started in.
func taskActor(arc *history.Arc) users.Actor {
	if arc.Actor != nil {
		return *arc.Actor
	}
	return users.Actor{}
}

// settleUnstartedTask closes off a task whose launch failed. The task and its
// call are settled as failed, since nothing will report on them, and the text
// says the work may be running anyway.
//
// The settle is written with None visibility, correlated to the task: the run
// that is still in flight reports the failure to the model through the call's
// own result, so the settle opens no arc.
func (a Agent) settleUnstartedTask(ctx context.Context, call *history.ToolCall, ref common.TaskRef, cause error, state *agentState) error {
	reason := fmt.Sprintf(
		"task %s could not be started: %s. Whether it took hold before the failure is not known, "+
			"so check before starting it again.", call.ToolName, cause)

	if _, err := a.persistAndFold(ctx, state, &events.Event{
		Typ:           events.AsyncTaskResultEventType,
		CorrelationID: &ref.ID,
		Payload: events.AsyncTaskResultPayload{
			CausationID:   ref.ID,
			Result:        reason,
			Outcome:       events.AsyncTaskOutcomeFailed,
			LLMVisibility: events.LLMVisibilityNone,
		},
	}); err != nil {
		return err
	}
	return a.settleCall(ctx, call, ToolOutcome{Result: reason, IsError: true}, state)
}

// cancelTask asks a running task to stop. The cancel is written as a CANCEL
// event whose correlation ID is the task ID, which is what settles the task in
// the log, with who cancelled it and when; the runner is then reached through
// the controller. What became of work already under way is the runner's to
// report.
func (a Agent) cancelTask(ctx context.Context, call *history.ToolCall, state *agentState) error {
	if err := a.recordDispatch(ctx, call, state); err != nil {
		return err
	}

	task, failure := a.addressedTask(call, state)
	if failure != "" {
		return a.settleCall(ctx, call, ToolOutcome{Result: failure, IsError: true}, state)
	}
	if task.FinishedSeq != nil {
		return a.settleCall(ctx, call, ToolOutcome{Result: fmt.Sprintf(
			"task %s had already finished (%s), so there was nothing to stop.",
			task.TaskName, task.Status), IsError: true}, state)
	}

	reason, _ := stringArg(call.Args, "reason")
	taskID := task.ID
	cancel := &events.Event{
		Typ:           events.CancelEventType,
		CorrelationID: &taskID,
		Actor:         taskActor(state.arc),
		Payload:       events.CancelPayload{TaskID: &taskID},
	}
	if reason != "" {
		cancel.Payload = events.CancelPayload{TaskID: &taskID, Reason: &reason}
	}
	if _, err := a.persistAndFold(ctx, state, cancel); err != nil {
		return err
	}

	ref := common.TaskRef{ID: task.ID}
	if err := state.tasks.Cancel(ctx, ref, reason); err != nil {
		return a.settleCall(ctx, call, ToolOutcome{Result: fmt.Sprintf(
			"task %s could not be reached to stop it: %s", task.TaskName, err), IsError: true}, state)
	}

	return a.settleCall(ctx, call, ToolOutcome{Result: fmt.Sprintf(
		"task %s was asked to stop. It reports what became of it, so it may have done some of "+
			"its work already.", task.TaskName)}, state)
}

// provideTaskInput hands a waiting task the answer it asked for. The task is
// given the input before the log is told, so the log cannot claim input the
// task never received.
//
// The INPUT_PROVIDED record is written correlated to the arc the ask opened, so
// the answer is in that arc's own transcript at any cutoff; the fold attaches it
// there and it opens no other arc. The event carries the actor the invoke
// recorded, since the answer is given under that actor.
func (a Agent) provideTaskInput(ctx context.Context, call *history.ToolCall, state *agentState) error {
	if err := a.recordDispatch(ctx, call, state); err != nil {
		return err
	}

	task, failure := a.addressedTask(call, state)
	if failure != "" {
		return a.settleCall(ctx, call, ToolOutcome{Result: failure, IsError: true}, state)
	}
	input, ok := stringArg(call.Args, "input")
	if !ok {
		return a.settleCall(ctx, call, ToolOutcome{
			Result: "no input was given to hand to the task", IsError: true}, state)
	}
	// A run reads the log as it stood when the run began, so a question asked
	// since is not here yet and this says so rather than claiming the task is
	// not asking. Answering ahead of the question is the other way in.
	if task.Status != history.AsyncTaskWaiting {
		return a.settleCall(ctx, call, ToolOutcome{Result: fmt.Sprintf(
			"task %s has asked nothing you have been given to answer, and reads as %s here. Wait "+
				"to be handed its question rather than answering one you expect it to ask.",
			task.TaskName, task.Status), IsError: true}, state)
	}

	// The answer names the question it answers, so a task that has moved on
	// refuses it rather than taking it for whatever it is waiting on now.
	request := lastInputRequest(task)
	if request == nil {
		return a.settleCall(ctx, call, ToolOutcome{Result: fmt.Sprintf(
			"task %s is waiting, but the log holds no question of its to answer.",
			task.TaskName), IsError: true}, state)
	}

	ref := common.TaskRef{ID: task.ID}
	// A task somebody else answered first refuses this answer, and a task that
	// cannot be reached at all fails the same way. Nothing here tells the two
	// apart, so the text covers both.
	if err := state.tasks.ProvideInput(ctx, ref, request.ID, input); err != nil {
		return a.settleCall(ctx, call, ToolOutcome{Result: fmt.Sprintf(
			"task %s did not take the input: %s. It may have been answered already, or have "+
				"finished, so read what it says next before answering again.",
			task.TaskName, err), IsError: true}, state)
	}

	// The record lands on the arc the ask opened, which is where the answer
	// belongs: that arc exists, so the event attaches to it and opens no
	// other.
	answerArc := task.CorrelationID
	if task.WaitingArc != nil {
		answerArc = *task.WaitingArc
	}
	provided := &events.Event{
		Typ:           events.AsyncTaskInputProvidedEventType,
		CorrelationID: &answerArc,
		Payload:       events.AsyncTaskInputProvidedPayload{CausationID: task.ID, Message: input},
	}
	if task.Actor != nil {
		provided.Actor = *task.Actor
	}

	// Both events say the same already-settled thing, that the input reached
	// the task, so neither may survive without the other.
	_, err := a.persistAndFold(ctx, state,
		provided,
		callResult(call, ToolOutcome{Result: fmt.Sprintf(
			"task %s was given the input and is running again.", task.TaskName)}, state),
	)
	return err
}

// addressedTask finds the task a call names in the session's task table, or
// says why it could not. The reason is written for the model to act on.
func (a Agent) addressedTask(call *history.ToolCall, state *agentState) (*history.AsyncTask, string) {
	id, ok := stringArg(call.Args, "task_id")
	if !ok {
		return nil, "no task id was given"
	}
	if task, ok := state.history.Tasks[id]; ok {
		return task, ""
	}
	// A task outside this session is as absent as one never started.
	return nil, fmt.Sprintf("no task with id %s is part of this conversation", id)
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
	_, err := a.persistAndFold(ctx, state, callResult(call, outcome, state))
	return err
}

// callResult is the record of a call having been answered, for a caller that
// writes it alongside something else.
func callResult(call *history.ToolCall, outcome ToolOutcome, state *agentState) *events.Event {
	return &events.Event{
		Typ:           events.ToolResultEventType,
		CorrelationID: &state.arc.ID,
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
