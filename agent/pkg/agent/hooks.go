package agent

import (
	"context"
	"fmt"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"fillbrunn.ai/kube-agent/proto/pkg/common"
	"fillbrunn.ai/kube-agent/proto/pkg/events"
	"fillbrunn.ai/kube-agent/proto/pkg/history"
	"fillbrunn.ai/kube-agent/proto/pkg/users"
)

// Hooks are the agent's extension points. They speak in the agent's own
// domain types — calls, replies, asks, runs — never in events.Event or the
// payload vocabulary: the agent maps whatever a hook returns onto the events
// it was about to write, in one place per hook. Input structs are snapshots
// passed by value; change structs use pointer fields, where nil means
// "unchanged".
//
// Two error semantics. A hook that returns a change or a decision and fails
// fails the step, which records a fatal ERROR (BeforeToolCall fails closed:
// the call is settled as an error and never run). A pure info hook (After*)
// returns nothing, so it can never fail a run.
type Hooks struct {
	// BeforeRun fires at admission, before the built-in checks: Pass leaves
	// them running as the floor, Refuse ends the run with a fatal error.
	BeforeRun func(ctx context.Context, run RunView) (RunDecision, error)

	// AfterRun fires when a run yields, whatever ended it. Info only.
	AfterRun func(ctx context.Context, sum RunSummary)

	// BeforeAssistant fires after the projection, before the model is
	// invoked. Changes affect this request only, never the log.
	BeforeAssistant func(ctx context.Context, req AssistantRequest) (AssistantRequestChanges, error)

	// AfterAssistant fires after the model replied, before the reply and its
	// tool calls are persisted. Changes are what lands in the log, which may
	// deviate from what was streamed to watchers.
	AfterAssistant func(ctx context.Context, reply AssistantReply) (AssistantReplyChanges, error)

	// BeforeToolCall fires once per pending ordinary tool call, in one pass
	// before any call of the step runs, so several asks are raised together.
	BeforeToolCall func(ctx context.Context, call ToolCallView) (ToolCallEffects, error)

	// AfterToolCall fires after a tool ran, before its result is persisted,
	// so a redaction never lands in the log at all.
	AfterToolCall func(ctx context.Context, res ToolCallResult) (ToolCallChanges, error)

	// BeforeTaskStart / BeforeTaskCancel / BeforeTaskInput fire like
	// BeforeToolCall, for the calls that start, cancel and answer a task.
	// Their views carry the operation's args as a proper type plus the call's
	// metadata. A start may override the args it runs with; cancel and input
	// run with the args as logged, so their effects are plain HookEffects.
	BeforeTaskStart  func(ctx context.Context, v TaskStartView) (TaskStartEffects, error)
	BeforeTaskCancel func(ctx context.Context, v TaskCancelView) (TaskCancelEffects, error)
	BeforeTaskInput  func(ctx context.Context, v TaskInputView) (TaskInputEffects, error)

	// AfterTaskResult / AfterTaskEvent fire for the one event a work item
	// names as its trigger — whichever arc the item names, so an event that
	// opened no arc is still seen. Info only: each triggering event fires
	// its hook once, when its item is worked, from the log or the fold; a
	// consumer that dies after firing may re-fire the item on retry. The
	// views carry the event's seq.
	AfterTaskResult func(ctx context.Context, res TaskResultView)
	AfterTaskEvent  func(ctx context.Context, ev TaskEventView)

	// BeforeSkillLoad / AfterSkillLoad fire around calls to the load_skill
	// tool, a name-filtered special case of the tool hooks.
	BeforeSkillLoad func(ctx context.Context, call ToolCallView) (ToolCallEffects, error)
	AfterSkillLoad  func(ctx context.Context, res ToolCallResult) (ToolCallChanges, error)

	// BeforeInterrupt fires before any INTERRUPT is written, arc-wide or
	// call-scoped. Block settles the originating call as a tool error instead.
	BeforeInterrupt func(ctx context.Context, ask InterruptRequest) (InterruptChanges, error)

	// AfterInterrupt fires after an interrupt was raised. Info only.
	AfterInterrupt func(ctx context.Context, ask InterruptView)

	// BeforeInterruptResponse fires on the answering side, before the
	// response is persisted: modify the answer or refuse the write.
	BeforeInterruptResponse func(ctx context.Context, resp InterruptResponseView) (InterruptResponseChanges, error)

	// AfterInterruptResponse fires for the INTERRUPT_RESPONSE a work item
	// names as its trigger. Info only; see AfterTaskResult for the firing
	// semantics.
	AfterInterruptResponse func(ctx context.Context, resp InterruptResponseView)

	// AfterArcClosed fires after an arc's close is persisted. Info only.
	AfterArcClosed func(ctx context.Context, sum ArcSummary)
}

// WithHooks gives an agent its hooks. They are agent-level: a run cannot
// replace them.
func WithHooks(hooks Hooks) AgentOption {
	return func(a *Agent) error {
		a.hooks = hooks
		return nil
	}
}

// beforeToolCall runs the Before* hook that governs an ordinary call — the
// skill one for the skill tool, the general one for every other tool — and
// validates its effects. A call no hook governs runs.
func (a Agent) beforeToolCall(ctx context.Context, call *history.ToolCall, state *agentState) (ToolCallEffects, error) {
	hook := toolCallHook(call.ToolName, state.hooks)
	if hook == nil {
		return ToolCallEffects{Run: true}, nil
	}
	eff, err := hook(ctx, ToolCallView{
		CallID:    call.ID,
		ToolName:  call.ToolName,
		Args:      call.Args,
		MessageID: call.MessageID,
		VendorID:  call.VendorID,
		ArcID:     state.arc.ID,
	})
	if err != nil {
		return ToolCallEffects{}, err
	}
	if err := eff.validate(); err != nil {
		return ToolCallEffects{}, err
	}
	return eff, nil
}

// toolCallHook returns the Before* hook governing an ordinary call by that
// name: the skill one for the skill tool, the general one for every other
// tool. Task operations have their own hooks and configured interrupts are
// governed by BeforeInterrupt, which fires where the interrupt is raised.
func toolCallHook(name string, hooks Hooks) func(context.Context, ToolCallView) (ToolCallEffects, error) {
	if name == LoadSkillToolName {
		return hooks.BeforeSkillLoad
	}
	return hooks.BeforeToolCall
}

// callMetaOf is the metadata of a pending call, carried by every Before*
// view alongside its operation-specific args.
func callMetaOf(call *history.ToolCall, state *agentState) CallMeta {
	return CallMeta{CallID: call.ID, VendorID: call.VendorID, MessageID: call.MessageID, ArcID: state.arc.ID}
}

// callDecision is what the decision pass resolved for one pending call: the
// hook's effects plus what only some calls carry — running alone, an args
// override, being an interrupt call that runs after the approvals, or a hook
// failure that settles the call closed.
type callDecision struct {
	HookEffects
	solo      bool
	override  any
	interrupt bool
	hookErr   error
}

// callDecisions is what the decision pass over a step's pending calls
// produced, keyed by call. It is a lookup, not the iteration source: iterate
// the calls the pass was given, in their order, and read each entry by key.
// Every call the pass was given has exactly one entry.
type callDecisions map[*history.ToolCall]callDecision

// decideCalls runs the Before* hook over every pending call of a step before
// any of them runs, so a step's blocks and answers settle in one write and
// its asks are raised together. A call that already carries a human's
// decision runs as it stands — the fold refuses a second interrupt on one
// call. A hook that fails fails its call closed: settled as an error, never
// run.
func (a Agent) decideCalls(ctx context.Context, calls []*history.ToolCall, state *agentState) callDecisions {
	out := callDecisions{}
	for _, call := range calls {
		if a.operation(call.ToolName) == opInterrupt {
			out[call] = callDecision{HookEffects: HookEffects{Run: true}, interrupt: true}
			continue
		}
		if call.Interrupt != nil {
			out[call] = callDecision{HookEffects: HookEffects{Run: true}}
			continue
		}
		switch a.operation(call.ToolName) {
		case opCancel:
			out[call] = a.decideTaskCancel(ctx, call, state)
		case opProvideInput:
			out[call] = a.decideTaskInput(ctx, call, state)
		default:
			out[call] = a.decideOrdinaryCall(ctx, call, state)
		}
	}
	return out
}

// partition groups the decisions by the calls the pass was given, in their
// order: what settles without running, what a human is asked, which calls
// run in waves, and which interrupt calls run last.
func (decisions callDecisions) partition(calls []*history.ToolCall, arcID string) (settles []*events.Event, approvals []approvalAsk, ordinary, asks []*history.ToolCall) {
	for _, call := range calls {
		d := decisions[call]
		switch {
		case d.hookErr != nil:
			settles = append(settles, callResult(call, ToolOutcome{
				IsError: true,
				Result:  fmt.Sprintf("the call policy failed, so the call was not run: %v", d.hookErr),
			}, arcID))
		case d.Block != nil:
			settles = append(settles, &events.Event{
				Typ:           events.GuardrailBlockedEventType,
				CorrelationID: &arcID,
				Payload:       events.GuardrailBlockedPayload{CausationID: call.ID, Message: d.Block.Message},
			})
		case d.Answer != nil:
			settles = append(settles, callResult(call, ToolOutcome{Result: d.Answer.Result, IsError: d.Answer.IsError}, arcID))
		case d.Ask != nil:
			approvals = append(approvals, approvalAsk{call: call, ask: d.Ask})
		case d.interrupt:
			asks = append(asks, call)
		default:
			ordinary = append(ordinary, call)
		}
	}
	return settles, approvals, ordinary, asks
}

// decideOrdinaryCall runs the Before* hook governing an ordinary tool call
// and returns its decision.
func (a Agent) decideOrdinaryCall(ctx context.Context, call *history.ToolCall, state *agentState) callDecision {
	eff, err := a.beforeToolCall(ctx, call, state)
	if err != nil {
		return callDecision{hookErr: err}
	}
	d := callDecision{HookEffects: eff.HookEffects, solo: eff.Run && eff.Sequential}
	if eff.Args != nil {
		d.override = *eff.Args
	}
	return d
}

// decideTaskCancel runs BeforeTaskCancel over a call that stops a task and
// returns its decision. The cancel runs with the args as logged, so the
// effects carry no override.
func (a Agent) decideTaskCancel(ctx context.Context, call *history.ToolCall, state *agentState) callDecision {
	hook := state.hooks.BeforeTaskCancel
	if hook == nil {
		return callDecision{HookEffects: HookEffects{Run: true}}
	}
	eff, err := hook(ctx, TaskCancelView{
		Args:     taskCancelArgsOf(call.Args),
		CallMeta: callMetaOf(call, state),
	})
	if err != nil {
		return callDecision{hookErr: err}
	}
	if err := eff.validate(); err != nil {
		return callDecision{hookErr: err}
	}
	return callDecision{HookEffects: eff}
}

// decideTaskInput runs BeforeTaskInput over a call that answers a waiting
// task and returns its decision. Like the cancel, it runs with the args as
// logged.
func (a Agent) decideTaskInput(ctx context.Context, call *history.ToolCall, state *agentState) callDecision {
	hook := state.hooks.BeforeTaskInput
	if hook == nil {
		return callDecision{HookEffects: HookEffects{Run: true}}
	}
	eff, err := hook(ctx, TaskInputView{
		Args:     taskInputArgsOf(call.Args),
		CallMeta: callMetaOf(call, state),
	})
	if err != nil {
		return callDecision{hookErr: err}
	}
	if err := eff.validate(); err != nil {
		return callDecision{hookErr: err}
	}
	return callDecision{HookEffects: eff}
}

// runView is the snapshot of the arc a run is about to work, handed to
// BeforeRun.
func runView(state *agentState) RunView {
	arc := state.arc
	return RunView{
		ArcID:           arc.ID,
		Content:         arc.Content,
		OpenedByTask:    arc.OpenedByTask,
		Actor:           arc.Actor,
		EstimatedTokens: state.history.EstimateArc(arc),
	}
}

// applyBeforeRun runs the BeforeRun hook and applies its decision: a refusal
// writes a fatal error event and reports that the run may not start. A hook
// error propagates.
func (a Agent) applyBeforeRun(ctx context.Context, state *agentState) (bool, error) {
	hook := state.hooks.BeforeRun
	if hook == nil {
		return true, nil
	}
	decision, err := hook(ctx, runView(state))
	if err != nil {
		return false, err
	}
	if decision.Refuse == nil {
		return true, nil
	}
	if _, err := a.persistAndFold(ctx, state, &events.Event{
		Typ:           events.ErrorEventType,
		CorrelationID: &state.arc.ID,
		Payload:       events.ErrorPayload{Message: decision.Refuse.Message, IsFatal: true},
	}); err != nil {
		return false, err
	}
	state.outcome = RunRefused
	return false, nil
}

// fireAfterRun logs the run's outcome and hands its summary to AfterRun.
func fireAfterRun(ctx context.Context, state *agentState, arcID string, err error) {
	sum := RunSummary{ArcID: arcID, Outcome: state.outcome, Err: err, Rounds: state.rounds, Written: state.written}
	switch state.outcome {
	case RunFailed:
		state.logger.ErrorContext(ctx, "run failed", "outcome", string(sum.Outcome), "rounds", sum.Rounds, "written", sum.Written, "error", err)
	default:
		state.logger.InfoContext(ctx, "run ended", "outcome", string(sum.Outcome), "rounds", sum.Rounds, "written", sum.Written, "error", err)
	}
	if hook := state.hooks.AfterRun; hook != nil {
		hook(ctx, sum)
	}
}

// arcSummary is the view of a closed arc handed to AfterArcClosed.
func arcSummary(state *agentState) ArcSummary {
	arc := state.arc
	summary := ArcSummary{
		ArcID:               arc.ID,
		ConclusionMessageID: arc.Responses[len(arc.Responses)-1].ID,
		Actor:               arc.Actor,
		Messages:            len(arc.Responses),
	}
	for _, response := range arc.Responses {
		summary.ToolCalls += len(response.ToolCalls)
		if response.Usage != nil {
			summary.InputTokens += response.Usage.Input
			summary.OutputTokens += response.Usage.Output
		}
	}
	return summary
}

// fireAfterArcClosed hands the closed arc's summary to AfterArcClosed.
func fireAfterArcClosed(ctx context.Context, state *agentState) {
	if hook := state.hooks.AfterArcClosed; hook != nil {
		hook(ctx, arcSummary(state))
	}
}

// applyBeforeAssistant runs BeforeAssistant over the request about to go to
// the model. Its changes reach only this call, never the log; a Reply
// short-circuits the model entirely and is returned for the caller to
// record.
func applyBeforeAssistant(ctx context.Context, req AssistantRequest, tools []*mcp.Tool, state *agentState) (AssistantRequest, []*mcp.Tool, *LLMResponse, error) {
	hook := state.hooks.BeforeAssistant
	if hook == nil {
		return req, tools, nil, nil
	}
	changes, err := hook(ctx, req)
	if err != nil {
		return req, tools, nil, err
	}
	if changes.SystemPrompt != nil {
		req.SystemPrompt = *changes.SystemPrompt
	}
	if changes.Transcript != nil {
		req.Transcript = changes.Transcript
	}
	if changes.Tools != nil {
		tools = *changes.Tools
	}
	return req, tools, changes.Reply, nil
}

// applyAfterAssistant runs AfterAssistant over the model's reply before
// anything is persisted, so its changes are what lands in the log — which may
// deviate from what was streamed to watchers. Abort vetoes the reply and is
// returned as the error that fails the run.
func applyAfterAssistant(ctx context.Context, reply AssistantReply, state *agentState) (AssistantReply, error) {
	hook := state.hooks.AfterAssistant
	if hook == nil {
		return reply, nil
	}
	changes, err := hook(ctx, reply)
	if err != nil {
		return reply, err
	}
	if changes.Abort != nil {
		return reply, changes.Abort
	}
	if changes.Content != nil {
		reply.Content = *changes.Content
	}
	if changes.Reasoning != nil {
		reply.Reasoning = *changes.Reasoning
	}
	if changes.Usage != nil {
		reply.Usage = changes.Usage
	}
	if changes.ToolCalls != nil {
		reply.ToolCalls = *changes.ToolCalls
	}
	return reply, nil
}

// applyAfterToolCall runs the After* hook for a settled call — the skill
// hook for the skill tool, the general one for every other — and returns the
// outcome with the changes applied plus the note events they asked for. A
// hook error fails the call.
func applyAfterToolCall(ctx context.Context, call *history.ToolCall, args any, outcome ToolOutcome, state *agentState) (ToolOutcome, []*events.Event, error) {
	hook := state.hooks.AfterToolCall
	if call.ToolName == LoadSkillToolName && state.hooks.AfterSkillLoad != nil {
		hook = state.hooks.AfterSkillLoad
	}
	if hook == nil {
		return outcome, nil, nil
	}
	changes, err := hook(ctx, ToolCallResult{
		CallID:   call.ID,
		ToolName: call.ToolName,
		Args:     args,
		Result:   outcome.Result,
		IsError:  outcome.IsError,
	})
	if err != nil {
		return outcome, nil, err
	}
	outcome = applyToolCallChanges(outcome, changes)
	var notes []*events.Event
	for _, note := range changes.Notes {
		notes = append(notes, &events.Event{
			Typ:           events.ToolEventEventType,
			CorrelationID: &state.arc.ID,
			Payload:       events.ToolEventPayload{CausationID: call.ID, Payload: note},
		})
	}
	return outcome, notes, nil
}

// execArgs returns the arguments a call executes with: the ones a Before*
// hook overrode, where it did, else the ones the model asked for. The log
// keeps the model's arguments either way. The tool step's decision pass
// resolves this per call and hands the result down to the running part; it
// is never kept on the state.
func execArgs(call *history.ToolCall, decisions callDecisions) any {
	if d, ok := decisions[call]; ok && d.override != nil {
		return d.override
	}
	return call.Args
}

// prepareInterrupt applies the BeforeInterrupt hook to an ask about to be
// raised and returns what to write: the INTERRUPT event — plus an
// INTERRUPT_RESPONSE where the hook auto-answered it — or, where the hook
// blocked it, the settle event for the originating call instead. An error
// means the hook failed or the changes were invalid; the caller fails closed.
func (a Agent) prepareInterrupt(ctx context.Context, req InterruptRequest, call *history.ToolCall, state *agentState) (evs []*events.Event, settle *events.Event, err error) {
	changes := InterruptChanges{}
	if hook := state.hooks.BeforeInterrupt; hook != nil {
		changes, err = hook(ctx, req)
		if err != nil {
			return nil, nil, err
		}
	}
	if changes.Block != nil {
		return nil, callResult(call, ToolOutcome{IsError: true, Result: changes.Block.Message}, state.arc.ID), nil
	}
	reason, context := req.Reason, req.Context
	if changes.Reason != nil {
		reason = *changes.Reason
	}
	if changes.Context != nil {
		context = *changes.Context
	}
	arcID := req.ArcID
	interrupt := &events.Event{
		ID:            common.NewEventID(),
		Typ:           events.InterruptEventType,
		CorrelationID: &arcID,
		Payload:       events.InterruptPayload{CausationID: req.CallID, Reason: reason, Context: context},
	}
	evs = []*events.Event{interrupt}
	if changes.AutoAnswer != nil {
		answer := *changes.AutoAnswer
		// A verdict is only meaningful on a call-scoped interrupt; on an
		// arc-wide one it is ignored — the fold would refuse the record —
		// rather than failing the hook.
		if req.CallID == "" {
			answer.IsRejected = false
		}
		evs = append(evs, &events.Event{
			Typ:           events.InterruptResponseEventType,
			CorrelationID: &arcID,
			Payload: events.InterruptResponsePayload{
				CausationID: interrupt.ID,
				Response:    answer.Response,
				IsRejected:  answer.IsRejected,
			},
		})
	}
	return evs, nil, nil
}

// notifyInterruptRaised fires the info-only AfterInterrupt hook for every
// INTERRUPT in a stored batch. The hook returns nothing, so it cannot fail
// the run.
func notifyInterruptRaised(ctx context.Context, state *agentState, stored []*events.Event) {
	hook := state.hooks.AfterInterrupt
	if hook == nil {
		return
	}
	for _, ev := range stored {
		if ev.Typ != events.InterruptEventType {
			continue
		}
		pl, ok := ev.Payload.(events.InterruptPayload)
		if !ok {
			continue
		}
		view := InterruptView{
			ID:      ev.ID,
			CallID:  pl.CausationID,
			Scope:   ScopeArc,
			Reason:  pl.Reason,
			Context: pl.Context,
		}
		if ev.CorrelationID != nil {
			view.ArcID = *ev.CorrelationID
		}
		if pl.CausationID != "" {
			view.Scope = ScopeCall
		}
		hook(ctx, view)
	}
}

// approvalAsk is a call held for a human, collected in the decision pass.
type approvalAsk struct {
	call *history.ToolCall
	ask  *AskRequest
}

// raiseApprovals writes one call-scoped INTERRUPT per ask, in a single batch,
// so a human decides on all of a step's asks together. Each ask first goes
// through BeforeInterrupt. The arc reads INTERRUPTED while any of them is
// pending, which ends the run at its next step; an approval leaves the call
// pending for the resumed run, and a rejection settles it REFUSED with the
// human's words as its result.
func (a Agent) raiseApprovals(ctx context.Context, asks []approvalAsk, state *agentState) error {
	if len(asks) == 0 {
		return nil
	}
	var batch []*events.Event
	for _, ap := range asks {
		req := InterruptRequest{
			ToolName: ap.call.ToolName,
			CallID:   ap.call.ID,
			ArcID:    state.arc.ID,
			Reason:   ap.ask.Reason,
			Context:  ap.ask.Context,
		}
		evs, settle, err := a.prepareInterrupt(ctx, req, ap.call, state)
		if err != nil {
			// Fail closed: no interrupt, the call settles as an error.
			settle = callResult(ap.call, ToolOutcome{
				IsError: true,
				Result:  fmt.Sprintf("the interrupt policy failed, so the call was not run: %v", err),
			}, state.arc.ID)
			evs = nil
		}
		if settle != nil {
			batch = append(batch, settle)
		}
		batch = append(batch, evs...)
	}
	if len(batch) == 0 {
		return nil
	}
	stored, err := a.persistAndFold(ctx, state, batch...)
	if err != nil {
		return err
	}
	notifyInterruptRaised(ctx, state, stored)
	return nil
}

// RunView is the snapshot of the arc a run is about to work, handed to
// BeforeRun.
type RunView struct {
	ArcID           string
	Content         string
	OpenedByTask    string
	Actor           *users.Actor
	EstimatedTokens int
}

// RunDecision is what BeforeRun returns: pass, or refuse. Pass leaves the
// built-in admission checks running as the floor, which is where compaction
// requests and suspensions come from — the hook does not trigger them.
type RunDecision struct {
	Pass   bool
	Refuse *Refusal
}

// Refusal ends a run before it starts, with a fatal error in the log.
type Refusal struct{ Message string }

// RunOutcome names why a run yielded.
type RunOutcome string

const (
	RunFinished    RunOutcome = "finished"
	RunInterrupted RunOutcome = "interrupted"
	RunSuspended   RunOutcome = "suspended"
	RunRefused     RunOutcome = "refused"
	RunCancelled   RunOutcome = "cancelled"
	RunFailed      RunOutcome = "failed"
)

// RunSummary is handed to AfterRun.
type RunSummary struct {
	ArcID   string
	Outcome RunOutcome
	Err     error
	Rounds  int
	Written int
}

// AssistantRequest is the snapshot of one model invocation, handed to
// BeforeAssistant.
type AssistantRequest struct {
	ArcID           string
	Modes           history.Modes
	SystemPrompt    string
	Transcript      *history.ModelTranscript // fresh projection per step; safe to mutate
	Tools           []*mcp.Tool
	EstimatedTokens int
}

// AssistantRequestChanges alters what is sent to the model. Reply replaces
// the model call entirely: the given reply is recorded as the answer.
type AssistantRequestChanges struct {
	SystemPrompt *string
	Transcript   *history.ModelTranscript
	Tools        *[]*mcp.Tool
	Reply        *LLMResponse
}

// AssistantReply is the model's reply as it will be recorded, handed to
// AfterAssistant.
type AssistantReply struct {
	ArcID     string
	MessageID string
	VendorID  *string
	ModelID   string
	Content   string
	Reasoning []byte
	Usage     *TokenUsage
	ToolCalls []LLMToolCall
}

// AssistantReplyChanges alters what is persisted. ToolCalls set to an empty
// slice drops every call; Abort vetoes the reply and fails the run.
type AssistantReplyChanges struct {
	Content   *string
	Reasoning *[]byte
	Usage     *TokenUsage
	ToolCalls *[]LLMToolCall
	Abort     error
}

// ToolCallView is the snapshot of one pending call, handed to the Before*
// call hooks.
type ToolCallView struct {
	CallID    string
	ToolName  string
	Args      any
	MessageID string
	VendorID  string
	ArcID     string
}

// CallMeta is the metadata of a pending call, carried by every task Before*
// view alongside its operation-specific args.
type CallMeta struct {
	CallID    string
	VendorID  string
	MessageID string
	ArcID     string
}

// TaskStartView is handed to BeforeTaskStart: the start's args, with the
// task kind as the tool name, plus the call's metadata.
type TaskStartView struct {
	TaskName string
	Args     TaskArgs
	CallMeta
}

// TaskCancelView is handed to BeforeTaskCancel.
type TaskCancelView struct {
	Args TaskCancelArgs
	CallMeta
}

// TaskInputView is handed to BeforeTaskInput.
type TaskInputView struct {
	Args TaskInputArgs
	CallMeta
}

// BlockRequest refuses a call without asking anyone.
type BlockRequest struct{ Message string }

// AskRequest holds a call for a human. Reason is what the human is asked, in
// words; Context is shown alongside the call's arguments.
type AskRequest struct {
	Reason  string
	Context any
}

// Answer settles a call without running it.
type Answer struct {
	Result  any
	IsError bool
}

// HookEffects is what a Before* hook returns. Exactly one of Run, Block,
// Ask, Answer is set.
type HookEffects struct {
	Run    bool
	Block  *BlockRequest
	Ask    *AskRequest
	Answer *Answer
}

// validate requires exactly one action.
func (e HookEffects) validate() error {
	n := 0
	for _, set := range []bool{e.Run, e.Block != nil, e.Ask != nil, e.Answer != nil} {
		if set {
			n++
		}
	}
	if n != 1 {
		return fmt.Errorf("a hook result sets %d of run/block/ask/answer, want exactly one", n)
	}
	return nil
}

// ToolCallEffects are the effects of a Before* hook on an ordinary call:
// HookEffects plus running alone (Sequential) and overriding the executed
// arguments (Args) — the log keeps the ones the model asked for.
type ToolCallEffects struct {
	HookEffects
	Sequential bool
	Args       *any
}

// TaskStartEffects are the effects of BeforeTaskStart, in task vocabulary:
// exactly one of Run, Block, Ask, Result, or a LaunchResult alone. A Result
// fakes the task's outcome — it is written verbatim as the task's
// ASYNC_TASK_RESULT — and LaunchResult controls what the launch itself
// reports: with IsError the text becomes the launch's Error and no task
// result is written at all, otherwise it is the launch answer the model is
// told the start call returned. A LaunchResult set alone leaves the task
// result empty. Args overrides the args the task runs with.
type TaskStartEffects struct {
	Run          bool
	Block        *BlockRequest
	Ask          *AskRequest
	Result       *events.TaskResult
	LaunchResult *LaunchResult
	Args         *TaskArgs
}

// validate requires exactly one of run/block/ask/result, or a launchResult
// alone. A launchResult may ride with a result to override the text the model
// is told, never with run/block/ask.
func (e TaskStartEffects) validate() error {
	n := 0
	for _, set := range []bool{e.Run, e.Block != nil, e.Ask != nil, e.Result != nil} {
		if set {
			n++
		}
	}
	if e.LaunchResult != nil && (e.Run || e.Block != nil || e.Ask != nil) {
		return fmt.Errorf("a task start effect sets launchResult together with run/block/ask; launchResult only applies when the start is settled without the task running")
	}
	if n > 1 || (n == 0 && e.LaunchResult == nil) {
		return fmt.Errorf("a task start effect sets %d of run/block/ask/result, want exactly one, or a launchResult alone", n)
	}
	return nil
}

// LaunchResult is what a BeforeTaskStart hook reports for the launch itself
// when it settled the start without the task running: Text becomes the
// launch's Error when IsError, and the launch answer the model is told
// otherwise. Set alone, no task result is written.
type LaunchResult struct {
	Text    string
	IsError bool
}

// TaskCancelEffects and TaskInputEffects are plain HookEffects: those calls
// run with the args as logged, so there is nothing to override.
type TaskCancelEffects = HookEffects
type TaskInputEffects = HookEffects

// ToolCallResult is the snapshot of one settled tool call, handed to
// AfterToolCall and AfterSkillLoad.
type ToolCallResult struct {
	CallID   string
	ToolName string
	Args     any // the arguments as executed, after any override
	Result   any
	IsError  bool
}

// ToolCallChanges alters the recorded result. Notes are written as
// notifications in the same batch as the result.
type ToolCallChanges struct {
	Result  *any
	IsError *bool
	Notes   []string
}

// applyToolCallChanges folds the changes into the outcome.
func applyToolCallChanges(outcome ToolOutcome, changes ToolCallChanges) ToolOutcome {
	if changes.Result != nil {
		outcome.Result = *changes.Result
	}
	if changes.IsError != nil {
		outcome.IsError = *changes.IsError
	}
	return outcome
}

// TaskResultView is handed to AfterTaskResult.
type TaskResultView struct {
	TaskID     string
	TaskName   string
	Outcome    history.AsyncTaskStatus
	Result     any
	Visibility string
	Seq        int
}

// TaskEventView is handed to AfterTaskEvent.
type TaskEventView struct {
	TaskID     string
	Type       string // NOTIFICATION, INPUT_REQUESTED, INPUT_PROVIDED
	Class      string // the notification's own name, for NOTIFICATION entries
	Payload    any
	Visibility string
	Seq        int
}

// fireTriggerHook fires the continuation hook for the one event a work item
// names as its trigger: from the folded batch when the read carried the
// event, otherwise from the fold itself. It reports whether the trigger was
// found at all, so the caller can fall back to a full read when a snapshot
// covered the event without its fold carrying it.
func fireTriggerHook(ctx context.Context, hooks Hooks, h *history.History, evs []*events.Event, seq int) bool {
	if hooks.AfterTaskResult == nil && hooks.AfterTaskEvent == nil && hooks.AfterInterruptResponse == nil {
		return true
	}
	for _, ev := range evs {
		if ev.Seq == seq {
			fireExternalEvent(ctx, hooks, h, ev)
			return true
		}
	}
	return fireTriggerFromFold(ctx, hooks, h, seq)
}

// fireExternalEvent fires the continuation hook for one folded event: a task
// result or notification, or an interrupt response.
func fireExternalEvent(ctx context.Context, hooks Hooks, h *history.History, ev *events.Event) {
	switch ev.Typ {
	case events.AsyncTaskResultEventType:
		if hooks.AfterTaskResult == nil {
			return
		}
		pl, ok := ev.Payload.(events.AsyncTaskResultPayload)
		if !ok {
			return
		}
		view := TaskResultView{
			TaskID:  pl.CausationID,
			Outcome: taskStatusOfOutcome(pl.Outcome),
			Result:  pl.Result,
			Seq:     ev.Seq,
		}
		if task, ok := h.Tasks[pl.CausationID]; ok {
			view.TaskName = task.TaskName
		}
		hooks.AfterTaskResult(ctx, view)
	case events.AsyncTaskEventEventType, events.AsyncTaskInputRequestedEventType, events.AsyncTaskInputProvidedEventType:
		if hooks.AfterTaskEvent == nil {
			return
		}
		if view, ok := taskEventView(ev); ok {
			hooks.AfterTaskEvent(ctx, view)
		}
	case events.InterruptResponseEventType:
		if hooks.AfterInterruptResponse == nil {
			return
		}
		pl, ok := ev.Payload.(events.InterruptResponsePayload)
		if !ok {
			return
		}
		calls := interruptCallIDs(h)
		view := InterruptResponseView{
			InterruptID: pl.CausationID,
			Response:    pl.Response,
			IsRejected:  pl.IsRejected,
			Actor:       pl.Actor,
			CallID:      calls[pl.CausationID],
			Seq:         ev.Seq,
		}
		if ev.CorrelationID != nil {
			view.ArcID = *ev.CorrelationID
		}
		hooks.AfterInterruptResponse(ctx, view)
	}
}

// fireTriggerFromFold resolves a trigger the fold already carries — one at
// or below a snapshot's seq — to its hook view and fires it. The fold keys
// every hooked event by seq: a settled task's FinishedSeq, a task transcript
// entry's Seq, an answered interrupt's ResolvedSeq. It reports whether any
// record matched the seq.
func fireTriggerFromFold(ctx context.Context, hooks Hooks, h *history.History, seq int) bool {
	for _, task := range h.Tasks {
		if hooks.AfterTaskResult != nil && task.FinishedSeq != nil && *task.FinishedSeq == seq {
			hooks.AfterTaskResult(ctx, TaskResultView{
				TaskID:   task.ID,
				TaskName: task.TaskName,
				Outcome:  task.Status,
				Result:   task.Result,
				Seq:      seq,
			})
			return true
		}
		if hooks.AfterTaskEvent != nil {
			for _, te := range task.Events {
				if te.Seq == seq {
					hooks.AfterTaskEvent(ctx, TaskEventView{
						TaskID:     task.ID,
						Type:       string(te.Type),
						Class:      te.Class,
						Payload:    te.Payload,
						Visibility: string(te.Visibility),
						Seq:        te.Seq,
					})
					return true
				}
			}
		}
	}
	if hooks.AfterInterruptResponse != nil {
		for _, arc := range h.Arcs {
			interrupts := append([]*history.Interrupt{}, arc.Interrupts...)
			for _, resp := range arc.Responses {
				for _, call := range resp.ToolCalls {
					if call.Interrupt != nil {
						interrupts = append(interrupts, call.Interrupt)
					}
				}
			}
			for _, in := range interrupts {
				if in.ResolvedSeq == nil || *in.ResolvedSeq != seq {
					continue
				}
				view := InterruptResponseView{
					InterruptID: in.ID,
					ArcID:       in.CorrelationID,
					CallID:      in.CausationID,
					Response:    in.Response,
					IsRejected:  in.Status == history.InterruptRejected,
					Seq:         seq,
				}
				if in.Responder != nil {
					view.Actor = *in.Responder
				}
				hooks.AfterInterruptResponse(ctx, view)
				return true
			}
		}
	}
	return false
}

// interruptCallIDs maps each interrupt in the folded history to the call it
// holds, empty for arc-wide ones.
func interruptCallIDs(h *history.History) map[string]string {
	calls := map[string]string{}
	for _, arc := range h.Arcs {
		for _, in := range arc.Interrupts {
			calls[in.ID] = in.CausationID
		}
		for _, resp := range arc.Responses {
			for _, call := range resp.ToolCalls {
				if call.Interrupt != nil {
					calls[call.Interrupt.ID] = call.Interrupt.CausationID
				}
			}
		}
	}
	return calls
}

// taskStatusOfOutcome maps a result's outcome onto the task status vocabulary
// the views use.
func taskStatusOfOutcome(o events.AsyncTaskOutcome) history.AsyncTaskStatus {
	switch o {
	case events.AsyncTaskOutcomeSuccess:
		return history.AsyncTaskSuccess
	case events.AsyncTaskOutcomeFailed:
		return history.AsyncTaskFailed
	case events.AsyncTaskOutcomeCancelled:
		return history.AsyncTaskCancelled
	}
	return ""
}

// taskEventView maps one persisted task event to its view.
func taskEventView(ev *events.Event) (TaskEventView, bool) {
	view := TaskEventView{Seq: ev.Seq}
	switch pl := ev.Payload.(type) {
	case events.AsyncTaskEventPayload:
		view.Type = string(history.AsyncTaskEventNotification)
		view.TaskID = pl.CausationID
		view.Class = pl.Class
		view.Payload = pl.Payload
		view.Visibility = string(pl.LLMVisibility)
	case events.AsyncTaskInputRequestedPayload:
		view.Type = string(history.AsyncTaskEventInputRequested)
		view.TaskID = pl.CausationID
		view.Payload = pl.Message
		view.Visibility = string(pl.LLMVisibility)
	case events.AsyncTaskInputProvidedPayload:
		view.Type = string(history.AsyncTaskEventInputProvided)
		view.TaskID = pl.CausationID
		view.Payload = pl.Message
	default:
		return TaskEventView{}, false
	}
	return view, true
}

// DecideInterruptResponse applies the BeforeInterruptResponse hook to an
// answer before the answering side persists it: the returned view is what
// should be written. A Refuse error rejects the write entirely, and a
// rejection of an arc-wide interrupt is refused, since the fold would refuse
// the record. The answering side calls this rather than the hook itself, so
// the hook never sees the raw event — the caller maps the view onto the
// INTERRUPT_RESPONSE it was about to write.
func DecideInterruptResponse(ctx context.Context, hooks Hooks, view InterruptResponseView) (InterruptResponseView, error) {
	hook := hooks.BeforeInterruptResponse
	if hook == nil {
		return view, nil
	}
	changes, err := hook(ctx, view)
	if err != nil {
		return view, err
	}
	if changes.Refuse != nil {
		return view, changes.Refuse
	}
	if changes.Response != nil {
		view.Response = *changes.Response
	}
	// A verdict is only meaningful on a call-scoped interrupt; on an arc-wide
	// one a rejection is ignored — the fold would refuse the record — rather
	// than failing the write.
	if changes.IsRejected != nil && (view.CallID != "" || !*changes.IsRejected) {
		view.IsRejected = *changes.IsRejected
	}
	return view, nil
}

// InterruptScope says what an interrupt holds.
type InterruptScope string

const (
	ScopeArc  InterruptScope = "arc"
	ScopeCall InterruptScope = "call"
)

// InterruptRequest is the ask an agent is about to raise, handed to
// BeforeInterrupt. CallID empty means arc-wide.
type InterruptRequest struct {
	ToolName string
	CallID   string
	ArcID    string
	Reason   string
	Context  any
}

// InterruptAnswer is an automatic answer to an interrupt. IsRejected is a
// verdict, meaningful only on a call-scoped interrupt; on an arc-wide one it
// is ignored.
type InterruptAnswer struct {
	Response   any
	IsRejected bool
}

// InterruptChanges alters an interrupt before it is written. Block raises no
// interrupt and settles the originating call as a tool error instead;
// AutoAnswer raises and answers in one write.
type InterruptChanges struct {
	Reason     *string
	Context    *any
	Block      *BlockRequest
	AutoAnswer *InterruptAnswer
}

// InterruptView is handed to AfterInterrupt.
type InterruptView struct {
	ID      string
	ArcID   string
	CallID  string
	Scope   InterruptScope
	Reason  string
	Context any
}

// InterruptResponseView is the human's answer, handed to the interrupt
// response hooks.
type InterruptResponseView struct {
	InterruptID string
	ArcID       string
	CallID      string
	Response    any
	IsRejected  bool
	Actor       users.Actor
	Seq         int // the response event's seq; zero on the answering side, before the write
}

// InterruptResponseChanges alters an answer before it is persisted. IsRejected
// is a verdict, meaningful only on a call-scoped interrupt; a rejection on an
// arc-wide one is ignored, since the fold would refuse the record. Refuse
// rejects the write entirely.
type InterruptResponseChanges struct {
	Response   *any
	IsRejected *bool
	Refuse     error
}

// ArcSummary is handed to AfterArcClosed.
type ArcSummary struct {
	ArcID               string
	ConclusionMessageID string
	Actor               *users.Actor
	Messages            int
	ToolCalls           int
	InputTokens         int
	OutputTokens        int
}
