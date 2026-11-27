package agent

import (
	"context"
	"fmt"

	"fillbrunn.ai/kube-agent/proto/pkg/events"

	"fillbrunn.ai/kube-agent/proto/pkg/common"
	"fillbrunn.ai/kube-agent/proto/pkg/history"
	"fillbrunn.ai/kube-agent/proto/pkg/users"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// This file holds the model-facing half of async tasks: the source of what may
// be started, the two tools that address a task already running, how a call by
// name is classified, and the adapter that offers the kinds to the model. What
// a kind of task is lives in common as plain metadata, shared with the ingress;
// what is agent-only is the controller — process-bound, since it runs in the
// process that starts the task — held by the source the kind is picked from.
// The provider-shaped tool is generated from the metadata. What a running task
// reports goes through the ingress package, which is none of this package's
// business. Nothing here records what is running. That is the log, which only
// the agent reads and writes.

// TaskSource is one source of task kinds: the task-side ToolProvider. The
// metadata half (common.TaskSet, what a definition offers from it) is
// resolved by name; the controller half starts the source's kinds and
// reaches them once running. A definition can pick its tasks from the
// sources but never invent them, and a kind is never offered without the
// source that can start it. The set a definition selects — see SelectTasks —
// is itself a TaskSource.
type TaskSource interface {
	common.TaskSet

	// The controller starts a kind by name and reaches a running task by ref.
	common.TaskController
}

// taskToolSet offers a task metadata set's kinds to the model, plus the two
// tools that address a task already running. The two are synthesized, since
// they are the same tools whatever tasks exist.
type taskToolSet struct {
	meta common.TaskSet
}

// All returns one tool per kind of task, plus the two that address a task
// already running.
func (s taskToolSet) All() []*mcp.Tool {
	if s.meta == nil || len(s.meta.All()) == 0 {
		// With no tasks to start there is nothing to cancel or answer.
		return nil
	}
	kinds := s.meta.All()
	tools := make([]*mcp.Tool, 0, len(kinds)+2)
	for _, meta := range kinds {
		tools = append(tools, toolFor(meta))
	}
	return append(tools, cancelTaskTool(), provideTaskInputTool())
}

// toolFor materializes the model-facing tool of one kind of task from its
// metadata. The metadata is plain data, so a kind can be declared in a
// settings file; the provider-shaped tool is derived here, where the
// provider's types live, rather than stored in the shared declaration.
func toolFor(meta common.TaskMetadata) *mcp.Tool {
	return &mcp.Tool{
		Name:        meta.Name,
		Description: meta.Description,
		InputSchema: meta.InputSchema,
	}
}

func cancelTaskTool() *mcp.Tool {
	return &mcp.Tool{
		Name: events.CancelTaskToolName,
		Description: "Ask a running task to stop. The task reports what became of it, " +
			"so a cancelled task may still have done some of its work.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"task_id": map[string]any{
					"type":        "string",
					"description": "The ID of the task to stop, as given in the notes about it.",
				},
				"reason": map[string]any{
					"type":        "string",
					"description": "Why it is being stopped.",
				},
			},
			"required": []any{"task_id"},
		},
	}
}

func provideTaskInputTool() *mcp.Tool {
	return &mcp.Tool{
		Name: events.ProvideTaskInputToolName,
		Description: "Answer a question a running task has asked. Only for a question you have " +
			"been given: a task you expect to ask something has not asked it yet, and a guess " +
			"made ahead of the question is not an answer to it. You may answer the task directly, " +
			"or put the question to the user first and pass their answer on here.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"task_id": map[string]any{
					"type":        "string",
					"description": "The ID of the waiting task, as given in the notes about it.",
				},
				"input": map[string]any{
					"type":        "string",
					"description": "The answer to what the task asked.",
				},
			},
			"required": []any{"task_id", "input"},
		},
	}
}

// taskOperation is what a call turns out to be once its name has been looked
// up.
type taskOperation int

const (
	// opTool is an ordinary tool call, handled by the tool registry.
	opTool taskOperation = iota
	// opStart begins a task of the named kind.
	opStart
	// opCancel asks a running task to stop.
	opCancel
	// opProvideInput answers a task that is waiting.
	opProvideInput
	// opInterrupt raises an arc-wide interrupt: a configured tool that holds
	// the arc until a human answers.
	opInterrupt
)

// classify says what a call by this name is. A name that is neither a task's
// nor one of the two operations is an ordinary tool.
func classify(tasks common.TaskSet, name string) taskOperation {
	if tasks == nil || len(tasks.All()) == 0 {
		return opTool
	}
	switch name {
	case events.CancelTaskToolName:
		return opCancel
	case events.ProvideTaskInputToolName:
		return opProvideInput
	}
	if _, ok := tasks.Get(name); ok {
		return opStart
	}
	return opTool
}

// TaskArgs is what a task start is given: a JSON object shaped by the task
// kind's declared input schema. The agent passes it through opaquely — to the
// log and to the controller — so it stays a map; the hooks see it typed.
type TaskArgs map[string]any

// TaskCancelArgs are the arguments of a cancel call.
type TaskCancelArgs struct {
	TaskID string
	Reason string
}

// TaskInputArgs are the arguments of a call that answers a waiting task.
type TaskInputArgs struct {
	TaskID string
	Input  string
}

// taskArgsOf reads a start's args as the JSON object the task kind's input
// schema shapes. Args that are no object read as nil.
func taskArgsOf(args any) TaskArgs {
	if fields, ok := args.(map[string]any); ok {
		return TaskArgs(fields)
	}
	return nil
}

// taskCancelArgsOf reads a cancel's args. Fields the model left out read as
// empty; the call's own checks report them.
func taskCancelArgsOf(args any) TaskCancelArgs {
	id, _ := stringArg(args, "task_id")
	reason, _ := stringArg(args, "reason")
	return TaskCancelArgs{TaskID: id, Reason: reason}
}

// taskInputArgsOf reads an input call's args, likewise.
func taskInputArgsOf(args any) TaskInputArgs {
	id, _ := stringArg(args, "task_id")
	input, _ := stringArg(args, "input")
	return TaskInputArgs{TaskID: id, Input: input}
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

// launchTasks starts every task of the arc whose launch is not recorded yet:
// an invoke without a launch-result event. One round of the loop launches them in log
// order, then the arc goes back to the model or closes.
func (a Agent) launchTasks(ctx context.Context, state *agentState) error {
	for _, task := range state.history.TasksOfArc(state.arc.ID) {
		if task.LaunchSeq != nil {
			continue
		}
		if err := a.launchTask(ctx, task, state); err != nil {
			return err
		}
		// The arc may have been cancelled or held mid-launch.
		if state.arc.Status() != history.ArcStatusPending {
			return nil
		}
	}
	return nil
}

// launchTask runs one task's launch: the BeforeTaskStart decision, then the
// controller's Start, then the launch-result event recording how the launch went. The
// dispatch is written before Start so a run that stops mid-launch leaves the
// marker, and the launch-result event after Start so it says what became of the work.
func (a Agent) launchTask(ctx context.Context, task *history.AsyncTask, state *agentState) error {
	// A previous run handed this launch off and stopped before recording it.
	// The launch is not retried: whether the task took hold is not known.
	if task.DispatchedSeq != nil {
		return a.recordLaunch(ctx, task, nil,
			"a run stopped before it recorded the launch of this task; whether it took hold is not "+
				"known, so check before starting it again", nil, state)
	}

	d := a.decideLaunch(ctx, task, state)
	switch {
	case d.hookErr != nil:
		return a.recordLaunch(ctx, task, nil,
			fmt.Sprintf("the task policy failed, so the task was not started: %v", d.hookErr), nil, state)
	case d.block != nil:
		state.logger.WarnContext(ctx, "task start blocked by policy", "task", task.ID, "kind", task.TaskName)
		// The guardrail event names the task by its invoke ID; the
		// launch-result event written beside it settles the task.
		return a.recordLaunch(ctx, task, nil, d.block.Message, nil, state,
			&events.Event{
				Typ:           events.GuardrailBlockedEventType,
				CorrelationID: &state.arc.ID,
				Payload:       events.GuardrailBlockedPayload{CausationID: task.ID, Message: d.block.Message},
			})
	case d.ask != nil:
		return a.raiseTaskAsk(ctx, task, d.ask, state)
	case d.answer != nil:
		// An answer settles the request without the task starting: nothing
		// ran, so the launch result says so — and the answer is recorded as
		// the task's result, in the same batch, the way a tool call's Answer
		// becomes its tool result. The projection renders it as the start
		// call's result.
		outcome := events.AsyncTaskOutcomeSuccess
		if d.answer.IsError {
			outcome = events.AsyncTaskOutcomeFailed
		}
		return a.recordLaunch(ctx, task, nil,
			"the task start was answered by policy without the task starting",
			&events.TaskAnswer{Result: d.answer.Result, IsError: d.answer.IsError}, state,
			&events.Event{
				Typ:           events.AsyncTaskResultEventType,
				CorrelationID: &task.ID,
				Payload: events.AsyncTaskResultPayload{
					CausationID:   task.ID,
					Result:        d.answer.Result,
					Outcome:       outcome,
					LLMVisibility: events.LLMVisibilityNone,
				},
			})
	}

	controller := a.controllerFor(state)
	if controller == nil {
		return a.recordLaunch(ctx, task, nil,
			"this run has no task controller bound to it, so the task could not be started", nil, state)
	}
	if _, err := a.persistAndFold(ctx, state, &events.Event{
		Typ:           events.ToolDispatchedEventType,
		CorrelationID: &state.arc.ID,
		Payload:       events.ToolDispatchedPayload{CausationID: task.ID},
	}); err != nil {
		return err
	}

	// The session is filled in by the task source the run was given, which
	// the worker binds to the session it is working; the agent addresses the
	// task by ID alone. Args the decision pass overrode are the ones that run;
	// the log keeps the ones the model asked for.
	args := task.Args
	if d.override != nil {
		args = d.override
	}
	ref := common.TaskRef{ID: task.ID}
	state.logger.DebugContext(ctx, "task launch handed to controller", "task", task.ID, "kind", task.TaskName)
	launch, err := controller.Start(ctx, task.TaskName, ref, args)
	if err != nil {
		state.logger.WarnContext(ctx, "task launch failed", "task", task.ID, "kind", task.TaskName, "error", err)
	}
	reason := ""
	if err != nil {
		reason = err.Error()
	}
	return a.recordLaunch(ctx, task, launch, reason, nil, state)
}

// recordLaunch writes the one launch-result event a launch produces: the launch
// verdict, the external reference the runner assigned (the task's own ID
// where the runner assigned none), and the runner's raw response. An empty
// reason means the launch succeeded; any other reason — a failure, a policy
// block, a policy answer, a handed-off launch never recorded — means it did
// not, and settles the task: ASYNC_TASK_RESULT is reserved for work that
// actually started, and no result event is written for a launch that did not.
func (a Agent) recordLaunch(ctx context.Context, task *history.AsyncTask, launch *common.TaskLaunch, reason string, answer *events.TaskAnswer, state *agentState, extra ...*events.Event) error {
	externalRef := task.ID
	var response any
	if launch != nil {
		if launch.ExternalRef != "" {
			externalRef = launch.ExternalRef
		}
		response = launch.Response
	}
	launchResult := &events.Event{
		Typ:           events.AsyncTaskLaunchResultEventType,
		CorrelationID: &task.ID,
		Payload: events.AsyncTaskLaunchResultPayload{
			CausationID:   task.ID,
			ExternalRef:   externalRef,
			Launched:      reason == "",
			Launch:        response,
			Error:         reason,
			Answer:        answer,
			LLMVisibility: events.LLMVisibilityNone,
		},
	}
	if _, err := a.persistAndFold(ctx, state, append([]*events.Event{launchResult}, extra...)...); err != nil {
		return err
	}
	if reason == "" {
		state.logger.InfoContext(ctx, "task launched", "task", task.ID, "kind", task.TaskName, "external_ref", externalRef)
	} else {
		state.logger.InfoContext(ctx, "task not launched", "task", task.ID, "kind", task.TaskName, "reason", reason)
	}
	return nil
}

// launchDecision is what the BeforeTaskStart pass over one unlaunched task
// returned.
type launchDecision struct {
	hookErr  error
	block    *BlockRequest
	ask      *AskRequest
	answer   *Answer
	override map[string]any
}

// decideLaunch runs BeforeTaskStart over one unlaunched task. A task whose
// start was already asked of a human and answered runs without asking again;
// a rejection refuses the start.
func (a Agent) decideLaunch(ctx context.Context, task *history.AsyncTask, state *agentState) launchDecision {
	if task.Interrupt != nil && task.Interrupt.ResolvedSeq != nil {
		if task.Interrupt.Status == history.InterruptRejected {
			return launchDecision{block: &BlockRequest{Message: "a human refused this task start"}}
		}
		return launchDecision{}
	}
	hook := state.hooks.BeforeTaskStart
	if hook == nil {
		return launchDecision{}
	}
	eff, err := hook(ctx, TaskStartView{
		TaskName: task.TaskName,
		Args:     taskArgsOf(task.Args),
		CallMeta: CallMeta{CallID: task.ID, MessageID: task.MessageID, ArcID: state.arc.ID},
	})
	if err != nil {
		return launchDecision{hookErr: err}
	}
	if err := eff.validate(); err != nil {
		return launchDecision{hookErr: err}
	}
	d := launchDecision{}
	switch {
	case eff.Run:
	case eff.Block != nil:
		d.block = eff.Block
	case eff.Ask != nil:
		d.ask = eff.Ask
	case eff.Answer != nil:
		d.answer = eff.Answer
	}
	if eff.Args != nil {
		d.override = map[string]any(*eff.Args)
	}
	return d
}

// raiseTaskAsk holds a task's start for a human: one INTERRUPT causated to the
// task, which the fold hangs off the task and the arc, so the arc reads as
// interrupted until the answer lands. A BeforeInterrupt block or policy
// failure refuses the start instead, recorded as a failed launch.
func (a Agent) raiseTaskAsk(ctx context.Context, task *history.AsyncTask, ask *AskRequest, state *agentState) error {
	req := InterruptRequest{ToolName: task.TaskName, CallID: task.ID, ArcID: state.arc.ID, Reason: ask.Reason, Context: ask.Context}
	changes := InterruptChanges{}
	if hook := state.hooks.BeforeInterrupt; hook != nil {
		c, err := hook(ctx, req)
		if err != nil {
			return a.recordLaunch(ctx, task, nil, fmt.Sprintf("the interrupt policy failed, so the task was not started: %v", err), nil, state)
		}
		changes = c
	}
	if changes.Block != nil {
		return a.recordLaunch(ctx, task, nil, changes.Block.Message, nil, state)
	}
	reason, context := req.Reason, req.Context
	if changes.Reason != nil {
		reason = *changes.Reason
	}
	if changes.Context != nil {
		context = *changes.Context
	}
	interrupt := &events.Event{
		ID:            common.NewEventID(),
		Typ:           events.InterruptEventType,
		CorrelationID: &state.arc.ID,
		Payload:       events.InterruptPayload{CausationID: task.ID, Reason: reason, Context: context},
	}
	evs := []*events.Event{interrupt}
	if changes.AutoAnswer != nil {
		answer := *changes.AutoAnswer
		evs = append(evs, &events.Event{
			Typ:           events.InterruptResponseEventType,
			CorrelationID: &state.arc.ID,
			Payload: events.InterruptResponsePayload{
				CausationID: interrupt.ID,
				Response:    answer.Response,
				IsRejected:  answer.IsRejected,
			},
		})
	}
	stored, err := a.persistAndFold(ctx, state, evs...)
	if err != nil {
		return err
	}
	state.logger.InfoContext(ctx, "task start held for a human", "task", task.ID, "interrupt", stored[0].ID)
	notifyInterruptRaised(ctx, state, stored)
	return nil
}

// controllerFor returns the controller a launch or task operation goes
// through: the run's, or — where the agent's task set is itself a source, as
// one built with an option like WithA2A — the agent's own.
func (a Agent) controllerFor(state *agentState) common.TaskController {
	if state.tasks != nil {
		return state.tasks
	}
	if src, ok := a.tasks.(common.TaskController); ok {
		return src
	}
	return nil
}

// taskActor is the actor a task's events carry: the one whose message opened
// the arc the task started in.
func taskActor(arc *history.Arc) users.Actor {
	if arc.Actor != nil {
		return *arc.Actor
	}
	return users.Actor{}
}



// cancelTask asks a running task to stop. The cancel is written as a CANCEL
// event whose correlation ID is the task ID, which is what settles the task in
// the log, with who cancelled it and when; the runner is then reached through
// the controller. What became of work already under way is the runner's to
// report.
func (a Agent) cancelTask(ctx context.Context, call *history.ToolCall, state *agentState) error {
	if state.tasks == nil {
		return a.noController(ctx, call, state)
	}
	if err := a.recordDispatch(ctx, call, state); err != nil {
		return err
	}

	cargs := taskCancelArgsOf(call.Args)
	task, failure := a.addressedTask(cargs.TaskID, state)
	if failure != "" {
		return a.settleCall(ctx, call, ToolOutcome{Result: failure, IsError: true}, state)
	}
	if task.FinishedSeq != nil {
		return a.settleCall(ctx, call, ToolOutcome{Result: fmt.Sprintf(
			"task %s had already finished (%s), so there was nothing to stop.",
			task.TaskName, task.Status), IsError: true}, state)
	}

	reason := cargs.Reason
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
func (a Agent) provideTaskInput(ctx context.Context, call *history.ToolCall, args any, state *agentState) error {
	if state.tasks == nil {
		return a.noController(ctx, call, state)
	}
	if err := a.recordDispatch(ctx, call, state); err != nil {
		return err
	}

	iargs := taskInputArgsOf(args)
	task, failure := a.addressedTask(iargs.TaskID, state)
	if failure != "" {
		return a.settleCall(ctx, call, ToolOutcome{Result: failure, IsError: true}, state)
	}
	input := iargs.Input
	if input == "" {
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
			"task %s was given the input and is running again.", task.TaskName)}, state.arc.ID),
	)
	return err
}

// addressedTask finds the task a call names in the session's task table, or
// says why it could not. The reason is written for the model to act on.
func (a Agent) addressedTask(id string, state *agentState) (*history.AsyncTask, string) {
	if id == "" {
		return nil, "no task id was given"
	}
	if task, ok := state.history.Tasks[id]; ok {
		return task, ""
	}
	// A task outside this session is as absent as one never started.
	return nil, fmt.Sprintf("no task with id %s is part of this conversation", id)
}

// noController settles a task tool call on a run that has no controller
// bound: the tools are offered from the agent's metadata, but only a
// controller can start or reach a task.
func (a Agent) noController(ctx context.Context, call *history.ToolCall, state *agentState) error {
	return a.settleCall(ctx, call, ToolOutcome{
		Result:  "this run has no task controller bound to it, so the task could not be reached",
		IsError: true,
	}, state)
}
