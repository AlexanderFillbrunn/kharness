package agent

import (
	"errors"
	"fmt"

	"fillbrunn.ai/kube-agent/proto/pkg/history"
)

var (
	ErrInvalidArcState = errors.New("invalid arc state")
)

type arcState int

const (
	ArcStateNone       arcState = iota
	ArcStateLLM        arcState = iota
	ArcStateToolCalls  arcState = iota
	ArcStateTaskLaunch arcState = iota
	ArcStateDone       arcState = iota
	ArcStateClose      arcState = iota
)

func (s arcState) String() string {
	switch s {
	case ArcStateLLM:
		return "llm"
	case ArcStateToolCalls:
		return "tool_calls"
	case ArcStateTaskLaunch:
		return "task_launch"
	case ArcStateDone:
		return "done"
	case ArcStateClose:
		return "close"
	default:
		return "none"
	}
}

// deriveArcState decides what the agent does next from the arc alone, so a run
// picking up an arc another run left behind reaches the same decision.
//
// An arc closes when the agent has nothing synchronous left to do: a response
// with no tool calls closes it, whatever tasks are unfinished. A running task
// does not hold its arc open — its later events open arcs of their own. A task
// whose invoke is recorded but whose launch is not does hold it open one more
// round, so the start runs before the arc is answered or closed.
func (a Agent) deriveArcState(state *agentState) (arcState, error) {
	arc := state.arc
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

	for _, task := range state.history.TasksOfArc(arc.ID) {
		// A task with no launch result and no settle is a launch not recorded
		// yet: the response that invoked it has just been written.
		if task.LaunchSeq == nil && task.FinishedSeq == nil {
			return ArcStateTaskLaunch, nil
		}
	}

	last := arc.Responses[len(arc.Responses)-1]
	if len(last.ToolCalls) == 0 && !startedTasksIn(state.history.TasksOfArc(arc.ID), last.ID) {
		return ArcStateClose, nil
	}
	return ArcStateLLM, nil
}

// startedTasksIn reports whether the named message started any tasks: a reply
// that did gets one more model turn after its starts launch, the way a reply
// with tool calls gets one after they settle.
func startedTasksIn(tasks []*history.AsyncTask, messageID string) bool {
	for _, task := range tasks {
		if task.MessageID == messageID {
			return true
		}
	}
	return false
}

// knows reports whether a call by this name has somewhere to go: a registered
// tool, a kind of task, a configured interrupt, or one of the two tools tasks
// are addressed with.
func (a Agent) knows(name string) bool {
	if a.operation(name) != opTool {
		return true
	}
	_, ok := a.tools.Get(name)
	return ok
}
