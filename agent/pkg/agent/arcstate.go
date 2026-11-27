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
	ArcStateNone      arcState = iota
	ArcStateLLM       arcState = iota
	ArcStateToolCalls arcState = iota
	ArcStateDone      arcState = iota
	ArcStateClose     arcState = iota
)

func (s arcState) String() string {
	switch s {
	case ArcStateLLM:
		return "llm"
	case ArcStateToolCalls:
		return "tool_calls"
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
