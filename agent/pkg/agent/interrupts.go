package agent

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"fillbrunn.ai/kube-agent/proto/pkg/events"
	"fillbrunn.ai/kube-agent/proto/pkg/history"
)

// InterruptSpec is one configured interrupt: a tool the model may call to
// hold the arc until a human answers. The request schema is the tool's input
// schema verbatim; the call's arguments land in the interrupt's Context, and
// the description is what the human is asked, in words.
//
// There is deliberately no response schema: the answer is information the
// model reads as a note, never something the agent validates.
type InterruptSpec struct {
	Name        string `json:"name"`
	Description string `json:"description"`

	// RequestSchema is the tool's input schema, a JSON schema as the MCP
	// tools carry them.
	RequestSchema any `json:"request_schema,omitempty"`
}

// WithInterrupts gives an agent the interrupts its definition names. Each
// becomes a tool that raises an arc-wide interrupt when called.
func WithInterrupts(specs []InterruptSpec) AgentOption {
	return func(a *Agent) error {
		for i, spec := range specs {
			if spec.Name == "" {
				return errors.New("an interrupt needs a name")
			}
			if slices.ContainsFunc(specs[:i], func(prev InterruptSpec) bool { return prev.Name == spec.Name }) {
				return fmt.Errorf("interrupt %s is declared twice", spec.Name)
			}
		}
		a.interruptSpecs = specs
		return nil
	}
}

// interruptToolSet offers one tool per configured interrupt. The tools are
// synthesized, since they are the same whatever the interrupt asks about.
type interruptToolSet struct {
	specs []InterruptSpec
}

// All returns one tool per interrupt, in declaration order.
func (s interruptToolSet) All() []*mcp.Tool {
	if len(s.specs) == 0 {
		return nil
	}
	tools := make([]*mcp.Tool, 0, len(s.specs))
	for _, spec := range s.specs {
		tools = append(tools, &mcp.Tool{
			Name:        spec.Name,
			Description: spec.Description,
			InputSchema: spec.RequestSchema,
		})
	}
	return tools
}

// operation says what a call by this name is: a configured interrupt, a task
// operation, or an ordinary tool.
func (a Agent) operation(name string) taskOperation {
	if slices.ContainsFunc(a.interruptSpecs, func(spec InterruptSpec) bool { return spec.Name == name }) {
		return opInterrupt
	}
	return classify(a.tasks, name)
}

// raiseInterrupt holds the arc on an arc-wide interrupt and settles the call
// that asked for it, in one write: the interrupt and the call's result go
// together, so a worker that dies between them leaves neither.
//
// No dispatch is recorded — nothing was handed to a tool — and the run ends
// at its next step, because the arc reads INTERRUPTED while the interrupt is
// pending. The answer reaches the model as a note when a later run resumes
// the arc.
func (a Agent) raiseInterrupt(ctx context.Context, call *history.ToolCall, state *agentState) error {
	spec := a.interruptSpec(call.ToolName)
	req := InterruptRequest{
		ToolName: call.ToolName,
		ArcID:    state.arc.ID,
		Reason:   spec.Description,
		Context:  call.Args,
	}
	// BeforeInterrupt governs the raise: it may change the ask, block it —
	// which settles the call as a tool error instead — or answer it at once.
	evs, settle, err := a.prepareInterrupt(ctx, req, call, state)
	if err != nil {
		state.logger.WarnContext(ctx, "the interrupt policy failed, so the interrupt was not raised", "tool", call.ToolName, "error", err)
		// Fail closed: no interrupt, the call settles as an error.
		settle = callResult(call, ToolOutcome{
			IsError: true,
			Result:  fmt.Sprintf("the interrupt policy failed, so the interrupt was not raised: %v", err),
		}, state.arc.ID)
		evs = nil
	}
	if settle != nil {
		_, err := a.persistAndFold(ctx, state, settle)
		return err
	}
	reason := evs[0].Payload.(events.InterruptPayload).Reason
	result := callResult(call, ToolOutcome{Result: fmt.Sprintf(
		"interrupt %s raised: %s. The arc is held until a human answers, and the answer "+
			"reaches you as a note when the arc resumes.",
		evs[0].ID, reason)}, state.arc.ID)
	stored, err := a.persistAndFold(ctx, state, append(evs, result)...)
	if err != nil {
		return err
	}
	state.logger.InfoContext(ctx, "interrupt raised, arc held", "interrupt", stored[0].ID, "tool", call.ToolName)
	notifyInterruptRaised(ctx, state, stored)
	return nil
}

// interruptSpec returns the spec a call's name was configured with.
func (a Agent) interruptSpec(name string) InterruptSpec {
	for _, spec := range a.interruptSpecs {
		if spec.Name == name {
			return spec
		}
	}
	return InterruptSpec{}
}
