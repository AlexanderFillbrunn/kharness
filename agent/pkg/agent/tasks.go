package agent

import (
	"fillbrunn.ai/kube-agent/proto/pkg/events"

	"fillbrunn.ai/kube-agent/proto/pkg/common"
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

// TaskSource is one source of task kinds: what the process holds, resolved by
// name the way a ToolProvider is, plus the controller that starts the source's
// kinds and reaches them once they are running. A definition can pick its
// tasks from the sources but never invent them, and a kind is never offered
// without the source that can start it. The set a definition selects — see
// SelectTasks — is itself a TaskSource, handed to the agent as it is.
type TaskSource interface {
	// All returns every kind the source holds, in a fixed order, so that two
	// agents built from the same sources carry the same tools.
	All() []common.TaskMetadata

	// Get returns one kind by name. It is also what makes a source a
	// common.MetadataSource, which is the sliver of it the ingress reads.
	Get(name string) (common.TaskMetadata, bool)

	// The controller starts a kind by name and reaches a running task by ref.
	common.TaskController
}



// taskToolSet offers a task source's kinds to the model, plus the two tools
// that address a task already running. The two are synthesized, since they are
// the same tools whatever tasks exist.
type taskToolSet struct {
	tasks TaskSource
}

// All returns one tool per kind of task, plus the two that address a task
// already running.
func (s taskToolSet) All() []*mcp.Tool {
	if s.tasks == nil || len(s.tasks.All()) == 0 {
		// With no tasks to start there is nothing to cancel or answer.
		return nil
	}
	kinds := s.tasks.All()
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
func classify(tasks TaskSource, name string) taskOperation {
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
