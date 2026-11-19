package agent

import (
	"fmt"

	"fillbrunn.ai/kube-agent/proto/pkg/common"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// This file holds the model-facing half of async tasks: the registry of what
// may be started, the two tools that address a task already running, how a
// call by name is classified, and the adapter that offers the kinds to the
// model. What a kind of task is lives in common as plain metadata, shared
// with the ingress; what is agent-only is the starter — process-bound, since
// it runs in the process that starts the task — and the provider-shaped tool
// generated from the metadata. What a running task reports goes through the
// ingress package, which is none of this package's business. Nothing here
// records what is running. That is the log, which only the agent reads and
// writes.

// TaskRegistry holds the kinds of task an agent may start: their metadata in
// the shared registry, and the starter that launches each one here. The two
// halves are registered together, so a kind is never declared without a way
// to start it, nor started without a declaration.
type TaskRegistry struct {
	meta     *common.TaskRegistry
	starters map[string]common.Starter
}

// NewTaskRegistry returns an empty registry ready to take task kinds.
func NewTaskRegistry() *TaskRegistry {
	return &TaskRegistry{meta: common.NewTaskRegistry(), starters: map[string]common.Starter{}}
}

// RegisterTask adds one kind of task: its metadata to the shared registry and
// its starter here. A kind with no starter could never be started, so it is
// refused before anything is registered.
func (r *TaskRegistry) RegisterTask(meta common.TaskMetadata, start common.Starter, options ...common.TaskOption) error {
	if start == nil {
		return fmt.Errorf("task %s has no way to start", meta.Name)
	}
	if err := r.meta.RegisterTask(meta, options...); err != nil {
		return err
	}
	r.starters[meta.Name] = start
	return nil
}

// Get returns one kind of task's metadata by name. It is also what makes the
// registry a common.MetadataSource, which is the sliver of it the ingress
// reads.
func (r *TaskRegistry) Get(name string) (common.TaskMetadata, bool) {
	if r == nil {
		return common.TaskMetadata{}, false
	}
	return r.meta.Get(name)
}

// Names returns every registered task name, in a fixed order.
func (r *TaskRegistry) Names() []string {
	if r == nil {
		return nil
	}
	return r.meta.Names()
}

// starter returns the launcher registered for one kind of task.
func (r *TaskRegistry) starter(name string) (common.Starter, bool) {
	if r == nil {
		return nil, false
	}
	start, ok := r.starters[name]
	return start, ok
}

// taskToolSet offers a task registry's kinds to the model, plus the two tools
// that address a task already running. The two are synthesized, since they are
// the same tools whatever tasks exist.
type taskToolSet struct {
	registry *TaskRegistry
}

// All returns one tool per kind of task, plus the two that address a task
// already running.
func (s taskToolSet) All() []*mcp.Tool {
	if s.registry == nil || len(s.registry.Names()) == 0 {
		// With no tasks to start there is nothing to cancel or answer.
		return nil
	}
	names := s.registry.Names()
	tools := make([]*mcp.Tool, 0, len(names)+2)
	for _, name := range names {
		meta, _ := s.registry.Get(name)
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
		Name: CancelTaskTool,
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
		Name: ProvideTaskInputTool,
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
)

// classify says what a call by this name is. A name that is neither a task's
// nor one of the two operations is an ordinary tool.
func classify(tasks *TaskRegistry, name string) taskOperation {
	if tasks == nil || len(tasks.Names()) == 0 {
		return opTool
	}
	switch name {
	case CancelTaskTool:
		return opCancel
	case ProvideTaskInputTool:
		return opProvideInput
	}
	if _, ok := tasks.Get(name); ok {
		return opStart
	}
	return opTool
}
