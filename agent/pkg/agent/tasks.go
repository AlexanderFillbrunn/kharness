package agent

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"fillbrunn.ai/kube-agent/pkg/events"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// This file holds the three parts an async task is made of and the tools they
// are offered to the model through. Nothing here records what is running. That
// is the log, which only the agent reads and writes.

// The names of the two tools every task-carrying agent offers. The agent
// routes on them, so both sides read the names from here.
const (
	CancelTaskTool       = "cancel_task"
	ProvideTaskInputTool = "provide_task_input"
)

// TaskRef names one running task well enough to reach it from outside this
// process. ID alone is the task's identity; the session is where its log lives.
type TaskRef struct {
	// SessionID is the session whose log holds the task. A report naming a
	// task reads that session's log, so the session has to travel with the ID.
	SessionID string

	// ID identifies the task in the log: the ID of its invoke event.
	ID string
}

// Starter launches one kind of task and returns as soon as the work is under
// way. It is handed the reference the task reports back under.
//
// An error means the launch failed, which is not the same as the task never
// having started: the work may be running anyway.
type Starter func(ctx context.Context, ref TaskRef, args any) error

// TaskController reaches a task that is already running. It says how to talk
// to a task, never what a task is doing, which is the log's to answer. What it
// returns is the fate of one call and nothing more: a refusal is not a status
// report, and a caller learns from it only that its own call got nowhere.
//
// Both methods may be called for a task that has already finished. An
// implementation that cannot find the task returns an error.
type TaskController interface {
	// Cancel asks a task to stop. It does not settle the task: what became of
	// work already under way is the runner's to report.
	Cancel(ctx context.Context, ref TaskRef, reason string) error

	// ProvideInput hands a waiting task what it asked for. requestID is the
	// request being answered, which a task mints before it asks and the
	// INPUT_REQUESTED event is written under.
	//
	// It must be refused unless the task is still waiting on that request, so
	// a second answer to one question is turned away rather than delivered
	// twice. Only the implementation can decide this. A caller decides from a
	// replay taken before it called, and two callers holding an answer to the
	// same question both read that the task was waiting.
	ProvideInput(ctx context.Context, ref TaskRef, requestID, input string) error
}

// NotificationClass is the declaration of one named class of notification a
// task kind may emit. Every notification carries its class in the payload, and
// the class decides how the event reaches the model and whether it is
// coalesced.
type NotificationClass struct {
	// Visibility is how events of this class reach the model: None, AsMessage
	// or InResult. AsMessage opens an arc, which costs a run, so the
	// declaration is the control on that cost.
	Visibility events.LLMVisibility

	// GroupWindow is the coalescing window for an AsMessage class: at most one
	// report per window is written. It is zero for classes that are never
	// coalesced, which is every class but AsMessage ones.
	GroupWindow time.Duration
}

// OverflowPolicy names what happens to an AsMessage-class notification past the
// task's event limit.
type OverflowPolicy string

const (
	// OverflowIgnore demotes a past-limit notification to None: it is logged
	// and opens no arc.
	OverflowIgnore OverflowPolicy = "IGNORE"
	// OverflowFold demotes a past-limit notification to InResult: it is folded
	// into the task's result message instead of opening an arc.
	OverflowFold OverflowPolicy = "FOLD"
)

// TaskDefinition is one kind of task: how the model is offered it, how one is
// started, and what its notifications mean.
type TaskDefinition struct {
	// Tool is the surface the model sees. Its name is the task's name and its
	// schema is what the arguments are checked against, exactly as for a tool.
	Tool *mcp.Tool

	// Start launches the task.
	Start Starter

	// Notifications declares each named class of notification the kind may
	// emit. A report naming a class the kind has not declared is refused at
	// the write path, not defaulted.
	Notifications map[string]NotificationClass

	// InResultTrail is how many InResult events the task's result message
	// carries, or -1 for the whole trail.
	InResultTrail int

	// EventLimit caps the arc-opening (AsMessage) notification events per
	// task. Zero means no limit. Past the limit a notification is demoted per
	// OverflowPolicy instead of opening an arc.
	EventLimit int

	// OverflowPolicy says what a past-limit AsMessage notification is demoted
	// to: ignored (None) or folded as InResult.
	OverflowPolicy OverflowPolicy
}

// withDefaults fills in the declarations a registration left unset.
func (d TaskDefinition) withDefaults() TaskDefinition {
	if d.InResultTrail == 0 {
		d.InResultTrail = -1
	}
	if d.OverflowPolicy == "" {
		d.OverflowPolicy = OverflowIgnore
	}
	if d.Notifications == nil {
		d.Notifications = map[string]NotificationClass{}
	}
	return d
}

// TaskOption configures one kind of task beyond its tool and starter.
type TaskOption func(*TaskDefinition)

// WithNotificationClasses declares the named notification classes the kind may
// emit.
func WithNotificationClasses(classes map[string]NotificationClass) TaskOption {
	return func(d *TaskDefinition) { d.Notifications = classes }
}

// WithInResultTrail sets how many InResult events the task's result message
// carries; -1 is the whole trail.
func WithInResultTrail(n int) TaskOption {
	return func(d *TaskDefinition) { d.InResultTrail = n }
}

// WithEventLimit caps the arc-opening notification events per task of this
// kind; zero means no limit.
func WithEventLimit(n int) TaskOption {
	return func(d *TaskDefinition) { d.EventLimit = n }
}

// WithOverflowPolicy declares what a past-limit AsMessage notification is
// demoted to.
func WithOverflowPolicy(policy OverflowPolicy) TaskOption {
	return func(d *TaskDefinition) { d.OverflowPolicy = policy }
}

// TaskRegistry holds the kinds of task an agent may start. It is written
// before any run begins, so one can be shared by every run in a process.
type TaskRegistry struct {
	tasks map[string]TaskDefinition
}

// NewTaskRegistry returns an empty registry ready to take task definitions.
func NewTaskRegistry() *TaskRegistry {
	return &TaskRegistry{tasks: map[string]TaskDefinition{}}
}

// RegisterTask adds one kind of task.
func (r *TaskRegistry) RegisterTask(tool *mcp.Tool, start Starter, options ...TaskOption) error {
	if tool == nil || tool.Name == "" {
		return errors.New("a task needs a tool with a name")
	}
	if start == nil {
		return fmt.Errorf("task %s has no way to start", tool.Name)
	}
	if _, exists := r.tasks[tool.Name]; exists {
		return fmt.Errorf("a task with name %s has already been registered", tool.Name)
	}
	if tool.Name == CancelTaskTool || tool.Name == ProvideTaskInputTool {
		return fmt.Errorf("task name %s is one of the tools every task registry offers", tool.Name)
	}

	definition := TaskDefinition{Tool: tool, Start: start}
	for _, option := range options {
		option(&definition)
	}
	for class, classDef := range definition.Notifications {
		if classDef.Visibility == "" || !classDef.Visibility.Valid() {
			return fmt.Errorf("notification class %s of task %s declares no visibility", class, tool.Name)
		}
	}
	r.tasks[tool.Name] = definition.withDefaults()
	return nil
}

// Get returns one kind of task by name.
func (r *TaskRegistry) Get(name string) (TaskDefinition, bool) {
	if r == nil {
		return TaskDefinition{}, false
	}
	definition, ok := r.tasks[name]
	return definition, ok
}

// Names returns every registered task name, in a fixed order.
func (r *TaskRegistry) Names() []string {
	if r == nil {
		return nil
	}
	names := make([]string, 0, len(r.tasks))
	for name := range r.tasks {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// All returns what the model is offered for these tasks: one tool per kind of
// task, plus the two that address a task already running. The two are
// synthesized, since they are the same tools whatever tasks exist.
func (r *TaskRegistry) All() []*mcp.Tool {
	if r == nil || len(r.tasks) == 0 {
		// With no tasks to start there is nothing to cancel or answer.
		return nil
	}

	tools := make([]*mcp.Tool, 0, len(r.tasks)+2)
	for _, name := range r.Names() {
		tools = append(tools, r.tasks[name].Tool)
	}
	return append(tools, cancelTaskTool(), provideTaskInputTool())
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
			"made ahead of the question is not an answer to it.",
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
func (r *TaskRegistry) classify(name string) taskOperation {
	if r == nil || len(r.tasks) == 0 {
		return opTool
	}
	switch name {
	case CancelTaskTool:
		return opCancel
	case ProvideTaskInputTool:
		return opProvideInput
	}
	if _, ok := r.tasks[name]; ok {
		return opStart
	}
	return opTool
}
