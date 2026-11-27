package common

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"fillbrunn.ai/kube-agent/proto/pkg/events"
)

// This file holds the declaration half of async tasks: what a kind of task is
// and how one is reached once running. The behavior — writing reports into
// the log, coalescing notifications — lives in the ingress package, which is
// the producer side; this file is the shared vocabulary both sides compile
// against.
//
// It is deliberately free of any model-provider types: the metadata here is
// plain data, so a task kind can be declared in a settings file as easily as
// in Go code. The one consumer that needs a provider-shaped tool — the agent,
// when it offers the kind to the model — generates it from this metadata on
// the fly rather than storing it here.

// InSession binds a controller to one session: a ref it is handed with no
// session is filled in with it, the same keep-if-set convention as AsSession
// and AsAgent on the write path. A run is given one of these rather than its
// session id — the work item names the session, and nothing between the
// worker and the controller has to carry it.
func InSession(sessionID string, c TaskController) TaskController {
	return &sessionController{sessionID: sessionID, controller: c}
}

type sessionController struct {
	sessionID  string
	controller TaskController
}

func (s *sessionController) fill(ref TaskRef) TaskRef {
	if ref.SessionID == "" {
		ref.SessionID = s.sessionID
	}
	return ref
}

func (s *sessionController) Start(ctx context.Context, name string, ref TaskRef, args any) (*TaskLaunch, error) {
	return s.controller.Start(ctx, name, s.fill(ref), args)
}

func (s *sessionController) Cancel(ctx context.Context, ref TaskRef, reason string) error {
	return s.controller.Cancel(ctx, s.fill(ref), reason)
}

func (s *sessionController) ProvideInput(ctx context.Context, ref TaskRef, requestID, input string) error {
	return s.controller.ProvideInput(ctx, s.fill(ref), requestID, input)
}

// TaskLaunch is what a controller's Start returned for the invocation: what
// the runner sent back when the work was handed to it. It is recorded in the
// task's ASYNC_TASK_LAUNCH_RESULT event, not shown to the model.
type TaskLaunch struct {
	// ExternalRef is the ID the runner assigned the task, for a runner that
	// assigns its own. Empty means the runner took the caller's ref.ID as
	// the task's ID, and the writer falls back to it.
	ExternalRef string

	// Response is the runner's raw reply to the invocation, kept as it came.
	Response any
}

// TaskController starts tasks and reaches the ones already running. It says
// how to talk to a task, never what a task is doing, which is the log's to
// answer. A controller is process-bound — it runs in the process that starts
// the task — so it is registered on the agent's side, not carried in the
// shared metadata.
//
// What Start returns is the fate of the launch: an error means the launch
// failed, which is not the same as the task never having started — the work
// may be running anyway. What Cancel and ProvideInput return is the fate of
// one call and nothing more: a refusal is not a status report, and a caller
// learns from it only that its own call got nowhere.
//
// Cancel and ProvideInput may be called for a task that has already finished.
// An implementation that cannot find the task returns an error.
type TaskController interface {
	// Start launches one kind of task, named as the model named it, and
	// returns as soon as the work is under way. It is handed the reference
	// the task reports back under, and returns what the runner said about
	// the launch — an error means the launch failed, which is not the same
	// as the task never having started, since the work may be running
	// anyway.
	Start(ctx context.Context, name string, ref TaskRef, args any) (*TaskLaunch, error)

	// Cancel asks a task to stop. It does not settle the task: what became of
	// work under way is the runner's to report.
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
	Visibility events.LLMVisibility `json:"visibility"`

	// GroupWindow is the coalescing window for an AsMessage class: at most one
	// report per window is written. It is zero for classes that are never
	// coalesced, which is every class but AsMessage ones.
	GroupWindow time.Duration `json:"group_window,omitempty"`
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

// TaskMetadata is what a kind of task is, independent of any process: its
// name, how it is described to the model, what it accepts, and what its
// notifications mean. It is the part of a task declaration every package can
// read — the ingress resolves reports against it, the agent derives its tool
// surface and result trail from it.
type TaskMetadata struct {
	// Name is the task's name: the tool name the model calls and the key the
	// registry holds it under.
	Name string `json:"name"`

	// Description is what the model is told the task does.
	Description string `json:"description,omitempty"`

	// InputSchema is what the task's arguments are checked against, in the
	// shape the provider's tool schema uses. Kept as plain data so a kind can
	// be declared in a settings file; the agent materializes a provider tool
	// from it.
	InputSchema any `json:"input_schema,omitempty"`

	// Notifications declares each named class of notification the kind may
	// emit. A report naming a class the kind has not declared is refused at
	// the write path, not defaulted.
	Notifications map[string]NotificationClass `json:"notifications,omitempty"`

	// InResultTrail is how many InResult events the task's result message
	// carries, or -1 for the whole trail.
	InResultTrail int `json:"in_result_trail,omitempty"`

	// InResultTrailChars caps how many characters the InResult trail in the
	// task's result message may carry, counting the entries joined with
	// newlines. The oldest entries are dropped first; a single entry that
	// alone exceeds the cap is truncated to it. Zero or negative means no
	// cap.
	InResultTrailChars int `json:"in_result_trail_chars,omitempty"`

	// InResultTrailHeader says whether each InResult trail entry in the
	// task's result message carries a header line with the entry's 1-based
	// index in the task's trail and its timestamp.
	InResultTrailHeader bool `json:"in_result_trail_header,omitempty"`

	// EventLimit caps the arc-opening (AsMessage) notification events per
	// task. Zero means no limit. Past the limit a notification is demoted per
	// OverflowPolicy instead of opening an arc.
	EventLimit int `json:"event_limit,omitempty"`

	// OverflowPolicy says what a past-limit AsMessage notification is demoted
	// to: ignored (None) or folded as InResult.
	OverflowPolicy OverflowPolicy `json:"overflow_policy,omitempty"`
}

// withDefaults fills in the declarations a registration left unset.
func (m TaskMetadata) withDefaults() TaskMetadata {
	if m.InResultTrail == 0 {
		m.InResultTrail = -1
	}
	if m.OverflowPolicy == "" {
		m.OverflowPolicy = OverflowIgnore
	}
	if m.Notifications == nil {
		m.Notifications = map[string]NotificationClass{}
	}
	return m
}

// TaskOption configures one kind of task's metadata at registration.
type TaskOption func(*TaskMetadata)

// WithNotificationClasses declares the named notification classes the kind may
// emit.
func WithNotificationClasses(classes map[string]NotificationClass) TaskOption {
	return func(m *TaskMetadata) { m.Notifications = classes }
}

// WithInResultTrail sets how many InResult events the task's result message
// carries; -1 is the whole trail.
func WithInResultTrail(n int) TaskOption {
	return func(m *TaskMetadata) { m.InResultTrail = n }
}

// WithInResultTrailChars caps how many characters the InResult trail in the
// task's result message carries; zero or negative means no cap.
func WithInResultTrailChars(n int) TaskOption {
	return func(m *TaskMetadata) { m.InResultTrailChars = n }
}

// WithInResultTrailHeader gives each InResult trail entry in the task's
// result message a header line with its index in the trail and its timestamp.
func WithInResultTrailHeader() TaskOption {
	return func(m *TaskMetadata) { m.InResultTrailHeader = true }
}

// WithEventLimit caps the arc-opening notification events per task of this
// kind; zero means no limit.
func WithEventLimit(n int) TaskOption {
	return func(m *TaskMetadata) { m.EventLimit = n }
}

// WithOverflowPolicy declares what a past-limit AsMessage notification is
// demoted to.
func WithOverflowPolicy(policy OverflowPolicy) TaskOption {
	return func(m *TaskMetadata) { m.OverflowPolicy = policy }
}

// TaskSet is the metadata of a set of task kinds — the task-side ToolSet:
// what a caller offers from it, in a fixed order and by name. Running a task
// is not here; that is the TaskController's, and a TaskSource is the two
// together.
type TaskSet interface {
	// All returns every kind in the set, in a fixed order, so that two
	// callers built from the same set offer the same kinds in the same order.
	All() []TaskMetadata

	// Get returns one kind by name.
	Get(name string) (TaskMetadata, bool)
}

// MetadataSource answers task metadata by name. It is the sliver of a TaskSet
// the ingress needs: the producer side resolves reports against the
// declarations and never starts anything.
type MetadataSource interface {
	Get(name string) (TaskMetadata, bool)
}

// TaskRegistry holds the metadata of every kind of task, keyed by name. It is
// written before any run begins, so one can be shared by every package in a
// process — the agent and the ingress each hold a reference to the same one.
type TaskRegistry struct {
	tasks map[string]TaskMetadata
}

// NewTaskRegistry returns an empty registry ready to take task metadata.
func NewTaskRegistry() *TaskRegistry {
	return &TaskRegistry{tasks: map[string]TaskMetadata{}}
}

// RegisterTask adds one kind of task's metadata.
func (r *TaskRegistry) RegisterTask(meta TaskMetadata, options ...TaskOption) error {
	if meta.Name == "" {
		return errors.New("a task needs a name")
	}
	if _, exists := r.tasks[meta.Name]; exists {
		return fmt.Errorf("a task with name %s has already been registered", meta.Name)
	}
	if meta.Name == events.CancelTaskToolName || meta.Name == events.ProvideTaskInputToolName {
		return fmt.Errorf("task name %s is one of the tools every task registry offers", meta.Name)
	}

	for _, option := range options {
		option(&meta)
	}
	for class, classDef := range meta.Notifications {
		if classDef.Visibility == "" || !classDef.Visibility.Valid() {
			return fmt.Errorf("notification class %s of task %s declares no visibility", class, meta.Name)
		}
	}
	r.tasks[meta.Name] = meta.withDefaults()
	return nil
}

// Get returns one kind of task's metadata by name.
func (r *TaskRegistry) Get(name string) (TaskMetadata, bool) {
	if r == nil {
		return TaskMetadata{}, false
	}
	meta, ok := r.tasks[name]
	return meta, ok
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

// All returns every registered kind's metadata, in the fixed order Names
// gives, so a registry is a TaskSet as it stands.
func (r *TaskRegistry) All() []TaskMetadata {
	if r == nil {
		return nil
	}
	names := r.Names()
	kinds := make([]TaskMetadata, 0, len(names))
	for _, name := range names {
		if meta, ok := r.tasks[name]; ok {
			kinds = append(kinds, meta)
		}
	}
	return kinds
}
