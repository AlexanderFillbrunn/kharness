package agent

import (
	"fillbrunn.ai/kube-agent/proto/pkg/common"
	"fillbrunn.ai/kube-agent/proto/pkg/events"
)

// The contracts the agent works against live in the common package, which the
// producers share; the aliases below keep this package's own code and its
// existing callers reading the same names. New code should name them through
// common directly.

type (
	ArcRef            = common.ArcRef
	TaskRef           = common.TaskRef
	EventPersistor    = common.EventPersistor
	EventSource       = common.EventSource
	EventSink         = common.EventSink
	Sink[E any]       = common.Sink[E]
	FuncSink[E any]   = common.FuncSink[E]
	CancelSource      = common.CancelSource
	TaskState         = common.TaskState
	TaskMetadata      = common.TaskMetadata
	NotificationClass = common.NotificationClass
	OverflowPolicy    = common.OverflowPolicy
	Starter           = common.Starter
	TaskController    = common.TaskController
)

var (
	NewEventID              = common.NewEventID
	MintIdentity            = common.MintIdentity
	AsAgent                 = common.AsAgent
	PublishEvents           = common.PublishEvents
	GuardCancellation       = common.GuardCancellation
	ErrCancelled            = common.ErrCancelled
	TaskStateOf             = common.TaskStateOf
	WithNotificationClasses = common.WithNotificationClasses
	WithInResultTrail       = common.WithInResultTrail
	WithEventLimit          = common.WithEventLimit
	WithOverflowPolicy      = common.WithOverflowPolicy
)

const (
	OverflowIgnore = common.OverflowIgnore
	OverflowFold   = common.OverflowFold
)

// SinkFromFunc is generic, so it is wrapped rather than aliased.
func SinkFromFunc[E any](f func(E) error) Sink[E] { return common.SinkFromFunc(f) }

// guardedArc is the loop's view of the same check the guard makes.
func guardedArc(evs []*events.Event) (ArcRef, bool) { return common.GuardedArc(evs) }

// The names of the two tools every task-carrying agent offers. They are
// protocol vocabulary defined in the events package, since the input tool's
// name appears in log-derived text the model reads.
const (
	CancelTaskTool       = events.CancelTaskToolName
	ProvideTaskInputTool = events.ProvideTaskInputToolName
)
