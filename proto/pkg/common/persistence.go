package common

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"fillbrunn.ai/kube-agent/proto/pkg/events"
	"fillbrunn.ai/kube-agent/proto/pkg/users"
)

type Sink[E any] interface {
	Consume(ev E) error
}

type FuncSink[E any] struct {
	fn func(i E) error
}

func (fs FuncSink[E]) Consume(i E) error {
	return fs.fn(i)
}

func SinkFromFunc[E any](f func(i E) error) Sink[E] {
	return FuncSink[E]{
		fn: f,
	}
}

type EventPersistor interface {
	// Persist appends the events in the order given, all of them or none. A
	// caller that writes two events to say one thing must not have one of
	// them survive without the other.
	Persist(ctx context.Context, evs ...*events.Event) ([]*events.Event, error)
}

type EventSource interface {
	// GetEventsForArc returns the events one arc is run from: everything in
	// its session up to and including the arc's opening event, plus everything
	// correlated to the arc or to a task afterwards.
	GetEventsForArc(ctx context.Context, ref ArcRef) ([]*events.Event, error)

	// GetEventsForSession returns the session's whole log in the order it was
	// written, which is what the task table and the report validation are
	// built from.
	GetEventsForSession(ctx context.Context, sessionID string) ([]*events.Event, error)

	// TaskState reads what the log says about one task — whether the session
	// holds it, its kind and actor, and whether it has settled — with a
	// targeted scan rather than a fold of the whole session. It is the
	// admission check for task-emitted writes.
	TaskState(ctx context.Context, sessionID, taskID string) (*TaskState, error)
}

// TaskState is what the log says about one async task, read without folding
// the session.
type TaskState struct {
	// Exists reports whether the session holds a task with this ID, i.e. an
	// invoke event under it.
	Exists bool

	// TaskName is the kind the task was invoked as.
	TaskName string

	// Actor is the one the invoke recorded; nil where none was.
	Actor *users.Actor

	// Settled reports whether the task has a result or a task-addressed
	// cancel, after which nothing more may be reported about it.
	Settled bool

	// Status is the task's lifecycle state, meaningful once Exists is set.
	Status events.TaskStatus
}

// TaskStateOf reads what a log says about one task of one session. Stores
// implement EventSource.TaskState as their read plus this, so the scan lives
// in one place. The log must be in write order.
func TaskStateOf(sessionID, taskID string, log []*events.Event) *TaskState {
	state := &TaskState{Status: events.TaskStatusPending}
	for _, ev := range log {
		if ev.SessionID != sessionID {
			continue
		}
		switch ev.Typ {
		case events.AsyncTaskInvokeEventType:
			if ev.ID != taskID {
				continue
			}
			state.Exists = true
			if payload, ok := ev.Payload.(events.AsyncTaskInvokePayload); ok {
				state.TaskName = payload.TaskName
				if payload.Actor.UserID != "" {
					actor := payload.Actor
					state.Actor = &actor
				}
			}
			if state.Actor == nil && ev.Actor.UserID != "" {
				actor := ev.Actor
				state.Actor = &actor
			}
		case events.AsyncTaskResultEventType:
			if payload, ok := ev.Payload.(events.AsyncTaskResultPayload); ok && payload.CausationID == taskID {
				state.Settled = true
				state.Status = statusOfOutcome(payload.Outcome)
			}
		case events.CancelEventType:
			if ev.CorrelationID != nil && *ev.CorrelationID == taskID {
				state.Settled = true
				state.Status = events.TaskStatusCancelled
			}
		}
	}
	return state
}

// statusOfOutcome maps a reported outcome to the task status the fold derives
// from the same payload.
func statusOfOutcome(outcome events.AsyncTaskOutcome) events.TaskStatus {
	switch outcome {
	case events.AsyncTaskOutcomeFailed:
		return events.TaskStatusFailed
	case events.AsyncTaskOutcomeCancelled:
		return events.TaskStatusCancelled
	default:
		return events.TaskStatusSuccess
	}
}

// EventSink is told about events once they have been written. It is the
// notification half of a write and never part of it.
//
// Consume returns nothing, so a sink cannot fail a write. That is the point
// rather than an oversight: a sink that could return an error would eventually
// have a caller that checked it, and a screen that had gone away would be able
// to end a run. An implementation that cares whether its publishes land says
// so through its own logging.
//
// It must also return promptly. It is called between a write and the next step
// of the agent loop, so an implementation that talks to something remote
// buffers internally and drops rather than blocking.
//
// A subscriber is therefore owed nothing. Every event carries its session, its
// arc and its sequence number, so a gap is the subscriber's cue to read the
// log again rather than this sink's cue to retry.
type EventSink interface {
	Consume(ctx context.Context, evs ...*events.Event)
}

// PublishEvents hands every batch that was written to a sink.
//
// It goes outermost of the persistor chain, so a write refused further in
// publishes nothing, and it publishes the events as they were stored, sequence
// numbers and all. A nil sink leaves the chain alone.
//
// Notification hangs here rather than off the agent because the persistor is
// what every writer has in common. A user's message, a cancel, what a task
// reports and what the agent records all pass through one of these, and an
// interface following only the agent would miss the first three.
func PublishEvents(persistor EventPersistor, sink EventSink) EventPersistor {
	if sink == nil {
		return persistor
	}
	return &publishingPersistor{persistor: persistor, sink: sink}
}

type publishingPersistor struct {
	persistor EventPersistor
	sink      EventSink
}

func (p *publishingPersistor) Persist(ctx context.Context, evs ...*events.Event) ([]*events.Event, error) {
	stored, err := p.persistor.Persist(ctx, evs...)
	if err != nil {
		return nil, err
	}
	// The batch goes whole because it was written whole: a subscriber handed a
	// reply without the calls beside it reads an answer the model never gave.
	// Synchronously, because a goroutine per batch would reorder them.
	p.sink.Consume(ctx, stored...)
	return stored, nil
}

// MintIdentity gives an event an ID and a timestamp. Every write goes through
// one of these, because a store is left to assign the sequence number and
// nothing else.
//
// It does not give an event a session or an agent. Every writer names the
// session it writes for and the agent that added the event, since nothing in
// the chain is tied to one session or one agent any more, and an event that
// arrives without either is a bug in the writer rather than something to
// fill in.
//
// The timestamp is taken here rather than at the append, so two events can
// carry times in the opposite order to their sequence numbers. The sequence
// number is the order.
func MintIdentity(persistor EventPersistor) EventPersistor {
	return &mintingPersistor{persistor: persistor}
}

type mintingPersistor struct {
	persistor EventPersistor
}

func (m *mintingPersistor) Persist(ctx context.Context, evs ...*events.Event) ([]*events.Event, error) {
	minted := make([]*events.Event, 0, len(evs))
	for _, ev := range evs {
		if ev.SessionID == "" {
			return nil, fmt.Errorf("%s event names no session", ev.Typ)
		}
		if ev.AgentID == "" {
			return nil, fmt.Errorf("%s event names no agent", ev.Typ)
		}
		// The caller's event is left alone, since they may still be holding it.
		one := *ev
		// An ID or a time already set is kept, which is how a caller ties two
		// events together before either is written. Uniqueness is not checked.
		if one.ID == "" {
			one.ID = NewEventID()
		}
		if one.Timestamp.IsZero() {
			one.Timestamp = time.Now().UTC()
		}
		minted = append(minted, &one)
	}
	return m.persistor.Persist(ctx, minted...)
}

// AsAgent stamps every event written through it with the agent that added
// it. An event already naming an agent is left alone, so a writer that knows
// its agent keeps its own stamp, the same convention as the ID and the time in
// MintIdentity.
//
// A run is handed one of these rather than its agent id, so an agent never
// holds the id it runs under: the work item names the agent, the persistor
// records it, and nothing between the two has to carry it.
func AsAgent(agentID string, persistor EventPersistor) EventPersistor {
	return &agentPersistor{agentID: agentID, persistor: persistor}
}

type agentPersistor struct {
	agentID   string
	persistor EventPersistor
}

func (a *agentPersistor) Persist(ctx context.Context, evs ...*events.Event) ([]*events.Event, error) {
	stamped := make([]*events.Event, 0, len(evs))
	for _, ev := range evs {
		// The caller's event is left alone, since they may still be holding it.
		one := *ev
		if one.AgentID == "" {
			one.AgentID = a.agentID
		}
		stamped = append(stamped, &one)
	}
	return a.persistor.Persist(ctx, stamped...)
}

// NewEventID mints an identifier for an event.
func NewEventID() string {
	var buf [16]byte
	rand.Read(buf[:])
	return hex.EncodeToString(buf[:])
}
