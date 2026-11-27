package common

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"fillbrunn.ai/kube-agent/proto/pkg/events"
	"fillbrunn.ai/kube-agent/proto/pkg/history"
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

	// SaveSnapshot stores a fold of the session's log at the fold's LastSeq,
	// the seq the snapshot covers. A snapshot is not a log entry: it never
	// enters the log, is never folded, and is replaced by the next one. A
	// store keeps the snapshot with the highest LastSeq and refuses one
	// beyond the log, so a snapshot never claims more than the log holds.
	// Saving is best-effort: one that fails costs the next reader a full
	// replay, nothing more.
	SaveSnapshot(ctx context.Context, sessionID string, fold *history.History) error
}

type EventSource interface {
	// GetEventsForArc returns the events one arc is run from: everything in
	// its session up to and including the arc's opening event, plus everything
	// correlated to the arc or to a task afterwards — COMPACTION_REQUESTED
	// included, since it correlates to the arc it was produced in — plus the
	// session-level records that carry no correlation ID, which is COMPACTED
	// alone, so a compaction that landed after the arc opened still reaches
	// the run that resumes it.
	//
	// With ReadOptions.UseSnapshots the source instead returns the session's
	// latest stored snapshot, if any, plus only the events above the
	// snapshot's seq that the same rule selects; a snapshot old enough to
	// cover the arc's opening replaces the whole prefix. The snapshot is nil
	// when none is stored or UseSnapshots is false, and then the events are
	// the whole selection. Fold the two with FoldEvents.
	GetEventsForArc(ctx context.Context, ref ArcRef, opts ReadOptions) (*Snapshot, []*events.Event, error)

	// GetEventsForSession returns the session's whole log in the order it was
	// written, which is what the task table and the report validation are
	// built from. With ReadOptions.UseSnapshots it returns the latest stored
	// snapshot, if any, plus only the events above the snapshot's seq — nil
	// snapshot and the whole log where none is stored or none is asked for.
	GetEventsForSession(ctx context.Context, sessionID string, opts ReadOptions) (*Snapshot, []*events.Event, error)

	// TaskState reads what the log says about one task — whether the session
	// holds it, its kind and actor, and whether it has settled — with a
	// targeted scan rather than a fold of the whole session. It is the
	// admission check for task-emitted writes.
	TaskState(ctx context.Context, sessionID, taskID string) (*TaskState, error)
}

// SelectArcEvents returns the events one arc is run from, out of the
// candidates given — the GetEventsForArc cutoff, applied here so every store
// answers alike. An event is selected when any of these holds:
//
//   - its seq is at or below the arc's opening seq: the conversation prefix
//     up to and including the opening event;
//   - it is correlated to the arc;
//   - it is correlated to a task of the session, so the fold builds the
//     session's task table;
//   - it carries no correlation ID: the session-level records, COMPACTED and
//     a session-level COMPACTION_REQUESTED, so a compaction that landed
//     after the arc opened still reaches the run that resumes it.
//
// The arc is found by its opening event: the earliest event correlated to
// the arc ID whose kind opens an arc (events.OpensArc — a USER_MESSAGE, or a
// task event written AsMessage). Where the opening seq and the task IDs come
// from depends on snap: with a snapshot, all is the tail above the
// snapshot's seq and both are read from the snapshot's fold where they are
// already folded in it and scanned from the tail where they are not; without
// one, all is the whole log and both are scanned from it. An arc found in
// neither place is an error.
func SelectArcEvents(all []*events.Event, ref ArcRef, snap *Snapshot) ([]*events.Event, error) {
	taskIDs := map[string]bool{}
	opening := -1
	if snap != nil && snap.Fold != nil {
		for id := range snap.Fold.Tasks {
			taskIDs[id] = true
		}
		for _, arc := range snap.Fold.Arcs {
			if arc.ID == ref.ArcID {
				opening = arc.Seq
				break
			}
		}
	}
	for _, ev := range all {
		if ev.Typ == events.AsyncTaskInvokeEventType {
			taskIDs[ev.ID] = true
		}
		if opening < 0 && correlatesTo(ev, ref.ArcID) && events.OpensArc(ev) {
			opening = ev.Seq
		}
	}
	if opening < 0 {
		return nil, fmt.Errorf("no arc %s in %s", ref, ref.SessionID)
	}

	var selected []*events.Event
	for _, ev := range all {
		if ev.Seq <= opening || ev.CorrelationID == nil || correlatesTo(ev, ref.ArcID) ||
			taskIDs[*ev.CorrelationID] {
			selected = append(selected, ev)
		}
	}
	return selected, nil
}

func correlatesTo(ev *events.Event, arcID string) bool {
	return ev.CorrelationID != nil && *ev.CorrelationID == arcID
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
		case events.AsyncTaskLaunchResultEventType:
			// A launch that failed settles the task like a failed result:
			// the RESULT event is reserved for work that actually started.
			if payload, ok := ev.Payload.(events.AsyncTaskLaunchResultPayload); ok && payload.CausationID == taskID && !payload.Launched {
				state.Settled = true
				state.Status = events.TaskStatusFailed
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

// SaveSnapshot passes the fold through untouched: a snapshot is not an event,
// so there is nothing to publish on it.
func (p *publishingPersistor) SaveSnapshot(ctx context.Context, sessionID string, fold *history.History) error {
	return p.persistor.SaveSnapshot(ctx, sessionID, fold)
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

// SaveSnapshot passes the fold through untouched: minting gives events IDs
// and times, and a snapshot already carries its seq in the fold's LastSeq.
func (m *mintingPersistor) SaveSnapshot(ctx context.Context, sessionID string, fold *history.History) error {
	return m.persistor.SaveSnapshot(ctx, sessionID, fold)
}

// AsSession stamps every event written through it with the session it
// belongs to. An event already naming a session is left alone, so a writer
// that knows its session keeps its own stamp, the same convention as the ID
// and the time in MintIdentity.
//
// A run is handed one of these rather than its session id, for the same
// reason AsAgent exists: the work item names the session, the persistor
// records it, and nothing between the two has to carry it. MintIdentity
// still refuses an event that reaches it with no session, so the stamp lands
// before that check.
func AsSession(sessionID string, persistor EventPersistor) EventPersistor {
	return &sessionPersistor{sessionID: sessionID, persistor: persistor}
}

type sessionPersistor struct {
	sessionID string
	persistor EventPersistor
}

func (s *sessionPersistor) Persist(ctx context.Context, evs ...*events.Event) ([]*events.Event, error) {
	stamped := make([]*events.Event, 0, len(evs))
	for _, ev := range evs {
		// The caller's event is left alone, since they may still be holding it.
		one := *ev
		if one.SessionID == "" {
			one.SessionID = s.sessionID
		}
		stamped = append(stamped, &one)
	}
	return s.persistor.Persist(ctx, stamped...)
}

// SaveSnapshot passes the fold through untouched: the session is the
// snapshot's own parameter, so there is nothing to stamp on it.
func (s *sessionPersistor) SaveSnapshot(ctx context.Context, sessionID string, fold *history.History) error {
	return s.persistor.SaveSnapshot(ctx, sessionID, fold)
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

// SaveSnapshot passes the fold through untouched: a snapshot names no agent,
// since it is the store's record of the session, not anyone's write.
func (a *agentPersistor) SaveSnapshot(ctx context.Context, sessionID string, fold *history.History) error {
	return a.persistor.SaveSnapshot(ctx, sessionID, fold)
}

// NewEventID mints an identifier for an event.
func NewEventID() string {
	var buf [16]byte
	rand.Read(buf[:])
	return hex.EncodeToString(buf[:])
}
