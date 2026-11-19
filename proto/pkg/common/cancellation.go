package common

import (
	"context"
	"errors"
	"fmt"

	"fillbrunn.ai/kube-agent/proto/pkg/events"
)

// ErrCancelled reports that the arc was cancelled while the agent was working
// on it. The arc is already closed, so a caller seeing this has nothing left
// to write.
var ErrCancelled = errors.New("the arc was cancelled")

// CancelSource answers whether an arc or a task has been cancelled. The
// answers are advisory: a cancel recorded after the read still slips through.
type CancelSource interface {
	// Cancelled returns the cancel recorded against an arc, or nil where there
	// is none.
	Cancelled(ctx context.Context, ref ArcRef) (*events.Event, error)

	// CancelledTask returns the task-addressed cancel recorded against the
	// task named — a CANCEL whose correlation ID is the task ID — or nil
	// where there is none.
	CancelledTask(ctx context.Context, sessionID, taskID string) (*events.Event, error)
}

// GuardCancellation wraps a persistor so that a run stops once the arc it is
// working on, or the task a write reaches, has been cancelled somewhere else.
//
// The guard sits on the write path because a dispatch is written immediately
// before its tool runs, so a check there stops the tool rather than the run
// that follows it. A refused write is reported as ErrCancelled and nothing is
// written. One event that starts work refuses the whole batch it arrived in,
// since a batch is written whole or not at all.
//
// On every batch the guard checks two axes: a write that starts work on a
// cancelled arc is refused, and any task-emitted event whose causation names a
// task the log shows cancelled is refused the same way — the whole batch,
// ErrCancelled, nothing written.
//
// It remembers nothing between writes. A cancelled arc or task is read again
// on every guarded write, which costs one read for a run that is about to end
// anyway: the refusal ends the run at the first dispatch it reaches, so a
// cache of already-cancelled records saved about one read while racing between
// the runs that share a persistor and growing for the life of the process.
func GuardCancellation(persistor EventPersistor, cancels CancelSource) EventPersistor {
	return &guardedPersistor{persistor: persistor, cancels: cancels}
}

type guardedPersistor struct {
	persistor EventPersistor
	cancels   CancelSource
}

func (g *guardedPersistor) Persist(ctx context.Context, evs ...*events.Event) ([]*events.Event, error) {
	// Only a write that is about to start work is worth stopping on the arc
	// axis. Refusing the rest would lose the record without preventing
	// anything. The task axis is checked for every task-emitted event in the
	// batch, since a report about a cancelled task is exactly what must not
	// be written.
	ref, guarded := GuardedArc(evs)
	if guarded {
		cancel, err := g.cancels.Cancelled(ctx, ref)
		if err != nil {
			// A guard that cannot answer lets the write through. The cancel is
			// still in the log, and the run stops at the next step that can see it.
		} else if cancel != nil {
			return nil, fmt.Errorf("%w: arc %s, %d events not written", ErrCancelled, ref, len(evs))
		}
	}

	for _, ev := range evs {
		taskID, ok := events.TaskCausation(ev.Payload)
		if !ok {
			continue
		}
		cancel, err := g.cancels.CancelledTask(ctx, ev.SessionID, taskID)
		if err != nil {
			// Failing open here too: the cancel is in the log, and the fold
			// drops the report that follows it.
			continue
		}
		if cancel != nil {
			return nil, fmt.Errorf("%w: task %s, %d events not written", ErrCancelled, taskID, len(evs))
		}
	}

	return g.persistor.Persist(ctx, evs...)
}

// GuardedArc returns the arc a batch would start work on, and whether any
// event in it does. One is enough, because the batch is refused whole. It is
// exported for the agent loop, which gives a work-starting write the same
// treatment the guard does.
func GuardedArc(evs []*events.Event) (ArcRef, bool) {
	for _, ev := range evs {
		if startsWork(ev) && ev.CorrelationID != nil {
			return ArcRef{SessionID: ev.SessionID, ArcID: *ev.CorrelationID}, true
		}
	}
	return ArcRef{}, false
}

// startsWork reports whether an event is written before something happens
// rather than after it. Only a dispatch and a task invocation are.
func startsWork(ev *events.Event) bool {
	switch ev.Typ {
	case events.ToolDispatchedEventType, events.AsyncTaskInvokeEventType:
		return true
	}
	return false
}
