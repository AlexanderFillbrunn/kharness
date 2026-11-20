package agent

import (
	"context"
	"sync"
)

// This file holds the worker's half of cancellation: how a running arc is told
// it has been cancelled while it is mid-tool or mid-stream. The write-path
// guard — GuardCancellation, CancelSource, ErrCancelled — lives in common,
// since producers write through it too.

// CancelNotifier tells watchers that an arc has been cancelled.
//
// It is best effort, and a failure here does not fail the cancel: the record
// is in the log, and the guard stops the run at its next step whatever happens
// to the notification.
type CancelNotifier interface {
	NotifyCancel(ctx context.Context, ref ArcRef) error
}

// CancelWatcher reports that an arc has been cancelled, as soon as it can.
//
// It is what reaches work already under way, which the guard cannot: a tool
// handler selecting on its context, or a model stream mid-token. Without it
// those run to the end and their cost is spent whatever the log says.
//
// Nothing it promises is relied on. A notification that never arrives costs
// time and money, never correctness.
type CancelWatcher interface {
	// Watch returns a channel that is closed when the arc is cancelled, and a
	// stop to call once the caller is done with the arc. A watcher that
	// cannot subscribe returns a channel nobody will close, because the run
	// has to go on either way.
	Watch(ctx context.Context, ref ArcRef) (cancelled <-chan struct{}, stop func())
}

// LocalCancels is a CancelNotifier and a CancelWatcher for one process.
type LocalCancels struct {
	mu       sync.Mutex
	watchers map[ArcRef]map[chan struct{}]bool
}

func NewLocalCancels() *LocalCancels {
	return &LocalCancels{watchers: map[ArcRef]map[chan struct{}]bool{}}
}

func (c *LocalCancels) Watch(_ context.Context, ref ArcRef) (<-chan struct{}, func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.watchers == nil {
		c.watchers = map[ArcRef]map[chan struct{}]bool{}
	}
	if c.watchers[ref] == nil {
		c.watchers[ref] = map[chan struct{}]bool{}
	}
	cancelled := make(chan struct{})
	c.watchers[ref][cancelled] = true

	return cancelled, func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		// A channel the notifier has already closed is gone from the map, so
		// stopping after a cancel does nothing and never closes twice.
		if watching, ok := c.watchers[ref]; ok {
			delete(watching, cancelled)
			if len(watching) == 0 {
				delete(c.watchers, ref)
			}
		}
	}
}

// NotifyCancel closes every channel watching the arc. Watching again after
// this gets a channel nobody closes, which is correct: the cancel is in the
// log, and a run that starts after it reads it there.
func (c *LocalCancels) NotifyCancel(_ context.Context, ref ArcRef) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for cancelled := range c.watchers[ref] {
		close(cancelled)
	}
	delete(c.watchers, ref)
	return nil
}
