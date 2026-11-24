package agent

import (
	"fillbrunn.ai/kube-agent/proto/pkg/common"
	"context"
	"errors"
	"fmt"

	"fillbrunn.ai/kube-agent/proto/pkg/history"
)

// StreamToken is one piece of a reply as the model writes it, addressed well
// enough to route from another process.
type StreamToken struct {
	SessionID string
	ArcID     string

	// MessageID is the assistant message that will record this reply. It is
	// minted before the model is called, so the tokens can name it.
	MessageID string

	Content string
	Seq     int
}

// TokenSink takes a reply as it is written.
//
// Like common.EventSink it returns nothing, so a watcher cannot fail a run, and it
// must return promptly: it is called from inside the model's stream.
//
// A token is not a record. It is lost on a dropped connection and never
// replayed, because the ASSISTANT_MESSAGE event behind it carries the same
// text and is in the log. So a subscriber treats streamed text as provisional:
// it replaces it with the recorded content when that event arrives, and
// discards it for an arc that ends without one.
type TokenSink interface {
	Token(ctx context.Context, tok StreamToken)
}

// ArcWorker runs the agent over an arc that already exists, under the lease
// that keeps two runs off it. Every way into an arc shares one worker, because
// the lease only excludes callers that acquire the same one.
//
// It holds nothing belonging to one session. The session arrives with the arc,
// so one worker serves a whole tier.
type ArcWorker struct {
	persistor common.EventPersistor
	source    common.EventSource
	resolver  AgentResolver
	lease     ArcLease

	// tokens follows a reply as it is written, and is nil for a worker nobody
	// is watching, which is every worker with no interface attached.
	tokens TokenSink

	// cancels stops a run that is already inside a tool or a stream, and is
	// nil for a worker that only stops between steps.
	cancels CancelWatcher

	// compactions starts the compaction a due round reports, outside the run,
	// and is nil for a worker that leaves the request standing in the log.
	compactions CompactionStarter

	// modes is how much of the log the worker's runs disclose in their
	// projections.
	modes history.Modes

	// snapshots is when the worker reads through a stored fold snapshot and
	// saves one. The zero value reads the whole log and never saves.
	snapshots SnapshotPolicy
}

// ArcWorkerOption configures parts of a worker that not every worker has.
type ArcWorkerOption func(*ArcWorker)

// WithTokenSink gives a worker somewhere to stream replies as they arrive.
func WithTokenSink(tokens TokenSink) ArcWorkerOption {
	return func(w *ArcWorker) { w.tokens = tokens }
}

// WithCancelWatcher lets a cancel reach work the run has already started.
// Without one a run stops at its next guarded write, which is the tool or the
// task after the one it is in.
func WithCancelWatcher(cancels CancelWatcher) ArcWorkerOption {
	return func(w *ArcWorker) { w.cancels = cancels }
}

// CompactionStarter starts the compaction a COMPACTION_REQUESTED asks for,
// outside the run that wrote it: a queue deployment enqueues the work item
// for the compaction worker to be served off it, a queue-less one runs the
// worker on a goroutine. It must return promptly — the run is waiting on the
// notification, not on the compaction.
type CompactionStarter interface {
	StartCompaction(ctx context.Context, sessionID, requestID, agentID string)
}

// WithCompactionStarter gives a worker the starter a run's compaction
// notification reaches, when a round finds compaction due.
func WithCompactionStarter(starter CompactionStarter) ArcWorkerOption {
	return func(w *ArcWorker) { w.compactions = starter }
}

// WithModes sets how much of the log the worker's runs disclose in their
// projections. Without one, DefaultModes.
func WithModes(modes history.Modes) ArcWorkerOption {
	return func(w *ArcWorker) { w.modes = modes }
}

// WithSnapshotPolicy sets when the worker reads the log through a stored fold
// snapshot and saves one at the end of a work item. Without one, snapshots
// are neither read nor saved.
func WithSnapshotPolicy(policy SnapshotPolicy) ArcWorkerOption {
	return func(w *ArcWorker) { w.snapshots = policy }
}

// NewArcWorker builds a worker from the parts a run is made of. A nil lease
// becomes a LocalLease.
func NewArcWorker(persistor common.EventPersistor, source common.EventSource, resolver AgentResolver, lease ArcLease, options ...ArcWorkerOption) (*ArcWorker, error) {
	if persistor == nil || source == nil || resolver == nil {
		return nil, errors.New("an arc worker needs a persistor, a source and a resolver")
	}
	if lease == nil {
		lease = NewLocalLease()
	}
	worker := &ArcWorker{persistor: persistor, source: source, resolver: resolver, lease: lease, modes: history.DefaultModes()}
	for _, option := range options {
		option(worker)
	}
	return worker, nil
}

// Work replays an arc from the log and runs the agent over it. It returns
// false without running when another caller holds the arc, which leaves that
// arc owed a run.
//
// The work item names the agent that works the arc, and there is no default:
// a caller that cannot name one has nothing to hand the worker. The agent is
// resolved once the arc is held, so a resolver that cannot build it fails a
// worker that owns the arc rather than one queued behind somebody else.
func (w *ArcWorker) Work(ctx context.Context, ref common.ArcRef, agentID string) (bool, error) {
	if agentID == "" {
		return false, errors.New("a work item names no agent")
	}
	release, ok, err := w.lease.Acquire(ctx, ref)
	if err != nil {
		return false, fmt.Errorf("could not take the lease on arc %s: %w", ref, err)
	}
	if !ok {
		return false, nil
	}
	defer release()

	ctx, stop := w.watchForCancel(ctx, ref)
	defer stop()

	agent, err := w.resolver.ResolveAgent(ctx, agentID)
	if err != nil {
		return false, fmt.Errorf("resolve agent %s for arc %s: %w", agentID, ref, err)
	}
	h, snapInfo, err := w.arcHistory(ctx, ref)
	if err != nil {
		return false, err
	}
	// The session the work item names is stamped onto everything the run
	// writes and every task ref it hands out, here where the session is
	// known: the run itself never sees it.
	runOpts := RunOptions{
		Persistor: common.AsSession(ref.SessionID, common.AsAgent(agentID, w.persistor)),
		History:   h,
		ArcID:     ref.ArcID,
		Modes:     w.modes,
		Tokens:    w.tokenSink(ctx, ref),
		OnCompactionDue: func(notifyCtx context.Context, requestID string) {
			w.notifyCompactionDue(notifyCtx, ref.SessionID, requestID, agentID)
		},
	}
	if agent.tasks != nil {
		runOpts.Tasks = common.InSession(ref.SessionID, agent.tasks)
	}
	runErr := agent.Run(ctx, runOpts)
	// The save point of a work item: once the run is over, never inside it.
	// The run folded into h as it went, so h is at the log's head now. A save
	// that fails or is not due costs the next read a full replay, nothing
	// more, so it does not touch the run's result.
	saveSnapshot(ctx, w.persistor, ref.SessionID, h, snapInfo, w.snapshots)
	return true, runErr
}

// notifyCompactionDue hands a run's compaction notification to the starter.
// The context is detached from the run's, because a run that suspends right
// after writing the request is over by the time the compaction should start,
// and the compaction must not end with it. A nil starter leaves the request
// standing in the log for anyone to take.
func (w *ArcWorker) notifyCompactionDue(ctx context.Context, sessionID, requestID, agentID string) {
	if w.compactions == nil {
		return
	}
	w.compactions.StartCompaction(context.WithoutCancel(ctx), sessionID, requestID, agentID)
}

// watchForCancel derives the context the run is stopped through, and a stop to
// call when the run is over.
//
// It subscribes before the caller replays the arc. A cancel written before
// this point is in what the replay reads, and one written after it is
// delivered here, so nothing falls between the two. A watcher that misses it
// anyway only costs latency, because the guard still refuses the run's next
// guarded write.
func (w *ArcWorker) watchForCancel(ctx context.Context, ref common.ArcRef) (context.Context, func()) {
	if w.cancels == nil {
		return ctx, func() {}
	}

	cancelled, unwatch := w.cancels.Watch(ctx, ref)
	ctx, abort := context.WithCancel(ctx)
	go func() {
		select {
		case <-cancelled:
			abort()
		case <-ctx.Done():
		}
	}()
	return ctx, func() {
		// The context is cancelled first, so the goroutine is on its way out
		// before the subscription it was watching goes away.
		abort()
		unwatch()
	}
}

// tokenSink labels what the agent streams with the arc it is streaming for.
// The agent knows the message, the worker knows the arc, and neither knows
// where the text ends up.
//
// It is nil for a worker nobody is watching, which is what stops the agent
// paying for a sink that discards.
func (w *ArcWorker) tokenSink(ctx context.Context, ref common.ArcRef) common.Sink[Token] {
	if w.tokens == nil {
		return nil
	}
	sink := w.tokens
	return common.SinkFromFunc(func(tok Token) error {
		sink.Token(ctx, StreamToken{
			SessionID: ref.SessionID,
			ArcID:     ref.ArcID,
			MessageID: tok.MessageID,
			Content:   tok.Content,
			Seq:       tok.Seq,
		})
		return nil
	})
}

// arcHistory loads and folds the events one arc is run from. Under the
// worker's snapshot policy the read returns the session's latest snapshot,
// when there is one, plus the events above it, and the fold resumes from the
// snapshot; what is returned alongside is only the snapshot's info, so the
// save point at the end of the work item can tell how stale it is — the
// snapshot's fold itself stays here, in the read.
func (w *ArcWorker) arcHistory(ctx context.Context, ref common.ArcRef) (*history.History, *common.SnapshotInfo, error) {
	// The store partitions by session, so what comes back is this session's;
	// the fold names no session and does not check.
	snap, stored, err := w.source.GetEventsForArc(ctx, ref, w.snapshots.readOptions())
	if err != nil {
		return nil, nil, err
	}
	h, err := common.FoldEvents(snap, stored)
	if err != nil {
		return nil, nil, err
	}
	return h, snap.Info(), nil
}
