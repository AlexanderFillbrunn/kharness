package common

import "context"

// WorkEnqueuer is the sliver of the work queue a producer needs: after an
// opening event is persisted, the new arc becomes a work item in the same
// queue user arcs are served from. Work items interleave in arrival order
// across arcs and users; the per-arc lease serializes the runs of one arc.
//
// The enqueue is the producer's, never the agent's: the agent worker only
// ever takes items off the queue. The one exception is the compaction item,
// which the arc worker's compaction starter enqueues when a run reports one
// due — see CompactionEnqueuer.
type WorkEnqueuer interface {
	// Enqueue adds an item for the arc named to the queue. triggerSeq is the
	// seq of the persisted event that caused the run, carried on the item so
	// the worker can fire the continuation hook for exactly that event; zero
	// when no single event caused it.
	Enqueue(ctx context.Context, ref ArcRef, agentID string, triggerSeq int) error
}

// CompactionEnqueuer is the sliver of the work queue a compaction starter
// needs: when a run reports a due compaction, the worker hands the compaction
// to the queue as a work item of its own kind, served by the compaction
// worker rather than the arc worker. The agent's own loop never sees this —
// it only writes COMPACTION_REQUESTED and calls its notification hook.
type CompactionEnqueuer interface {
	EnqueueCompaction(ctx context.Context, sessionID, requestID, agentID string) error
}
