package common

import "context"

// WorkEnqueuer is the sliver of the work queue a producer needs: after an
// opening event is persisted, the new arc becomes a work item in the same
// queue user arcs are served from. Work items interleave in arrival order
// across arcs and users; the per-arc lease serializes the runs of one arc.
//
// The enqueue is the producer's, never the agent's: the agent worker only
// ever takes items off the queue.
type WorkEnqueuer interface {
	Enqueue(ctx context.Context, ref ArcRef, agentID string) error
}
