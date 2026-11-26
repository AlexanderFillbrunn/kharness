// Package common holds the contracts shared by every service in the system:
// the agent worker that runs arcs, and the producers — the user-message
// ingress and the task controllers — that write events into the log and work
// items into the queue.
//
// It holds types and interfaces only, plus the persistor chain every writer
// shares. It has no behavior of its own beyond that plumbing, so the agent
// package can depend on it without depending on any producer, and a producer
// can depend on it without depending on the agent.
package common

// ArcRef names one arc well enough to find it from any process. An arc ID is
// unique on its own, but nothing that stores, locks or routes by arc can be
// partitioned by session without being told the session as well, and a worker
// claiming arcs off a shared queue has no other way to learn it.
type ArcRef struct {
	SessionID string
	ArcID     string
}

func (r ArcRef) String() string { return r.SessionID + "/" + r.ArcID }

// TaskRef names one running task well enough to reach it from any process.
// ID alone is the task's identity; the session is where its log lives.
type TaskRef struct {
	// SessionID is the session whose log holds the task. A report naming a
	// task reads that session's log, so the session has to travel with the ID.
	SessionID string

	// ID identifies the task in the log: the ID of its invoke event.
	ID string
}

// WorkItem is one unit of work on the queue: an arc, named, and the agent
// that is to work it. Producers push items when they write an opening event;
// the agent worker pops them.
// Work item kinds. The zero value of Kind is an arc item, so an entry written
// before kinds existed still reads as the arc it names.
const (
	WorkItemKindArc        = "arc"
	WorkItemKindCompaction = "compaction"
)

type WorkItem struct {
	SessionID string `json:"session_id"`
	ArcID     string `json:"arc_id"`
	AgentID   string `json:"agent_id"`

	// Kind says what the item is: an arc run (the default, also for entries
	// written before kinds existed) or a compaction. A compaction item names
	// no arc; it names the COMPACTION_REQUESTED event whose watermark the
	// compaction is to reach.
	Kind string `json:"kind,omitempty"`

	// RequestID is the ID of the COMPACTION_REQUESTED event a compaction item
	// serves. Empty for arc items.
	RequestID string `json:"request_id,omitempty"`

	// TriggerSeq is the seq of the event that caused this item: the write
	// that owes the arc a run. The worker fires the continuation hook for
	// exactly this event, resolved from the log or the fold by seq. Zero for
	// items no single event caused, like the ones a landed compaction
	// releases an arc with.
	TriggerSeq int `json:"trigger_seq,omitempty"`
}

// Ref returns the arc the item names. It is meaningless for a compaction item.
func (i WorkItem) Ref() ArcRef { return ArcRef{SessionID: i.SessionID, ArcID: i.ArcID} }

// IsCompaction reports whether the item is a compaction rather than an arc run.
func (i WorkItem) IsCompaction() bool { return i.Kind == WorkItemKindCompaction }
