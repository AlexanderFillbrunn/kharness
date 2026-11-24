package agent

import (
	"context"
	"time"

	"fillbrunn.ai/kube-agent/proto/pkg/common"
	"fillbrunn.ai/kube-agent/proto/pkg/history"
)

// This file holds the workers' snapshot policy: when they read the log through
// a stored fold snapshot and when they save one. A snapshot is a cache of the
// fold at one seq, so reading through it and saving one are both safe to skip
// — the log is the source of truth either way.

// SnapshotPolicy says when a worker considers fold snapshots. The zero value
// considers neither: reads return the whole log and nothing is ever saved,
// which is how a deployment that has not opted in behaves.
//
// Saving happens only at a worker's save points — the end of an arc work item
// and the end of a compaction — never inside a run's rounds, so a run never
// pays for one mid-flight. MinEvents and MinInterval make saving less often
// than that: a snapshot is saved only once the log has grown by at least
// MinEvents events and at least MinInterval has passed since the stored one,
// each skipped when zero.
type SnapshotPolicy struct {
	// Enabled turns snapshot reads and saves on.
	Enabled bool

	// MinEvents is how many new events the log must hold beyond the stored
	// snapshot before another snapshot is saved. Zero saves at every save
	// point.
	MinEvents int

	// MinInterval is how long after the stored snapshot was saved before
	// another is saved. Zero saves at every save point.
	MinInterval time.Duration
}

// readOptions is how the worker reads the log under this policy.
func (p SnapshotPolicy) readOptions() common.ReadOptions {
	return common.ReadOptions{UseSnapshots: p.Enabled}
}

// due reports whether a snapshot of the fold at seq should be saved now,
// given what the read found stored — the info of the stored snapshot, nil
// where none is stored — and the time now.
func (p SnapshotPolicy) due(info *common.SnapshotInfo, seq int, now time.Time) bool {
	if !p.Enabled {
		return false
	}
	if info == nil {
		return true
	}
	if p.MinEvents > 0 && seq-info.Seq < p.MinEvents {
		return false
	}
	if p.MinInterval > 0 && now.Sub(info.SavedAt) < p.MinInterval {
		return false
	}
	return true
}

// saveSnapshot stores the fold at its LastSeq when the policy says a snapshot
// is due, given what the read found stored. It is the save point's whole job
// and it is best-effort: a save that fails costs the next reader a full
// replay, nothing more, so the error is dropped rather than failing the run
// or compaction that just ended.
func saveSnapshot(ctx context.Context, persistor common.EventPersistor, sessionID string, fold *history.History, info *common.SnapshotInfo, policy SnapshotPolicy) {
	if fold == nil || !policy.due(info, fold.LastSeq, time.Now().UTC()) {
		return
	}
	_ = persistor.SaveSnapshot(ctx, sessionID, fold)
}
