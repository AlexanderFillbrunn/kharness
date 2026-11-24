package common

import (
	"encoding/json"
	"fmt"
	"time"

	"fillbrunn.ai/kube-agent/proto/pkg/events"
	"fillbrunn.ai/kube-agent/proto/pkg/history"
)

// This file holds snapshots of the fold: a stored History of one session at
// one sequence number, which lets a reader fold the tail of the log alone
// instead of the whole session. The log stays the source of truth — a
// snapshot is a cache, and a reader that ignores snapshots replays the
// session whole, exactly as before.

// Snapshot is one stored fold of a session's log. The fold covers every event
// with Seq <= Fold.LastSeq; the events above that are still read from the log.
type Snapshot struct {
	SessionID string

	// SavedAt is when the snapshot was stored, not when the fold was made:
	// the fold is as of Fold.LastSeq, whenever that was written.
	SavedAt time.Time

	// Fold is the decoded history the snapshot holds. Its encoding is owned
	// by the history package; stores go through history.EncodeFold and
	// history.DecodeFold rather than choosing one themselves.
	Fold *history.History
}

// Seq is the sequence number the snapshot's fold covers: every event with
// Seq <= Seq is folded into Fold.
func (s *Snapshot) Seq() int {
	if s == nil || s.Fold == nil {
		return -1
	}
	return s.Fold.LastSeq
}

// SnapshotInfo is what a save decision needs to know about the stored
// snapshot: the seq it covers and when it was saved. It is a view of a
// Snapshot, not the snapshot — the fold itself never leaves the read path.
type SnapshotInfo struct {
	Seq     int
	SavedAt time.Time
}

// Info returns the snapshot's info, or nil on a nil snapshot — so a read
// with no stored snapshot needs no special case at the call site.
func (s *Snapshot) Info() *SnapshotInfo {
	if s == nil || s.Fold == nil {
		return nil
	}
	return &SnapshotInfo{Seq: s.Seq(), SavedAt: s.SavedAt}
}

// ReadOptions is what a caller tells an EventSource about how to read the
// log. The zero value reads exactly as the store always has: every event,
// no snapshot.
type ReadOptions struct {
	// UseSnapshots asks the source to return the latest stored snapshot of
	// the session, if any, together with only the events above the snapshot's
	// seq. Without a stored snapshot the read returns the whole log and a nil
	// snapshot, which is the same as not asking.
	UseSnapshots bool
}

// snapshotWire is the stored envelope of a snapshot: the fold as raw JSON
// beside the seq it covers and when it was saved, so a store can compare seqs
// — in a script, say — without decoding the fold itself. EncodeSnapshot and
// DecodeSnapshot are the only ways in and out, so every store stores the same
// shape.
type snapshotWire struct {
	Seq     int             `json:"seq"`
	SavedAt time.Time       `json:"saved_at"`
	Fold    json.RawMessage `json:"fold"`
}

// EncodeSnapshot serializes a snapshot for storage.
func EncodeSnapshot(snap *Snapshot) ([]byte, error) {
	if snap == nil || snap.Fold == nil {
		return nil, fmt.Errorf("cannot encode a snapshot with no fold")
	}
	fold, err := history.EncodeFold(snap.Fold)
	if err != nil {
		return nil, err
	}
	return json.Marshal(snapshotWire{Seq: snap.Fold.LastSeq, SavedAt: snap.SavedAt, Fold: fold})
}

// DecodeSnapshot deserializes what EncodeSnapshot wrote. The session ID is
// the one the snapshot is stored under — the store's key — so it is the
// snapshot's, not the fold's: a History names no session.
func DecodeSnapshot(data []byte, sessionID string) (*Snapshot, error) {
	var wire snapshotWire
	if err := json.Unmarshal(data, &wire); err != nil {
		return nil, fmt.Errorf("decode snapshot of %s: %w", sessionID, err)
	}
	fold, err := history.DecodeFold(wire.Fold)
	if err != nil {
		return nil, fmt.Errorf("decode fold of %s: %w", sessionID, err)
	}
	return &Snapshot{SessionID: sessionID, SavedAt: wire.SavedAt, Fold: fold}, nil
}

// FoldEvents folds what a snapshot-aware read returned: the snapshot's fold
// where the read carried one, an empty history where it did not, with the
// read's events folded on top in either case. A nil snapshot with events is
// an ordinary full replay; a snapshot with events is history.Resume, which
// refuses a tail that does not start above the snapshot's seq.
//
// The snapshot's fold is folded into and returned, so the caller owns it
// after this call: a store always decodes a fresh snapshot per read, which is
// what keeps the stored one from moving.
func FoldEvents(snap *Snapshot, evs []*events.Event) (*history.History, error) {
	if snap == nil {
		return history.Replay(evs)
	}
	if snap.Fold == nil {
		return nil, fmt.Errorf("snapshot of %s carries no fold", snap.SessionID)
	}
	return history.Resume(snap.Fold, evs)
}
