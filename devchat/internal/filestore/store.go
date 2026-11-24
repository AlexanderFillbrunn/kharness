// Package filestore keeps an event log in a single file, so the agent can be
// exercised end to end without standing anything up.
//
// It is a development and test store, living in the devchat module so that
// the framework cannot reach it. It holds the whole log in memory on every
// read, scans the file for every query, never compacts, and assumes it is the
// only writer.
//
// What it does hold to is the contract the projection depends on. See
// GetEventsForArc.
package filestore

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"fillbrunn.ai/kube-agent/proto/pkg/common"
	"fillbrunn.ai/kube-agent/proto/pkg/events"
	"fillbrunn.ai/kube-agent/proto/pkg/history"
)

// Store is an append-only log of JSON events, one per line.
type Store struct {
	mu   sync.Mutex
	path string
	file *os.File

	// seq is the sequence number of the last event written, or -1 for an empty
	// log, so that the first event is numbered 0 as the fold expects.
	seq int
}

// Open returns a store over the file at path, creating it and the directory
// holding it if they are not there, and picking up where an existing log left
// off.
func Open(path string) (*Store, error) {
	if dir := filepath.Dir(path); dir != "." {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return nil, fmt.Errorf("create %s: %w", dir, err)
		}
	}

	existing, err := readFile(path)
	if err != nil {
		return nil, err
	}

	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}

	seq := -1
	if len(existing) > 0 {
		seq = existing[len(existing)-1].Seq
	}
	return &Store{path: path, file: file, seq: seq}, nil
}

// Rotate moves the log aside and starts an empty one in its place, returning
// the path the old one was moved to, or "" where there was nothing to move. The
// log is renamed rather than truncated, so the old session stays on disk.
func (s *Store) Rotate() (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.seq < 0 {
		// An empty log is already a fresh one, and rotating it would leave an
		// empty file behind for every attempt.
		return "", nil
	}

	if err := s.file.Close(); err != nil {
		return "", fmt.Errorf("close %s: %w", s.path, err)
	}

	archived := fmt.Sprintf("%s.%s", s.path, time.Now().UTC().Format("20060102-150405"))
	if err := os.Rename(s.path, archived); err != nil {
		return "", fmt.Errorf("move %s aside: %w", s.path, err)
	}

	file, err := os.OpenFile(s.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return "", fmt.Errorf("open %s: %w", s.path, err)
	}
	s.file = file
	s.seq = -1
	return archived, nil
}

// Close releases the file.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.file.Close()
}

// Persist writes events and returns them as they were stored. The sequence
// number is the store's to assign, since nothing outside the lock knows the
// position in the log, and it is the only thing the store assigns.
//
// An event arrives named and timed, which is common.MintIdentity's job on the
// way in. One that does not is a bug in the writer and is refused, since a log
// line with no ID is one nothing can refer to afterwards.
//
// A batch is one write and one sync, and the sequence number only advances
// once it is on disk. That is as close to atomic as a file gets: a write that
// fails part way still leaves a torn line that readFile refuses, which is the
// kind of thing this store is allowed to do and a real one is not.
func (s *Store) Persist(_ context.Context, evs ...*events.Event) ([]*events.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(evs) == 0 {
		return nil, nil
	}

	seq := s.seq
	stored := make([]*events.Event, 0, len(evs))
	var lines []byte
	for _, ev := range evs {
		if ev.ID == "" {
			return nil, fmt.Errorf("%s event has no ID", ev.Typ)
		}
		if ev.Timestamp.IsZero() {
			return nil, fmt.Errorf("%s event %s has no timestamp", ev.Typ, ev.ID)
		}
		// The caller's event is left alone, since they may still be holding it.
		one := *ev
		seq++
		one.Seq = seq

		line, err := json.Marshal(&one)
		if err != nil {
			return nil, fmt.Errorf("marshal %s event: %w", one.Typ, err)
		}
		lines = append(append(lines, line...), '\n')
		stored = append(stored, &one)
	}

	if _, err := s.file.Write(lines); err != nil {
		return nil, fmt.Errorf("append to %s: %w", s.path, err)
	}
	// A crash between the write and the next step is exactly the case the
	// dispatch record exists for, so the lines are on disk before the caller
	// acts on them.
	if err := s.file.Sync(); err != nil {
		return nil, fmt.Errorf("sync %s: %w", s.path, err)
	}
	s.seq = seq
	return stored, nil
}

// Events returns the whole log in the order it was written.
func (s *Store) Events(_ context.Context) ([]*events.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return pointers(readFile(s.path))
}

// snapshotPath names the sidecar file the log's latest fold snapshot lives
// in. A snapshot never enters the log itself: it is a cache of the fold at
// one seq, kept beside the log file and replaced by the next save.
func (s *Store) snapshotPath() string {
	return s.path + ".snapshot"
}

// SaveSnapshot stores the fold of the session at the fold's LastSeq in the
// sidecar file. The store keeps the snapshot with the highest seq and refuses
// one beyond the log, so the snapshot never claims more than the log holds
// and never regresses; a save that loses that race is a nil error, since the
// newer snapshot is already on disk.
func (s *Store) SaveSnapshot(_ context.Context, sessionID string, fold *history.History) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if fold == nil {
		return fmt.Errorf("cannot save a nil fold")
	}
	if fold.LastSeq > s.seq {
		return fmt.Errorf("snapshot of %s claims seq %d beyond the log, whose last is %d", sessionID, fold.LastSeq, s.seq)
	}
	held, err := s.readSnapshot(sessionID)
	if err != nil {
		return err
	}
	if held != nil && held.Seq() >= fold.LastSeq {
		return nil
	}

	record, err := common.EncodeSnapshot(&common.Snapshot{SessionID: sessionID, SavedAt: time.Now().UTC(), Fold: fold})
	if err != nil {
		return fmt.Errorf("encode snapshot of %s: %w", sessionID, err)
	}
	// The sidecar is written whole and synced before it is visible, so a
	// crash mid-save leaves the previous snapshot rather than a torn one.
	tmp := s.snapshotPath() + ".tmp"
	file, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("open %s: %w", tmp, err)
	}
	if _, err := file.Write(record); err != nil {
		file.Close()
		return fmt.Errorf("write %s: %w", tmp, err)
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return fmt.Errorf("sync %s: %w", tmp, err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, s.snapshotPath()); err != nil {
		return fmt.Errorf("move %s into place: %w", tmp, err)
	}
	return nil
}

// readSnapshot returns the latest stored snapshot, or nil where none has been
// saved. The session ID is the one the read is for, and the one the snapshot
// is filed under. It is called with the lock held.
func (s *Store) readSnapshot(sessionID string) (*common.Snapshot, error) {
	data, err := os.ReadFile(s.snapshotPath())
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", s.snapshotPath(), err)
	}
	return common.DecodeSnapshot(data, sessionID)
}

// GetEventsForSession returns the session's whole log in the order it was
// written. A file holds one session, so the session ID only ever rules events
// out here; a store that held more than one would partition on it.
//
// With UseSnapshots it instead returns the latest stored snapshot, when there
// is one, plus only the session's events above the snapshot's seq.
func (s *Store) GetEventsForSession(_ context.Context, sessionID string, opts common.ReadOptions) (*common.Snapshot, []*events.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if opts.UseSnapshots {
		snap, err := s.readSnapshot(sessionID)
		if err != nil {
			return nil, nil, err
		}
		if snap != nil {
			all, err := readFile(s.path)
			if err != nil {
				return nil, nil, err
			}
			var tail []*events.Event
			for i := range all {
				if all[i].SessionID == sessionID && all[i].Seq > snap.Seq() {
					tail = append(tail, &all[i])
				}
			}
			return snap, tail, nil
		}
	}

	all, err := readFile(s.path)
	if err != nil {
		return nil, nil, err
	}
	var selected []*events.Event
	for i := range all {
		if all[i].SessionID == sessionID {
			selected = append(selected, &all[i])
		}
	}
	return nil, selected, nil
}

// GetEventsForArc returns the events one arc is run from: everything in its
// session up to and including the event that opened the arc, plus everything
// correlated to the arc afterwards, plus the session's task-correlated events
// and the session-level records that carry no correlation ID, so the fold
// builds the session's task table and a compaction that landed after the arc
// opened still reaches the run that resumes it.
//
// An arc is found by its opening event — the earliest event correlated to the
// arc ID whose kind is opening: a USER_MESSAGE, or a task event whose payload
// carries AsMessage visibility. The cutoff rule is otherwise unchanged. That
// query is the projection's cutoff, so a store that returned more or less of
// the arc's own events would change what the model is told. See PROJECTION.md.
//
// With UseSnapshots and a stored snapshot, the prefix up to the snapshot's
// seq is replaced by the snapshot itself: only the session's events above it
// are read, and the opening and the task IDs the cutoff needs come from the
// snapshot's fold where they are already in it and from the tail where they
// are not.
//
// A file holds one session, so the session in the ref only ever rules events
// out here. A store that held more than one would partition on it.
func (s *Store) GetEventsForArc(_ context.Context, ref common.ArcRef, opts common.ReadOptions) (*common.Snapshot, []*events.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if opts.UseSnapshots {
		snap, err := s.readSnapshot(ref.SessionID)
		if err != nil {
			return nil, nil, err
		}
		if snap != nil {
			all, err := readFile(s.path)
			if err != nil {
				return nil, nil, err
			}
			var tail []*events.Event
			for i := range all {
				if all[i].SessionID == ref.SessionID && all[i].Seq > snap.Seq() {
					tail = append(tail, &all[i])
				}
			}
			selected, err := common.SelectArcEvents(tail, ref, snap)
			if err != nil {
				return nil, nil, err
			}
			return snap, selected, nil
		}
	}

	all, err := readFile(s.path)
	if err != nil {
		return nil, nil, err
	}
	var mine []events.Event
	for i := range all {
		if all[i].SessionID == ref.SessionID {
			mine = append(mine, all[i])
		}
	}
	pointers := make([]*events.Event, 0, len(mine))
	for i := range mine {
		pointers = append(pointers, &mine[i])
	}
	selected, err := common.SelectArcEvents(pointers, ref, nil)
	if err != nil {
		return nil, nil, err
	}
	return nil, selected, nil
}

// Cancelled returns the cancel recorded against an arc, or nil where there is
// none. It scans the log, which is what everything here does.
func (s *Store) Cancelled(_ context.Context, ref common.ArcRef) (*events.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	all, err := readFile(s.path)
	if err != nil {
		return nil, err
	}
	for i := range all {
		if all[i].Typ == events.CancelEventType && belongsTo(&all[i], ref) {
			return &all[i], nil
		}
	}
	return nil, nil
}

// CancelledTask returns the task-addressed cancel recorded against the task
// named — a CANCEL whose correlation ID is the task ID — or nil where there is
// none.
func (s *Store) CancelledTask(_ context.Context, sessionID, taskID string) (*events.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	all, err := readFile(s.path)
	if err != nil {
		return nil, err
	}
	for i := range all {
		if all[i].Typ == events.CancelEventType && all[i].SessionID == sessionID && correlatesTo(&all[i], taskID) {
			return &all[i], nil
		}
	}
	return nil, nil
}

// TaskState reads what the log says about one task with a scan of the file,
// the way every other query here works. The scan itself lives in the agent
// package, so every store answers alike.
func (s *Store) TaskState(_ context.Context, sessionID, taskID string) (*common.TaskState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	all, err := pointers(readFile(s.path))
	if err != nil {
		return nil, err
	}
	return common.TaskStateOf(sessionID, taskID, all), nil
}

func correlatesTo(ev *events.Event, arcID string) bool {
	return ev.CorrelationID != nil && *ev.CorrelationID == arcID
}

// belongsTo reports whether an event is part of the arc named, in the session
// named. An arc ID is unique on its own, so the session only catches a ref
// that was built wrong.
func belongsTo(ev *events.Event, ref common.ArcRef) bool {
	return ev.SessionID == ref.SessionID && correlatesTo(ev, ref.ArcID)
}

// readFile decodes the whole log. A missing file is an empty log, since a
// session starts by writing its first event.
func readFile(path string) ([]events.Event, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	defer file.Close()

	var decoded []events.Event
	reader := bufio.NewReader(file)
	for line := 1; ; line++ {
		raw, err := reader.ReadBytes('\n')
		if len(raw) > 0 {
			var ev events.Event
			if err := json.Unmarshal(raw, &ev); err != nil {
				return nil, fmt.Errorf("decode %s line %d: %w", path, line, err)
			}
			decoded = append(decoded, ev)
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return decoded, nil
			}
			return nil, fmt.Errorf("read %s: %w", path, err)
		}
	}
}

func pointers(decoded []events.Event, err error) ([]*events.Event, error) {
	if err != nil {
		return nil, err
	}
	out := make([]*events.Event, 0, len(decoded))
	for i := range decoded {
		out = append(out, &decoded[i])
	}
	return out, nil
}

// NewID mints an identifier for an event or an arc.
func NewID() string {
	var buf [16]byte
	rand.Read(buf[:])
	return hex.EncodeToString(buf[:])
}
