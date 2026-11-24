package redisinfra

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"fillbrunn.ai/kube-agent/proto/pkg/common"
	"fillbrunn.ai/kube-agent/proto/pkg/events"
	"fillbrunn.ai/kube-agent/proto/pkg/history"
)

// logKey names the list one session's log lives under. The filestore keeps a
// session per file; this store keeps one per key, so it holds many.
func logKey(sessionID string) string {
	return "kube-agent:log:" + sessionID
}

// snapshotKey names the string one session's latest fold snapshot lives
// under. A snapshot never enters the log: it is a cache of the fold at one
// seq, kept beside the log and replaced by the next save.
func snapshotKey(sessionID string) string {
	return "kube-agent:snapshot:" + sessionID
}

// saveSnapshotScript stores a snapshot only when it is newer than the one
// held and does not reach beyond the log: the seq is compared against the
// list's length, so a snapshot can never claim events that were never
// written, and against the stored seq, so a slow writer cannot regress it.
// It returns 1 when saved, 0 when the held snapshot is at least as new, and
// -1 when the seq is beyond the log.
var saveSnapshotScript = redis.NewScript(`
local key, log = KEYS[1], KEYS[2]
local seq, record = tonumber(ARGV[1]), ARGV[2]
if seq > redis.call("LLEN", log) - 1 then
	return -1
end
local cur = redis.call("GET", key)
if cur then
	local held = cjson.decode(cur)
	if held.seq >= seq then
		return 0
	end
end
redis.call("SET", key, record)
return 1
`)

// appendScript appends a batch to a session's log in one call and returns the
// sequence number the batch starts at, which is the length the list had. Each
// entry is stamped with its number on the way in, so what is recorded is what
// a reader decodes. The script is what makes the numbering right with many
// writers: no two batches can observe the same length, and the lines of a
// batch land beside their numbers or not at all.
var appendScript = redis.NewScript(`
local start = redis.call("LLEN", KEYS[1])
for i = 1, #ARGV do
	local ev = cjson.decode(ARGV[i])
	ev.seq = start + i - 1
	redis.call("RPUSH", KEYS[1], cjson.encode(ev))
end
return start
`)

// RedisEventStore is the log of one or many sessions in Redis, implementing
// the framework's persistor and source. The filestore is the same store for
// one process; this one is for the case where the writers are many.
//
// It is an append-only list of JSON events per session, one entry per event,
// and the sequence number is the entry's index. It never compacts and holds
// the whole session on every query, the same stance as the filestore, so a
// session that outgrows a context window is a problem for compaction and not
// solved here.
//
// What it does hold to is the contract the projection depends on. See
// GetEventsForArc.
type RedisEventStore struct {
	client *redis.Client
}

// NewRedisEventStore returns a store over the client given. The client is
// borrowed rather than owned: closing it is the caller's, since a service
// will have more than one of these on the same connection pool.
func NewRedisEventStore(client *redis.Client) *RedisEventStore {
	return &RedisEventStore{client: client}
}

var _ common.EventPersistor = (*RedisEventStore)(nil)
var _ common.EventSource = (*RedisEventStore)(nil)
var _ common.CancelSource = (*RedisEventStore)(nil)

// Persist writes events and returns them as they were stored. The sequence
// number is the store's to assign, since nothing outside the append knows the
// position in the log, and it is the only thing the store assigns.
//
// An event arrives named and timed, which is common.MintIdentity's job on the
// way in. One that does not is a bug in the writer and is refused, since a
// log entry with no ID is one nothing can refer to afterwards.
//
// A batch is one append and one session, all of it or none. A batch naming
// two sessions is refused, since the two halves could not share a sequence
// and the caller would have to be told which half landed.
func (s *RedisEventStore) Persist(ctx context.Context, evs ...*events.Event) ([]*events.Event, error) {
	if len(evs) == 0 {
		return nil, nil
	}

	session := evs[0].SessionID
	lines := make([]string, 0, len(evs))
	stored := make([]*events.Event, 0, len(evs))
	for _, ev := range evs {
		if ev.SessionID != session {
			return nil, fmt.Errorf("a batch cannot span sessions %s and %s", session, ev.SessionID)
		}
		if ev.ID == "" {
			return nil, fmt.Errorf("%s event has no ID", ev.Typ)
		}
		if ev.Timestamp.IsZero() {
			return nil, fmt.Errorf("%s event %s has no timestamp", ev.Typ, ev.ID)
		}
		line, err := json.Marshal(ev)
		if err != nil {
			return nil, fmt.Errorf("marshal %s event: %w", ev.Typ, err)
		}
		lines = append(lines, string(line))
		stored = append(stored, ev)
	}

	start, err := appendScript.Run(ctx, s.client, []string{logKey(session)}, lines).Int()
	if err != nil {
		return nil, fmt.Errorf("append to %s: %w", session, err)
	}
	for i := range stored {
		// The caller's event is left alone, since they may still be holding
		// it. The copy is what carries the number the entry was given.
		one := *stored[i]
		one.Seq = start + i
		stored[i] = &one
	}
	return stored, nil
}

// SaveSnapshot stores the fold of a session at the fold's LastSeq. The store
// keeps the snapshot with the highest seq and refuses one beyond the log, so
// the snapshot never claims more than the log holds and never regresses; a
// save that loses that race is a nil error, since the newer snapshot is
// already there.
func (s *RedisEventStore) SaveSnapshot(ctx context.Context, sessionID string, fold *history.History) error {
	if fold == nil {
		return fmt.Errorf("cannot save a nil fold")
	}
	// The session is the key the snapshot is stored under, not a field of the
	// fold: a History names no session.
	record, err := common.EncodeSnapshot(&common.Snapshot{SessionID: sessionID, SavedAt: time.Now().UTC(), Fold: fold})
	if err != nil {
		return fmt.Errorf("encode snapshot of %s: %w", sessionID, err)
	}
	res, err := saveSnapshotScript.Run(ctx, s.client, []string{snapshotKey(sessionID), logKey(sessionID)}, fold.LastSeq, string(record)).Int()
	if err != nil {
		return fmt.Errorf("save snapshot of %s: %w", sessionID, err)
	}
	if res < 0 {
		return fmt.Errorf("snapshot of %s claims seq %d beyond the log", sessionID, fold.LastSeq)
	}
	return nil
}

// snapshot returns the session's latest stored snapshot, or nil where none
// has been saved.
func (s *RedisEventStore) snapshot(ctx context.Context, sessionID string) (*common.Snapshot, error) {
	line, err := s.client.Get(ctx, snapshotKey(sessionID)).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read snapshot of %s: %w", sessionID, err)
	}
	return common.DecodeSnapshot([]byte(line), sessionID)
}

// Events returns a session's whole log in the order it was written.
func (s *RedisEventStore) Events(ctx context.Context, sessionID string) ([]*events.Event, error) {
	return s.read(ctx, sessionID)
}

// GetEventsForSession returns a session's whole log in the order it was
// written. With UseSnapshots it instead returns the latest stored snapshot,
// when there is one, plus only the entries above the snapshot's seq, read
// from that index rather than from the head of the list.
func (s *RedisEventStore) GetEventsForSession(ctx context.Context, sessionID string, opts common.ReadOptions) (*common.Snapshot, []*events.Event, error) {
	if opts.UseSnapshots {
		snap, err := s.snapshot(ctx, sessionID)
		if err != nil {
			return nil, nil, err
		}
		if snap != nil {
			tail, err := s.readFrom(ctx, sessionID, snap.Seq())
			if err != nil {
				return nil, nil, err
			}
			return snap, tail, nil
		}
	}
	all, err := s.read(ctx, sessionID)
	if err != nil {
		return nil, nil, err
	}
	return nil, all, nil
}

// GetEventsForArc returns the events one arc is run from: everything in its
// session up to and including the event that opened the arc, plus everything
// correlated to the arc afterwards, plus the session's task-correlated events
// and the session-level records that carry no correlation ID — COMPACTED and
// a session-level COMPACTION_REQUESTED — so the fold builds the session's
// task table and a compaction that landed after the arc opened still reaches
// the run that resumes it.
//
// An arc is found by its opening event — the earliest event correlated to the
// arc ID whose kind is opening: a USER_MESSAGE, or a task event whose payload
// carries AsMessage visibility. The cutoff rule is otherwise unchanged. That
// query is the projection's cutoff, so a store that returned more or less of
// the arc's own events would change what the model is told. See PROJECTION.md.
//
// With UseSnapshots and a stored snapshot, the prefix up to the snapshot's
// seq is replaced by the snapshot itself: only the entries above it are read,
// and the opening and the task IDs the cutoff needs come from the snapshot's
// fold where they are already in it and from the tail where they are not.
func (s *RedisEventStore) GetEventsForArc(ctx context.Context, ref common.ArcRef, opts common.ReadOptions) (*common.Snapshot, []*events.Event, error) {
	if opts.UseSnapshots {
		snap, err := s.snapshot(ctx, ref.SessionID)
		if err != nil {
			return nil, nil, err
		}
		if snap != nil {
			tail, err := s.readFrom(ctx, ref.SessionID, snap.Seq())
			if err != nil {
				return nil, nil, err
			}
			selected, err := common.SelectArcEvents(tail, ref, snap)
			if err != nil {
				return nil, nil, err
			}
			return snap, selected, nil
		}
	}
	all, err := s.read(ctx, ref.SessionID)
	if err != nil {
		return nil, nil, err
	}
	selected, err := common.SelectArcEvents(all, ref, nil)
	if err != nil {
		return nil, nil, err
	}
	return nil, selected, nil
}

// Cancelled returns the cancel recorded against an arc, or nil where there is
// none. It scans the session, which is what everything here does.
func (s *RedisEventStore) Cancelled(ctx context.Context, ref common.ArcRef) (*events.Event, error) {
	all, err := s.read(ctx, ref.SessionID)
	if err != nil {
		return nil, err
	}
	for _, ev := range all {
		if ev.Typ == events.CancelEventType && belongsTo(ev, ref) {
			return ev, nil
		}
	}
	return nil, nil
}

// CancelledTask returns the task-addressed cancel recorded against the task
// named — a CANCEL whose correlation ID is the task ID — or nil where there is
// none.
func (s *RedisEventStore) CancelledTask(ctx context.Context, sessionID, taskID string) (*events.Event, error) {
	all, err := s.read(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	for _, ev := range all {
		if ev.Typ == events.CancelEventType && ev.SessionID == sessionID && correlatesTo(ev, taskID) {
			return ev, nil
		}
	}
	return nil, nil
}

// TaskState reads what the log says about one task with a scan of the
// session's list, the way every other query here works. The scan itself lives
// in the agent package, so every store answers alike.
func (s *RedisEventStore) TaskState(ctx context.Context, sessionID, taskID string) (*common.TaskState, error) {
	all, err := s.read(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	return common.TaskStateOf(sessionID, taskID, all), nil
}

// read decodes a session's whole log. A session nothing has been written to is
// an empty one, since a session starts by writing its first event.
func (s *RedisEventStore) read(ctx context.Context, sessionID string) ([]*events.Event, error) {
	lines, err := s.client.LRange(ctx, logKey(sessionID), 0, -1).Result()
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", sessionID, err)
	}
	return decodeLines(sessionID, lines)
}

// readFrom decodes the entries of a session's log from the given seq on — the
// tail above a snapshot — in the order they were written.
func (s *RedisEventStore) readFrom(ctx context.Context, sessionID string, fromSeq int) ([]*events.Event, error) {
	lines, err := s.client.LRange(ctx, logKey(sessionID), int64(fromSeq+1), -1).Result()
	if err != nil {
		return nil, fmt.Errorf("read %s above seq %d: %w", sessionID, fromSeq, err)
	}
	return decodeLines(sessionID, lines)
}

func decodeLines(sessionID string, lines []string) ([]*events.Event, error) {
	decoded := make([]*events.Event, 0, len(lines))
	for i, line := range lines {
		var ev events.Event
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			return nil, fmt.Errorf("decode %s entry %d: %w", sessionID, i, err)
		}
		decoded = append(decoded, &ev)
	}
	return decoded, nil
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
