package agent

import (
	"context"
	"errors"
	"fillbrunn.ai/kube-agent/proto/pkg/common"
	"sync"
	"testing"
	"time"

	"fillbrunn.ai/kube-agent/proto/pkg/events"
	"fillbrunn.ai/kube-agent/proto/pkg/history"
	"fillbrunn.ai/kube-agent/proto/pkg/users"
)

// logStore is a persistor that is also readable, which is what tests that
// write and then read back need: it persists events and answers the source
// and cancel queries from the same in-memory log. It holds at most one
// snapshot, kept at the highest seq, the way a store keeps its latest.
type logStore struct {
	mu     sync.Mutex
	events []*events.Event
	next   int
	snap   *common.Snapshot
}

func (s *logStore) Persist(_ context.Context, evs ...*events.Event) ([]*events.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	stored := make([]*events.Event, 0, len(evs))
	for _, ev := range evs {
		one := *ev
		one.Seq = s.next
		if one.ID == "" {
			one.ID = "ev-" + string(rune('a'+s.next))
		}
		if one.Timestamp.IsZero() {
			one.Timestamp = time.Unix(int64(s.next), 0)
		}
		s.next++
		s.events = append(s.events, &one)
		stored = append(stored, &one)
	}
	return stored, nil
}

// SaveSnapshot keeps the fold as the latest snapshot, at the highest seq, the
// way a store does. The fold is round-tripped through the snapshot encoding,
// so a test reading through the snapshot sees exactly what a store would
// have stored.
func (s *logStore) SaveSnapshot(_ context.Context, sessionID string, fold *history.History) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if fold == nil {
		return errors.New("nil fold")
	}
	if s.snap != nil && s.snap.Seq() >= fold.LastSeq {
		return nil
	}
	data, err := history.EncodeFold(fold)
	if err != nil {
		return err
	}
	decoded, err := history.DecodeFold(data)
	if err != nil {
		return err
	}
	s.snap = &common.Snapshot{SessionID: sessionID, SavedAt: time.Now().UTC(), Fold: decoded}
	return nil
}

// copySnapshot hands out the stored snapshot as a fresh decode, the way a
// real store decodes its stored bytes on every read: the caller folds the
// tail into the fold it is given, and the store's snapshot must not move.
func (s *logStore) copySnapshot() (*common.Snapshot, error) {
	data, err := common.EncodeSnapshot(s.snap)
	if err != nil {
		return nil, err
	}
	return common.DecodeSnapshot(data, s.snap.SessionID)
}

// GetEventsForSession returns the whole log, the way a store that holds many
// sessions returns one session's slice. With UseSnapshots it returns the
// stored snapshot, when there is one, plus only the events above its seq.
func (s *logStore) GetEventsForSession(_ context.Context, sessionID string, opts common.ReadOptions) (*common.Snapshot, []*events.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if opts.UseSnapshots && s.snap != nil {
		snap, err := s.copySnapshot()
		if err != nil {
			return nil, nil, err
		}
		var tail []*events.Event
		for _, ev := range s.events {
			if ev.SessionID == sessionID && ev.Seq > s.snap.Seq() {
				tail = append(tail, ev)
			}
		}
		return snap, tail, nil
	}
	var out []*events.Event
	for _, ev := range s.events {
		if ev.SessionID == sessionID {
			out = append(out, ev)
		}
	}
	return nil, out, nil
}

// GetEventsForArc applies the projection's cutoff the way the real store does:
// everything up to the arc's own opening event, plus everything correlated to
// it or to a task afterwards, plus the session-level records that carry no
// correlation ID, so a COMPACTED that landed after the arc opened still
// reaches the run that resumes it. With UseSnapshots the stored snapshot, when
// there is one, replaces the prefix above which the tail is selected.
func (s *logStore) GetEventsForArc(_ context.Context, ref common.ArcRef, opts common.ReadOptions) (*common.Snapshot, []*events.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// A store holds many sessions and partitions on the session in the ref,
	// the way the Redis key and the filestore's filter do.
	var mine []*events.Event
	for _, ev := range s.events {
		if ev.SessionID == ref.SessionID {
			mine = append(mine, ev)
		}
	}
	if opts.UseSnapshots && s.snap != nil {
		snap, err := s.copySnapshot()
		if err != nil {
			return nil, nil, err
		}
		var tail []*events.Event
		for _, ev := range mine {
			if ev.Seq > s.snap.Seq() {
				tail = append(tail, ev)
			}
		}
		selected, err := common.SelectArcEvents(tail, ref, snap)
		if err != nil {
			return nil, nil, err
		}
		return snap, selected, nil
	}
	selected, err := common.SelectArcEvents(mine, ref, nil)
	if err != nil {
		return nil, nil, err
	}
	return nil, selected, nil
}

func (s *logStore) Cancelled(_ context.Context, ref common.ArcRef) (*events.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ev := range s.events {
		if ev.Typ == events.CancelEventType && ev.CorrelationID != nil && *ev.CorrelationID == ref.ArcID {
			return ev, nil
		}
	}
	return nil, nil
}

func (s *logStore) CancelledTask(_ context.Context, _, taskID string) (*events.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ev := range s.events {
		if ev.Typ == events.CancelEventType && ev.CorrelationID != nil && *ev.CorrelationID == taskID {
			return ev, nil
		}
	}
	return nil, nil
}

// common.TaskState scans the in-memory log, the way a store scans its file.
func (s *logStore) TaskState(_ context.Context, sessionID, taskID string) (*common.TaskState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return common.TaskStateOf(sessionID, taskID, s.events), nil
}

// runningTaskStore is a log holding one arc that started a task and answered
// without waiting for it, which is where every report arrives.
func runningTaskStore(t *testing.T) *logStore {
	t.Helper()
	arc := testArcID
	actor := users.Actor{UserID: "alice", TenantID: "acme"}
	store := &logStore{}
	for _, ev := range []*events.Event{
		{SessionID: testSessionID, AgentID: testAgentID, Typ: events.UserMessageEventType, CorrelationID: &arc, Actor: actor,
			Payload: events.UserMessagePayload{Content: "audit the cluster", Actor: actor}},
		{SessionID: testSessionID, AgentID: testAgentID, ID: "msg-1", Typ: events.AssistantMessageEventType, CorrelationID: &arc,
			Payload: events.AssistantMessagePayload{ModelID: "gpt-5", Content: "starting the audit"}},
		{SessionID: testSessionID, AgentID: testAgentID, ID: "task-1", Typ: events.AsyncTaskInvokeEventType, CorrelationID: &arc,
			Actor: actor, Payload: events.AsyncTaskInvokePayload{TaskName: "auditor", Actor: actor}},
	} {
		if _, err := store.Persist(context.Background(), ev); err != nil {
			t.Fatalf("persist: %v", err)
		}
	}
	// The arc's opening has to carry the arc's own ID, which the store only
	// assigns to events that arrive without one.
	store.events[0].ID = arc
	return store
}

func (s *logStore) types() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return typesOf(s.events)
}
