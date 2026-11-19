package agent

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"fillbrunn.ai/kube-agent/proto/pkg/events"
	"fillbrunn.ai/kube-agent/proto/pkg/users"
)

// logStore is a persistor that is also readable, which is what tests that
// write and then read back need: it persists events and answers the source
// and cancel queries from the same in-memory log.
type logStore struct {
	mu     sync.Mutex
	events []*events.Event
	next   int
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

// GetEventsForSession returns the whole log, the way a store that holds many
// sessions returns one session's slice.
func (s *logStore) GetEventsForSession(_ context.Context, sessionID string) ([]*events.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*events.Event
	for _, ev := range s.events {
		if ev.SessionID == sessionID {
			out = append(out, ev)
		}
	}
	return out, nil
}

// GetEventsForArc applies the projection's cutoff the way the real store does:
// everything up to the arc's own opening event, plus everything correlated to
// it or to a task afterwards.
func (s *logStore) GetEventsForArc(_ context.Context, ref ArcRef) ([]*events.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	taskIDs := map[string]bool{}
	opening := -1
	for _, ev := range s.events {
		if ev.Typ == events.AsyncTaskInvokeEventType {
			taskIDs[ev.ID] = true
		}
		if opening < 0 && ev.CorrelationID != nil && *ev.CorrelationID == ref.ArcID && events.OpensArc(ev) {
			opening = ev.Seq
		}
	}
	if opening < 0 {
		return nil, errors.New("no arc " + ref.ArcID)
	}
	var selected []*events.Event
	for _, ev := range s.events {
		if ev.Seq <= opening || (ev.CorrelationID != nil && (*ev.CorrelationID == ref.ArcID || taskIDs[*ev.CorrelationID])) {
			selected = append(selected, ev)
		}
	}
	return selected, nil
}

func (s *logStore) Cancelled(_ context.Context, ref ArcRef) (*events.Event, error) {
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

// TaskState scans the in-memory log, the way a store scans its file.
func (s *logStore) TaskState(_ context.Context, sessionID, taskID string) (*TaskState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return TaskStateOf(sessionID, taskID, s.events), nil
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
