package filestore

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fillbrunn.ai/kube-agent/agent/pkg/agent"
	"fillbrunn.ai/kube-agent/proto/pkg/events"
	"fillbrunn.ai/kube-agent/agent/pkg/history"
	"fillbrunn.ai/kube-agent/proto/pkg/users"
)

func openTemp(t *testing.T) *Store {
	t.Helper()
	store, err := Open(filepath.Join(t.TempDir(), "log.jsonl"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

// userMessage opens an arc. It arrives named and timed, as everything written
// through agent.MintIdentity does.
func userMessage(arcID, content string) *events.Event {
	actor := users.Actor{UserID: "dev", TenantID: "dev"}
	return &events.Event{
		SessionID:     "session",
		ID:            NewID(),
		Timestamp:     time.Now().UTC(),
		Typ:           events.UserMessageEventType,
		CorrelationID: &arcID,
		Actor:         actor,
		Payload:       events.UserMessagePayload{Content: content, Actor: actor},
	}
}

func assistantMessage(arcID, content string) *events.Event {
	return &events.Event{
		SessionID:     "session",
		ID:            NewID(),
		Timestamp:     time.Now().UTC(),
		Typ:           events.AssistantMessageEventType,
		CorrelationID: &arcID,
		Payload:       events.AssistantMessagePayload{ModelID: "gpt-5", Content: content},
	}
}

func TestPersistNumbersFromZero(t *testing.T) {
	store := openTemp(t)

	writtenFirst, err := store.Persist(context.Background(), userMessage("arc-1", "hello"))
	if err != nil {
		t.Fatalf("Persist: %v", err)
	}
	if first := writtenFirst[0]; first.Seq != 0 {
		t.Errorf("first event is seq %d, want 0", first.Seq)
	}

	writtenSecond, err := store.Persist(context.Background(), assistantMessage("arc-1", "hi"))
	if err != nil {
		t.Fatalf("Persist: %v", err)
	}
	if second := writtenSecond[0]; second.Seq != 1 {
		t.Errorf("second event is seq %d, want 1", second.Seq)
	}
}

// The store assigns the sequence number and nothing else, so an event that
// arrives unnamed or untimed is refused rather than written as it came.
func TestPersistRefusesAnEventWithoutAnIdentity(t *testing.T) {
	store := openTemp(t)

	unnamed := userMessage("arc-1", "hello")
	unnamed.ID = ""
	if _, err := store.Persist(context.Background(), unnamed); err == nil {
		t.Error("an event with no ID was written")
	}

	untimed := userMessage("arc-1", "hello")
	untimed.Timestamp = time.Time{}
	if _, err := store.Persist(context.Background(), untimed); err == nil {
		t.Error("an event with no timestamp was written")
	}

	stored, err := store.Events(context.Background())
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	if len(stored) != 0 {
		t.Errorf("the log holds %d events, want none", len(stored))
	}
}

// A reopened log carries on where it stopped, or the fold rejects the next
// event for going backwards.
func TestOpenResumesTheSequence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log.jsonl")
	first, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for _, ev := range []*events.Event{userMessage("arc-1", "hello"), assistantMessage("arc-1", "hi")} {
		if _, err := first.Persist(context.Background(), ev); err != nil {
			t.Fatalf("Persist: %v", err)
		}
	}
	first.Close()

	second, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer second.Close()

	written, err := second.Persist(context.Background(), userMessage("arc-2", "still there?"))
	if err != nil {
		t.Fatalf("Persist: %v", err)
	}
	next := written[0]
	if next.Seq != 2 {
		t.Errorf("resumed at seq %d, want 2", next.Seq)
	}

	stored, err := second.Events(context.Background())
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	if len(stored) != 3 {
		t.Fatalf("log holds %d events, want 3", len(stored))
	}
	if payload, ok := stored[0].Payload.(events.UserMessagePayload); !ok || payload.Content != "hello" {
		t.Errorf("first event decoded as %#v", stored[0].Payload)
	}
}

// ref addresses an arc of the session the fixtures write under.
func ref(arcID string) agent.ArcRef {
	return agent.ArcRef{SessionID: "session", ArcID: arcID}
}

// The cutoff is the contract the projection rests on: everything up to the
// arc's own user message, plus everything correlated to it afterwards, and
// nothing else.
func TestGetEventsForArcAppliesTheCutoff(t *testing.T) {
	store := openTemp(t)
	ctx := context.Background()

	for _, ev := range []*events.Event{
		userMessage("arc-1", "first question"),     // seq 0
		assistantMessage("arc-1", "first answer"),  // seq 1
		userMessage("arc-2", "second question"),    // seq 2
		assistantMessage("arc-2", "second answer"), // seq 3
	} {
		if _, err := store.Persist(ctx, ev); err != nil {
			t.Fatalf("Persist: %v", err)
		}
	}
	// An arc can be added to after a later arc opened, and that record still
	// belongs to it.
	if _, err := store.Persist(ctx, assistantMessage("arc-1", "late addition")); err != nil {
		t.Fatalf("Persist: %v", err)
	}

	selected, err := store.GetEventsForArc(ctx, ref("arc-1"))
	if err != nil {
		t.Fatalf("GetEventsForArc: %v", err)
	}

	var seqs []int
	for _, ev := range selected {
		seqs = append(seqs, ev.Seq)
	}
	// 0 and 1 are the arc's own; 2 and 3 belong to arc-2 and fall after the
	// cutoff; 4 is correlated to arc-1 however late it arrived.
	want := []int{0, 1, 4}
	if len(seqs) != len(want) {
		t.Fatalf("selected seqs %v, want %v", seqs, want)
	}
	for i := range want {
		if seqs[i] != want[i] {
			t.Fatalf("selected seqs %v, want %v", seqs, want)
		}
	}

	// A later arc sees the earlier one whole, because it is conversation
	// history by then.
	selected, err = store.GetEventsForArc(ctx, ref("arc-2"))
	if err != nil {
		t.Fatalf("GetEventsForArc: %v", err)
	}
	if len(selected) != 4 {
		t.Errorf("arc-2 selected %d events, want the 3 before it plus its own reply", len(selected))
	}
}

func TestGetEventsForArcRejectsAnUnknownArc(t *testing.T) {
	store := openTemp(t)
	if _, err := store.GetEventsForArc(context.Background(), ref("absent")); err == nil {
		t.Error("an unknown arc was accepted")
	}
}

// An arc is addressed by session as well as by ID, so a ref naming another
// session finds nothing even when the arc ID is one the log holds.
func TestGetEventsForArcIsScopedToItsSession(t *testing.T) {
	store := openTemp(t)
	ctx := context.Background()
	if _, err := store.Persist(ctx, userMessage("arc-1", "first question")); err != nil {
		t.Fatalf("Persist: %v", err)
	}

	elsewhere := agent.ArcRef{SessionID: "another-session", ArcID: "arc-1"}
	if _, err := store.GetEventsForArc(ctx, elsewhere); err == nil {
		t.Error("an arc was served to a ref naming a different session")
	}
}

// Sessions are kept in a directory of their own, and the first run is the one
// that has to make it.
func TestOpenCreatesTheDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sessions", "log.jsonl")
	store, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	if _, err := store.Persist(context.Background(), userMessage("arc-1", "hello")); err != nil {
		t.Fatalf("Persist: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Errorf("the log was not written: %v", err)
	}
}

// A log that is not there yet is an empty one, since a session starts by
// writing to it.
func TestOpenToleratesAMissingFile(t *testing.T) {
	store := openTemp(t)
	stored, err := store.Events(context.Background())
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	if len(stored) != 0 {
		t.Errorf("a fresh log holds %d events", len(stored))
	}
}

// AC7: GetEventsForArc finds a task-opened arc by its opening event — a task
// event carrying AsMessage visibility, not a user message — and returns that
// event, everything before it, and everything correlated to the arc or to a
// task afterwards. Replay folds the arc and RenderChat shows it.
func TestGetEventsForArcFindsATaskOpenedArc(t *testing.T) {
	store := openTemp(t)
	ctx := context.Background()

	arc1, taskArc, taskID := "arc-1", "arc-task", "task-1"
	actor := users.Actor{UserID: "dev", TenantID: "dev"}
	invoke := &events.Event{
		SessionID: "session", ID: taskID, Timestamp: time.Now().UTC(),
		Typ: events.AsyncTaskInvokeEventType, CorrelationID: &arc1, Actor: actor,
		Payload: events.AsyncTaskInvokePayload{TaskName: "auditor", Actor: actor},
	}
	noise := &events.Event{
		SessionID: "session", ID: "n-1", Timestamp: time.Now().UTC(),
		Typ: events.AsyncTaskEventEventType, CorrelationID: &taskID,
		Payload: events.AsyncTaskEventPayload{
			CausationID: taskID, Class: "detail", Payload: "noise",
			LLMVisibility: events.LLMVisibilityNone},
	}
	ask := &events.Event{
		SessionID: "session", ID: "ask-1", Timestamp: time.Now().UTC(),
		Typ: events.AsyncTaskInputRequestedEventType, CorrelationID: &taskArc, Actor: actor,
		Payload: events.AsyncTaskInputRequestedPayload{
			CausationID: taskID, Message: "which namespace?", LLMVisibility: events.LLMVisibilityAsMessage},
	}
	answer := &events.Event{
		SessionID: "session", ID: "ap-1", Timestamp: time.Now().UTC(),
		Typ: events.AsyncTaskInputProvidedEventType, CorrelationID: &taskArc, Actor: actor,
		Payload: events.AsyncTaskInputProvidedPayload{CausationID: taskID, Message: "kube-system"},
	}

	var stored []events.Event
	for _, ev := range []*events.Event{
		userMessage(arc1, "audit the cluster"),       // seq 0
		assistantMessage(arc1, "starting the audit"), // seq 1
		invoke, // seq 2
		noise,  // seq 3, correlated to the task
		ask,    // seq 4, opens the task arc
		assistantMessage(taskArc, "looking at it"), // seq 5
		answer,                                 // seq 6, correlated to the task arc
		userMessage("arc-2", "what happened?"), // seq 7, after the cutoff
	} {
		persisted, err := store.Persist(ctx, ev)
		if err != nil {
			t.Fatalf("Persist: %v", err)
		}
		stored = append(stored, *persisted[0])
	}

	selected, err := store.GetEventsForArc(ctx, ref(taskArc))
	if err != nil {
		t.Fatalf("GetEventsForArc: %v", err)
	}
	var seqs []int
	for _, ev := range selected {
		seqs = append(seqs, ev.Seq)
	}
	// Everything up to and including the opening ask, plus what correlates to
	// the arc afterwards; the later user message is not in it.
	want := []int{0, 1, 2, 3, 4, 5, 6}
	if len(seqs) != len(want) {
		t.Fatalf("selected seqs %v, want %v", seqs, want)
	}
	for i := range want {
		if seqs[i] != want[i] {
			t.Fatalf("selected seqs %v, want %v", seqs, want)
		}
	}

	// The origin arc's read includes the task-correlated event, so a run sees
	// the task table the session holds.
	selected, err = store.GetEventsForArc(ctx, ref(arc1))
	if err != nil {
		t.Fatalf("GetEventsForArc: %v", err)
	}
	if len(selected) != 4 {
		t.Errorf("arc-1 selected %d events, want its own three plus the task's notification", len(selected))
	}

	// Replay folds the task-opened arc, and RenderChat shows it.
	h, err := history.Replay("session", stored)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	var opened *history.Arc
	for _, arc := range h.Arcs {
		if arc.ID == taskArc {
			opened = arc
		}
	}
	if opened == nil {
		t.Fatal("replay minted no arc for the ask")
	}
	if opened.OpenedByTask != taskID {
		t.Errorf("arc opened by %q, want task %s", opened.OpenedByTask, taskID)
	}
	if !strings.Contains(opened.Content, "asks a question") || !strings.Contains(opened.Content, "which namespace?") {
		t.Errorf("arc content = %q, want the task note and the question", opened.Content)
	}
	if h.Tasks[taskID] == nil {
		t.Error("the session task table holds no task-1")
	}
	if out := h.RenderChat(); !strings.Contains(out, "asks a question") {
		t.Errorf("RenderChat does not show the task-opened arc:\n%s", out)
	}
}
