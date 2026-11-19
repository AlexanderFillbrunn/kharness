package redisinfra

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"fillbrunn.ai/kube-agent/agent/pkg/agent"
	"fillbrunn.ai/kube-agent/proto/pkg/events"
	"fillbrunn.ai/kube-agent/agent/pkg/history"
	"fillbrunn.ai/kube-agent/proto/pkg/users"
)

func openStore(t *testing.T) *RedisEventStore {
	t.Helper()
	client, _ := newRedis(t)
	return NewRedisEventStore(client)
}

// userMessage opens an arc. It arrives named and timed, as everything written
// through agent.MintIdentity does.
func userMessage(sessionID, arcID, content string) *events.Event {
	actor := users.Actor{UserID: "dev", TenantID: "dev"}
	return &events.Event{
		SessionID:     sessionID,
		ID:            agent.NewEventID(),
		Timestamp:     time.Now().UTC(),
		Typ:           events.UserMessageEventType,
		CorrelationID: &arcID,
		Actor:         actor,
		Payload:       events.UserMessagePayload{Content: content, Actor: actor},
	}
}

func assistantMessage(sessionID, arcID, content string) *events.Event {
	return &events.Event{
		SessionID:     sessionID,
		ID:            agent.NewEventID(),
		Timestamp:     time.Now().UTC(),
		Typ:           events.AssistantMessageEventType,
		CorrelationID: &arcID,
		Payload:       events.AssistantMessagePayload{ModelID: "gpt-5", Content: content},
	}
}

func TestStorePersistNumbersFromZero(t *testing.T) {
	store := openStore(t)
	ctx := context.Background()

	stored, err := store.Persist(ctx,
		userMessage("session", "arc-1", "question"),
		assistantMessage("session", "arc-1", "answer"),
	)
	if err != nil {
		t.Fatalf("Persist: %v", err)
	}
	if stored[0].Seq != 0 || stored[1].Seq != 1 {
		t.Errorf("seqs %d and %d, want 0 and 1", stored[0].Seq, stored[1].Seq)
	}
	// The caller's event is left alone, since they may still be holding it.
	if stored[0].Seq == 0 {
		all, err := store.Events(ctx, "session")
		if err != nil {
			t.Fatalf("Events: %v", err)
		}
		if all[0].Seq != 0 {
			t.Errorf("the log holds seq %d, want 0", all[0].Seq)
		}
	}
}

func TestStorePersistRefusesAnEventWithoutAnIdentity(t *testing.T) {
	store := openStore(t)

	unnamed := userMessage("session", "arc-1", "question")
	unnamed.ID = ""
	if _, err := store.Persist(context.Background(), unnamed); err == nil {
		t.Error("an unnamed event was accepted")
	}

	untimed := userMessage("session", "arc-1", "question")
	untimed.Timestamp = time.Time{}
	if _, err := store.Persist(context.Background(), untimed); err == nil {
		t.Error("an untimed event was accepted")
	}
}

// A batch naming two sessions is refused whole, since the halves could not
// share a sequence and the caller would have to be told which half landed.
func TestStorePersistRefusesABatchSpanningSessions(t *testing.T) {
	store := openStore(t)
	ctx := context.Background()

	if _, err := store.Persist(ctx,
		userMessage("session-a", "arc-1", "question"),
		assistantMessage("session-b", "arc-1", "answer"),
	); err == nil {
		t.Error("a batch spanning sessions was accepted")
	}
	// Nothing of the refused batch landed.
	if all, err := store.Events(ctx, "session-a"); err != nil {
		t.Fatalf("Events: %v", err)
	} else if len(all) != 0 {
		t.Errorf("session-a holds %d events, want none", len(all))
	}
}

// Two writers number one log without meeting, because the append and the
// numbering are one Redis call.
func TestStorePersistNumbersConcurrentWriters(t *testing.T) {
	store := openStore(t)
	ctx := context.Background()

	const writers, batches = 4, 8
	var wg sync.WaitGroup
	seqs := make(chan int, writers*batches)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for b := 0; b < batches; b++ {
				stored, err := store.Persist(ctx, assistantMessage("session", "arc-1", "answer"))
				if err != nil {
					t.Errorf("Persist: %v", err)
					return
				}
				for _, ev := range stored {
					seqs <- ev.Seq
				}
			}
		}()
	}
	wg.Wait()
	close(seqs)

	seen := make(map[int]bool)
	for seq := range seqs {
		if seen[seq] {
			t.Fatalf("seq %d was handed to two events", seq)
		}
		seen[seq] = true
	}
	for seq := 0; seq < writers*batches; seq++ {
		if !seen[seq] {
			t.Fatalf("seq %d was never handed out", seq)
		}
	}
}

func TestStoreGetEventsForArcAppliesTheCutoff(t *testing.T) {
	store := openStore(t)
	ctx := context.Background()

	for _, ev := range []*events.Event{
		userMessage("session", "arc-1", "first question"),     // seq 0
		assistantMessage("session", "arc-1", "first answer"),  // seq 1
		userMessage("session", "arc-2", "second question"),     // seq 2
		assistantMessage("session", "arc-2", "second answer"), // seq 3
	} {
		if _, err := store.Persist(ctx, ev); err != nil {
			t.Fatalf("Persist: %v", err)
		}
	}
	// An arc can be added to after a later arc opened, and that record still
	// belongs to it.
	if _, err := store.Persist(ctx, assistantMessage("session", "arc-1", "late addition")); err != nil {
		t.Fatalf("Persist: %v", err)
	}

	selected, err := store.GetEventsForArc(ctx, agent.ArcRef{SessionID: "session", ArcID: "arc-1"})
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
	selected, err = store.GetEventsForArc(ctx, agent.ArcRef{SessionID: "session", ArcID: "arc-2"})
	if err != nil {
		t.Fatalf("GetEventsForArc: %v", err)
	}
	if len(selected) != 4 {
		t.Errorf("arc-2 selected %d events, want the 3 before it plus its own reply", len(selected))
	}
}

func TestStoreGetEventsForArcRejectsAnUnknownArc(t *testing.T) {
	store := openStore(t)
	ref := agent.ArcRef{SessionID: "session", ArcID: "absent"}
	if _, err := store.GetEventsForArc(context.Background(), ref); err == nil {
		t.Error("an unknown arc was accepted")
	}
}

// An arc is addressed by session as well as by ID, so a ref naming another
// session finds nothing even when the arc ID is one the store holds.
func TestStoreGetEventsForArcIsScopedToItsSession(t *testing.T) {
	store := openStore(t)
	ctx := context.Background()
	if _, err := store.Persist(ctx, userMessage("session", "arc-1", "first question")); err != nil {
		t.Fatalf("Persist: %v", err)
	}

	elsewhere := agent.ArcRef{SessionID: "another-session", ArcID: "arc-1"}
	if _, err := store.GetEventsForArc(ctx, elsewhere); err == nil {
		t.Error("an arc was served to a ref naming a different session")
	}
}

// Unlike the filestore this store holds many sessions, so two sessions keep
// two logs and each numbers its own from zero.
func TestStoreKeepsSessionsApart(t *testing.T) {
	store := openStore(t)
	ctx := context.Background()

	for _, ev := range []*events.Event{
		userMessage("session-a", "arc-1", "question a"),
		userMessage("session-b", "arc-1", "question b"),
	} {
		if _, err := store.Persist(ctx, ev); err != nil {
			t.Fatalf("Persist: %v", err)
		}
	}

	for _, session := range []string{"session-a", "session-b"} {
		all, err := store.Events(ctx, session)
		if err != nil {
			t.Fatalf("Events: %v", err)
		}
		if len(all) != 1 || all[0].Seq != 0 {
			t.Errorf("session %s holds %v, want its own one event numbered 0", session, all)
		}
	}
}

func TestStoreCancelledFindsTheArcsCancel(t *testing.T) {
	store := openStore(t)
	ctx := context.Background()
	ref := agent.ArcRef{SessionID: "session", ArcID: "arc-1"}

	if cancel, err := store.Cancelled(ctx, ref); err != nil {
		t.Fatalf("Cancelled: %v", err)
	} else if cancel != nil {
		t.Errorf("a cancel was found where none was written: %#v", cancel)
	}

	arcID := ref.ArcID
	reason := "enough"
	if _, err := store.Persist(ctx, &events.Event{
		SessionID: "session", ID: agent.NewEventID(), Timestamp: time.Now().UTC(),
		Typ: events.CancelEventType, CorrelationID: &arcID,
		Payload: events.CancelPayload{Reason: &reason},
	}); err != nil {
		t.Fatalf("Persist: %v", err)
	}

	cancel, err := store.Cancelled(ctx, ref)
	if err != nil {
		t.Fatalf("Cancelled: %v", err)
	}
	if cancel == nil {
		t.Fatal("the cancel was not found")
	}
}

// AC7: GetEventsForArc finds a task-opened arc by its opening event — a task
// event carrying AsMessage visibility, not a user message — and returns that
// event, everything before it, and everything correlated to the arc or to a
// task afterwards. Replay folds the arc and RenderChat shows it.
func TestStoreGetEventsForArcFindsATaskOpenedArc(t *testing.T) {
	store := openStore(t)
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
		userMessage("session", arc1, "audit the cluster"),      // seq 0
		assistantMessage("session", arc1, "starting the audit"), // seq 1
		invoke,                                                  // seq 2
		noise,                                                   // seq 3, correlated to the task
		ask,                                                     // seq 4, opens the task arc
		assistantMessage("session", taskArc, "looking at it"),   // seq 5
		answer,                                                  // seq 6, correlated to the task arc
		userMessage("session", "arc-2", "what happened?"),       // seq 7, after the cutoff
	} {
		persisted, err := store.Persist(ctx, ev)
		if err != nil {
			t.Fatalf("Persist: %v", err)
		}
		stored = append(stored, *persisted[0])
	}

	selected, err := store.GetEventsForArc(ctx, agent.ArcRef{SessionID: "session", ArcID: taskArc})
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
	selected, err = store.GetEventsForArc(ctx, agent.ArcRef{SessionID: "session", ArcID: arc1})
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
