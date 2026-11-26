package agent

import (
	"context"
	"errors"
	"fillbrunn.ai/kube-agent/proto/pkg/common"
	"fmt"
	"strings"
	"testing"
	"time"

	"fillbrunn.ai/kube-agent/proto/pkg/history"
	"fillbrunn.ai/kube-agent/proto/pkg/events"
	"fillbrunn.ai/kube-agent/proto/pkg/users"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// fakeCompactionNotifier records the compaction notifications the loop sends
// through RunOptions.OnCompactionDue.
type fakeCompactionNotifier struct {
	notified []compactionNotification
}

type compactionNotification struct {
	requestID string
}

func (f *fakeCompactionNotifier) notify(_ context.Context, requestID string) {
	f.notified = append(f.notified, compactionNotification{requestID})
}

// fakeArcEnqueuer records the arc items the completion scan requeues.
type fakeArcEnqueuer struct {
	refs []common.ArcRef
}

func (f *fakeArcEnqueuer) Enqueue(_ context.Context, ref common.ArcRef, _ string, _ int) error {
	f.refs = append(f.refs, ref)
	return nil
}

// compactionConfig is a config with compaction on and small budgets, so tests
// can cross them with short text.
func compactionConfig() *AgentConfig {
	return &AgentConfig{
		Compaction:          true,
		LowerBudget:         100,
		HigherBudget:        150,
		MaxToolResultChars:  1000,
		ArcThresholdTokens:  10000,
		ContextWindow:       32000,
	}
}

// testLimits are the worker-side compaction limits the watermark tests run
// under; the tail cap's KeepRecentTokens is set per test.
func testLimits() CompactionLimits {
	return CompactionLimits{KeepRecentTokens: 50, MaxToolResultChars: 1000, ArcThresholdTokens: 10000}
}

func compactionAgent(t *testing.T, llm LLM, cfg *AgentConfig) *Agent {
	t.Helper()
	if cfg == nil {
		cfg = compactionConfig()
	}
	agent, err := NewAgent(cfg, nil, llm)
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	return agent
}

// prunePlan is the compaction plan tests equip the compaction worker with.
func prunePlan() *CompactionPlan {
	return &CompactionPlan{Compactor: PruneCompactor{}, Limits: testLimits()}
}

// arcHistoryWith builds a history from events written straight into a
// logStore, so a test controls the whole log, sequence numbers included.
func arcHistoryWith(t *testing.T, store *logStore, evs ...events.Event) *history.History {
	t.Helper()
	if _, err := store.Persist(context.Background(), ptrs(evs...)...); err != nil {
		t.Fatalf("persist: %v", err)
	}
	_, log, err := store.GetEventsForSession(context.Background(), testSessionID, common.ReadOptions{})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	h, err := history.Replay(log)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	return h
}

// buildLog folds events with their own sequence numbers, skipping the store's
// renumbering, which is what watermark tests need: arcs at exact sequences.
func buildLog(t *testing.T, evs ...events.Event) ([]*events.Event, *history.History) {
	t.Helper()
	stored := ptrs(evs...)
	h, err := history.Replay(stored)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	return stored, h
}

func ptrs(evs ...events.Event) []*events.Event {
	out := make([]*events.Event, 0, len(evs))
	for i := range evs {
		out = append(out, &evs[i])
	}
	return out
}

func userEvent(seq int, arc, content string) events.Event {
	return events.Event{
		SessionID: testSessionID, ID: arc, Seq: seq, Timestamp: time.Unix(int64(seq), 0),
		Typ: events.UserMessageEventType, CorrelationID: &arc,
		Actor:   users.Actor{UserID: "alice", TenantID: "acme"},
		Payload: events.UserMessagePayload{Content: content, Actor: users.Actor{UserID: "alice", TenantID: "acme"}},
	}
}

func assistantEvent(seq int, arc, id, content string, usage *events.TokenUsage) events.Event {
	return events.Event{
		SessionID: testSessionID, ID: id, Seq: seq, Timestamp: time.Unix(int64(seq), 0),
		Typ: events.AssistantMessageEventType, CorrelationID: &arc,
		Payload: events.AssistantMessagePayload{ModelID: "model-1", Content: content, Usage: usage},
	}
}

func requestEvent(seq int, arc, id string, watermark int) events.Event {
	return events.Event{
		SessionID: testSessionID, ID: id, Seq: seq, Timestamp: time.Unix(int64(seq), 0),
		Typ: events.CompactionRequestedEventType, CorrelationID: &arc,
		Payload: events.CompactionRequestedPayload{Watermark: watermark},
	}
}

func suspendEvent(seq int, arc, requestID string) events.Event {
	return events.Event{
		SessionID: testSessionID, ID: fmt.Sprintf("sus-%d", seq), Seq: seq, Timestamp: time.Unix(int64(seq), 0),
		Typ: events.ArcSuspendedEventType, CorrelationID: &arc,
		Payload: events.ArcSuspendedPayload{CausationID: requestID},
	}
}

func compactedEvent(seq int, id string, through int, summary string) events.Event {
	return events.Event{
		SessionID: testSessionID, ID: id, Seq: seq, Timestamp: time.Unix(int64(seq), 0),
		Typ:     events.CompactedEventType, // no correlation ID
		Payload: events.CompactedPayload{ThroughSeq: through, Summary: summary, Method: CompactionMethodPrune},
	}
}

// AC-2: with open arcs whose bounds are 40 and 80 and a tail cap above that,
// ThroughSeq is 39 — the cut sits just below the oldest open arc's bound.
func TestComputeWatermarkOpenArcBounds(t *testing.T) {
	_, h := buildLog(t,
		userEvent(0, "a1", "closed arc"),
		assistantEvent(1, "a1", "r1", "done", nil),
		events.Event{SessionID: testSessionID, ID: "close", Seq: 2, Timestamp: time.Unix(2, 0),
			Typ: events.ArcClosedEventType, CorrelationID: ptr("a1"), Payload: events.ArcClosedPayload{CausationID: "r1"}},
		userEvent(40, "a2", "open arc two"),
		userEvent(80, "a3", "open arc three"),
	)
	limits := testLimits()
	limits.KeepRecentTokens = 1 // the tail cap reaches the end of the log, above 39
	through := computeWatermark(h, limits)
	if through != 39 {
		t.Errorf("ThroughSeq = %d, want 39 (min(40, 80) - 1)", through)
	}
}

// Two open arcs whose events interleave still bound at their openings: the
// bound is read off the arc, not off where its latest event sits.
func TestComputeWatermarkInterleavedOpenArcs(t *testing.T) {
	_, h := buildLog(t,
		userEvent(0, "a0", strings.Repeat("x", 40)),
		assistantEvent(1, "a0", "r0", "done", nil),
		events.Event{SessionID: testSessionID, ID: "close-a0", Seq: 2, Timestamp: time.Unix(2, 0),
			Typ: events.ArcClosedEventType, CorrelationID: ptr("a0"), Payload: events.ArcClosedPayload{CausationID: "r0"}},
		// a1 opens at 3, a2 at 5, and a1 speaks again at 7, after a2 opened.
		userEvent(3, "a1", strings.Repeat("y", 40)),
		assistantEvent(4, "a1", "r1", "reply one", nil),
		userEvent(5, "a2", strings.Repeat("z", 40)),
		assistantEvent(6, "a2", "r2", "reply two", nil),
		assistantEvent(7, "a1", "r3", "later reply in one", nil),
	)
	limits := testLimits()
	limits.KeepRecentTokens = 100000 // the tail cap reaches the end of the log
	through := computeWatermark(h, limits)
	// Both arcs are open and fit the threshold, so each bounds at its
	// opening; the cut is the smallest opening minus one, however the events
	// interleave above it.
	if through != 2 {
		t.Errorf("ThroughSeq = %d, want 2 (open arcs a1 at 3 and a2 at 5, min bound 3, minus 1)", through)
	}
}

// An open arc whose own log fits the threshold bounds at its opening; one
// over the threshold bounds at its live boundary, its last response.
func TestComputeWatermarkLongOpenArcBoundsAtLiveBoundary(t *testing.T) {
	_, h := buildLog(t,
		userEvent(0, "a0", "closed"),
		assistantEvent(1, "a0", "r0", "done", nil),
		events.Event{SessionID: testSessionID, ID: "close-a0", Seq: 2, Timestamp: time.Unix(2, 0),
			Typ: events.ArcClosedEventType, CorrelationID: ptr("a0"), Payload: events.ArcClosedPayload{CausationID: "r0"}},
		userEvent(3, "a1", strings.Repeat("y", 4000)),
		assistantEvent(4, "a1", "r1", strings.Repeat("c", 4000), nil),
		assistantEvent(5, "a1", "r2", "latest", nil),
	)
	limits := testLimits()
	limits.KeepRecentTokens = 100000 // the tail cap reaches the end of the log

	limits.ArcThresholdTokens = 100000 // a1 fits: the bound is its opening
	if got := computeWatermark(h, limits); got != 2 {
		t.Errorf("fitting arc: ThroughSeq = %d, want 2 (opening 3 minus 1)", got)
	}

	limits.ArcThresholdTokens = 100 // a1's log estimates over it: the bound is its live boundary
	if got := computeWatermark(h, limits); got != 4 {
		t.Errorf("over-threshold arc: ThroughSeq = %d, want 4 (live boundary 5 minus 1)", got)
	}
}

// AC-3: the tail cap is the last turn boundary at or below the point where the
// backward accumulation of per-event estimates reaches KeepRecentTokens, and
// ThroughSeq never exceeds it. Each 40-character record estimates to 10
// tokens; with K = 25 the walk stops at seq 1, itself a turn boundary.
func TestTailCapIsLastTurnBoundaryAtOrBelowTheTokenPoint(t *testing.T) {
	store := &logStore{}
	arcHistoryWith(t, store,
		userEvent(0, "a1", strings.Repeat("a", 40)),
		assistantEvent(1, "a1", "r1", strings.Repeat("b", 40), nil),
		userEvent(2, "a2", strings.Repeat("c", 40)),
		assistantEvent(3, "a2", "r2", strings.Repeat("d", 40), nil),
	)
	// Close both arcs so no bound rule applies and the cap alone decides.
	for i, arc := range []string{"a1", "a2"} {
		msg := "r1"
		if i == 1 {
			msg = "r2"
		}
		_, err := store.Persist(context.Background(), &events.Event{
			SessionID: testSessionID, ID: "close-" + arc, Seq: 4 + i, Timestamp: time.Unix(int64(4+i), 0),
			Typ: events.ArcClosedEventType, CorrelationID: &arc, Payload: events.ArcClosedPayload{CausationID: msg},
		})
		if err != nil {
			t.Fatalf("persist close: %v", err)
		}
	}
	_, log, _ := store.GetEventsForSession(context.Background(), testSessionID, common.ReadOptions{})
	h2, err := history.Replay(log)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}

	limits := testLimits()
	limits.KeepRecentTokens = 25
	if got := h2.TailCap(limits.KeepRecentTokens); got != 1 {
		t.Errorf("tailCap = %d, want 1 (accumulation reaches 25 at seq 1, a turn boundary)", got)
	}
	through := computeWatermark(h2, limits)
	if through > 1 {
		t.Errorf("ThroughSeq = %d, want at most the tail cap 1", through)
	}

	// When the whole log estimates below K, the cap constrains nothing.
	limits.KeepRecentTokens = 100000
	if got := h2.TailCap(limits.KeepRecentTokens); got != 5 {
		t.Errorf("tailCap = %d, want the end of the log 5 when the whole log is below K", got)
	}
}

// AC-4: a reply whose reported input exceeds the lower budget, above the
// watermark in force, with no request standing, leaves exactly one
// COMPACTION_REQUESTED after the round, and the run's listener is notified
// once.
func TestLoopWritesOneRequestWhenDue(t *testing.T) {
	store := &logStore{}
	h := arcHistoryWith(t, store, userEvent(0, testArcID, "how many pods?"))

	persistor := &fakePersistor{next: 1}
	notifier := &fakeCompactionNotifier{}
	llm := &scriptedLLM{replies: []*LLMResponse{{
		ModelID: "model-1", Content: "there are two", Usage: &TokenUsage{Input: 500},
	}}}
	agent := compactionAgent(t, llm, nil)

	err := agent.Run(context.Background(), RunOptions{
		Persistor: persistor, History: h, ArcID: testArcID,
		OnCompactionDue: notifier.notify,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	var requests []*events.Event
	for _, ev := range persistor.persisted {
		if ev.Typ == events.CompactionRequestedEventType {
			requests = append(requests, ev)
		}
	}
	if len(requests) != 1 {
		t.Fatalf("persisted %d COMPACTION_REQUESTED, want exactly 1 (types: %s)", len(requests), typesOf(persistor.persisted))
	}
	if requests[0].CorrelationID != nil {
		t.Errorf("the request carries correlation ID %s; it is a session-level record", *requests[0].CorrelationID)
	}
	if len(notifier.notified) != 1 || notifier.notified[0].requestID != requests[0].ID {
		t.Errorf("notifications = %+v, want one for request %s", notifier.notified, requests[0].ID)
	}
}

// AC-4: a second due round with the request still unsatisfied writes no
// duplicate — the predicate's unsatisfied-request clause holds the loop to
// one.
func TestLoopDoesNotDuplicateAStandingRequest(t *testing.T) {
	store := &logStore{}
	h := arcHistoryWith(t, store,
		userEvent(0, testArcID, "how many pods?"),
		requestEvent(1, testArcID, "req-1", -1),
	)
	persistor := &fakePersistor{next: 2}
	notifier := &fakeCompactionNotifier{}
	llm := &scriptedLLM{replies: []*LLMResponse{{
		ModelID: "model-1", Content: "two", Usage: &TokenUsage{Input: 500},
	}}}
	agent := compactionAgent(t, llm, nil)
	if err := agent.Run(context.Background(), RunOptions{
		Persistor: persistor, History: h, ArcID: testArcID, OnCompactionDue: notifier.notify,
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	for _, ev := range persistor.persisted {
		if ev.Typ == events.CompactionRequestedEventType {
			t.Errorf("a second request was written while req-1 stands: %s", typesOf(persistor.persisted))
		}
	}
	if len(notifier.notified) != 0 {
		t.Errorf("notifications sent for a standing request: %+v", notifier.notified)
	}
}

// AC-6: an admission estimate over the higher budget starts no run — the
// request and an ARC_SUSPENDED naming it are written and the arc is left
// unworked.
func TestAdmissionSuspendsOverHigherBudget(t *testing.T) {
	store := &logStore{}
	// A long message: the transcript estimate exceeds the higher budget of 150.
	h := arcHistoryWith(t, store, userEvent(0, testArcID, strings.Repeat("x", 700)))

	persistor := &fakePersistor{next: 1}
	llm := &scriptedLLM{replies: []*LLMResponse{{ModelID: "model-1", Content: "never called"}}}
	cfg := compactionConfig()
	agent := compactionAgent(t, llm, cfg)

	err := agent.Run(context.Background(), RunOptions{Persistor: persistor, History: h, ArcID: testArcID})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if llm.calls != 0 {
		t.Errorf("the model was called %d times for a suspended arc", llm.calls)
	}
	var request, suspension *events.Event
	for _, ev := range persistor.persisted {
		switch ev.Typ {
		case events.CompactionRequestedEventType:
			request = ev
		case events.ArcSuspendedEventType:
			suspension = ev
		}
	}
	if request == nil || suspension == nil {
		t.Fatalf("persisted %s, want a request and a suspension", typesOf(persistor.persisted))
	}
	if *suspension.CorrelationID != testArcID {
		t.Errorf("the suspension correlates to %s, want the arc %s", *suspension.CorrelationID, testArcID)
	}
	payload := suspension.Payload.(events.ArcSuspendedPayload)
	if payload.CausationID != request.ID {
		t.Errorf("the suspension names request %s, want %s", payload.CausationID, request.ID)
	}
}


// AC-7: a run that crosses the higher budget between rounds suspends onto the
// request its own earlier round triggered and writes no second one.
func TestRunSuspendsOntoItsOwnRequest(t *testing.T) {
	store := &logStore{}
	// Small enough to pass the admission check (estimate 10 < higher budget
	// 150); the first round crosses the lower budget, and the tool result
	// pushes the estimate over the higher budget before the next round.
	h := arcHistoryWith(t, store, userEvent(0, testArcID, strings.Repeat("x", 40)))

	persistor := &fakePersistor{next: 1}
	notifier := &fakeCompactionNotifier{}
	registry := registryWith(t, "noop", func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: strings.Repeat("r", 600)}}}, nil
	})
	llm := &scriptedLLM{replies: []*LLMResponse{
		{ModelID: "model-1", Content: strings.Repeat("c", 400), Usage: &TokenUsage{Input: 120}, ToolCalls: []LLMToolCall{{VendorID: "c1", ToolName: "noop", Args: map[string]any{}}}},
		{ModelID: "model-1", Content: "done", Usage: &TokenUsage{Input: 130}},
	}}
	cfg := compactionConfig()
	agent, err := NewAgent(cfg, registry, llm)
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}

	runErr := agent.Run(context.Background(), RunOptions{
		Persistor: persistor, History: h, ArcID: testArcID, OnCompactionDue: notifier.notify,
	})
	if runErr != nil {
		t.Fatalf("Run: %v", runErr)
	}
	var requests, suspensions int
	for _, ev := range persistor.persisted {
		switch ev.Typ {
		case events.CompactionRequestedEventType:
			requests++
		case events.ArcSuspendedEventType:
			suspensions++
		}
	}
	if requests != 1 {
		t.Errorf("%d COMPACTION_REQUESTED written, want 1 (types: %s)", requests, typesOf(persistor.persisted))
	}
	if suspensions != 1 {
		t.Errorf("%d ARC_SUSPENDED written, want 1 (types: %s)", suspensions, typesOf(persistor.persisted))
	}
	if llm.calls != 1 {
		t.Errorf("the model was called %d times; the run should have suspended before its second round", llm.calls)
	}
}

// AC-8: a classified rejection mid-run writes a non-fatal Error, then the
// request if none stands, then the suspension, and the run ends without a
// failure record.
func TestOverflowRecoveryWritesErrorRequestSuspension(t *testing.T) {
	store := &logStore{}
	h := arcHistoryWith(t, store, userEvent(0, testArcID, "how many pods?"))

	persistor := &fakePersistor{next: 1}
	overflow := &ContextOverflowError{LLMError: LLMError{Status: 400, Type: "invalid_request_error", Code: "context_length_exceeded", Message: "this model's maximum context length is 32000 tokens"}}
	llm := &scriptedLLM{failure: overflow}
	agent := compactionAgent(t, llm, nil)

	if err := agent.Run(context.Background(), RunOptions{Persistor: persistor, History: h, ArcID: testArcID}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	var failure *events.Event
	var requests, suspensions int
	for _, ev := range persistor.persisted {
		switch ev.Typ {
		case events.ErrorEventType:
			failure = ev
		case events.CompactionRequestedEventType:
			requests++
		case events.ArcSuspendedEventType:
			suspensions++
		}
	}
	if failure == nil || failure.Payload.(events.ErrorPayload).IsFatal {
		t.Errorf("the recovery Error must be non-fatal: %+v", failure)
	}
	if !strings.Contains(failure.Payload.(events.ErrorPayload).Message, "context_length_exceeded") {
		t.Errorf("the Error carries the provider's reason: %q", failure.Payload.(events.ErrorPayload).Message)
	}
	if requests != 1 || suspensions != 1 {
		t.Errorf("persisted %s, want one request and one suspension", typesOf(persistor.persisted))
	}
}

// AC-8: a second classified rejection on an arc whose suspension is already
// resolved is fatal — one recovery per arc.
func TestSecondOverflowAfterResolvedSuspensionIsFatal(t *testing.T) {
	store := &logStore{}
	// The arc was suspended, the compaction landed, and a response followed —
	// the suspension is resolved. The response asked for a call that already
	// settled, so the next step is another model call.
	h := arcHistoryWith(t, store,
		userEvent(0, testArcID, "how many pods?"),
		requestEvent(1, testArcID, "req-1", -1),
		suspendEvent(2, testArcID, "req-1"),
		compactedEvent(3, "c1", 3, "summary"),
		assistantEvent(4, testArcID, "r1", "resumed answer", &events.TokenUsage{Input: 10}),
		events.Event{SessionID: testSessionID, ID: "call-1", Seq: 5, Timestamp: time.Unix(5, 0),
			Typ: events.ToolCallEventType, CorrelationID: ptr(testArcID),
			Payload: events.ToolCallPayload{MessageID: "r1", VendorID: "v1", ToolName: "noop", Args: map[string]any{}}},
		events.Event{SessionID: testSessionID, ID: "res-1", Seq: 6, Timestamp: time.Unix(6, 0),
			Typ: events.ToolResultEventType, CorrelationID: ptr(testArcID),
			Payload: events.ToolResultPayload{CausationID: "call-1", Result: "ok"}},
	)
	persistor := &fakePersistor{next: 7}
	overflow := &ContextOverflowError{LLMError: LLMError{Status: 400, Code: "context_length_exceeded", Message: "too long"}}
	llm := &scriptedLLM{failure: overflow}
	agent := compactionAgent(t, llm, nil)

	err := agent.Run(context.Background(), RunOptions{Persistor: persistor, History: h, ArcID: testArcID})
	if err == nil {
		t.Fatal("the second rejection must fail the run")
	}
	var fatal *events.Event
	var suspensions int
	for _, ev := range persistor.persisted {
		switch ev.Typ {
		case events.ErrorEventType:
			if ev.Payload.(events.ErrorPayload).IsFatal {
				fatal = ev
			}
		case events.ArcSuspendedEventType:
			suspensions++
		}
	}
	if fatal == nil {
		t.Errorf("no fatal Error recorded (persisted %s)", typesOf(persistor.persisted))
	}
	if suspensions != 0 {
		t.Errorf("a second suspension was written for an arc that already recovered once")
	}
}

// AC-6: after the covering COMPACTED lands, the compaction worker's completion
// scan enqueues the arc whose suspension it released.
func TestCompactionWorkerComputesAndScans(t *testing.T) {
	store := &logStore{}
	// The log: a closed arc, then a suspended open arc and its request.
	arcHistoryWith(t, store,
		userEvent(0, "a0", strings.Repeat("x", 200)),
		assistantEvent(1, "a0", "r0", "done", &events.TokenUsage{Input: 10}),
		events.Event{SessionID: testSessionID, ID: "close-a0", Seq: 2, Timestamp: time.Unix(2, 0),
			Typ: events.ArcClosedEventType, CorrelationID: ptr("a0"), Payload: events.ArcClosedPayload{CausationID: "r0"}},
		userEvent(3, "a1", strings.Repeat("y", 200)),
		assistantEvent(4, "a1", "r1", "working", &events.TokenUsage{Input: 10}),
		requestEvent(5, "a1", "req-1", -1),
		suspendEvent(6, "a1", "req-1"),
	)
	enqueuer := &fakeArcEnqueuer{}
	worker, err := NewCompactionWorker(store, store, NewFixedCompactionResolver(prunePlan()),
		NewLocalSessionLease(), enqueuer, history.DefaultModes())
	if err != nil {
		t.Fatalf("NewCompactionWorker: %v", err)
	}

	worked, err := worker.Work(context.Background(), testSessionID, "req-1", testAgentID)
	if err != nil {
		t.Fatalf("Work: %v", err)
	}
	if !worked {
		t.Fatal("the item was not worked")
	}

	var compacted *events.Event
	for _, ev := range store.events {
		if ev.Typ == events.CompactedEventType {
			compacted = ev
		}
	}
	if compacted == nil {
		t.Fatal("no COMPACTED was written")
	}
	if compacted.CorrelationID != nil {
		t.Errorf("a COMPACTED carries no correlation ID, got %s", *compacted.CorrelationID)
	}
	payload := compacted.Payload.(events.CompactedPayload)
	if payload.Method != CompactionMethodPrune || payload.Summary == "" {
		t.Errorf("payload = %+v, want a prune summary", payload)
	}
	// The watermark is pinned by the open arc a1 (opening seq 3, fitting the
	// threshold): the cut sits at 3 - 1 = 2, just below the arc.
	if payload.ThroughSeq != 2 {
		t.Errorf("ThroughSeq = %d, want 2 (the open arc's bound 3, minus 1)", payload.ThroughSeq)
	}
	if len(enqueuer.refs) != 1 || enqueuer.refs[0].ArcID != "a1" {
		t.Errorf("completion scan enqueued %+v, want arc a1", enqueuer.refs)
	}
}

// An item whose request a landed COMPACTED already covers is discarded on
// read, and a session lease held by another worker leaves the item for
// autoclaim.
func TestCompactionWorkerDiscardsCoveredAndHeld(t *testing.T) {
	store := &logStore{}
	arcHistoryWith(t, store,
		userEvent(0, "a1", "q"),
		requestEvent(1, "a1", "req-1", -1),
		compactedEvent(2, "c1", 5, "already covers req-1"),
	)
	worker, err := NewCompactionWorker(store, store, NewFixedCompactionResolver(prunePlan()),
		NewLocalSessionLease(), nil, history.DefaultModes())
	if err != nil {
		t.Fatalf("NewCompactionWorker: %v", err)
	}
	worked, err := worker.Work(context.Background(), testSessionID, "req-1", testAgentID)
	if err != nil || !worked {
		t.Fatalf("Work = %v, %v; a covered request is discarded without computing", worked, err)
	}
	for _, ev := range store.events {
		if ev.Typ == events.CompactedEventType && ev.ID != "c1" {
			t.Errorf("a second COMPACTED %s was written for a covered request", ev.ID)
		}
	}

	// A held session lease leaves the item unworked.
	store2 := &logStore{}
	arcHistoryWith(t, store2, userEvent(0, "a1", "q"), requestEvent(1, "a1", "req-1", -1))
	lease := NewLocalSessionLease()
	release, ok, _ := lease.Acquire(context.Background(), testSessionID)
	if !ok {
		t.Fatal("could not take the lease")
	}
	worker2, err := NewCompactionWorker(store2, store2, NewFixedCompactionResolver(prunePlan()),
		lease, nil, history.DefaultModes())
	if err != nil {
		t.Fatalf("NewCompactionWorker: %v", err)
	}
	worked, err = worker2.Work(context.Background(), testSessionID, "req-1", testAgentID)
	if err != nil {
		t.Fatalf("Work: %v", err)
	}
	if worked {
		t.Error("the item was worked while another worker held the session lease")
	}
	release()
}

// AC-1, AC-15: a run in flight keeps the transcript it started with. The run's
// history is fetched once and folds only its own writes, so a COMPACTED that
// lands mid-run is not in force under it; the next run reads it.
func TestInFlightRunKeepsItsTranscript(t *testing.T) {
	store := &logStore{}
	h := arcHistoryWith(t, store, userEvent(0, testArcID, "how many pods?"))

	persistor := &fakePersistor{next: 1}
	// The model, called mid-run, lands a COMPACTED in the store the way another
	// worker would. The run's own history must not see it.
	llm := &scriptedLLM{}
	llm.replies = []*LLMResponse{{ModelID: "model-1", Content: "two", Usage: &TokenUsage{Input: 10}}}
	agent := compactionAgent(t, llm, nil)

	runErr := make(chan error, 1)
	go func() {
		runErr <- agent.Run(context.Background(), RunOptions{Persistor: persistor, History: h, ArcID: testArcID})
	}()
	// The compaction lands while the run is in flight.
	if _, err := store.Persist(context.Background(), &events.Event{
		SessionID: testSessionID, ID: "c-mid-run", Typ: events.CompactedEventType,
		Payload: events.CompactedPayload{ThroughSeq: 0, Summary: "MID-RUN-SUMMARY", Method: CompactionMethodPrune},
	}); err != nil {
		t.Fatalf("persist mid-run compaction: %v", err)
	}
	if err := <-runErr; err != nil {
		t.Fatalf("Run: %v", err)
	}

	if h.Watermark() != -1 {
		t.Errorf("the in-flight run's watermark moved to %d; a landing mid-run is not in force under it", h.Watermark())
	}
	transcript, err := h.Project(testArcID, history.DefaultModes())
	if err != nil {
		t.Fatalf("Project: %v", err)
	}
	for _, m := range transcript.Messages {
		if note, ok := m.(history.ModelNoteMessage); ok && note.Content == "MID-RUN-SUMMARY" {
			t.Error("the in-flight run's transcript shows the mid-run summary")
		}
	}

	// The next run, fetched fresh, reads it.
	_, fresh, err := store.GetEventsForArc(context.Background(), common.ArcRef{SessionID: testSessionID, ArcID: testArcID}, common.ReadOptions{})
	if err != nil {
		t.Fatalf("GetEventsForArc: %v", err)
	}
	h2, err := history.Replay(fresh)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if h2.Watermark() != 0 {
		t.Errorf("the next run's watermark = %d, want 0 (the mid-run COMPACTED reached it)", h2.Watermark())
	}
}

// A COMPACTED that lands mid-run is invisible to the run only because the
// cutoff now includes session-level records — the store returns them to the
// next run.
func TestCutoffIncludesSessionLevelRecords(t *testing.T) {
	store := &logStore{}
	arcHistoryWith(t, store,
		userEvent(0, "a1", "q"),
		compactedEvent(1, "c1", 0, "summary"),
		requestEvent(2, "a1", "req-1", 0),
	)
	_, got, err := store.GetEventsForArc(context.Background(), common.ArcRef{SessionID: testSessionID, ArcID: "a1"}, common.ReadOptions{})
	if err != nil {
		t.Fatalf("GetEventsForArc: %v", err)
	}
	var sawCompacted, sawRequest bool
	for _, ev := range got {
		if ev.Typ == events.CompactedEventType {
			sawCompacted = true
		}
		if ev.Typ == events.CompactionRequestedEventType {
			sawRequest = true
		}
	}
	if !sawCompacted || !sawRequest {
		t.Errorf("the cutoff dropped session-level records: compacted=%v request=%v", sawCompacted, sawRequest)
	}
}

// The bound rule never separates a tool call from its result: the cut lands on
// a turn boundary, which is where the projection keeps a call and its result
// contiguous.
func TestWatermarkNeverStraddlesCallAndResult(t *testing.T) {
	store := &logStore{}
	h := arcHistoryWith(t, store,
		userEvent(0, "a1", strings.Repeat("x", 40)),
		assistantEvent(1, "a1", "r1", strings.Repeat("y", 40), nil),
		events.Event{SessionID: testSessionID, ID: "call-1", Seq: 2, Timestamp: time.Unix(2, 0),
			Typ: events.ToolCallEventType, CorrelationID: ptr("a1"),
			Payload: events.ToolCallPayload{MessageID: "r1", VendorID: "v1", ToolName: "noop", Args: map[string]any{}}},
		events.Event{SessionID: testSessionID, ID: "res-1", Seq: 3, Timestamp: time.Unix(3, 0),
			Typ: events.ToolResultEventType, CorrelationID: ptr("a1"),
			Payload: events.ToolResultPayload{CausationID: "call-1", Result: strings.Repeat("z", 40)}},
		assistantEvent(4, "a1", "r2", strings.Repeat("w", 40), nil),
		userEvent(5, "a2", strings.Repeat("v", 40)),
	)
	limits := testLimits()
	limits.KeepRecentTokens = 15 // token point lands inside the log; the cap snaps to a turn boundary
	through := computeWatermark(h, limits)
	if through == 2 {
		t.Error("ThroughSeq = 2 separates the call at seq 2 from its result at seq 3")
	}
	// The projection above the cut keeps the pair together.
	transcript, err := h.Project("a2", history.DefaultModes(), history.ProjectionOptions{ArcThresholdTokens: limits.ArcThresholdTokens})
	if err != nil {
		t.Fatalf("Project: %v", err)
	}
	var callIDs, resultIDs []string
	for _, m := range transcript.Messages {
		switch typed := m.(type) {
		case history.ModelAssistantMessage:
			for _, c := range typed.ToolCalls {
				callIDs = append(callIDs, c.ID)
			}
		case history.ModelToolMessage:
			resultIDs = append(resultIDs, typed.ToolCallID)
		}
	}
	for _, id := range callIDs {
		found := false
		for _, r := range resultIDs {
			if r == id {
				found = true
			}
		}
		if !found && id == "call-1" {
			t.Errorf("call-1 reached the transcript above the cut without its result")
		}
	}
}

// TestPruneCompactorSummaryNamesTheSessionAndTheCounts checks what the prune
// method's note says: that the region was pruned, the first user message, and
// the counts of what was pruned by kind.
func TestPruneCompactorSummaryNamesTheSessionAndTheCounts(t *testing.T) {
	transcript := history.ModelTranscript{Messages: []history.ModelMessage{
		history.ModelTextMessage{MessageRole: history.ModelRoleUser, Content: "how many pods are running?"},
		history.ModelAssistantMessage{Content: "let me check", ToolCalls: []history.ModelToolCall{
			{ID: "c1", Name: "list_pods"}, {ID: "c2", Name: "list_pods"},
		}},
		history.ModelToolMessage{ToolCallID: "c1", Outcome: history.ModelToolOutcomeSuccess, Content: "3 pods"},
		history.ModelToolMessage{ToolCallID: "c2", Outcome: history.ModelToolOutcomeSuccess, Content: "3 pods"},
		history.ModelTextMessage{MessageRole: history.ModelRoleUser, Content: "and in the other namespace?"},
		history.ModelAssistantMessage{Content: "checking"},
		history.ModelNoteMessage{Content: "a synthesized note"},
	}}

	result, err := PruneCompactor{}.Compact(context.Background(), transcript, 42)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if result.Method != CompactionMethodPrune {
		t.Errorf("method = %q, want prune", result.Method)
	}
	for _, want := range []string{
		"seq 42",
		`"how many pods are running?"`,
		"2 user messages",
		"2 assistant messages carrying 2 tool calls",
		"2 tool results",
		"1 notes",
	} {
		if !strings.Contains(result.Summary, want) {
			t.Errorf("summary does not contain %q:\n%s", want, result.Summary)
		}
	}
	// The second user message must not displace the first.
	if strings.Contains(result.Summary, "other namespace") {
		t.Errorf("summary names a later user message as the first:\n%s", result.Summary)
	}
}

// fakeCompactionStarter records the starts the worker hands it.
type fakeCompactionStarter struct {
	starts []startedCompaction
}

type startedCompaction struct {
	sessionID string
	requestID string
	agentID   string
}

func (f *fakeCompactionStarter) StartCompaction(_ context.Context, sessionID, requestID, agentID string) {
	f.starts = append(f.starts, startedCompaction{sessionID, requestID, agentID})
}

// The worker turns a run's compaction notification into a start outside the
// run: Work wires RunOptions.OnCompactionDue to the starter, so a framework
// user that never builds a queue still gets its compactions started.
func TestArcWorkerStartsCompactionOutsideTheRun(t *testing.T) {
	store := &logStore{}
	arcHistoryWith(t, store, userEvent(0, testArcID, "how many pods?"))

	llm := &scriptedLLM{replies: []*LLMResponse{{
		ModelID: "model-1", Content: "there are two", Usage: &TokenUsage{Input: 500},
	}}}
	starter := &fakeCompactionStarter{}
	worker, err := NewArcWorker(store, store, NewFixedResolver(compactionAgent(t, llm, nil)), nil,
		WithCompactionStarter(starter))
	if err != nil {
		t.Fatalf("NewArcWorker: %v", err)
	}

	worked, err := worker.Work(context.Background(), testRef(), testAgentID, 0)
	if err != nil || !worked {
		t.Fatalf("Work = %v, %v", worked, err)
	}

	var requestID string
	_, log, err := store.GetEventsForSession(context.Background(), testSessionID, common.ReadOptions{})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	for _, ev := range log {
		if ev.Typ == events.CompactionRequestedEventType {
			requestID = ev.ID
		}
	}
	if requestID == "" {
		t.Fatal("no COMPACTION_REQUESTED in the log")
	}
	if len(starter.starts) != 1 || starter.starts[0].requestID != requestID ||
		starter.starts[0].sessionID != testSessionID || starter.starts[0].agentID != testAgentID {
		t.Errorf("starts = %+v, want one for request %s in session %s under agent %s",
			starter.starts, requestID, testSessionID, testAgentID)
	}
}

// fakeTranscriptLLM answers a transcript call with a scripted reply, for the
// LLM-summary compactor.
type fakeTranscriptLLM struct {
	reply *LLMResponse
	err   error
	got   *history.ModelTranscript
}

func (f *fakeTranscriptLLM) RespondToTranscript(_ context.Context, t history.ModelTranscript, _ string) (*LLMResponse, error) {
	f.got = &t
	return f.reply, f.err
}

func TestLLMCompactorSummarisesThroughTheModel(t *testing.T) {
	llm := &fakeTranscriptLLM{reply: &LLMResponse{
		ModelID: "summariser", Content: "the summary", Usage: &TokenUsage{Input: 10, Output: 5},
	}}
	transcript := history.ModelTranscript{Messages: []history.ModelMessage{
		history.ModelTextMessage{MessageRole: history.ModelRoleUser, Content: "q"},
	}}
	result, err := (&LLMCompactor{LLM: llm}).Compact(context.Background(), transcript, 42)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if result.Summary != "the summary" || result.Method != CompactionMethodLLM || result.ModelID != "summariser" {
		t.Errorf("result = %+v, want the summariser's reply as an llm summary", result)
	}
	if result.Usage == nil || result.Usage.Input != 10 || result.Usage.Output != 5 {
		t.Errorf("usage = %+v, want the reply's usage carried through", result.Usage)
	}
	if llm.got == nil || len(llm.got.Messages) != 1 {
		t.Error("the transcript was not handed to the model")
	}

	llm.err = errors.New("provider down")
	if _, err := (&LLMCompactor{LLM: llm}).Compact(context.Background(), transcript, 42); err == nil {
		t.Error("a failed summary call returned no error")
	}
}

// fakeEndpointLLM is an LLM with the provider compaction endpoint.
type fakeEndpointLLM struct {
	reply *LLMResponse
	err   error
}

func (f *fakeEndpointLLM) StreamResponse(context.Context, *history.ModelTranscript, *LLMOptions, common.Sink[RawToken]) (*LLMResponse, error) {
	return f.reply, f.err
}
func (f *fakeEndpointLLM) Response(context.Context, *history.ModelTranscript, *LLMOptions) (*LLMResponse, error) {
	return f.reply, f.err
}
func (f *fakeEndpointLLM) CompactTranscript(context.Context, history.ModelTranscript) (*LLMResponse, error) {
	return f.reply, f.err
}

func TestProviderCompactorUsesTheEndpoint(t *testing.T) {
	llm := &fakeEndpointLLM{reply: &LLMResponse{ModelID: "gpt-5", Content: "compacted by the provider"}}
	result, err := (&ProviderCompactor{LLM: llm}).Compact(context.Background(), history.ModelTranscript{}, 7)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if result.Summary != "compacted by the provider" || result.Method != CompactionMethodProvider {
		t.Errorf("result = %+v, want the endpoint's reply as a provider compaction", result)
	}

	// A client without the endpoint is refused, not silently skipped.
	if _, err := (&ProviderCompactor{LLM: &scriptedLLM{}}).Compact(context.Background(), history.ModelTranscript{}, 7); err == nil {
		t.Error("a model client with no compaction endpoint was accepted")
	}
}

// failingCompactor fails every compaction, and failingCompactionResolver fails
// every resolution, for the worker's error paths.
type failingCompactor struct{ err error }

func (f failingCompactor) Compact(context.Context, history.ModelTranscript, int) (CompactionResult, error) {
	return CompactionResult{}, f.err
}

type failingCompactionResolver struct{ err error }

func (f failingCompactionResolver) ResolveCompaction(context.Context, string) (*CompactionPlan, error) {
	return nil, f.err
}

func TestCompactionWorkerPropagatesCompactorFailure(t *testing.T) {
	store := &logStore{}
	arcHistoryWith(t, store, userEvent(0, "a1", "q"), requestEvent(1, "a1", "req-1", -1))
	worker, err := NewCompactionWorker(store, store, NewFixedCompactionResolver(&CompactionPlan{
		Compactor: failingCompactor{err: errors.New("the summariser is down")},
		Limits:    testLimits(),
	}), NewLocalSessionLease(), nil, history.DefaultModes())
	if err != nil {
		t.Fatalf("NewCompactionWorker: %v", err)
	}

	_, err = worker.Work(context.Background(), testSessionID, "req-1", testAgentID)
	if err == nil || !strings.Contains(err.Error(), "the summariser is down") {
		t.Fatalf("Work error = %v, want the compactor's failure", err)
	}
	for _, ev := range store.events {
		if ev.Typ == events.CompactedEventType {
			t.Error("a COMPACTED was written despite the failure")
		}
	}
}

func TestCompactionWorkerPropagatesResolverFailure(t *testing.T) {
	store := &logStore{}
	arcHistoryWith(t, store, userEvent(0, "a1", "q"), requestEvent(1, "a1", "req-1", -1))
	worker, err := NewCompactionWorker(store, store, failingCompactionResolver{err: errors.New("no such agent")},
		NewLocalSessionLease(), nil, history.DefaultModes())
	if err != nil {
		t.Fatalf("NewCompactionWorker: %v", err)
	}

	_, err = worker.Work(context.Background(), testSessionID, "req-1", testAgentID)
	if err == nil || !strings.Contains(err.Error(), "no such agent") {
		t.Fatalf("Work error = %v, want the resolver's failure", err)
	}
	for _, ev := range store.events {
		if ev.Typ == events.CompactedEventType {
			t.Error("a COMPACTED was written despite the failure")
		}
	}
}

// An arc with an outstanding suspension is left to its compaction at admission:
// no run starts, nothing is written, the model is never called.
func TestAdmissionLeavesASuspendedArcUnworked(t *testing.T) {
	store := &logStore{}
	h := arcHistoryWith(t, store,
		userEvent(0, testArcID, "how many pods?"),
		requestEvent(1, testArcID, "req-1", -1),
		suspendEvent(2, testArcID, "req-1"),
	)
	persistor := &fakePersistor{next: 3}
	llm := &scriptedLLM{replies: []*LLMResponse{{ModelID: "model-1", Content: "never called"}}}
	agent := compactionAgent(t, llm, nil)

	if err := agent.Run(context.Background(), RunOptions{Persistor: persistor, History: h, ArcID: testArcID}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if llm.calls != 0 {
		t.Errorf("the model was called %d times for a suspended arc", llm.calls)
	}
	if len(persistor.persisted) != 0 {
		t.Errorf("a suspended arc's admission wrote %s", typesOf(persistor.persisted))
	}
}

// A due check on a closed arc writes the request but never a suspension: the
// arc is done, and only a pending arc can be paused.
func TestClosedArcWritesRequestButNeverSuspends(t *testing.T) {
	store := &logStore{}
	h := arcHistoryWith(t, store,
		userEvent(0, testArcID, "how many pods?"),
		assistantEvent(1, testArcID, "r1", "two", &events.TokenUsage{Input: 500}),
		events.Event{SessionID: testSessionID, ID: "close-1", Seq: 2, Timestamp: time.Unix(2, 0),
			Typ: events.ArcClosedEventType, CorrelationID: ptr(testArcID), Payload: events.ArcClosedPayload{CausationID: "r1"}},
	)
	persistor := &fakePersistor{next: 3}
	llm := &scriptedLLM{replies: []*LLMResponse{{ModelID: "model-1", Content: "never called"}}}
	agent := compactionAgent(t, llm, nil)

	if err := agent.Run(context.Background(), RunOptions{Persistor: persistor, History: h, ArcID: testArcID}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if llm.calls != 0 {
		t.Errorf("the model was called %d times for a closed arc", llm.calls)
	}
	var requests, suspensions int
	for _, ev := range persistor.persisted {
		switch ev.Typ {
		case events.CompactionRequestedEventType:
			requests++
		case events.ArcSuspendedEventType:
			suspensions++
		}
	}
	if requests != 1 {
		t.Errorf("%d COMPACTION_REQUESTED written, want 1 (types: %s)", requests, typesOf(persistor.persisted))
	}
	if suspensions != 0 {
		t.Errorf("a closed arc was suspended: %s", typesOf(persistor.persisted))
	}
}

// A worker with no compaction starter leaves the request standing in the log:
// the run completes, the request is recorded, and no notification is made.
func TestArcWorkerWithoutStarterLeavesTheRequestStanding(t *testing.T) {
	store := &logStore{}
	arcHistoryWith(t, store, userEvent(0, testArcID, "how many pods?"))

	llm := &scriptedLLM{replies: []*LLMResponse{{
		ModelID: "model-1", Content: "there are two", Usage: &TokenUsage{Input: 500},
	}}}
	worker, err := NewArcWorker(store, store, NewFixedResolver(compactionAgent(t, llm, nil)), nil,
		WithModes(history.DefaultModes()))
	if err != nil {
		t.Fatalf("NewArcWorker: %v", err)
	}

	worked, err := worker.Work(context.Background(), testRef(), testAgentID, 0)
	if err != nil || !worked {
		t.Fatalf("Work = %v, %v", worked, err)
	}
	_, log, err := store.GetEventsForSession(context.Background(), testSessionID, common.ReadOptions{})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var requests int
	for _, ev := range log {
		if ev.Typ == events.CompactionRequestedEventType {
			requests++
		}
	}
	if requests != 1 {
		t.Errorf("%d COMPACTION_REQUESTED in the log, want 1 standing with no starter behind it", requests)
	}
}

var _ = errors.New
