package history

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"fillbrunn.ai/kube-agent/proto/pkg/events"
	"fillbrunn.ai/kube-agent/proto/pkg/users"
)

// compactionLogger builds a log with explicit sequence numbers, so a test can
// place records at the sequences the scenario needs.
type compactionLogger struct {
	events []*events.Event
	start  time.Time
}

func newCompactionLogger() *compactionLogger {
	return &compactionLogger{start: time.Date(2026, 11, 19, 9, 0, 0, 0, time.UTC)}
}

func (l *compactionLogger) add(seq int, id, arc string, typ events.EventType, payload events.EventPayload) {
	var correlation *string
	if arc != "" {
		correlation = &arc
	}
	l.events = append(l.events, &events.Event{
		SessionID:     "session-1",
		ID:            id,
		Seq:           seq,
		Timestamp:     l.start.Add(time.Duration(seq) * time.Second),
		Typ:           typ,
		CorrelationID: correlation,
		Actor:         testActor,
		Payload:       payload,
	})
}

func (l *compactionLogger) user(seq int, arc, content string) {
	l.add(seq, arc, arc, events.UserMessageEventType, events.UserMessagePayload{Content: content, Actor: testActor})
}

func (l *compactionLogger) assistant(seq int, arc, id, content string, usage *events.TokenUsage) {
	l.add(seq, id, arc, events.AssistantMessageEventType, events.AssistantMessagePayload{
		ModelID: "model-1", Content: content, Usage: usage,
	})
}

func (l *compactionLogger) toolCall(seq int, arc, message, id, name string, args any) {
	l.add(seq, id, arc, events.ToolCallEventType, events.ToolCallPayload{MessageID: message, VendorID: "v-" + id, ToolName: name, Args: args})
}

func (l *compactionLogger) toolResult(seq int, arc, call string, result any) {
	l.add(seq, fmt.Sprintf("result-%s", call), arc, events.ToolResultEventType, events.ToolResultPayload{CausationID: call, Result: result})
}

func (l *compactionLogger) close(seq int, arc, message string) {
	l.add(seq, fmt.Sprintf("close-%s", arc), arc, events.ArcClosedEventType, events.ArcClosedPayload{CausationID: message})
}

func (l *compactionLogger) request(seq int, arc, id string, watermark int) {
	// Correlated to the arc it was produced in; an empty arc means a request
	// written outside any run.
	l.add(seq, id, arc, events.CompactionRequestedEventType, events.CompactionRequestedPayload{Watermark: watermark})
}

func (l *compactionLogger) suspend(seq int, arc, requestID string) {
	l.add(seq, fmt.Sprintf("suspend-%s-%d", arc, seq), arc, events.ArcSuspendedEventType, events.ArcSuspendedPayload{CausationID: requestID})
}

func (l *compactionLogger) compacted(seq int, id string, through int, summary string) {
	// No correlation ID: the cutoff rule gives the record its scope.
	l.add(seq, id, "", events.CompactedEventType, events.CompactedPayload{
		ThroughSeq: through, Summary: summary, Method: "prune",
	})
}

func (l *compactionLogger) invoke(seq int, arc, id, task string, args any) {
	l.add(seq, id, arc, events.AsyncTaskInvokeEventType, events.AsyncTaskInvokePayload{TaskName: task, Args: args, Actor: testActor})
}

func (l *compactionLogger) fold(t *testing.T) *History {
	t.Helper()
	h, err := Replay(l.events)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	return h
}

// AC-1, AC-14: the compaction in force is the one with the greatest
// ThroughSeq among those written, not the one that arrived last — a compaction
// computed through 100 but written at 141 is honoured by its watermark, and a
// later-arriving one with a smaller watermark replaces nothing.
func TestLastCompactionFollowsCoverageNotArrival(t *testing.T) {
	log := newCompactionLogger()
	log.user(0, "a1", "first question")
	log.assistant(1, "a1", "r1", "first answer", nil)
	log.compacted(141, "c1", 100, "summary through 100")
	log.compacted(150, "c2", 120, "summary through 120")
	log.compacted(160, "c3", 110, "summary through 110")

	h := log.fold(t)
	if h.LastCompaction == nil || h.LastCompaction.ThroughSeq != 120 {
		t.Fatalf("LastCompaction = %+v, want ThroughSeq 120", h.LastCompaction)
	}
	if h.Watermark() != 120 {
		t.Errorf("Watermark() = %d, want 120", h.Watermark())
	}
}

// A request is satisfied by the first COMPACTED whose watermark covers the
// request's — reaching the floor counts.
func TestRequestSatisfiedByCoveringWatermark(t *testing.T) {
	log := newCompactionLogger()
	log.user(0, "a1", "q")
	log.request(1, "a1", "req-1", 100)
	log.compacted(2, "c1", 99, "below the floor")
	h := log.fold(t)
	if h.CompactionRequestSatisfied(h.CompactionRequestByID("req-1")) {
		t.Error("a COMPACTED through 99 satisfies a request for 100 — it does not cover the floor")
	}
	if h.UnsatisfiedRequest() == nil {
		t.Error("the request should stand unsatisfied")
	}

	log2 := newCompactionLogger()
	log2.user(0, "a1", "q")
	log2.request(1, "a1", "req-1", 100)
	log2.compacted(2, "c1", 100, "reaches the floor")
	h2 := log2.fold(t)
	if !h2.CompactionRequestSatisfied(h2.CompactionRequestByID("req-1")) {
		t.Error("a COMPACTED through 100 covers a request for 100")
	}
	if h2.UnsatisfiedRequest() != nil {
		t.Error("no request should stand unsatisfied")
	}
}

// The suspension states are derived: outstanding until a covering COMPACTED
// lands, released from then until a response follows, resolved once one does.
func TestSuspensionStatesDeriveFromTheLog(t *testing.T) {
	log := newCompactionLogger()
	log.user(0, "a1", "q")
	log.request(1, "a1", "req-1", -1)
	log.suspend(2, "a1", "req-1")
	h := log.fold(t)
	arc := h.Arcs[0]
	if got := h.SuspensionStateOf(arc, arc.Suspensions[0]); got != SuspensionOutstanding {
		t.Fatalf("state = %s, want OUTSTANDING", got)
	}

	log.compacted(3, "c1", 10, "summary")
	h = log.fold(t)
	arc = h.Arcs[0]
	if got := h.SuspensionStateOf(arc, arc.Suspensions[0]); got != SuspensionReleased {
		t.Fatalf("state = %s, want RELEASED", got)
	}
	if !h.ArcSuspended(arc) {
		t.Error("a released, unresolved suspension still suspends the arc")
	}

	log.assistant(4, "a1", "r1", "answer", nil)
	h = log.fold(t)
	arc = h.Arcs[0]
	if got := h.SuspensionStateOf(arc, arc.Suspensions[0]); got != SuspensionResolved {
		t.Fatalf("state = %s, want RESOLVED", got)
	}
	if h.ArcSuspended(arc) {
		t.Error("a resolved suspension does not suspend the arc")
	}
}

// AC-5: CompactionDue reads the last reply's reported input, and where the
// provider reported nothing it falls back to the transcript estimate — nil
// never counts as "not due", and a reported zero counts as zero.
func TestCompactionDue(t *testing.T) {
	build := func(usage *events.TokenUsage, compactedThrough int) *History {
		log := newCompactionLogger()
		log.user(0, "a1", strings.Repeat("x", 400)) // estimate 100
		log.assistant(1, "a1", "r1", strings.Repeat("y", 400), usage)
		if compactedThrough >= 0 {
			log.compacted(2, "c1", compactedThrough, "summary")
		}
		return log.fold(t)
	}

	if !build(&events.TokenUsage{Input: 500}, -1).CompactionDue(100) {
		t.Error("usage 500 over budget 100 with no compaction is due")
	}
	if build(&events.TokenUsage{Input: 50}, -1).CompactionDue(100) {
		t.Error("usage 50 under budget 100 is not due")
	}
	// A reply at or below the watermark in force does not count: its cost was
	// measured against the replaced transcript.
	if build(&events.TokenUsage{Input: 500}, 1).CompactionDue(100) {
		t.Error("a reply at the watermark is not due, however large its usage")
	}
	// An unsatisfied request means one is already standing.
	log := newCompactionLogger()
	log.user(0, "a1", strings.Repeat("x", 400))
	log.assistant(1, "a1", "r1", "a", &events.TokenUsage{Input: 500})
	log.request(2, "a1", "req-1", -1)
	if log.fold(t).CompactionDue(100) {
		t.Error("due with an unsatisfied request standing — the loop must not write a second one")
	}
	// Nil usage falls back to the estimate; zero usage counts zero.
	if !build(nil, -1).CompactionDue(100) {
		t.Error("nil usage must fall back to the transcript estimate, which is 100+ here, not count as not due")
	}
	if build(&events.TokenUsage{Input: 0}, -1).CompactionDue(100) {
		t.Error("a reported zero is a measured zero, not over budget")
	}
}

// AC-13: the estimate is ceil(chars/4) over the projected text, identically
// for every model.
func TestEstimateTokensTable(t *testing.T) {
	cases := []struct {
		text string
		want int
	}{
		{"", 0},
		{"a", 1},
		{"abcd", 1},
		{"abcde", 2},
		{strings.Repeat("x", 40), 10},
		{"日本語", 1}, // four characters, not twelve bytes
	}
	for _, c := range cases {
		if got := EstimateTokens(c.text); got != c.want {
			t.Errorf("EstimateTokens(%q) = %d, want %d", c.text, got, c.want)
		}
	}

	log := newCompactionLogger()
	log.user(0, "a1", "abcd")
	if got := EstimateEvent(log.events[0]); got != 1 {
		t.Errorf("EstimateEvent(user message of 4 chars) = %d, want 1", got)
	}
	log.toolResult(1, "a1", "call-1", "abcdefgh")
	if got := EstimateEvent(log.events[1]); got != 2 {
		t.Errorf("EstimateEvent(tool result of 8 chars) = %d, want 2", got)
	}
}

// REQ-6: a turn's transcript is the summary plus the projection of the events
// above the watermark; the events at or below it are replaced.
func TestProjectionReadsSummaryPlusEventsAboveWatermark(t *testing.T) {
	log := newCompactionLogger()
	log.user(0, "a1", "first question")
	log.assistant(1, "a1", "r1", "first answer", nil)
	log.close(2, "a1", "r1")
	log.user(3, "a2", "second question")
	log.compacted(4, "c1", 2, "SUMMARY-OF-ARC-ONE")
	h := log.fold(t)

	transcript, err := h.Project("a2", DefaultModes())
	if err != nil {
		t.Fatalf("Project: %v", err)
	}
	if len(transcript.Messages) == 0 {
		t.Fatal("empty transcript")
	}
	head, ok := transcript.Messages[0].(ModelNoteMessage)
	if !ok {
		t.Fatalf("head message is %T, want a note carrying the summary", transcript.Messages[0])
	}
	if head.Content != "SUMMARY-OF-ARC-ONE" {
		t.Errorf("head note = %q, want the summary", head.Content)
	}
	for _, m := range transcript.Messages {
		if text, ok := m.(ModelTextMessage); ok && text.Content == "first question" {
			t.Error("the summarised arc's opening request reached the transcript")
		}
	}
	// The arc above the watermark is whole.
	var found bool
	for _, m := range transcript.Messages {
		if text, ok := m.(ModelTextMessage); ok && text.Content == "second question" {
			found = true
		}
	}
	if !found {
		t.Error("the arc above the watermark is missing from the transcript")
	}
}

// AC-11: a suspended arc the cut passes through renders as a skeleton — the
// opening request verbatim, the calls without their results above the cut, the
// precedence note at the cut, and the live part whole below it.
func TestProjectionSpanningArcRendersAsSkeleton(t *testing.T) {
	log := newCompactionLogger()
	log.user(0, "a1", "OPENING-REQUEST")
	// Spent prefix: a reply with a call and its result, all below the cut.
	log.assistant(1, "a1", "r1", "spent reply", nil)
	log.toolCall(2, "a1", "r1", "call-1", "list_pods", map[string]any{"ns": "default"})
	log.toolResult(3, "a1", "call-1", "SPENT-RESULT")
	// The cut.
	log.compacted(4, "c1", 3, "SUMMARY")
	log.request(5, "a1", "req-1", 3)
	log.suspend(6, "a1", "req-1")
	// Live part: a reply with a call and result, above the cut.
	log.assistant(7, "a1", "r2", "live reply", nil)
	log.toolCall(8, "a1", "r2", "call-2", "list_pods", nil)
	log.toolResult(9, "a1", "call-2", "LIVE-RESULT")
	h := log.fold(t)

	transcript, err := h.Project("a1", DefaultModes(), ProjectionOptions{ArcThresholdTokens: 1})
	if err != nil {
		t.Fatalf("Project: %v", err)
	}
	var texts []string
	for _, m := range transcript.Messages {
		switch typed := m.(type) {
		case ModelTextMessage:
			texts = append(texts, "text:"+typed.Content)
		case ModelAssistantMessage:
			texts = append(texts, "assistant:"+typed.Content)
			for _, c := range typed.ToolCalls {
				texts = append(texts, "call:"+c.ID)
			}
		case ModelToolMessage:
			texts = append(texts, "result:"+typed.ToolCallID+":"+typed.Content)
		case ModelNoteMessage:
			texts = append(texts, "note:"+typed.Content)
		}
	}
	joined := strings.Join(texts, "\n")
	for _, want := range []string{
		"note:SUMMARY",
		"text:OPENING-REQUEST",      // the opening request, verbatim
		"call:call-1",               // the spent call stays
		"note:[earlier work",        // the note at the cut
		"paused while its context",  // the suspension is named
		"call:call-2",               // the live part is whole
		"result:call-2:LIVE-RESULT", // with its result
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("transcript is missing %q:\n%s", want, joined)
		}
	}
	if strings.Contains(joined, "SPENT-RESULT") {
		t.Errorf("the spent prefix's result reached the transcript:\n%s", joined)
	}
	if strings.Contains(joined, "spent reply") {
		t.Errorf("the spent prefix's reply text reached the transcript:\n%s", joined)
	}
}

// A spanning arc that fits the threshold renders whole, not as a skeleton.
func TestProjectionSpanningArcThatFitsRendersWhole(t *testing.T) {
	log := newCompactionLogger()
	log.user(0, "a1", "OPENING")
	log.assistant(1, "a1", "r1", "small reply", nil)
	log.compacted(2, "c1", 1, "SUMMARY")
	log.assistant(3, "a1", "r2", "later reply", nil)
	h := log.fold(t)

	transcript, err := h.Project("a1", DefaultModes(), ProjectionOptions{ArcThresholdTokens: 10000})
	if err != nil {
		t.Fatalf("Project: %v", err)
	}
	joined := renderAll(transcript.Messages)
	if !strings.Contains(joined, "small reply") || !strings.Contains(joined, "later reply") {
		t.Errorf("a fitting spanning arc renders whole; transcript:\n%s", joined)
	}
}

// AC-9: a tool result over MaxToolResultChars is thinned in the projection —
// truncated with a marker naming the call and the omitted size — and the
// thinning is never an IsError outcome.
func TestProjectionThinsOversizedToolResult(t *testing.T) {
	log := newCompactionLogger()
	log.user(0, "a1", "q")
	log.assistant(1, "a1", "r1", "checking", nil)
	log.toolCall(2, "a1", "r1", "call-1", "dump", nil)
	log.toolResult(3, "a1", "call-1", strings.Repeat("z", 100))
	h := log.fold(t)

	transcript, err := h.Project("a1", DefaultModes(), ProjectionOptions{MaxToolResultChars: 10})
	if err != nil {
		t.Fatalf("Project: %v", err)
	}
	var tool *ModelToolMessage
	for i, m := range transcript.Messages {
		if tm, ok := m.(ModelToolMessage); ok {
			tool = &tm
			_ = i
		}
	}
	if tool == nil {
		t.Fatal("no tool message in the transcript")
	}
	if tool.IsError {
		t.Errorf("the thinned result reads as an error: %+v", tool)
	}
	if tool.Outcome != ModelToolOutcomeSuccess {
		t.Errorf("outcome = %s, want SUCCESS — the harness thinned it, the tool did not fail", tool.Outcome)
	}
	if !strings.Contains(tool.Content, "zzzzzzzzzz") || strings.Contains(tool.Content, "zzz\n") {
		// truncated to the cap
	}
	if !strings.Contains(tool.Content, "truncated by the harness") || !strings.Contains(tool.Content, "call-1") || !strings.Contains(tool.Content, "90") {
		t.Errorf("the marker must name the call and the omitted size:\n%s", tool.Content)
	}
	if strings.Count(tool.Content, "z") != 10 {
		t.Errorf("the result was not truncated to the cap: %d z's", strings.Count(tool.Content, "z"))
	}

	// With no cap the whole result reaches the transcript.
	full, err := h.Project("a1", DefaultModes(), ProjectionOptions{})
	if err != nil {
		t.Fatalf("Project: %v", err)
	}
	if !strings.Contains(renderAll(full.Messages), strings.Repeat("z", 100)) {
		t.Error("with no cap the whole result must reach the transcript")
	}
}

// AC-12: a task whose invocation falls below the watermark still gets its
// invocation note, synthesized from the task table with the ID verbatim.
func TestProjectionSynthesizesTaskInvocationNoteBelowWatermark(t *testing.T) {
	log := newCompactionLogger()
	log.user(0, "a1", "start a job")
	log.assistant(1, "a1", "r1", "starting", nil)
	log.toolCall(2, "a1", "r1", "call-1", "auditor", nil)
	log.toolResult(3, "a1", "call-1", "started")
	log.invoke(4, "a1", "task-1", "auditor", map[string]any{"depth": 2})
	log.compacted(5, "c1", 4, "SUMMARY")
	log.user(6, "a2", "next question")
	h := log.fold(t)

	transcript, err := h.Project("a2", DefaultModes())
	if err != nil {
		t.Fatalf("Project: %v", err)
	}
	joined := renderAll(transcript.Messages)
	if !strings.Contains(joined, "task-1") {
		t.Errorf("the synthesized invocation note must carry the task ID verbatim:\n%s", joined)
	}
	if !strings.Contains(joined, "auditor (id task-1)") {
		t.Errorf("the note must name the task and its ID:\n%s", joined)
	}
	if strings.Contains(joined, "start a job") {
		t.Errorf("the summarised arc's opening reached the transcript:\n%s", joined)
	}
}

// ProjectThrough is the compactor's input: the previous summary at the head
// plus what landed since, capped at the watermark being computed.
func TestProjectThroughBuildsTheRegion(t *testing.T) {
	log := newCompactionLogger()
	log.user(0, "a1", "first question")
	log.assistant(1, "a1", "r1", "first answer", nil)
	log.close(2, "a1", "r1")
	log.compacted(3, "c1", 2, "OLD-SUMMARY")
	log.user(4, "a2", "second question")
	log.assistant(5, "a2", "r2", "second answer", nil)
	h := log.fold(t)

	transcript, err := h.ProjectThrough(5, DefaultModes(), ProjectionOptions{})
	if err != nil {
		t.Fatalf("ProjectThrough: %v", err)
	}
	joined := renderAll(transcript.Messages)
	if !strings.Contains(joined, "OLD-SUMMARY") {
		t.Errorf("the region starts with the previous summary:\n%s", joined)
	}
	if !strings.Contains(joined, "second question") || !strings.Contains(joined, "second answer") {
		t.Errorf("the events since the last compaction are missing:\n%s", joined)
	}
	if strings.Contains(joined, "first question") || strings.Contains(joined, "first answer") {
		t.Errorf("events at or below the old watermark are already in the summary and must not repeat:\n%s", joined)
	}
}

func renderAll(messages []ModelMessage) string {
	var parts []string
	for _, m := range messages {
		switch typed := m.(type) {
		case ModelTextMessage:
			parts = append(parts, typed.Content)
		case ModelAssistantMessage:
			parts = append(parts, typed.Content)
		case ModelToolMessage:
			parts = append(parts, typed.Content)
		case ModelNoteMessage:
			parts = append(parts, typed.Content)
		}
	}
	return strings.Join(parts, "\n")
}

var _ = users.Actor{}

// A request correlated to an arc the log does not hold is rejected at the
// fold, like any other reference to a missing record; one written outside any
// run carries no correlation and folds fine.
func TestCompactionRequestCorrelationIsValidated(t *testing.T) {
	log := newCompactionLogger()
	log.user(0, "a1", "q")
	log.request(1, "no-such-arc", "req-bad", -1)
	if _, err := Replay(log.events); err == nil {
		t.Error("a request correlated to a missing arc folded")
	}

	log2 := newCompactionLogger()
	log2.user(0, "a1", "q")
	log2.request(1, "", "req-free", -1)
	h, err := Replay(log2.events)
	if err != nil {
		t.Fatalf("a request with no correlation should fold: %v", err)
	}
	if h.CompactionRequestByID("req-free") == nil {
		t.Error("the uncorrelated request was not recorded")
	}
}

// ProjectedText renders the text a record contributes to the estimate, per
// payload kind: the material the model would be told for that record alone.
// The estimate is ceil(len/4) over exactly this text, so the table pins what
// each record kind counts as.
func TestProjectedTextPerPayload(t *testing.T) {
	reason := "because"
	cases := []struct {
		name    string
		payload events.EventPayload
		want    string
	}{
		{"user message", events.UserMessagePayload{Content: "hello"}, "hello"},
		{"assistant message", events.AssistantMessagePayload{Content: "hi"}, "hi"},
		{"assistant with reasoning", events.AssistantMessagePayload{Content: "hi", Reasoning: []byte("thinking")}, "hi\nthinking"},
		{"tool call", events.ToolCallPayload{ToolName: "list_pods", Args: map[string]any{"ns": "default"}}, `list_pods {"ns":"default"}`},
		{"tool result string", events.ToolResultPayload{Result: "3 pods"}, "3 pods"},
		{"tool result object", events.ToolResultPayload{Result: map[string]any{"pods": 3}}, `{"pods":3}`},
		{"tool event", events.ToolEventPayload{Payload: "step 1"}, `"step 1"`},
		{"dispatch renders nothing", events.ToolDispatchedPayload{}, ""},
		{"interrupt", events.InterruptPayload{Reason: "needs approval", Context: map[string]any{"x": 1}}, `needs approval {"x":1}`},
		{"interrupt response", events.InterruptResponsePayload{Response: "yes"}, `"yes"`},
		{"task invoke", events.AsyncTaskInvokePayload{TaskName: "deploy", Args: map[string]any{"v": "2"}}, `deploy {"v":"2"}`},
		{"task result", events.AsyncTaskResultPayload{Result: "done"}, "done"},
		{"task event", events.AsyncTaskEventPayload{Payload: "progress"}, "progress"},
		{"input requested", events.AsyncTaskInputRequestedPayload{Message: "which env?"}, "which env?"},
		{"input provided", events.AsyncTaskInputProvidedPayload{Message: "prod"}, "prod"},
		{"error", events.ErrorPayload{Message: "boom"}, "boom"},
		{"guardrail", events.GuardrailBlockedPayload{Message: "blocked"}, "blocked"},
		{"cancel with reason", events.CancelPayload{Reason: &reason}, "because"},
		{"cancel without reason", events.CancelPayload{}, ""},
		{"arc closed", events.ArcClosedPayload{}, ""},
		{"compacted carries the summary", events.CompactedPayload{Summary: "a summary"}, "a summary"},
		{"request renders nothing", events.CompactionRequestedPayload{}, ""},
		{"suspension renders nothing", events.ArcSuspendedPayload{}, ""},
	}
	for _, c := range cases {
		ev := &events.Event{Typ: c.payload.EventType(), Payload: c.payload}
		if got := ProjectedText(ev); got != c.want {
			t.Errorf("%s: ProjectedText = %q, want %q", c.name, got, c.want)
		}
	}
}
