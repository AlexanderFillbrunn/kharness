package harness

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"

	"fillbrunn.ai/kube-agent/agent/pkg/agent"
	"fillbrunn.ai/kube-agent/devchat/internal/filestore"
	"fillbrunn.ai/kube-agent/proto/pkg/common"
	"fillbrunn.ai/kube-agent/proto/pkg/events"
	"fillbrunn.ai/kube-agent/proto/pkg/history"
	"fillbrunn.ai/kube-agent/proto/pkg/users"
)

// interruptibleModel returns a model over a session that writes the way the
// real one does, with one arc already opened, and no run in flight.
func interruptibleModel(t *testing.T, arc string) (model, *filestore.Store) {
	t.Helper()

	store, err := filestore.Open(filepath.Join(t.TempDir(), "log.jsonl"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	actor := users.Actor{UserID: "dev", TenantID: "dev"}
	s := &session{
		id:        "session",
		store:     store,
		persistor: common.MintIdentity(common.GuardCancellation(store, store)),
		actor:     actor,
	}
	if _, err := s.persistor.Persist(context.Background(), &events.Event{
		SessionID: s.id, AgentID: localAgentID, Typ: events.UserMessageEventType, CorrelationID: &arc, Actor: actor,
		Payload: events.UserMessagePayload{Content: "deploy it", Actor: actor},
	}); err != nil {
		t.Fatalf("Persist: %v", err)
	}

	settings := agent.AgentSettings{Model: agent.ModelRef{Source: "openai", Name: "gpt-5"}}
	m := newModel(s, &sender{}, settings, "", newTranscript()).resize(80, 24)
	return m, store
}

func storeEvents(t *testing.T, store *filestore.Store) []*events.Event {
	t.Helper()
	stored, err := store.Events(context.Background())
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	return stored
}

func lastEvent(t *testing.T, store *filestore.Store) *events.Event {
	t.Helper()
	stored := storeEvents(t, store)
	if len(stored) == 0 {
		t.Fatal("the log is empty")
	}
	return stored[len(stored)-1]
}

func findEvent(t *testing.T, store *filestore.Store, typ events.EventType) *events.Event {
	t.Helper()
	for _, ev := range storeEvents(t, store) {
		if ev.Typ == typ {
			return ev
		}
	}
	return nil
}

// questionsContext is what an ask_user call's arguments look like as an
// interrupt's context.
func questionsContext() map[string]any {
	return map[string]any{
		"questions": []any{
			map[string]any{"id": "cluster", "text": "Which cluster?", "kind": "single_choice", "options": []any{"prod", "staging"}},
			map[string]any{"id": "note", "text": "Anything to add?", "kind": "text", "required": false},
		},
	}
}

// An interrupt whose context carries questions renders as a form block, and
// the response marks it answered.
func TestTranscriptRendersQuestionsForm(t *testing.T) {
	m, store := interruptibleModel(t, "arc-1")
	arc := "arc-1"

	// An interrupt whose context carries questions, as the ask tool writes it.
	stored, err := m.session.persistor.Persist(context.Background(), &events.Event{
		SessionID: "session", AgentID: localAgentID, Typ: events.InterruptEventType,
		CorrelationID: &arc, Actor: m.session.actor,
		Payload: events.InterruptPayload{Reason: "Ask the user", Context: questionsContext()},
	})
	if err != nil {
		t.Fatalf("Persist: %v", err)
	}
	intID := stored[0].ID

	tr := newTranscript()
	for _, ev := range storeEvents(t, store) {
		tr.event(ev)
	}
	if len(tr.blocks) != 2 { // user message + form
		t.Fatalf("blocks = %d, want the user message and the form", len(tr.blocks))
	}
	form := tr.blocks[1]
	if form.kind != blockQuestions || form.interrupt != intID || form.arc != arc {
		t.Fatalf("block = %+v, want a questions form for the interrupt", form)
	}
	if len(form.questions) != 2 || form.questions[0].ID != "cluster" || !form.questions[0].required() {
		t.Fatalf("questions = %+v", form.questions)
	}
	if form.questions[1].required() {
		t.Error("the note question is required, want optional")
	}

	// The rendered form shows the questions and the choices.
	rendered := renderTranscript(tr, 60)
	if !strings.Contains(rendered, "Which cluster?") || !strings.Contains(rendered, "prod") || !strings.Contains(rendered, "staging") {
		t.Errorf("rendered form = %q, want the questions and options", rendered)
	}

	// A response marks the form answered and keeps the response for display.
	if _, err := m.session.answerInterrupt(context.Background(), arc, intID, map[string]any{"answers": map[string]any{"cluster": "prod"}}); err != nil {
		t.Fatalf("answerInterrupt: %v", err)
	}
	for _, ev := range storeEvents(t, store) {
		if ev.Typ == events.InterruptResponseEventType {
			tr.event(ev)
		}
	}
	if !form.answered {
		t.Error("the form is not marked answered after the response")
	}
	rendered = renderTranscript(tr, 60)
	if !strings.Contains(rendered, "answered") {
		t.Errorf("rendered form = %q, want it marked answered", rendered)
	}

	// A reopened session renders the form from the log alone.
	h, err := history.Replay(storeEvents(t, store))
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	reopened := newTranscript()
	reopened.fromHistory(h)
	found := false
	for _, b := range reopened.blocks {
		if b.kind == blockQuestions && b.answered && b.interrupt == intID {
			found = true
		}
	}
	if !found {
		t.Error("the reopened transcript has no answered questions block")
	}
}

// /answer writes the INTERRUPT_RESPONSE naming the interrupt and the arc, and
// the arc reads PENDING again once the answer is in the log.
func TestAnswerCommandWritesTheResponse(t *testing.T) {
	m, store := interruptibleModel(t, "arc-1")

	intID, err := m.session.interrupt(context.Background(), "arc-1", "which cluster?")
	if err != nil {
		t.Fatalf("interrupt: %v", err)
	}

	m, _ = answerInterruptTurn(m, intID+" production")

	ev := lastEvent(t, store)
	if ev.Typ != events.InterruptResponseEventType {
		t.Fatalf("last event = %s, want INTERRUPT_RESPONSE", ev.Typ)
	}
	payload := ev.Payload.(events.InterruptResponsePayload)
	if payload.CausationID != intID {
		t.Errorf("causation = %q, want the interrupt %s", payload.CausationID, intID)
	}
	if payload.Response != "production" || payload.IsRejected {
		t.Errorf("response = %#v rejected=%v, want the answer, not a verdict", payload.Response, payload.IsRejected)
	}
	if ev.CorrelationID == nil || *ev.CorrelationID != "arc-1" {
		t.Errorf("correlation = %v, want arc-1", ev.CorrelationID)
	}

	h, err := history.Replay(storeEvents(t, store))
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	interrupt := h.Arcs[0].Interrupts[0]
	if interrupt.Status != history.InterruptAnswered || interrupt.Response != "production" {
		t.Errorf("interrupt = %#v, want answered with production", interrupt)
	}
	if h.Arcs[0].Status() != history.ArcStatusPending {
		t.Errorf("arc status = %s, want PENDING once answered", h.Arcs[0].Status())
	}
}

// /answer on an unknown or already-answered interrupt writes nothing.
func TestAnswerCommandRefusesUnknownOrAnswered(t *testing.T) {
	m, store := interruptibleModel(t, "arc-1")

	m, _ = answerInterruptTurn(m, "no-such-interrupt hello")
	last := m.transcript.blocks[len(m.transcript.blocks)-1]
	if last.kind != blockNote || !strings.Contains(last.text, "no interrupt") {
		t.Errorf("transcript ends with %+v, want a no-interrupt note", last)
	}

	intID, err := m.session.interrupt(context.Background(), "arc-1", "which cluster?")
	if err != nil {
		t.Fatalf("interrupt: %v", err)
	}
	if _, err := m.session.answerInterrupt(context.Background(), "arc-1", intID, "production"); err != nil {
		t.Fatalf("answerInterrupt: %v", err)
	}
	before := len(storeEvents(t, store))

	m, _ = answerInterruptTurn(m, intID+" again")
	if got := len(storeEvents(t, store)); got != before {
		t.Errorf("the log grew from %d to %d, want the second answer refused", before, got)
	}
	last = m.transcript.blocks[len(m.transcript.blocks)-1]
	if !strings.Contains(last.text, "already answered") {
		t.Errorf("transcript ends with %+v, want an already-answered note", last)
	}
}

// /interrupts lists what the log holds.
func TestListInterrupts(t *testing.T) {
	m, _ := interruptibleModel(t, "arc-1")
	m, _ = listInterrupts(m, "")
	if last := m.transcript.blocks[len(m.transcript.blocks)-1]; last.text != "no interrupts" {
		t.Errorf("transcript ends with %+v, want no interrupts", last)
	}

	intID, err := m.session.interrupt(context.Background(), "arc-1", "which cluster?")
	if err != nil {
		t.Fatalf("interrupt: %v", err)
	}
	m, _ = listInterrupts(m, "")
	last := m.transcript.blocks[len(m.transcript.blocks)-1]
	if last.kind != blockNote || !strings.Contains(last.text, intID) || !strings.Contains(last.text, "pending") {
		t.Errorf("transcript ends with %+v, want the pending interrupt listed", last)
	}
}

// The answer form: a pending questions block puts the chat into answer mode,
// digits pick choices, typing fills text questions, and enter writes the
// INTERRUPT_RESPONSE with all answers and leaves the form.
func TestAnswerFormSubmitsAnswers(t *testing.T) {
	m, store := interruptibleModel(t, "arc-1")
	arc := "arc-1"

	intID, err := m.session.interrupt(context.Background(), arc, "Ask the user")
	if err != nil {
		t.Fatalf("interrupt: %v", err)
	}
	evs := storeEvents(t, store)
	for _, ev := range evs {
		if ev.Typ == events.InterruptEventType {
			ev.Payload = events.InterruptPayload{Reason: "Ask the user", Context: questionsContext()}
		}
	}
	for _, ev := range evs {
		m.transcript.event(ev)
	}
	m.startAnswering()
	if m.answering == nil {
		t.Fatal("the chat did not enter answer mode on the pending form")
	}
	if m.transcript.active == nil || m.transcript.cursor != 0 {
		t.Fatalf("active = %v cursor = %d, want the form active at question 0", m.transcript.active, m.transcript.cursor)
	}

	// Pick "staging" (option 2) on the choice question, move down, type a
	// note, submit.
	tm, _ := m.answerKey(tea.KeyPressMsg{Code: '2'})
	m = tm.(model)
	if got := m.answering.answers["cluster"]; got != "staging" {
		t.Fatalf("choice answer = %#v, want staging", got)
	}
	tm, _ = m.answerKey(tea.KeyPressMsg{Code: tea.KeyDown})
	m = tm.(model)
	if m.answering.cursor != 1 {
		t.Fatalf("cursor = %d, want 1", m.answering.cursor)
	}
	m.input.SetValue("because reasons")
	tm, _ = m.answerKey(tea.KeyPressMsg{Code: tea.KeyEnter})
	m = tm.(model)

	if m.answering != nil {
		t.Error("answer mode did not end on submit")
	}
	ev := lastEvent(t, store)
	if ev.Typ != events.InterruptResponseEventType {
		t.Fatalf("last event = %s, want INTERRUPT_RESPONSE", ev.Typ)
	}
	payload := ev.Payload.(events.InterruptResponsePayload)
	if payload.CausationID != intID || ev.CorrelationID == nil || *ev.CorrelationID != arc {
		t.Errorf("response event = %+v, want it naming %s on %s", payload, intID, arc)
	}
	answers, ok := payload.Response.(map[string]any)["answers"].(map[string]any)
	if !ok {
		t.Fatalf("response = %#v, want an answers map", payload.Response)
	}
	if answers["cluster"] != "staging" || answers["note"] != "because reasons" {
		t.Errorf("answers = %#v, want cluster=staging and the note", answers)
	}

	h, err := history.Replay(storeEvents(t, store))
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if h.Arcs[0].Interrupts[0].Status != history.InterruptAnswered {
		t.Errorf("interrupt status = %s, want answered", h.Arcs[0].Interrupts[0].Status)
	}
	if h.Arcs[0].Status() != history.ArcStatusPending {
		t.Errorf("arc status = %s, want PENDING once answered", h.Arcs[0].Status())
	}
}

// A required question left empty blocks the submit.
func TestAnswerFormRequiresRequiredAnswers(t *testing.T) {
	m, store := interruptibleModel(t, "arc-1")
	arc := "arc-1"

	if _, err := m.session.interrupt(context.Background(), arc, "Ask the user"); err != nil {
		t.Fatalf("interrupt: %v", err)
	}
	evs := storeEvents(t, store)
	for _, ev := range evs {
		if ev.Typ == events.InterruptEventType {
			ev.Payload = events.InterruptPayload{Reason: "Ask the user", Context: questionsContext()}
		}
	}
	for _, ev := range evs {
		m.transcript.event(ev)
	}
	m.startAnswering()

	// Submit with the required choice question unanswered.
	tm, _ := m.answerKey(tea.KeyPressMsg{Code: tea.KeyEnter})
	m = tm.(model)
	if m.answering == nil {
		t.Fatal("submit went through with a required question unanswered")
	}
	if findEvent(t, store, events.InterruptResponseEventType) != nil {
		t.Error("an INTERRUPT_RESPONSE was written despite the missing answer")
	}
}

// The end-to-end gate through the ask tool: the model calls ask_user, the run
// writes the interrupt and ends without another model call, and the answer in
// the log lets the next run finish the arc.
func TestAskUserToolInterruptsTheTurn(t *testing.T) {
	model := stubModel(t,
		sse(
			`data: {"id":"chatcmpl-1","model":"gpt-5","choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_ask","type":"function","function":{"name":"ask_user","arguments":"{\"questions\":[{\"id\":\"cluster\",\"text\":\"Which cluster?\",\"kind\":\"single_choice\",\"options\":[\"prod\",\"staging\"]}]}"}}]}}]}`,
			`data: {"id":"chatcmpl-1","choices":[{"delta":{},"finish_reason":"tool_calls"}]}`,
		),
		sse(`data: {"id":"chatcmpl-2","model":"gpt-5","choices":[{"delta":{"content":"deploying to staging"},"finish_reason":"stop"}]}`),
	)

	path := filepath.Join(t.TempDir(), "log.jsonl")
	store, err := filestore.Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	registry := agent.NewToolRegistry()
	if err := registerFakeTools(registry); err != nil {
		t.Fatalf("registerFakeTools: %v", err)
	}
	chat, err := agent.NewAgent(
		&agent.AgentConfig{SystemPrompt: "be terse"},
		registry,
		agent.NewOpenAI("test-key", "gpt-5", agent.WithBaseURL(model.URL)),
		agent.WithInterrupts([]agent.InterruptSpec{{
			Name:          "ask_user",
			Description:   "Ask the user one or more questions",
			RequestSchema: questionsContext()["questions"],
		}}),
	)
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	persistor := common.MintIdentity(store)
	worker, err := agent.NewArcWorker(persistor, store, agent.NewFixedResolver(chat), nil)
	if err != nil {
		t.Fatalf("NewArcWorker: %v", err)
	}
	session := &session{
		id:        "session",
		store:     store,
		persistor: persistor,
		worker:    worker,
		actor:     users.Actor{UserID: "dev", TenantID: "dev"},
	}

	ctx := context.Background()
	arc := session.newArcID()
	if err := session.turn(ctx, arc, "deploy it"); err != nil {
		t.Fatalf("turn: %v", err)
	}

	// The turn ended at the interrupt: the log holds it with the questions in
	// its context, and no second model call happened (the stub fails the test
	// if asked a third time).
	intEv := findEvent(t, store, events.InterruptEventType)
	if intEv == nil {
		t.Fatalf("log = %s, want an INTERRUPT", typesOf(storeEvents(t, store)))
	}
	payload := intEv.Payload.(events.InterruptPayload)
	if payload.CausationID != "" {
		t.Errorf("interrupt causation = %q, want arc-wide", payload.CausationID)
	}
	if questionsOf(payload.Context) == nil {
		t.Errorf("interrupt context = %#v, want it to carry the questions", payload.Context)
	}

	h, err := history.Replay(storeEvents(t, store))
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if h.Arcs[0].Status() != history.ArcStatusInterrupted {
		t.Fatalf("arc status = %s, want INTERRUPTED after the ask", h.Arcs[0].Status())
	}

	// The answer resumes the arc and the turn finishes.
	if _, err := session.answerInterrupt(ctx, arc, intEv.ID, map[string]any{"answers": map[string]any{"cluster": "staging"}}); err != nil {
		t.Fatalf("answerInterrupt: %v", err)
	}
	if worked, err := worker.Work(ctx, session.ref(arc), localAgentID, 0); err != nil || !worked {
		t.Fatalf("Work after the answer: worked=%v err=%v", worked, err)
	}

	stored := storeEvents(t, store)
	if findEvent(t, store, events.InterruptResponseEventType) == nil {
		t.Fatalf("log = %s, want the INTERRUPT_RESPONSE", typesOf(stored))
	}
	replayed, err := history.Replay(stored)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if replayed.Arcs[0].Status() != history.ArcStatusDone {
		t.Errorf("arc status = %s, want DONE after the resumed turn", replayed.Arcs[0].Status())
	}
	if got := replayed.Arcs[0].Responses[1].Content; got != "deploying to staging" {
		t.Errorf("second reply = %q", got)
	}
}

// A plain typed message while an interrupt pends is written as that
// interrupt's response — no new arc is opened, which would strand the
// interrupted one.
func TestPlainTextAnswersPendingInterrupt(t *testing.T) {
	m, store := interruptibleModel(t, "arc-1")

	intID, err := m.session.interrupt(context.Background(), "arc-1", "which cluster?")
	if err != nil {
		t.Fatalf("interrupt: %v", err)
	}
	for _, ev := range storeEvents(t, store) {
		m.transcript.event(ev)
	}
	if m.transcript.pendingInterrupt() == nil {
		t.Fatal("no pending interrupt on the transcript")
	}

	m.input.SetValue("production, obviously")
	tm, _ := m.key(tea.KeyPressMsg{Code: tea.KeyEnter})
	m = tm.(model)

	ev := lastEvent(t, store)
	if ev.Typ != events.InterruptResponseEventType {
		t.Fatalf("last event = %s, want INTERRUPT_RESPONSE", ev.Typ)
	}
	payload := ev.Payload.(events.InterruptResponsePayload)
	if payload.CausationID != intID || payload.Response != "production, obviously" {
		t.Errorf("response = %#v for %q, want the typed text answering %s", payload.Response, payload.CausationID, intID)
	}
	if ev.CorrelationID == nil || *ev.CorrelationID != "arc-1" {
		t.Errorf("correlation = %v, want arc-1", ev.CorrelationID)
	}
	// The block is answered the moment the write landed, not when the
	// INTERRUPT_RESPONSE event comes back around: another message must not
	// be written as a second response to the same interrupt.
	if m.transcript.pendingInterrupt() != nil {
		t.Error("the interrupt still reads pending right after the answer was written")
	}
	for _, e := range storeEvents(t, store) {
		if e.Typ == events.UserMessageEventType && e.Payload.(events.UserMessagePayload).Content == "production, obviously" {
			t.Error("the typed message also opened a new arc as a user message")
		}
	}

	h, err := history.Replay(storeEvents(t, store))
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if h.Arcs[0].Status() != history.ArcStatusPending {
		t.Errorf("arc status = %s, want PENDING after the plain-text answer", h.Arcs[0].Status())
	}
	if len(h.Arcs) != 1 {
		t.Errorf("arcs = %d, want still one", len(h.Arcs))
	}
}

// The same holds when the pending interrupt is a questions form: plain text
// answers it directly instead of filling the form. And with no interrupt
// pending, a plain message opens a new arc as before.
func TestPlainTextWithFormPendingAndControlCase(t *testing.T) {
	m, store := interruptibleModel(t, "arc-1")
	arc := "arc-1"

	stored, err := m.session.persistor.Persist(context.Background(), &events.Event{
		SessionID: "session", AgentID: localAgentID, Typ: events.InterruptEventType,
		CorrelationID: &arc, Actor: m.session.actor,
		Payload: events.InterruptPayload{Reason: "Ask the user", Context: questionsContext()},
	})
	if err != nil {
		t.Fatalf("Persist: %v", err)
	}
	intID := stored[0].ID
	for _, ev := range storeEvents(t, store) {
		m.transcript.event(ev)
	}

	m.input.SetValue("just prod")
	tm, _ := m.key(tea.KeyPressMsg{Code: tea.KeyEnter})
	m = tm.(model)

	ev := lastEvent(t, store)
	if ev.Typ != events.InterruptResponseEventType {
		t.Fatalf("last event = %s, want INTERRUPT_RESPONSE", ev.Typ)
	}
	if payload := ev.Payload.(events.InterruptResponsePayload); payload.CausationID != intID || payload.Response != "just prod" {
		t.Errorf("response = %#v, want the plain text answering %s", payload.Response, intID)
	}

	// Control: with the interrupt answered, a plain message opens a new arc.
	for _, e := range storeEvents(t, store) {
		if e.Typ == events.InterruptResponseEventType {
			m.transcript.event(e)
		}
	}
	if m.transcript.pendingInterrupt() != nil {
		t.Fatal("the interrupt still reads pending on the transcript")
	}
	// A turn needs a worker; the stub model answers one reply.
	server := stubModel(t, sse(`data: {"id":"c1","model":"gpt-5","choices":[{"delta":{"content":"ok"},"finish_reason":"stop"}]}`))
	chat, err := agent.NewAgent(&agent.AgentConfig{}, agent.NewToolRegistry(),
		agent.NewOpenAI("k", "gpt-5", agent.WithBaseURL(server.URL)))
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	worker, err := agent.NewArcWorker(m.session.persistor, store, agent.NewFixedResolver(chat), nil)
	if err != nil {
		t.Fatalf("NewArcWorker: %v", err)
	}
	m.session.worker = worker


	m.input.SetValue("next question")
	tm, _ = m.key(tea.KeyPressMsg{Code: tea.KeyEnter})
	m = tm.(model)
	if m.arc == "" {
		t.Fatal("with nothing pending, enter did not start a new turn")
	}
	// The turn writes on its own goroutine; wait for the opening record.
	var opened bool
	for range 40 {
		for _, e := range storeEvents(t, store) {
			if e.Typ == events.UserMessageEventType && e.Payload.(events.UserMessagePayload).Content == "next question" {
				opened = true
			}
		}
		if opened {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if !opened {
		t.Errorf("with nothing pending, enter wrote %s, want a new USER_MESSAGE arc", typesOf(storeEvents(t, store)))
	}

}

// The whole path a devchat run takes: a settings file declaring ask_user
// resolves an agent that offers the tool on the wire, and the model calling
// it raises the interrupt.
func TestSettingsFilePutsAskUserOnTheWire(t *testing.T) {
	model := stubModel(t,
		sse(
			`data: {"id":"chatcmpl-1","model":"gpt-5","choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_ask","type":"function","function":{"name":"ask_user","arguments":"{\"questions\":[{\"id\":\"cluster\",\"text\":\"Which cluster?\",\"kind\":\"single_choice\",\"options\":[\"prod\",\"staging\"]}]}"}}]}}]}`,
			`data: {"id":"chatcmpl-1","choices":[{"delta":{},"finish_reason":"tool_calls"}]}`,
		),
	)

	var lastRequest map[string]any
	recorder := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(raw, &lastRequest); err != nil {
			t.Errorf("unmarshal request: %v", err)
		}
		proxy, err := http.Post(model.URL, "application/json", strings.NewReader(string(raw)))
		if err != nil {
			t.Errorf("proxy: %v", err)
			return
		}
		defer proxy.Body.Close()
		io.Copy(w, proxy.Body)
	}))
	t.Cleanup(recorder.Close)

	settingsPath := filepath.Join(t.TempDir(), "agents.json")
	settings := `{
		"system_prompt": "be terse",
		"model": {"source": "test", "name": "gpt-5"},
		"tools": [{"source": "local", "name": "list_pods"}],
		"interrupts": [{
			"name": "ask_user",
			"description": "Ask the user one or more questions. The conversation is held until they answer.",
			"request_schema": {"type": "object", "properties": {"questions": {"type": "array", "minItems": 1}}, "required": ["questions"]}
		}]
	}`
	if err := os.WriteFile(settingsPath, []byte(settings), 0o644); err != nil {
		t.Fatalf("write settings: %v", err)
	}

	store, err := filestore.Open(filepath.Join(t.TempDir(), "log.jsonl"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	registry := agent.NewToolRegistry()
	if err := registerFakeTools(registry); err != nil {
		t.Fatalf("registerFakeTools: %v", err)
	}
	resolver := agent.NewFileResolver(settingsPath,
		map[string]agent.ToolProvider{"local": registry},
		nil, nil,
		map[string]agent.ModelFactory{"test": func(ref agent.ModelRef) (agent.LLM, error) {
			return agent.NewOpenAI("test-key", ref.Name, agent.WithBaseURL(recorder.URL)), nil
			}},
	)
	persistor := common.MintIdentity(store)
	worker, err := agent.NewArcWorker(persistor, store, resolver, nil)
	if err != nil {
		t.Fatalf("NewArcWorker: %v", err)
	}
	session := &session{
		id: "session", store: store, persistor: persistor, worker: worker,
		actor: users.Actor{UserID: "dev", TenantID: "dev"},
	}

	if err := session.turn(context.Background(), session.newArcID(), "deploy the web thing"); err != nil {
		t.Fatalf("turn: %v", err)
	}

	// The request the provider saw has to carry ask_user with its schema.
	toolsRaw, err := json.Marshal(lastRequest["tools"])
	if err != nil {
		t.Fatalf("marshal tools: %v", err)
	}
	if !strings.Contains(string(toolsRaw), `"ask_user"`) || !strings.Contains(string(toolsRaw), `"questions"`) {
		t.Errorf("the wire carried tools %s, want ask_user with the questions schema", toolsRaw)
	}

	// And the model's call raised the interrupt with the questions as context.
	intEv := findEvent(t, store, events.InterruptEventType)
	if intEv == nil {
		t.Fatalf("log = %s, want an INTERRUPT from the ask_user call", typesOf(storeEvents(t, store)))
	}
	if questionsOf(intEv.Payload.(events.InterruptPayload).Context) == nil {
		t.Errorf("interrupt context = %#v, want the questions", intEv.Payload)
	}
	h, err := history.Replay(storeEvents(t, store))
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if h.Arcs[0].Status() != history.ArcStatusInterrupted {
		t.Errorf("arc status = %s, want INTERRUPTED", h.Arcs[0].Status())
	}
}

// queueSink is the screen sink, but into a channel the test pumps through
// Update itself, so delivery order is exactly what the program would see:
// events published mid-Update land behind the keys already queued.
type queueSink struct{ msgs chan<- tea.Msg }

func (q queueSink) Consume(_ context.Context, evs ...*events.Event) {
	for _, ev := range evs {
		q.msgs <- eventMsg{event: ev}
	}
}

// The whole flow over the real update path: a turn raises the interrupt, the
// turn ends, the form is answered with keys, and enter submits. A second
// enter right after the submit — key repeat, or an impatient press — must not
// put the chat back into answer mode on the block that was just answered: the
// response is durable the moment it is written, so the block is answered then,
// not when the INTERRUPT_RESPONSE event comes back around the program.
func TestAnswerFormSubmitIsAnsweredAtOnce(t *testing.T) {
	server := stubModel(t,
		sse(
			`data: {"id":"c1","model":"gpt-5","choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_ask","type":"function","function":{"name":"ask_user","arguments":"{\"questions\":[{\"id\":\"cluster\",\"text\":\"Which cluster?\",\"kind\":\"single_choice\",\"options\":[\"prod\",\"staging\"]},{\"id\":\"note\",\"text\":\"Anything to add?\",\"kind\":\"text\",\"required\":false}]}"}}]}}]}`,
			`data: {"id":"c1","choices":[{"delta":{},"finish_reason":"tool_calls"}]}`,
		),
		sse(`data: {"id":"c2","model":"gpt-5","choices":[{"delta":{"content":"deploying to staging"},"finish_reason":"stop"}]}`),
	)

	store, err := filestore.Open(filepath.Join(t.TempDir(), "log.jsonl"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	registry := agent.NewToolRegistry()
	if err := registerFakeTools(registry); err != nil {
		t.Fatalf("registerFakeTools: %v", err)
	}
	chat, err := agent.NewAgent(&agent.AgentConfig{SystemPrompt: "be terse"}, registry,
		agent.NewOpenAI("k", "gpt-5", agent.WithBaseURL(server.URL)),
		agent.WithInterrupts([]agent.InterruptSpec{{
			Name:          "ask_user",
			Description:   "Ask the user",
			RequestSchema: map[string]any{"type": "object"},
		}}))
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}

	msgs := make(chan tea.Msg, 1000)
	persistor := common.PublishEvents(common.MintIdentity(common.GuardCancellation(store, store)), queueSink{msgs})
	worker, err := agent.NewArcWorker(persistor, store, agent.NewFixedResolver(chat), agent.NewLocalLease())
	if err != nil {
		t.Fatalf("NewArcWorker: %v", err)
	}
	s := &session{
		id: "session", store: store, persistor: persistor, worker: worker,
		actor: users.Actor{UserID: "dev", TenantID: "dev"},
	}

	m := newModel(s, &sender{}, agent.AgentSettings{}, "", newTranscript()).resize(80, 24)

	ctx := context.Background()
	arc := s.newArcID()
	go func() { msgs <- turnDoneMsg{err: s.turn(ctx, arc, "deploy it")} }()

	pump := func(msg tea.Msg) {
		tm, _ := m.Update(msg)
		m = tm.(model)
	}

	answered := false
	deadline := time.After(10 * time.Second)
	for {
		select {
		case msg := <-msgs:
			pump(msg)
			// The turn is over and the form is pending: answer it like the
			// user does, then press enter again right away.
			if !answered && m.answering != nil {
				answered = true
				pump(tea.KeyPressMsg{Code: '2'})
				pump(tea.KeyPressMsg{Code: tea.KeyDown})
				for _, r := range "because reasons" {
					pump(tea.KeyPressMsg{Code: r})
				}
				pump(tea.KeyPressMsg{Code: tea.KeyEnter})
				if m.answering != nil {
					t.Fatal("answer mode did not end on submit")
				}
				if m.transcript.pendingQuestions() != nil {
					t.Fatal("the form still reads pending right after the response was written")
				}
				pump(tea.KeyPressMsg{Code: tea.KeyEnter})
				if m.answering != nil {
					t.Fatalf("a second enter put the chat back into answer mode on the answered block %s", m.answering.block.interrupt)
				}
			}
		case <-deadline:
			t.Fatal("timed out waiting for the flow")
		}
		stored, _ := store.Events(ctx)
		done := false
		for _, ev := range stored {
			if ev.Typ == events.ArcClosedEventType {
				done = true
			}
		}
		if done {
			for {
				select {
				case msg := <-msgs:
					pump(msg)
				default:
					goto drained
				}
			}
		drained:
			break
		}
	}

	responses := 0
	for _, ev := range storeEvents(t, store) {
		if ev.Typ == events.InterruptResponseEventType {
			responses++
		}
	}
	if responses != 1 {
		t.Errorf("INTERRUPT_RESPONSE count = %d, want exactly 1", responses)
	}
	h, err := history.Replay(storeEvents(t, store))
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(h.Arcs) != 1 || h.Arcs[0].Status() != history.ArcStatusDone {
		t.Fatalf("arc = %+v, want one arc DONE after the resume", h.Arcs)
	}
	if got := h.Arcs[0].Responses[1].Content; got != "deploying to staging" {
		t.Errorf("second reply = %q, want the resumed run's answer", got)
	}
	if m.answering != nil {
		t.Errorf("answer mode is still active at the end: %s", m.answering.block.interrupt)
	}
}

func ptr[T any](v T) *T { return &v }
