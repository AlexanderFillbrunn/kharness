package harness

import (
	"context"
	"io"
	"strings"
	"sync"
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

// probe wraps the model so a test can watch it while the program runs. The
// program keeps the returned model, so the probe always holds the latest one.
type probe struct {
	mu sync.Mutex
	m  model
}

func (p *probe) Init() tea.Cmd { return p.m.Init() }

func (p *probe) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	tm, cmd := p.m.Update(msg)
	if m, ok := tm.(model); ok {
		p.mu.Lock()
		p.m = m
		p.mu.Unlock()
	}
	return p, cmd
}

func (p *probe) View() tea.View {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.m.View()
}

func (p *probe) state() model {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.m
}

// The answer form over a real program: keys go through program.Send, the same
// channel the event relay uses, so a second enter queued right behind the
// submit's enter is processed before the INTERRUPT_RESPONSE event — the order
// a fast key repeat produces. The submit's write is durable at once, so the
// block must read answered immediately and the second enter must not put the
// chat back into answer mode.
func TestProgramAnswerFormSubmitSticks(t *testing.T) {
	server := stubModel(t,
		sse(
			`data: {"id":"c1","model":"gpt-5","choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_ask","type":"function","function":{"name":"ask_user","arguments":"{\"questions\":[{\"id\":\"goal\",\"text\":\"What now?\",\"kind\":\"text\"},{\"id\":\"context\",\"text\":\"Which namespace?\",\"kind\":\"text\"}]}"}}]}}]}`,
			`data: {"id":"c1","choices":[{"delta":{},"finish_reason":"tool_calls"}]}`,
		),
		sse(`data: {"id":"c2","model":"gpt-5","choices":[{"delta":{"content":"all done, thanks"},"finish_reason":"stop"}]}`),
	)

	store, err := filestore.Open(t.TempDir() + "/session.jsonl")
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

	relay := &sender{}
	persistor := common.PublishEvents(common.MintIdentity(common.GuardCancellation(store, store)), screenEvents{relay: relay})
	worker, err := agent.NewArcWorker(persistor, store, agent.NewFixedResolver(chat), agent.NewLocalLease(),
		agent.WithTokenSink(screenTokens{relay: relay}))
	if err != nil {
		t.Fatalf("NewArcWorker: %v", err)
	}
	s := &session{
		id: "session", store: store, persistor: persistor, worker: worker,
		actor: users.Actor{UserID: "dev", TenantID: "dev"},
	}

	p := &probe{m: newModel(s, relay, agent.AgentSettings{}, "", newTranscript()).resize(80, 24)}
	inputR, inputW := io.Pipe()
	t.Cleanup(func() { inputW.Close() })
	program := tea.NewProgram(p, tea.WithInput(inputR), tea.WithOutput(io.Discard))
	relay.attach(program)
	runDone := make(chan error, 1)
	go func() { _, err := program.Run(); runDone <- err }()
	t.Cleanup(func() {
		program.Quit()
		select {
		case <-runDone:
		case <-time.After(5 * time.Second):
		}
	})

	keys := func(text string) {
		for _, r := range text {
			program.Send(tea.KeyPressMsg{Code: r, Text: string(r)})
		}
	}
	enter := func() { program.Send(tea.KeyPressMsg{Code: tea.KeyEnter}) }
	down := func() { program.Send(tea.KeyPressMsg{Code: tea.KeyDown}) }

	waitFor := func(what string, cond func() bool) {
		t.Helper()
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) {
			if cond() {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatalf("timed out waiting for %s", what)
	}

	// Ask the question and let the run raise the interrupt.
	keys("ask me 2 questions")
	enter()
	waitFor("answer mode", func() bool { return p.state().answering != nil })

	// Answer both text questions, submit, and press enter again at once —
	// the second enter is queued behind the first, ahead of the response
	// event the first produces.
	keys("fix the ingress deployment")
	down()
	keys("ingress")
	enter()
	enter()

	waitFor("the arc to close after the resume", func() bool {
		for _, ev := range storeEvents(t, store) {
			if ev.Typ == events.ArcClosedEventType {
				return true
			}
		}
		return false
	})

	// A later enter with everything settled must not re-enter answer mode.
	enter()
	time.Sleep(300 * time.Millisecond)

	program.Quit()
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("program: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the program did not quit")
	}

	m := p.state()
	if m.answering != nil {
		t.Errorf("STUCK in answer mode for interrupt %s; help line: %s",
			m.answering.block.interrupt, strings.TrimSpace(m.helpView()))
	}
	if m.transcript.pendingQuestions() != nil || m.transcript.pendingInterrupt() != nil {
		t.Error("a pending interrupt still reads pending after the answer")
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
		t.Fatalf("arcs = %+v, want one arc DONE after the resume", h.Arcs)
	}
	if got := h.Arcs[0].Responses[1].Content; got != "all done, thanks" {
		t.Errorf("second reply = %q, want the resumed run's answer", got)
	}

	// The fold agrees the interrupt was answered, with the form's answers.
	in := h.Arcs[0].Interrupts[0]
	if in.Status != history.InterruptAnswered {
		t.Errorf("interrupt status = %s, want answered", in.Status)
	}
	answers, ok := in.Response.(map[string]any)["answers"].(map[string]any)
	if !ok || answers["goal"] != "fix the ingress deployment" || answers["context"] != "ingress" {
		t.Errorf("answers = %#v", in.Response)
	}
	_ = context.Background()
}
