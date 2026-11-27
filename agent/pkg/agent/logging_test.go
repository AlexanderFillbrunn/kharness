package agent

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"fillbrunn.ai/kube-agent/proto/pkg/events"
	"fillbrunn.ai/kube-agent/proto/pkg/users"
)

// bufferLogger is a logger at Debug level writing to buf, for tests that read
// what a component logged.
func bufferLogger(buf *bytes.Buffer) *slog.Logger {
	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// A run reports its lifecycle to the agent's logger, tagged with the
// component and the arc — and never carries the content it worked on.
func TestRunLogsItsLifecycle(t *testing.T) {
	registry := registryWith(t, "list_pods", func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "2 pending"}}}, nil
	})
	llm := &scriptedLLM{replies: []*LLMResponse{
		{ModelID: "gpt-5", Content: "SECRET_REPLY_TEXT", ToolCalls: []LLMToolCall{{
			VendorID: "call_abc", ToolName: "list_pods", Args: map[string]any{"namespace": "SECRET_NAMESPACE"},
		}}},
		{ModelID: "gpt-5", Content: "SECRET_FINAL_TEXT"},
	}}

	var buf bytes.Buffer
	agent, err := NewAgent(nil, registry, llm, WithLogging(bufferLogger(&buf)))
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	h := userArcHistory(t)
	if err := agent.Run(context.Background(), RunOptions{Persistor: &fakePersistor{next: 1}, History: h, ArcID: testArcID}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	out := buf.String()
	for _, want := range []string{
		"component=agent", "arc=" + testArcID,
		"run starting", "calling model", "persisted events", "arc closed", "run ended", "outcome=finished",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("log is missing %q\n%s", want, out)
		}
	}
	for _, secret := range []string{"SECRET_REPLY_TEXT", "SECRET_FINAL_TEXT", "SECRET_NAMESPACE"} {
		if strings.Contains(out, secret) {
			t.Errorf("log contains content %q; logs carry metadata only\n%s", secret, out)
		}
	}
}

// A failed run logs the failure at Error, alongside the fatal error it
// recorded in the log.
func TestAFailedRunLogsTheFailure(t *testing.T) {
	var buf bytes.Buffer
	agent, err := NewAgent(nil, nil, &scriptedLLM{failure: errors.New("boom")}, WithLogging(bufferLogger(&buf)))
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	h := userArcHistory(t)
	if err := agent.Run(context.Background(), RunOptions{Persistor: &fakePersistor{next: 1}, History: h, ArcID: testArcID}); err == nil {
		t.Fatal("Run succeeded, want the scripted failure")
	}
	out := buf.String()
	if !strings.Contains(out, "level=ERROR") || !strings.Contains(out, "run failed") {
		t.Errorf("log is missing the failure line\n%s", out)
	}
}

// A tool receives the run's logger on the context it is invoked with, so a
// tool implementation logs to the same place as the run that called it.
func TestAToolCanLogThroughTheRunLogger(t *testing.T) {
	registry := registryWith(t, "list_pods", func(ctx context.Context, _ *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		if l := LoggerFrom(ctx); l != nil {
			l.Info("the tool is running")
		}
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "2 pending"}}}, nil
	})
	llm := &scriptedLLM{replies: []*LLMResponse{
		{ModelID: "gpt-5", ToolCalls: []LLMToolCall{{VendorID: "call_1", ToolName: "list_pods"}}},
		{ModelID: "gpt-5", Content: "two pods"},
	}}

	var buf bytes.Buffer
	agent, err := NewAgent(nil, registry, llm, WithLogging(bufferLogger(&buf)))
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	h := userArcHistory(t)
	if err := agent.Run(context.Background(), RunOptions{Persistor: &fakePersistor{next: 1}, History: h, ArcID: testArcID}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "the tool is running") || !strings.Contains(out, "arc="+testArcID) {
		t.Errorf("the tool's line did not reach the run's logger with the arc attribute\n%s", out)
	}
}

// A hook receives the run's logger on its context, so anything plugged in
// through WithHooks — webhooks included — logs to the same place as the run
// without the agent knowing it exists.
func TestHooksReceiveTheRunLogger(t *testing.T) {
	var buf bytes.Buffer
	var fromHook *slog.Logger
	hooks := Hooks{AfterRun: func(ctx context.Context, sum RunSummary) {
		fromHook = LoggerFrom(ctx)
	}}
	agent, err := NewAgent(nil, nil, &scriptedLLM{replies: []*LLMResponse{{ModelID: "gpt-5", Content: "done"}}},
		WithLogging(bufferLogger(&buf)), WithHooks(hooks))
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	h := userArcHistory(t)
	if err := agent.Run(context.Background(), RunOptions{Persistor: &fakePersistor{next: 1}, History: h, ArcID: testArcID}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if fromHook == nil {
		t.Fatal("the hook's context carried no logger")
	}
	fromHook.Info("line from a hook")
	out := buf.String()
	if !strings.Contains(out, "line from a hook") || !strings.Contains(out, "arc="+testArcID) {
		t.Errorf("the hook's line did not reach the agent's logger with the arc attribute\n%s", out)
	}
}

// A webhook dispatcher plugged in through the hooks alone logs to the run's
// logger, tagged as a webhook — unless it was built with a logger of its own,
// which then wins.
func TestWebhooksPluggedInAsHooksLogToTheRunLogger(t *testing.T) {
	srv, _ := newWebhookServer(t, func(_ int, _ webhookRequest, _ http.ResponseWriter) (int, string) {
		return 500, "boom"
	})
	cfg := WebhookConfig{
		Events:        map[WebhookEvent]DeliveryConfig{EventRunAfter: {URL: srv.URL, MaxRetries: -1}},
		AllowInsecure: true,
	}

	var runBuf bytes.Buffer
	w, err := NewWebhooks(cfg)
	if err != nil {
		t.Fatalf("NewWebhooks: %v", err)
	}
	invoked := false
	// Attached through WithHooks alone: the agent holds no webhook reference.
	agent := webhookAgent(t, &invoked, WithLogging(bufferLogger(&runBuf)), WithHooks(w.Compose(Hooks{})))
	runWebhookAgent(t, agent)
	closeWebhooks(t, w)

	out := runBuf.String()
	if !strings.Contains(out, "giving up on delivery") || !strings.Contains(out, "component=webhooks") {
		t.Errorf("the webhook failure did not reach the run's logger tagged as a webhook\n%s", out)
	}

	// An explicit WebhookConfig.Logger wins over the run's logger.
	var ownBuf bytes.Buffer
	w2, err := NewWebhooks(WebhookConfig{
		Events:        map[WebhookEvent]DeliveryConfig{EventRunAfter: {URL: srv.URL, MaxRetries: -1}},
		AllowInsecure: true,
		Logger:        bufferLogger(&ownBuf),
	})
	if err != nil {
		t.Fatalf("NewWebhooks: %v", err)
	}
	var runBuf2 bytes.Buffer
	agent2 := webhookAgent(t, &invoked, WithLogging(bufferLogger(&runBuf2)), WithHooks(w2.Compose(Hooks{})))
	runWebhookAgent(t, agent2)
	closeWebhooks(t, w2)

	if !strings.Contains(ownBuf.String(), "giving up on delivery") || strings.Contains(runBuf2.String(), "giving up on delivery") {
		t.Errorf("the explicit logger did not win: run log %q, own log %q", runBuf2.String(), ownBuf.String())
	}
}

// seedWorkedArc persists the one user message that opens testArcID in a fresh
// store, so a worker can work it.
func seedWorkedArc(t *testing.T, store *logStore) {
	t.Helper()
	actor := users.Actor{UserID: "alice", TenantID: "acme"}
	arc := testArcID
	if _, err := store.Persist(context.Background(), &events.Event{
		SessionID: testSessionID, ID: arc, Typ: events.UserMessageEventType,
		CorrelationID: &arc, Actor: actor, Payload: events.UserMessagePayload{Content: "go", Actor: actor},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
}

// An agent resolved by a worker without a logger of its own inherits the
// worker's: its run logs where the worker logs, tagged with the arc.
func TestAnAgentInheritsTheWorkersLogger(t *testing.T) {
	ctx := context.Background()
	store := &logStore{}
	seedWorkedArc(t, store)

	ag, err := NewAgent(nil, NewToolRegistry(), &scriptedLLM{replies: []*LLMResponse{finalReply("done")}})
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	var buf bytes.Buffer
	worker, err := NewArcWorker(store, store, NewFixedResolver(ag), nil, WithLogger(bufferLogger(&buf)))
	if err != nil {
		t.Fatalf("NewArcWorker: %v", err)
	}
	if _, err := worker.Work(ctx, testRef(), testAgentID, 0); err != nil {
		t.Fatalf("Work: %v", err)
	}

	out := buf.String()
	for _, want := range []string{"component=arc-worker", "arc=" + testArcID, "run ended", "outcome=finished"} {
		if !strings.Contains(out, want) {
			t.Errorf("log is missing %q\n%s", want, out)
		}
	}
}

// An agent built with WithLogging logs to its own logger even when the run
// arrives through a worker with a different one.
func TestAnExplicitAgentLoggerWinsOverTheWorkers(t *testing.T) {
	ctx := context.Background()
	store := &logStore{}
	seedWorkedArc(t, store)

	var agentBuf bytes.Buffer
	ag, err := NewAgent(nil, NewToolRegistry(), &scriptedLLM{replies: []*LLMResponse{finalReply("done")}},
		WithLogging(bufferLogger(&agentBuf)))
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	var workerBuf bytes.Buffer
	worker, err := NewArcWorker(store, store, NewFixedResolver(ag), nil, WithLogger(bufferLogger(&workerBuf)))
	if err != nil {
		t.Fatalf("NewArcWorker: %v", err)
	}
	if _, err := worker.Work(ctx, testRef(), testAgentID, 0); err != nil {
		t.Fatalf("Work: %v", err)
	}

	if !strings.Contains(agentBuf.String(), "run ended") || !strings.Contains(agentBuf.String(), "component=agent") {
		t.Errorf("the run did not log to the agent's own logger\n%s", agentBuf.String())
	}
	if strings.Contains(workerBuf.String(), "run ended") {
		t.Errorf("the run leaked into the worker's logger\n%s", workerBuf.String())
	}
	if !strings.Contains(workerBuf.String(), "working arc") {
		t.Errorf("the worker's own line is missing\n%s", workerBuf.String())
	}
}
