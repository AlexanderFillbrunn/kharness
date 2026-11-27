package agent

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"fillbrunn.ai/kube-agent/proto/pkg/events"
	"fillbrunn.ai/kube-agent/proto/pkg/history"
)

// webhookRequest is one request a test server received.
type webhookRequest struct {
	event   string
	id      string
	attempt string
	ts      string
	sig     string
	body    []byte
}

// newWebhookServer records every request and answers through respond, which
// sees the 1-based attempt number and may set response headers first.
func newWebhookServer(t *testing.T, respond func(attempt int, req webhookRequest, w http.ResponseWriter) (int, string)) (*httptest.Server, func() []webhookRequest) {
	t.Helper()
	var mu sync.Mutex
	var reqs []webhookRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		wr := webhookRequest{
			event:   r.Header.Get("X-Webhook-Event"),
			id:      r.Header.Get("X-Webhook-ID"),
			attempt: r.Header.Get("X-Webhook-Attempt"),
			ts:      r.Header.Get("X-Webhook-Timestamp"),
			sig:     r.Header.Get("X-Webhook-Signature"),
			body:    body,
		}
		mu.Lock()
		reqs = append(reqs, wr)
		n := len(reqs)
		mu.Unlock()
		status, respBody := respond(n, wr, w)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		fmt.Fprint(w, respBody)
	}))
	t.Cleanup(srv.Close)
	return srv, func() []webhookRequest {
		mu.Lock()
		defer mu.Unlock()
		return append([]webhookRequest(nil), reqs...)
	}
}

// fastDelivery keeps retries quick in tests: one retry allowed at most unless
// the test says otherwise, with millisecond backoff.
func fastDelivery(url string) DeliveryConfig {
	return DeliveryConfig{URL: url, MaxRetries: -1, BackoffMin: time.Millisecond, BackoffMax: 5 * time.Millisecond}
}

func closeWebhooks(t *testing.T, w *Webhooks) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := w.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestNewWebhooksValidation(t *testing.T) {
	if _, err := NewWebhooks(WebhookConfig{}); err == nil {
		t.Error("no URL anywhere: want an error")
	}
	if _, err := NewWebhooks(WebhookConfig{DeliveryConfig: DeliveryConfig{URL: "http://hooks.example.com/"}}); err == nil {
		t.Error("http without AllowInsecure: want an error")
	}
	if _, err := NewWebhooks(WebhookConfig{DeliveryConfig: DeliveryConfig{URL: "http://hooks.example.com/"}, AllowInsecure: true}); err != nil {
		t.Errorf("http with AllowInsecure: %v", err)
	}
	if _, err := NewWebhooks(WebhookConfig{DeliveryConfig: DeliveryConfig{URL: "ftp://hooks.example.com/"}}); err == nil {
		t.Error("ftp scheme: want an error")
	}
	if _, err := NewWebhooks(WebhookConfig{DeliveryConfig: DeliveryConfig{URL: "https://hooks.example.com/", Disabled: true}}); err == nil {
		t.Error("top-level Disabled: want an error")
	}
	_, err := NewWebhooks(WebhookConfig{
		DeliveryConfig: DeliveryConfig{URL: "https://hooks.example.com/"},
		Events:         map[WebhookEvent]DeliveryConfig{"tool_call.befor": {URL: "https://guard.example.com/"}},
	})
	if err == nil || !strings.Contains(err.Error(), "unknown event") {
		t.Errorf("typo'd event key: want an unknown-event error, got %v", err)
	}
	// A per-event URL alone is a valid target even with no global URL.
	if _, err := NewWebhooks(WebhookConfig{
		Events: map[WebhookEvent]DeliveryConfig{EventToolCallBefore: {URL: "https://guard.example.com/"}},
	}); err != nil {
		t.Errorf("per-event URL only: %v", err)
	}
}

func TestResolveDefaults(t *testing.T) {
	w, err := NewWebhooks(WebhookConfig{DeliveryConfig: DeliveryConfig{URL: "https://hooks.example.com/"}})
	if err != nil {
		t.Fatalf("NewWebhooks: %v", err)
	}
	before := w.resolve(EventToolCallBefore, DeliverySync)
	if before.mode != DeliverySync || before.failPolicy != FailClosed || before.timeout != DefaultWebhookTimeout || before.maxRetries != DefaultWebhookMaxRetries {
		t.Errorf("before defaults = %+v", before)
	}
	after := w.resolve(EventRunAfter, DeliveryAsync)
	if after.mode != DeliveryAsync {
		t.Errorf("after auto mode = %v, want async", after.mode)
	}
	// A per-event override wins over the global and over auto.
	w2, err := NewWebhooks(WebhookConfig{
		DeliveryConfig: DeliveryConfig{URL: "https://hooks.example.com/", Mode: DeliveryAsync},
		Events:         map[WebhookEvent]DeliveryConfig{EventToolCallBefore: {Mode: DeliverySync, FailPolicy: FailOpen}},
	})
	if err != nil {
		t.Fatalf("NewWebhooks: %v", err)
	}
	rd := w2.resolve(EventToolCallBefore, DeliverySync)
	if rd.mode != DeliverySync || rd.failPolicy != FailOpen {
		t.Errorf("override = %+v, want sync/fail-open", rd)
	}
	if got := w2.resolve(EventRunAfter, DeliveryAsync); got.mode != DeliveryAsync {
		t.Errorf("global mode = %v, want async", got.mode)
	}
}

// An async delivery arrives as one signed envelope with a stable id.
func TestAsyncDeliveryEnvelopeAndSignature(t *testing.T) {
	const secret = "test-secret"
	srv, reqs := newWebhookServer(t, func(_ int, _ webhookRequest, _ http.ResponseWriter) (int, string) {
		return 200, ""
	})
	w, err := NewWebhooks(WebhookConfig{
		DeliveryConfig: DeliveryConfig{URL: srv.URL, Secret: secret, MaxRetries: -1},
		AllowInsecure:  true,
	})
	if err != nil {
		t.Fatalf("NewWebhooks: %v", err)
	}
	w.notify(context.Background(), EventRunAfter, w.resolve(EventRunAfter, DeliveryAsync), RunSummary{ArcID: "arc-1", Outcome: RunFinished, Rounds: 2})
	closeWebhooks(t, w)

	got := reqs()
	if len(got) != 1 {
		t.Fatalf("got %d requests, want 1", len(got))
	}
	req := got[0]
	if req.event != "run.after" {
		t.Errorf("event header = %q", req.event)
	}
	var env struct {
		ID      string         `json:"id"`
		Event   string         `json:"event"`
		Seq     int64          `json:"seq"`
		Attempt int            `json:"attempt"`
		Data    map[string]any `json:"data"`
	}
	if err := json.Unmarshal(req.body, &env); err != nil {
		t.Fatalf("envelope does not parse: %v", err)
	}
	if env.Event != "run.after" || env.ID != req.id || env.Seq != 1 || env.Attempt != 1 {
		t.Errorf("envelope = %+v, headers id=%s", env, req.id)
	}
	if env.Data["ArcID"] != "arc-1" || env.Data["Rounds"] != float64(2) {
		t.Errorf("data = %v", env.Data)
	}
	// The signature is HMAC-SHA256 of "<timestamp>.<body>".
	var ts, v1 string
	for _, p := range strings.Split(req.sig, ",") {
		if strings.HasPrefix(p, "t=") {
			ts = strings.TrimPrefix(p, "t=")
		}
		if strings.HasPrefix(p, "v1=") {
			v1 = strings.TrimPrefix(p, "v1=")
		}
	}
	if ts == "" || v1 == "" || ts != req.ts {
		t.Fatalf("signature header %q, timestamp header %q", req.sig, req.ts)
	}
	mac := hmac.New(sha256.New, []byte(secret))
	fmt.Fprintf(mac, "%s.", ts)
	mac.Write(req.body)
	if hex.EncodeToString(mac.Sum(nil)) != v1 {
		t.Error("signature does not match t.body")
	}
}

// Per-event URLs override the global one; events without an entry fall back
// to it; Disabled suppresses an event entirely.
func TestPerEventRoutingFallbackAndDisabled(t *testing.T) {
	global, globalReqs := newWebhookServer(t, func(_ int, _ webhookRequest, _ http.ResponseWriter) (int, string) {
		return 200, ""
	})
	guard, guardReqs := newWebhookServer(t, func(_ int, _ webhookRequest, _ http.ResponseWriter) (int, string) {
		return 200, ""
	})
	w, err := NewWebhooks(WebhookConfig{
		DeliveryConfig: fastDelivery(global.URL),
		Events: map[WebhookEvent]DeliveryConfig{
			EventToolCallBefore:   {URL: guard.URL},
			EventAssistantBefore:  {Disabled: true},
		},
		AllowInsecure: true,
	})
	if err != nil {
		t.Fatalf("NewWebhooks: %v", err)
	}
	w.notify(context.Background(), EventRunAfter, w.resolve(EventRunAfter, DeliveryAsync), RunSummary{})
	w.notify(context.Background(), EventToolCallBefore, w.resolve(EventToolCallBefore, DeliverySync), ToolCallView{ToolName: "delete_pod"})
	w.notify(context.Background(), EventAssistantBefore, w.resolve(EventAssistantBefore, DeliverySync), AssistantRequest{})
	closeWebhooks(t, w)

	if got := globalReqs(); len(got) != 1 || got[0].event != "run.after" {
		t.Errorf("global server got %v, want one run.after", got)
	}
	if got := guardReqs(); len(got) != 1 || got[0].event != "tool_call.before" {
		t.Errorf("guard server got %v, want one tool_call.before", got)
	}
}

// A 5xx is retried with backoff and succeeds; every attempt carries the same
// delivery id and a rising attempt number.
func TestRetryOn5xxThenSuccess(t *testing.T) {
	srv, reqs := newWebhookServer(t, func(attempt int, _ webhookRequest, _ http.ResponseWriter) (int, string) {
		if attempt < 3 {
			return 500, "boom"
		}
		return 200, ""
	})
	w, err := NewWebhooks(WebhookConfig{
		DeliveryConfig: DeliveryConfig{URL: srv.URL, MaxRetries: 4, BackoffMin: time.Millisecond, BackoffMax: 5 * time.Millisecond},
		AllowInsecure:  true,
	})
	if err != nil {
		t.Fatalf("NewWebhooks: %v", err)
	}
	w.notify(context.Background(), EventRunAfter, w.resolve(EventRunAfter, DeliveryAsync), RunSummary{})
	closeWebhooks(t, w)

	got := reqs()
	if len(got) != 3 {
		t.Fatalf("got %d requests, want 3", len(got))
	}
	if got[0].id != got[2].id {
		t.Errorf("delivery id changed across attempts: %s vs %s", got[0].id, got[2].id)
	}
	for i, req := range got {
		if req.attempt != fmt.Sprint(i+1) {
			t.Errorf("request %d has attempt %q", i, req.attempt)
		}
	}
	if w.Dropped() != 0 {
		t.Errorf("dropped = %d, want 0", w.Dropped())
	}
}

// A 400 is permanent: no retry.
func TestNoRetryOn400(t *testing.T) {
	srv, reqs := newWebhookServer(t, func(_ int, _ webhookRequest, _ http.ResponseWriter) (int, string) {
		return 400, "bad request"
	})
	w, err := NewWebhooks(WebhookConfig{
		DeliveryConfig: DeliveryConfig{URL: srv.URL, MaxRetries: 4, BackoffMin: time.Millisecond, BackoffMax: 5 * time.Millisecond},
		AllowInsecure:  true,
	})
	if err != nil {
		t.Fatalf("NewWebhooks: %v", err)
	}
	w.notify(context.Background(), EventRunAfter, w.resolve(EventRunAfter, DeliveryAsync), RunSummary{})
	closeWebhooks(t, w)
	if got := reqs(); len(got) != 1 {
		t.Fatalf("got %d requests, want 1", len(got))
	}
	if w.Dropped() != 1 {
		t.Errorf("dropped = %d, want 1", w.Dropped())
	}
}

// A 429 is retried and Retry-After is honored.
func TestRetryAfterOn429(t *testing.T) {
	srv, reqs := newWebhookServer(t, func(attempt int, _ webhookRequest, w http.ResponseWriter) (int, string) {
		if attempt == 1 {
			w.Header().Set("Retry-After", "1")
			return 429, "slow down"
		}
		return 200, ""
	})
	w, err := NewWebhooks(WebhookConfig{
		DeliveryConfig: DeliveryConfig{URL: srv.URL, MaxRetries: 3, BackoffMin: time.Millisecond, BackoffMax: 5 * time.Second},
		AllowInsecure:  true,
	})
	if err != nil {
		t.Fatalf("NewWebhooks: %v", err)
	}
	start := time.Now()
	w.notify(context.Background(), EventRunAfter, w.resolve(EventRunAfter, DeliveryAsync), RunSummary{})
	closeWebhooks(t, w)
	if got := reqs(); len(got) != 2 {
		t.Fatalf("got %d requests, want 2", len(got))
	}
	if elapsed := time.Since(start); elapsed < 900*time.Millisecond {
		t.Errorf("retried after %v, want the Retry-After second honored", elapsed)
	}
}

// An attempt that exceeds the timeout fails and is not retried past
// MaxRetries.
func TestAttemptTimeout(t *testing.T) {
	srv, reqs := newWebhookServer(t, func(_ int, _ webhookRequest, w http.ResponseWriter) (int, string) {
		time.Sleep(300 * time.Millisecond)
		return 200, ""
	})
	w, err := NewWebhooks(WebhookConfig{
		DeliveryConfig: DeliveryConfig{URL: srv.URL, MaxRetries: -1, Timeout: 50 * time.Millisecond},
		AllowInsecure:  true,
	})
	if err != nil {
		t.Fatalf("NewWebhooks: %v", err)
	}
	w.notify(context.Background(), EventRunAfter, w.resolve(EventRunAfter, DeliveryAsync), RunSummary{})
	closeWebhooks(t, w)
	if got := reqs(); len(got) != 1 {
		t.Fatalf("got %d requests, want 1", len(got))
	}
	if w.Dropped() != 1 {
		t.Errorf("dropped = %d, want 1", w.Dropped())
	}
}

// The circuit breaker opens after consecutive failures and fails fast, then
// lets one probe through after the cooldown.
func TestCircuitBreaker(t *testing.T) {
	srv, reqs := newWebhookServer(t, func(_ int, _ webhookRequest, _ http.ResponseWriter) (int, string) {
		return 500, "down"
	})
	w, err := NewWebhooks(WebhookConfig{
		DeliveryConfig: fastDelivery(srv.URL),
		AllowInsecure:  true,
		Workers:        1, // one worker, so failures are recorded in order
	})
	if err != nil {
		t.Fatalf("NewWebhooks: %v", err)
	}
	w.breakerThreshold = 2
	w.breakerCooldown = 100 * time.Millisecond

	rd := w.resolve(EventRunAfter, DeliveryAsync)
	w.notify(context.Background(), EventRunAfter, rd, RunSummary{}) // failure 1
	w.notify(context.Background(), EventRunAfter, rd, RunSummary{}) // failure 2, opens the breaker
	w.notify(context.Background(), EventRunAfter, rd, RunSummary{}) // dropped, no request
	closeWebhooks(t, w)
	if got := reqs(); len(got) != 2 {
		t.Fatalf("got %d requests, want 2 (breaker failed the third fast)", len(got))
	}
	if w.Dropped() != 3 {
		t.Errorf("dropped = %d, want 3", w.Dropped())
	}

	time.Sleep(150 * time.Millisecond)
	w2notify := w.notify // the dispatcher is closed; use a fresh one for the probe
	_ = w2notify
	w2, err := NewWebhooks(WebhookConfig{
		DeliveryConfig: fastDelivery(srv.URL),
		AllowInsecure:  true,
	})
	if err != nil {
		t.Fatalf("NewWebhooks: %v", err)
	}
	w2.breakerThreshold = 2
	w2.breakerCooldown = 100 * time.Millisecond
	br := w2.breakerFor(srv.URL)
	br.recordFailure()
	br.recordFailure() // open
	if br.allow() {
		t.Error("breaker should refuse while open")
	}
	time.Sleep(150 * time.Millisecond)
	if !br.allow() {
		t.Fatal("breaker should let a probe through after the cooldown")
	}
	if br.allow() {
		t.Error("only one probe at a time")
	}
	br.recordFailure() // probe failed, reopens
	if br.allow() {
		t.Error("breaker should reopen after a failed probe")
	}
	br.recordSuccess()
	if !br.allow() {
		t.Error("breaker should close after a success")
	}
}

// A full queue drops the newest delivery instead of blocking the caller.
func TestQueueFullDropsNewest(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	srv, reqs := newWebhookServer(t, func(_ int, _ webhookRequest, _ http.ResponseWriter) (int, string) {
		once.Do(func() { close(started) })
		<-release
		return 200, ""
	})
	w, err := NewWebhooks(WebhookConfig{
		DeliveryConfig: fastDelivery(srv.URL),
		AllowInsecure:  true,
		Workers:        1,
		QueueSize:      1,
	})
	if err != nil {
		t.Fatalf("NewWebhooks: %v", err)
	}
	rd := w.resolve(EventRunAfter, DeliveryAsync)
	w.notify(context.Background(), EventRunAfter, rd, RunSummary{})
	<-started // the single worker is busy inside the handler
	w.notify(context.Background(), EventRunAfter, rd, RunSummary{})
	w.notify(context.Background(), EventRunAfter, rd, RunSummary{}) // queue full: dropped
	close(release)
	closeWebhooks(t, w)

	if got := reqs(); len(got) != 2 {
		t.Errorf("got %d requests, want 2 (one in flight, one queued)", len(got))
	}
	if w.Dropped() != 1 {
		t.Errorf("dropped = %d, want 1", w.Dropped())
	}
}

// Redact shapes the payload; the size cap replaces an oversized payload with
// a truncation marker.
func TestRedactAndPayloadCap(t *testing.T) {
	redSrv, redReqs := newWebhookServer(t, func(_ int, _ webhookRequest, _ http.ResponseWriter) (int, string) {
		return 200, ""
	})
	w, err := NewWebhooks(WebhookConfig{
		DeliveryConfig: DeliveryConfig{
			URL:             redSrv.URL,
			MaxRetries:      -1,
			Redact:          func(e WebhookEvent, _ any) any { return map[string]any{"event": string(e)} },
			MaxPayloadBytes: 1 << 20,
		},
		AllowInsecure: true,
	})
	if err != nil {
		t.Fatalf("NewWebhooks: %v", err)
	}
	w.notify(context.Background(), EventRunAfter, w.resolve(EventRunAfter, DeliveryAsync), RunSummary{ArcID: "secret-arc"})
	closeWebhooks(t, w)

	capSrv, capReqs := newWebhookServer(t, func(_ int, _ webhookRequest, _ http.ResponseWriter) (int, string) {
		return 200, ""
	})
	w2, err := NewWebhooks(WebhookConfig{
		DeliveryConfig: DeliveryConfig{URL: capSrv.URL, MaxRetries: -1, MaxPayloadBytes: 8},
		AllowInsecure:  true,
	})
	if err != nil {
		t.Fatalf("NewWebhooks: %v", err)
	}
	w2.notify(context.Background(), EventRunAfter, w2.resolve(EventRunAfter, DeliveryAsync), RunSummary{ArcID: "a-very-long-arc-id-that-will-not-fit"})
	closeWebhooks(t, w2)

	red := redReqs()
	if len(red) != 1 {
		t.Fatalf("redact server got %d requests, want 1", len(red))
	}
	cap := capReqs()
	if len(cap) != 1 {
		t.Fatalf("cap server got %d requests, want 1", len(cap))
	}
	got := []webhookRequest{red[0], cap[0]}
	var env struct {
		Data map[string]any `json:"data"`
	}
	if err := json.Unmarshal(got[0].body, &env); err != nil {
		t.Fatalf("envelope: %v", err)
	}
	if env.Data["event"] != "run.after" || env.Data["ArcID"] != nil {
		t.Errorf("redacted data = %v", env.Data)
	}
	if err := json.Unmarshal(got[1].body, &env); err != nil {
		t.Fatalf("envelope: %v", err)
	}
	if env.Data["truncated"] != true || env.Data["original_bytes"] == nil {
		t.Errorf("capped data = %v, want a truncation marker", env.Data)
	}
}

// Close is idempotent, drains queued deliveries, and is a no-op on a
// dispatcher that never started.
func TestCloseDrainsAndIsIdempotent(t *testing.T) {
	srv, reqs := newWebhookServer(t, func(_ int, _ webhookRequest, _ http.ResponseWriter) (int, string) {
		return 200, ""
	})
	w, err := NewWebhooks(WebhookConfig{DeliveryConfig: fastDelivery(srv.URL), AllowInsecure: true})
	if err != nil {
		t.Fatalf("NewWebhooks: %v", err)
	}
	w.notify(context.Background(), EventRunAfter, w.resolve(EventRunAfter, DeliveryAsync), RunSummary{})
	closeWebhooks(t, w)
	closeWebhooks(t, w) // second Close is a no-op
	if got := reqs(); len(got) != 1 {
		t.Errorf("got %d requests after Close, want the queued one drained", len(got))
	}

	fresh, err := NewWebhooks(WebhookConfig{DeliveryConfig: fastDelivery(srv.URL), AllowInsecure: true})
	if err != nil {
		t.Fatalf("NewWebhooks: %v", err)
	}
	closeWebhooks(t, fresh) // never delivered, never started
}

// webhookAgent builds an agent whose delete_pod call is the test's vehicle.
func webhookAgent(t *testing.T, invoked *bool, opts ...AgentOption) *Agent {
	t.Helper()
	registry := registryWith(t, "delete_pod", func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		*invoked = true
		return textResult("deleted"), nil
	})
	llm := &scriptedLLM{replies: []*LLMResponse{
		toolCallReply("call_1", "delete_pod", map[string]any{"name": "web-1"}),
		finalReply("not deleting anything"),
	}}
	agent, err := NewAgent(&AgentConfig{}, registry, llm, opts...)
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	return agent
}

func runWebhookAgent(t *testing.T, agent *Agent) (*fakePersistor, *history.History) {
	t.Helper()
	persistor := &fakePersistor{next: 1}
	h := userArcHistory(t)
	if err := agent.Run(context.Background(), RunOptions{Persistor: persistor, History: h, ArcID: testArcID}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	return persistor, h
}

// A sync tool_call.before webhook decides inline: its reply body decodes into
// ToolCallEffects and blocks the call.
func TestSyncWebhookDecidesBlock(t *testing.T) {
	srv, reqs := newWebhookServer(t, func(_ int, _ webhookRequest, _ http.ResponseWriter) (int, string) {
		return 200, `{"Block":{"Message":"nope"}}`
	})
	w, err := NewWebhooks(WebhookConfig{
		DeliveryConfig: DeliveryConfig{URL: srv.URL, MaxRetries: -1},
		AllowInsecure:  true,
	})
	if err != nil {
		t.Fatalf("NewWebhooks: %v", err)
	}
	invoked := false
	agent := webhookAgent(t, &invoked, WithWebhooks(w))
	persistor, h := runWebhookAgent(t, agent)
	closeWebhooks(t, w)

	if invoked {
		t.Error("the blocked tool ran")
	}
	blocked := persistedOfType(persistor, events.GuardrailBlockedEventType)
	if len(blocked) != 1 {
		t.Fatalf("persisted %d GUARDRAIL_BLOCKED events, want 1", len(blocked))
	}
	if got := blocked[0].Payload.(events.GuardrailBlockedPayload); got.Message != "nope" {
		t.Errorf("block message = %q, want the webhook's", got.Message)
	}
	if h.Arcs[0].Responses[0].ToolCalls[0].Status != history.ToolCallStatusBlocked {
		t.Errorf("call status = %s, want BLOCKED", h.Arcs[0].Responses[0].ToolCalls[0].Status)
	}
	// The decision event was delivered synchronously: the server saw it
	// before the run ended.
	seen := false
	for _, r := range reqs() {
		if r.event == "tool_call.before" {
			seen = true
		}
	}
	if !seen {
		t.Error("server never saw tool_call.before")
	}
}

// A failing sync Before* webhook fails closed by default: the call settles as
// an error and never runs.
func TestSyncWebhookFailClosed(t *testing.T) {
	good, _ := newWebhookServer(t, func(_ int, _ webhookRequest, _ http.ResponseWriter) (int, string) {
		return 200, ""
	})
	bad, _ := newWebhookServer(t, func(_ int, _ webhookRequest, _ http.ResponseWriter) (int, string) {
		return 500, "down"
	})
	w, err := NewWebhooks(WebhookConfig{
		DeliveryConfig: DeliveryConfig{URL: good.URL, MaxRetries: -1},
		Events:         map[WebhookEvent]DeliveryConfig{EventToolCallBefore: {URL: bad.URL}},
		AllowInsecure:  true,
	})
	if err != nil {
		t.Fatalf("NewWebhooks: %v", err)
	}
	invoked := false
	agent := webhookAgent(t, &invoked, WithWebhooks(w))
	persistor, _ := runWebhookAgent(t, agent)
	closeWebhooks(t, w)

	if invoked {
		t.Error("the tool ran despite the failing webhook")
	}
	results := persistedOfType(persistor, events.ToolResultEventType)
	if len(results) != 1 {
		t.Fatalf("persisted %d TOOL_RESULT events, want 1", len(results))
	}
	pl := results[0].Payload.(events.ToolResultPayload)
	if !pl.IsError || !strings.Contains(fmt.Sprint(pl.Result), "tool_call.before webhook failed") {
		t.Errorf("tool result = %+v, want an error naming the webhook", pl)
	}
}

// FailOpen lets the step proceed when the sync webhook fails.
func TestSyncWebhookFailOpen(t *testing.T) {
	good, _ := newWebhookServer(t, func(_ int, _ webhookRequest, _ http.ResponseWriter) (int, string) {
		return 200, ""
	})
	bad, _ := newWebhookServer(t, func(_ int, _ webhookRequest, _ http.ResponseWriter) (int, string) {
		return 500, "down"
	})
	w, err := NewWebhooks(WebhookConfig{
		DeliveryConfig: DeliveryConfig{URL: good.URL, MaxRetries: -1},
		Events:         map[WebhookEvent]DeliveryConfig{EventToolCallBefore: {URL: bad.URL, FailPolicy: FailOpen}},
		AllowInsecure:  true,
	})
	if err != nil {
		t.Fatalf("NewWebhooks: %v", err)
	}
	invoked := false
	agent := webhookAgent(t, &invoked, WithWebhooks(w))
	_, h := runWebhookAgent(t, agent)
	closeWebhooks(t, w)

	if !invoked {
		t.Error("the tool did not run under fail-open")
	}
	if h.Arcs[0].Status() != history.ArcStatusDone {
		t.Errorf("arc = %s, want DONE", h.Arcs[0].Status())
	}
}

// A local hook decides first and its decision stands; the webhook is still
// notified, asynchronously.
func TestLocalHookWinsAndWebhookNotified(t *testing.T) {
	srv, reqs := newWebhookServer(t, func(_ int, _ webhookRequest, _ http.ResponseWriter) (int, string) {
		return 200, `{"Run":true}`
	})
	w, err := NewWebhooks(WebhookConfig{
		DeliveryConfig: DeliveryConfig{URL: srv.URL, MaxRetries: -1},
		AllowInsecure:  true,
	})
	if err != nil {
		t.Fatalf("NewWebhooks: %v", err)
	}
	invoked := false
	agent := webhookAgent(t, &invoked,
		WithHooks(Hooks{
			BeforeToolCall: func(context.Context, ToolCallView) (ToolCallEffects, error) {
				return ToolCallEffects{HookEffects: HookEffects{Block: &BlockRequest{Message: "local says no"}}}, nil
			},
		}),
		WithWebhooks(w),
	)
	persistor, _ := runWebhookAgent(t, agent)
	closeWebhooks(t, w)

	if invoked {
		t.Error("the blocked tool ran")
	}
	blocked := persistedOfType(persistor, events.GuardrailBlockedEventType)
	if len(blocked) != 1 || blocked[0].Payload.(events.GuardrailBlockedPayload).Message != "local says no" {
		t.Fatalf("blocked = %v, want the local hook's message", blocked)
	}
	seen := false
	for _, r := range reqs() {
		if r.event == "tool_call.before" {
			seen = true
		}
	}
	if !seen {
		t.Error("webhook was not notified of the locally decided call")
	}
}

// An async Before* webhook is notification-only: the call runs and the
// receiver gets the event.
func TestAsyncBeforeIsPassThrough(t *testing.T) {
	srv, reqs := newWebhookServer(t, func(_ int, _ webhookRequest, _ http.ResponseWriter) (int, string) {
		return 200, `{"Block":{"Message":"ignored, this is async"}}`
	})
	w, err := NewWebhooks(WebhookConfig{
		DeliveryConfig: DeliveryConfig{URL: srv.URL, MaxRetries: -1},
		Events:         map[WebhookEvent]DeliveryConfig{EventToolCallBefore: {Mode: DeliveryAsync}},
		AllowInsecure:  true,
	})
	if err != nil {
		t.Fatalf("NewWebhooks: %v", err)
	}
	invoked := false
	agent := webhookAgent(t, &invoked, WithWebhooks(w))
	_, h := runWebhookAgent(t, agent)
	closeWebhooks(t, w)

	if !invoked {
		t.Error("the call did not run under an async before webhook")
	}
	if h.Arcs[0].Status() != history.ArcStatusDone {
		t.Errorf("arc = %s, want DONE", h.Arcs[0].Status())
	}
	seen := false
	for _, r := range reqs() {
		if r.event == "tool_call.before" {
			seen = true
		}
	}
	if !seen {
		t.Error("webhook was not notified of tool_call.before")
	}
}

// A failing sync info-hook webhook only logs: a hook that returns nothing can
// never fail the run, and neither can its webhook.
func TestSyncInfoHookNeverFailsRun(t *testing.T) {
	good, _ := newWebhookServer(t, func(_ int, _ webhookRequest, _ http.ResponseWriter) (int, string) {
		return 200, ""
	})
	bad, badReqs := newWebhookServer(t, func(_ int, _ webhookRequest, _ http.ResponseWriter) (int, string) {
		return 500, "down"
	})
	w, err := NewWebhooks(WebhookConfig{
		DeliveryConfig: DeliveryConfig{URL: good.URL, MaxRetries: -1},
		Events:         map[WebhookEvent]DeliveryConfig{EventRunAfter: {URL: bad.URL, Mode: DeliverySync}},
		AllowInsecure:  true,
	})
	if err != nil {
		t.Fatalf("NewWebhooks: %v", err)
	}
	invoked := false
	agent := webhookAgent(t, &invoked, WithWebhooks(w))
	_, h := runWebhookAgent(t, agent)
	closeWebhooks(t, w)

	if !invoked || h.Arcs[0].Status() != history.ArcStatusDone {
		t.Errorf("invoked=%v arc=%s, want the run to succeed despite the failing after webhook", invoked, h.Arcs[0].Status())
	}
	seen := false
	for _, r := range badReqs() {
		if r.event == "run.after" {
			seen = true
		}
	}
	if !seen {
		t.Error("bad server never saw run.after")
	}
}
