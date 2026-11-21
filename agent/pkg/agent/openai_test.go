package agent

import (
	"fillbrunn.ai/kube-agent/proto/pkg/common"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fillbrunn.ai/kube-agent/proto/pkg/events"
	"fillbrunn.ai/kube-agent/agent/pkg/history"
	"fillbrunn.ai/kube-agent/proto/pkg/users"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	testArcID     = "arc-1"
	testSessionID = "session"
)

// testRef addresses the arc the fixtures build, the way everything that
// stores, locks or routes by arc is addressed.
func testRef() common.ArcRef { return common.ArcRef{SessionID: testSessionID, ArcID: testArcID} }

// testAgentID names the agent the fixtures run under, the way a work item
// would name it.
const testAgentID = "test-agent"

// newTestServer serves one canned response and captures the request that asked
// for it.
func newTestServer(t *testing.T, body string, captured *openAIRequest) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request: %v", err)
		}
		if captured != nil {
			if err := json.Unmarshal(raw, captured); err != nil {
				t.Errorf("unmarshal request: %v", err)
			}
		}
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, body)
	}))
	t.Cleanup(server.Close)
	return server
}

func newTestLLM(t *testing.T, server *httptest.Server) *OpenAI {
	t.Helper()
	return NewOpenAI("test-key", "gpt-5", WithBaseURL(server.URL))
}

// userArcHistory returns a history holding one arc and nothing else.
func userArcHistory(t *testing.T) *history.History {
	t.Helper()
	arc := testArcID
	actor := users.Actor{UserID: "alice", TenantID: "acme"}
	h, err := history.Replay(testSessionID, []events.Event{{
		SessionID: testSessionID, ID: arc, Seq: 0, Timestamp: time.Unix(0, 0),
		Typ: events.UserMessageEventType, CorrelationID: &arc, Actor: actor,
		Payload: events.UserMessagePayload{Content: "how many pods are pending?", Actor: actor},
	}})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	return h
}

// projectLast projects the last arc of a history the way a run projects the
// arc it is working: default modes, no extra projection options.
func projectLast(t *testing.T, h *history.History) *history.ModelTranscript {
	t.Helper()
	transcript, err := h.Project(h.Arcs[len(h.Arcs)-1].ID, history.DefaultModes(), history.ProjectionOptions{})
	if err != nil {
		t.Fatalf("project: %v", err)
	}
	return transcript
}

func collectTokens() (*[]string, common.Sink[RawToken]) {
	var tokens []string
	return &tokens, common.SinkFromFunc(func(tok RawToken) error {
		tokens = append(tokens, tok.Content)
		return nil
	})
}

func TestStreamResponseYieldsTokensAndReply(t *testing.T) {
	stream := strings.Join([]string{
		`data: {"id":"chatcmpl-9","model":"gpt-5","choices":[{"delta":{"role":"assistant","content":"Checking"}}]}`,
		`data: {"id":"chatcmpl-9","choices":[{"delta":{"content":" pods"}}]}`,
		`data: {"id":"chatcmpl-9","choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_abc","type":"function","function":{"name":"list_pods","arguments":"{\"namespace\":"}}]}}]}`,
		`data: {"id":"chatcmpl-9","choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"kube-system\"}"}}]}}]}`,
		`data: {"id":"chatcmpl-9","choices":[{"delta":{},"finish_reason":"tool_calls"}]}`,
		`data: {"id":"chatcmpl-9","choices":[],"usage":{"prompt_tokens":120,"completion_tokens":18,"prompt_tokens_details":{"cached_tokens":64}}}`,
		"data: [DONE]",
		"",
	}, "\n\n")

	llm := newTestLLM(t, newTestServer(t, stream, nil))
	tokens, tokSink := collectTokens()

	response, err := llm.StreamResponse(context.Background(), projectLast(t, userArcHistory(t)), &LLMOptions{}, tokSink)
	if err != nil {
		t.Fatalf("StreamResponse: %v", err)
	}

	if got := strings.Join(*tokens, "|"); got != "Checking| pods" {
		t.Errorf("tokens = %q, want %q", got, "Checking| pods")
	}
	if response.Content != "Checking pods" {
		t.Errorf("content = %q, want %q", response.Content, "Checking pods")
	}
	if response.VendorID == nil || *response.VendorID != "chatcmpl-9" {
		t.Errorf("vendor ID = %v, want chatcmpl-9", response.VendorID)
	}
	if response.ModelID != "gpt-5" {
		t.Errorf("model = %q, want gpt-5", response.ModelID)
	}

	if len(response.ToolCalls) != 1 {
		t.Fatalf("requested %d calls, want 1", len(response.ToolCalls))
	}
	call := response.ToolCalls[0]
	if call.VendorID != "call_abc" || call.ToolName != "list_pods" {
		t.Errorf("call = %+v, want the list_pods call issued as call_abc", call)
	}
	args, ok := call.Args.(map[string]any)
	if !ok || args["namespace"] != "kube-system" {
		t.Errorf("args = %#v, want namespace kube-system", call.Args)
	}

	want := TokenUsage{Input: 120, CachedInput: 64, Output: 18}
	if response.Usage == nil || *response.Usage != want {
		t.Errorf("usage = %+v, want %+v", response.Usage, want)
	}
}

// A stream reports nothing about its cost unless the request asks for it.
func TestStreamAsksForUsage(t *testing.T) {
	var captured openAIRequest
	stream := "data: [DONE]\n\n"
	llm := newTestLLM(t, newTestServer(t, stream, &captured))
	_, tokSink := collectTokens()

	if _, err := llm.StreamResponse(context.Background(), projectLast(t, userArcHistory(t)), &LLMOptions{}, tokSink); err != nil {
		t.Fatalf("StreamResponse: %v", err)
	}
	if captured.StreamOptions == nil || !captured.StreamOptions.IncludeUsage {
		t.Errorf("stream options = %+v, want usage included", captured.StreamOptions)
	}
}

// An endpoint that reports no usage leaves it unrecorded, because a zero in
// the log has to mean a measured zero.
func TestUnreportedUsageStaysNil(t *testing.T) {
	body := `{"id":"chatcmpl-3","model":"gpt-5","choices":[{"message":{"content":"two"},"finish_reason":"stop"}]}`
	llm := newTestLLM(t, newTestServer(t, body, nil))

	response, err := llm.Response(context.Background(), projectLast(t, userArcHistory(t)), &LLMOptions{})
	if err != nil {
		t.Fatalf("Response: %v", err)
	}
	if response.Usage != nil {
		t.Errorf("usage = %+v, want nil", response.Usage)
	}
	if recordedUsage(response.Usage) != nil {
		t.Error("an unreported cost reached the payload")
	}
}

func TestResponseEncodesPromptAndTools(t *testing.T) {
	registry := ToolRegistry{tools: map[string]registeredTool{}}
	if err := registry.RegisterTool(&mcp.Tool{
		Name:        "list_pods",
		Description: "list pods in a namespace",
		InputSchema: map[string]any{"type": "object"},
	}, nil); err != nil {
		t.Fatalf("register: %v", err)
	}

	var captured openAIRequest
	body := `{"id":"chatcmpl-1","model":"gpt-5","choices":[{"message":{"content":"two"},"finish_reason":"stop"}],` +
		`"usage":{"prompt_tokens":80,"completion_tokens":3,"prompt_tokens_details":{"cached_tokens":0}}}`
	llm := newTestLLM(t, newTestServer(t, body, &captured))

	opts := &LLMOptions{SystemPrompt: "you are a cluster operator", Tools: &registry}
	response, err := llm.Response(context.Background(), projectLast(t, userArcHistory(t)), opts)
	if err != nil {
		t.Fatalf("Response: %v", err)
	}

	if captured.Stream {
		t.Error("Response asked for a stream")
	}
	if len(captured.Messages) != 2 {
		t.Fatalf("sent %d messages, want 2", len(captured.Messages))
	}
	if captured.Messages[0].Role != openAIRoleSystem || *captured.Messages[0].Content != opts.SystemPrompt {
		t.Errorf("first message = %+v, want the system prompt", captured.Messages[0])
	}
	if captured.Messages[1].Role != openAIRoleUser {
		t.Errorf("second message role = %q, want user", captured.Messages[1].Role)
	}
	if len(captured.Tools) != 1 || captured.Tools[0].Function.Name != "list_pods" {
		t.Errorf("tools = %+v, want the list_pods function", captured.Tools)
	}
	if response.Content != "two" || len(response.ToolCalls) != 0 {
		t.Errorf("response = %+v, want the answer two and no calls", response)
	}
	if want := (TokenUsage{Input: 80, Output: 3}); response.Usage == nil || *response.Usage != want {
		t.Errorf("usage = %+v, want %+v", response.Usage, want)
	}
}

// A call the model already issued has to come back to it under the identifier
// it issued, and with its result attached to that identifier.
func TestEncodePairsCallsUnderVendorIDs(t *testing.T) {
	arc := testArcID
	at := time.Unix(0, 0)
	actor := users.Actor{UserID: "alice", TenantID: "acme"}
	h, err := history.Replay("session", []events.Event{
		{
			SessionID: "session", ID: arc, Seq: 0, Timestamp: at,
			Typ: events.UserMessageEventType, CorrelationID: &arc, Actor: actor,
			Payload: events.UserMessagePayload{Content: "how many pods are pending?", Actor: actor},
		},
		{
			SessionID: "session", ID: "msg-1", Seq: 1, Timestamp: at,
			Typ: events.AssistantMessageEventType, CorrelationID: &arc,
			Payload: events.AssistantMessagePayload{ModelID: "gpt-5", Content: "let me look"},
		},
		{
			SessionID: "session", ID: "tc-1", Seq: 2, Timestamp: at,
			Typ: events.ToolCallEventType, CorrelationID: &arc,
			Payload: events.ToolCallPayload{
				MessageID: "msg-1", VendorID: "call_abc", ToolName: "list_pods",
				Args: map[string]any{"namespace": "kube-system"},
			},
		},
		{
			SessionID: "session", ID: "tr-1", Seq: 3, Timestamp: at,
			Typ: events.ToolResultEventType, CorrelationID: &arc,
			Payload: events.ToolResultPayload{CausationID: "tc-1", Result: "2 pending"},
		},
	})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}

	var captured openAIRequest
	body := `{"id":"chatcmpl-2","model":"gpt-5","choices":[{"message":{"content":"two"},"finish_reason":"stop"}]}`
	llm := newTestLLM(t, newTestServer(t, body, &captured))

	if _, err := llm.Response(context.Background(), projectLast(t, h), &LLMOptions{}); err != nil {
		t.Fatalf("Response: %v", err)
	}

	if len(captured.Messages) != 3 {
		t.Fatalf("sent %d messages, want user, assistant and tool", len(captured.Messages))
	}
	assistant := captured.Messages[1]
	if len(assistant.ToolCalls) != 1 || assistant.ToolCalls[0].ID != "call_abc" {
		t.Fatalf("assistant tool calls = %+v, want the call under call_abc", assistant.ToolCalls)
	}
	if assistant.ToolCalls[0].Function.Arguments != `{"namespace":"kube-system"}` {
		t.Errorf("arguments = %q", assistant.ToolCalls[0].Function.Arguments)
	}
	result := captured.Messages[2]
	if result.Role != openAIRoleTool || result.ToolCallID != "call_abc" {
		t.Errorf("result message = %+v, want a tool message keyed by call_abc", result)
	}
}

// taskAskHistory returns a history whose last arc was opened by a task's ask,
// so a request against it carries the ask's opening message.
func taskAskHistory(t *testing.T) *history.History {
	t.Helper()
	arc := testArcID
	askArc := "arc-ask"
	actor := users.Actor{UserID: "alice", TenantID: "acme"}
	at := time.Unix(0, 0)
	h, err := history.Replay(testSessionID, []events.Event{
		{
			SessionID: testSessionID, ID: arc, Seq: 0, Timestamp: at,
			Typ: events.UserMessageEventType, CorrelationID: &arc, Actor: actor,
			Payload: events.UserMessagePayload{Content: "audit the cluster", Actor: actor},
		},
		{
			SessionID: testSessionID, ID: "task-1", Seq: 1, Timestamp: at,
			Typ: events.AsyncTaskInvokeEventType, CorrelationID: &arc, Actor: actor,
			Payload: events.AsyncTaskInvokePayload{TaskName: "auditor", Actor: actor},
		},
		{
			SessionID: testSessionID, ID: "ask-1", Seq: 2, Timestamp: at,
			Typ: events.AsyncTaskInputRequestedEventType, CorrelationID: &askArc, Actor: actor,
			Payload: events.AsyncTaskInputRequestedPayload{CausationID: "task-1", Message: "which namespace?", LLMVisibility: events.LLMVisibilityAsMessage},
		},
	})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	return h
}

// taskOpening returns the message that carries the ask arc's opening content.
func taskOpening(captured *openAIRequest) *openAIMessage {
	for i := range captured.Messages {
		m := &captured.Messages[i]
		if m.Content != nil && strings.Contains(*m.Content, "asks a question") {
			return m
		}
	}
	return nil
}

// A task-opened arc's opening message is not the user speaking, so a provider
// with an operator role can carry it as one. By default it goes out as user,
// which every provider accepts.
func TestEncodeCarriesATaskArcUnderTheConfiguredRole(t *testing.T) {
	body := `{"id":"chatcmpl-3","model":"gpt-5","choices":[{"message":{"content":"ok"},"finish_reason":"stop"}]}`
	h := taskAskHistory(t)

	var captured openAIRequest
	captureServer := newTestServer(t, body, &captured)
	def := NewOpenAI("test-key", "gpt-5", WithBaseURL(captureServer.URL))
	if _, err := def.Response(context.Background(), projectLast(t, h), &LLMOptions{}); err != nil {
		t.Fatalf("default Response: %v", err)
	}
	opening := taskOpening(&captured)
	if opening == nil {
		t.Fatalf("no message carries the ask's opening content in %+v", captured.Messages)
	}
	if opening.Role != openAIRoleUser {
		t.Errorf("default task arc role = %q, want user", opening.Role)
	}
	if !strings.Contains(*opening.Content, "[background task, not the user") {
		t.Errorf("task arc content = %q, want the background-task header", *opening.Content)
	}

	captured = openAIRequest{}
	captureServer = newTestServer(t, body, &captured)
	dev := NewOpenAI("test-key", "gpt-5", WithBaseURL(captureServer.URL), WithTaskRole(openAIRoleDeveloper))
	if _, err := dev.Response(context.Background(), projectLast(t, h), &LLMOptions{}); err != nil {
		t.Fatalf("developer Response: %v", err)
	}
	opening = taskOpening(&captured)
	if opening == nil {
		t.Fatalf("no message carries the ask's opening content in %+v", captured.Messages)
	}
	if opening.Role != openAIRoleDeveloper {
		t.Errorf("configured task arc role = %q, want developer", opening.Role)
	}
}

// A role the API does not define is refused at configuration time, not as a
// 400 on the first request.
func TestSetTaskRoleRefusesAnUnknownRole(t *testing.T) {
	llm := NewOpenAI("test-key", "gpt-5")
	if err := llm.SetTaskRole("bogus"); err == nil {
		t.Error("SetTaskRole accepted a role OpenAI does not define")
	}
	if err := llm.SetTaskRole(openAIRoleDeveloper); err != nil {
		t.Fatalf("SetTaskRole: %v", err)
	}
	if llm.taskRole != openAIRoleDeveloper {
		t.Errorf("taskRole = %q, want developer", llm.taskRole)
	}
}

// A final length stop is a context-budget signal: the partial reply comes
// back non-nil alongside a *ContextOverflowError, so the caller records what
// the model did say and still recognises the budget. An output-limit stop
// such as max_tokens never is: the reply returns with no error at all.
func TestLengthStopReturnsPartialReplyWithOverflow(t *testing.T) {
	body := `{"id":"c1","model":"gpt-5","choices":[{"message":{"content":"partial"},"finish_reason":"length"}]}`
	llm := newTestLLM(t, newTestServer(t, body, nil))
	response, err := llm.Response(context.Background(), projectLast(t, userArcHistory(t)), &LLMOptions{})
	var overflow *ContextOverflowError
	if !errors.As(err, &overflow) {
		t.Fatalf("error is %T (%v), want *ContextOverflowError", err, err)
	}
	if overflow.StopReason != "length" {
		t.Errorf("StopReason = %q, want %q", overflow.StopReason, "length")
	}
	if response == nil || response.Content != "partial" || response.StopReason != "length" {
		t.Errorf("response = %+v, want the partial reply carrying its stop reason", response)
	}

	body = `{"id":"c1","model":"gpt-5","choices":[{"message":{"content":"two"},"finish_reason":"max_tokens"}]}`
	llm = newTestLLM(t, newTestServer(t, body, nil))
	response, err = llm.Response(context.Background(), projectLast(t, userArcHistory(t)), &LLMOptions{})
	if err != nil {
		t.Fatalf("an output-limit stop is not an overflow: %v", err)
	}
	if response.StopReason != "max_tokens" {
		t.Errorf("StopReason = %q, want %q", response.StopReason, "max_tokens")
	}
}

// An over-budget rejection decodes into a *ContextOverflowError carrying the
// status and the error object's type, code and message — typed, and still
// unwrapping to the provider's plain *LLMError.
func TestErrorResponseIsTyped(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, `{"error":{"message":"This model's maximum context length is 32000 tokens","type":"invalid_request_error","code":"context_length_exceeded"}}`)
	}))
	t.Cleanup(server.Close)
	llm := NewOpenAI("k", "gpt-5", WithBaseURL(server.URL))

	_, err := llm.Response(context.Background(), projectLast(t, userArcHistory(t)), &LLMOptions{})
	var overflow *ContextOverflowError
	if !errors.As(err, &overflow) {
		t.Fatalf("error is %T (%v), want *ContextOverflowError", err, err)
	}
	if overflow.Status != 400 || overflow.Code != "context_length_exceeded" || overflow.Type != "invalid_request_error" {
		t.Errorf("ContextOverflowError = %+v", &overflow.LLMError)
	}
	var lerr *LLMError
	if !errors.As(err, &lerr) {
		t.Errorf("the overflow does not unwrap to *LLMError: %v", err)
	}
}

// A rejection that is not over budget stays a plain *LLMError: the vendor's
// classification never rewrites an unrelated complaint.
func TestNonOverflowErrorStaysPlain(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, `{"error":{"message":"some other complaint","type":"invalid_request_error","code":"bad_request"}}`)
	}))
	t.Cleanup(server.Close)
	llm := NewOpenAI("k", "gpt-5", WithBaseURL(server.URL))

	_, err := llm.Response(context.Background(), projectLast(t, userArcHistory(t)), &LLMOptions{})
	var lerr *LLMError
	if !errors.As(err, &lerr) {
		t.Fatalf("error is %T (%v), want *LLMError", err, err)
	}
	var overflow *ContextOverflowError
	if errors.As(err, &overflow) {
		t.Errorf("a non-overflow rejection was classified as one: %v", overflow)
	}
}

// An error object inside an otherwise-200 stream is a typed overflow too,
// with the status the stream carried.
func TestStreamErrorObjectIsTyped(t *testing.T) {
	body := "data: {\"error\":{\"message\":\"prompt is too long\",\"type\":\"invalid_request_error\",\"code\":\"context_length_exceeded\"}}\n\n" +
		"data: [DONE]\n\n"
	llm := newTestLLM(t, newTestServer(t, body, nil))
	_, tokSink := collectTokens()
	_, err := llm.StreamResponse(context.Background(), projectLast(t, userArcHistory(t)), &LLMOptions{}, tokSink)
	var overflow *ContextOverflowError
	if !errors.As(err, &overflow) {
		t.Fatalf("error is %T (%v), want *ContextOverflowError", err, err)
	}
	if overflow.Code != "context_length_exceeded" || overflow.Status != 200 {
		t.Errorf("ContextOverflowError = %+v, want the in-stream code with status 200", &overflow.LLMError)
	}
}

// The vendor classifies where the provider's own fields are read: a context
// code, a 413, a message pattern where no code exists, or an error object
// inside a 200 stream come back as a *ContextOverflowError — never from the
// status alone. A final length stop is classified where the reply is built,
// not from an error (see TestLengthStopReturnsPartialReplyWithOverflow), and a
// non-overflow error crosses unchanged.
func TestAsOverflow(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"the context code", &LLMError{Status: 400, Type: "invalid_request_error", Code: "context_length_exceeded", Message: "too long"}, true},
		{"a 413", &LLMError{Status: 413, Message: "request entity too large"}, true},
		{"a message pattern, no code", &LLMError{Status: 400, Message: "prompt is too long for this model"}, true},
		{"an in-stream error object", &LLMError{Status: 200, Code: "context_length_exceeded", Message: "x"}, true},
		{"the status alone is not enough", &LLMError{Status: 400, Message: "some other complaint"}, false},
		{"an unrelated error", errors.New("connection reset"), false},
	}
	for _, c := range cases {
		got := asOverflow(c.err)
		if (got != nil) != c.want {
			t.Errorf("%s: asOverflow = %v, want overflow %v", c.name, got, c.want)
		}
		if !c.want && overflowOr(c.err) != c.err {
			t.Errorf("%s: overflowOr rewrote a non-overflow error to %v", c.name, overflowOr(c.err))
		}
	}
}

// The compactor's call goes over a transcript, not a history: the request
// carries the transcript's messages and no tools.
func TestRespondToTranscriptSendsNoTools(t *testing.T) {
	var captured openAIRequest
	body := `{"id":"c1","model":"gpt-5","choices":[{"message":{"content":"SUMMARY"},"finish_reason":"stop"}]}`
	llm := newTestLLM(t, newTestServer(t, body, &captured))
	transcript := history.ModelTranscript{Messages: []history.ModelMessage{
		history.ModelTextMessage{MessageRole: history.ModelRoleUser, Content: "what happened?"},
	}}
	response, err := llm.RespondToTranscript(context.Background(), transcript, "summarise")
	if err != nil {
		t.Fatalf("RespondToTranscript: %v", err)
	}
	if response.Content != "SUMMARY" {
		t.Errorf("content = %q, want SUMMARY", response.Content)
	}
	if len(captured.Tools) != 0 {
		t.Errorf("tools = %d, want none on a summarising call", len(captured.Tools))
	}
	if len(captured.Messages) != 2 || captured.Messages[0].Role != "system" || captured.Messages[0].Content == nil || *captured.Messages[0].Content != "summarise" {
		t.Errorf("messages = %+v, want the system prompt then the transcript", captured.Messages)
	}
}
