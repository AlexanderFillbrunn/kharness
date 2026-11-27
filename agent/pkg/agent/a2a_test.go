package agent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"fillbrunn.ai/kube-agent/proto/pkg/common"
	"fillbrunn.ai/kube-agent/proto/pkg/events"
)

// a2aFixture is a fake A2A agent: an agent card with one skill and a JSON-RPC
// endpoint that records what it was called with.
type a2aFixture struct {
	server *httptest.Server

	mu       sync.Mutex
	calls    []string
	lastSend map[string]any
	cancelID string
	auth     string
	tenant   string
}

func newA2AFixture(t *testing.T) *a2aFixture {
	t.Helper()
	f := &a2aFixture{}
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.auth = r.Header.Get("Authorization")
		f.tenant = r.Header.Get("X-Tenant")
		f.mu.Unlock()
		if r.Method == http.MethodGet && r.URL.Path == "/.well-known/agent-card.json" {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"name": "remote", "description": "a remote agent",
				"url": f.server.URL,
				"skills": []any{map[string]any{
					"id": "research", "name": "Research", "description": "researches a topic",
				}},
			})
			return
		}
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			JSONRPC string          `json:"jsonrpc"`
			ID      json.RawMessage `json:"id"`
			Method  string          `json:"method"`
			Params  json.RawMessage `json:"params"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var params map[string]any
		_ = json.Unmarshal(req.Params, &params)
		f.mu.Lock()
		f.calls = append(f.calls, req.Method)
		if req.Method == "message/send" {
			f.lastSend, _ = params["message"].(map[string]any)
		}
		if req.Method == "tasks/cancel" {
			f.cancelID, _ = params["id"].(string)
		}
		f.mu.Unlock()

		var result any
		switch req.Method {
		case "message/send":
			result = map[string]any{
				"kind": "task", "id": "srv-task-1", "contextId": "ctx-1",
				"status": map[string]any{"state": "submitted"},
			}
		case "tasks/cancel":
			result = map[string]any{
				"kind": "task", "id": "srv-task-1",
				"status": map[string]any{"state": "canceled"},
			}
		default:
			result = map[string]any{}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": result})
	})
	f.server = httptest.NewServer(mux)
	t.Cleanup(f.server.Close)
	return f
}

// WithA2A offers the card's skills as task kinds, and a run with no controller
// of its own starts the task through the agent's own source: the launch
// carries our task ID as the A2A taskId and the session as the contextId, and
// the server's task ID comes back as the launch result's external ref.
func TestWithA2AStartsRemoteTasks(t *testing.T) {
	fx := newA2AFixture(t)
	agent, err := NewAgent(nil, NewToolRegistry(), &scriptedLLM{replies: []*LLMResponse{
		toolCallReply("call_1", "research", map[string]any{"args": map[string]any{"topic": "kubernetes"}}),
		finalReply("started"),
	}}, WithA2A(A2AConfig{Name: "remote", BaseURL: fx.server.URL}))
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}

	// The skill is offered as a task kind on the model-facing surface, with
	// the launch envelope {contextId?, args} as its args schema.
	names := toolNames(taskToolSet{agent.tasks}.All())
	found := false
	for _, n := range names {
		if n == "research" {
			found = true
		}
	}
	if !found {
		t.Fatalf("task tools = %v, want the card's research skill", names)
	}
	meta, _ := agent.tasks.Get("research")
	schema, ok := meta.InputSchema.(map[string]any)
	if !ok {
		t.Fatalf("research input schema = %#v, want the launch envelope object", meta.InputSchema)
	}
	props, _ := schema["properties"].(map[string]any)
	if _, ok := props["contextId"]; !ok {
		t.Errorf("launch envelope properties = %v, want a contextId property", props)
	}
	if _, ok := props["args"]; !ok {
		t.Errorf("launch envelope properties = %v, want an args property", props)
	}

	persistor := &fakePersistor{next: 1}
	h := userArcHistory(t)
	// No RunOptions.Controller: the agent's own task source stands in.
	if err := agent.Run(context.Background(), RunOptions{Persistor: persistor, History: h, ArcID: testArcID}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	invokes := persistedOfType(persistor, events.AsyncTaskInvokeEventType)
	if len(invokes) != 1 {
		t.Fatalf("persisted %d ASYNC_TASK_INVOKE events, want 1", len(invokes))
	}
	launchResults := persistedOfType(persistor, events.AsyncTaskLaunchResultEventType)
	if len(launchResults) != 1 {
		t.Fatalf("persisted %d ASYNC_TASK_LAUNCH_RESULT events, want 1", len(launchResults))
	}
	lr := launchResults[0].Payload.(events.AsyncTaskLaunchResultPayload)
	if !lr.Launched || lr.ExternalRef != "srv-task-1" || lr.CausationID != invokes[0].ID {
		t.Errorf("launch result = %+v, want launched, causation %s, external ref srv-task-1", lr, invokes[0].ID)
	}

	fx.mu.Lock()
	send := fx.lastSend
	calls := append([]string(nil), fx.calls...)
	fx.mu.Unlock()
	// The run had no controller of its own, so the agent's source stood in
	// without a session binding: the taskId is ours, no contextId is sent, and
	// the parts carry the envelope's inner args only.
	if send["taskId"] != invokes[0].ID {
		t.Errorf("message/send carried taskId=%v, want our task id %s", send["taskId"], invokes[0].ID)
	}
	if _, ok := send["contextId"]; ok {
		t.Errorf("message/send carried contextId=%v, want none: the server generates a new context", send["contextId"])
	}
	parts, _ := send["parts"].([]any)
	if len(parts) != 1 {
		t.Fatalf("message parts = %v, want the one data part", send["parts"])
	}
	part, _ := parts[0].(map[string]any)
	if data, _ := part["data"].(map[string]any); data["topic"] != "kubernetes" || len(data) != 1 {
		t.Errorf("message part = %v, want only the inner args, no envelope keys", part)
	}
	if len(calls) == 0 || calls[0] != "message/send" {
		t.Errorf("RPC calls = %v, want message/send first", calls)
	}

	// The synthesized start result carries the launch response, so the
	// server-generated context id reaches the model to reuse for follow-ups.
	if text := projectedTaskResult(t, h, testArcID, invokes[0].ID).Content; !strings.Contains(text, "ctx-1") {
		t.Errorf("projected start result = %q, want it to carry the launch response's context id ctx-1", text)
	}
}

// Cancel and ProvideInput address the remote task by the vendor ID the launch
// returned, not by ours.
func TestWithA2ACancelAndInputUseVendorID(t *testing.T) {
	fx := newA2AFixture(t)
	agent, err := NewAgent(nil, NewToolRegistry(), &scriptedLLM{}, WithA2A(A2AConfig{
		Name: "remote", BaseURL: fx.server.URL, AuthToken: "secret", Headers: map[string]string{"X-Tenant": "acme"},
	}))
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	controller, ok := agent.tasks.(common.TaskController)
	if !ok {
		t.Fatal("the agent's task set is not a controller")
	}

	ref := common.TaskRef{SessionID: testSessionID, ID: "task-1"}
	launch, err := controller.Start(context.Background(), "research", ref,
		map[string]any{"contextId": "ctx-9", "args": map[string]any{"topic": "x"}})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if launch.ExternalRef != "srv-task-1" {
		t.Fatalf("launch external ref = %q, want srv-task-1", launch.ExternalRef)
	}

	fx.mu.Lock()
	start := fx.lastSend
	fx.mu.Unlock()
	if start["contextId"] != "ctx-9" {
		t.Errorf("start message contextId = %v, want the envelope's ctx-9", start["contextId"])
	}
	parts, _ := start["parts"].([]any)
	if len(parts) != 1 {
		t.Fatalf("start message parts = %v, want the one data part", start["parts"])
	}
	if data, _ := parts[0].(map[string]any)["data"].(map[string]any); data["topic"] != "x" || len(data) != 1 {
		t.Errorf("start message part = %v, want only the inner args, no contextId", parts[0])
	}

	if err := controller.Cancel(context.Background(), ref, "no longer needed"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if err := controller.ProvideInput(context.Background(), ref, "req-1", "kube-system"); err != nil {
		t.Fatalf("ProvideInput: %v", err)
	}

	fx.mu.Lock()
	defer fx.mu.Unlock()
	if fx.cancelID != "srv-task-1" {
		t.Errorf("tasks/cancel id = %q, want the vendor id srv-task-1", fx.cancelID)
	}
	if fx.lastSend["taskId"] != "srv-task-1" {
		t.Errorf("input message taskId = %v, want the vendor id", fx.lastSend["taskId"])
	}
	if _, ok := fx.lastSend["contextId"]; ok {
		t.Errorf("input message carried contextId=%v, want none: the vendor task id addresses the task", fx.lastSend["contextId"])
	}
	if fx.auth != "Bearer secret" || fx.tenant != "acme" {
		t.Errorf("RPC saw auth=%q tenant=%q, want the configured credentials", fx.auth, fx.tenant)
	}
}

// An unreachable agent fails the build rather than a run.
func TestWithA2AUnreachableFailsBuild(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	srv.Close()
	_, err := NewAgent(nil, NewToolRegistry(), &scriptedLLM{}, WithA2A(A2AConfig{
		Name: "down", BaseURL: srv.URL,
	}))
	if err == nil || !strings.Contains(err.Error(), "fetch card") {
		t.Errorf("NewAgent error = %v, want the card fetch to fail the build", err)
	}
}

// A remote skill whose name a local task kind already took fails the build.
func TestWithA2ATaskNameCollisionFailsBuild(t *testing.T) {
	fx := newA2AFixture(t)
	local := common.NewTaskRegistry()
	if err := local.RegisterTask(common.TaskMetadata{Name: "research"}); err != nil {
		t.Fatalf("RegisterTask: %v", err)
	}
	_, err := NewAgent(nil, NewToolRegistry(), &scriptedLLM{},
		WithTasks(local), WithA2A(A2AConfig{Name: "remote", BaseURL: fx.server.URL}))
	if err == nil || !strings.Contains(err.Error(), "research") {
		t.Errorf("NewAgent error = %v, want the collision named", err)
	}
}
