package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"fillbrunn.ai/kube-agent/proto/pkg/common"
)

// This file bridges one A2A agent into an agent's task surface: WithA2A
// fetches the remote agent's card, maps its skills to task kinds, and folds a
// TaskSource over a minimal JSON-RPC client into the agent's tasks, so the
// existing name-collision checks in NewAgent apply to remote kinds unchanged
// and the run's controller (or the agent's own task source, when the run has
// none — see controllerFor) starts them like any other task.
//
// The launch carries our task ID as the A2A taskId and our session as the
// contextId, so the server echoes them back; when the server answers with a
// task of its own, that ID becomes the launch's ExternalRef and the
// ASYNC_TASK_LAUNCH_RESULT event records it, which is what routes the server's
// later callbacks to the task. Reports from the remote agent reach the log
// through the ingress, as for any runner.
//
// Logging follows docs/LOGGING.md: lines carry IDs, methods and outcomes only,
// never task arguments or input text.

// A2AConfig configures one remote A2A agent whose skills an agent offers as
// task kinds.
type A2AConfig struct {
	// Name identifies the remote agent in errors and log lines.
	Name string

	// BaseURL is the agent's base URL; the card is read from
	// {BaseURL}/.well-known/agent-card.json (then /.well-known/agent.json),
	// and its url field, when present, is the JSON-RPC endpoint.
	BaseURL string

	// HTTPClient is the client for the card fetch and every RPC. When nil,
	// one is built from AuthToken and Headers; pass one to control TLS,
	// proxies, timeouts.
	HTTPClient *http.Client

	// AuthToken is sent as a bearer token on every request.
	AuthToken string

	// Headers are sent on every request, after AuthToken.
	Headers map[string]string

	// Timeout bounds the card fetch and each RPC. Zero means
	// DefaultA2ATimeout.
	Timeout time.Duration

	// Logger receives the bridge's records, tagged component=a2a. Nil means
	// slog.Default().
	Logger *slog.Logger
}

// DefaultA2ATimeout is how long WithA2A waits for the card and each call.
const DefaultA2ATimeout = 10 * time.Second

// WithA2A gives an agent the skills of one remote A2A agent as task kinds. The
// card is fetched while the agent is built, so an unreachable agent or a name
// collision with a local tool or task fails NewAgent rather than a run.
func WithA2A(cfg A2AConfig) AgentOption {
	return func(a *Agent) error {
		src, err := connectA2A(cfg)
		if err != nil {
			return err
		}
		a.tasks = combineTaskSets(a.tasks, src)
		return nil
	}
}

// agentCard is the subset of the A2A agent card the bridge reads.
type agentCard struct {
	Name        string     `json:"name"`
	Description string     `json:"description"`
	URL         string     `json:"url"`
	Version     string     `json:"version"`
	Skills      []a2aSkill `json:"skills"`
}

type a2aSkill struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
	// InputSchema is not part of the core card; honored when the card
	// carries one, as the task kind's input schema.
	InputSchema any `json:"inputSchema,omitempty"`
}

// connectA2A fetches the card and builds the task source over it.
func connectA2A(cfg A2AConfig) (TaskSource, error) {
	if cfg.Name == "" {
		return nil, errors.New("an A2A agent needs a name")
	}
	if cfg.BaseURL == "" {
		return nil, fmt.Errorf("A2A agent %s names no base URL", cfg.Name)
	}
	logger := resolveLogger(cfg.Logger).With("component", "a2a", "agent", cfg.Name)

	httpClient := cfg.HTTPClient
	if httpClient == nil && (cfg.AuthToken != "" || len(cfg.Headers) > 0) {
		httpClient = &http.Client{Transport: &headerTransport{
			base: http.DefaultTransport, token: cfg.AuthToken, headers: cfg.Headers,
		}}
	}
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = DefaultA2ATimeout
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	card, err := fetchAgentCard(ctx, httpClient, strings.TrimRight(cfg.BaseURL, "/"))
	if err != nil {
		return nil, fmt.Errorf("fetch card of A2A agent %s: %w", cfg.Name, err)
	}
	endpoint := card.URL
	if endpoint == "" {
		endpoint = strings.TrimRight(cfg.BaseURL, "/")
	}

	registry := common.NewTaskRegistry()
	for _, skill := range card.Skills {
		name := skill.ID
		if name == "" {
			name = skill.Name
		}
		if name == "" {
			continue
		}
		description := skill.Description
		if description == "" {
			description = card.Description
		}
		if err := registry.RegisterTask(common.TaskMetadata{
			Name:        name,
			Description: description,
			InputSchema: skill.InputSchema,
		}); err != nil {
			return nil, fmt.Errorf("A2A agent %s: %w", cfg.Name, err)
		}
	}
	logger.InfoContext(ctx, "A2A agent card loaded", "skills", len(card.Skills), "endpoint", endpoint)

	client := &a2aClient{endpoint: endpoint, http: httpClient, timeout: timeout, logger: logger}
	return &a2aSource{set: registry, controller: &a2aController{client: client, byTask: map[string]string{}}}, nil
}

// fetchAgentCard reads the card from the well-known locations.
func fetchAgentCard(ctx context.Context, client *http.Client, baseURL string) (*agentCard, error) {
	var lastErr error
	for _, path := range []string{"/.well-known/agent-card.json", "/.well-known/agent.json"} {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+path, nil)
		if err != nil {
			return nil, err
		}
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode == http.StatusNotFound {
			_ = resp.Body.Close()
			lastErr = fmt.Errorf("%s: not found", path)
			continue
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("%s: %s", path, resp.Status)
		}
		var card agentCard
		if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&card); err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		return &card, nil
	}
	return nil, lastErr
}

// a2aClient is a minimal A2A JSON-RPC client: POST, one response, no
// streaming. Enough for message/send, tasks/cancel and tasks/get.
type a2aClient struct {
	endpoint string
	http     *http.Client
	timeout  time.Duration
	logger   *slog.Logger

	mu     sync.Mutex
	nextID int64
}

type rpcRequest struct {
	JSONRPC string `json:"jsonrpc"`
	ID      int64  `json:"id"`
	Method  string `json:"method"`
	Params  any    `json:"params,omitempty"`
}

type rpcResponse struct {
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// call runs one JSON-RPC method and decodes the result into out.
func (c *a2aClient) call(ctx context.Context, method string, params any, out any) error {
	c.mu.Lock()
	c.nextID++
	id := c.nextID
	c.mu.Unlock()

	body, err := json.Marshal(rpcRequest{JSONRPC: "2.0", ID: id, Method: method, Params: params})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	c.logger.DebugContext(ctx, "A2A call", "method", method, "id", id)
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: %s", method, resp.Status)
	}
	var rpcResp rpcResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<22)).Decode(&rpcResp); err != nil {
		return fmt.Errorf("%s: decode response: %w", method, err)
	}
	if rpcResp.Error != nil {
		return fmt.Errorf("%s: rpc error %d: %s", method, rpcResp.Error.Code, rpcResp.Error.Message)
	}
	if out != nil && len(rpcResp.Result) > 0 {
		if err := json.Unmarshal(rpcResp.Result, out); err != nil {
			return fmt.Errorf("%s: decode result: %w", method, err)
		}
	}
	return nil
}

// a2aSource is one remote A2A agent as a TaskSource: the card's skills as the
// kind metadata, and a controller that speaks to the agent over JSON-RPC.
type a2aSource struct {
	set        *common.TaskRegistry
	controller *a2aController
}

func (s *a2aSource) All() []common.TaskMetadata                  { return s.set.All() }
func (s *a2aSource) Get(name string) (common.TaskMetadata, bool) { return s.set.Get(name) }
func (s *a2aSource) Start(ctx context.Context, name string, ref common.TaskRef, args any) (*common.TaskLaunch, error) {
	return s.controller.Start(ctx, name, ref, args)
}
func (s *a2aSource) Cancel(ctx context.Context, ref common.TaskRef, reason string) error {
	return s.controller.Cancel(ctx, ref, reason)
}
func (s *a2aSource) ProvideInput(ctx context.Context, ref common.TaskRef, requestID, input string) error {
	return s.controller.ProvideInput(ctx, ref, requestID, input)
}

// a2aController starts remote tasks and reaches them. It is process-bound:
// the vendor task ID a server assigned is remembered here, keyed by our task
// ID — the log's launch-result event carries it too, but the controller reads
// its own table, since it is the process that started the task.
type a2aController struct {
	client *a2aClient

	mu     sync.Mutex
	byTask map[string]string
}

// Start sends the task's args as a message with our task ID as the A2A taskId
// and our session as the contextId, so the server's callbacks echo them back.
// The task object in the reply, when there is one, carries the server's own
// ID, which becomes the launch's ExternalRef.
func (c *a2aController) Start(ctx context.Context, name string, ref common.TaskRef, args any) (*common.TaskLaunch, error) {
	argsJSON, err := json.Marshal(args)
	if err != nil {
		return nil, fmt.Errorf("marshal args for task %s: %w", name, err)
	}
	params := map[string]any{
		"message": map[string]any{
			"role":      "user",
			"messageId": common.NewEventID(),
			"taskId":    ref.ID,
			"contextId": ref.SessionID,
			"parts":     []any{map[string]any{"kind": "text", "text": string(argsJSON)}},
		},
	}
	var result map[string]any
	if err := c.client.call(ctx, "message/send", params, &result); err != nil {
		return nil, err
	}
	externalRef := ref.ID
	if task, ok := result["task"].(map[string]any); ok {
		if id, ok := task["id"].(string); ok && id != "" {
			externalRef = id
		}
	}
	c.mu.Lock()
	c.byTask[ref.ID] = externalRef
	c.mu.Unlock()
	return &common.TaskLaunch{ExternalRef: externalRef, Response: result}, nil
}

// Cancel asks the remote task to stop, addressed by the vendor task ID.
func (c *a2aController) Cancel(ctx context.Context, ref common.TaskRef, reason string) error {
	return c.client.call(ctx, "tasks/cancel", map[string]any{"id": c.vendorID(ref)}, nil)
}

// ProvideInput answers a waiting remote task with a message on the same task,
// the request ID carried in the message metadata for the server to correlate.
func (c *a2aController) ProvideInput(ctx context.Context, ref common.TaskRef, requestID, input string) error {
	params := map[string]any{
		"message": map[string]any{
			"role":      "user",
			"messageId": common.NewEventID(),
			"taskId":    c.vendorID(ref),
			"contextId": ref.SessionID,
			"metadata":  map[string]any{"request_id": requestID},
			"parts":     []any{map[string]any{"kind": "text", "text": input}},
		},
	}
	return c.client.call(ctx, "message/send", params, nil)
}

// vendorID is the ID the remote server knows the task by: the one it assigned,
// or ours when it took ours.
func (c *a2aController) vendorID(ref common.TaskRef) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if id, ok := c.byTask[ref.ID]; ok {
		return id
	}
	return ref.ID
}
