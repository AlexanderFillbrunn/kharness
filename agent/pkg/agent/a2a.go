package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/a2aproject/a2a-go/a2a"
	"github.com/a2aproject/a2a-go/a2aclient"
	"github.com/a2aproject/a2a-go/a2aclient/agentcard"

	"fillbrunn.ai/kube-agent/proto/pkg/common"
)

// This file bridges one A2A agent into an agent's task surface: WithA2A
// resolves the remote agent's card with the official a2a-go SDK, maps its
// skills to task kinds, and folds a TaskSource over an a2aclient into the
// agent's tasks, so the existing name-collision checks in NewAgent apply to
// remote kinds unchanged and the run's controller (or the agent's own task
// source, when the run has none — see controllerFor) starts them like any
// other task.
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

	// BaseURL is the agent's base URL; the card is resolved from
	// {BaseURL}/.well-known/agent-card.json (then /.well-known/agent.json),
	// and the card's url field is the JSON-RPC endpoint.
	BaseURL string

	// HTTPClient is the client for the card fetch and every RPC. When nil,
	// one is built from AuthToken and Headers; pass one to control TLS,
	// proxies, timeouts.
	HTTPClient *http.Client

	// AuthToken is sent as a bearer token on every request.
	AuthToken string

	// Headers are sent on every request, after AuthToken.
	Headers map[string]string

	// Timeout bounds the card fetch and each call. Zero means
	// DefaultA2ATimeout.
	Timeout time.Duration

	// Logger receives the bridge's records, tagged component=a2a. Nil means
	// slog.Default().
	Logger *slog.Logger
}

// DefaultA2ATimeout is how long WithA2A waits for the card and each call.
const DefaultA2ATimeout = 10 * time.Second

// WithA2A gives an agent the skills of one remote A2A agent as task kinds. The
// card is resolved while the agent is built, so an unreachable agent or a name
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

// connectA2A resolves the card and builds the task source over an a2a-go
// client speaking JSON-RPC.
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
	baseURL := strings.TrimRight(cfg.BaseURL, "/")
	resolver := agentcard.NewResolver(httpClient)
	card, err := resolver.Resolve(ctx, baseURL)
	if err != nil {
		// Older agents publish the card at agent.json.
		card, err = resolver.Resolve(ctx, baseURL, agentcard.WithPath("/.well-known/agent.json"))
	}
	if err != nil {
		return nil, fmt.Errorf("fetch card of A2A agent %s: %w", cfg.Name, err)
	}
	if card.URL == "" {
		card.URL = baseURL
	}
	if card.PreferredTransport == "" {
		card.PreferredTransport = a2a.TransportProtocolJSONRPC
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
		}); err != nil {
			return nil, fmt.Errorf("A2A agent %s: %w", cfg.Name, err)
		}
	}

	client, err := a2aclient.NewFromCard(ctx, card,
		a2aclient.WithConfig(a2aclient.Config{
			PreferredTransports: []a2a.TransportProtocol{a2a.TransportProtocolJSONRPC},
		}),
		a2aclient.WithJSONRPCTransport(httpClient),
	)
	if err != nil {
		return nil, fmt.Errorf("connect to A2A agent %s at %s: %w", cfg.Name, card.URL, err)
	}
	logger.InfoContext(ctx, "A2A agent card loaded", "skills", len(card.Skills), "endpoint", card.URL)

	return &a2aSource{set: registry, controller: &a2aController{
		client: client, timeout: timeout, logger: logger, byTask: map[string]string{},
	}}, nil
}

// a2aSource is one remote A2A agent as a TaskSource: the card's skills as the
// kind metadata, and a controller that speaks A2A through the SDK's client.
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
	client  *a2aclient.Client
	timeout time.Duration
	logger  *slog.Logger

	mu     sync.Mutex
	byTask map[string]string
}

// Start sends the task's args as a message with our task ID as the A2A taskId
// and our session as the contextId, so the server's callbacks echo them back.
// The task object in the reply, when there is one, carries the server's own
// ID, which becomes the launch's ExternalRef.
func (c *a2aController) Start(ctx context.Context, name string, ref common.TaskRef, args any) (*common.TaskLaunch, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	res, err := c.client.SendMessage(ctx, &a2a.MessageSendParams{Message: &a2a.Message{
		ID:        a2a.NewMessageID(),
		Role:      a2a.MessageRoleUser,
		TaskID:    a2a.TaskID(ref.ID),
		ContextID: ref.SessionID,
		Parts:     a2aParts(args),
	}})
	if err != nil {
		return nil, err
	}
	externalRef := ref.ID
	if task, ok := res.(*a2a.Task); ok && task.ID != "" {
		externalRef = string(task.ID)
	}
	c.mu.Lock()
	c.byTask[ref.ID] = externalRef
	c.mu.Unlock()
	c.logger.DebugContext(ctx, "A2A task started", "task", ref.ID, "external_ref", externalRef)
	return &common.TaskLaunch{ExternalRef: externalRef, Response: res}, nil
}

// Cancel asks the remote task to stop, addressed by the vendor task ID.
func (c *a2aController) Cancel(ctx context.Context, ref common.TaskRef, reason string) error {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	_, err := c.client.CancelTask(ctx, &a2a.TaskIDParams{
		ID:       a2a.TaskID(c.vendorID(ref)),
		Metadata: map[string]any{"reason": reason},
	})
	return err
}

// ProvideInput answers a waiting remote task with a message on the same task,
// the request ID carried in the message metadata for the server to correlate.
func (c *a2aController) ProvideInput(ctx context.Context, ref common.TaskRef, requestID, input string) error {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	_, err := c.client.SendMessage(ctx, &a2a.MessageSendParams{Message: &a2a.Message{
		ID:        a2a.NewMessageID(),
		Role:      a2a.MessageRoleUser,
		TaskID:    a2a.TaskID(c.vendorID(ref)),
		ContextID: ref.SessionID,
		Metadata:  map[string]any{"request_id": requestID},
		Parts:     a2a.ContentParts{a2a.TextPart{Text: input}},
	}})
	return err
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

// a2aParts carries the task's args as message parts: a data part when the
// args are a JSON object, a text part holding the JSON otherwise.
func a2aParts(args any) a2a.ContentParts {
	if m, ok := args.(map[string]any); ok {
		return a2a.ContentParts{a2a.DataPart{Data: m}}
	}
	raw, err := json.Marshal(args)
	if err != nil {
		raw = []byte(fmt.Sprintf("%v", args))
	}
	return a2a.ContentParts{a2a.TextPart{Text: string(raw)}}
}
