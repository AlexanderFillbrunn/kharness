package agent

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// This file bridges one MCP server into an agent's tool surface: WithMCP
// connects to the server, lists its tools once at build — one agent offers
// one consistent tool list, the way one arc sees one consistent agent — and
// folds a provider over the session into the agent's tools, so the existing
// name-collision checks in NewAgent apply to remote names unchanged. The
// tool list is fixed for the agent's lifetime; a server that changes its
// list needs a rebuilt agent.
//
// Logging follows docs/LOGGING.md: lines carry the server and tool names and
// outcomes only, never arguments or results.

// MCPServerConfig configures one MCP server whose tools an agent offers.
type MCPServerConfig struct {
	// Name identifies the server in errors and log lines.
	Name string

	// Endpoint is the server's streamable HTTP endpoint. Ignored when
	// Transport is set.
	Endpoint string

	// Transport is an optional custom transport (stdio, SSE, in-memory), for
	// servers that are not streamable-HTTP or for tests.
	Transport mcp.Transport

	// Prefix is prepended to every remote tool's name as offered to the
	// model, so a remote name can be namespaced away from local tools.
	Prefix string

	// HTTPClient is the client for HTTP transports. When nil, one is built
	// from AuthToken and Headers; pass one to control TLS, proxies, timeouts.
	HTTPClient *http.Client

	// AuthToken is sent as a bearer token on every request.
	AuthToken string

	// Headers are sent on every request, after AuthToken.
	Headers map[string]string

	// ConnectTimeout bounds the connect and the initial tool listing. Zero
	// means DefaultMCPConnectTimeout.
	ConnectTimeout time.Duration

	// Logger receives the bridge's records, tagged component=mcp. Nil means
	// slog.Default().
	Logger *slog.Logger
}

// DefaultMCPConnectTimeout is how long WithMCP waits for the server to
// answer the initialize handshake and the tool listing.
const DefaultMCPConnectTimeout = 10 * time.Second

// WithMCP gives an agent the tools of one MCP server. The server is contacted
// while the agent is built, so an unreachable server or a name collision with
// a local tool fails NewAgent rather than a run. The session lives as long as
// the agent.
func WithMCP(cfg MCPServerConfig) AgentOption {
	return func(a *Agent) error {
		provider, err := connectMCP(cfg)
		if err != nil {
			return err
		}
		a.tools = combineProviders(a.tools, provider)
		return nil
	}
}

// headerTransport adds auth headers to every request.
type headerTransport struct {
	base    http.RoundTripper
	token   string
	headers map[string]string
}

func (t *headerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	r := req.Clone(req.Context())
	if t.token != "" {
		r.Header.Set("Authorization", "Bearer "+t.token)
	}
	for k, v := range t.headers {
		r.Header.Set(k, v)
	}
	return t.base.RoundTrip(r)
}

// connectMCP dials the server and lists its tools once.
func connectMCP(cfg MCPServerConfig) (*mcpToolProvider, error) {
	if cfg.Name == "" {
		return nil, fmt.Errorf("an MCP server needs a name")
	}
	logger := resolveLogger(cfg.Logger).With("component", "mcp", "server", cfg.Name)

	httpClient := cfg.HTTPClient
	if httpClient == nil && (cfg.AuthToken != "" || len(cfg.Headers) > 0) {
		httpClient = &http.Client{Transport: &headerTransport{
			base: http.DefaultTransport, token: cfg.AuthToken, headers: cfg.Headers,
		}}
	}
	transport := cfg.Transport
	if transport == nil {
		if cfg.Endpoint == "" {
			return nil, fmt.Errorf("MCP server %s names no endpoint and no transport", cfg.Name)
		}
		transport = &mcp.StreamableClientTransport{
			Endpoint:   cfg.Endpoint,
			HTTPClient: httpClient,
			// The tool list is read once at connect, so the standalone SSE
			// stream would only hold a connection open for list-change
			// notifications nobody acts on.
			DisableStandaloneSSE: true,
		}
	}

	timeout := cfg.ConnectTimeout
	if timeout <= 0 {
		timeout = DefaultMCPConnectTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	client := mcp.NewClient(&mcp.Implementation{Name: "kube-agent", Version: "dev"}, nil)
	session, err := client.Connect(ctx, transport, nil)
	if err != nil {
		return nil, fmt.Errorf("connect MCP server %s: %w", cfg.Name, err)
	}
	listed, err := session.ListTools(ctx, nil)
	if err != nil {
		_ = session.Close()
		return nil, fmt.Errorf("list tools of MCP server %s: %w", cfg.Name, err)
	}

	provider := &mcpToolProvider{
		session: session,
		byName:  map[string]string{},
		logger:  logger,
	}
	for _, tool := range listed.Tools {
		offered := *tool
		offered.Name = cfg.Prefix + tool.Name
		provider.tools = append(provider.tools, &offered)
		provider.byName[offered.Name] = tool.Name
	}
	logger.InfoContext(ctx, "MCP server connected", "tools", len(provider.tools))
	return provider, nil
}

// mcpToolProvider is one MCP server's tools as a ToolProvider: the list the
// model is offered is the one read at connect, and a call by name goes to the
// session under the name the server knows.
type mcpToolProvider struct {
	session *mcp.ClientSession
	tools   []*mcp.Tool
	byName  map[string]string
	logger  *slog.Logger
}

func (p *mcpToolProvider) All() []*mcp.Tool { return p.tools }

func (p *mcpToolProvider) Get(name string) (*mcp.Tool, bool) {
	if _, ok := p.byName[name]; !ok {
		return nil, false
	}
	for _, tool := range p.tools {
		if tool.Name == name {
			return tool, true
		}
	}
	return nil, false
}

// Invoke runs the remote tool. The returned error means the call could not be
// made; a tool reporting its own failure returns an IsError outcome, the
// distinction MCP draws and ToolRegistry.Invoke keeps.
func (p *mcpToolProvider) Invoke(ctx context.Context, name string, args any) (ToolOutcome, error) {
	remote, ok := p.byName[name]
	if !ok {
		return ToolOutcome{}, fmt.Errorf("tool not registered: %s", name)
	}
	p.logger.DebugContext(ctx, "MCP tool invoked", "tool", name)
	result, err := p.session.CallTool(ctx, &mcp.CallToolParams{Name: remote, Arguments: args})
	if err != nil {
		p.logger.WarnContext(ctx, "MCP tool call failed", "tool", name, "error", err)
		return ToolOutcome{}, fmt.Errorf("call %s: %w", name, err)
	}
	if result == nil {
		return ToolOutcome{}, fmt.Errorf("tool %s returned no result", name)
	}
	return ToolOutcome{Result: resultValue(result), IsError: result.IsError}, nil
}
