package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// echoMCPServer returns an in-memory MCP server with one echo tool.
func echoMCPServer(t *testing.T) *mcp.Server {
	t.Helper()
	server := mcp.NewServer(&mcp.Implementation{Name: "test-server", Version: "0"}, nil)
	server.AddTool(&mcp.Tool{
		Name:        "echo",
		Description: "echoes its text argument",
		InputSchema: map[string]any{"type": "object", "properties": map[string]any{"text": map[string]any{"type": "string"}}},
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
	}, func(_ context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		var args map[string]any
		if err := json.Unmarshal(req.Params.Arguments, &args); err != nil {
			return nil, err
		}
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: fmt.Sprint(args["text"])}},
		}, nil
	})
	return server
}

// WithMCP folds the server's tools into the agent's registry, so the model is
// offered them and a call by name reaches the server.
func TestWithMCPBridgesRemoteTools(t *testing.T) {
	server := echoMCPServer(t)
	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	go func() { _ = server.Run(context.Background(), serverTransport) }()

	agent, err := NewAgent(nil, NewToolRegistry(), &scriptedLLM{}, WithMCP(MCPServerConfig{
		Name:      "test",
		Transport: clientTransport,
	}))
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}

	tool, ok := agent.tools.Get("echo")
	if !ok {
		t.Fatal("echo is not offered")
	}
	if !ReadOnly(tool) {
		t.Error("the remote tool's read-only annotation did not survive the bridge")
	}
	outcome, err := agent.tools.Invoke(context.Background(), "echo", map[string]any{"text": "hello"})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if outcome.IsError || outcome.Result != "hello" {
		t.Errorf("outcome = %+v, want the echoed text", outcome)
	}
	if _, ok := agent.tools.Get("nope"); ok {
		t.Error("an unknown name was offered")
	}
}

// A prefix namespaces the remote tools, and calls go out under the remote's
// own name.
func TestWithMCPPrefix(t *testing.T) {
	server := echoMCPServer(t)
	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	go func() { _ = server.Run(context.Background(), serverTransport) }()

	agent, err := NewAgent(nil, NewToolRegistry(), &scriptedLLM{}, WithMCP(MCPServerConfig{
		Name: "test", Transport: clientTransport, Prefix: "fs_",
	}))
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	if _, ok := agent.tools.Get("echo"); ok {
		t.Error("the unprefixed remote name is still offered")
	}
	if _, ok := agent.tools.Get("fs_echo"); !ok {
		t.Fatal("the prefixed name is not offered")
	}
	outcome, err := agent.tools.Invoke(context.Background(), "fs_echo", map[string]any{"text": "hi"})
	if err != nil || outcome.Result != "hi" {
		t.Errorf("Invoke via the prefixed name = %+v, %v", outcome, err)
	}
}

// A remote tool whose name a local tool already took fails the build, via the
// same collision check as two local tools.
func TestWithMCPNameCollisionFailsBuild(t *testing.T) {
	server := echoMCPServer(t)
	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	go func() { _ = server.Run(context.Background(), serverTransport) }()

	local := NewToolRegistry()
	if err := local.RegisterTool(&mcp.Tool{Name: "echo"}, nil); err != nil {
		t.Fatalf("RegisterTool: %v", err)
	}
	_, err := NewAgent(nil, local, &scriptedLLM{}, WithMCP(MCPServerConfig{
		Name: "test", Transport: clientTransport,
	}))
	if err == nil || !strings.Contains(err.Error(), "echo") {
		t.Errorf("NewAgent error = %v, want the collision named", err)
	}
}

// AuthToken and Headers ride on every request, the handshake included, when
// no HTTPClient is given.
func TestWithMCPAuthHeaders(t *testing.T) {
	server := echoMCPServer(t)
	handler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server }, nil)

	var gotAuth, gotTenant string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotTenant = r.Header.Get("X-Tenant")
		handler.ServeHTTP(w, r)
	}))
	defer srv.Close()

	agent, err := NewAgent(nil, NewToolRegistry(), &scriptedLLM{}, WithMCP(MCPServerConfig{
		Name: "auth", Endpoint: srv.URL,
		AuthToken: "secret-token", Headers: map[string]string{"X-Tenant": "acme"},
	}))
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	if gotAuth != "Bearer secret-token" || gotTenant != "acme" {
		t.Errorf("handshake saw auth=%q tenant=%q, want the configured values", gotAuth, gotTenant)
	}
	if _, ok := agent.tools.Get("echo"); !ok {
		t.Error("the tool list did not arrive over the authenticated endpoint")
	}
}

// An unreachable server fails the build rather than a run.
func TestWithMCPUnreachableFailsBuild(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	srv.Close()
	_, err := NewAgent(nil, NewToolRegistry(), &scriptedLLM{}, WithMCP(MCPServerConfig{
		Name: "down", Endpoint: srv.URL, ConnectTimeout: 0,
	}))
	if err == nil {
		t.Error("NewAgent succeeded against a closed server")
	}
}
