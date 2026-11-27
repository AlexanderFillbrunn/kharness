External providers: MCP tools and A2A tasks
=============================================

Status: implemented. `WithMCP` bridges one MCP server into an agent's tool
surface; `WithA2A` bridges one remote A2A agent's skills into its task
surface. Both are `AgentOption`s on `NewAgent`, both connect at build so an
unreachable server or a name collision fails the build rather than a run,
and both take auth and transport options. See `agent/pkg/agent/mcp.go` and
`agent/pkg/agent/a2a.go`.

    agent.NewAgent(cfg, tools, llm,
        agent.WithMCP(agent.MCPServerConfig{
            Name: "fs", Endpoint: "https://mcp.internal/fs/mcp",
            AuthToken: os.Getenv("FS_MCP_TOKEN"),          // bearer
            Headers: map[string]string{"X-Tenant": "acme"}, // extra headers
            // HTTPClient: client,                          // or your own
            // Prefix: "fs_",                               // namespace remote names
            // Transport: stdioTransport,                   // non-HTTP / tests
            // ConnectTimeout: 10 * time.Second, Logger: l,
        }),
        agent.WithA2A(agent.A2AConfig{
            Name: "researcher", BaseURL: "https://a2a.internal/researcher",
            AuthToken: os.Getenv("A2A_TOKEN"),
        }),
    )

MCP → tools
--------------------
`WithMCP` lists the server's tools once at connect and folds a provider over
the session into the agent's tools. Calls go out under the server's own name;
`Prefix` renames what the model sees. Remote `ReadOnlyHint` annotations
survive, so remote read-only tools join parallel waves like local ones. A
remote name colliding with a local tool, a task kind or an interrupt fails
`NewAgent` (the duplicate check spans all combined providers). The session
lives as long as the agent; the standalone SSE stream is disabled because the
tool list is fixed at build.

A2A → tasks
--------------------
`WithA2A` resolves the agent card (`{BaseURL}/.well-known/agent-card.json`,
then `/.well-known/agent.json`) with the official
[a2a-go](https://github.com/a2aproject/a2a-go) SDK and maps each skill to a
task kind: the skill's ID (or name) as the kind name, its description. The
kinds are offered to the model exactly like local task kinds — one tool per
kind plus the cancel/answer tools.

The controller speaks A2A through the SDK's client (JSON-RPC transport,
`message/send`, `tasks/cancel`). Each skill's tool takes an envelope
`{contextId?, args}`: the controller sends the inner `args` as a message part
(a data part when it is a JSON object) with **our task ID as the A2A
`taskId`**, and `contextId` only when the model passed one — omit it to start
a new conversation, pass the context id a previous task returned to continue
that conversation with a new task. The server's launch response (its task
object, carrying the context id whether or not we sent one) is recorded on
the `ASYNC_TASK_LAUNCH_RESULT` event and rendered as the start call's result,
so the context id reaches the model. If the server answers with a task of its
own ID, that ID becomes the launch's `ExternalRef`, recorded in the
`ASYNC_TASK_LAUNCH_RESULT` event (falling back to our task ID when the server
assigned none) — that field is what routes the server's callbacks to the
task: index it in the log store
(e.g. a jsonb path index over `ASYNC_TASK_LAUNCH_RESULT` in Postgres, or a
secondary key maintained at persist time in Redis), and the callback
component resolves `external_ref` → `TaskRef{SessionID, ID}` before calling
`ingress.TaskReports.Report`, which persists the event and enqueues the work
item. Cancel and input address the remote task by the vendor ID from the
launch, remembered in the process-bound controller.

If the run has no controller of its own, the agent's combined task set acts
as one when it is a `TaskSource` (which `WithA2A`'s combination is), so a
process that only builds an agent can still start A2A tasks; the worker path
(`WithTaskController`) is unchanged.

Logging
--------------------
Both bridges follow docs/LOGGING.md: `component=mcp` / `component=a2a` tags,
IDs, names, methods and outcomes only — never tool arguments, task args, input
text, or the runner's raw response (that lives in the launch-result event).
