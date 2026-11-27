Logging
==========

Status: implemented. Every module except devchat logs through `log/slog`;
devchat is a binary and installs a file handler as the process default.

Purpose
--------------------
The event log is the source of truth for what happened, so the logs are not
another record of it. A log line says when something happened, where in the
loop, and how it ended — the log carries the content. Concretely: **logs
never carry message text, tool arguments or results, notification payloads,
or the system prompt.** They carry metadata only: event types, IDs and
sequence numbers, token counts, attempt counts, outcomes. A line that needs
to say what a run said names the event ID to read.

Library stance
--------------------
The four library modules (`proto`, `agent`, `ingress`, `redisinfra`) never
call `slog.SetDefault` and never install a handler. Each component takes a
`*slog.Logger` through a functional option; nil means `slog.Default()` (the
agent excepted — see below), so a consumer that wants silence passes a logger
with a discarding handler. Level
control belongs to the consumer's handler, not to the library: the library
decides what is emitted at which level, the handler decides what is kept.
Every component tags its lines with a `component` attribute, and per run or
per item with `arc`, `session`, `task`, `entry` attributes, so one handler
serves the whole process and can be filtered by attribute.

`proto` logs nothing: it is the wire contract, and everything in it reports
through return values. The callers log.

Configuration
--------------------

    agent.NewAgent(cfg, tools, llm, agent.WithLogging(logger))
    agent.NewArcWorker(..., agent.WithLogger(logger))
    agent.NewCompactionWorker(..., agent.WithCompactionLogger(logger))
    agent.NewWebhooks(agent.WebhookConfig{...})   // attached with agent.WithWebhooks(w)
    ingress.NewTaskReports(persistor, source, tasks, enqueue, ingress.WithReportsLogger(logger))
    ingress.NewCoalescer(ingress.WithCoalesceLogger(logger))
    redisinfra.NewRedisWorkQueue(client, redisinfra.WithQueueLogger(logger))
    redisinfra.NewRedisLease(client, redisinfra.WithLeaseLogger(logger))
    redisinfra.NewRedisSessionLease(client, redisinfra.WithLeaseLogger(logger))
    redisinfra.NewRedisEventStore(client, redisinfra.WithStoreLogger(logger))
    redisinfra.NewRedisCancels(client, redisinfra.WithCancelsLogger(logger))

An agent's logger defaults to nil, and `Run` fills it in: the logger on the
run's context if there is one, else a discarding handler. `ArcWorker.Work`
carries the worker's logger on that context, so an agent resolved by a worker
without `WithLogging` inherits the worker's logger (tagged with the arc),
while an agent built with `WithLogging` always logs to its own. A deployment
that only configures the worker gets agent, hook and tool lines in the
worker's stream; a direct `Run` with no logger anywhere logs nothing. The
other components (workers, ingress, redisinfra) keep the simpler rule: nil
means `slog.Default()`.

Webhooks are plain hooks: a `*Webhooks` attaches with `WithHooks(w.Compose(myHooks))`
(or the `WithWebhooks` sugar for that), and the agent holds no reference to
it. Every hook already receives the run's context, and the run's logger rides
on it — `agent.LoggerFrom(ctx)` — so a dispatcher built without its own
`WebhookConfig.Logger` logs to the run's logger, tagged `component=webhooks`.
An explicit `WebhookConfig.Logger` always wins over the run's logger.

Levels
--------------------

Debug — per-event and per-round detail: persisted batches with their type
list and seq range, each round's arc state, model calls with token usage and
stop reason, tool invocations by name, lease acquisitions and renewals,
queue reads and acknowledgements, snapshot saves, webhook deliveries.

Info — lifecycle: a run starting, admitted, not admitted (with the outcome),
and ended (outcome, rounds, events written); an arc closed or suspended; a
compaction requested or computed; an interrupt raised; a work item enqueued,
worked or picked up; a cancel announced; a report persisted and its arc
enqueued.

Warn — recoverable degradation: a tool call settled as an error, an
abandoned dispatch, a re-offered or demoted item, a dropped or retried
webhook, a lost lease heartbeat (the unfenced-writer hole, named), a failed
requeue or acknowledgement (the entry stays pending), a refused report, a
snapshot save that failed, a cancel announcement that could not be published
(the guard still stops the run).

Error — failures that end a run or lose a write: a model call that failed, a
persist or fold failure, a fatal run outcome, an unreadable queue entry and
its move to the dead stream, an item that exhausted its attempts, a lease or
queue Redis error, a report that could not be persisted (the report is
lost).

devchat
--------------------
`cmd/devchat` opens `-log` (default `cmd/devchat/.devchat/devchat.log`, run
from the `devchat` directory) and installs it as the process default at
Debug, then hands it to the resolver's `WithLogging`, the workers' logger
options and the ingress options, so everything lands in one file while the
terminal stays readable. The directory is a working-file directory and is
already covered by `.gitignore`.
