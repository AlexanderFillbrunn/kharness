Webhooks
========

`WithWebhooks` turns the agent's [hook points](../agent/pkg/agent/hooks.go)
into HTTP webhook calls: every hook firing is POSTed as JSON to a configured
URL — one URL for all events, or one URL per event — with every delivery knob
overridable per event. The adapter is a `Hooks` value under the hood, so it
composes with an in-process `WithHooks` rather than replacing it.

Wiring
------

    wh, err := agent.NewWebhooks(agent.WebhookConfig{
        DeliveryConfig: agent.DeliveryConfig{
            URL:    "https://hooks.example.com/agent",
            Secret: os.Getenv("WEBHOOK_SECRET"),
        },
    })
    if err != nil { ... }

    a, err := agent.NewAgent(cfg, tools, llm, agent.WithHooks(myHooks), agent.WithWebhooks(wh))
    // when the agent is done:
    wh.Close(ctx) // drains the queue, stops the workers

`NewWebhooks` only validates the config and allocates the queue; the worker
goroutines start lazily on the first async delivery, so a webhook setup that
only delivers synchronously — or an agent that never runs — spawns none.
`Close` is idempotent, safe on a dispatcher that never delivered, and bounded
by its context: on the deadline, in-flight requests are canceled.

Apply `WithWebhooks` after `WithHooks`. Options run in order and a later
`WithHooks` replaces the whole hook set, webhooks included; where both are
set, the local hook decides first and its decision stands — the webhook is
still notified, asynchronously, but cannot override it. Where no local hook
is set, the webhook is authoritative (see delivery modes).

Events
------

One event name per hook point, used as the `event` field of the envelope, the
`X-Webhook-Event` header, and the key of `WebhookConfig.Events`:

    run.before / run.after
    assistant.before / assistant.after
    tool_call.before / tool_call.after
    task.start.before / task.cancel.before / task.input.before
    task.result.after / task.event.after
    skill.load.before / skill.load.after
    interrupt.before / interrupt.after
    interrupt_response.before / interrupt_response.after
    arc.closed.after

The `data` of the envelope is the hook's view (`RunView`, `ToolCallView`,
`RunSummary`, ...) as JSON. Unknown keys in `Events` are a construction error,
so a typo'd event name cannot silently drop traffic.

URLs
----

`DeliveryConfig.URL` receives every event that `Events` does not override.
An entry in `Events` with its own `URL` replaces it for that event; an entry
with `Disabled: true` suppresses the event even where the global URL would
deliver it — use that for `assistant.before` if the transcript must not
leave the process. At least one URL, global or per event, is required.
URLs must be https unless `WebhookConfig.AllowInsecure` is set (for tests).

Delivery modes
--------------

`DeliveryConfig.Mode`, globally or per event:

- `DeliveryAsync` (default for the After* hooks): the delivery is queued and
  sent by the workers. It never blocks a run and never fails it. For a
  Before* hook this means notification-only: the decision passes through.
- `DeliverySync` (default for the Before* hooks): the POST runs inline with
  the run's context. For a decision hook the reply body is decoded into the
  hook's result type (`ToolCallEffects`, `RunDecision`, ...) — a 2xx with an
  empty or `null` body means "allow". For a void info hook the reply is read
  and discarded.
- `DeliveryAuto` (zero): inherit the global `Mode`, else the per-kind default
  above.

A failing sync decision webhook follows `FailPolicy`: `FailClosed` (the
default) fails the step exactly as an erroring hook does; `FailOpen` logs and
lets the step proceed. Void info hooks never fail the run, whatever the
policy.

The wire format
---------------

Every POST is one envelope:

    {
      "id": "…",            // idempotency key: dedupe retries on it
      "event": "tool_call.before",
      "timestamp": "…",     // when this attempt was sent
      "seq": 42,            // per-dispatcher sequence, for ordering
      "attempt": 1,         // 1-based attempt of this delivery
      "data": { ... }       // the hook's view
    }

Headers: `Content-Type: application/json`, `User-Agent: kharness-webhooks/1.0`,
`X-Webhook-ID`, `X-Webhook-Event`, `X-Webhook-Attempt`, any configured
`Headers`, and — where `Secret` is set — `X-Webhook-Timestamp` and
`X-Webhook-Signature: t=<unix>,v1=<hex>`, the HMAC-SHA256 of `"<t>.<body>"`
keyed with the secret. Verify by recomputing the MAC over the received
timestamp header and raw body, and reject stale timestamps.

Delivery behavior
-----------------

- One attempt is bounded by `Timeout` (default 10s), connection included.
- Failed attempts retry up to `MaxRetries` times (default 4, negative = no
  retries) with exponential backoff and full jitter between `BackoffMin`
  (500ms) and `BackoffMax` (30s). `Retry-After` on a 429/503 overrides the
  backoff, capped at `BackoffMax`.
- Retried: transport errors, 408, 429, 5xx. Permanent: every other 4xx.
- Any 2xx is success. The reply body is read (bounded, 1 MiB) and the
  connection reused.
- A circuit breaker per URL opens after 5 consecutive failed attempts and
  fails fast for 30s, then lets one probe through; the probe's result closes
  or reopens it. Events sharing a URL share a breaker.
- The async queue is bounded (`QueueSize`, default 1024, drained by
  `Workers` workers, default 4). A full queue drops the newest delivery and
  logs it — a run is never stalled by a webhook. Drops are counted in
  `Webhooks.Dropped()`.
- Delivery is at-least-once: retries repeat the same `id`, so receivers must
  dedupe. Nothing is persisted if the process dies; the hooks themselves are
  documented as re-fireable, so at-least-once is the contract.

Payload shaping
---------------

`Redact func(event, data) any` runs before marshaling — per event or globally —
to strip what a receiver must not see. `MaxPayloadBytes` (default 1 MiB,
negative = no cap) replaces an oversized payload with
`{"truncated": true, "original_bytes": n}` rather than sending broken JSON.

Configuration reference
-----------------------

`DeliveryConfig` (the embedded global default, and the `Events` map value;
zero fields inherit, then take defaults):

| Field | Default | Meaning |
|---|---|---|
| `URL` | — | delivery target; empty inherits |
| `Mode` | auto | `DeliverySync` / `DeliveryAsync` |
| `Secret` | — | HMAC signing secret; empty = unsigned |
| `Headers` | — | extra request headers; non-nil replaces the parent map |
| `Timeout` | 10s | per attempt |
| `MaxRetries` | 4 | retries per delivery; negative = none |
| `BackoffMin` / `BackoffMax` | 500ms / 30s | backoff bounds, full jitter |
| `FailPolicy` | `FailClosed` | for failing sync decision webhooks |
| `MaxPayloadBytes` | 1 MiB | payload cap; negative = no cap |
| `Redact` | identity | payload shaper |
| `Disabled` | false | suppress the event (per-event only) |

`WebhookConfig` adds the process-level knobs, which cannot appear inside an
`Events` entry: `Events`, `AllowInsecure`, `HTTPClient`, `Logger`, `Workers`,
`QueueSize`.
