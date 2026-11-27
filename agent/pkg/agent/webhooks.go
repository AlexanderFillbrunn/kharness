package agent

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"fillbrunn.ai/kube-agent/proto/pkg/common"
)

// WithWebhooks turns an agent's hook points into webhook calls. Every hook
// firing is delivered as a JSON POST to a configured URL — one URL for all
// events, or one URL per event — with per-event overrides of every delivery
// knob. See docs/WEBHOOKS.md for the wire format.
//
// Two delivery modes. Async deliveries are queued and sent by background
// workers, never block a run and never fail it: this is the only mode for
// the info-only After* hooks by default, and the only thing an async Before*
// hook can do is notify — the decision passes through. Sync deliveries are
// POSTed inline with the run's context and their reply body is decoded into
// the hook's result type, so a webhook can decide (block, ask, answer,
// refuse) — the default for the Before* hooks. A sync failure follows the
// configured FailPolicy: FailClosed fails the step, as a hook error does;
// FailOpen lets the step proceed.
//
// Every delivery carries an id (the idempotency key), a per-dispatcher
// sequence number and the attempt number, and is signed with HMAC-SHA256
// where a secret is configured. Retries follow 5xx, 429, 408 and transport
// errors with exponential backoff and jitter, honoring Retry-After; other
// 4xx are permanent. A circuit breaker per URL stops the calls to an
// endpoint that keeps failing. The queue is bounded and drops the newest
// delivery, logged, when full — a run is never stalled by a webhook.

// DeliveryMode says how one event is delivered.
type DeliveryMode int

const (
	// DeliveryAuto inherits: the global Mode where set, else sync for the
	// Before* hooks and async for the After* hooks.
	DeliveryAuto DeliveryMode = iota
	// DeliverySync POSTs inline with the run's context and waits for the
	// reply. For Before* hooks the reply body is decoded into the hook's
	// result; for After* hooks the reply is read and discarded.
	DeliverySync
	// DeliveryAsync queues the delivery for the background workers and
	// returns immediately. The reply is never awaited: for Before* hooks
	// the decision passes through, for After* hooks there is none to make.
	DeliveryAsync
)

// FailPolicy says what a failing sync Before* webhook does to the step.
type FailPolicy int

const (
	// FailAuto inherits: the global FailPolicy where set, else FailClosed.
	FailAuto FailPolicy = iota
	// FailClosed fails the step, exactly as an erroring hook does.
	FailClosed
	// FailOpen logs the failure and lets the step proceed.
	FailOpen
)

// WebhookEvent names one hook point on the wire, and keys WebhookConfig.Events.
type WebhookEvent string

const (
	EventRunBefore               WebhookEvent = "run.before"
	EventRunAfter                WebhookEvent = "run.after"
	EventAssistantBefore         WebhookEvent = "assistant.before"
	EventAssistantAfter          WebhookEvent = "assistant.after"
	EventToolCallBefore          WebhookEvent = "tool_call.before"
	EventToolCallAfter           WebhookEvent = "tool_call.after"
	EventTaskStartBefore         WebhookEvent = "task.start.before"
	EventTaskCancelBefore        WebhookEvent = "task.cancel.before"
	EventTaskInputBefore         WebhookEvent = "task.input.before"
	EventTaskResultAfter         WebhookEvent = "task.result.after"
	EventTaskEventAfter          WebhookEvent = "task.event.after"
	EventSkillLoadBefore         WebhookEvent = "skill.load.before"
	EventSkillLoadAfter          WebhookEvent = "skill.load.after"
	EventInterruptBefore         WebhookEvent = "interrupt.before"
	EventInterruptAfter          WebhookEvent = "interrupt.after"
	EventInterruptResponseBefore WebhookEvent = "interrupt_response.before"
	EventInterruptResponseAfter  WebhookEvent = "interrupt_response.after"
	EventArcClosedAfter          WebhookEvent = "arc.closed.after"
)

// allWebhookEvents is every event name, for validating Events keys.
var allWebhookEvents = []WebhookEvent{
	EventRunBefore, EventRunAfter,
	EventAssistantBefore, EventAssistantAfter,
	EventToolCallBefore, EventToolCallAfter,
	EventTaskStartBefore, EventTaskCancelBefore, EventTaskInputBefore,
	EventTaskResultAfter, EventTaskEventAfter,
	EventSkillLoadBefore, EventSkillLoadAfter,
	EventInterruptBefore, EventInterruptAfter,
	EventInterruptResponseBefore, EventInterruptResponseAfter,
	EventArcClosedAfter,
}

// Defaults applied where a DeliveryConfig field is zero. Negative disables:
// MaxRetries below zero means a single attempt, MaxPayloadBytes below zero
// means no cap.
const (
	DefaultWebhookTimeout    = 10 * time.Second
	DefaultWebhookMaxRetries = 4
	DefaultWebhookBackoffMin = 500 * time.Millisecond
	DefaultWebhookBackoffMax = 30 * time.Second
	DefaultWebhookWorkers    = 4
	DefaultWebhookQueueSize  = 1024
	DefaultMaxPayloadBytes   = 1 << 20
	webhookUserAgent         = "kharness-webhooks/1.0"
	maxResponseBodyBytes     = 1 << 20
	breakerFailureThreshold  = 5
	breakerCooldown          = 30 * time.Second
)

// DeliveryConfig is the settings of one delivery: the global defaults when
// embedded in WebhookConfig, or a per-event override as a WebhookConfig.Events
// value. A zero field inherits from the parent, then falls back to a default.
type DeliveryConfig struct {
	// URL is where the event is POSTed. Empty inherits the parent URL; an
	// event with no URL anywhere is not delivered.
	URL string

	// Mode is how the event is delivered. DeliveryAuto inherits.
	Mode DeliveryMode

	// Secret signs the request body: X-Webhook-Signature carries
	// "t=<unix>,v1=<hex hmac-sha256 of "t.body">" and X-Webhook-Timestamp the
	// t. Empty inherits; a receiver should treat an unsigned request as
	// untrusted where a secret is configured.
	Secret string

	// Headers are added to every request of this event. Non-nil replaces the
	// parent map wholesale, so per-event headers do not inherit key by key.
	Headers map[string]string

	// Timeout bounds one attempt, connection included.
	Timeout time.Duration

	// MaxRetries is how many times a failed attempt is retried. Zero means
	// DefaultWebhookMaxRetries, negative means no retries.
	MaxRetries int

	// BackoffMin and BackoffMax bound the exponential backoff between
	// attempts; the actual sleep is uniform in [0, min(max, min*2^(n-1))].
	BackoffMin time.Duration
	BackoffMax time.Duration

	// FailPolicy governs a failing sync Before* delivery. FailAuto inherits;
	// the top-level default is FailClosed.
	FailPolicy FailPolicy

	// MaxPayloadBytes caps the marshaled data of one envelope; a larger
	// payload is replaced by a truncation marker. Zero means
	// DefaultMaxPayloadBytes, negative means no cap.
	MaxPayloadBytes int

	// Redact shapes the payload before it is marshaled — to strip fields a
	// receiver must not see, like the transcript in assistant.before. Nil
	// inherits, then delivers the view as marshaled.
	Redact func(event WebhookEvent, data any) any

	// Disabled suppresses the event even where a URL would deliver it. Only
	// valid inside Events; an error at the top level.
	Disabled bool
}

// WebhookConfig is what NewWebhooks takes: the global DeliveryConfig, the
// per-event overrides, and the process-level knobs, which by construction
// cannot appear inside an Events entry.
type WebhookConfig struct {
	DeliveryConfig

	// Events overrides the global delivery settings per event name. Keys are
	// validated against the known event names, so a typo is a construction
	// error rather than silently undelivered traffic.
	Events map[WebhookEvent]DeliveryConfig

	// AllowInsecure permits http:// URLs, for tests. The default requires
	// https everywhere, including per-event URLs.
	AllowInsecure bool

	// HTTPClient sends the requests. Nil builds a client with the default
	// transport; per-attempt timeouts come from the context, so a client
	// Timeout only tightens them.
	HTTPClient *http.Client

	// Logger receives drops and permanent failures. Nil means the run's
	// logger where the delivery was queued from a hook (LoggerFrom), else
	// slog.Default().
	Logger *slog.Logger

	// Workers is how many background workers drain the queue. They start
	// lazily, on the first async delivery — a webhook setup that only ever
	// delivers synchronously spawns none. Zero means DefaultWebhookWorkers.
	Workers int

	// QueueSize bounds the async queue. A full queue drops the newest
	// delivery, logged. Zero means DefaultWebhookQueueSize.
	QueueSize int
}

// resolvedDelivery is one event's DeliveryConfig with every override applied
// and every default filled in.
type resolvedDelivery struct {
	event      WebhookEvent
	url        string
	mode       DeliveryMode
	secret     string
	headers    map[string]string
	timeout    time.Duration
	maxRetries int
	backoffMin time.Duration
	backoffMax time.Duration
	failPolicy FailPolicy
	maxPayload int
	redact     func(WebhookEvent, any) any
}

// Webhooks is a webhook dispatcher: the queue, the workers, the circuit
// breakers and the HTTP client the hook adapters share. NewWebhooks builds
// it, WithWebhooks attaches it to an agent, Close drains and stops it.
type Webhooks struct {
	cfg      WebhookConfig
	client   *http.Client
	logger   *slog.Logger
	workers  int
	queue    chan delivery
	quit     chan struct{}
	baseCtx  context.Context
	cancel   context.CancelFunc
	breakers map[string]*circuitBreaker

	// breaker tuning, fields rather than constants so tests can shrink them.
	breakerThreshold int
	breakerCooldown  time.Duration

	mu      sync.Mutex
	started bool
	closed  bool
	wg      sync.WaitGroup

	seq     atomic.Int64
	dropped atomic.Int64
}

// NewWebhooks validates the config and builds the dispatcher. It starts no
// workers; they begin on the first async delivery and stop at Close.
func NewWebhooks(cfg WebhookConfig) (*Webhooks, error) {
	if cfg.Disabled {
		return nil, errors.New("webhooks: Disabled is only valid per event, not at the top level")
	}
	if cfg.URL != "" {
		if err := validateWebhookURL(cfg.URL, cfg.AllowInsecure); err != nil {
			return nil, fmt.Errorf("webhooks: URL: %w", err)
		}
	}
	target := cfg.URL != ""
	for ev, over := range cfg.Events {
		if !knownWebhookEvent(ev) {
			return nil, fmt.Errorf("webhooks: unknown event %q", ev)
		}
		if over.URL != "" {
			if err := validateWebhookURL(over.URL, cfg.AllowInsecure); err != nil {
				return nil, fmt.Errorf("webhooks: event %s: %w", ev, err)
			}
			target = true
		}
	}
	if !target {
		return nil, errors.New("webhooks: no URL configured, neither global nor per event")
	}
	if cfg.BackoffMin < 0 || cfg.BackoffMax < 0 {
		return nil, errors.New("webhooks: backoff bounds must not be negative")
	}
	workers := cfg.Workers
	if workers <= 0 {
		workers = DefaultWebhookWorkers
	}
	queueSize := cfg.QueueSize
	if queueSize <= 0 {
		queueSize = DefaultWebhookQueueSize
	}
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{}
	}
	w := &Webhooks{
		cfg:              cfg,
		client:           client,
		logger:           cfg.Logger,
		workers:          workers,
		queue:            make(chan delivery, queueSize),
		quit:             make(chan struct{}),
		breakers:         map[string]*circuitBreaker{},
		breakerThreshold: breakerFailureThreshold,
		breakerCooldown:  breakerCooldown,
	}
	w.baseCtx, w.cancel = context.WithCancel(context.Background())
	return w, nil
}

// Close stops accepting deliveries, drains the queue and waits for the
// workers and their in-flight requests, bounded by ctx: on its deadline the
// in-flight requests are canceled and Close returns the ctx error while the
// workers finish in the background. It is idempotent and safe to call on a
// dispatcher that never delivered anything.
func (w *Webhooks) Close(ctx context.Context) error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	w.closed = true
	close(w.quit)
	w.mu.Unlock()

	done := make(chan struct{})
	go func() { w.wg.Wait(); close(done) }()
	select {
	case <-done:
		w.cancel()
		return nil
	case <-ctx.Done():
		w.cancel()
		return ctx.Err()
	}
}

// Dropped is how many async deliveries were dropped — queue full or
// dispatcher closed — rather than sent.
func (w *Webhooks) Dropped() int64 { return w.dropped.Load() }

// WithWebhooks attaches the dispatcher to an agent by composing it into the
// agent's hooks — nothing else: the agent holds no reference to the
// dispatcher, and the dispatcher logs to its own WebhookConfig.Logger, or to
// the run's logger (see LoggerFrom) where it has none. Every hook point is
// wrapped so its firing is also delivered to the configured webhooks. The
// agent's hooks set by an earlier WithHooks are composed in — a local hook
// always decides first, and where one is set the webhook is notified
// asynchronously and cannot override it; where none is set, a sync webhook
// decides and an async one lets the step pass. Apply WithWebhooks after
// WithHooks: options run in order, and a later WithHooks replaces the whole
// set, webhooks included. To combine webhooks with hooks in one WithHooks,
// use Compose directly.
func WithWebhooks(w *Webhooks) AgentOption {
	return func(a *Agent) error {
		if w == nil {
			return errors.New("webhooks need a non-nil *Webhooks")
		}
		a.hooks = w.Compose(a.hooks)
		return nil
	}
}

// Compose wraps every hook point of local with its webhook delivery, for a
// consumer that attaches the dispatcher through WithHooks itself:
// WithHooks(w.Compose(myHooks)).
func (w *Webhooks) Compose(local Hooks) Hooks {
	var out Hooks
	run := ToolCallEffects{HookEffects: HookEffects{Run: true}}
	out.BeforeRun = decideHook(w, EventRunBefore, local.BeforeRun, RunDecision{Pass: true}, DeliverySync)
	out.AfterRun = infoHook(w, EventRunAfter, local.AfterRun)
	out.BeforeAssistant = decideHook(w, EventAssistantBefore, local.BeforeAssistant, AssistantRequestChanges{}, DeliverySync)
	out.AfterAssistant = decideHook(w, EventAssistantAfter, local.AfterAssistant, AssistantReplyChanges{}, DeliveryAsync)
	out.BeforeToolCall = decideHook(w, EventToolCallBefore, local.BeforeToolCall, run, DeliverySync)
	out.AfterToolCall = decideHook(w, EventToolCallAfter, local.AfterToolCall, ToolCallChanges{}, DeliveryAsync)
	out.BeforeTaskStart = decideHook(w, EventTaskStartBefore, local.BeforeTaskStart, TaskStartEffects{Run: true}, DeliverySync)
	out.BeforeTaskCancel = decideHook(w, EventTaskCancelBefore, local.BeforeTaskCancel, TaskCancelEffects{Run: true}, DeliverySync)
	out.BeforeTaskInput = decideHook(w, EventTaskInputBefore, local.BeforeTaskInput, TaskInputEffects{Run: true}, DeliverySync)
	out.AfterTaskResult = infoHook(w, EventTaskResultAfter, local.AfterTaskResult)
	out.AfterTaskEvent = infoHook(w, EventTaskEventAfter, local.AfterTaskEvent)
	out.BeforeSkillLoad = decideHook(w, EventSkillLoadBefore, local.BeforeSkillLoad, run, DeliverySync)
	out.AfterSkillLoad = decideHook(w, EventSkillLoadAfter, local.AfterSkillLoad, ToolCallChanges{}, DeliveryAsync)
	out.BeforeInterrupt = decideHook(w, EventInterruptBefore, local.BeforeInterrupt, InterruptChanges{}, DeliverySync)
	out.AfterInterrupt = infoHook(w, EventInterruptAfter, local.AfterInterrupt)
	out.BeforeInterruptResponse = decideHook(w, EventInterruptResponseBefore, local.BeforeInterruptResponse, InterruptResponseChanges{}, DeliverySync)
	out.AfterInterruptResponse = infoHook(w, EventInterruptResponseAfter, local.AfterInterruptResponse)
	out.AfterArcClosed = infoHook(w, EventArcClosedAfter, local.AfterArcClosed)
	return out
}

// decideHook wraps a hook that returns a decision or changes. allow is the
// result the hook yields where no decision comes back — async mode, an empty
// reply body, or FailOpen. auto is the mode DeliveryAuto resolves to.
//
// A local hook, where set, always decides first and its result stands; the
// webhook is still notified, asynchronously. With no local hook the webhook
// is authoritative: sync mode decodes the reply into Res, async mode passes
// through with allow.
func decideHook[Req, Res any](w *Webhooks, event WebhookEvent, local func(context.Context, Req) (Res, error), allow Res, auto DeliveryMode) func(context.Context, Req) (Res, error) {
	rd := w.resolve(event, auto)
	if rd.url == "" {
		return local
	}
	if local == nil {
		return func(ctx context.Context, req Req) (Res, error) {
			if rd.mode == DeliveryAsync {
				w.notify(ctx, event, rd, req)
				return allow, nil
			}
			body, err := w.callSync(ctx, rd, event, req)
			if err == nil {
				out, derr := decodeReply(event, body, allow)
				if derr == nil {
					return out, nil
				}
				err = derr
			}
			if rd.failPolicy == FailOpen {
				w.logf(ctx, "the %s webhook failed, failing open: %v", event, err)
				return allow, nil
			}
			return allow, fmt.Errorf("the %s webhook failed: %w", event, err)
		}
	}
	return func(ctx context.Context, req Req) (Res, error) {
		res, err := local(ctx, req)
		w.notify(ctx, event, rd, req)
		return res, err
	}
}

// infoHook wraps a hook that returns nothing. The webhook is notified of every
// firing — async by default, or inline in sync mode — and a sync failure is
// only logged: an info hook can never fail a run.
func infoHook[Req any](w *Webhooks, event WebhookEvent, local func(context.Context, Req)) func(context.Context, Req) {
	rd := w.resolve(event, DeliveryAsync)
	if rd.url == "" {
		return local
	}
	return func(ctx context.Context, req Req) {
		if local != nil {
			local(ctx, req)
		}
		if rd.mode == DeliveryAsync {
			w.notify(ctx, event, rd, req)
			return
		}
		if _, err := w.callSync(ctx, rd, event, req); err != nil {
			w.logf(ctx, "the %s webhook failed: %v", event, err)
		}
	}
}

// decodeReply unmarshals a sync webhook's reply body into the hook's result
// type. An empty or null body yields allow.
func decodeReply[Res any](event WebhookEvent, body []byte, allow Res) (Res, error) {
	if len(body) == 0 || string(body) == "null" {
		return allow, nil
	}
	var out Res
	if err := json.Unmarshal(body, &out); err != nil {
		return allow, fmt.Errorf("the %s webhook returned an undecodable reply: %w", event, err)
	}
	return out, nil
}

// resolve applies the per-event override and the defaults to the global
// DeliveryConfig. auto is what DeliveryAuto resolves to for this hook point.
func (w *Webhooks) resolve(event WebhookEvent, auto DeliveryMode) resolvedDelivery {
	base := w.cfg.DeliveryConfig
	over, ok := w.cfg.Events[event]

	rd := resolvedDelivery{
		event:      event,
		url:        base.URL,
		mode:       base.Mode,
		secret:     base.Secret,
		headers:    base.Headers,
		timeout:    base.Timeout,
		maxRetries: base.MaxRetries,
		backoffMin: base.BackoffMin,
		backoffMax: base.BackoffMax,
		failPolicy: base.FailPolicy,
		maxPayload: base.MaxPayloadBytes,
		redact:     base.Redact,
	}
	if ok {
		switch {
		case over.Disabled:
			rd.url = ""
		case over.URL != "":
			rd.url = over.URL
		}
		if over.Mode != DeliveryAuto {
			rd.mode = over.Mode
		}
		if over.Secret != "" {
			rd.secret = over.Secret
		}
		if over.Headers != nil {
			rd.headers = over.Headers
		}
		if over.Timeout != 0 {
			rd.timeout = over.Timeout
		}
		if over.MaxRetries != 0 {
			rd.maxRetries = over.MaxRetries
		}
		if over.BackoffMin != 0 {
			rd.backoffMin = over.BackoffMin
		}
		if over.BackoffMax != 0 {
			rd.backoffMax = over.BackoffMax
		}
		if over.FailPolicy != FailAuto {
			rd.failPolicy = over.FailPolicy
		}
		if over.MaxPayloadBytes != 0 {
			rd.maxPayload = over.MaxPayloadBytes
		}
		if over.Redact != nil {
			rd.redact = over.Redact
		}
	}
	if rd.mode == DeliveryAuto {
		rd.mode = auto
	}
	if rd.failPolicy == FailAuto {
		rd.failPolicy = FailClosed
	}
	if rd.timeout <= 0 {
		rd.timeout = DefaultWebhookTimeout
	}
	if rd.maxRetries < 0 {
		rd.maxRetries = 0
	} else if rd.maxRetries == 0 {
		rd.maxRetries = DefaultWebhookMaxRetries
	}
	if rd.backoffMin <= 0 {
		rd.backoffMin = DefaultWebhookBackoffMin
	}
	if rd.backoffMax <= 0 {
		rd.backoffMax = DefaultWebhookBackoffMax
	}
	if rd.backoffMin > rd.backoffMax {
		rd.backoffMin = rd.backoffMax
	}
	if rd.maxPayload == 0 {
		rd.maxPayload = DefaultMaxPayloadBytes
	}
	return rd
}

// delivery is one queued async dispatch: the event, its resolved settings and
// the marshaled payload data, ready to be wrapped in an envelope per attempt.
type delivery struct {
	id    string
	event WebhookEvent
	seq   int64
	rd    resolvedDelivery
	data  []byte

	// log is the logger the delivery was queued with: the run's when the
	// firing hook's context carried one, else the dispatcher's own.
	log *slog.Logger
}

// webhookEnvelope is the body of every POST. ID is the idempotency key a
// receiver dedupes retries on; Seq orders deliveries of one dispatcher.
type webhookEnvelope struct {
	ID        string          `json:"id"`
	Event     WebhookEvent    `json:"event"`
	Timestamp time.Time       `json:"timestamp"`
	Seq       int64           `json:"seq"`
	Attempt   int             `json:"attempt"`
	Data      json.RawMessage `json:"data"`
}

// notify queues one async delivery. It never blocks: a full queue or a closed
// dispatcher drops the delivery and counts it in Dropped. The delivery
// carries the logger of the context it was queued from, so the worker logs to
// the run's logger where the dispatcher has none of its own.
func (w *Webhooks) notify(ctx context.Context, event WebhookEvent, rd resolvedDelivery, data any) {
	if rd.url == "" {
		return
	}
	log := w.loggerFor(ctx)
	raw, ok := w.marshalData(ctx, rd, event, data)
	if !ok {
		return
	}
	d := delivery{id: common.NewEventID(), event: event, seq: w.seq.Add(1), rd: rd, data: raw, log: log}
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		w.dropped.Add(1)
		w.logf(ctx, "webhook %s: dispatcher closed, delivery %s dropped", event, d.id)
		return
	}
	w.ensureStartedLocked()
	select {
	case w.queue <- d:
	default:
		w.mu.Unlock()
		w.dropped.Add(1)
		w.logf(ctx, "webhook %s: queue full, delivery %s dropped", event, d.id)
		return
	}
	w.mu.Unlock()
}

// callSync delivers one event inline with ctx and returns the reply body of
// the first 2xx response. Retries, backoff and the circuit breaker apply.
func (w *Webhooks) callSync(ctx context.Context, rd resolvedDelivery, event WebhookEvent, data any) ([]byte, error) {
	raw, ok := w.marshalData(ctx, rd, event, data)
	if !ok {
		return nil, fmt.Errorf("the %s payload does not marshal", event)
	}
	br := w.breakerFor(rd.url)
	if !br.allow() {
		return nil, errBreakerOpen
	}
	id := common.NewEventID()
	seq := w.seq.Add(1)
	attempts := rd.maxRetries + 1
	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		status, body, retryAfter, err := w.attempt(ctx, rd, id, event, seq, attempt, raw)
		if err == nil && status >= 200 && status < 300 {
			br.recordSuccess()
			return body, nil
		}
		br.recordFailure()
		if err != nil {
			lastErr = err
		} else {
			lastErr = fmt.Errorf("status %d", status)
		}
		if !retryable(status, err) || attempt == attempts {
			break
		}
		if !sleepCtx(ctx, sleepFor(rd, attempt, retryAfter)) {
			return nil, ctx.Err()
		}
	}
	return nil, lastErr
}

// marshalData redacts and marshals one payload, enforcing the size cap by
// replacing an oversized payload with a truncation marker.
func (w *Webhooks) marshalData(ctx context.Context, rd resolvedDelivery, event WebhookEvent, data any) ([]byte, bool) {
	if rd.redact != nil {
		data = rd.redact(event, data)
	}
	raw, err := json.Marshal(data)
	if err != nil {
		w.logf(ctx, "webhook %s: payload does not marshal: %v", event, err)
		return nil, false
	}
	if rd.maxPayload > 0 && len(raw) > rd.maxPayload {
		w.logf(ctx, "webhook %s: payload of %d bytes exceeds the cap of %d, truncating", event, len(raw), rd.maxPayload)
		raw, _ = json.Marshal(map[string]any{"truncated": true, "original_bytes": len(raw)})
	}
	return raw, true
}

// ensureStartedLocked starts the workers once, on the first async delivery.
// Callers hold w.mu.
func (w *Webhooks) ensureStartedLocked() {
	if w.started {
		return
	}
	w.started = true
	for i := 0; i < w.workers; i++ {
		w.wg.Add(1)
		go w.worker()
	}
}

// worker drains the queue until the dispatcher is closed and the queue empty.
func (w *Webhooks) worker() {
	defer w.wg.Done()
	for {
		w.mu.Lock()
		done := w.closed && len(w.queue) == 0
		w.mu.Unlock()
		if done {
			return
		}
		select {
		case d := <-w.queue:
			w.process(d)
		case <-w.quit:
			// Loop around: the check above exits once the queue is drained.
		}
	}
}

// process delivers one queued event with retries, backoff and the circuit
// breaker. Failures are logged, never propagated: async delivery is
// fire-and-forget.
func (w *Webhooks) process(d delivery) {
	rd := d.rd
	l := d.log
	if l == nil {
		l = slog.Default()
	}
	l.Debug("delivering webhook", "event", string(d.event), "delivery", d.id, "mode", rd.mode)
	br := w.breakerFor(rd.url)
	if !br.allow() {
		w.dropped.Add(1)
		logfDelivery(w.baseCtx, l, "webhook %s: circuit open for %s, delivery %s dropped", d.event, rd.url, d.id)
		return
	}
	attempts := rd.maxRetries + 1
	for attempt := 1; attempt <= attempts; attempt++ {
		status, _, retryAfter, err := w.attempt(w.baseCtx, rd, d.id, d.event, d.seq, attempt, d.data)
		if err == nil && status >= 200 && status < 300 {
			br.recordSuccess()
			l.Debug("webhook delivered", "event", string(d.event), "delivery", d.id, "attempt", attempt, "status", status)
			return
		}
		br.recordFailure()
		if !retryable(status, err) || attempt == attempts {
			w.dropped.Add(1)
			logfDelivery(w.baseCtx, l, "webhook %s: giving up on delivery %s after %d attempt(s), status %d, err %v", d.event, d.id, attempt, status, err)
			return
		}
		if !sleepCtx(w.baseCtx, sleepFor(rd, attempt, retryAfter)) {
			return
		}
	}
}

// attempt builds the envelope, marshals it and sends one HTTP request. It
// returns the status, the reply body (2xx only) and a Retry-After hint.
func (w *Webhooks) attempt(ctx context.Context, rd resolvedDelivery, id string, event WebhookEvent, seq int64, n int, raw []byte) (status int, respBody []byte, retryAfter time.Duration, err error) {
	ts := time.Now().UTC()
	env := webhookEnvelope{ID: id, Event: event, Timestamp: ts, Seq: seq, Attempt: n, Data: json.RawMessage(raw)}
	body, err := json.Marshal(env)
	if err != nil {
		return 0, nil, 0, err
	}
	actx, cancel := context.WithTimeout(ctx, rd.timeout)
	defer cancel()
	return w.send(actx, rd, id, event, n, ts, body)
}

// send issues one POST with the envelope body, the configured headers and the
// signature, and drains the response so the connection can be reused.
func (w *Webhooks) send(ctx context.Context, rd resolvedDelivery, id string, event WebhookEvent, n int, ts time.Time, body []byte) (status int, respBody []byte, retryAfter time.Duration, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, rd.url, bytes.NewReader(body))
	if err != nil {
		return 0, nil, 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", webhookUserAgent)
	req.Header.Set("X-Webhook-ID", id)
	req.Header.Set("X-Webhook-Event", string(event))
	req.Header.Set("X-Webhook-Attempt", strconv.Itoa(n))
	for k, v := range rd.headers {
		req.Header.Set(k, v)
	}
	if rd.secret != "" {
		mac := hmac.New(sha256.New, []byte(rd.secret))
		fmt.Fprintf(mac, "%d.", ts.Unix())
		mac.Write(body)
		req.Header.Set("X-Webhook-Timestamp", strconv.FormatInt(ts.Unix(), 10))
		req.Header.Set("X-Webhook-Signature", fmt.Sprintf("t=%d,v1=%s", ts.Unix(), hex.EncodeToString(mac.Sum(nil))))
	}
	resp, err := w.client.Do(req)
	if err != nil {
		return 0, nil, 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		respBody, err = io.ReadAll(io.LimitReader(resp.Body, maxResponseBodyBytes+1))
		return resp.StatusCode, respBody, 0, err
	}
	io.Copy(io.Discard, io.LimitReader(resp.Body, maxResponseBodyBytes))
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusServiceUnavailable {
		retryAfter = parseRetryAfter(resp.Header.Get("Retry-After"))
	}
	return resp.StatusCode, nil, retryAfter, nil
}

// retryable reports whether a failed attempt is worth retrying: transport
// errors, 408, 429 and 5xx are; other 4xx are permanent.
func retryable(status int, err error) bool {
	if err != nil {
		return true
	}
	return status == http.StatusRequestTimeout || status == http.StatusTooManyRequests || status >= 500
}

// sleepFor picks the wait before the next attempt: Retry-After where the
// server sent one (capped at backoffMax), else the jittered backoff.
func sleepFor(rd resolvedDelivery, attempt int, retryAfter time.Duration) time.Duration {
	if retryAfter > 0 {
		if retryAfter > rd.backoffMax {
			return rd.backoffMax
		}
		return retryAfter
	}
	return rd.backoff(attempt)
}

// backoff is the full-jitter exponential backoff for one attempt.
func (rd resolvedDelivery) backoff(attempt int) time.Duration {
	d := rd.backoffMin
	for i := 1; i < attempt && d < rd.backoffMax; i++ {
		d *= 2
	}
	if d > rd.backoffMax || d <= 0 {
		d = rd.backoffMax
	}
	return time.Duration(rand.Int64N(int64(d)))
}

// sleepCtx sleeps for d, reporting whether the sleep completed before ctx ended.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// parseRetryAfter reads a Retry-After header, in seconds or as an HTTP date.
func parseRetryAfter(v string) time.Duration {
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil {
		if secs < 0 {
			return 0
		}
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		return time.Until(t)
	}
	return 0
}

// breakerFor returns the circuit breaker of one endpoint URL, creating it on
// first use: events sharing a URL share a breaker.
func (w *Webhooks) breakerFor(rawURL string) *circuitBreaker {
	w.mu.Lock()
	defer w.mu.Unlock()
	b, ok := w.breakers[rawURL]
	if !ok {
		b = w.newBreaker()
		w.breakers[rawURL] = b
	}
	return b
}

var errBreakerOpen = errors.New("the webhook circuit breaker is open")

// circuitBreaker stops the calls to an endpoint that keeps failing: after
// threshold consecutive failed attempts it opens for a cooldown, failing
// fast; then one probe is let through, and its result closes or reopens it.
type circuitBreaker struct {
	mu        sync.Mutex
	failures  int
	openedAt  time.Time
	probing   bool
	threshold int
	cooldown  time.Duration
}

func (b *circuitBreaker) allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.openedAt.IsZero() {
		return true
	}
	if time.Since(b.openedAt) < b.cooldown {
		return false
	}
	if b.probing {
		return false
	}
	b.probing = true
	return true
}

func (b *circuitBreaker) recordSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures = 0
	b.openedAt = time.Time{}
	b.probing = false
}

func (b *circuitBreaker) recordFailure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures++
	b.probing = false
	if b.failures >= b.threshold {
		b.openedAt = time.Now()
	}
}

// thresholdAndCooldown wires the dispatcher's tuning into a new breaker.
func (w *Webhooks) newBreaker() *circuitBreaker {
	return &circuitBreaker{threshold: w.breakerThreshold, cooldown: w.breakerCooldown}
}

// loggerFor picks the logger a delivery logs to: the dispatcher's own where
// WebhookConfig.Logger set one, then the run's logger carried on the hook's
// context (tagged as a webhook), then the process default. A dispatcher
// plugged in through the hooks alone has no other way to reach the agent's
// logger.
func (w *Webhooks) loggerFor(ctx context.Context) *slog.Logger {
	if w.logger != nil {
		return w.logger
	}
	if l := LoggerFrom(ctx); l != nil {
		return l.With("component", "webhooks")
	}
	return slog.Default()
}

// logf logs a drop or failure at Warn on the logger the context calls for.
func (w *Webhooks) logf(ctx context.Context, format string, args ...any) {
	w.loggerFor(ctx).WarnContext(ctx, fmt.Sprintf(format, args...))
}

// logfDelivery logs a drop or failure of one queued delivery at Warn, on the
// logger the delivery was queued with.
func logfDelivery(ctx context.Context, l *slog.Logger, format string, args ...any) {
	l.WarnContext(ctx, fmt.Sprintf(format, args...))
}

func knownWebhookEvent(e WebhookEvent) bool {
	for _, known := range allWebhookEvents {
		if e == known {
			return true
		}
	}
	return false
}

// validateWebhookURL requires an absolute URL with a host, https unless
// insecure is allowed.
func validateWebhookURL(raw string, allowInsecure bool) error {
	u, err := url.Parse(raw)
	if err != nil {
		return err
	}
	if u.Host == "" {
		return errors.New("no host")
	}
	switch u.Scheme {
	case "https":
		return nil
	case "http":
		if allowInsecure {
			return nil
		}
		return errors.New("http is not allowed unless AllowInsecure is set")
	default:
		return fmt.Errorf("scheme %q is not supported, want https", u.Scheme)
	}
}
