package ingress

import (
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"fillbrunn.ai/kube-agent/proto/pkg/common"
	"fillbrunn.ai/kube-agent/proto/pkg/events"
)

// Coalescer is the buffer a controller keeps for the notifications of each
// AsMessage class: per task and class, at most one report per group window.
// The buffer is the controller's memory, so coalescing is per controller
// instance. Coalescing happens before the log; nothing about it is recorded.
//
// It also enforces the task's event limit: past it, an AsMessage-class
// notification is demoted per the kind's overflow policy and opens no arc.
// Asks and results are always written whole and never pass through here.
type Coalescer struct {
	mu       sync.Mutex
	windows  map[coalesceKey]*coalesceWindow
	admitted map[string]int

	// limitOverride is the agent config's global override of the per-kind
	// event limit, where one is set.
	limitOverride *int

	// logger receives the coalescer's records: a merged flush at Debug, a
	// demotion past the event limit at Warn. Set with WithCoalesceLogger; nil
	// becomes slog.Default() at build.
	logger *slog.Logger

	now func() time.Time
}

// CoalesceOption configures a Coalescer.
type CoalesceOption func(*Coalescer)

// WithEventLimitOverride sets the global override of the per-kind event limit.
func WithEventLimitOverride(limit int) CoalesceOption {
	return func(c *Coalescer) { c.limitOverride = &limit }
}

// WithCoalesceLogger gives a coalescer the logger its coalescing reports to.
// Nil means slog.Default().
func WithCoalesceLogger(logger *slog.Logger) CoalesceOption {
	return func(c *Coalescer) { c.logger = logger }
}

// NewCoalescer returns an empty coalescer.
func NewCoalescer(options ...CoalesceOption) *Coalescer {
	c := &Coalescer{
		windows:  map[coalesceKey]*coalesceWindow{},
		admitted: map[string]int{},
		now:      time.Now,
	}
	for _, option := range options {
		option(c)
	}
	c.logger = resolveLogger(c.logger).With("component", "coalescer")
	return c
}

type coalesceKey struct {
	task  string
	class string
}

type coalesceWindow struct {
	ref      common.TaskRef
	def      common.TaskMetadata
	class    string
	file     func(TaskReport)
	openedAt time.Time
	window   time.Duration
	payloads []any
	messages []string
	timer    *time.Timer
}

// Offer decides what happens to one notification report for a task of the
// given metadata. file is called with the report to write to the log now:
// a None or InResult notification flows as it is, a past-limit AsMessage
// notification is filed demoted, and an AsMessage notification inside its
// limit is held for coalescing, with one merged report flushed when its window
// closes. A report naming a class the kind has not declared is passed on
// unchanged, for the write path to refuse.
func (c *Coalescer) Offer(ref common.TaskRef, metadata common.TaskMetadata, report TaskReport, file func(TaskReport)) {
	class, ok := metadata.Notifications[report.Class]
	if !ok || class.Visibility.Effective() != events.LLMVisibilityAsMessage {
		// None and InResult notifications flow as they are.
		file(report)
		return
	}

	c.mu.Lock()
	limit := metadata.EventLimit
	if c.limitOverride != nil {
		limit = *c.limitOverride
	}
	if limit > 0 && c.admitted[ref.ID] >= limit {
		c.mu.Unlock()
		// Past the limit the event is demoted instead of opening an arc: the
		// overflow policy says ignored (None) or folded (InResult).
		c.logger.Warn("notification past the kind's event limit, demoting it", "task", ref.ID, "class", report.Class, "limit", limit)
		demoted := report
		switch metadata.OverflowPolicy {
		case common.OverflowFold:
			demoted.Visibility = events.LLMVisibilityInResult
		default:
			demoted.Visibility = events.LLMVisibilityNone
		}
		file(demoted)
		return
	}

	key := coalesceKey{task: ref.ID, class: report.Class}
	now := c.now()
	window := c.windows[key]
	if window != nil && now.Sub(window.openedAt) >= window.window {
		// The window has closed without its timer having fired yet: flush it
		// and open a new one with this notification.
		c.flushLocked(key)
		window = nil
	}
	if window == nil {
		window = &coalesceWindow{
			ref:      ref,
			def:      metadata,
			class:    report.Class,
			file:     file,
			openedAt: now,
			window:   class.GroupWindow,
		}
		window.timer = time.AfterFunc(class.GroupWindow, func() {
			c.mu.Lock()
			defer c.mu.Unlock()
			c.flushLocked(key)
		})
		c.windows[key] = window
	}
	window.payloads = append(window.payloads, notificationPayload(report))
	if report.Message != "" {
		window.messages = append(window.messages, report.Message)
	}
	c.mu.Unlock()
}

// flushLocked flushes one window as a single merged report. The window is
// fixed from its first notification, so the merged report carries the whole
// window's payloads in order.
func (c *Coalescer) flushLocked(key coalesceKey) {
	window, ok := c.windows[key]
	if !ok {
		return
	}
	delete(c.windows, key)
	if window.timer != nil {
		window.timer.Stop()
	}
	c.admitted[window.ref.ID]++
	c.logger.Debug("coalesced notifications into one report", "task", window.ref.ID, "class", window.class, "merged", len(window.payloads))

	merged := TaskReport{
		Kind:    TaskReportNotification,
		Class:   window.class,
		Payload: map[string]any{"events": window.payloads},
		Message: fmt.Sprintf("[%d notifications of class %s were merged into this one report]", len(window.payloads), window.class),
	}
	if len(window.messages) > 0 {
		merged.Message += "\n" + strings.Join(window.messages, "\n")
	}
	window.file(merged)
}

// notificationPayload is what a notification carries: its structured payload
// where it has one, its text otherwise.
func notificationPayload(report TaskReport) any {
	if report.Payload != nil {
		return report.Payload
	}
	return report.Message
}
