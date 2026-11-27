package agent

import (
	"context"
	"log/slog"
)

// resolveLogger fills in a component's logger where none was given: the
// process's default, slog.Default(). A library never installs a handler of
// its own, so a consumer that wants silence passes a logger with a discarding
// handler rather than nil.
func resolveLogger(l *slog.Logger) *slog.Logger {
	if l == nil {
		return slog.Default()
	}
	return l
}

// loggerCtxKey carries a run's logger to its hooks. Hooks all receive the
// run's context, so this is how a hook implementation — webhooks included —
// logs to the same logger as the run without the agent knowing it exists.
type loggerCtxKey struct{}

// withLogger returns a context carrying the logger, for Run to put the run's
// logger on the context every hook receives.
func withLogger(ctx context.Context, l *slog.Logger) context.Context {
	return context.WithValue(ctx, loggerCtxKey{}, l)
}

// LoggerFrom returns the logger of the run whose context this is, tagged with
// the run's arc, or nil outside a run. A hook that wants to log uses this
// where it has a context and falls back to its own logger where it does not.
func LoggerFrom(ctx context.Context) *slog.Logger {
	l, _ := ctx.Value(loggerCtxKey{}).(*slog.Logger)
	return l
}

// Logging rules for this package: the event log is the source of truth for
// content, so logs carry metadata only — event types, IDs and sequence
// numbers, token counts, outcomes — never message text, tool arguments or
// the system prompt. What happened is in the log; what the log line adds is
// when, where in the loop, and how it ended.
//
// Levels: Debug for per-event and per-round detail, Info for lifecycle (a run
// admitted or ended, an arc closed or suspended, a compaction requested, a
// work item picked up), Warn for recoverable degradation (a tool call settled
// as an error, a re-offered item, a lost lease heartbeat), Error for failures
// that end a run or lose a write.
