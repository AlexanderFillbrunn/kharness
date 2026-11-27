package redisinfra

import "log/slog"

// resolveLogger fills in a component's logger where none was given: the
// process's default, slog.Default(). A library never installs a handler of
// its own, so a consumer that wants silence passes a logger with a discarding
// handler rather than nil.
//
// Logging rules for this package: the event log is the source of truth for
// content, so logs carry metadata only — keys, entry IDs, sequence numbers,
// attempt counts — never event payloads. Every Redis structure here is a
// hint about the log, and the log lines say what the hint did.
func resolveLogger(l *slog.Logger) *slog.Logger {
	if l == nil {
		return slog.Default()
	}
	return l
}
