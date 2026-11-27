// Command devchat is a terminal harness for driving the agent by hand.
//
// It is a development tool, not the product. It talks to one model over one
// file-based log and offers tools that answer from canned data.
//
//	export OPENAI_API_KEY=...
//	cd devchat && go run ./cmd/devchat
//
// Type a message and the agent runs until it is your turn again. Enter sends
// and esc leaves. A run in progress is stopped by ctrl+c or by /cancel, which
// withdraws the arc in the log rather than dropping it on the screen.
//
// Sessions are kept under .devchat/. Pointing -store at a log again resumes
// that conversation, and /clear moves the current one aside. The library
// modules' logs go to a file under cmd/devchat/.devchat/ (run from the
// devchat directory, as above), so the terminal stays readable.
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"fillbrunn.ai/kube-agent/devchat/internal/harness"
)

// defaultStore keeps sessions in a directory of their own. They are working
// files of a development tool, and they hold whole conversations, so they do
// not belong next to the source.
const defaultStore = ".devchat/session.jsonl"

// defaultSettings is the agent settings file the resolver reads. It is a
// working file of a development tool, so it lives with the sessions.
const defaultSettings = ".devchat/agents.json"

// defaultLog is where the agent, ingress and queue logs go: a working file of
// a development tool, kept beside the command rather than next to the source.
const defaultLog = "cmd/devchat/.devchat/devchat.log"

func main() {
	store := flag.String("store", defaultStore, "event log to append to, resuming it if it exists")
	settings := flag.String("settings", defaultSettings, "agent settings the resolver reads, created if absent")
	user := flag.String("user", "dev", "who the messages are from")
	logPath := flag.String("log", defaultLog, "log file the agent and ingress write to")
	flag.Parse()

	logger, err := openLog(*logPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "devchat: %v\n", err)
		os.Exit(1)
	}
	slog.SetDefault(logger)
	logger.Info("devchat starting", "store", *store, "settings", *settings, "log", *logPath)

	if err := harness.Run(*store, *settings, *user, logger); err != nil {
		fmt.Fprintf(os.Stderr, "devchat: %v\n", err)
		os.Exit(1)
	}
}

// openLog opens the log file for appending, creating its directory, and
// returns the logger the harness hands to the library modules. devchat is a
// binary, not a library, so installing it as the process default here is
// what makes every component without an explicit logger write to the file.
func openLog(path string) (*slog.Logger, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create the log directory: %w", err)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open the log file: %w", err)
	}
	return slog.New(slog.NewTextHandler(f, &slog.HandlerOptions{Level: slog.LevelDebug})), nil
}
