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
// that conversation, and /clear moves the current one aside.
package main

import (
	"flag"
	"fmt"
	"os"

	"fillbrunn.ai/kube-agent/devchat/internal/harness"
)

// defaultStore keeps sessions in a directory of their own. They are working
// files of a development tool, and they hold whole conversations, so they do
// not belong next to the source.
const defaultStore = ".devchat/session.jsonl"

// defaultSettings is the agent settings file the resolver reads. It is a
// working file of a development tool, so it lives with the sessions.
const defaultSettings = ".devchat/agents.json"

func main() {
	store := flag.String("store", defaultStore, "event log to append to, resuming it if it exists")
	settings := flag.String("settings", defaultSettings, "agent settings the resolver reads, created if absent")
	user := flag.String("user", "dev", "who the messages are from")
	flag.Parse()

	if err := harness.Run(*store, *settings, *user); err != nil {
		fmt.Fprintf(os.Stderr, "devchat: %v\n", err)
		os.Exit(1)
	}
}
