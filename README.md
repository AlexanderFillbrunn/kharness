kube-agent
==========

This repository holds five modules, so the wire protocol, the agent worker,
the producer-side ingress, and the concrete implementations cannot grow into
each other:

    fillbrunn.ai/kube-agent/proto       the wire contract, in proto/
    fillbrunn.ai/kube-agent/agent       the agent worker, in agent/
    fillbrunn.ai/kube-agent/ingress     the task-report ingress, in ingress/
    fillbrunn.ai/kube-agent/devchat     the terminal harness, in devchat/
    fillbrunn.ai/kube-agent/redisinfra  Redis-backed lease, cancellation hub and work queue, in redisinfra/

Each module lives in its own folder under the repository root and is laid out
the same way: `go.mod` at the root, importable packages under `pkg/`, and
`cmd/` or `internal/` where they apply. go.work ties the five into one
workspace, so a change to the protocol is built and tested against every other
module at once:

    go.work               the workspace tying the five modules together
    proto/
      pkg/events/         the event types and their payloads
      pkg/users/          the actor
      pkg/common/         the contracts both sides share: refs, work items,
                          the persistor chain, the cancel guard, task declarations
    agent/
      pkg/agent/          the agent loop, the worker, the tool registry, the OpenAI client
      pkg/history/        the fold, and the projection into a model transcript
    ingress/
      pkg/ingress/        what running tasks report, coalescing, and the
                          persist-then-enqueue path producers use
    devchat/
      cmd/devchat/        the harness binary
      internal/harness/   the harness itself, unreachable from other modules
      internal/filestore/ the file-backed event log
    redisinfra/
      pkg/redisinfra/     the Redis-backed lease, cancellation hub, work queue and log

The dependency direction is enforced by the module graph: agent requires proto
and nothing else, ingress requires proto (plus agent for tests only), and
nothing may import the ingress from the agent — outside the workspace that is
a build error, not a convention.

An event-sourced agent. The log is the source of truth: a conversation is a
sequence of events, a `History` is the fold of them, and what the model sees is
derived from that fold rather than kept alongside it.

Two documents specify the transitions between the log and a provider's request,
and the code follows them rather than the other way round:

    docs/PROJECTION.md   what the model is told
    docs/ENCODING.md     how it is carried

The rest are proposals rather than specifications:

    docs/ASYNC-TASKS.md  whether a task belongs to an arc or stands beside one
    docs/COMPACTION.md   how a session continues past the size of a context window
    docs/INTERRUPTS.md   how a tool call is put to a human
    docs/RE-ENTRY.md     how an arc that already exists is worked again
    docs/CANCELLATION.md what stopping an arc reaches, and what it does not
    docs/EVENT-SINK.md   how a run is followed from outside the process running it

The code follows none of them in full, and several are partly built: the log
and the projection carry interrupts already but nothing raises one, an arc is
re-entered under a lease but no queue drives it, and a cancel is recorded and
guarded and reaches a run in this process, but nothing further.

Development harness
--------------------
`devchat` is a separate module in this repository, so the agent framework and
its UI cannot grow into each other. `devchat` and `devchat/internal/filestore`
exist to exercise the agent by hand. Neither is part of the product.

    export OPENAI_API_KEY=...
    cd devchat
    go run ./cmd/devchat

A full-screen terminal interface, built on Bubble Tea. Type a message; the
agent runs until it is your turn again, with the reply appearing as it is
written and each tool call showing whether it is queued, running, done or
failed. Enter sends, esc leaves, and ctrl+c stops the run in progress rather
than the program. Pointing `-store` at the same file again resumes that
conversation, which is drawn from the log when the session opens.

A line starting with a slash is addressed to the harness rather than the
model. `/quit` leaves. `/clear` starts a new session: it moves the log aside,
under a timestamped name, and opens an empty one. Clearing only the screen
would be a lie, since the next turn replays the log and the model would go on
remembering what you had wiped.

`/model <name>` switches models for the rest of the session, keeping the
conversation. Every reply records the model that produced it, so a session
that changed models halfway still reads as one. With no name it says which
model is in use. Nothing validates the name: whether an endpoint serves it is
only answered by asking, so a wrong one shows up as a failed turn.

The interface never reads the agent's state. It follows a run through two
sinks, neither of which the agent knows about: events are published by the
persistor that wrote them, whoever wrote them, and streamed text is published
by the worker. So what it shows is what was recorded.

The tools it offers answer from canned data and touch no cluster. Sessions are
kept under `.devchat/`, one JSON event per line, which makes a session
something you can read with `cat` and a failure something you can replay:

    jq -r '"\(.seq)\t\(.type)"' .devchat/session.jsonl

`-store` takes any path if you want a session somewhere else; the directory is
created if it is not there.

`devchat/internal/filestore` lives in the harness's module, out of the framework's
reach, on purpose. It reads the whole log for every query, never compacts, and
assumes a single writer. It is a way to
run the agent without standing anything up, not the first draft of a real
store. The one thing it does take seriously is the cutoff `GetEventsForArc`
implements, because the projection's correctness rests on it.
