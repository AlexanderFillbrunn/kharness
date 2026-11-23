Settings hash
==========

Status: design only, not implemented.

Purpose
--------------------
Every event a run writes carries a hash of the settings that run was processed
under. The hash is a tripwire, not a replay bundle: as long as it does not
change across a conversation, nothing that influences the model's responses
changed; when it changes, the settings stored outside the log must explain
the difference.

The split is deliberate: the log carries the identifier, the session layer
stores the content. The full settings (system prompt, tool and skill schemas,
model, modes) are kept by the session layer keyed by hash, so a log line plus
its hash is enough to fetch exactly the configuration that produced it. The
log never enumerates settings fields in payloads — that was the half-solution
the `Modes` field on `COMPACTED` was, and it was removed for this reason.

Recipe
--------------------
Versioned so that redefining the hash set can never collide with old hashes:

    settings_hash = "v1:" + hex(sha256(canonical_json(recipe_v1)))

`canonical_json` is JSON with object keys sorted at every level, so the same
settings always serialize identically. `recipe_v1` is:

    {
      "recipe": "v1",
      "system_prompt": "...",
      "model": "source/name",
      "tools": [ {"name": "...", "schema": {...}}, ... ],   // sorted by name
      "modes": { "foreign_arc": "...", "pending": "...", "tool_events": "...",
                 "error": "...", "interrupt": "...", "quote_limit": N }
    }

What is in: everything the model is told or asked under — the system prompt,
the model ref, every available tool or skill as name plus input schema (the
model reads the schema and behaves accordingly), and the projection modes.

What is out: timestamps, event and message IDs, tool results and arguments
(content, not configuration), and the budgets and compaction thresholds. The
budgets shape the run's control flow — whether it suspends, when it requests a
compaction — but not the model's reply, so a budget change passes under the
hash silently. That is a deliberate scoping of v1 to "influences the
responses"; if the hash should also explain the shape of the log, add the
budgets under a new recipe version.

Where it is stamped
--------------------
On the event envelope, next to `Actor`, as an optional field that old log
lines simply do not have:

    type Event struct {
        ...
        Actor        *users.Actor `json:"actor,omitempty"`
        SettingsHash string       `json:"settings_hash,omitempty"`
    }

An empty hash reads as "unknown version" — a line written before hashes
existed — never as equal to the current one, the same convention
`LLMVisibility.Effective()` uses for old lines.

The stamp is the writer's: the agent sets it in `persistAndFold`, where it
already sets `SessionID`, on every event the run writes; the compaction worker
stamps its own hash (its config plus the compactor's identity) on the
`COMPACTED` it writes. Events written from outside the agent — user messages,
task-runner reports — carry no hash, because they were not processed under the
agent's settings. A `COMPACTED` stamped with the worker's hash answers "under
which modes was this summary made" through the outer store, with no mode
fields in the payload.

A run's configuration is fixed for the run's lifetime, so every event of one
run shares one hash; stamping each of them rather than only the arc's opening
record costs a few dozen bytes per event and makes the audit query uniform —
group the session's events by hash and the configuration boundaries fall out.

Reproducibility
--------------------
Reproducing a reply needs three things, and the hash ties them together: the
event log (what was said and done), the settings fetched by hash from the
session layer (under what the model was asked), and the per-message fields
already in the payloads (`ModelID`, `VendorID`, `Usage`). The hash itself
reproduces nothing; it only says which settings version to fetch.
