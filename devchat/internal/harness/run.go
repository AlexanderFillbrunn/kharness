// Package harness is the devchat terminal application behind the devchat
// command: the session, the interface and the canned tools. It lives in
// internal so that nothing outside this module can depend on it.
package harness

import (
	"fillbrunn.ai/kube-agent/proto/pkg/common"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	tea "charm.land/bubbletea/v2"

	"fillbrunn.ai/kube-agent/devchat/internal/filestore"
	"fillbrunn.ai/kube-agent/agent/pkg/agent"
	"fillbrunn.ai/kube-agent/proto/pkg/history"
	"fillbrunn.ai/kube-agent/ingress/pkg/ingress"
	"fillbrunn.ai/kube-agent/proto/pkg/users"
)

// localAgentID names the one agent this harness runs. The resolver ignores
// it, but every work item and every event carries it, so the plumbing is
// exercised exactly as a deployed tier would exercise it.
const localAgentID = "local"

const defaultSystemPrompt = `You are a Kubernetes operations assistant with read and restart tools.
Use them rather than guessing, and say plainly when a tool tells you something you did not expect.

Some work runs as background tasks. Each message a task sends arrives as a turn of its own with a "[background task, not the user]" header — the header, not the message role, is what marks it as coming from a task. When a background task asks a question, you may answer it yourself with provide_task_input, or put the question to the user first and then call provide_task_input with their answer. Do not ask the user about a question that has already been answered.`

// Run drives one devchat session: it opens the log, wires the agent to the
// terminal and blocks until the interface leaves. The flags the command
// takes are parsed in cmd/devchat; everything else lives here.
func Run(storePath, settingsPath, userID string) error {
	apiKey := os.Getenv("OPENAI_API_KEY")
	if apiKey == "" {
		return fmt.Errorf("OPENAI_API_KEY is not set")
	}

	store, err := filestore.Open(storePath)
	if err != nil {
		return err
	}
	defer store.Close()

	registry := agent.NewToolRegistry()
	if err := registerFakeTools(registry); err != nil {
		return err
	}

	runner, err := newLocalTasks()
	if err != nil {
		return err
	}

	// The model sources hold what the settings file cannot: which endpoints
	// exist and how to reach them. "openai" is the vendor's own, and "local"
	// is any OpenAI-compatible endpoint this process is pointed at, so a
	// definition asks a source and never says where the source lives.
	openai := func(ref agent.ModelRef) (agent.LLM, error) {
		return agent.NewOpenAI(apiKey, ref.Name), nil
	}
	modelSources := map[string]agent.ModelFactory{"openai": openai}
	if baseURL := os.Getenv("OPENAI_BASE_URL"); baseURL != "" {
		modelSources["local"] = func(ref agent.ModelRef) (agent.LLM, error) {
			return agent.NewOpenAI(apiKey, ref.Name, agent.WithBaseURL(baseURL)), nil
		}
	}

	// The settings file is what the resolver reads, so the prompt, the model
	// and the tools are edited between arcs rather than passed once at
	// startup. A missing file is the first run, and it gets the defaults.
	if _, err := os.Stat(settingsPath); os.IsNotExist(err) {
		if err := writeDefaultSettings(settingsPath); err != nil {
			return err
		}
	}
	settings, err := readSettings(settingsPath)
	if err != nil {
		return err
	}

	// "local" is the one tool source this harness holds: the fake tools
	// compiled into it. A settings file may name no other.
	sources := map[string]agent.ToolProvider{"local": registry}

	// Skills live next to the settings file, in the Agent Skills layout:
	// <settings dir>/skills/<name>/SKILL.md. Like the tools, a settings file
	// may name no skill source but "local".
	skillSources := map[string]agent.SkillSource{
		"local": newFileSkillSource(filepath.Join(filepath.Dir(settingsPath), "skills")),
	}

	// "local" is the one task source this harness holds: the canned tasks
	// compiled into it. Like the tools, a settings file may name no other.
	taskSources := map[string]agent.TaskSource{"local": runner}
	resolver := agent.NewFileResolver(settingsPath, sources, skillSources, taskSources, modelSources)

	// The sinks report into the interface, and the interface cannot exist
	// until the model is built, so they go through a sender that is pointed at
	// the program once there is one.
	relay := &sender{}

	// Notification hangs off the persistor rather than the agent, so
	// everything written reaches the screen: the user's own message and a
	// cancel as much as what a run records. The guard turns a cancel recorded
	// in the log into a run that stops, wherever the cancel was recorded.
	persistor := common.PublishEvents(
		common.MintIdentity(common.GuardCancellation(store, store)),
		screenEvents{relay: relay},
	)

	// One worker for the process, so that a person's turn, a task's report
	// and anything else that enters an arc contend for the same lease. The
	// session arrives with the arc rather than being built in.
	//
	// The cancels go through the same halves a deployed one would: the session
	// announces, the worker watches. In one process they could have been the
	// run's own CancelFunc, but then the harness would exercise a path nothing
	// else has.
	cancels := agent.NewLocalCancels()
	// The two enqueuers are built before the workers they dispatch to,
	// because each worker is built with the other's queue: the arc worker
	// starts the compactions its runs report, and the compaction worker's
	// completion scan re-enqueues the arcs a landed compaction releases.
	enqueuer := &arcEnqueuer{relay: relay}
	compactionStart := &compactionStarter{relay: relay}
	worker, err := agent.NewArcWorker(persistor, store, resolver, agent.NewLocalLease(),
		agent.WithTokenSink(screenTokens{relay: relay}),
		agent.WithCancelWatcher(cancels),
		agent.WithCompactionStarter(compactionStart),
		agent.WithSnapshotPolicy(agent.SnapshotPolicy{Enabled: true}))
	if err != nil {
		return err
	}
	enqueuer.worker = worker

	// The compaction worker serves the compactions a due round reports, under
	// a lease scoped to the session rather than to an arc, since a compaction
	// names no arc.
	compactionWorker, err := agent.NewCompactionWorker(persistor, store, resolver,
		agent.NewLocalSessionLease(), enqueuer, history.DefaultModes(),
		agent.WithCompactionSnapshotPolicy(agent.SnapshotPolicy{Enabled: true}))
	if err != nil {
		return err
	}
	compactionStart.worker = compactionWorker

	// The work queue in this harness is the worker itself: an arc a report
	// opened becomes a work item served from the same place a person's turn
	// is, in arrival order, and the lease keeps two runs off one arc.

	session := &session{
		id:         storePath,
		store:      store,
		persistor:  persistor,
		worker:     worker,
		cancels:    cancels,
		controller: runner,
		actor:      users.Actor{UserID: userID, TenantID: "dev"},
	}

	// The inbound half. A task calls this rather than writing to the log, which
	// is the path a job somewhere else would have to take, so the harness
	// exercises it rather than going around it.
	// The ingress resolves reports against the runner's own held kinds, not
	// against the agent's resolver: the ingress and the agent are separate
	// halves, and in a deployed tier each process resolves on its own.
	reports, err := ingress.NewTaskReports(persistor, store, runner, enqueuer)
	if err != nil {
		return err
	}
	runner.attach(reports, localAgentID,
		func(ref common.TaskRef, err error) { relay.send(taskUnfiledMsg{ref: ref, err: err}) },
	)

	// The conversation already on disk is shown before anything is asked, so
	// a resumed session opens where it left off.
	recorded, err := session.fullHistory(context.Background())
	if err != nil {
		return err
	}

	// The only runner here is this process, so a task in that log is one an
	// earlier process died on. Left alone it would hold its arc open for
	// ever.
	settled, err := ingress.SettleAbandonedTasks(context.Background(), common.AsAgent(localAgentID, persistor), session.id, recorded)
	if err != nil {
		return err
	}
	if settled > 0 {
		if recorded, err = session.fullHistory(context.Background()); err != nil {
			return err
		}
	}

	existing := newTranscript()
	existing.fromHistory(recorded)

	program := tea.NewProgram(newModel(session, relay, settings, settingsPath, existing))
	relay.attach(program)

	_, err = program.Run()
	return err
}

// arcEnqueuer is the devchat work queue: an arc a report opened is served by
// the worker on its own goroutine, from the same queue a person's turn is
// served from. A run that finds the arc's lease held comes back later, since
// the holder's run covers the item by then.
type arcEnqueuer struct {
	worker *agent.ArcWorker
	relay  *sender
}

func (e *arcEnqueuer) Enqueue(_ context.Context, ref common.ArcRef, agentID string) error {
	go func() {
		for {
			worked, err := e.worker.Work(context.Background(), ref, agentID)
			if err != nil {
				e.relay.send(arcDoneMsg{err: err})
				return
			}
			if worked {
				return
			}
			// The arc is held by another run, which is reading the same log.
			time.Sleep(50 * time.Millisecond)
		}
	}()
	return nil
}

// compactionStarter starts a compaction outside the run that reported it: the
// compaction worker runs on its own goroutine, in parallel with the run, the
// same way an arc item is served by the arc worker on its own. A run that
// finds the session's lease held comes back later, since the holder's
// compaction covers the request by then.
type compactionStarter struct {
	worker *agent.CompactionWorker
	relay  *sender
}

func (s *compactionStarter) StartCompaction(_ context.Context, sessionID, requestID, agentID string) {
	go func() {
		for {
			worked, err := s.worker.Work(context.Background(), sessionID, requestID, agentID)
			if err != nil {
				s.relay.send(arcDoneMsg{err: err})
				return
			}
			if worked {
				return
			}
			// The session is held by another compaction, which is reading
			// the same log.
			time.Sleep(50 * time.Millisecond)
		}
	}()
}

// writeDefaultSettings leaves a settings file for the resolver to read, so a
// first run has an agent and the prompt, the model and the tools are there to
// be edited. The tools are the fake ones this harness registers, the skill is
// the sample one written beside them, and the task is the canned one, since a
// definition can only pick what its process holds.
func writeDefaultSettings(path string) error {
	skillsRoot := filepath.Join(filepath.Dir(path), "skills")
	if err := writeDefaultSkill(skillsRoot); err != nil {
		return err
	}
	settings := agent.AgentSettings{
		SystemPrompt: defaultSystemPrompt,
		Model:        agent.ModelRef{Source: "openai", Name: "gpt-5"},
		TaskRole:     "developer",
		Tools: []agent.ToolRef{
			{Source: "local", Name: "list_pods"},
			{Source: "local", Name: "wait"},
			{Source: "local", Name: "restart_deployment"},
		},
		Skills: []agent.SkillRef{{Source: "local", Name: defaultSkillName}},
		Tasks:  []agent.TaskRef{{Source: "local", Name: auditTaskName}},
	}
	return writeSettings(path, settings)
}

// defaultSkillName is the one skill a first run ships, so the skills list in
// the default settings resolves.
const defaultSkillName = "devchat-workflow"

// defaultSkillFiles are written under <root>/<defaultSkillName>/ on a first
// run, and left alone after that: a skill is the user's to edit.
var defaultSkillFiles = map[string]string{
	"SKILL.md": `---
name: devchat-workflow
description: >-
  How to drive this devchat harness: the canned tools it offers, when to use
  background tasks, and how to read what they report.
---

# Devchat workflow

This harness runs one agent over a file-backed log, with canned Kubernetes
tools and background tasks.

- Read state with list_pods before changing anything. restart_deployment
  asks for a rollout but this harness touches no cluster, so it only reports
  what it was asked to do.
- Long waits go through the wait tool, which caps them at 15 seconds.

See references/tools.md for what each canned tool answers.
`,
	"references/tools.md": `# Canned tools

| Tool | Answers |
| --- | --- |
| list_pods | The fake pods and their phase, optionally filtered by namespace. |
| wait | How long it waited, shortened to the 15 second cap where asked for more. |
| restart_deployment | What it was asked to restart; nothing is actually restarted. |

A call that a stopped run never reported on says so in its result, since
whether it took effect is not known.
`,
}

// writeDefaultSkill leaves the sample skill on disk unless it is already
// there.
func writeDefaultSkill(root string) error {
	dir := filepath.Join(root, defaultSkillName)
	if _, err := os.Stat(filepath.Join(dir, "SKILL.md")); err == nil {
		return nil
	}
	for name, content := range defaultSkillFiles {
		path := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			return err
		}
	}
	return nil
}

// readSettings parses the settings file, for the interface as much as the
// resolver: the header shows the model the definition names, not one the
// process holds a pointer to.
func readSettings(path string) (agent.AgentSettings, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return agent.AgentSettings{}, err
	}
	var settings agent.AgentSettings
	if err := json.Unmarshal(raw, &settings); err != nil {
		return agent.AgentSettings{}, fmt.Errorf("parse agent settings %s: %w", path, err)
	}
	return settings, nil
}

// writeSettings puts the settings back, which is how /model changes the model:
// the file is the definition, the resolver rereads it on the next arc, and one
// run never changes the model another arc is already asking.
func writeSettings(path string, settings agent.AgentSettings) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(settings, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, raw, 0o644)
}
