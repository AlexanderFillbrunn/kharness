package agent

import (
	"context"
	"errors"
	"fillbrunn.ai/kube-agent/proto/pkg/common"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"fillbrunn.ai/kube-agent/proto/pkg/history"
	"fillbrunn.ai/kube-agent/proto/pkg/events"
)

// A work item that cannot say who works the arc is refused before anything is
// taken or read: there is no default agent to fall back on.
func TestWorkRefusesAnItemNamingNoAgent(t *testing.T) {
	agent, err := NewAgent(nil, nil, &scriptedLLM{})
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	lease := NewLocalLease()
	worker, err := NewArcWorker(&logStore{}, &logStore{}, NewFixedResolver(agent), lease)
	if err != nil {
		t.Fatalf("NewArcWorker: %v", err)
	}

	if _, err := worker.Work(context.Background(), testRef(), "", 0); err == nil {
		t.Error("a work item naming no agent was accepted")
	}
	// The refusal came before the lease, so the arc is free to whoever names
	// an agent.
	if release, ok, err := lease.Acquire(context.Background(), testRef()); err != nil || !ok {
		t.Errorf("the refused item left the arc held: ok=%v err=%v", ok, err)
	} else {
		release()
	}
}

// Everything a run writes carries the agent the work item named, which is the
// whole of how the log learns who added what: the agent itself never knows.
func TestARunStampsItsEventsWithTheWorkItemsAgent(t *testing.T) {
	store := runningTaskStore(t)
	agent, err := NewAgent(nil, nil, &scriptedLLM{replies: []*LLMResponse{{ModelID: "gpt-5", Content: "done"}}})
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	worker, err := NewArcWorker(store, store, NewFixedResolver(agent), nil)
	if err != nil {
		t.Fatalf("NewArcWorker: %v", err)
	}

	if _, err := worker.Work(context.Background(), testRef(), testAgentID, 0); err != nil {
		t.Fatalf("Work: %v", err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.events) == 0 {
		t.Fatal("the run wrote nothing")
	}
	for _, ev := range store.events {
		if ev.AgentID != testAgentID {
			t.Errorf("event %s carries agent %q, want %q", ev.ID, ev.AgentID, testAgentID)
		}
	}
}

// The resolver is asked only once the arc is held, so a resolver that cannot
// build the agent fails a worker that owns the arc, not one queued behind
// somebody else.
type refusingResolver struct{ err error }

func (r refusingResolver) ResolveAgent(context.Context, string) (*Agent, error) {
	return nil, r.err
}

func TestAnUnresolvableAgentFailsTheWork(t *testing.T) {
	worker, err := NewArcWorker(&logStore{}, &logStore{}, refusingResolver{err: errors.New("no such definition")}, nil)
	if err != nil {
		t.Fatalf("NewArcWorker: %v", err)
	}
	if _, err := worker.Work(context.Background(), testRef(), testAgentID, 0); err == nil {
		t.Error("a work item naming an unresolvable agent was accepted")
	}
}

// The file resolver answers every id with the same agent, and reads the file
// again only once it changed.
func TestFileResolverIgnoresTheAgentIDAndRereadsOnChange(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agents.json")
	if err := os.WriteFile(path, []byte(settingsJSON("first", "gpt-5")), 0o644); err != nil {
		t.Fatalf("write settings: %v", err)
	}
	resolver := NewFileResolver(path, nil, nil, nil, scriptedModels)

	first, err := resolver.ResolveAgent(context.Background(), "anybody")
	if err != nil {
		t.Fatalf("ResolveAgent: %v", err)
	}
	if first.config.SystemPrompt != "first" {
		t.Errorf("prompt = %q, want the one in the file", first.config.SystemPrompt)
	}

	// The same file answers with the same agent, whatever the id.
	again, err := resolver.ResolveAgent(context.Background(), "somebody else")
	if err != nil {
		t.Fatalf("ResolveAgent: %v", err)
	}
	if again != first {
		t.Error("an unchanged file built a second agent")
	}

	// A changed file builds a fresh agent on the next arc, asking the model it
	// names rather than one the process chose.
	later := time.Now().Add(time.Hour)
	if err := os.WriteFile(path, []byte(settingsJSON("second", "gpt-5-mini")), 0o644); err != nil {
		t.Fatalf("write settings: %v", err)
	}
	if err := os.Chtimes(path, later, later); err != nil {
		t.Fatalf("touch settings: %v", err)
	}
	fresh, err := resolver.ResolveAgent(context.Background(), "anybody")
	if err != nil {
		t.Fatalf("ResolveAgent: %v", err)
	}
	if fresh == first || fresh.config.SystemPrompt != "second" {
		t.Errorf("a changed file resolved to %+v, want a fresh agent on the new prompt", fresh.config)
	}
	if fresh.llm == first.llm {
		t.Error("the model did not change with the file")
	}
	if name := fresh.llm.(*namedLLM).name; name != "gpt-5-mini" {
		t.Errorf("the factory built %q, want the model the file names", name)
	}
}

// namedLLM is the model a definition named, remembered so a test can check
// that the ref travelled: the factory is where a settings file says gpt-5 and
// a client says it back.
type namedLLM struct{ name string }

func (l *namedLLM) StreamResponse(context.Context, *history.ModelTranscript, *LLMOptions, common.Sink[RawToken]) (*LLMResponse, error) {
	return nil, fmt.Errorf("the named model %s was asked for a reply", l.name)
}

func (l *namedLLM) Response(context.Context, *history.ModelTranscript, *LLMOptions) (*LLMResponse, error) {
	return nil, fmt.Errorf("the named model %s was asked for a reply", l.name)
}

// The model a definition names is the model the agent asks, through the
// factory the process supplied: the ref travels, the credentials do not.
var scriptedModels = map[string]ModelFactory{
	"test": func(ref ModelRef) (LLM, error) {
		return &namedLLM{name: ref.Name}, nil
	},
}

func settingsJSON(prompt, model string) string {
	return fmt.Sprintf(`{"system_prompt":%q,"model":{"source":"test","name":%q}}`, prompt, model)
}

func TestFileResolverFailsOnAMissingFile(t *testing.T) {
	resolver := NewFileResolver(filepath.Join(t.TempDir(), "absent.json"), nil, nil, nil, scriptedModels)
	if _, err := resolver.ResolveAgent(context.Background(), testAgentID); err == nil {
		t.Error("a settings file that is not there resolved")
	}
}

// A settings file naming no model is not an agent, since there is nothing to
// ask.
func TestFileResolverRefusesSettingsNamingNoModel(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agents.json")
	if err := os.WriteFile(path, []byte(`{"system_prompt":"a prompt"}`), 0o644); err != nil {
		t.Fatalf("write settings: %v", err)
	}
	resolver := NewFileResolver(path, nil, nil, nil, scriptedModels)
	if _, err := resolver.ResolveAgent(context.Background(), testAgentID); err == nil {
		t.Error("settings naming no model resolved")
	}
}

// The tools a definition names are the tools the agent offers, in the order
// they are named, each answered by the source that holds it.
func TestSelectToolsOffersTheNamedToolsFromTheirSources(t *testing.T) {
	first, second := NewToolRegistry(), NewToolRegistry()
	if err := first.RegisterTool(&mcp.Tool{Name: "list_pods"}, nil); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := second.RegisterTool(&mcp.Tool{Name: "restart_deployment"}, nil); err != nil {
		t.Fatalf("register: %v", err)
	}
	sources := map[string]ToolProvider{"first": first, "second": second}

	selected, err := SelectTools([]ToolRef{
		{Source: "second", Name: "restart_deployment"},
		{Source: "first", Name: "list_pods"},
	}, sources)
	if err != nil {
		t.Fatalf("SelectTools: %v", err)
	}
	if got := toolNames(selected.All()); !slices.Equal(got, []string{"restart_deployment", "list_pods"}) {
		t.Errorf("offered %v, want the named tools in the named order", got)
	}

	// A tool the definition did not name is not offered and not callable.
	if _, ok := selected.Get("wait"); ok {
		t.Error("a tool the definition did not name is offered")
	}
	if _, err := selected.Invoke(context.Background(), "wait", nil); err == nil {
		t.Error("a call for an unselected tool was not refused")
	}
}

func TestSelectToolsRefusesWhatItCannotResolve(t *testing.T) {
	held := NewToolRegistry()
	if err := held.RegisterTool(&mcp.Tool{Name: "list_pods"}, nil); err != nil {
		t.Fatalf("register: %v", err)
	}
	sources := map[string]ToolProvider{"local": held}

	for name, refs := range map[string][]ToolRef{
		"an unknown source":  {{Source: "nowhere", Name: "list_pods"}},
		"an unknown tool":    {{Source: "local", Name: "wait"}},
		"a tool named twice": {{Source: "local", Name: "list_pods"}, {Source: "local", Name: "list_pods"}},
	} {
		if _, err := SelectTools(refs, sources); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
}

// A settings file naming a model source the process does not hold is refused,
// like a tool it does not hold: a definition cannot know what the process can
// reach, and the model it names has to be askable.
func TestFileResolverRefusesAnUnknownModelSource(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agents.json")
	settings := `{"system_prompt":"a prompt","model":{"source":"nowhere","name":"gpt-5"}}`
	if err := os.WriteFile(path, []byte(settings), 0o644); err != nil {
		t.Fatalf("write settings: %v", err)
	}
	resolver := NewFileResolver(path, nil, nil, nil, scriptedModels)
	if _, err := resolver.ResolveAgent(context.Background(), testAgentID); err == nil {
		t.Error("settings naming a model source nothing holds resolved")
	}
}

// roleLLM is a named model that can also carry a task role, the way an
// encoder-backed client can.
type roleLLM struct {
	namedLLM
	role string
}

func (l *roleLLM) SetTaskRole(role string) error {
	if role != "user" && role != "developer" && role != "system" {
		return fmt.Errorf("unknown role %q", role)
	}
	l.role = role
	return nil
}

// A definition's task_role travels to the model client that can carry it, so
// a task-opened arc reaches the model under the named role. A client without
// the setter, or a role nobody defines, is a settings error rather than a
// silent fallback to user.
func TestFileResolverAppliesTaskRoleToTheModel(t *testing.T) {
	roleModels := map[string]ModelFactory{
		"test": func(ref ModelRef) (LLM, error) {
			return &roleLLM{namedLLM: namedLLM{name: ref.Name}}, nil
		},
	}
	write := func(t *testing.T, body string) string {
		t.Helper()
		path := filepath.Join(t.TempDir(), "agents.json")
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatalf("write settings: %v", err)
		}
		return path
	}

	path := write(t, `{"system_prompt":"p","model":{"source":"test","name":"gpt-5"},"task_role":"developer"}`)
	resolver := NewFileResolver(path, nil, nil, nil, roleModels)
	agent, err := resolver.ResolveAgent(context.Background(), testAgentID)
	if err != nil {
		t.Fatalf("ResolveAgent: %v", err)
	}
	if role := agent.llm.(*roleLLM).role; role != "developer" {
		t.Errorf("task role = %q, want developer", role)
	}

	// No task_role: the client keeps its default.
	path = write(t, `{"system_prompt":"p","model":{"source":"test","name":"gpt-5"}}`)
	resolver = NewFileResolver(path, nil, nil, nil, roleModels)
	if agent, err = resolver.ResolveAgent(context.Background(), testAgentID); err != nil {
		t.Fatalf("ResolveAgent: %v", err)
	}
	if role := agent.llm.(*roleLLM).role; role != "" {
		t.Errorf("task role = %q, want unset without task_role in the file", role)
	}

	// A role the client refuses is a settings error.
	path = write(t, `{"system_prompt":"p","model":{"source":"test","name":"gpt-5"},"task_role":"bogus"}`)
	resolver = NewFileResolver(path, nil, nil, nil, roleModels)
	if _, err = resolver.ResolveAgent(context.Background(), testAgentID); err == nil {
		t.Error("an unknown task_role resolved")
	}

	// A client that cannot carry a task role at all is a settings error too.
	path = write(t, `{"system_prompt":"p","model":{"source":"test","name":"gpt-5"},"task_role":"developer"}`)
	resolver = NewFileResolver(path, nil, nil, nil, scriptedModels)
	if _, err = resolver.ResolveAgent(context.Background(), testAgentID); err == nil {
		t.Error("task_role against a model without a setter resolved")
	}
}

// The compaction block resolves into the agent's config: the method, the
// budgets with their defaults off the context window, the limits, and the
// compactor the method names. The context window comes from the catalog,
// overridable globally and per model in the defaults section.
func TestFileResolverResolvesTheCompactionBlock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agents.json")
	settings := `{
		"system_prompt": "a prompt",
		"model": {"source": "test", "name": "gpt-4o"},
		"compaction": {"method": "prune", "keep_recent_tokens": 500, "max_tool_result_chars": 2000},
		"defaults": {"context_window": 64000, "context_windows": {"test/gpt-4o": 90000}}
	}`
	if err := os.WriteFile(path, []byte(settings), 0o644); err != nil {
		t.Fatalf("write settings: %v", err)
	}
	resolver := NewFileResolver(path, nil, nil, nil, scriptedModels)
	agent, err := resolver.ResolveAgent(context.Background(), testAgentID)
	if err != nil {
		t.Fatalf("ResolveAgent: %v", err)
	}
	cfg := agent.config
	if !cfg.Compaction {
		t.Error("compaction is off for a prune method in the settings")
	}
	if cfg.ContextWindow != 90000 {
		t.Errorf("context window = %d, want the per-model override 90000", cfg.ContextWindow)
	}
	if cfg.HigherBudget != 90000-ContextWindowReserve {
		t.Errorf("higher budget = %d, want the context window less the reserve (%d)", cfg.HigherBudget, 90000-ContextWindowReserve)
	}
	if cfg.LowerBudget != 90000*3/4 {
		t.Errorf("lower budget = %d, want 3/4 of the context window", cfg.LowerBudget)
	}
	if cfg.MaxToolResultChars != 2000 {
		t.Errorf("max tool result chars = %d, want the settings' 2000", cfg.MaxToolResultChars)
	}
	// The arc threshold defaults to disabled (-1).
	if cfg.ArcThresholdTokens != -1 {
		t.Errorf("arc threshold default = %d, want -1 (disabled)", cfg.ArcThresholdTokens)
	}
	// The compactor is not on the agent anymore; the resolver equips a
	// compaction worker with a plan instead.
	plan, err := resolver.ResolveCompaction(context.Background(), testAgentID)
	if err != nil {
		t.Fatalf("ResolveCompaction: %v", err)
	}
	if _, ok := plan.Compactor.(PruneCompactor); !ok {
		t.Errorf("compactor = %T, want PruneCompactor for the prune method", plan.Compactor)
	}
	if plan.Limits.KeepRecentTokens != 500 || plan.Limits.MaxToolResultChars != 2000 || plan.Limits.ArcThresholdTokens != -1 {
		t.Errorf("plan limits = %+v, want the settings' 500/2000 and the -1 default", plan.Limits)
	}

	// A settings file with no compaction block disables compaction.
	plain := filepath.Join(t.TempDir(), "plain.json")
	if err := os.WriteFile(plain, []byte(settingsJSON("p", "unknown-model")), 0o644); err != nil {
		t.Fatalf("write settings: %v", err)
	}
	resolver = NewFileResolver(plain, nil, nil, nil, scriptedModels)
	agent, err = resolver.ResolveAgent(context.Background(), testAgentID)
	if err != nil {
		t.Fatalf("ResolveAgent: %v", err)
	}
	if agent.config.Compaction || agent.config.compactionEnabled() {
		t.Errorf("compaction is on without a block: %+v", agent.config)
	}
	if _, err := resolver.ResolveCompaction(context.Background(), testAgentID); err == nil {
		t.Error("ResolveCompaction succeeded without a compaction block")
	}
	if agent.config.ContextWindow != DefaultContextWindow {
		t.Errorf("context window = %d, want the catalog default %d for an unknown model", agent.config.ContextWindow, DefaultContextWindow)
	}
}

// fakeSkillSource holds canned skills by name.
type fakeSkillSource map[string]Skill

func (s fakeSkillSource) Get(name string) (Skill, bool) {
	skill, ok := s[name]
	return skill, ok
}

func testSkillSource() fakeSkillSource {
	return fakeSkillSource{
		"git-workflow": {Name: "git-workflow", Description: "Commit and push this repo's way"},
		"deploy":       {Name: "deploy", Description: "Deploy the service"},
	}
}

// The skills a definition names are the skills the agent offers, in the order
// they are named, each held by the source that names it — the tool selection's
// mirror.
func TestSelectSkillsOffersTheNamedSkillsFromTheirSources(t *testing.T) {
	sources := map[string]SkillSource{"local": testSkillSource()}
	selected, err := SelectSkills([]SkillRef{
		{Source: "local", Name: "deploy"},
		{Source: "local", Name: "git-workflow"},
	}, sources)
	if err != nil {
		t.Fatalf("SelectSkills: %v", err)
	}
	names := make([]string, 0, 2)
	for _, skill := range selected.All() {
		names = append(names, skill.Name)
	}
	if !slices.Equal(names, []string{"deploy", "git-workflow"}) {
		t.Errorf("offered %v, want the named skills in the named order", names)
	}
	if _, ok := selected.Get("other"); ok {
		t.Error("a skill the definition did not name is offered")
	}
}

func TestSelectSkillsRefusesWhatItCannotResolve(t *testing.T) {
	sources := map[string]SkillSource{"local": testSkillSource()}
	for name, refs := range map[string][]SkillRef{
		"an unknown source":   {{Source: "nowhere", Name: "deploy"}},
		"an unknown skill":    {{Source: "local", Name: "other"}},
		"a skill named twice": {{Source: "local", Name: "deploy"}, {Source: "local", Name: "deploy"}},
	} {
		if _, err := SelectSkills(refs, sources); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
}

// The skills block resolves into the agent: the catalog is appended to the
// prompt at build, and load_skill is offered. A skill the process does not
// hold is a settings error, like a tool it does not hold.
func TestFileResolverResolvesTheSkillsBlock(t *testing.T) {
	write := func(t *testing.T, body string) string {
		t.Helper()
		path := filepath.Join(t.TempDir(), "agents.json")
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatalf("write settings: %v", err)
		}
		return path
	}
	skillSources := map[string]SkillSource{"local": testSkillSource()}

	path := write(t, `{
		"system_prompt": "a prompt",
		"model": {"source": "test", "name": "gpt-5"},
		"skills": [{"source": "local", "name": "git-workflow"}]
	}`)
	resolver := NewFileResolver(path, nil, skillSources, nil, scriptedModels)
	agent, err := resolver.ResolveAgent(context.Background(), testAgentID)
	if err != nil {
		t.Fatalf("ResolveAgent: %v", err)
	}
	if agent.skills == nil {
		t.Fatal("the agent holds no skills")
	}
	if !strings.Contains(agent.systemPrompt, "<name>git-workflow</name>") {
		t.Errorf("the prompt has no catalog for the named skill:\n%s", agent.systemPrompt)
	}
	if _, ok := agent.tools.Get(LoadSkillToolName); !ok {
		t.Error("load_skill is not offered")
	}

	// A skill no source holds refuses the whole definition.
	path = write(t, `{
		"system_prompt": "a prompt",
		"model": {"source": "test", "name": "gpt-5"},
		"skills": [{"source": "local", "name": "other"}]
	}`)
	resolver = NewFileResolver(path, nil, skillSources, nil, scriptedModels)
	if _, err := resolver.ResolveAgent(context.Background(), testAgentID); err == nil {
		t.Error("settings naming a skill nothing holds resolved")
	}

	// No skills block: no catalog, no load_skill.
	path = write(t, settingsJSON("p", "gpt-5"))
	resolver = NewFileResolver(path, nil, skillSources, nil, scriptedModels)
	agent, err = resolver.ResolveAgent(context.Background(), testAgentID)
	if err != nil {
		t.Fatalf("ResolveAgent: %v", err)
	}
	if agent.skills != nil || strings.Contains(agent.systemPrompt, "available_skills") {
		t.Errorf("an agent built without a skills block carries skills: %q", agent.systemPrompt)
	}
	if _, ok := agent.tools.Get(LoadSkillToolName); ok {
		t.Error("load_skill is offered without a skills block")
	}
}

// testTaskSource returns one task source holding two canned kinds.
func testTaskSource() TaskSource {
	return &fakeTaskSource{
		kinds: map[string]common.TaskMetadata{
			"auditor":  {Name: "auditor"},
			"migrator": {Name: "migrator"},
		},
		TaskController: &fakeController{},
	}
}

// The tasks a definition names are the tasks the agent may start, in the order
// they are named, each held by the source that names it — the tool selection's
// mirror.
func TestSelectTasksOffersTheNamedTasksFromTheirSources(t *testing.T) {
	sources := map[string]TaskSource{"local": testTaskSource()}
	selected, err := SelectTasks([]TaskRef{
		{Source: "local", Name: "migrator"},
		{Source: "local", Name: "auditor"},
	}, sources)
	if err != nil {
		t.Fatalf("SelectTasks: %v", err)
	}
	names := make([]string, 0, 2)
	for _, meta := range selected.All() {
		names = append(names, meta.Name)
	}
	if !slices.Equal(names, []string{"migrator", "auditor"}) {
		t.Errorf("offered %v, want the named tasks in the named order", names)
	}
	if _, ok := selected.Get("other"); ok {
		t.Error("a task the definition did not name is offered")
	}
}

// A kind the source does not hold has no way to be started, so selecting it is
// refused rather than offered — the "never declared without a way to start it"
// rule, at the point a definition picks its tasks.
func TestSelectTasksRefusesWhatItCannotResolve(t *testing.T) {
	sources := map[string]TaskSource{"local": testTaskSource()}
	for name, refs := range map[string][]TaskRef{
		"an unknown source":  {{Source: "nowhere", Name: "auditor"}},
		"an unknown task":    {{Source: "local", Name: "other"}},
		"a task named twice": {{Source: "local", Name: "auditor"}, {Source: "local", Name: "auditor"}},
	} {
		if _, err := SelectTasks(refs, sources); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
}

// A cancel or an answer reaches the source that started the task, not whichever
// source is asked first.
func TestSelectedTasksRouteCancelToTheStartingSource(t *testing.T) {
	a := &fakeController{}
	b := &fakeController{}
	sources := map[string]TaskSource{
		"a": &fakeTaskSource{kinds: map[string]common.TaskMetadata{"ta": {Name: "ta"}}, TaskController: a},
		"b": &fakeTaskSource{kinds: map[string]common.TaskMetadata{"tb": {Name: "tb"}}, TaskController: b},
	}
	selected, err := SelectTasks([]TaskRef{{Source: "a", Name: "ta"}, {Source: "b", Name: "tb"}}, sources)
	if err != nil {
		t.Fatalf("SelectTasks: %v", err)
	}

	ref := common.TaskRef{SessionID: "s", ID: "task-1"}
	if _, err := selected.Start(context.Background(), "tb", ref, nil); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := selected.Cancel(context.Background(), ref, "done"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if len(a.cancelled) != 0 || len(b.cancelled) != 1 {
		t.Errorf("cancelled a=%v b=%v, want only source b's task reached", a.cancelled, b.cancelled)
	}

	// A task this process never started has no source to route to.
	if err := selected.Cancel(context.Background(), common.TaskRef{ID: "elsewhere"}, ""); err == nil {
		t.Error("a cancel for a task started elsewhere was routed")
	}
}

// The tasks block resolves into the agent: one tool per kind plus the two that
// address a running task, and the resolver hands the same set back for the
// ingress's metadata. A task the process does not hold is a settings error,
// like a tool it does not hold.
func TestFileResolverResolvesTheTasksBlock(t *testing.T) {
	write := func(t *testing.T, body string) string {
		t.Helper()
		path := filepath.Join(t.TempDir(), "agents.json")
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatalf("write settings: %v", err)
		}
		return path
	}
	taskSources := map[string]TaskSource{"local": testTaskSource()}

	path := write(t, `{
		"system_prompt": "a prompt",
		"model": {"source": "test", "name": "gpt-5"},
		"tasks": [{"source": "local", "name": "auditor"}]
	}`)
	resolver := NewFileResolver(path, nil, nil, taskSources, scriptedModels)
	agent, err := resolver.ResolveAgent(context.Background(), testAgentID)
	if err != nil {
		t.Fatalf("ResolveAgent: %v", err)
	}
	if agent.tasks == nil {
		t.Fatal("the agent holds no tasks")
	}
	want := []string{"auditor", events.CancelTaskToolName, events.ProvideTaskInputToolName}
	if got := toolNames(taskToolSet{agent.tasks}.All()); !slices.Equal(got, want) {
		t.Errorf("tools = %v, want %v", got, want)
	}
	// A task no source holds refuses the whole definition.
	path = write(t, `{
		"system_prompt": "a prompt",
		"model": {"source": "test", "name": "gpt-5"},
		"tasks": [{"source": "local", "name": "other"}]
	}`)
	resolver = NewFileResolver(path, nil, nil, taskSources, scriptedModels)
	if _, err := resolver.ResolveAgent(context.Background(), testAgentID); err == nil {
		t.Error("settings naming a task nothing holds resolved")
	}

	// No tasks block: no task tools, and Tasks reports none.
	path = write(t, settingsJSON("p", "gpt-5"))
	resolver = NewFileResolver(path, nil, nil, taskSources, scriptedModels)
	agent, err = resolver.ResolveAgent(context.Background(), testAgentID)
	if err != nil {
		t.Fatalf("ResolveAgent: %v", err)
	}
	if agent.tasks != nil {
		t.Error("an agent built without a tasks block carries tasks")
	}
}
