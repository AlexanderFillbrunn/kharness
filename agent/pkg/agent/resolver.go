package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"sync"
	"time"

	"fillbrunn.ai/kube-agent/proto/pkg/common"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// AgentResolver answers which agent works an arc. It is asked inside the lease
// and after the work item has named the agent, so a failure means this worker
// owned the arc and could not build the agent for it — never that somebody
// else holds the arc.
//
// What a resolver does between calls is its own business. One that reads a
// definition from somewhere changing decides for itself when a resolved agent
// is stale; the contract is only that one arc sees one consistent agent, since
// a run that changed model or tools halfway through would be two agents
// wearing one log.
type AgentResolver interface {
	// ResolveAgent returns the agent that works an arc. The id is the one the
	// work item named; an implementation that cannot tell its agents apart is
	// free to ignore it, and one that can must refuse an id it does not know
	// rather than guess.
	ResolveAgent(ctx context.Context, agentID string) (*Agent, error)
}

// FixedResolver answers every id with the same agent. It is the resolver a
// process with one agent already built has, and the one tests run under.
type FixedResolver struct {
	agent *Agent
}

// NewFixedResolver answers every id with the agent given.
func NewFixedResolver(agent *Agent) *FixedResolver {
	return &FixedResolver{agent: agent}
}

// ResolveAgent returns the agent, whatever the id.
func (f *FixedResolver) ResolveAgent(_ context.Context, _ string) (*Agent, error) {
	if f.agent == nil {
		return nil, errors.New("no agent was given to resolve")
	}
	return f.agent, nil
}

// FixedCompactionResolver answers every id with the same compaction plan. It
// is the resolver a process with one compaction already equipped has, and the
// one tests run under.
type FixedCompactionResolver struct {
	plan *CompactionPlan
}

// NewFixedCompactionResolver answers every id with the plan given.
func NewFixedCompactionResolver(plan *CompactionPlan) *FixedCompactionResolver {
	return &FixedCompactionResolver{plan: plan}
}

// ResolveCompaction returns the plan, whatever the id.
func (f *FixedCompactionResolver) ResolveCompaction(_ context.Context, _ string) (*CompactionPlan, error) {
	if f.plan == nil {
		return nil, errors.New("no compaction plan was given to resolve")
	}
	return f.plan, nil
}

// ModelRef names the model an agent asks: which source serves it, and what
// the model is called there. A source groups the models one endpoint serves,
// so a local OpenAI-compatible API is a source of its own — "local", say —
// configured with its base URL by the process that can reach it. A definition
// says no more than source and name, since everything else about reaching a
// source belongs to the process doing the asking.
type ModelRef struct {
	Source string `json:"source"`
	Name   string `json:"name"`
}

// ToolRef names one tool a definition wants: the source that runs it, and the
// tool's name there. A source is a provider the process already holds, so a
// definition can pick its tools but never invent them.
type ToolRef struct {
	Source string `json:"source"`
	Name   string `json:"name"`
}

// SkillRef names one skill a definition wants: the source that holds it, and
// the skill's name there. Like a tool ref, it names what the process holds —
// where the skill's files live is the source's business, not the definition's.
type SkillRef struct {
	Source string `json:"source"`
	Name   string `json:"name"`
}

// TaskRef names one kind of task a definition wants: the source that runs it,
// and the task's name there. Like a tool ref, it names what the process holds —
// the source holds both the kind's metadata and the controller that starts it.
type TaskRef struct {
	Source string `json:"source"`
	Name   string `json:"name"`
}

// AgentSettings is what a definition says an agent is: what the model is
// told, which model to ask, and which tools to offer. It holds only what a
// definition can say, which is why the tools are names and the model is a
// name: turning those into the code behind them is the resolver's half.
type AgentSettings struct {
	SystemPrompt string    `json:"system_prompt"`
	Model        ModelRef  `json:"model"`
	Tools        []ToolRef `json:"tools"`

	// Skills is the list of skills the agent offers: each named by source and
	// name, resolved like the tools. The agent derives the available_skills
	// catalog in the system prompt and the load_skill tool from them.
	Skills []SkillRef `json:"skills,omitempty"`

	// Tasks is the list of task kinds the agent may start: each named by
	// source and name, resolved like the tools. The agent derives one tool
	// per kind plus the two that address a running task, and starts each
	// kind through the source's controller.
	Tasks []TaskRef `json:"tasks,omitempty"`

	// TaskRole names the provider role the opening message of a task-opened
	// arc is carried as — "developer" where the model has an operator role,
	// so a background task's question does not arrive looking like the user
	// speaking. Empty means "user", the default. The model source must be
	// able to carry it; one that cannot is a settings error, not a fallback.
	TaskRole string `json:"task_role,omitempty"`

	// Interrupts is the list of interrupts the agent may raise: each becomes
	// a tool the model can call to hold the arc until a human answers. The
	// request schema is the tool's input schema, and the call's arguments
	// land in the interrupt's context. Empty means no such tools.
	Interrupts []InterruptSpec `json:"interrupts,omitempty"`

	// Compaction is the session-past-its-own-history block: which compactor
	// runs and the budgets and limits the loop checks. Empty disables
	// compaction.
	Compaction CompactionSettings `json:"compaction,omitempty"`

	// Defaults holds the cross-cutting defaults: the context window the
	// higher budget sits close to, globally and per model.
	Defaults DefaultsSettings `json:"defaults,omitempty"`
}

// CompactionSettings is what a definition says about compaction.
type CompactionSettings struct {
	// Method names the compactor: "llm", "prune" or "provider".
	Method string `json:"method,omitempty"`

	// Model is the model the "llm" method summarises with. Nil means the
	// agent's own model.
	Model *ModelRef `json:"model,omitempty"`

	// LowerBudget triggers a compaction request, in input tokens. Defaults
	// to three quarters of the context window.
	LowerBudget int `json:"lower_budget,omitempty"`

	// HigherBudget suspends runs, in estimated tokens. Defaults to the
	// context window.
	HigherBudget int `json:"higher_budget,omitempty"`

	// KeepRecentTokens is the tail the cut always leaves above it.
	KeepRecentTokens int `json:"keep_recent_tokens,omitempty"`

	// MaxToolResultChars caps one tool result's contribution to a transcript.
	MaxToolResultChars int `json:"max_tool_result_chars,omitempty"`

	// ArcThresholdTokens is the arc size that moves the cut to the arc's
	// live boundary.
	ArcThresholdTokens int `json:"arc_threshold_tokens,omitempty"`
}

// DefaultsSettings holds the cross-cutting defaults a definition can set.
type DefaultsSettings struct {
	// ContextWindow overrides DefaultContextWindow for every model.
	ContextWindow int `json:"context_window,omitempty"`

	// ContextWindows overrides the context window per model, keyed by
	// "source/name" or by the bare model name.
	ContextWindows map[string]int `json:"context_windows,omitempty"`
}

// TaskRoleSetter is implemented by an LLM client whose encoder can carry a
// task-opened arc under a chosen provider role.
type TaskRoleSetter interface {
	SetTaskRole(role string) error
}

// ModelFactory builds the client that asks one source's models. A process
// supplies one per source with the credentials and endpoints it holds; a
// definition supplies the ref.
type ModelFactory func(ref ModelRef) (LLM, error)

// FileResolver builds an agent from a settings file. It ignores the agent id:
// every id names the one agent the file describes, which is what local
// testing wants and nothing a deployed resolver would promise.
//
// The file is read again when it changes, and not before: the mod time is
// what the resolver remembers, so editing the prompt, the model or the tools
// between one arc and the next takes effect on the next arc without anything
// watching the file.
type FileResolver struct {
	path         string
	toolSources  map[string]ToolProvider
	skillSources map[string]SkillSource
	taskSources  map[string]TaskSource
	modelSources map[string]ModelFactory
	options      []AgentOption

	mu      sync.Mutex
	modTime time.Time
	agent   *Agent
}

// NewFileResolver reads agent settings from the file at path, building its
// agent from the tool sources, the skill sources, the task sources, the model
// sources and the options given, which are the parts a file cannot supply.
func NewFileResolver(path string, toolSources map[string]ToolProvider, skillSources map[string]SkillSource, taskSources map[string]TaskSource, modelSources map[string]ModelFactory, options ...AgentOption) *FileResolver {
	return &FileResolver{path: path, toolSources: toolSources, skillSources: skillSources, taskSources: taskSources, modelSources: modelSources, options: options}
}

// ResolveAgent returns the agent the settings file describes, reading it
// again only if it changed since the last read.
func (f *FileResolver) ResolveAgent(_ context.Context, _ string) (*Agent, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	info, err := os.Stat(f.path)
	if err != nil {
		return nil, fmt.Errorf("read agent settings %s: %w", f.path, err)
	}
	if f.agent != nil && !info.ModTime().After(f.modTime) {
		return f.agent, nil
	}

	settings, err := f.loadSettings()
	if err != nil {
		return nil, err
	}

	agent, err := f.build(settings)
	if err != nil {
		return nil, fmt.Errorf("the agent in %s does not build: %w", f.path, err)
	}
	f.modTime = info.ModTime()
	f.agent = agent
	return agent, nil
}

// loadSettings reads and parses the settings file. The caller holds f.mu.
func (f *FileResolver) loadSettings() (AgentSettings, error) {
	raw, err := os.ReadFile(f.path)
	if err != nil {
		return AgentSettings{}, fmt.Errorf("read agent settings %s: %w", f.path, err)
	}
	var settings AgentSettings
	if err := json.Unmarshal(raw, &settings); err != nil {
		return AgentSettings{}, fmt.Errorf("parse agent settings %s: %w", f.path, err)
	}
	return settings, nil
}

// ResolveCompaction equips a compaction under any agent this resolver
// describes: the compactor the settings' method names and the limits the cut
// and the projection are drawn under. It reads the same file as ResolveAgent
// and never builds the agent, so a process that only compacts pays for
// neither its tools nor its prompt.
func (f *FileResolver) ResolveCompaction(_ context.Context, _ string) (*CompactionPlan, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	settings, err := f.loadSettings()
	if err != nil {
		return nil, err
	}
	compaction := settings.Compaction
	if compaction.Method == "" {
		return nil, fmt.Errorf("the settings in %s name no compaction method", f.path)
	}
	if settings.Model.Name == "" || settings.Model.Source == "" {
		return nil, errors.New("the settings name no model to ask")
	}
	factory, ok := f.modelSources[settings.Model.Source]
	if !ok {
		return nil, fmt.Errorf("no model source %s is held by this process", settings.Model.Source)
	}
	llm, err := factory(settings.Model)
	if err != nil {
		return nil, err
	}
	compactor, err := buildCompactor(compaction.Method, compaction.Model, llm, f.modelSources)
	if err != nil {
		return nil, err
	}
	return &CompactionPlan{
		Compactor: compactor,
		Limits: CompactionLimits{
			KeepRecentTokens:   orDefault(compaction.KeepRecentTokens, DefaultKeepRecentTokens),
			MaxToolResultChars: orDefault(compaction.MaxToolResultChars, DefaultMaxToolResultChars),
			ArcThresholdTokens: orDefault(compaction.ArcThresholdTokens, DefaultArcThresholdTokens),
		},
	}, nil
}

// build turns what the file says into the agent that runs it.
func (f *FileResolver) build(settings AgentSettings) (*Agent, error) {
	if settings.Model.Name == "" || settings.Model.Source == "" {
		return nil, errors.New("the settings name no model to ask")
	}
	tools, err := SelectTools(settings.Tools, f.toolSources)
	if err != nil {
		return nil, err
	}
	options := f.options
	if len(settings.Skills) > 0 {
		skills, err := SelectSkills(settings.Skills, f.skillSources)
		if err != nil {
			return nil, err
		}
		options = append(slices.Clone(f.options), WithSkills(skills))
	}
	if len(settings.Tasks) > 0 {
		tasks, err := SelectTasks(settings.Tasks, f.taskSources)
		if err != nil {
			return nil, err
		}
		options = append(slices.Clone(options), WithTasks(tasks))
	}
	if len(settings.Interrupts) > 0 {
		options = append(slices.Clone(options), WithInterrupts(settings.Interrupts))
	}
	factory, ok := f.modelSources[settings.Model.Source]
	if !ok {
		return nil, fmt.Errorf("no model source %s is held by this process", settings.Model.Source)
	}
	llm, err := factory(settings.Model)
	if err != nil {
		return nil, err
	}
	if settings.TaskRole != "" {
		setter, ok := llm.(TaskRoleSetter)
		if !ok {
			return nil, fmt.Errorf("model source %s cannot carry task messages under a %q role", settings.Model.Source, settings.TaskRole)
		}
		if err := setter.SetTaskRole(settings.TaskRole); err != nil {
			return nil, fmt.Errorf("task_role %q: %w", settings.TaskRole, err)
		}
	}

	config := &AgentConfig{SystemPrompt: settings.SystemPrompt}
	config.ContextWindow = contextWindowFor(settings.Model, settings.Defaults)
	compaction := settings.Compaction
	if compaction.Method != "" {
		// The method names which compactor the worker resolves, not the
		// agent: the agent's config only records that compaction is on.
		config.Compaction = true
		config.LowerBudget = orDefault(compaction.LowerBudget, config.ContextWindow*3/4)
		// The higher budget sits just below the context window, leaving a
		// reserve for one more tool result.
		config.HigherBudget = orDefault(compaction.HigherBudget, config.ContextWindow-ContextWindowReserve)
		config.MaxToolResultChars = orDefault(compaction.MaxToolResultChars, DefaultMaxToolResultChars)
		config.ArcThresholdTokens = orDefault(compaction.ArcThresholdTokens, DefaultArcThresholdTokens)
	}

	// The compactor the method names is not built here: the agent only checks
	// budgets and writes records, and ResolveCompaction equips the worker.
	return NewAgent(config, tools, llm, options...)
}

// contextWindowFor resolves the context window of a model: the settings'
// per-model override, then its global default, then the built-in catalog.
func contextWindowFor(ref ModelRef, defaults DefaultsSettings) int {
	window := ContextWindow(ref)
	if defaults.ContextWindow > 0 {
		window = defaults.ContextWindow
	}
	key := ref.Source + "/" + ref.Name
	if v, ok := defaults.ContextWindows[key]; ok {
		window = v
	} else if v, ok := defaults.ContextWindows[ref.Name]; ok {
		window = v
	}
	return window
}

// orDefault returns value where the settings set one, and fallback where they
// left it at zero. A negative value is not "unset": -1 is the explicit marker
// for a disabled limit, and it travels through as it is.
func orDefault(value, fallback int) int {
	if value != 0 {
		return value
	}
	return fallback
}

// buildCompactor builds the compactor a method names. The LLM method may name
// its own model, defaulting to the agent's; the provider method uses the
// agent's client and needs an endpoint on it.
func buildCompactor(method string, modelRef *ModelRef, agentLLM LLM, sources map[string]ModelFactory) (Compactor, error) {
	switch method {
	case CompactionMethodPrune:
		return PruneCompactor{}, nil
	case CompactionMethodLLM:
		llm := agentLLM
		if modelRef != nil {
			factory, ok := sources[modelRef.Source]
			if !ok {
				return nil, fmt.Errorf("no model source %s is held by this process", modelRef.Source)
			}
			built, err := factory(*modelRef)
			if err != nil {
				return nil, err
			}
			llm = built
		}
		transcriptLLM, ok := llm.(TranscriptLLM)
		if !ok {
			return nil, fmt.Errorf("the model %T cannot be called over a transcript to summarise one", llm)
		}
		return &LLMCompactor{LLM: transcriptLLM}, nil
	case CompactionMethodProvider:
		return &ProviderCompactor{LLM: agentLLM}, nil
	default:
		return nil, fmt.Errorf("unknown compaction method %q", method)
	}
}

// SelectTools turns the tools a definition names into the provider that
// offers exactly those, in the order they are named, each run by the source
// that holds it.
//
// A ref naming a source the process does not hold, or a tool its source does
// not, is refused rather than skipped: a definition that silently offered
// fewer tools than it asked for would have the model plan around tools that
// do not exist. The same goes for a tool named twice, since the model picks
// by name and the agent routes by name, so one name must mean one tool.
func SelectTools(refs []ToolRef, sources map[string]ToolProvider) (ToolProvider, error) {
	selected := selectedTools{order: []string{}, byName: map[string]ToolProvider{}}
	for _, ref := range refs {
		source, ok := sources[ref.Source]
		if !ok {
			return nil, fmt.Errorf("no tool source %s is held by this process", ref.Source)
		}
		if _, ok := source.Get(ref.Name); !ok {
			return nil, fmt.Errorf("tool source %s holds no tool %s", ref.Source, ref.Name)
		}
		if _, taken := selected.byName[ref.Name]; taken {
			return nil, fmt.Errorf("tool %s is named twice", ref.Name)
		}
		selected.byName[ref.Name] = source
		selected.order = append(selected.order, ref.Name)
	}
	return selected, nil
}

// selectedTools offers the tools a definition named, each answered by the
// source that holds it.
type selectedTools struct {
	order  []string
	byName map[string]ToolProvider
}

func (s selectedTools) All() []*mcp.Tool {
	tools := make([]*mcp.Tool, 0, len(s.order))
	for _, name := range s.order {
		if tool, ok := s.byName[name].Get(name); ok {
			tools = append(tools, tool)
		}
	}
	return tools
}

func (s selectedTools) Get(tool string) (*mcp.Tool, bool) {
	source, ok := s.byName[tool]
	if !ok {
		return nil, false
	}
	return source.Get(tool)
}

// Invoke runs a selected tool on the source that holds it. A tool the
// definition did not name is not offered to the model, so a call for one is
// refused here rather than reaching a source the definition said nothing
// about.
func (s selectedTools) Invoke(ctx context.Context, name string, args any) (ToolOutcome, error) {
	source, ok := s.byName[name]
	if !ok {
		return ToolOutcome{}, fmt.Errorf("tool not selected: %s", name)
	}
	return source.Invoke(ctx, name, args)
}

// SelectSkills turns the skills a definition names into the set that offers
// exactly those, in the order they are named, each held by the source that
// names it. It refuses what it cannot resolve, for the same reasons
// SelectTools does: a definition that silently offered fewer skills than it
// asked for would have the model plan around skills that do not exist, and one
// name must mean one skill.
func SelectSkills(refs []SkillRef, sources map[string]SkillSource) (SkillSet, error) {
	selected := selectedSkills{order: []string{}, byName: map[string]SkillSource{}}
	for _, ref := range refs {
		source, ok := sources[ref.Source]
		if !ok {
			return nil, fmt.Errorf("no skill source %s is held by this process", ref.Source)
		}
		if _, ok := source.Get(ref.Name); !ok {
			return nil, fmt.Errorf("skill source %s holds no skill %s", ref.Source, ref.Name)
		}
		if _, taken := selected.byName[ref.Name]; taken {
			return nil, fmt.Errorf("skill %s is named twice", ref.Name)
		}
		selected.byName[ref.Name] = source
		selected.order = append(selected.order, ref.Name)
	}
	return selected, nil
}

// selectedSkills offers the skills a definition named, each held by the source
// that names it.
type selectedSkills struct {
	order  []string
	byName map[string]SkillSource
}

func (s selectedSkills) All() []Skill {
	skills := make([]Skill, 0, len(s.order))
	for _, name := range s.order {
		if skill, ok := s.byName[name].Get(name); ok {
			skills = append(skills, skill)
		}
	}
	return skills
}

func (s selectedSkills) Get(name string) (Skill, bool) {
	source, ok := s.byName[name]
	if !ok {
		return Skill{}, false
	}
	return source.Get(name)
}

// SelectTasks turns the tasks a definition names into the source that offers
// exactly those, in the order they are named, each started and reached by the
// source that holds it. It refuses what it cannot resolve, for the same
// reasons SelectTools does: a definition that silently offered fewer tasks
// than it asked for would have the model plan around tasks that do not exist,
// one name must mean one task, and a kind the source does not hold has no way
// to be started.
func SelectTasks(refs []TaskRef, sources map[string]TaskSource) (TaskSource, error) {
	selected := &selectedTasks{order: []string{}, byName: map[string]TaskSource{}, byTask: map[string]TaskSource{}}
	for _, ref := range refs {
		source, ok := sources[ref.Source]
		if !ok {
			return nil, fmt.Errorf("no task source %s is held by this process", ref.Source)
		}
		if _, ok := source.Get(ref.Name); !ok {
			return nil, fmt.Errorf("task source %s holds no task %s", ref.Source, ref.Name)
		}
		if _, taken := selected.byName[ref.Name]; taken {
			return nil, fmt.Errorf("task %s is named twice", ref.Name)
		}
		selected.byName[ref.Name] = source
		selected.order = append(selected.order, ref.Name)
	}
	return selected, nil
}

// selectedTasks offers the tasks a definition named, each held by the source
// that names it. It is itself a TaskSource, so the selected set is handed to
// WithTasks as it is.
type selectedTasks struct {
	order  []string
	byName map[string]TaskSource

	// mu guards byTask, which remembers which source started which task, so a
	// cancel or an answer reaches the source that can answer it. A task
	// started by another process is not remembered; with one source that
	// source is the only candidate anyway.
	mu     sync.Mutex
	byTask map[string]TaskSource
}

func (s *selectedTasks) All() []common.TaskMetadata {
	tasks := make([]common.TaskMetadata, 0, len(s.order))
	for _, name := range s.order {
		if meta, ok := s.byName[name].Get(name); ok {
			tasks = append(tasks, meta)
		}
	}
	return tasks
}

func (s *selectedTasks) Get(name string) (common.TaskMetadata, bool) {
	source, ok := s.byName[name]
	if !ok {
		return common.TaskMetadata{}, false
	}
	return source.Get(name)
}

// Start launches one kind on the source that holds it, and remembers which
// source that was, so the task's later cancels and answers are routed back.
func (s *selectedTasks) Start(ctx context.Context, name string, ref common.TaskRef, args any) (*common.TaskLaunch, error) {
	source, ok := s.byName[name]
	if !ok {
		return nil, fmt.Errorf("task not selected: %s", name)
	}
	s.mu.Lock()
	s.byTask[ref.ID] = source
	s.mu.Unlock()
	return source.Start(ctx, name, ref, args)
}

func (s *selectedTasks) Cancel(ctx context.Context, ref common.TaskRef, reason string) error {
	source, err := s.sourceFor(ref)
	if err != nil {
		return err
	}
	return source.Cancel(ctx, ref, reason)
}

func (s *selectedTasks) ProvideInput(ctx context.Context, ref common.TaskRef, requestID, input string) error {
	source, err := s.sourceFor(ref)
	if err != nil {
		return err
	}
	return source.ProvideInput(ctx, ref, requestID, input)
}

// sourceFor finds the source that started a task: remembered from the start,
// or the only source there is where the process holds one.
func (s *selectedTasks) sourceFor(ref common.TaskRef) (TaskSource, error) {
	s.mu.Lock()
	source, ok := s.byTask[ref.ID]
	s.mu.Unlock()
	if ok {
		return source, nil
	}
	if len(s.byName) == 1 {
		for _, source := range s.byName {
			return source, nil
		}
	}
	return nil, fmt.Errorf("task %s was not started by this process", ref.ID)
}
