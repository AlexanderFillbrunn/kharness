package agent

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"fillbrunn.ai/kube-agent/agent/pkg/history"
	"fillbrunn.ai/kube-agent/proto/pkg/common"
)

// fakeSkillSet is a SkillSet of canned skills, with a Load that reports the
// path it was asked for.
type fakeSkillSet struct {
	skills []Skill
}

func (s fakeSkillSet) All() []Skill { return s.skills }

func (s fakeSkillSet) Get(name string) (Skill, bool) {
	for _, skill := range s.skills {
		if skill.Name == name {
			return skill, true
		}
	}
	return Skill{}, false
}

func testSkillSet() fakeSkillSet {
	return fakeSkillSet{skills: []Skill{
		{
			Name:        "git-workflow",
			Description: "Commit and push this repo's way",
			Load: func(_ context.Context, path string) (string, error) {
				if path == "" {
					return "git-workflow body", nil
				}
				return "git-workflow reference " + path, nil
			},
		},
		{
			Name:        "deploy",
			Description: "Deploy <the service> to prod",
			Load: func(_ context.Context, path string) (string, error) {
				if path == "references/never.md" {
					return "", errors.New("the file is not there")
				}
				return "deploy body", nil
			},
		},
	}}
}

// The skills a definition names become the catalog in the system prompt, in the
// Agent Skills standard's <available_skills> format, appended to the configured
// prompt exactly once — at build, not per run. Descriptions are escaped.
func TestWithSkillsAppendsTheCatalogToTheSystemPrompt(t *testing.T) {
	agent, err := NewAgent(&AgentConfig{SystemPrompt: "base prompt"}, nil, &scriptedLLM{}, WithSkills(testSkillSet()))
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	prompt := agent.systemPrompt
	for _, want := range []string{
		"base prompt",
		"<available_skills>",
		"<name>git-workflow</name>",
		"<description>Commit and push this repo&#39;s way</description>",
		"<name>deploy</name>",
		"<description>Deploy &lt;the service&gt; to prod</description>",
		LoadSkillToolName,
	} {
		if !strings.Contains(prompt, want) && !strings.Contains(prompt, strings.ReplaceAll(want, "&#39;", "'")) {
			t.Errorf("the system prompt does not contain %q:\n%s", want, prompt)
		}
	}
	if strings.Count(prompt, "<available_skills>") != 1 {
		t.Errorf("the catalog appears %d times, want once", strings.Count(prompt, "<available_skills>"))
	}

	// An agent with no skills keeps the configured prompt as it is.
	plain, err := NewAgent(&AgentConfig{SystemPrompt: "base prompt"}, nil, &scriptedLLM{})
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	if plain.systemPrompt != "base prompt" {
		t.Errorf("prompt without skills = %q, want the configured one", plain.systemPrompt)
	}
	if _, ok := plain.tools.Get(LoadSkillToolName); ok {
		t.Error("load_skill is offered to an agent with no skills")
	}
}

// The load_skill tool is offered with the configured names as its enum, and it
// is read-only, so its calls may run in a parallel wave and repeat safely.
func TestLoadSkillToolCarriesTheSkillNames(t *testing.T) {
	agent, err := NewAgent(nil, nil, &scriptedLLM{}, WithSkills(testSkillSet()))
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	tool, ok := agent.tools.Get(LoadSkillToolName)
	if !ok {
		t.Fatal("load_skill is not registered")
	}
	if !ReadOnly(tool) || !SafeToRepeat(tool) {
		t.Error("load_skill is not declared read-only and safe to repeat")
	}
	schema := tool.InputSchema.(map[string]any)
	name := schema["properties"].(map[string]any)["name"].(map[string]any)
	enum := name["enum"].([]any)
	got := make([]string, 0, len(enum))
	for _, v := range enum {
		got = append(got, v.(string))
	}
	if !slices.Equal(got, []string{"git-workflow", "deploy"}) {
		t.Errorf("name enum = %v, want the configured skill names", got)
	}
	if _, ok := schema["properties"].(map[string]any)["path"]; !ok {
		t.Error("the schema has no path property for reference files")
	}
}

// A call loads the skill's body, or a reference file by path; a name no skill
// holds and a file that cannot be read come back as results the model can act
// on, not as failures of the run.
func TestLoadSkillInvoke(t *testing.T) {
	agent, err := NewAgent(nil, nil, &scriptedLLM{}, WithSkills(testSkillSet()))
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}

	outcome, err := agent.tools.Invoke(context.Background(), LoadSkillToolName, map[string]any{"name": "git-workflow"})
	if err != nil || outcome.IsError || outcome.Result != "git-workflow body" {
		t.Errorf("body load = %+v, %v; want the body", outcome, err)
	}
	outcome, err = agent.tools.Invoke(context.Background(), LoadSkillToolName,
		map[string]any{"name": "git-workflow", "path": "references/branching.md"})
	if err != nil || outcome.IsError || outcome.Result != "git-workflow reference references/branching.md" {
		t.Errorf("reference load = %+v, %v; want the reference file", outcome, err)
	}
	outcome, err = agent.tools.Invoke(context.Background(), LoadSkillToolName, map[string]any{"name": "nope"})
	if err != nil || !outcome.IsError {
		t.Fatalf("unknown name = %+v, %v; want an error result", outcome, err)
	}
	if !strings.Contains(outcome.Result.(string), "git-workflow") {
		t.Errorf("the error does not list the available skills: %v", outcome.Result)
	}
	outcome, err = agent.tools.Invoke(context.Background(), LoadSkillToolName, map[string]any{"name": "deploy", "path": "references/never.md"})
	if err != nil || !outcome.IsError || !strings.Contains(outcome.Result.(string), "the file is not there") {
		t.Errorf("unreadable file = %+v, %v; want an error result with the reason", outcome, err)
	}
	outcome, err = agent.tools.Invoke(context.Background(), LoadSkillToolName, map[string]any{})
	if err != nil || !outcome.IsError {
		t.Errorf("no name = %+v, %v; want an error result", outcome, err)
	}
}

// One name meaning two things is a call that reaches whichever is checked
// first, so a tool or a task named load_skill collides with the skills' tool.
func TestLoadSkillNameCollidesWithToolsAndTasks(t *testing.T) {
	tools := registryWith(t, LoadSkillToolName, nil)
	if _, err := NewAgent(nil, tools, &scriptedLLM{}, WithSkills(testSkillSet())); err == nil {
		t.Error("an agent was built with load_skill as both a tool and the skills' tool")
	}

	tasks := taskSourceWith(t, &fakeController{}, LoadSkillToolName)
	if _, err := NewAgent(nil, nil, &scriptedLLM{}, WithTasks(tasks), WithSkills(testSkillSet())); err == nil {
		t.Error("an agent was built with load_skill as both a task and the skills' tool")
	}
}

// recordingLLM remembers the options one call was made with.
type recordingLLM struct {
	opts *LLMOptions
}

func (l *recordingLLM) StreamResponse(_ context.Context, _ *history.ModelTranscript, opts *LLMOptions, _ common.Sink[RawToken]) (*LLMResponse, error) {
	l.opts = opts
	return &LLMResponse{ModelID: "gpt-5", Content: "done"}, nil
}

func (l *recordingLLM) Response(ctx context.Context, t *history.ModelTranscript, opts *LLMOptions) (*LLMResponse, error) {
	return l.StreamResponse(ctx, t, opts, nil)
}

// A run asks the model under the prompt the catalog was appended to, and
// offers load_skill beside the ordinary tools.
func TestARunSendsTheCatalogAndOffersLoadSkill(t *testing.T) {
	llm := &recordingLLM{}
	agent, err := NewAgent(&AgentConfig{SystemPrompt: "base prompt"}, nil, llm, WithSkills(testSkillSet()))
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	persistor := &fakePersistor{next: 1}
	if err := agent.Run(context.Background(), RunOptions{Persistor: persistor, History: userArcHistory(t), ArcID: testArcID}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if llm.opts == nil {
		t.Fatal("the model was never called")
	}
	if !strings.Contains(llm.opts.SystemPrompt, "<available_skills>") {
		t.Errorf("the run's prompt has no skills catalog:\n%s", llm.opts.SystemPrompt)
	}
	names := toolNames(llm.opts.Tools.All())
	if !slices.Contains(names, LoadSkillToolName) {
		t.Errorf("the offered tools %v do not include %s", names, LoadSkillToolName)
	}
}

// The load_skill tool of a skill set is synthesized per provider, so the same
// set offered through two agents carries the same schema.
func TestLoadSkillToolSchemaIsStable(t *testing.T) {
	first, second := loadSkillProvider{skills: testSkillSet()}, loadSkillProvider{skills: testSkillSet()}
	a, _ := first.Get(LoadSkillToolName)
	b, _ := second.Get(LoadSkillToolName)
	if fmt.Sprintf("%v", a.InputSchema) != fmt.Sprintf("%v", b.InputSchema) {
		t.Error("two providers over the same set built different schemas")
	}
	var empty loadSkillProvider
	if tools := (loadSkillProvider{skills: fakeSkillSet{}}).All(); tools != nil {
		t.Errorf("an empty skill set offers %v, want nothing", tools)
	}
	if _, ok := empty.Get(LoadSkillToolName); ok {
		t.Error("an empty skill set still answers Get for load_skill")
	}
}
