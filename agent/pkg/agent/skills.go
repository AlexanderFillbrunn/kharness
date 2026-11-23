package agent

import (
	"context"
	"fmt"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// LoadSkillToolName is the name of the one tool skills are loaded with.
const LoadSkillToolName = "load_skill"

// Skill is one skill an agent is configured with: its identity for the prompt
// catalog and for load_skill, and how its files are read. The files are read
// when the model asks for them, not when the agent is built, so editing a
// skill takes effect on the next call without a rebuild.
type Skill struct {
	Name        string
	Description string

	// Load reads one of the skill's files and returns its content: path empty
	// is the skill's body, otherwise a reference file inside the skill, which
	// the body mentions for progressive disclosure. Implementations confine
	// path to the skill's own directory.
	Load func(ctx context.Context, path string) (string, error)
}

// SkillSet is the skills an agent offers, resolved from the definition.
type SkillSet interface {
	// All returns every skill, in a fixed order, so that two agents built from
	// the same set carry the same catalog.
	All() []Skill

	// Get returns one skill by name.
	Get(name string) (Skill, bool)
}

// SkillSource is one source of skills: what the process holds, resolved by
// name, the way a ToolProvider is. A definition can pick its skills but never
// invent them.
type SkillSource interface {
	Get(name string) (Skill, bool)
}

// loadSkillProvider offers the load_skill tool over a skill set and answers
// calls to it. NewAgent folds it into the agent's tools, so a call by that
// name is offered, routed and classified like any other tool.
type loadSkillProvider struct {
	skills SkillSet
}

// All returns the one tool, or nil where there are no skills to load.
func (p loadSkillProvider) All() []*mcp.Tool {
	if p.skills == nil {
		return nil
	}
	skills := p.skills.All()
	if len(skills) == 0 {
		return nil
	}
	names := make([]any, 0, len(skills))
	for _, skill := range skills {
		names = append(names, skill.Name)
	}
	return []*mcp.Tool{{
		Name: LoadSkillToolName,
		Description: "Load a skill's instructions into the conversation. Call it with a skill's name " +
			"when a task matches the skill's description; the skill's body comes back, and reference " +
			"files the body mentions are loaded with the path argument.",
		// Loading reads files and changes nothing, so the call is safe to
		// repeat and may run beside other read-only calls.
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true, IdempotentHint: true},
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"name": map[string]any{
					"type":        "string",
					"enum":        names,
					"description": "The skill to load, one of the names in the available_skills catalog.",
				},
				"path": map[string]any{
					"type":        "string",
					"description": "A reference file inside the skill, as the body names it. Empty or absent loads the skill body itself.",
				},
			},
			"required": []any{"name"},
		},
	}}
}

func (p loadSkillProvider) Get(name string) (*mcp.Tool, bool) {
	if name != LoadSkillToolName {
		return nil, false
	}
	tools := p.All()
	if len(tools) == 0 {
		return nil, false
	}
	return tools[0], true
}

// Invoke loads one skill's file. A name no skill holds, or a file that cannot
// be read, is a result with IsError set — the model can correct the name and
// call again — not a Go error, since the call itself was made.
func (p loadSkillProvider) Invoke(ctx context.Context, name string, args any) (ToolOutcome, error) {
	if name != LoadSkillToolName {
		return ToolOutcome{}, fmt.Errorf("tool not registered: %s", name)
	}
	skillName, ok := stringArg(args, "name")
	if !ok {
		return ToolOutcome{Result: "no skill name was given", IsError: true}, nil
	}
	skill, ok := p.skills.Get(skillName)
	if !ok {
		return ToolOutcome{Result: fmt.Sprintf("no skill named %q. Available skills: %s",
			skillName, strings.Join(skillNames(p.skills.All()), ", ")), IsError: true}, nil
	}
	path, _ := stringArg(args, "path")
	content, err := skill.Load(ctx, path)
	if err != nil {
		return ToolOutcome{Result: fmt.Sprintf("skill %q could not be loaded: %s", skillName, err), IsError: true}, nil
	}
	return ToolOutcome{Result: content}, nil
}

func skillNames(skills []Skill) []string {
	names := make([]string, 0, len(skills))
	for _, skill := range skills {
		names = append(names, skill.Name)
	}
	return names
}

// skillsCatalog appends the catalog of available skills to the system prompt,
// in the format the Agent Skills standard recommends: an <available_skills>
// block with one <skill> per entry, carrying the name and description from the
// skill's metadata. The bodies are not here — the model loads them with
// load_skill, which is what keeps the catalog at a few dozen tokens a skill.
func skillsCatalog(prompt string, skills SkillSet) string {
	var b strings.Builder
	b.WriteString(prompt)
	if prompt != "" {
		b.WriteString("\n\n")
	}
	b.WriteString("<available_skills>\n")
	for _, skill := range skills.All() {
		b.WriteString("  <skill>\n    <name>")
		b.WriteString(xmlEscape(skill.Name))
		b.WriteString("</name>\n    <description>")
		b.WriteString(xmlEscape(skill.Description))
		b.WriteString("</description>\n  </skill>\n")
	}
	b.WriteString("</available_skills>\n\n")
	b.WriteString("When a task matches a skill's description, call " + LoadSkillToolName +
		" with the skill's name to load its full instructions before doing the work. " +
		"Where the instructions name further files inside the skill, load those too, " +
		"with the path argument.")
	return b.String()
}

// xmlEscape escapes the markup characters that matter inside element text. The
// catalog is prompt text, so line breaks stay line breaks rather than becoming
// character references.
func xmlEscape(s string) string {
	return strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;").Replace(s)
}
