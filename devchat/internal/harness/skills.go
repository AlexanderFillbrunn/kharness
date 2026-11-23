package harness

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"fillbrunn.ai/kube-agent/agent/pkg/agent"
)

// fileSkillSource holds skills as SKILL.md folders under one root, the Agent
// Skills layout: <root>/<name>/SKILL.md with YAML frontmatter naming and
// describing the skill, plus any reference files the body mentions. The files
// are read when the model loads them, not when the agent is built, so editing
// a skill takes effect on the next load_skill call.
type fileSkillSource struct {
	root string
}

// newFileSkillSource resolves skill names against the SKILL.md folders under
// root.
func newFileSkillSource(root string) *fileSkillSource {
	return &fileSkillSource{root: root}
}

// Get resolves one skill by name: the description comes from the SKILL.md
// frontmatter, and Load reads the skill's files on demand.
func (s *fileSkillSource) Get(name string) (agent.Skill, bool) {
	dir, err := s.skillDir(name)
	if err != nil {
		return agent.Skill{}, false
	}
	body, err := os.ReadFile(filepath.Join(dir, "SKILL.md"))
	if err != nil {
		return agent.Skill{}, false
	}
	return agent.Skill{
		Name:        name,
		Description: parseFrontmatter(string(body))["description"],
		Load: func(_ context.Context, path string) (string, error) {
			return s.load(name, path)
		},
	}, true
}

// load reads one file of a skill: the SKILL.md body where path is empty,
// otherwise a reference file inside the skill. The path is confined to the
// skill's directory and resolved, so a reference cannot reach outside it.
func (s *fileSkillSource) load(name, path string) (string, error) {
	dir, err := s.skillDir(name)
	if err != nil {
		return "", err
	}
	file := filepath.Join(dir, "SKILL.md")
	if path != "" {
		file, err = confine(dir, path)
		if err != nil {
			return "", err
		}
	}
	resolved, err := filepath.EvalSymlinks(file)
	if err != nil {
		return "", err
	}
	if err := within(dir, resolved); err != nil {
		return "", err
	}
	content, err := os.ReadFile(resolved)
	if err != nil {
		return "", err
	}
	return string(content), nil
}

// skillDir confines a skill name to the source's root, so a name coming from a
// settings file cannot point outside it.
func (s *fileSkillSource) skillDir(name string) (string, error) {
	root, err := filepath.EvalSymlinks(s.root)
	if err != nil {
		return "", err
	}
	dir, err := confine(root, name)
	if err != nil {
		return "", fmt.Errorf("skill name %q: %w", name, err)
	}
	return dir, nil
}

// confine joins path onto dir and returns the resolved path only if it stays
// inside dir: no absolute paths, no escaping through .., and symlinks
// resolved and rechecked.
func confine(dir, path string) (string, error) {
	if filepath.IsAbs(path) {
		return "", fmt.Errorf("path %q is absolute; a path inside a skill is relative to the skill", path)
	}
	clean := filepath.Clean(path)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path %q leaves the directory it is relative to", path)
	}
	resolvedDir, err := filepath.EvalSymlinks(dir)
	if err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(filepath.Join(resolvedDir, clean))
	if err != nil {
		return "", err
	}
	if err := within(resolvedDir, resolved); err != nil {
		return "", fmt.Errorf("path %q resolves outside the directory it is relative to", path)
	}
	return resolved, nil
}

// within reports whether path is dir itself or inside it.
func within(dir, path string) error {
	if path != dir && !strings.HasPrefix(path, dir+string(filepath.Separator)) {
		return fmt.Errorf("%q is not inside %q", path, dir)
	}
	return nil
}

// parseFrontmatter reads the YAML frontmatter of a SKILL.md — the block between
// the opening --- and the next --- — as plain key: value pairs, with folded
// (>) and literal (|) scalars joined into one line. Only the metadata the
// harness needs is understood; the body is handed to the model verbatim.
func parseFrontmatter(body string) map[string]string {
	meta := map[string]string{}
	lines := strings.Split(body, "\n")
	if len(lines) == 0 || strings.TrimSpace(lines[0]) != "---" {
		return meta
	}
	key := ""
	var block []string
	flush := func() {
		if key == "" {
			return
		}
		meta[key] = strings.Join(strings.Fields(strings.Join(block, " ")), " ")
		key, block = "", nil
	}
	for _, line := range lines[1:] {
		if key != "" && (strings.HasPrefix(line, " ") || strings.HasPrefix(line, "\t")) {
			block = append(block, strings.TrimSpace(line))
			continue
		}
		flush()
		trimmed := strings.TrimSpace(line)
		if trimmed == "---" {
			break
		}
		k, v, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		key = strings.TrimSpace(k)
		v = strings.TrimSpace(v)
		if v == ">" || v == ">-" || v == "|" || v == "|-" {
			block = nil
			continue
		}
		meta[key] = strings.Trim(v, `"'`)
		key = ""
	}
	flush()
	return meta
}
