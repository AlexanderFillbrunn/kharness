package harness

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeTestSkill lays down one skill in the Agent Skills layout under a
// temporary root.
func writeTestSkill(t *testing.T, root string) {
	t.Helper()
	dir := filepath.Join(root, "git-workflow")
	if err := os.MkdirAll(filepath.Join(dir, "references"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	skill := `---
name: git-workflow
description: >-
  Commit and push this repo's way, including the branch naming and the
  commit message format.
---

# Git workflow

See references/branching.md for how branches are named.
`
	if err := os.WriteFile(filepath.Join(dir, "SKILL.md"), []byte(skill), 0o644); err != nil {
		t.Fatalf("write SKILL.md: %v", err)
	}
	ref := "# Branching\n\nBranches are <area>/<short-description>.\n"
	if err := os.WriteFile(filepath.Join(dir, "references", "branching.md"), []byte(ref), 0o644); err != nil {
		t.Fatalf("write reference: %v", err)
	}
}

// A skill resolves by name, with its description from the SKILL.md frontmatter
// — a folded scalar joined into one line.
func TestFileSkillSourceResolvesFrontmatter(t *testing.T) {
	root := t.TempDir()
	writeTestSkill(t, root)
	source := newFileSkillSource(root)

	skill, ok := source.Get("git-workflow")
	if !ok {
		t.Fatal("git-workflow did not resolve")
	}
	if skill.Name != "git-workflow" {
		t.Errorf("name = %q", skill.Name)
	}
	want := "Commit and push this repo's way, including the branch naming and the commit message format."
	if skill.Description != want {
		t.Errorf("description = %q, want %q", skill.Description, want)
	}
	if _, ok := source.Get("nope"); ok {
		t.Error("an unknown skill resolved")
	}
	if _, ok := source.Get("../git-workflow"); ok {
		t.Error("a name escaping the root resolved")
	}
}

// Loading reads the body where no path is given, and a reference file inside
// the skill where one is — both fresh from disk on every call.
func TestFileSkillSourceLoadsBodyAndReferences(t *testing.T) {
	root := t.TempDir()
	writeTestSkill(t, root)
	source := newFileSkillSource(root)
	skill, _ := source.Get("git-workflow")

	body, err := skill.Load(context.Background(), "")
	if err != nil {
		t.Fatalf("load body: %v", err)
	}
	if !strings.Contains(body, "# Git workflow") {
		t.Errorf("body = %q, want the SKILL.md content", body)
	}

	ref, err := skill.Load(context.Background(), "references/branching.md")
	if err != nil {
		t.Fatalf("load reference: %v", err)
	}
	if !strings.Contains(ref, "<area>/<short-description>") {
		t.Errorf("reference = %q, want the reference file", ref)
	}

	// An edit is picked up on the next load, without rebuilding anything.
	if err := os.WriteFile(filepath.Join(root, "git-workflow", "SKILL.md"), []byte("---\nname: git-workflow\ndescription: d\n---\n\nedited\n"), 0o644); err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	body, err = skill.Load(context.Background(), "")
	if err != nil || !strings.Contains(body, "edited") {
		t.Errorf("reloaded body = %q, %v; want the edited content", body, err)
	}
}

// A path argument never reaches outside the skill: absolute paths and ..
// escapes are refused, and so is a symlink that resolves out of the skill.
func TestFileSkillSourceConfinesThePath(t *testing.T) {
	root := t.TempDir()
	writeTestSkill(t, root)
	secret := filepath.Join(t.TempDir(), "secret.md")
	if err := os.WriteFile(secret, []byte("secret"), 0o644); err != nil {
		t.Fatalf("write secret: %v", err)
	}
	if err := os.Symlink(secret, filepath.Join(root, "git-workflow", "references", "leak.md")); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	source := newFileSkillSource(root)
	skill, _ := source.Get("git-workflow")

	for _, path := range []string{
		"../outside.md",
		"../../etc/passwd",
		"references/../../outside.md",
		secret, // absolute
		"references/leak.md",
		"references/missing.md",
	} {
		if _, err := skill.Load(context.Background(), path); err == nil {
			t.Errorf("path %q was loaded, want a refusal or a read error", path)
		}
	}
}

func TestParseFrontmatter(t *testing.T) {
	body := "---\nname: a-skill\ndescription: \"quoted: with colon\"\nlicense: MIT\n---\n\n# Body\n"
	meta := parseFrontmatter(body)
	if meta["name"] != "a-skill" || meta["description"] != "quoted: with colon" || meta["license"] != "MIT" {
		t.Errorf("meta = %v", meta)
	}
	if meta := parseFrontmatter("# No frontmatter\n"); len(meta) != 0 {
		t.Errorf("body without frontmatter parsed as %v", meta)
	}
	literal := "---\ndescription: |\n  line one\n  line two\n---\n"
	if got := parseFrontmatter(literal)["description"]; got != "line one line two" {
		t.Errorf("literal block = %q", got)
	}
}
