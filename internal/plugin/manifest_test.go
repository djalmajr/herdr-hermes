package plugin_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// TestManifest and TestHooks (the CLI-level plugin tests live in
// internal/cli) are the slice-4b acceptance gates for the plugin manifest:
// it parses with the small line-based TOML subset reader below (no
// third-party module), carries the bridge id, every section id matches the
// Herdr id rule, and every command is an argv array starting with
// herdr-hermes.

var sectionIDRe = regexp.MustCompile(`^[a-z][a-z0-9-]*$`)

type manifestSection struct {
	name    string
	id      string
	command []string
}

type manifest struct {
	id       string
	sections []manifestSection
}

// parseManifest is the line-based TOML subset reader sufficient for
// plugin/herdr-plugin.toml: top-level key = "value" lines, [table] and
// [[table]] headers, and quoted-string / quoted-array values inside
// sections. It is deliberately strict: a line that matches none of those
// shapes fails the test.
func parseManifest(t *testing.T, path string) manifest {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var m manifest
	var cur *manifestSection
	for _, raw := range strings.Split(string(data), "\n") {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "[[") && strings.HasSuffix(line, "]]") {
			m.sections = append(m.sections, manifestSection{name: strings.TrimSpace(line[2 : len(line)-2])})
			cur = &m.sections[len(m.sections)-1]
			continue
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			cur = nil // plain table: nothing the assertions read
			continue
		}
		key, val, eq := strings.Cut(line, "=")
		if !eq {
			t.Fatalf("manifest line %q is not a key/value line or a table header", raw)
		}
		key = strings.TrimSpace(key)
		val = strings.TrimSpace(val)
		if cur == nil {
			if key == "id" {
				m.id = unquoteManifest(t, val)
			}
			continue
		}
		switch key {
		case "id":
			cur.id = unquoteManifest(t, val)
		case "command":
			cur.command = unquoteArrayManifest(t, val)
		}
	}
	return m
}

func unquoteManifest(t *testing.T, v string) string {
	t.Helper()
	if len(v) < 2 || !strings.HasPrefix(v, `"`) || !strings.HasSuffix(v, `"`) {
		t.Fatalf("value %q is not a double-quoted string", v)
	}
	return v[1 : len(v)-1]
}

func unquoteArrayManifest(t *testing.T, v string) []string {
	t.Helper()
	inner := strings.TrimSpace(v)
	if len(inner) < 2 || !strings.HasPrefix(inner, "[") || !strings.HasSuffix(inner, "]") {
		t.Fatalf("command value %q is not an array", v)
	}
	inner = strings.TrimSpace(inner[1 : len(inner)-1])
	if inner == "" {
		t.Fatalf("empty command array")
	}
	var out []string
	for _, part := range strings.Split(inner, ",") {
		out = append(out, unquoteManifest(t, strings.TrimSpace(part)))
	}
	return out
}

func TestManifest(t *testing.T) {
	m := parseManifest(t, filepath.Join("..", "..", "plugin", "herdr-plugin.toml"))

	if m.id != "djalmajr.herdr-hermes" {
		t.Fatalf("plugin id = %q, want djalmajr.herdr-hermes", m.id)
	}
	if len(m.sections) == 0 {
		t.Fatal("manifest has no sections; the reader is not reading the file")
	}

	wantPairs := map[string]bool{
		"startup sync":             false,
		"events workspace-created": false,
		"events workspace-closed":  false,
		"events agent-status":      false,
		"actions status":           false,
		"actions sync":             false,
	}
	for _, s := range m.sections {
		if _, want := wantPairs[s.name+" "+s.id]; want {
			wantPairs[s.name+" "+s.id] = true
		}
		if !sectionIDRe.MatchString(s.id) {
			t.Errorf("[%s] id %q does not match [a-z][a-z0-9-]*", s.name, s.id)
		}
		if len(s.command) == 0 {
			t.Errorf("[%s %s] has no command", s.name, s.id)
			continue
		}
		if s.command[0] != "herdr-hermes" {
			t.Errorf("[%s %s] command %v does not start with herdr-hermes", s.name, s.id, s.command)
		}
	}
	for name, seen := range wantPairs {
		if !seen {
			t.Errorf("manifest is missing the expected section pair %q", name)
		}
	}
}
