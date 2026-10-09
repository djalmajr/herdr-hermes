package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestConfigGetDefaults(t *testing.T) {
	dir := t.TempDir()
	stdout, _, exit := runCLI(t, dir, false, "config", "get", "machine_label")
	if exit != 0 {
		t.Fatalf("config get exit = %d, stdout %q", exit, stdout)
	}
	if stdout != "{\"key\":\"machine_label\",\"value\":\"\"}\n" {
		t.Errorf("config get = %q", stdout)
	}
	// Read-only: a fresh config dir is not created.
	assertDirEmpty(t, dir)
}

func TestConfigSetGet(t *testing.T) {
	dir := t.TempDir()
	stdout, _, exit := runCLI(t, dir, false, "config", "set", "machine_label", "lab-1")
	if exit != 0 {
		t.Fatalf("config set exit = %d, stdout %q", exit, stdout)
	}
	if stdout != "{\"key\":\"machine_label\",\"value\":\"lab-1\"}\n" {
		t.Errorf("config set = %q", stdout)
	}
	if _, err := os.Stat(filepath.Join(dir, "config")); err != nil {
		t.Fatalf("config file missing: %v", err)
	}
	stdout, _, exit = runCLI(t, dir, false, "config", "get", "machine_label")
	if exit != 0 || stdout != "{\"key\":\"machine_label\",\"value\":\"lab-1\"}\n" {
		t.Errorf("config get after set = %q (exit %d)", stdout, exit)
	}
	// Other keys keep their defaults; the file holds key=value lines.
	data, err := os.ReadFile(filepath.Join(dir, "config"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"machine_label=lab-1",
		"dispatcher_url=",
		"herdr_soho_bin=herdr-soho",
		"push_timeout_s=15",
	} {
		if !strings.Contains(string(data), want) {
			t.Errorf("config file missing %q:\n%s", want, data)
		}
	}
}

func TestConfigList(t *testing.T) {
	dir := t.TempDir()
	if _, _, exit := runCLI(t, dir, false, "config", "set", "dispatcher_url", "set-value"); exit != 0 {
		t.Fatal("setup: config set dispatcher_url")
	}
	stdout, _, exit := runCLI(t, dir, false, "config", "list")
	if exit != 0 {
		t.Fatalf("config list exit = %d", exit)
	}
	want := `{"config":{"machine_label":"","dispatcher_url":"set-value","herdr_soho_bin":"herdr-soho","push_timeout_s":15}}
`
	if stdout != want {
		t.Errorf("config list = %q, want %q", stdout, want)
	}
}

func TestConfigPushTimeout(t *testing.T) {
	dir := t.TempDir()
	stdout, _, exit := runCLI(t, dir, false, "config", "set", "push_timeout_s", "30")
	if exit != 0 {
		t.Fatalf("set 30: exit %d, stdout %q", exit, stdout)
	}
	stdout, _, exit = runCLI(t, dir, false, "config", "get", "push_timeout_s")
	if exit != 0 || stdout != "{\"key\":\"push_timeout_s\",\"value\":\"30\"}\n" {
		t.Errorf("get push_timeout_s = %q (exit %d)", stdout, exit)
	}
	for _, bad := range []string{"0", "-5", "abc", "1.5", ""} {
		stdout, _, exit := runCLI(t, dir, false, "config", "set", "push_timeout_s", bad)
		if exit != 2 || !strings.Contains(stdout, `"status":"2"`) {
			t.Errorf("set %q: exit %d, stdout %q; want exit 2", bad, exit, stdout)
		}
	}
	// The valid value survives the rejected writes.
	stdout, _, exit = runCLI(t, dir, false, "config", "get", "push_timeout_s")
	if exit != 0 || stdout != "{\"key\":\"push_timeout_s\",\"value\":\"30\"}\n" {
		t.Errorf("get after bad sets = %q (exit %d)", stdout, exit)
	}
}

func TestConfigUnknownKey(t *testing.T) {
	dir := t.TempDir()
	stdout, _, exit := runCLI(t, dir, false, "config", "set", "not_a_key", "x")
	if exit != 2 || !strings.Contains(stdout, `"status":"2"`) || !strings.Contains(stdout, "not_a_key") {
		t.Errorf("set unknown: exit %d, stdout %q", exit, stdout)
	}
	stdout, _, exit = runCLI(t, dir, false, "config", "get", "not_a_key")
	if exit != 2 || !strings.Contains(stdout, `"status":"2"`) {
		t.Errorf("get unknown: exit %d, stdout %q", exit, stdout)
	}
	// An unknown key already in the file is also refused at read time.
	if err := os.WriteFile(filepath.Join(dir, "config"), []byte("machine_label=lab\nbogus=1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	stdout, _, exit = runCLI(t, dir, false, "config", "get", "machine_label")
	if exit != 2 || !strings.Contains(stdout, `"status":"2"`) {
		t.Errorf("get with unknown key in file: exit %d, stdout %q", exit, stdout)
	}
}

func TestConfigCommentsAndBlanks(t *testing.T) {
	dir := t.TempDir()
	content := "# machine notes\n\nmachine_label=lab-9\n   # indented comment\n"
	if err := os.WriteFile(filepath.Join(dir, "config"), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	stdout, _, exit := runCLI(t, dir, false, "config", "get", "machine_label")
	if exit != 0 || stdout != "{\"key\":\"machine_label\",\"value\":\"lab-9\"}\n" {
		t.Errorf("config get with comments = %q (exit %d)", stdout, exit)
	}
	// set rewrites the file canonically (no comments kept, all keys present)
	if _, _, exit := runCLI(t, dir, false, "config", "set", "machine_label", "lab-10"); exit != 0 {
		t.Fatal("config set")
	}
	data, err := os.ReadFile(filepath.Join(dir, "config"))
	if err != nil {
		t.Fatal(err)
	}
	s := string(data)
	if strings.Contains(s, "#") || strings.Contains(s, "lab-9") {
		t.Errorf("rewritten file keeps old content:\n%s", s)
	}
	for _, want := range []string{"machine_label=lab-10", "dispatcher_url=", "herdr_soho_bin=herdr-soho", "push_timeout_s=15"} {
		if !strings.Contains(s, want) {
			t.Errorf("rewritten file missing %q:\n%s", want, s)
		}
	}
}

func TestConfigUsage(t *testing.T) {
	dir := t.TempDir()
	for _, args := range [][]string{
		{},
		{"frobnicate"},
		{"get"},
		{"get", "a", "b"},
		{"set", "machine_label"},
		{"set", "machine_label", "x", "y"},
		{"list", "extra"},
	} {
		stdout, stderr, exit := runCLI(t, dir, false, append([]string{"config"}, args...)...)
		if exit != 2 || !strings.Contains(stderr, "commands:") || !strings.Contains(stdout, `"status":"2"`) {
			t.Errorf("config %v: exit %d, stderr %q, stdout %q", args, exit, stderr, stdout)
		}
	}
}
