package cli

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/djalmajr/herdr-hermes/internal/outbox"
)

// runCLI runs Run with a hermetic env over a fresh ConfigDir and returns
// stdout, stderr and the exit code.
func runCLI(t *testing.T, configDir string, nowrite bool, args ...string) (string, string, int) {
	t.Helper()
	var out, errb bytes.Buffer
	env := Env{
		Stdin:     strings.NewReader(""),
		Stdout:    &out,
		Stderr:    &errb,
		Getenv:    getenvFor(nowrite),
		ConfigDir: configDir,
		Now:       func() time.Time { return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC) },
		Sleep:     sleepCtx,
	}
	exit := Run(args, env)
	return out.String(), errb.String(), exit
}

func getenvFor(nowrite bool) func(string) string {
	return func(k string) string {
		if k == nowriteVar && nowrite {
			return "1"
		}
		return ""
	}
}

// assertDirEmpty requires the configuration directory to hold no files at
// all (read-only commands must not create the config dir, state dir or any
// other file).
func assertDirEmpty(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read config dir %s: %v", dir, err)
	}
	if len(entries) != 0 {
		var names []string
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("config dir %s is not empty: %v", dir, names)
	}
}

var allCommandNames = []string{
	"job", "wake", "sync", "push", "outbox", "session", "decision", "auth",
	"config", "doctor", "capabilities", "version", "help", "plugin",
}

var allExitCodes = []string{"0", "2", "3", "4", "40", "41", "42", "43"}

func TestHelp(t *testing.T) {
	stdout, stderr, exit := runCLI(t, t.TempDir(), false, "help")
	if exit != 0 {
		t.Fatalf("help exit = %d, stderr %q", exit, stderr)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Errorf("help wrote to stderr: %q", stderr)
	}
	for _, name := range allCommandNames {
		if !strings.Contains(stdout, name) {
			t.Errorf("usage does not list command %q", name)
		}
	}
	for _, code := range allExitCodes {
		if !strings.Contains(stdout, code) {
			t.Errorf("usage does not list exit code %q", code)
		}
	}
	// No arguments prints the usage like help.
	stdout, _, exit = runCLI(t, t.TempDir(), false)
	if exit != 0 || !strings.Contains(stdout, "commands:") {
		t.Errorf("no-args exit = %d, stdout %q", exit, stdout)
	}
}

func TestUnknownCommand(t *testing.T) {
	stdout, stderr, exit := runCLI(t, t.TempDir(), false, "frobnicate")
	if exit != 2 {
		t.Fatalf("unknown command exit = %d, want 2", exit)
	}
	if !strings.Contains(stderr, "commands:") {
		t.Errorf("usage not printed to stderr:\n%s", stderr)
	}
	if !strings.Contains(stdout, `"status":"2"`) || !strings.Contains(stdout, "frobnicate") {
		t.Errorf("stdout = %q", stdout)
	}
	// A bad flag of a known command also exits 2 with the usage on stderr.
	stdout, stderr, exit = runCLI(t, t.TempDir(), false, "version", "--bogus")
	if exit != 2 || !strings.Contains(stderr, "commands:") || !strings.Contains(stdout, `"status":"2"`) {
		t.Errorf("bad flag: exit %d, stderr %q, stdout %q", exit, stderr, stdout)
	}
}

func TestCapabilities(t *testing.T) {
	stdout, _, exit := runCLI(t, t.TempDir(), false, "capabilities", "--json")
	if exit != 0 {
		t.Fatalf("capabilities exit = %d", exit)
	}
	if stdout != "{\"schema\":1,\"bridge\":1,\"outbox\":1,\"push\":1}\n" {
		t.Errorf("capabilities stdout = %q", stdout)
	}
	// Without --json: exit 2.
	stdout, stderr, exit := runCLI(t, t.TempDir(), false, "capabilities")
	if exit != 2 || !strings.Contains(stderr, "commands:") || !strings.Contains(stdout, `"status":"2"`) {
		t.Errorf("capabilities without --json: exit %d, stderr %q, stdout %q", exit, stderr, stdout)
	}
}

func TestVersion(t *testing.T) {
	stdout, _, exit := runCLI(t, t.TempDir(), false, "version")
	if exit != 0 {
		t.Fatalf("version exit = %d", exit)
	}
	var v struct {
		Version string `json:"version"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(stdout)), &v); err != nil {
		t.Fatalf("version stdout %q is not a JSON line: %v", stdout, err)
	}
	if v.Version != "dev" && !strings.HasPrefix(v.Version, "dev+") {
		t.Errorf("version = %q, want dev or dev+<revision>", v.Version)
	}
	// Dev builds built inside a VCS checkout append the revision.
	if strings.HasPrefix(v.Version, "dev+") && strings.Count(v.Version, "+") != 1 {
		t.Errorf("version = %q", v.Version)
	}
}

func TestNowrite(t *testing.T) {
	// Read-only commands work and write nothing into a fresh ConfigDir.
	for _, tc := range []struct {
		args   []string
		stdout string
	}{
		{[]string{"outbox"}, "{\"outbox\":\"fim\",\"ultimo_seq\":0,\"entregue_seq\":0}\n"},
		{[]string{"outbox", "--since", "0", "--wait", "0"}, "{\"outbox\":\"fim\",\"ultimo_seq\":0,\"entregue_seq\":0}\n"},
		{[]string{"config", "get", "machine_label"}, "{\"key\":\"machine_label\",\"value\":\"\"}\n"},
		{[]string{"capabilities", "--json"}, "{\"schema\":1,\"bridge\":1,\"outbox\":1,\"push\":1}\n"},
		{[]string{"help"}, ""},
	} {
		dir := t.TempDir()
		stdout, _, exit := runCLI(t, dir, true, tc.args...)
		if exit != 0 {
			t.Fatalf("%v under NOWRITE: exit %d, stdout %q", tc.args, exit, stdout)
		}
		if tc.stdout != "" && stdout != tc.stdout {
			t.Errorf("%v stdout = %q, want %q", tc.args, stdout, tc.stdout)
		}
		assertDirEmpty(t, dir)
	}
	// config list under NOWRITE: defaults, nothing written.
	dir := t.TempDir()
	stdout, _, exit := runCLI(t, dir, true, "config", "list")
	if exit != 0 || !strings.Contains(stdout, `"herdr_soho_bin":"herdr-soho"`) || !strings.Contains(stdout, `"push_timeout_s":15`) {
		t.Errorf("config list under NOWRITE: exit %d, stdout %q", exit, stdout)
	}
	assertDirEmpty(t, dir)
	// version under NOWRITE: nothing written.
	dir = t.TempDir()
	_, _, exit = runCLI(t, dir, true, "version")
	if exit != 0 {
		t.Fatalf("version under NOWRITE: exit %d", exit)
	}
	assertDirEmpty(t, dir)
	// The one writing command in this slice refuses before any side effect.
	dir = t.TempDir()
	stdout, _, exit = runCLI(t, dir, true, "config", "set", "machine_label", "x")
	if exit != 2 {
		t.Fatalf("config set under NOWRITE: exit %d, want 2", exit)
	}
	if stdout != "{\"status\":\"nowrite\",\"motivo\":\"HERDR_HERMES_NOWRITE=1\"}\n" {
		t.Errorf("config set under NOWRITE stdout = %q", stdout)
	}
	assertDirEmpty(t, dir)
	// Unknown command still exits 2 under NOWRITE.
	dir = t.TempDir()
	_, _, exit = runCLI(t, dir, true, "frobnicate")
	if exit != 2 {
		t.Fatalf("unknown command under NOWRITE: exit %d, want 2", exit)
	}
	assertDirEmpty(t, dir)
}

// TestNoConfigDir: when the user config directory is unavailable (ConfigDir
// empty) the commands that need the config dir refuse with exit 2 and the
// no_config_dir JSON; help, version and capabilities keep working. There is
// no silent temp-dir fallback.
func TestNoConfigDir(t *testing.T) {
	for _, args := range [][]string{
		{"outbox"},
		{"outbox", "--since", "1", "--wait", "0"},
		{"config", "get", "machine_label"},
		{"config", "list"},
		{"config", "set", "machine_label", "x"},
	} {
		stdout, _, exit := runCLI(t, "", false, args...)
		if exit != 2 {
			t.Fatalf("%v with empty ConfigDir: exit %d, stdout %q; want 2", args, exit, stdout)
		}
		if stdout != "{\"status\":\"no_config_dir\",\"motivo\":\"user config directory unavailable\"}\n" {
			t.Errorf("%v stdout = %q; want the no_config_dir JSON", args, stdout)
		}
	}
	// help, version and capabilities do not need the config dir.
	stdout, _, exit := runCLI(t, "", false, "help")
	if exit != 0 || !strings.Contains(stdout, "usage: herdr-hermes <command> [args]") {
		t.Errorf("help with empty ConfigDir: exit %d", exit)
	}
	stdout, _, exit = runCLI(t, "", false, "capabilities", "--json")
	if exit != 0 || stdout != "{\"schema\":1,\"bridge\":1,\"outbox\":1,\"push\":1}\n" {
		t.Errorf("capabilities with empty ConfigDir: exit %d, stdout %q", exit, stdout)
	}
	_, _, exit = runCLI(t, "", false, "version")
	if exit != 0 {
		t.Fatalf("version with empty ConfigDir: exit %d", exit)
	}
	// No arguments still prints usage (no config dir needed).
	stdout, _, exit = runCLI(t, "", false)
	if exit != 0 || !strings.Contains(stdout, "usage: herdr-hermes <command> [args]") {
		t.Errorf("no args with empty ConfigDir: exit %d", exit)
	}
}

// TestOutboxTrailerUnchangedByLastPush: recording last_push in cursor.json
// must not change the outbox command trailer (delivered_seq is untouched).
func TestOutboxTrailerUnchangedByLastPush(t *testing.T) {
	dir := t.TempDir()
	s, err := outbox.Open(outbox.StateDir(dir), outbox.Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := s.Append(outbox.Record{
		Tipo:    outbox.TipoDispatch,
		Maquina: "machine-a",
		Projeto: "org/repo",
		Dados:   json.RawMessage(`{}`),
	}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := s.SetDeliveredSeq(1); err != nil {
		t.Fatalf("SetDeliveredSeq: %v", err)
	}
	if err := s.SetLastPush(outbox.PushResult{Status: "ok", Code: 200}); err != nil {
		t.Fatalf("SetLastPush: %v", err)
	}
	stdout, _, exit := runCLI(t, dir, false, "outbox")
	if exit != 0 {
		t.Fatalf("outbox: exit %d, stdout %q", exit, stdout)
	}
	lines := strings.Split(strings.TrimSpace(stdout), "\n")
	lastLine := lines[len(lines)-1]
	if lastLine != `{"outbox":"fim","ultimo_seq":1,"entregue_seq":1}` {
		t.Errorf("trailer = %q; want {\"outbox\":\"fim\",\"ultimo_seq\":1,\"entregue_seq\":1}", lastLine)
	}
}
