// Process-level contract for the nau binary: the single program of the
// repository. The state directory keeps its herdr-hermes name, so the
// config root contains the herdr-hermes directory and no nau directory.
// TestMain builds the binary once; every case runs it with an isolated
// HOME/XDG_CONFIG_HOME/APPDATA root, so no state on the machine is read
// or written.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/djalmajr/nau/internal/testutil/fakesoho"
)

const (
	// buildBound bounds the one-shot go build in TestMain.
	buildBound = 180 * time.Second
	// caseBound is the outer deadline of one CLI case.
	caseBound = 30 * time.Second
	// graceWaitDelay is the WaitDelay of the build and of every CLI run.
	graceWaitDelay = 5 * time.Second
)

var (
	// nauBin is the binary built by TestMain.
	nauBin string
)

// TestMain lets the re-executed test binary act as the fake herdr-soho
// of the forwarder test, then builds the binary once (into a temp dir
// removed after m.Run) and runs the package.
func TestMain(m *testing.M) {
	fakesoho.Main()
	dir, err := os.MkdirTemp("", "nau-identity-build-")
	if err != nil {
		fmt.Fprintf(os.Stderr, "nau identity test: %v\n", err)
		os.Exit(1)
	}
	if nauBin, err = buildIdentity(dir, "nau", "github.com/djalmajr/nau/cmd/nau"); err != nil {
		fmt.Fprintf(os.Stderr, "nau identity test: %v\n", err)
		_ = os.RemoveAll(dir)
		os.Exit(1)
	}
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

// buildIdentity builds the named identity out of tree with the go tool
// found on the test's PATH (the go tool puts its own bin dir there), under
// a context deadline and a WaitDelay, with no shell.
func buildIdentity(dir, name, importPath string) (string, error) {
	goBin, err := exec.LookPath("go")
	if err != nil {
		return "", fmt.Errorf("look up go on PATH: %w", err)
	}
	ext := ""
	if runtime.GOOS == "windows" {
		ext = ".exe"
	}
	out := filepath.Join(dir, name+ext)
	ctx, cancel := context.WithTimeout(context.Background(), buildBound)
	defer cancel()
	cmd := exec.CommandContext(ctx, goBin, "build", "-o", out, importPath)
	cmd.WaitDelay = graceWaitDelay
	if o, err := cmd.CombinedOutput(); err != nil {
		return "", fmt.Errorf("go build %s: %v\n%s", importPath, err, o)
	}
	return out, nil
}

// cfgDirFor returns the herdr-hermes config dir the child resolves from
// its explicit environment; it matches os.UserConfigDir for the standard
// per-OS variables the test sets (HOME, XDG_CONFIG_HOME, APPDATA).
func cfgDirFor(root string) string {
	switch runtime.GOOS {
	case "darwin":
		return filepath.Join(root, "home", "Library", "Application Support", "herdr-hermes")
	case "windows":
		return filepath.Join(root, "appdata", "herdr-hermes")
	default:
		return filepath.Join(root, "xdg", "herdr-hermes")
	}
}

// runIdentity runs bin with the isolated config root rooted at root and
// returns stdout, stderr and the exit code. The child environment carries
// only HOME, XDG_CONFIG_HOME and APPDATA, so the binary resolves the
// config dir from the fixture and never touches state on the machine.
func runIdentity(t *testing.T, bin, root string, args ...string) (string, string, int) {
	t.Helper()
	env := []string{
		"HOME=" + filepath.Join(root, "home"),
		"XDG_CONFIG_HOME=" + filepath.Join(root, "xdg"),
		"APPDATA=" + filepath.Join(root, "appdata"),
	}
	ctx, cancel := context.WithTimeout(context.Background(), caseBound)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Env = env
	cmd.WaitDelay = graceWaitDelay
	var out, errb bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errb
	err := cmd.Run()
	exit := -1
	if cmd.ProcessState != nil {
		exit = cmd.ProcessState.ExitCode()
	}
	var exitErr *exec.ExitError
	if err != nil && !errors.As(err, &exitErr) {
		t.Fatalf("run %s %v: %v (exit %d, stdout %q, stderr %q)", filepath.Base(bin), args, err, exit, out.String(), errb.String())
	}
	return out.String(), errb.String(), exit
}

// TestConfigRoundTrip: a config value written by nau is read back over the
// same config root.
func TestConfigRoundTrip(t *testing.T) {
	root := t.TempDir()
	stdout, stderr, exit := runIdentity(t, nauBin, root, "config", "set", "machine_label", "m1")
	if exit != 0 {
		t.Fatalf("nau config set exit = %d, stderr %q", exit, stderr)
	}
	if stdout != `{"key":"machine_label","value":"m1"}`+"\n" {
		t.Fatalf("nau config set stdout = %q", stdout)
	}
	stdout, stderr, exit = runIdentity(t, nauBin, root, "config", "get", "machine_label")
	if exit != 0 {
		t.Fatalf("nau config get exit = %d, stderr %q", exit, stderr)
	}
	if stdout != `{"key":"machine_label","value":"m1"}`+"\n" {
		t.Fatalf("nau config get stdout = %q, want m1", stdout)
	}
	// A second value over the same root.
	if _, stderr, exit := runIdentity(t, nauBin, root, "config", "set", "machine_label", "m2"); exit != 0 {
		t.Fatalf("nau config set exit = %d, stderr %q", exit, stderr)
	}
	stdout, stderr, exit = runIdentity(t, nauBin, root, "config", "get", "machine_label")
	if exit != 0 {
		t.Fatalf("nau config get exit = %d, stderr %q", exit, stderr)
	}
	if stdout != `{"key":"machine_label","value":"m2"}`+"\n" {
		t.Fatalf("nau config get stdout = %q, want m2", stdout)
	}
}

// TestOnlyHerdrHermesStateDir: after the binary ran, the config root
// contains the herdr-hermes directory and no nau directory.
func TestOnlyHerdrHermesStateDir(t *testing.T) {
	root := t.TempDir()
	if _, stderr, exit := runIdentity(t, nauBin, root, "config", "set", "machine_label", "m1"); exit != 0 {
		t.Fatalf("nau config set exit = %d, stderr %q", exit, stderr)
	}
	parent := filepath.Dir(cfgDirFor(root))
	entries, err := os.ReadDir(parent)
	if err != nil {
		t.Fatalf("read config root %s: %v", parent, err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	if len(names) != 1 || names[0] != "herdr-hermes" {
		t.Fatalf("config root = %v, want exactly [herdr-hermes]", names)
	}
}

// TestPreexistingNauDirIgnored: a nau directory with a different config
// next to the state dir is ignored: the binary reads only the
// herdr-hermes directory and leaves the nau directory untouched.
func TestPreexistingNauDirIgnored(t *testing.T) {
	root := t.TempDir()
	parent := filepath.Dir(cfgDirFor(root))
	naudir := filepath.Join(parent, "nau")
	if err := os.MkdirAll(naudir, 0o700); err != nil {
		t.Fatalf("create nau dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(naudir, "config"), []byte("machine_label=other-label\n"), 0o600); err != nil {
		t.Fatalf("write nau config: %v", err)
	}
	stdout, stderr, exit := runIdentity(t, nauBin, root, "config", "get", "machine_label")
	if exit != 0 {
		t.Fatalf("nau config get exit = %d, stderr %q", exit, stderr)
	}
	if stdout != `{"key":"machine_label","value":""}`+"\n" {
		t.Fatalf("nau config get stdout = %q, want the empty value (the nau dir is ignored)", stdout)
	}
	// The pre-existing nau dir is untouched: still exactly its config file.
	entries, err := os.ReadDir(naudir)
	if err != nil {
		t.Fatalf("read nau dir: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "config" {
		var names []string
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("nau dir = %v, want exactly [config]", names)
	}
}

// TestHelpAndNoArgsUsage: help and no-args print the usage on stdout,
// exit 0, empty stderr; the usage leads with the title line, carries the
// usage line and no deprecation line.
func TestHelpAndNoArgsUsage(t *testing.T) {
	const titleSuffix = " - universal messaging and registry core; this build bridges a remote job dispatcher and a Herdr node"
	for _, args := range [][]string{{"help"}, {}} {
		stdout, stderr, exit := runIdentity(t, nauBin, t.TempDir(), args...)
		if exit != 0 {
			t.Fatalf("nau %v exit = %d, want 0, stderr %q", args, exit, stderr)
		}
		if stderr != "" {
			t.Fatalf("nau %v stderr = %q, want empty", args, stderr)
		}
		if !strings.HasPrefix(stdout, "nau"+titleSuffix+"\n") {
			t.Fatalf("nau %v stdout does not lead with the title line: %q", args, firstNLines(stdout, 2))
		}
		if strings.Contains(stdout, "deprecated") {
			t.Errorf("nau %v carries a deprecation mention: %q", args, firstNLines(stdout, 3))
		}
		if strings.Contains(stdout, "herdr-hermes") {
			t.Errorf("nau %v mentions the retired name: %q", args, firstNLines(stdout, 3))
		}
		if !strings.Contains(stdout, "usage: nau <command> [args]\n") {
			t.Errorf("nau %v stdout lacks the usage line", args)
		}
	}
}

// TestBadUsage: bad usage prints exactly one JSON error line on stdout,
// exits 2, and the usage on stderr carries the program name.
func TestBadUsage(t *testing.T) {
	for _, args := range [][]string{
		{"frobnicate"},
		{"help", "extra"},
	} {
		stdout, stderr, exit := runIdentity(t, nauBin, t.TempDir(), args...)
		if exit != 2 {
			t.Fatalf("nau %v exit = %d, want 2", args, exit)
		}
		body := strings.TrimSuffix(stdout, "\n")
		if !strings.HasSuffix(stdout, "\n") || strings.Count(body, "\n") != 0 {
			t.Fatalf("nau %v stdout is not exactly one line: %q", args, stdout)
		}
		var line struct {
			Status string `json:"status"`
			Motivo string `json:"motivo"`
		}
		if err := json.Unmarshal([]byte(body), &line); err != nil {
			t.Fatalf("nau %v stdout is not one JSON line: %v (%q)", args, err, stdout)
		}
		if line.Status != "2" {
			t.Fatalf("nau %v status = %q, want 2", args, line.Status)
		}
		if !strings.HasPrefix(stderr, "nau - ") || !strings.Contains(stderr, "usage: nau <command> [args]") {
			t.Errorf("nau %v stderr does not carry the nau usage: %q", args, firstNLines(stderr, 3))
		}
	}
}

// TestVersion: the version output is one JSON line with a version value.
func TestVersion(t *testing.T) {
	out, stderr, exit := runIdentity(t, nauBin, t.TempDir(), "version")
	if exit != 0 {
		t.Fatalf("version exit = %d, want 0, stderr %q", exit, stderr)
	}
	body := strings.TrimSuffix(out, "\n")
	if !strings.HasSuffix(out, "\n") || strings.Count(body, "\n") != 0 {
		t.Fatalf("version stdout is not exactly one line: %q", out)
	}
	var line map[string]string
	if err := json.Unmarshal([]byte(body), &line); err != nil {
		t.Fatalf("version stdout is not JSON: %v (%q)", err, out)
	}
	if line["version"] == "" {
		t.Errorf("version line lacks a version value: %q", out)
	}
}

// firstNLines returns the first n lines of s joined with newlines, for
// failure messages.
func firstNLines(s string, n int) string {
	lines := strings.Split(s, "\n")
	if len(lines) > n {
		lines = lines[:n]
	}
	return strings.Join(lines, "\n")
}
