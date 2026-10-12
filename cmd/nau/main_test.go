// Process-level identity contract: the nau and the deprecated herdr-hermes
// binaries are the same program. They share one state directory and one
// configuration, print the same version, and differ only in the program
// name they carry in the usage text and the stderr diagnostic prefix.
// TestMain builds both binaries once (like the herdr-hermes package
// test); every case runs one of them with an isolated
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
)

const (
	// buildBound bounds the one-shot go build of one identity in TestMain.
	buildBound = 180 * time.Second
	// caseBound is the outer deadline of one CLI case.
	caseBound = 30 * time.Second
	// graceWaitDelay is the WaitDelay of the builds and of every CLI run.
	graceWaitDelay = 5 * time.Second
)

var (
	// nauBin and herdrHermesBin are the two identity binaries built by
	// TestMain.
	nauBin         string
	herdrHermesBin string
)

// TestMain builds both identities once (into a temp dir removed after
// m.Run) and runs the package.
func TestMain(m *testing.M) {
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
	if herdrHermesBin, err = buildIdentity(dir, "herdr-hermes", "github.com/djalmajr/nau/cmd/herdr-hermes"); err != nil {
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
// only HOME, XDG_CONFIG_HOME and APPDATA, so the binaries resolve the
// config dir from the fixture and never touch state on the machine.
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

// TestConfigRoundTripAcrossNames: a config value written by one name is
// read by the other, and vice versa, over the same config root.
func TestConfigRoundTripAcrossNames(t *testing.T) {
	root := t.TempDir()
	stdout, stderr, exit := runIdentity(t, nauBin, root, "config", "set", "machine_label", "m1")
	if exit != 0 {
		t.Fatalf("nau config set exit = %d, stderr %q", exit, stderr)
	}
	if stdout != `{"key":"machine_label","value":"m1"}`+"\n" {
		t.Fatalf("nau config set stdout = %q", stdout)
	}
	stdout, stderr, exit = runIdentity(t, herdrHermesBin, root, "config", "get", "machine_label")
	if exit != 0 {
		t.Fatalf("herdr-hermes config get exit = %d, stderr %q", exit, stderr)
	}
	if stdout != `{"key":"machine_label","value":"m1"}`+"\n" {
		t.Fatalf("herdr-hermes config get stdout = %q, want m1 written by nau", stdout)
	}
	// Vice versa.
	if _, stderr, exit := runIdentity(t, herdrHermesBin, root, "config", "set", "machine_label", "m2"); exit != 0 {
		t.Fatalf("herdr-hermes config set exit = %d, stderr %q", exit, stderr)
	}
	stdout, stderr, exit = runIdentity(t, nauBin, root, "config", "get", "machine_label")
	if exit != 0 {
		t.Fatalf("nau config get exit = %d, stderr %q", exit, stderr)
	}
	if stdout != `{"key":"machine_label","value":"m2"}`+"\n" {
		t.Fatalf("nau config get stdout = %q, want m2 written by herdr-hermes", stdout)
	}
}

// TestOnlyHerdrHermesStateDir: after both binaries ran, the config root
// contains the herdr-hermes directory and no nau directory.
func TestOnlyHerdrHermesStateDir(t *testing.T) {
	root := t.TempDir()
	if _, stderr, exit := runIdentity(t, nauBin, root, "config", "set", "machine_label", "m1"); exit != 0 {
		t.Fatalf("nau config set exit = %d, stderr %q", exit, stderr)
	}
	if _, stderr, exit := runIdentity(t, herdrHermesBin, root, "config", "get", "machine_label"); exit != 0 {
		t.Fatalf("herdr-hermes config get exit = %d, stderr %q", exit, stderr)
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
// next to the state dir is ignored by both names: they read only the
// herdr-hermes directory and leave the nau directory untouched.
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
	for _, tc := range []struct {
		bin  string
		name string
	}{
		{nauBin, "nau"},
		{herdrHermesBin, "herdr-hermes"},
	} {
		stdout, stderr, exit := runIdentity(t, tc.bin, root, "config", "get", "machine_label")
		if exit != 0 {
			t.Fatalf("%s config get exit = %d, stderr %q", tc.name, exit, stderr)
		}
		if stdout != `{"key":"machine_label","value":""}`+"\n" {
			t.Fatalf("%s config get stdout = %q, want the empty value (the nau dir is ignored)", tc.name, stdout)
		}
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

// TestHelpAndNoArgsUsage: help and no-args print the usage of the invoked
// program on stdout, exit 0, empty stderr; the deprecation line appears
// only right after the title under herdr-hermes, and the two usages are
// otherwise the same text under their names.
func TestHelpAndNoArgsUsage(t *testing.T) {
	const titleSuffix = " - universal messaging and registry core; this build bridges a remote job dispatcher and a Herdr node"
	const deprecation = "herdr-hermes is the deprecated name of nau: same commands, same state, same exit codes."
	var nauUsage, hhUsage string
	for _, tc := range []struct {
		bin       string
		name      string
		usageLine string
		depreca   bool
	}{
		{nauBin, "nau", "usage: nau <command> [args]", false},
		{herdrHermesBin, "herdr-hermes", "usage: herdr-hermes <command> [args]", true},
	} {
		for _, args := range [][]string{{"help"}, {}} {
			stdout, stderr, exit := runIdentity(t, tc.bin, t.TempDir(), args...)
			if exit != 0 {
				t.Fatalf("%s %v exit = %d, want 0, stderr %q", tc.name, args, exit, stderr)
			}
			if stderr != "" {
				t.Fatalf("%s %v stderr = %q, want empty", tc.name, args, stderr)
			}
			if !strings.HasPrefix(stdout, tc.name+titleSuffix+"\n") {
				t.Fatalf("%s %v stdout does not lead with its title line: %q", tc.name, args, firstNLines(stdout, 2))
			}
			hasDep := strings.Contains(stdout, "deprecated name of nau")
			if hasDep != tc.depreca {
				t.Errorf("%s %v deprecation line present = %v, want %v", tc.name, args, hasDep, tc.depreca)
			}
			if tc.depreca {
				if want := tc.name + titleSuffix + "\n" + deprecation + "\n"; !strings.HasPrefix(stdout, want) {
					t.Errorf("%s %v deprecation line is not right after the title: %q", tc.name, args, firstNLines(stdout, 3))
				}
			}
			if !strings.Contains(stdout, tc.usageLine+"\n") {
				t.Errorf("%s %v stdout lacks %q", tc.name, args, tc.usageLine)
			}
			if tc.depreca {
				hhUsage = stdout
			} else {
				nauUsage = stdout
			}
		}
	}
	// Same text otherwise: dropping the deprecation line and normalizing
	// the name makes the deprecated usage byte-identical to the primary
	// one.
	normalized := strings.ReplaceAll(hhUsage, deprecation+"\n", "")
	normalized = strings.ReplaceAll(normalized, "herdr-hermes", "nau")
	if normalized != nauUsage {
		t.Errorf("the two usages differ beyond the name and the deprecation line:\n--- nau ---\n%s--- herdr-hermes normalized ---\n%s", nauUsage, normalized)
	}
}

// TestBadUsageBothNames: bad usage under both names prints exactly one
// JSON error line on stdout, exits 2, and the usage on stderr carries the
// invoked program's name.
func TestBadUsageBothNames(t *testing.T) {
	for _, tc := range []struct {
		bin  string
		prog string
	}{
		{nauBin, "nau"},
		{herdrHermesBin, "herdr-hermes"},
	} {
		for _, args := range [][]string{
			{"frobnicate"},
			{"help", "extra"},
		} {
			stdout, stderr, exit := runIdentity(t, tc.bin, t.TempDir(), args...)
			if exit != 2 {
				t.Fatalf("%s %v exit = %d, want 2", tc.prog, args, exit)
			}
			body := strings.TrimSuffix(stdout, "\n")
			if !strings.HasSuffix(stdout, "\n") || strings.Count(body, "\n") != 0 {
				t.Fatalf("%s %v stdout is not exactly one line: %q", tc.prog, args, stdout)
			}
			var line struct {
				Status string `json:"status"`
				Motivo string `json:"motivo"`
			}
			if err := json.Unmarshal([]byte(body), &line); err != nil {
				t.Fatalf("%s %v stdout is not one JSON line: %v (%q)", tc.prog, args, err, stdout)
			}
			if line.Status != "2" {
				t.Fatalf("%s %v status = %q, want 2", tc.prog, args, line.Status)
			}
			if !strings.HasPrefix(stderr, tc.prog+" - ") || !strings.Contains(stderr, "usage: "+tc.prog+" <command> [args]") {
				t.Errorf("%s %v stderr does not carry the %s usage: %q", tc.prog, args, tc.prog, firstNLines(stderr, 3))
			}
		}
	}
}

// TestVersionIdenticalAcrossNames: the version output is byte-identical
// under both names (the version line carries no program name).
func TestVersionIdenticalAcrossNames(t *testing.T) {
	nauOut, _, nauExit := runIdentity(t, nauBin, t.TempDir(), "version")
	hhOut, _, hhExit := runIdentity(t, herdrHermesBin, t.TempDir(), "version")
	if nauExit != 0 || hhExit != 0 {
		t.Fatalf("version exit = %d/%d, want 0/0", nauExit, hhExit)
	}
	if !bytes.Equal([]byte(nauOut), []byte(hhOut)) {
		t.Fatalf("version outputs differ: %q vs %q", nauOut, hhOut)
	}
	var line map[string]string
	if err := json.Unmarshal([]byte(strings.TrimSuffix(nauOut, "\n")), &line); err != nil {
		t.Fatalf("version stdout is not JSON: %v (%q)", err, nauOut)
	}
	if line["version"] == "" {
		t.Errorf("version line lacks a version value: %q", nauOut)
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
