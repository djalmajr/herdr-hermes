package router_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/djalmajr/herdr-hermes/internal/router"
	"github.com/djalmajr/herdr-hermes/internal/testutil/fakesoho"
)

// installFake installs the fake herdr executable in a temp dir and
// returns the executable path and the fake dir (for the call log).
func installFake(t *testing.T, rules ...fakesoho.Rule) (exe, dir string) {
	t.Helper()
	dir = t.TempDir()
	return fakesoho.Install(t, dir, rules...), dir
}

// TestExecPassThrough: the child's stdout and exit code come back
// unchanged, err nil, also for non-zero exits.
func TestExecPassThrough(t *testing.T) {
	exe, _ := installFake(t,
		fakesoho.Rule{Argv: []string{"agent", "list"}, Code: 0, Stdout: "line1\nline2\n"},
		fakesoho.Rule{Argv: []string{"machine", "list", "--json"}, Code: 7, Stdout: "boom\n"},
	)
	exec := router.NewExec(exe, []string{"PATH=/bin"})

	out, code, err := exec(context.Background(), []string{"agent", "list"})
	if err != nil {
		t.Fatalf("exec: %v", err)
	}
	if string(out) != "line1\nline2\n" || code != 0 {
		t.Errorf("got (%q, %d), want (line1/line2, 0)", out, code)
	}

	out, code, err = exec(context.Background(), []string{"machine", "list", "--json"})
	if err != nil {
		t.Fatalf("exec: %v", err)
	}
	if string(out) != "boom\n" || code != 7 {
		t.Errorf("got (%q, %d), want (boom, 7)", out, code)
	}
}

// TestExecArgvLogged: the child sees the argv exactly, a label with dots
// and dashes as its own element after --machine.
func TestExecArgvLogged(t *testing.T) {
	exe, dir := installFake(t,
		fakesoho.Rule{Argv: []string{"--machine", "win.a_1-2", "agent", "list"}, Code: 0},
		fakesoho.Rule{Argv: []string{"machine", "list", "--json"}, Code: 0},
	)
	exec := router.NewExec(exe, []string{"PATH=/bin"})
	for _, argv := range [][]string{
		{"--machine", "win.a_1-2", "agent", "list"},
		{"machine", "list", "--json"},
	} {
		if _, _, err := exec(context.Background(), argv); err != nil {
			t.Fatalf("exec %v: %v", argv, err)
		}
	}
	calls, err := fakesoho.ReadCalls(dir)
	if err != nil {
		t.Fatalf("ReadCalls: %v", err)
	}
	if len(calls) != 2 {
		t.Fatalf("calls = %d, want 2", len(calls))
	}
	if !equalArgv(calls[0].Argv, []string{"--machine", "win.a_1-2", "agent", "list"}) {
		t.Errorf("call 0 argv = %v, want the --machine argv as separate elements", calls[0].Argv)
	}
	if !equalArgv(calls[1].Argv, []string{"machine", "list", "--json"}) {
		t.Errorf("call 1 argv = %v, want the catalog argv", calls[1].Argv)
	}
}

// TestExecChildEnvExact: the child environment is exactly the given
// environ, nothing added, nothing removed.
func TestExecChildEnvExact(t *testing.T) {
	exe, dir := installFake(t, fakesoho.Rule{Argv: []string{"agent", "list"}, Code: 0})
	environ := []string{"PATH=" + os.Getenv("PATH"), "HERDR_HERMES_NOWRITE=1"}
	exec := router.NewExec(exe, environ)
	if _, _, err := exec(context.Background(), []string{"agent", "list"}); err != nil {
		t.Fatalf("exec: %v", err)
	}
	calls, err := fakesoho.ReadCalls(dir)
	if err != nil {
		t.Fatalf("ReadCalls: %v", err)
	}
	if len(calls) != 1 {
		t.Fatalf("calls = %d, want 1", len(calls))
	}
	env := calls[0].Env
	if runtime.GOOS == "windows" {
		// The Go runtime adds SYSTEMROOT to a child environment on
		// Windows when it is missing: the standard library's behavior,
		// not a leak.
		var kept []string
		for _, kv := range env {
			if !strings.HasPrefix(kv, "SYSTEMROOT=") {
				kept = append(kept, kv)
			}
		}
		env = kept
	}
	if len(env) != 2 || env[0] != environ[0] || env[1] != environ[1] {
		t.Errorf("child env = %v, want exactly %v", env, environ)
	}
}

// TestExecStdinEmpty: the child's stdin is empty, never the parent's.
func TestExecStdinEmpty(t *testing.T) {
	exe, dir := installFake(t, fakesoho.Rule{Argv: []string{"agent", "list"}, Code: 0})
	exec := router.NewExec(exe, []string{"PATH=/bin"})
	if _, _, err := exec(context.Background(), []string{"agent", "list"}); err != nil {
		t.Fatalf("exec: %v", err)
	}
	calls, err := fakesoho.ReadCalls(dir)
	if err != nil {
		t.Fatalf("ReadCalls: %v", err)
	}
	if len(calls) != 1 {
		t.Fatalf("calls = %d, want 1", len(calls))
	}
	if calls[0].Stdin != "" {
		t.Errorf("child stdin = %q, want empty", calls[0].Stdin)
	}
}

// TestExecDeadline: a child that outlives the context deadline is an
// error wrapping ErrProbeTimeout, in well under 10 s.
func TestExecDeadline(t *testing.T) {
	exe, _ := installFake(t, fakesoho.Rule{
		Argv:       []string{"agent", "list"},
		Delay:      10000,
		ExitOnTerm: 143,
		Code:       0,
	})
	exec := router.NewExec(exe, []string{"PATH=/bin"})
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, _, err := exec(ctx, []string{"agent", "list"})
	elapsed := time.Since(start)
	if err == nil {
		t.Fatalf("exec = nil error, want an error")
	}
	if !errors.Is(err, router.ErrProbeTimeout) {
		t.Fatalf("err = %v, want an error wrapping ErrProbeTimeout", err)
	}
	if elapsed >= 10*time.Second {
		t.Errorf("elapsed = %v, want well under 10 s", elapsed)
	}
	t.Logf("deadline probe took %v", elapsed)
}

// TestExecMissingBinary: a missing executable is an error wrapping
// ErrHerdrUnavailable.
func TestExecMissingBinary(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "no-such-herdr")
	exec := router.NewExec(bin, []string{"PATH=/bin"})
	_, _, err := exec(context.Background(), []string{"agent", "list"})
	if err == nil {
		t.Fatalf("exec = nil error, want an error")
	}
	if !errors.Is(err, router.ErrHerdrUnavailable) {
		t.Fatalf("err = %v, want an error wrapping ErrHerdrUnavailable", err)
	}
}

// TestExecOversizeCapped: stdout longer than MaxProbeOutput is capped at
// MaxProbeOutput+1 bytes; the run itself succeeds.
func TestExecOversizeCapped(t *testing.T) {
	exe, _ := installFake(t, fakesoho.Rule{
		Argv:        []string{"agent", "list"},
		StdoutBytes: make([]byte, router.MaxProbeOutput+100),
		Code:        0,
	})
	exec := router.NewExec(exe, []string{"PATH=/bin"})
	out, code, err := exec(context.Background(), []string{"agent", "list"})
	if err != nil {
		t.Fatalf("exec: %v", err)
	}
	if len(out) != router.MaxProbeOutput+1 {
		t.Errorf("stdout length = %d, want %d", len(out), router.MaxProbeOutput+1)
	}
	if code != 0 {
		t.Errorf("exit code = %d, want 0", code)
	}
}
