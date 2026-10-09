package soho_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/djalmajr/herdr-hermes/internal/soho"
	"github.com/djalmajr/herdr-hermes/internal/testutil/fakesoho"
)

// installFake installs the fake herdr-soho in a temp dir and returns the
// executable path and the fake dir (for the call log).
func installFake(t *testing.T, rules ...fakesoho.Rule) (exe, dir string) {
	t.Helper()
	dir = t.TempDir()
	return fakesoho.Install(t, dir, rules...), dir
}

var childEnv = []string{"PATH=/bin", "SOHO_TEST=1"}

// TestSohoRunExitCodes: the runner returns the child exit code unchanged.
func TestSohoRunExitCodes(t *testing.T) {
	var rules []fakesoho.Rule
	for _, code := range []int{0, 3, 7, 20, 22, 24} {
		id := "J" + strconv.Itoa(code)
		rules = append(rules, fakesoho.Rule{
			Argv:   []string{"job", "status", "--id", id},
			Code:   code,
			Stdout: strconv.Itoa(code) + "\n",
		})
	}
	exe, dir := installFake(t, rules...)
	r := soho.Runner{Bin: exe, Environ: childEnv}
	for _, code := range []int{0, 3, 7, 20, 22, 24} {
		var out bytes.Buffer
		id := "J" + strconv.Itoa(code)
		exit, err := r.Run(context.Background(), []string{"job", "status", "--id", id}, nil, &out, io.Discard, 10*time.Second)
		if err != nil {
			t.Fatalf("code %d: Run: %v", code, err)
		}
		if exit != code {
			t.Errorf("code %d: exit = %d", code, exit)
		}
		if out.String() != strconv.Itoa(code)+"\n" {
			t.Errorf("code %d: stdout = %q", code, out.String())
		}
	}
	calls, err := fakesoho.ReadCalls(dir)
	if err != nil {
		t.Fatalf("ReadCalls: %v", err)
	}
	if len(calls) != 6 {
		t.Fatalf("fake calls = %d, want 6", len(calls))
	}
}

// TestSohoRunStdioPassThrough: stdin, stdout and stderr are carried
// unchanged between the caller and the child.
func TestSohoRunStdioPassThrough(t *testing.T) {
	exe, dir := installFake(t, fakesoho.Rule{
		Argv:   []string{"job", "start", "--id", "J1", "--repo", "o/r"},
		Stdout: "child-out-σ",
		Stderr: "child-err-σ",
		Code:   5,
	})
	r := soho.Runner{Bin: exe, Environ: childEnv}
	var out, errb bytes.Buffer
	exit, err := r.Run(context.Background(),
		[]string{"job", "start", "--id", "J1", "--repo", "o/r"},
		bytes.NewBufferString("brief-body"), &out, &errb, 10*time.Second)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if exit != 5 {
		t.Errorf("exit = %d, want 5", exit)
	}
	if out.String() != "child-out-σ" {
		t.Errorf("stdout = %q", out.String())
	}
	if errb.String() != "child-err-σ" {
		t.Errorf("stderr = %q", errb.String())
	}
	calls, err := fakesoho.ReadCalls(dir)
	if err != nil {
		t.Fatalf("ReadCalls: %v", err)
	}
	if len(calls) != 1 {
		t.Fatalf("calls = %d, want 1", len(calls))
	}
	if calls[0].Stdin != "brief-body" {
		t.Errorf("child stdin = %q, want %q", calls[0].Stdin, "brief-body")
	}
}

// TestSohoRunChildEnv: the child sees exactly the environment the runner
// was given, nothing more, nothing less.
func TestSohoRunChildEnv(t *testing.T) {
	exe, dir := installFake(t, fakesoho.Rule{
		Argv: []string{"capabilities", "--json"}, Code: 0,
	})
	r := soho.Runner{Bin: exe, Environ: []string{"A=1", "B=two words"}}
	var out bytes.Buffer
	if _, err := r.Run(context.Background(), []string{"capabilities", "--json"}, nil, &out, io.Discard, 10*time.Second); err != nil {
		t.Fatalf("Run: %v", err)
	}
	calls, err := fakesoho.ReadCalls(dir)
	if err != nil {
		t.Fatalf("ReadCalls: %v", err)
	}
	if len(calls) != 1 {
		t.Fatalf("calls = %d, want 1", len(calls))
	}
	if len(calls[0].Env) != 2 || calls[0].Env[0] != "A=1" || calls[0].Env[1] != "B=two words" {
		t.Errorf("child env = %v, want [A=1 B=two words]", calls[0].Env)
	}
}

// TestSohoRunDeadline: a child that runs longer than the bound is killed
// within bound + WaitDelay and the runner reports ErrDeadline.
func TestSohoRunDeadline(t *testing.T) {
	exe, _ := installFake(t, fakesoho.Rule{
		Argv:  []string{"job", "wait", "--id", "J1"},
		Delay: 30000,
		Code:  0,
	})
	r := soho.Runner{Bin: exe, Environ: childEnv}
	start := time.Now()
	_, err := r.Run(context.Background(), []string{"job", "wait", "--id", "J1"}, nil, io.Discard, io.Discard, 200*time.Millisecond)
	elapsed := time.Since(start)
	var de *soho.ErrDeadline
	if !errors.As(err, &de) {
		t.Fatalf("Run: err = %v, want ErrDeadline", err)
	}
	if elapsed < 150*time.Millisecond {
		t.Errorf("killed after %s, before the bound (200ms)", elapsed)
	}
	if elapsed >= 8*time.Second {
		t.Errorf("killed after %s, beyond bound + WaitDelay (5.2s)", elapsed)
	}
}

// TestSohoRunNotFound: a missing executable is a typed ErrUnavailable that
// wraps the LookPath error.
func TestSohoRunNotFound(t *testing.T) {
	r := soho.Runner{Bin: "no-such-herdr-soho-binary", Environ: childEnv}
	_, err := r.Run(context.Background(), []string{"job", "status", "--id", "J1"}, nil, io.Discard, io.Discard, 10*time.Second)
	var ue *soho.ErrUnavailable
	if !errors.As(err, &ue) {
		t.Fatalf("Run: err = %v, want ErrUnavailable", err)
	}
	if !errors.Is(ue.Err, exec.ErrNotFound) {
		t.Errorf("cause = %v, want ErrNotFound", ue.Err)
	}
}

// TestSohoRunNotRunnable: an absolute path that exists but is not
// executable is reported as ErrUnavailable.
func TestSohoRunNotRunnable(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "herdr-soho")
	if err := os.WriteFile(bin, []byte("not executable\n"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	r := soho.Runner{Bin: bin, Environ: childEnv}
	_, err := r.Run(context.Background(), []string{"job", "status", "--id", "J1"}, nil, io.Discard, io.Discard, 10*time.Second)
	var ue *soho.ErrUnavailable
	if !errors.As(err, &ue) {
		t.Fatalf("Run: err = %v, want ErrUnavailable", err)
	}
}

// TestSohoCapabilities: the capabilities client runs
// `capabilities --json` and parses the line leniently.
func TestSohoCapabilities(t *testing.T) {
	exe, dir := installFake(t, fakesoho.Rule{
		Argv:   []string{"capabilities", "--json"},
		Stdout: "{\"schema\":1,\"worker_collaboration\":1,\"ephemeral_job\":1,\"job_events\":1}\n",
		Code:   0,
	})
	r := soho.Runner{Bin: exe, Environ: childEnv}
	caps, err := r.Capabilities(context.Background())
	if err != nil {
		t.Fatalf("Capabilities: %v", err)
	}
	want := "{\"schema\":1,\"worker_collaboration\":1,\"ephemeral_job\":1,\"job_events\":1}"
	if caps.Raw != want {
		t.Errorf("Raw = %q, want %q", caps.Raw, want)
	}
	if !caps.Has("ephemeral_job") || !caps.Has("job_events") {
		t.Errorf("caps %v: ephemeral_job and job_events must be present", caps.Raw)
	}
	if caps.Has("unknown_cap") {
		t.Errorf("caps %v: unknown capability reported present", caps.Raw)
	}
	calls, err := fakesoho.ReadCalls(dir)
	if err != nil {
		t.Fatalf("ReadCalls: %v", err)
	}
	if len(calls) != 1 || len(calls[0].Argv) != 2 || calls[0].Argv[0] != "capabilities" || calls[0].Argv[1] != "--json" {
		t.Errorf("argv = %v, want [capabilities --json]", calls)
	}
}

// TestSohoCapabilitiesNonJSON: a non-JSON line is an error, not a silent
// capability set.
func TestSohoCapabilitiesNonJSON(t *testing.T) {
	exe, _ := installFake(t, fakesoho.Rule{
		Argv:   []string{"capabilities", "--json"},
		Stdout: "not json\n",
		Code:   0,
	})
	r := soho.Runner{Bin: exe, Environ: childEnv}
	if _, err := r.Capabilities(context.Background()); err == nil {
		t.Error("Capabilities with non-JSON output: err = nil, want error")
	}
}

// TestSohoCapabilitiesNonZeroExit: a non-zero child exit is an error.
func TestSohoCapabilitiesNonZeroExit(t *testing.T) {
	exe, _ := installFake(t, fakesoho.Rule{
		Argv:   []string{"capabilities", "--json"},
		Stdout: "{}\n",
		Code:   1,
	})
	r := soho.Runner{Bin: exe, Environ: childEnv}
	if _, err := r.Capabilities(context.Background()); err == nil {
		t.Error("Capabilities exit 1: err = nil, want error")
	}
}

// TestSohoEvents: the events reader runs `job events --id <id> --since
// <n>` without --wait and returns the raw lines plus the parsed trailer.
func TestSohoEvents(t *testing.T) {
	const stdout = `{"seq":4,"ts":"2026-01-01T00:00:04-03:00","tipo":"commit","resumo":"c4"}
{"seq":5,"ts":"2026-01-01T00:00:05-03:00","tipo":"push","resumo":"p5"}
{"eventos":"fim","ultimo_seq":5,"estado":"running"}
`
	exe, dir := installFake(t, fakesoho.Rule{
		Argv:   []string{"job", "events", "--id", "J1", "--since", "3"},
		Stdout: stdout,
		Code:   0,
	})
	r := soho.Runner{Bin: exe, Environ: childEnv}
	res, err := r.Events(context.Background(), "J1", 3)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	if len(res.Lines) != 2 {
		t.Fatalf("lines = %d, want 2: %v", len(res.Lines), res.Lines)
	}
	if res.Lines[0] != `{"seq":4,"ts":"2026-01-01T00:00:04-03:00","tipo":"commit","resumo":"c4"}` {
		t.Errorf("line 1 = %q", res.Lines[0])
	}
	if res.Trailer.Eventos != "fim" || res.Trailer.UltimoSeq != 5 || res.Trailer.Estado != "running" {
		t.Errorf("trailer = %+v", res.Trailer)
	}
	calls, err := fakesoho.ReadCalls(dir)
	if err != nil {
		t.Fatalf("ReadCalls: %v", err)
	}
	if len(calls) != 1 {
		t.Fatalf("calls = %d, want 1", len(calls))
	}
	want := []string{"job", "events", "--id", "J1", "--since", "3"}
	if len(calls[0].Argv) != len(want) {
		t.Fatalf("argv = %v, want %v", calls[0].Argv, want)
	}
	for i := range want {
		if calls[0].Argv[i] != want[i] {
			t.Fatalf("argv = %v, want %v", calls[0].Argv, want)
		}
	}
	for _, a := range calls[0].Argv {
		if a == "--wait" {
			t.Errorf("argv carries --wait: %v", calls[0].Argv)
		}
	}
}

// TestSohoEventsNoNew: a --since beyond the last event yields only the
// trailer, no lines.
func TestSohoEventsNoNew(t *testing.T) {
	exe, _ := installFake(t, fakesoho.Rule{
		Argv:   []string{"job", "events", "--id", "J1", "--since", "5"},
		Stdout: `{"eventos":"fim","ultimo_seq":5,"estado":"done"}` + "\n",
		Code:   0,
	})
	r := soho.Runner{Bin: exe, Environ: childEnv}
	res, err := r.Events(context.Background(), "J1", 5)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	if len(res.Lines) != 0 || res.Trailer.UltimoSeq != 5 || res.Trailer.Estado != "done" {
		t.Errorf("res = %+v", res)
	}
}

// TestSohoEventsNoTrailer: output without the fim trailer line is an
// error.
func TestSohoEventsNoTrailer(t *testing.T) {
	exe, _ := installFake(t, fakesoho.Rule{
		Argv:   []string{"job", "events", "--id", "J1", "--since", "0"},
		Stdout: `{"seq":1,"ts":"2026-01-01T00:00:01-03:00","tipo":"accepted","resumo":"a1"}` + "\n",
		Code:   0,
	})
	r := soho.Runner{Bin: exe, Environ: childEnv}
	if _, err := r.Events(context.Background(), "J1", 0); err == nil {
		t.Error("Events without trailer: err = nil, want error")
	}
}

// TestSohoEventsNonZeroExit: a non-zero child exit is an error.
func TestSohoEventsNonZeroExit(t *testing.T) {
	exe, _ := installFake(t, fakesoho.Rule{
		Argv:   []string{"job", "events", "--id", "X", "--since", "0"},
		Stdout: `{"eventos":"fim","ultimo_seq":1,"estado":"done"}` + "\n",
		Code:   3,
	})
	r := soho.Runner{Bin: exe, Environ: childEnv}
	if _, err := r.Events(context.Background(), "X", 0); err == nil {
		t.Error("Events exit 3: err = nil, want error")
	}
}
