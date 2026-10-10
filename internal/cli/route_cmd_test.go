package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/djalmajr/herdr-hermes/internal/testutil/fakesoho"
)

// routeCatalogWinA is the `machine list --json` fixture with win-a enabled.
const routeCatalogWinA = `[{"id":"a1","label":"win-a","target":"t","session":"default","enabled":true,"selected":false}]`

// routeAgentList is the `agent list` fixture: n orchestrators plus one
// build worker that must never be counted.
func routeAgentList(n int) string {
	var parts []string
	for i := 0; i < n; i++ {
		parts = append(parts, fmt.Sprintf(`{"pane_id":"w1:p%d","agent_status":"working","name":"orchestrator"}`, i+1))
	}
	parts = append(parts, `{"pane_id":"w1:p9","agent_status":"idle","name":"build"}`)
	return `{"id":"cli:agent:list","result":{"agents":[` + strings.Join(parts, ",") + `]}}`
}

// routeCandidate is one candidate of the route result JSON.
type routeCandidate struct {
	Machine       string `json:"maquina"`
	State         string `json:"estado"`
	Orchestrators *int   `json:"orquestradores"`
	Reason        string `json:"motivo"`
}

// routeResult is the route success JSON line (router.Result field order).
type routeResult struct {
	Machine       string           `json:"maquina"`
	Motivo        string           `json:"motivo"`
	Requested     string           `json:"solicitada"`
	Orchestrators int              `json:"orquestradores"`
	Candidates    []routeCandidate `json:"candidatos"`
}

func parseRouteResult(t *testing.T, stdout string) routeResult {
	t.Helper()
	var res routeResult
	if err := json.Unmarshal([]byte(strings.TrimSuffix(stdout, "\n")), &res); err != nil {
		t.Fatalf("parse route stdout %q: %v", stdout, err)
	}
	return res
}

// routeUnavailable is the route exit-4 JSON line.
type routeUnavailable struct {
	Status     string           `json:"status"`
	Motivo     string           `json:"motivo"`
	Candidates []routeCandidate `json:"candidatos"`
}

func parseRouteUnavailable(t *testing.T, stdout string) routeUnavailable {
	t.Helper()
	var res routeUnavailable
	if err := json.Unmarshal([]byte(strings.TrimSuffix(stdout, "\n")), &res); err != nil {
		t.Fatalf("parse route stdout %q: %v", stdout, err)
	}
	return res
}

// writeRouteConfig writes the herdr-hermes config file with the given
// key=value pairs; Load fills the defaults for the absent keys.
func writeRouteConfig(t *testing.T, dir string, kv map[string]string) {
	t.Helper()
	var b strings.Builder
	for k, v := range kv {
		b.WriteString(k)
		b.WriteString("=")
		b.WriteString(v)
		b.WriteString("\n")
	}
	if err := os.WriteFile(filepath.Join(dir, "config"), []byte(b.String()), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
}

// routeCalls returns the argvs logged by the fake in dir (nil when the
// fake was never called).
func routeCalls(t *testing.T, dir string) [][]string {
	t.Helper()
	calls, err := fakesoho.ReadCalls(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatalf("ReadCalls: %v", err)
	}
	var argvs [][]string
	for _, c := range calls {
		argvs = append(argvs, c.Argv)
	}
	return argvs
}

// equalStringSlice reports whether two string slices are element-equal.
func equalStringSlice(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// runRouteEnv runs Run with a hermetic env whose child environment is
// exactly childEnv, like the notify flow tests do.
func runRouteEnv(t *testing.T, dir string, childEnv []string, args ...string) (string, string, int) {
	t.Helper()
	var out, errb bytes.Buffer
	env := Env{
		Stdin:     strings.NewReader(""),
		Stdout:    &out,
		Stderr:    &errb,
		Getenv:    getenvFor(false),
		Environ:   func() []string { return childEnv },
		ConfigDir: dir,
		Now:       func() time.Time { return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC) },
		Sleep:     sleepCtx,
	}
	exit := Run(args, env)
	return out.String(), errb.String(), exit
}

// TestRouteLeastLoad: the available machine with the fewest orchestrators
// wins; the build worker is never counted.
func TestRouteLeastLoad(t *testing.T) {
	fakeDir := t.TempDir()
	exe := fakesoho.Install(t, fakeDir,
		fakesoho.Rule{Argv: []string{"machine", "list", "--json"}, Code: 0, Stdout: routeCatalogWinA},
		fakesoho.Rule{Argv: []string{"agent", "list"}, Code: 0, Stdout: routeAgentList(2)},
		fakesoho.Rule{Argv: []string{"--machine", "win-a", "agent", "list"}, Code: 0, Stdout: routeAgentList(1)},
	)
	dir := t.TempDir()
	writeRouteConfig(t, dir, map[string]string{
		"herdr_bin":      exe,
		"route_machines": "local,win-a",
	})
	stdout, stderr, exit := runCLI(t, dir, false, "route")
	if exit != 0 {
		t.Fatalf("route exit = %d, stderr %q", exit, stderr)
	}
	want := `{"maquina":"win-a","motivo":"least_load","orquestradores":1,"candidatos":[{"maquina":"local","estado":"available","orquestradores":2},{"maquina":"win-a","estado":"available","orquestradores":1}]}`
	if stdout != want+"\n" {
		t.Errorf("stdout = %q, want %q", stdout, want)
	}
}

// TestRouteTieFirstConfigured: on a tie the first machine in the
// configured order wins.
func TestRouteTieFirstConfigured(t *testing.T) {
	fakeDir := t.TempDir()
	exe := fakesoho.Install(t, fakeDir,
		fakesoho.Rule{Argv: []string{"machine", "list", "--json"}, Code: 0, Stdout: `[{"id":"a1","label":"mac-a","target":"t","session":"default","enabled":true,"selected":false},{"id":"a2","label":"win-a","target":"t","session":"default","enabled":true,"selected":false}]`},
		fakesoho.Rule{Argv: []string{"--machine", "mac-a", "agent", "list"}, Code: 0, Stdout: routeAgentList(1)},
		fakesoho.Rule{Argv: []string{"--machine", "win-a", "agent", "list"}, Code: 0, Stdout: routeAgentList(1)},
	)
	dir := t.TempDir()
	writeRouteConfig(t, dir, map[string]string{
		"herdr_bin":      exe,
		"route_machines": "mac-a,win-a",
	})
	stdout, stderr, exit := runCLI(t, dir, false, "route")
	if exit != 0 {
		t.Fatalf("route exit = %d, stderr %q", exit, stderr)
	}
	res := parseRouteResult(t, stdout)
	if res.Machine != "mac-a" || res.Motivo != "least_load" || res.Orchestrators != 1 {
		t.Errorf("result = %+v, want mac-a least_load 1", res)
	}
	if len(res.Candidates) != 2 || res.Candidates[0].Machine != "mac-a" || res.Candidates[1].Machine != "win-a" {
		t.Errorf("candidates = %+v, want the configured order", res.Candidates)
	}
}

// TestRouteRequestedWins: an available requested machine wins despite
// greater load, and the JSON carries solicitada.
func TestRouteRequestedWins(t *testing.T) {
	fakeDir := t.TempDir()
	exe := fakesoho.Install(t, fakeDir,
		fakesoho.Rule{Argv: []string{"machine", "list", "--json"}, Code: 0, Stdout: routeCatalogWinA},
		fakesoho.Rule{Argv: []string{"agent", "list"}, Code: 0, Stdout: routeAgentList(1)},
		fakesoho.Rule{Argv: []string{"--machine", "win-a", "agent", "list"}, Code: 0, Stdout: routeAgentList(5)},
	)
	dir := t.TempDir()
	writeRouteConfig(t, dir, map[string]string{
		"herdr_bin":      exe,
		"route_machines": "local,win-a",
	})
	stdout, stderr, exit := runCLI(t, dir, false, "route", "--machine", "win-a")
	if exit != 0 {
		t.Fatalf("route exit = %d, stderr %q", exit, stderr)
	}
	res := parseRouteResult(t, stdout)
	if res.Machine != "win-a" || res.Motivo != "requested" || res.Requested != "win-a" || res.Orchestrators != 5 {
		t.Errorf("result = %+v, want win-a requested 5", res)
	}
}

// TestRouteRequestedOfflineFallback: a requested machine whose agent list
// exits non-zero is unavailable/probe_failed and the least loaded
// available machine is chosen with motivo fallback.
func TestRouteRequestedOfflineFallback(t *testing.T) {
	fakeDir := t.TempDir()
	exe := fakesoho.Install(t, fakeDir,
		fakesoho.Rule{Argv: []string{"machine", "list", "--json"}, Code: 0, Stdout: routeCatalogWinA},
		fakesoho.Rule{Argv: []string{"agent", "list"}, Code: 0, Stdout: routeAgentList(1)},
		fakesoho.Rule{Argv: []string{"--machine", "win-a", "agent", "list"}, Code: 1},
	)
	dir := t.TempDir()
	writeRouteConfig(t, dir, map[string]string{
		"herdr_bin":      exe,
		"route_machines": "local,win-a",
	})
	stdout, stderr, exit := runCLI(t, dir, false, "route", "--machine", "win-a")
	if exit != 0 {
		t.Fatalf("route exit = %d, stderr %q", exit, stderr)
	}
	res := parseRouteResult(t, stdout)
	if res.Machine != "local" || res.Motivo != "fallback" || res.Requested != "win-a" || res.Orchestrators != 1 {
		t.Errorf("result = %+v, want local fallback 1 with solicitada win-a", res)
	}
	if len(res.Candidates) != 2 {
		t.Fatalf("candidates = %+v, want 2", res.Candidates)
	}
	winA := res.Candidates[1]
	if winA.Machine != "win-a" || winA.State != "unavailable" || winA.Reason != "probe_failed" || winA.Orchestrators != nil {
		t.Errorf("win-a candidate = %+v, want unavailable probe_failed with no count", winA)
	}
}

// TestRouteRequestedNotConfigured: a requested label outside
// route_machines exits 2 without any herdr call.
func TestRouteRequestedNotConfigured(t *testing.T) {
	fakeDir := t.TempDir()
	exe := fakesoho.Install(t, fakeDir,
		fakesoho.Rule{Argv: []string{"machine", "list", "--json"}, Code: 0, Stdout: routeCatalogWinA},
	)
	dir := t.TempDir()
	writeRouteConfig(t, dir, map[string]string{
		"herdr_bin":      exe,
		"route_machines": "local,win-a",
	})
	stdout, stderr, exit := runCLI(t, dir, false, "route", "--machine", "ghost")
	if exit != 2 {
		t.Fatalf("route exit = %d, want 2 (stdout %q, stderr %q)", exit, stdout, stderr)
	}
	if !strings.Contains(stdout, `"status":"2"`) || !strings.Contains(stdout, "route: requested machine is not configured") {
		t.Errorf("stdout = %q", stdout)
	}
	if got := routeCalls(t, fakeDir); len(got) != 0 {
		t.Errorf("calls = %v, want none", got)
	}
}

// routeUsageText and routeHelpText are the exact route-scoped texts the
// command must print on stderr (they pin routeUsage and routeHelp in
// route_cmd.go to the DJA-240 wording).
const routeUsageText = "usage: herdr-hermes route [--machine <label>]\nrun 'herdr-hermes route --help' for the route options\n"

const routeHelpText = `usage: herdr-hermes route [--machine <label>]

Choose the machine for a new job (dispatcher side, read-only). Prints one
JSON line on stdout; diagnostics go to stderr.

options:
  --machine <label>   prefer this configured machine; the label is a separate
                      argument (the --machine=<label> form is not accepted)

configuration (herdr-hermes config set <key> <value>):
  route_machines, route_disabled, route_orchestrator_name,
  route_probe_timeout_s, herdr_bin

exit codes:
  0   a machine was chosen
  2   bad usage, invalid configuration, route_machines not configured or
      the requested machine is not configured
  4   no eligible machine is available
`

// The exact stdout error line per detail class. json.Marshal escapes the
// < and > characters, so the literals carry the \u003c and \u003e
// sequences as bytes.
const (
	routeOutHelp        = `{"status":"2","motivo":"route: help requested; usage: route [--machine \u003clabel\u003e]"}`
	routeOutEquals      = `{"status":"2","motivo":"route: the --machine=\u003clabel\u003e form is not accepted; pass the label as a separate argument: --machine \u003clabel\u003e; usage: route [--machine \u003clabel\u003e]"}`
	routeOutNeedsLabel  = `{"status":"2","motivo":"route: --machine needs a machine label; usage: route [--machine \u003clabel\u003e]"}`
	routeOutRepeated    = `{"status":"2","motivo":"route: --machine is given more than once; usage: route [--machine \u003clabel\u003e]"}`
	routeOutEmpty       = `{"status":"2","motivo":"route: the machine label is empty; usage: route [--machine \u003clabel\u003e]"}`
	routeOutFlagLike    = `{"status":"2","motivo":"route: the --machine value looks like a flag; a machine label starts with a letter or digit; usage: route [--machine \u003clabel\u003e]"}`
	routeOutTooLong     = `{"status":"2","motivo":"route: the machine label is longer than 64 characters; usage: route [--machine \u003clabel\u003e]"}`
	routeOutInvalid     = `{"status":"2","motivo":"route: invalid machine label: use a letter or digit, then up to 63 letters, digits, '.', '_' or '-'; usage: route [--machine \u003clabel\u003e]"}`
	routeOutUnknownFlag = `{"status":"2","motivo":"route: unknown flag; usage: route [--machine \u003clabel\u003e]"}`
	routeOutUnexpected  = `{"status":"2","motivo":"route: unexpected argument; usage: route [--machine \u003clabel\u003e]"}`
)

// TestRouteBadUsage: every malformed argument class exits 2 with its own
// exact motivo on stdout and the route-scoped diagnostic on stderr (the
// global usage is never printed), and never calls herdr.
func TestRouteBadUsage(t *testing.T) {
	fakeDir := t.TempDir()
	exe := fakesoho.Install(t, fakeDir,
		fakesoho.Rule{Argv: []string{"machine", "list", "--json"}, Code: 0, Stdout: routeCatalogWinA},
	)
	dir := t.TempDir()
	writeRouteConfig(t, dir, map[string]string{
		"herdr_bin":      exe,
		"route_machines": "local,win-a",
	})
	cases := []struct {
		name    string
		args    []string
		detail  string
		wantOut string
	}{
		{"equals form", []string{"--machine=local"}, "the --machine=<label> form is not accepted; pass the label as a separate argument: --machine <label>", routeOutEquals},
		{"equals form empty", []string{"--machine="}, "the --machine=<label> form is not accepted; pass the label as a separate argument: --machine <label>", routeOutEquals},
		{"missing value", []string{"--machine"}, "--machine needs a machine label", routeOutNeedsLabel},
		{"repeated flag", []string{"--machine", "local", "--machine", "win-a"}, "--machine is given more than once", routeOutRepeated},
		{"empty value", []string{"--machine", ""}, "the machine label is empty", routeOutEmpty},
		{"flag-like value", []string{"--machine", "-x"}, "the --machine value looks like a flag; a machine label starts with a letter or digit", routeOutFlagLike},
		{"flag-like value --help", []string{"--machine", "--help"}, "the --machine value looks like a flag; a machine label starts with a letter or digit", routeOutFlagLike},
		{"flag-like value -h", []string{"--machine", "-h"}, "the --machine value looks like a flag; a machine label starts with a letter or digit", routeOutFlagLike},
		{"too long value", []string{"--machine", strings.Repeat("a", 65)}, "the machine label is longer than 64 characters", routeOutTooLong},
		{"invalid label space", []string{"--machine", "bad label"}, "invalid machine label: use a letter or digit, then up to 63 letters, digits, '.', '_' or '-'", routeOutInvalid},
		{"invalid label leading dot", []string{"--machine", ".x"}, "invalid machine label: use a letter or digit, then up to 63 letters, digits, '.', '_' or '-'", routeOutInvalid},
		{"invalid label slash", []string{"--machine", "a/b"}, "invalid machine label: use a letter or digit, then up to 63 letters, digits, '.', '_' or '-'", routeOutInvalid},
		{"invalid label non-ascii", []string{"--machine", "é"}, "invalid machine label: use a letter or digit, then up to 63 letters, digits, '.', '_' or '-'", routeOutInvalid},
		{"unknown flag long", []string{"--bogus"}, "unknown flag", routeOutUnknownFlag},
		{"unknown flag short", []string{"-x"}, "unknown flag", routeOutUnknownFlag},
		{"unknown flag --", []string{"--"}, "unknown flag", routeOutUnknownFlag},
		{"unexpected argument", []string{"extra"}, "unexpected argument", routeOutUnexpected},
		{"unexpected bare label", []string{"local"}, "unexpected argument", routeOutUnexpected},
		{"unexpected after valid flag", []string{"--machine", "local", "extra"}, "unexpected argument", routeOutUnexpected},
		{"unexpected before flag", []string{"extra", "--machine", "local"}, "unexpected argument", routeOutUnexpected},
		{"help after unknown flag", []string{"--bogus", "--help"}, "help requested", routeOutHelp},
		{"short help", []string{"-h"}, "help requested", routeOutHelp},
		{"long help", []string{"--help"}, "help requested", routeOutHelp},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wantErr := "herdr-hermes: route: " + tc.detail + "\n" + routeUsageText
			if tc.detail == "help requested" {
				wantErr = routeHelpText
			}
			stdout, stderr, exit := runCLI(t, dir, false, append([]string{"route"}, tc.args...)...)
			if exit != 2 {
				t.Fatalf("route %v exit = %d, want 2", tc.args, exit)
			}
			if stdout != tc.wantOut+"\n" {
				t.Errorf("stdout = %q, want exactly %q", stdout, tc.wantOut+"\n")
			}
			if stderr != wantErr {
				t.Errorf("stderr = %q, want exactly %q", stderr, wantErr)
			}
			if strings.Contains(stderr, "commands:") {
				t.Errorf("stderr contains the global usage: %q", stderr)
			}
		})
	}
	if got := routeCalls(t, fakeDir); len(got) != 0 {
		t.Errorf("calls = %v, want none", got)
	}
}

// TestRouteNoEchoOfOffendingValue: an argument value that looks like a
// secret or carries control bytes never appears on stdout or stderr,
// whatever the error is: the diagnostics are fixed detail text and the
// JSON line is one.
func TestRouteNoEchoOfOffendingValue(t *testing.T) {
	secret := "sk-test-SECRET-0123456789"
	longValue := strings.Repeat(secret, 8) // 200 bytes
	escapeValue := "\n\x1b[31m"
	cases := [][]string{
		{"--machine", secret},
		{"--machine=" + secret},
		{"--sk-test-SECRET-flag"},
		{"--machine", longValue},
		{"--machine", escapeValue},
	}
	fakeDir := t.TempDir()
	exe := fakesoho.Install(t, fakeDir,
		fakesoho.Rule{Argv: []string{"machine", "list", "--json"}, Code: 0, Stdout: routeCatalogWinA},
	)
	dir := t.TempDir()
	writeRouteConfig(t, dir, map[string]string{
		"herdr_bin":      exe,
		"route_machines": "local,win-a",
	})
	for _, args := range cases {
		stdout, stderr, exit := runCLI(t, dir, false, append([]string{"route"}, args...)...)
		if exit != 2 {
			t.Errorf("route %v exit = %d, want 2", args, exit)
			continue
		}
		if strings.Count(stdout, "\n") != 1 {
			t.Errorf("route %v stdout = %q, want exactly one line", args, stdout)
		}
		combined := stdout + stderr
		if strings.Contains(combined, secret) {
			t.Errorf("route %v: the secret appears on stdout or stderr: %q", args, combined)
		}
		if strings.Contains(combined, "\x1b") {
			t.Errorf("route %v: an escape byte appears on stdout or stderr", args)
		}
		for _, value := range args {
			if value == "--machine" {
				continue
			}
			if strings.Contains(stdout, value) || strings.Contains(stderr, value) {
				t.Errorf("route %v: the raw argument %q is echoed (stdout %q, stderr %q)", args, value, stdout, stderr)
			}
		}
	}
	if got := routeCalls(t, fakeDir); len(got) != 0 {
		t.Errorf("calls = %v, want none", got)
	}
}

// TestRouteNoConfigReadBeforeValidation: a usage problem is decided
// before the config file is read: with an invalid config file the
// malformed route invocations still give the usage motivo, not the
// config error; the valid syntax with the same file gives the config
// error (control that the file is really invalid).
func TestRouteNoConfigReadBeforeValidation(t *testing.T) {
	fakeDir := t.TempDir()
	exe := fakesoho.Install(t, fakeDir,
		fakesoho.Rule{Argv: []string{"machine", "list", "--json"}, Code: 0, Stdout: routeCatalogWinA},
	)
	dir := t.TempDir()
	writeRouteConfig(t, dir, map[string]string{
		"herdr_bin":             exe,
		"route_probe_timeout_s": "0",
	})
	for _, args := range [][]string{
		{"route", "--machine=local"},
		{"route", "--bogus"},
	} {
		stdout, stderr, exit := runCLI(t, dir, false, args...)
		if exit != 2 {
			t.Errorf("%v exit = %d, want 2", args, exit)
			continue
		}
		if strings.Contains(stdout, "config:") {
			t.Errorf("%v stdout = %q, want the usage motivo, not the config error", args, stdout)
		}
		if !strings.Contains(stdout, `usage: route [--machine \u003clabel\u003e]`) {
			t.Errorf("%v stdout = %q, want the route usage in the motivo", args, stdout)
		}
		if strings.Contains(stderr, "config:") {
			t.Errorf("%v stderr = %q, want the route diagnostic, not the config error", args, stderr)
		}
	}
	stdout, _, exit := runCLI(t, dir, false, "route")
	if exit != 2 || !strings.Contains(stdout, "config: route_probe_timeout_s must be an integer from 1 to 120") {
		t.Errorf("route with invalid config: exit %d stdout %q, want the config error", exit, stdout)
	}
	if got := routeCalls(t, fakeDir); len(got) != 0 {
		t.Errorf("calls = %v, want none", got)
	}
}

// TestRouteMachinesNotConfigured: an empty route_machines exits 2 without
// any herdr call.
func TestRouteMachinesNotConfigured(t *testing.T) {
	fakeDir := t.TempDir()
	exe := fakesoho.Install(t, fakeDir,
		fakesoho.Rule{Argv: []string{"machine", "list", "--json"}, Code: 0, Stdout: routeCatalogWinA},
	)
	dir := t.TempDir()
	writeRouteConfig(t, dir, map[string]string{"herdr_bin": exe})
	stdout, stderr, exit := runCLI(t, dir, false, "route")
	if exit != 2 {
		t.Fatalf("route exit = %d, want 2 (stdout %q, stderr %q)", exit, stdout, stderr)
	}
	if !strings.Contains(stdout, "route: route_machines is not configured") {
		t.Errorf("stdout = %q", stdout)
	}
	if got := routeCalls(t, fakeDir); len(got) != 0 {
		t.Errorf("calls = %v, want none", got)
	}
}

// TestRouteAllUnavailable: every machine unavailable (the catalog exits
// non-zero and local is not in the fleet) exits 4 with the unavailable
// JSON line and the diagnostic on stderr.
func TestRouteAllUnavailable(t *testing.T) {
	fakeDir := t.TempDir()
	exe := fakesoho.Install(t, fakeDir,
		fakesoho.Rule{Argv: []string{"machine", "list", "--json"}, Code: 1},
	)
	dir := t.TempDir()
	writeRouteConfig(t, dir, map[string]string{
		"herdr_bin":      exe,
		"route_machines": "mac-a,win-a",
	})
	stdout, stderr, exit := runCLI(t, dir, false, "route")
	if exit != 4 {
		t.Fatalf("route exit = %d, want 4 (stdout %q, stderr %q)", exit, stdout, stderr)
	}
	res := parseRouteUnavailable(t, stdout)
	if res.Status != "unavailable" || res.Motivo != "no eligible machine is available" {
		t.Errorf("result = %+v, want status unavailable with the motivo", res)
	}
	if len(res.Candidates) != 2 || res.Candidates[0].Machine != "mac-a" || res.Candidates[0].State != "unavailable" || res.Candidates[0].Reason != "catalog_unavailable" ||
		res.Candidates[1].Machine != "win-a" || res.Candidates[1].State != "unavailable" || res.Candidates[1].Reason != "catalog_unavailable" {
		t.Errorf("candidates = %+v, want both unavailable catalog_unavailable", res.Candidates)
	}
	// One diagnostic line per unavailable machine (DJA-238), then the
	// exit-4 line.
	wantErr := "herdr-hermes: route: mac-a unavailable: catalog_unavailable (herdr exit 1): herdr reported no recognized cause\n" +
		"herdr-hermes: route: win-a unavailable: catalog_unavailable (herdr exit 1): herdr reported no recognized cause\n" +
		"herdr-hermes: route: no eligible machine is available\n"
	if stderr != wantErr {
		t.Errorf("stderr = %q, want %q", stderr, wantErr)
	}
}

// TestRouteProbeTimeout: one machine whose agent list outlives the probe
// timeout is probe_timeout; the other is chosen, and the run ends at the
// probe bound, far before the fake delay. The bound is 5 s, not the 1 s
// minimum, so a slow start of the fake under -race on a loaded machine
// does not time out the fast probes too.
func TestRouteProbeTimeout(t *testing.T) {
	fakeDir := t.TempDir()
	exe := fakesoho.Install(t, fakeDir,
		fakesoho.Rule{Argv: []string{"machine", "list", "--json"}, Code: 0, Stdout: routeCatalogWinA},
		fakesoho.Rule{Argv: []string{"agent", "list"}, Code: 0, Stdout: routeAgentList(0)},
		fakesoho.Rule{Argv: []string{"--machine", "win-a", "agent", "list"}, Code: 0, Delay: 60000, ExitOnTerm: 143},
	)
	dir := t.TempDir()
	writeRouteConfig(t, dir, map[string]string{
		"herdr_bin":             exe,
		"route_machines":        "local,win-a",
		"route_probe_timeout_s": "5",
	})
	start := time.Now()
	stdout, stderr, exit := runCLI(t, dir, false, "route")
	elapsed := time.Since(start)
	if exit != 0 {
		t.Fatalf("route exit = %d, stderr %q", exit, stderr)
	}
	res := parseRouteResult(t, stdout)
	if res.Machine != "local" || res.Motivo != "least_load" || res.Orchestrators != 0 {
		t.Errorf("result = %+v, want local least_load 0", res)
	}
	if len(res.Candidates) != 2 || res.Candidates[1].Machine != "win-a" || res.Candidates[1].State != "unavailable" || res.Candidates[1].Reason != "probe_timeout" || res.Candidates[1].Orchestrators != nil {
		t.Errorf("win-a candidate = %+v, want unavailable probe_timeout with no count", res.Candidates)
	}
	if elapsed >= 30*time.Second {
		t.Errorf("elapsed = %v, want well under the 60 s fake delay", elapsed)
	}
	t.Logf("probe timeout test took %v", elapsed)
}

// TestRouteMalformedAgentList: a Herdr error object and invalid JSON on a
// zero-exit agent list are probe_malformed, never counted as zero.
func TestRouteMalformedAgentList(t *testing.T) {
	cases := []struct {
		name   string
		stdout string
	}{
		{"herdr error object", `{"id":"cli:agent:list","error":{"code":"server_not_running","message":"x"}}`},
		{"invalid json", `{not json`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fakeDir := t.TempDir()
			exe := fakesoho.Install(t, fakeDir,
				fakesoho.Rule{Argv: []string{"machine", "list", "--json"}, Code: 0, Stdout: routeCatalogWinA},
				fakesoho.Rule{Argv: []string{"agent", "list"}, Code: 0, Stdout: routeAgentList(0)},
				fakesoho.Rule{Argv: []string{"--machine", "win-a", "agent", "list"}, Code: 0, Stdout: tc.stdout},
			)
			dir := t.TempDir()
			writeRouteConfig(t, dir, map[string]string{
				"herdr_bin":      exe,
				"route_machines": "local,win-a",
			})
			stdout, stderr, exit := runCLI(t, dir, false, "route")
			if exit != 0 {
				t.Fatalf("route exit = %d, stderr %q", exit, stderr)
			}
			res := parseRouteResult(t, stdout)
			if res.Machine != "local" || res.Orchestrators != 0 {
				t.Errorf("result = %+v, want local 0", res)
			}
			winA := res.Candidates[1]
			if winA.Machine != "win-a" || winA.State != "unavailable" || winA.Reason != "probe_malformed" || winA.Orchestrators != nil {
				t.Errorf("win-a candidate = %+v, want unavailable probe_malformed with no count", winA)
			}
		})
	}
}

// TestRouteDisabledMachines: a route_disabled label and a catalog
// disabled saved machine are disabled, and neither gets an agent-list
// call.
func TestRouteDisabledMachines(t *testing.T) {
	fakeDir := t.TempDir()
	exe := fakesoho.Install(t, fakeDir,
		fakesoho.Rule{Argv: []string{"machine", "list", "--json"}, Code: 0, Stdout: `[{"id":"a1","label":"mac-a","target":"t","session":"default","enabled":true,"selected":false},{"id":"a2","label":"win-a","target":"t","session":"default","enabled":false,"selected":false}]`},
		fakesoho.Rule{Argv: []string{"agent", "list"}, Code: 0, Stdout: routeAgentList(0)},
	)
	dir := t.TempDir()
	writeRouteConfig(t, dir, map[string]string{
		"herdr_bin":      exe,
		"route_machines": "local,mac-a,win-a",
		"route_disabled": "mac-a",
	})
	stdout, stderr, exit := runCLI(t, dir, false, "route")
	if exit != 0 {
		t.Fatalf("route exit = %d, stderr %q", exit, stderr)
	}
	res := parseRouteResult(t, stdout)
	if res.Machine != "local" || res.Motivo != "least_load" {
		t.Errorf("result = %+v, want local least_load", res)
	}
	if len(res.Candidates) != 3 {
		t.Fatalf("candidates = %+v, want 3", res.Candidates)
	}
	macA, winA := res.Candidates[1], res.Candidates[2]
	if macA.State != "disabled" || macA.Reason != "disabled_by_config" {
		t.Errorf("mac-a candidate = %+v, want disabled disabled_by_config", macA)
	}
	if winA.State != "disabled" || winA.Reason != "machine_disabled" {
		t.Errorf("win-a candidate = %+v, want disabled machine_disabled", winA)
	}
	got := routeCalls(t, fakeDir)
	want := [][]string{{"machine", "list", "--json"}, {"agent", "list"}}
	if len(got) != 2 {
		t.Fatalf("calls = %v, want exactly the catalog and the local agent list", got)
	}
	for i := range want {
		if !equalStringSlice(got[i], want[i]) {
			t.Errorf("call %d = %v, want %v", i, got[i], want[i])
		}
	}
}

// TestRouteNowrite: under HERDR_HERMES_NOWRITE=1 the command works (it is
// read-only) and the config dir keeps exactly the file it held before.
func TestRouteNowrite(t *testing.T) {
	fakeDir := t.TempDir()
	exe := fakesoho.Install(t, fakeDir,
		fakesoho.Rule{Argv: []string{"machine", "list", "--json"}, Code: 0, Stdout: routeCatalogWinA},
		fakesoho.Rule{Argv: []string{"agent", "list"}, Code: 0, Stdout: routeAgentList(1)},
		fakesoho.Rule{Argv: []string{"--machine", "win-a", "agent", "list"}, Code: 0, Stdout: routeAgentList(0)},
	)
	dir := t.TempDir()
	writeRouteConfig(t, dir, map[string]string{
		"herdr_bin":      exe,
		"route_machines": "local,win-a",
	})
	stdout, stderr, exit := runCLI(t, dir, true, "route")
	if exit != 0 {
		t.Fatalf("route under NOWRITE exit = %d, stderr %q", exit, stderr)
	}
	res := parseRouteResult(t, stdout)
	if res.Machine != "win-a" || res.Motivo != "least_load" {
		t.Errorf("result = %+v, want win-a least_load", res)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read config dir: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "config" {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Errorf("config dir = %v, want exactly [config]", names)
	}
}

// TestRouteNoSecretPropagation: with a stored API key, the child
// environment of every herdr call is exactly the environment the test
// passed through Env.Environ, and the key value appears nowhere — not in
// any logged argv or env, not on stdout or stderr.
func TestRouteNoSecretPropagation(t *testing.T) {
	fakeDir := t.TempDir()
	exe := fakesoho.Install(t, fakeDir,
		fakesoho.Rule{Argv: []string{"agent", "list"}, Code: 0, Stdout: routeAgentList(1)},
	)
	dir := t.TempDir()
	// Store the sentinel key through the existing auth login path.
	if out, errb, exit := runCLIStdin(t, dir, sentinelKey+"\n", nil, "auth", "login", "--key", "-"); exit != 0 {
		t.Fatalf("auth login exit = %d, stdout %q, stderr %q", exit, out, errb)
	}
	data, err := os.ReadFile(filepath.Join(dir, "credentials"))
	if err != nil || strings.TrimSpace(string(data)) != sentinelKey {
		t.Fatalf("credentials file = %q, err %v; want the sentinel key stored", data, err)
	}
	writeRouteConfig(t, dir, map[string]string{
		"herdr_bin":      exe,
		"route_machines": "local",
	})
	stdout, stderr, exit := runRouteEnv(t, dir, fakeChildEnv, "route")
	if exit != 0 {
		t.Fatalf("route exit = %d, stderr %q", exit, stderr)
	}
	calls, err := fakesoho.ReadCalls(fakeDir)
	if err != nil {
		t.Fatalf("ReadCalls: %v", err)
	}
	if len(calls) != 1 {
		t.Fatalf("calls = %d, want the single local agent list", len(calls))
	}
	for i, c := range calls {
		env := c.Env
		if runtime.GOOS == "windows" {
			// The Go runtime adds SYSTEMROOT to a child environment on
			// Windows when it is missing: the standard library's
			// behavior, not a leak.
			var kept []string
			for _, kv := range env {
				if !strings.HasPrefix(kv, "SYSTEMROOT=") {
					kept = append(kept, kv)
				}
			}
			env = kept
		}
		if !equalStringSlice(env, fakeChildEnv) {
			t.Errorf("call %d env = %v, want exactly %v", i, env, fakeChildEnv)
		}
		for _, a := range c.Argv {
			if strings.Contains(a, sentinelKey) {
				t.Errorf("call %d argv contains the key", i)
			}
		}
		for _, e := range c.Env {
			if strings.Contains(e, sentinelKey) {
				t.Errorf("call %d env contains the key: %q", i, e)
			}
		}
	}
	for i, s := range []string{stdout, stderr} {
		if strings.Contains(s, sentinelKey) {
			t.Errorf("output %d (stdout=%v) contains the key: %q", i, i == 0, s)
		}
	}
}
