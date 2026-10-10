package cli

import (
	"strings"
	"testing"

	"github.com/djalmajr/herdr-hermes/internal/testutil/fakesoho"
)

// routeServerNotRunningStderr is the shape of the stderr a real herdr
// prints when no server listens on its socket; the path is generic and the
// message also carries the test sentinel, which must never be echoed.
const routeServerNotRunningStderr = `{"id":"cli:agent:list","error":{"code":"server_not_running","message":"no herdr server is running at /tmp/hh-test/nx.sock; run ` + "`herdr`" + ` to start or attach it ` + sentinelKey + `"}}` + "\n"

// assertNoLeak fails when s carries the sentinel key, the generic socket
// path, an escape, a NUL or a carriage return.
func assertNoLeak(t *testing.T, name, s string) {
	t.Helper()
	for _, bad := range []string{sentinelKey, "/tmp/hh-test", "\x1b", "\x00", "\r", "example.invalid"} {
		if strings.Contains(s, bad) {
			t.Errorf("%s contains %q: %q", name, bad, s)
		}
	}
}

// TestRouteDiagServerNotRunningFallback: the requested local machine
// fails with herdr's server_not_running error; the route falls back with
// exit 0, stdout is the unchanged one-line decision and stderr carries one
// classified line without any part of herdr's message.
func TestRouteDiagServerNotRunningFallback(t *testing.T) {
	fakeDir := t.TempDir()
	exe := fakesoho.Install(t, fakeDir,
		fakesoho.Rule{Argv: []string{"machine", "list", "--json"}, Code: 0, Stdout: routeCatalogWinA},
		fakesoho.Rule{Argv: []string{"agent", "list"}, Code: 1, Stderr: routeServerNotRunningStderr},
		fakesoho.Rule{Argv: []string{"--machine", "win-a", "agent", "list"}, Code: 0, Stdout: routeAgentList(1)},
	)
	dir := t.TempDir()
	writeRouteConfig(t, dir, map[string]string{
		"herdr_bin":      exe,
		"route_machines": "local,win-a",
	})
	stdout, stderr, exit := runCLI(t, dir, false, "route", "--machine", "local")
	if exit != 0 {
		t.Fatalf("route exit = %d, stderr %q", exit, stderr)
	}
	wantOut := `{"maquina":"win-a","motivo":"fallback","solicitada":"local","orquestradores":1,"candidatos":[{"maquina":"local","estado":"unavailable","motivo":"probe_failed"},{"maquina":"win-a","estado":"available","orquestradores":1}]}` + "\n"
	if stdout != wantOut {
		t.Errorf("stdout = %q, want %q", stdout, wantOut)
	}
	wantErr := "herdr-hermes: route: local unavailable: probe_failed: server_not_running (herdr exit 1): no Herdr server is running at the configured socket; start herdr or check the socket path\n"
	if stderr != wantErr {
		t.Errorf("stderr = %q, want %q", stderr, wantErr)
	}
	assertNoLeak(t, "stdout", stdout)
	assertNoLeak(t, "stderr", stderr)
}

// TestRouteDiagSocketPathTooLong: herdr's sun_path error is classified as
// socket_path_too_long while the other machine wins by least load.
func TestRouteDiagSocketPathTooLong(t *testing.T) {
	fakeDir := t.TempDir()
	exe := fakesoho.Install(t, fakeDir,
		fakesoho.Rule{Argv: []string{"machine", "list", "--json"}, Code: 0, Stdout: routeCatalogWinA},
		fakesoho.Rule{Argv: []string{"agent", "list"}, Code: 1, Stderr: `Error: Custom { kind: InvalidInput, error: "local socket name length exceeds capacity of sun_path of sockaddr_un" }` + "\n"},
		fakesoho.Rule{Argv: []string{"--machine", "win-a", "agent", "list"}, Code: 0, Stdout: routeAgentList(3)},
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
	if res.Machine != "win-a" || res.Motivo != "least_load" || res.Orchestrators != 3 {
		t.Errorf("result = %+v, want win-a least_load 3", res)
	}
	wantErr := "herdr-hermes: route: local unavailable: probe_failed: socket_path_too_long (herdr exit 1): the Herdr socket path is longer than the operating system allows\n"
	if stderr != wantErr {
		t.Errorf("stderr = %q, want %q", stderr, wantErr)
	}
}

// TestRouteDiagHostileStderr: an unrecognized, oversized stderr full of
// secrets, escapes, NULs and carriage returns yields one fixed line; none
// of the bytes reach stdout or stderr and stdout stays one line.
func TestRouteDiagHostileStderr(t *testing.T) {
	var hostile strings.Builder
	for i := 0; i < 3000; i++ {
		hostile.WriteString("\x1b[31mtoken=" + sentinelKey + "\r\x00 at /tmp/hh-test/nx.sock\n")
	}
	// A JSON error with a code outside the allowlist is not a cause.
	hostile.WriteString(`{"error":{"code":"` + sentinelKey + `","message":"x"}}` + "\n")
	fakeDir := t.TempDir()
	exe := fakesoho.Install(t, fakeDir,
		fakesoho.Rule{Argv: []string{"agent", "list"}, Code: 9, StderrBytes: []byte(hostile.String()), Stdout: `{"error":{"code":"` + sentinelKey + `"}}`},
	)
	dir := t.TempDir()
	writeRouteConfig(t, dir, map[string]string{
		"herdr_bin":      exe,
		"route_machines": "local",
	})
	stdout, stderr, exit := runCLI(t, dir, false, "route")
	if exit != 4 {
		t.Fatalf("route exit = %d, want 4 (stderr %q)", exit, stderr)
	}
	if strings.Count(stdout, "\n") != 1 || !strings.HasSuffix(stdout, "\n") {
		t.Errorf("stdout is not exactly one line: %q", stdout)
	}
	wantErr := "herdr-hermes: route: local unavailable: probe_failed (herdr exit 9): herdr reported no recognized cause\n" +
		"herdr-hermes: route: no eligible machine is available\n"
	if stderr != wantErr {
		t.Errorf("stderr = %q, want %q", stderr, wantErr)
	}
	assertNoLeak(t, "stdout", stdout)
	assertNoLeak(t, "stderr", stderr)
}

// TestRouteDiagCatalogConnectionRefused: a catalog that fails with a
// transport error gives every non-local machine the classified cause and
// the herdr exit code, while local still wins with exit 0.
func TestRouteDiagCatalogConnectionRefused(t *testing.T) {
	fakeDir := t.TempDir()
	exe := fakesoho.Install(t, fakeDir,
		fakesoho.Rule{Argv: []string{"machine", "list", "--json"}, Code: 255, Stderr: "ssh: connect to host example.invalid port 22: Connection refused\n"},
		fakesoho.Rule{Argv: []string{"agent", "list"}, Code: 0, Stdout: routeAgentList(2)},
	)
	dir := t.TempDir()
	writeRouteConfig(t, dir, map[string]string{
		"herdr_bin":      exe,
		"route_machines": "win-a,local",
	})
	stdout, stderr, exit := runCLI(t, dir, false, "route")
	if exit != 0 {
		t.Fatalf("route exit = %d, stderr %q", exit, stderr)
	}
	wantOut := `{"maquina":"local","motivo":"least_load","orquestradores":2,"candidatos":[{"maquina":"win-a","estado":"unavailable","motivo":"catalog_unavailable"},{"maquina":"local","estado":"available","orquestradores":2}]}` + "\n"
	if stdout != wantOut {
		t.Errorf("stdout = %q, want %q", stdout, wantOut)
	}
	wantErr := "herdr-hermes: route: win-a unavailable: catalog_unavailable: connection_refused (herdr exit 255): the remote machine refused the connection\n"
	if stderr != wantErr {
		t.Errorf("stderr = %q, want %q", stderr, wantErr)
	}
	assertNoLeak(t, "stderr", stderr)
}

// TestRouteDiagUnsupportedAndTimeout: an unknown saved machine and a
// timed-out probe each get their fixed reason hint.
func TestRouteDiagUnsupportedAndTimeout(t *testing.T) {
	fakeDir := t.TempDir()
	exe := fakesoho.Install(t, fakeDir,
		fakesoho.Rule{Argv: []string{"machine", "list", "--json"}, Code: 0, Stdout: routeCatalogWinA},
		fakesoho.Rule{Argv: []string{"agent", "list"}, Code: 0, Stdout: routeAgentList(0)},
		fakesoho.Rule{Argv: []string{"--machine", "win-a", "agent", "list"}, Code: 0, Delay: 2500, Stdout: routeAgentList(0)},
	)
	dir := t.TempDir()
	writeRouteConfig(t, dir, map[string]string{
		"herdr_bin":             exe,
		"route_machines":        "local,win-a,ghost",
		"route_probe_timeout_s": "1",
	})
	stdout, stderr, exit := runCLI(t, dir, false, "route")
	if exit != 0 {
		t.Fatalf("route exit = %d, stderr %q", exit, stderr)
	}
	if res := parseRouteResult(t, stdout); res.Machine != "local" {
		t.Errorf("machine = %q, want local", res.Machine)
	}
	wantErr := "herdr-hermes: route: win-a unavailable: probe_timeout: the probe did not finish within route_probe_timeout_s\n" +
		"herdr-hermes: route: ghost unsupported: unknown_machine: not a saved Herdr machine (herdr machine list) and not local\n"
	if stderr != wantErr {
		t.Errorf("stderr = %q, want %q", stderr, wantErr)
	}
}

// TestRouteDisabledUnmatchedWarning: a route_disabled label outside the
// fleet is reported on stderr without changing stdout or the exit code; a
// matching exclusion is silent and its disabled candidate gets no
// diagnostic line.
func TestRouteDisabledUnmatchedWarning(t *testing.T) {
	fakeDir := t.TempDir()
	exe := fakesoho.Install(t, fakeDir,
		fakesoho.Rule{Argv: []string{"machine", "list", "--json"}, Code: 0, Stdout: routeCatalogWinA},
		fakesoho.Rule{Argv: []string{"agent", "list"}, Code: 0, Stdout: routeAgentList(2)},
		fakesoho.Rule{Argv: []string{"--machine", "win-a", "agent", "list"}, Code: 0, Stdout: routeAgentList(1)},
	)
	cases := []struct {
		name, disabled, wantOut, wantErr string
	}{
		{
			name:     "unmatched",
			disabled: "ghost-box",
			wantOut:  `{"maquina":"win-a","motivo":"least_load","orquestradores":1,"candidatos":[{"maquina":"local","estado":"available","orquestradores":2},{"maquina":"win-a","estado":"available","orquestradores":1}]}`,
			wantErr:  "herdr-hermes: route: warning: route_disabled labels not in route_machines exclude nothing: ghost-box\n",
		},
		{
			name:     "partly unmatched",
			disabled: "typo,win-a",
			wantOut:  `{"maquina":"local","motivo":"least_load","orquestradores":2,"candidatos":[{"maquina":"local","estado":"available","orquestradores":2},{"maquina":"win-a","estado":"disabled","motivo":"disabled_by_config"}]}`,
			wantErr:  "herdr-hermes: route: warning: route_disabled labels not in route_machines exclude nothing: typo\n",
		},
		{
			name:     "matching",
			disabled: "win-a",
			wantOut:  `{"maquina":"local","motivo":"least_load","orquestradores":2,"candidatos":[{"maquina":"local","estado":"available","orquestradores":2},{"maquina":"win-a","estado":"disabled","motivo":"disabled_by_config"}]}`,
			wantErr:  "",
		},
		{
			name:     "empty",
			disabled: "",
			wantOut:  `{"maquina":"win-a","motivo":"least_load","orquestradores":1,"candidatos":[{"maquina":"local","estado":"available","orquestradores":2},{"maquina":"win-a","estado":"available","orquestradores":1}]}`,
			wantErr:  "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			writeRouteConfig(t, dir, map[string]string{
				"herdr_bin":      exe,
				"route_machines": "local,win-a",
				"route_disabled": tc.disabled,
			})
			stdout, stderr, exit := runCLI(t, dir, false, "route")
			if exit != 0 {
				t.Fatalf("route exit = %d, stderr %q", exit, stderr)
			}
			if stdout != tc.wantOut+"\n" {
				t.Errorf("stdout = %q, want %q", stdout, tc.wantOut)
			}
			if stderr != tc.wantErr {
				t.Errorf("stderr = %q, want %q", stderr, tc.wantErr)
			}
		})
	}
}
