package router_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/djalmajr/herdr-hermes/internal/router"
)

// TestClassifyFailureRealPayloads: the three observed herdr failure
// payloads classify into the mapped causes.
func TestClassifyFailureRealPayloads(t *testing.T) {
	cases := []struct {
		name   string
		stderr []byte
		want   string
	}{
		{"server not running", []byte(`{"id":"cli:agent:list","error":{"code":"server_not_running","message":"no herdr server is running at /tmp/hh-test/nx.sock; run ` + "`herdr`" + ` to start or attach it"}}`), router.CauseServerNotRunning},
		{"socket path too long", []byte(`Error: Custom { kind: InvalidInput, error: "local socket name length exceeds capacity of sun_path of sockaddr_un" }`), router.CauseSocketPathTooLong},
		{"unknown machine", []byte("error: unknown machine 'ghost-box'; use " + "`herdr machine list`"), router.CauseUnknownMachine},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := router.ClassifyFailure(nil, tc.stderr); got != tc.want {
				t.Errorf("ClassifyFailure = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestClassifyFailureTable: one case per remaining cause, the JSON error
// on stdout, an unknown or non-exact JSON code, empty input and the
// first-match-wins order.
func TestClassifyFailureTable(t *testing.T) {
	cases := []struct {
		name   string
		stdout []byte
		stderr []byte
		want   string
	}{
		{"server unavailable json", nil, []byte(`{"id":"cli:agent:list","error":{"code":"server_unavailable","message":"nope"}}`), router.CauseServerUnavailable},
		{"protocol mismatch json", nil, []byte(`{"error":{"code":"protocol_mismatch"}}`), router.CauseProtocolMismatch},
		{"endpoint timeout json", nil, []byte(`{"error":{"code":"endpoint_timeout"}}`), router.CauseEndpointTimeout},
		{"json error on stdout not stderr", []byte(`{"error":{"code":"protocol_mismatch"}}`), nil, router.CauseProtocolMismatch},
		{"stdout json wins over stderr substring", []byte(`{"error":{"code":"endpoint_timeout"}}`), []byte("dial tcp: connection refused"), router.CauseEndpointTimeout},
		{"stderr json wins over stdout json", []byte(`{"error":{"code":"endpoint_timeout"}}`), []byte(`{"error":{"code":"server_not_running"}}`), router.CauseServerNotRunning},
		{"unknown json code", nil, []byte(`{"error":{"code":"made_up"}}`), ""},
		{"json code not exact", nil, []byte(`{"error":{"code":"server_not_running x"}}`), ""},
		{"error null", nil, []byte(`{"error":null}`), ""},
		{"error wrong type", nil, []byte(`{"error":"down"}`), ""},
		{"empty input", nil, nil, ""},
		{"connection refused", nil, []byte("dial tcp 10.0.0.1:22: connection refused"), router.CauseConnectionRefused},
		{"could not resolve hostname", nil, []byte("could not resolve hostname 'win-z'"), router.CauseHostUnresolved},
		{"name or service not known", nil, []byte("lookup win-z: name or service not known"), router.CauseHostUnresolved},
		{"nodename nor servname", nil, []byte("getaddrinfo win-z: nodename nor servname provided, or not known"), router.CauseHostUnresolved},
		{"permission denied", nil, []byte("ssh: permission denied"), router.CausePermissionDenied},
		{"connection timed out", nil, []byte("dial: connection timed out"), router.CauseConnectionTimedOut},
		{"operation timed out", nil, []byte("recv: operation timed out"), router.CauseConnectionTimedOut},
		{"no route to host", nil, []byte("dial: no route to host"), router.CauseHostUnreachable},
		{"network is unreachable", nil, []byte("write udp: network is unreachable"), router.CauseHostUnreachable},
		{"case insensitive", nil, []byte("Dial: CONNECTION REFUSED"), router.CauseConnectionRefused},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := router.ClassifyFailure(tc.stdout, tc.stderr); got != tc.want {
				t.Errorf("ClassifyFailure = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestClassifyFailureOnlyFirstDiagBytes: a recognized JSON error beyond
// the first MaxProbeDiag bytes of each input is not considered.
func TestClassifyFailureOnlyFirstDiagBytes(t *testing.T) {
	beyond := make([]byte, 4096)
	copy(beyond, "x")
	payload := `{"error":{"code":"server_unavailable"}}`
	stderr := append(beyond, []byte("\n"+payload)...)
	if got := router.ClassifyFailure(nil, stderr); got != "" {
		t.Errorf("stderr beyond cap: ClassifyFailure = %q, want %q", got, "")
	}
	stdout := append(beyond, []byte(payload)...)
	if got := router.ClassifyFailure(stdout, nil); got != "" {
		t.Errorf("stdout beyond cap: ClassifyFailure = %q, want %q", got, "")
	}
}

// TestCauseHintExact: every cause maps to its exact fixed hint and an
// empty or unknown cause gets the neutral hint.
func TestCauseHintExact(t *testing.T) {
	cases := []struct {
		cause string
		want  string
	}{
		{router.CauseServerNotRunning, "no Herdr server is running at the configured socket; start herdr or check the socket path"},
		{router.CauseServerUnavailable, "the Herdr server did not accept the request"},
		{router.CauseProtocolMismatch, "the herdr client and server versions are not compatible"},
		{router.CauseEndpointTimeout, "the Herdr endpoint did not answer in time"},
		{router.CauseSocketPathTooLong, "the Herdr socket path is longer than the operating system allows"},
		{router.CauseUnknownMachine, "herdr does not know this machine; check herdr machine list"},
		{router.CauseConnectionRefused, "the remote machine refused the connection"},
		{router.CauseHostUnresolved, "the remote host name could not be resolved"},
		{router.CausePermissionDenied, "the remote machine refused the credentials"},
		{router.CauseConnectionTimedOut, "the connection to the remote machine timed out"},
		{router.CauseHostUnreachable, "the remote machine is not reachable on the network"},
		{"", "herdr reported no recognized cause"},
		{"made_up", "herdr reported no recognized cause"},
	}
	for _, tc := range cases {
		t.Run(tc.cause, func(t *testing.T) {
			if got := router.CauseHint(tc.cause); got != tc.want {
				t.Errorf("CauseHint(%q) = %q, want %q", tc.cause, got, tc.want)
			}
		})
	}
}

// TestDiagnosticExact: the one-line format, the exact example strings,
// the empty result for available and disabled candidates and the
// <invalid label> substitution.
func TestDiagnosticExact(t *testing.T) {
	cases := []struct {
		name string
		cand router.Candidate
		want string
	}{
		{"probe failed with cause and exit",
			router.Candidate{Machine: "local", State: router.StateUnavailable, Reason: router.ReasonProbeFailed, Cause: router.CauseServerNotRunning, ExitCode: 1},
			"local unavailable: probe_failed: server_not_running (herdr exit 1): no Herdr server is running at the configured socket; start herdr or check the socket path"},
		{"probe timeout",
			router.Candidate{Machine: "win-a", State: router.StateUnavailable, Reason: router.ReasonProbeTimeout},
			"win-a unavailable: probe_timeout: the probe did not finish within route_probe_timeout_s"},
		{"unknown machine",
			router.Candidate{Machine: "ghost", State: router.StateUnsupported, Reason: router.ReasonUnknownMachine},
			"ghost unsupported: unknown_machine: not a saved Herdr machine (herdr machine list) and not local"},
		{"catalog unavailable with cause and exit",
			router.Candidate{Machine: "win-a", State: router.StateUnavailable, Reason: router.ReasonCatalogUnavailable, Cause: router.CauseConnectionRefused, ExitCode: 3},
			"win-a unavailable: catalog_unavailable: connection_refused (herdr exit 3): the remote machine refused the connection"},
		{"probe failed without cause",
			router.Candidate{Machine: "local", State: router.StateUnavailable, Reason: router.ReasonProbeFailed},
			"local unavailable: probe_failed: herdr reported no recognized cause"},
		{"available",
			router.Candidate{Machine: "local", State: router.StateAvailable, Orchestrators: countPtr(1)},
			""},
		{"disabled",
			router.Candidate{Machine: "mac-a", State: router.StateDisabled, Reason: router.ReasonDisabledByConfig},
			""},
		{"invalid label is substituted",
			router.Candidate{Machine: "bad label\nsk-test-SECRET", State: router.StateUnavailable, Reason: router.ReasonProbeFailed, Cause: router.CauseUnknownMachine, ExitCode: 1},
			"<invalid label> unavailable: probe_failed: unknown_machine (herdr exit 1): herdr does not know this machine; check herdr machine list"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.cand.Diagnostic(); got != tc.want {
				t.Errorf("Diagnostic = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestDiagnosticRedactsHostileStderr: a hostile stderr (a secret token,
// ANSI escapes, CR, NUL and many lines) never leaks into the candidate
// or its diagnostic, with and without a recognized cause.
func TestDiagnosticRedactsHostileStderr(t *testing.T) {
	secret := "sk-test-SECRET-0123456789"
	var b strings.Builder
	b.WriteString(`{"id":"cli:agent:list","error":{"code":"server_not_running","message":"no server at /tmp/hh-test/nx.sock; token ` + secret + `"}}`)
	b.WriteString("\n")
	b.WriteString("\x1b[31m" + secret + "\r\n")
	b.WriteString(strings.Repeat("filler "+secret+"\n", 60))
	b.WriteString("\x00" + secret)
	hostile := []byte(b.String())

	fe := newFakeExec(func(argv []string) ([]byte, []byte, int, error) {
		return nil, hostile, 1, nil
	})
	f := router.Fleet{Machines: []string{router.Local}, Exec: fe.Run}
	out := f.Probe(context.Background())
	c := out[0]
	if c.State != router.StateUnavailable || c.Reason != router.ReasonProbeFailed {
		t.Fatalf("candidate = %+v, want unavailable probe_failed", c)
	}
	if c.Cause != router.CauseServerNotRunning {
		t.Errorf("cause = %q, want %q", c.Cause, router.CauseServerNotRunning)
	}
	if c.ExitCode != 1 {
		t.Errorf("exit code = %d, want 1", c.ExitCode)
	}
	diag := c.Diagnostic()
	for _, banned := range []string{secret, "\x1b", "\n", "\r", "\x00", "/tmp/hh-test"} {
		if strings.Contains(diag, banned) {
			t.Errorf("diagnostic contains %q: %q", banned, diag)
		}
	}
	if len(diag) > 512 {
		t.Errorf("diagnostic length = %d, want at most 512", len(diag))
	}

	// Same checks for a stderr where nothing is recognized.
	unrecognized := []byte(strings.Repeat(secret+"\x1b[31m\r"+strings.Repeat("junk\n", 50), 10))
	fe2 := newFakeExec(func(argv []string) ([]byte, []byte, int, error) {
		return nil, unrecognized, 5, nil
	})
	f2 := router.Fleet{Machines: []string{router.Local}, Exec: fe2.Run}
	out2 := f2.Probe(context.Background())
	c2 := out2[0]
	if c2.Cause != "" {
		t.Errorf("cause = %q, want empty for an unrecognized stderr", c2.Cause)
	}
	if c2.ExitCode != 5 {
		t.Errorf("exit code = %d, want 5", c2.ExitCode)
	}
	diag2 := c2.Diagnostic()
	if !strings.Contains(diag2, "herdr reported no recognized cause") {
		t.Errorf("diagnostic = %q, want the unrecognized-cause hint", diag2)
	}
	for _, banned := range []string{secret, "\x1b", "\n", "\r", "\x00", "/tmp/hh-test"} {
		if strings.Contains(diag2, banned) {
			t.Errorf("diagnostic contains %q: %q", banned, diag2)
		}
	}
	if len(diag2) > 512 {
		t.Errorf("diagnostic length = %d, want at most 512", len(diag2))
	}
}

// TestProbeProbeFailedCarriesCauseAndExit: a local probe that exits 1
// with a recognized stderr gets the cause and exit code, the other
// machines keep their own state and the candidates' JSON stays
// byte-identical to the pre-change shape.
func TestProbeProbeFailedCarriesCauseAndExit(t *testing.T) {
	serverNotRunning := []byte(`{"id":"cli:agent:list","error":{"code":"server_not_running","message":"no herdr server is running"}}`)
	fe := newFakeExec(func(argv []string) ([]byte, []byte, int, error) {
		switch {
		case equalArgv(argv, []string{"machine", "list", "--json"}):
			return catalogStdout(catEntry{id: "1", label: "win-a", enabled: true}), nil, 0, nil
		case equalArgv(argv, []string{"agent", "list"}):
			return nil, serverNotRunning, 1, nil
		case equalArgv(argv, []string{"--machine", "win-a", "agent", "list"}):
			return agentsJSON(agent{pane: "p1", status: "idle", name: namePtr("orchestrator")}), nil, 0, nil
		}
		t.Errorf("unexpected exec: %v", argv)
		return nil, nil, 1, nil
	})
	f := router.Fleet{Machines: []string{router.Local, "win-a"}, Exec: fe.Run}
	out := f.Probe(context.Background())
	local := out[0]
	if local.State != router.StateUnavailable || local.Reason != router.ReasonProbeFailed || local.Cause != router.CauseServerNotRunning || local.ExitCode != 1 {
		t.Errorf("local = %+v, want unavailable probe_failed server_not_running exit 1", local)
	}
	win := out[1]
	if win.State != router.StateAvailable || win.Reason != "" || win.Cause != "" || win.ExitCode != 0 {
		t.Errorf("win-a = %+v, want available with no cause or exit code", win)
	}
	got, err := json.Marshal(out)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	want := `[{"maquina":"local","estado":"unavailable","motivo":"probe_failed"},{"maquina":"win-a","estado":"available","orquestradores":1}]`
	if string(got) != want {
		t.Errorf("candidates JSON = %s, want %s", got, want)
	}
}

// TestProbeCatalogUnavailableCarriesCauseAndExit: a catalog that exits 3
// with a connection-refused stderr gives every non-local candidate the
// shared cause and exit code while local keeps its own probe; a timed-out
// catalog leaves both empty.
func TestProbeCatalogUnavailableCarriesCauseAndExit(t *testing.T) {
	fe := newFakeExec(func(argv []string) ([]byte, []byte, int, error) {
		if equalArgv(argv, []string{"machine", "list", "--json"}) {
			return nil, []byte("dial tcp: connection refused"), 3, nil
		}
		return agentsJSON(agent{pane: "p1", status: "idle", name: namePtr("orchestrator")}), nil, 0, nil
	})
	f := router.Fleet{Machines: []string{router.Local, "win-a", "mac-a"}, Exec: fe.Run}
	out := f.Probe(context.Background())
	if out[0].Machine != router.Local || out[0].State != router.StateAvailable {
		t.Errorf("local = %+v, want available from its own probe", out[0])
	}
	for i, label := range []string{"win-a", "mac-a"} {
		c := out[i+1]
		if c.State != router.StateUnavailable || c.Reason != router.ReasonCatalogUnavailable || c.Cause != router.CauseConnectionRefused || c.ExitCode != 3 {
			t.Errorf("%s = %+v, want catalog_unavailable connection_refused exit 3", label, c)
		}
	}
	if calls := fe.argvs(); len(calls) != 2 {
		t.Errorf("exec calls = %v, want the catalog and the local agent list only", calls)
	}

	fe2 := newFakeExec(func(argv []string) ([]byte, []byte, int, error) {
		if equalArgv(argv, []string{"machine", "list", "--json"}) {
			return nil, nil, 0, fmt.Errorf("bound expired: %w", router.ErrProbeTimeout)
		}
		return nil, nil, 1, nil
	})
	f2 := router.Fleet{Machines: []string{router.Local, "win-a"}, Exec: fe2.Run}
	out2 := f2.Probe(context.Background())
	if out2[1].State != router.StateUnavailable || out2[1].Reason != router.ReasonCatalogUnavailable {
		t.Errorf("win-a = %+v, want unavailable catalog_unavailable", out2[1])
	}
	if out2[1].Cause != "" || out2[1].ExitCode != 0 {
		t.Errorf("win-a = %+v, want no cause or exit code for a timed-out catalog", out2[1])
	}
}
