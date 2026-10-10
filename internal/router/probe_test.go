package router_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/djalmajr/herdr-hermes/internal/router"
)

// fakeExec is an in-memory Exec that records every argv call (and the
// deadline of the context it ran with) under a mutex and answers from fn.
type fakeExec struct {
	mu        sync.Mutex
	calls     [][]string
	deadlines []time.Duration
	fn        func(argv []string) ([]byte, []byte, int, error)
}

func newFakeExec(fn func(argv []string) ([]byte, []byte, int, error)) *fakeExec {
	return &fakeExec{fn: fn}
}

// Run implements router.Exec.
func (f *fakeExec) Run(ctx context.Context, argv []string) ([]byte, []byte, int, error) {
	f.mu.Lock()
	f.calls = append(f.calls, append([]string(nil), argv...))
	f.deadlines = append(f.deadlines, contextDeadlineIn(ctx))
	f.mu.Unlock()
	return f.fn(argv)
}

// contextDeadlineIn reports the time left until the context deadline, or
// a negative value when the context has none.
func contextDeadlineIn(ctx context.Context) time.Duration {
	dl, ok := ctx.Deadline()
	if !ok {
		return -1
	}
	return time.Until(dl)
}

func (f *fakeExec) argvs() [][]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][]string(nil), f.calls...)
}

func (f *fakeExec) deadlineList() []time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]time.Duration(nil), f.deadlines...)
}

// catEntry is one saved machine for catalogStdout.
type catEntry struct {
	id      string
	label   string
	enabled bool
}

// catalogStdout builds the stdout of `herdr machine list --json` from the
// entries, carrying the extra fields the real CLI prints.
func catalogStdout(entries ...catEntry) []byte {
	var b strings.Builder
	b.WriteByte('[')
	for i, e := range entries {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"id":%q,"label":%q,"target":"t","session":"default","enabled":%t,"selected":false}`,
			e.id, e.label, e.enabled)
	}
	b.WriteByte(']')
	return []byte(b.String())
}

// agent is one agent entry for agentsJSON. A nil name with nullName set
// renders "name": null; a nil name without it is an absent field.
type agent struct {
	pane     string
	status   string
	name     *string
	nullName bool
}

// agentsJSON builds the stdout of `herdr agent list`: the Herdr API
// response with the given agents.
func agentsJSON(agents ...agent) []byte {
	var b strings.Builder
	b.WriteString(`{"id":"cli:agent:list","result":{"agents":[`)
	for i, a := range agents {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"pane_id":%q,"agent_status":%q`, a.pane, a.status)
		if a.name != nil {
			fmt.Fprintf(&b, `,"name":%q`, *a.name)
		} else if a.nullName {
			b.WriteString(`,"name":null`)
		}
		b.WriteByte('}')
	}
	b.WriteString(`]}}`)
	return []byte(b.String())
}

func namePtr(s string) *string { return &s }

// equalArgv reports whether two argvs are element-equal.
func equalArgv(a, b []string) bool {
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

// wantCandidate checks the candidate at index i against the expected
// machine, state, orchestrator count (nil = no count) and reason ("" =
// none).
func wantCandidate(t *testing.T, got []router.Candidate, i int, m string, s router.State, count *int, reason string) {
	t.Helper()
	c := got[i]
	if c.Machine != m {
		t.Errorf("candidate %d machine = %q, want %q", i, c.Machine, m)
	}
	if c.State != s {
		t.Errorf("candidate %d state = %q, want %q", i, c.State, s)
	}
	if reason != "" {
		if c.Reason != reason {
			t.Errorf("candidate %d reason = %q, want %q", i, c.Reason, reason)
		}
		if c.Orchestrators != nil {
			t.Errorf("candidate %d orchestrators = %d, want none", i, *c.Orchestrators)
		}
		return
	}
	if c.Reason != "" {
		t.Errorf("candidate %d reason = %q, want none", i, c.Reason)
	}
	if count == nil {
		if c.Orchestrators != nil {
			t.Errorf("candidate %d orchestrators = %d, want none", i, *c.Orchestrators)
		}
		return
	}
	if c.Orchestrators == nil || *c.Orchestrators != *count {
		t.Errorf("candidate %d orchestrators = %v, want %d", i, c.Orchestrators, *count)
	}
}

// TestProbeFleetInConfiguredOrder: a fleet of several machines is probed
// concurrently (the agent-list calls overlap in the fake's delays) and
// the candidates come back in configured order, with the catalog run
// exactly once and the default orchestrator name counted.
func TestProbeFleetInConfiguredOrder(t *testing.T) {
	fe := newFakeExec(func(argv []string) ([]byte, []byte, int, error) {
		if equalArgv(argv, []string{"machine", "list", "--json"}) {
			time.Sleep(50 * time.Millisecond)
			return catalogStdout(
				catEntry{id: "1", label: "mac-a", enabled: true},
				catEntry{id: "2", label: "mac-b", enabled: true},
				catEntry{id: "3", label: "mac-c", enabled: true},
			), nil, 0, nil
		}
		time.Sleep(150 * time.Millisecond)
		switch {
		case equalArgv(argv, []string{"agent", "list"}):
			return agentsJSON(agent{pane: "p1", status: "idle", name: namePtr("orchestrator")}), nil, 0, nil
		case equalArgv(argv, []string{"--machine", "mac-a", "agent", "list"}):
			return agentsJSON(
				agent{pane: "p2", status: "working", name: namePtr("orchestrator")},
				agent{pane: "p3", status: "idle", name: namePtr("build")},
			), nil, 0, nil
		case equalArgv(argv, []string{"--machine", "mac-b", "agent", "list"}):
			return agentsJSON(
				agent{pane: "p4", status: "idle", name: namePtr("orchestrator")},
				agent{pane: "p5", status: "idle", name: namePtr("orchestrator-2")},
				agent{pane: "p6", status: "idle", name: namePtr("review-3")},
			), nil, 0, nil
		case equalArgv(argv, []string{"--machine", "mac-c", "agent", "list"}):
			return agentsJSON(), nil, 0, nil
		}
		t.Errorf("unexpected exec: %v", argv)
		return nil, nil, 1, nil
	})
	f := router.Fleet{
		Machines: []string{router.Local, "mac-a", "mac-b", "mac-c"},
		Exec:     fe.Run,
	}
	out := f.Probe(context.Background())
	if len(out) != 4 {
		t.Fatalf("candidates = %d, want 4", len(out))
	}
	wantCandidate(t, out, 0, router.Local, router.StateAvailable, countPtr(1), "")
	wantCandidate(t, out, 1, "mac-a", router.StateAvailable, countPtr(1), "")
	wantCandidate(t, out, 2, "mac-b", router.StateAvailable, countPtr(2), "")
	wantCandidate(t, out, 3, "mac-c", router.StateAvailable, countPtr(0), "")
	calls := fe.argvs()
	if len(calls) != 5 {
		t.Fatalf("exec calls = %d, want 5 (one catalog + four agent lists)", len(calls))
	}
	if !equalArgv(calls[0], []string{"machine", "list", "--json"}) {
		t.Errorf("first call = %v, want the catalog", calls[0])
	}
	for _, want := range [][]string{
		{"agent", "list"},
		{"--machine", "mac-a", "agent", "list"},
		{"--machine", "mac-b", "agent", "list"},
		{"--machine", "mac-c", "agent", "list"},
	} {
		found := false
		for _, c := range calls[1:] {
			if equalArgv(c, want) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("agent-list call %v missing from %v", want, calls)
		}
	}
}

// TestProbeCustomOrchestratorName: the configured base name is counted,
// not the default.
func TestProbeCustomOrchestratorName(t *testing.T) {
	fe := newFakeExec(func(argv []string) ([]byte, []byte, int, error) {
		return agentsJSON(
			agent{pane: "p1", status: "idle", name: namePtr("build")},
			agent{pane: "p2", status: "idle", name: namePtr("build-2")},
			agent{pane: "p3", status: "idle", name: namePtr("orchestrator")},
		), nil, 0, nil
	})
	f := router.Fleet{
		Machines:         []string{router.Local},
		OrchestratorName: "build",
		Exec:             fe.Run,
	}
	out := f.Probe(context.Background())
	wantCandidate(t, out, 0, router.Local, router.StateAvailable, countPtr(2), "")
	if got := fe.argvs(); len(got) != 1 || !equalArgv(got[0], []string{"agent", "list"}) {
		t.Errorf("exec calls = %v, want the single local agent list", got)
	}
}

// TestProbeOnlyLocalNoCatalog: a local-only fleet runs no catalog and one
// agent-list probe without --machine.
func TestProbeOnlyLocalNoCatalog(t *testing.T) {
	fe := newFakeExec(func(argv []string) ([]byte, []byte, int, error) {
		return agentsJSON(agent{pane: "p1", status: "idle", name: namePtr("orchestrator")}), nil, 0, nil
	})
	f := router.Fleet{Machines: []string{router.Local}, Exec: fe.Run}
	out := f.Probe(context.Background())
	wantCandidate(t, out, 0, router.Local, router.StateAvailable, countPtr(1), "")
	calls := fe.argvs()
	if len(calls) != 1 || !equalArgv(calls[0], []string{"agent", "list"}) {
		t.Errorf("exec calls = %v, want exactly [agent list]", calls)
	}
}

// TestProbeCatalogResolution: unknown, ambiguous and disabled saved
// machines are mapped before any agent-list probe, and a label with dots
// and dashes is its own --machine argv element.
func TestProbeCatalogResolution(t *testing.T) {
	fe := newFakeExec(func(argv []string) ([]byte, []byte, int, error) {
		if equalArgv(argv, []string{"machine", "list", "--json"}) {
			return catalogStdout(
				catEntry{id: "1", label: "mac-a", enabled: true},
				catEntry{id: "2", label: "mac-b", enabled: true},
				catEntry{id: "3", label: "mac-c", enabled: false},
				catEntry{id: "4", label: "dup", enabled: true},
				catEntry{id: "5", label: "dup", enabled: true},
				catEntry{id: "6", label: "win.a_1-2", enabled: true},
			), nil, 0, nil
		}
		return agentsJSON(), nil, 0, nil
	})
	f := router.Fleet{
		Machines: []string{"mac-a", "mac-b", "mac-c", "ghost", "dup", "win.a_1-2"},
		Exec:     fe.Run,
	}
	out := f.Probe(context.Background())
	wantCandidate(t, out, 0, "mac-a", router.StateAvailable, countPtr(0), "")
	wantCandidate(t, out, 1, "mac-b", router.StateAvailable, countPtr(0), "")
	wantCandidate(t, out, 2, "mac-c", router.StateDisabled, nil, router.ReasonMachineDisabled)
	wantCandidate(t, out, 3, "ghost", router.StateUnsupported, nil, router.ReasonUnknownMachine)
	wantCandidate(t, out, 4, "dup", router.StateUnsupported, nil, router.ReasonAmbiguousMachine)
	wantCandidate(t, out, 5, "win.a_1-2", router.StateAvailable, countPtr(0), "")
	var agentCalls [][]string
	catalogs := 0
	for _, c := range fe.argvs() {
		if equalArgv(c, []string{"machine", "list", "--json"}) {
			catalogs++
			continue
		}
		agentCalls = append(agentCalls, c)
	}
	if catalogs != 1 {
		t.Errorf("catalog calls = %d, want 1", catalogs)
	}
	for _, want := range [][]string{
		{"--machine", "mac-a", "agent", "list"},
		{"--machine", "mac-b", "agent", "list"},
		{"--machine", "win.a_1-2", "agent", "list"},
	} {
		found := false
		for _, c := range agentCalls {
			if equalArgv(c, want) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("agent-list call %v missing from %v", want, agentCalls)
		}
	}
	if len(agentCalls) != 3 {
		t.Errorf("agent-list calls = %v, want exactly the three resolvable labels", agentCalls)
	}
}

// TestProbeConfigExclusions: an invalid label and a disabled label are
// mapped before any probe and never reach the Exec, and a local-only
// remainder runs no catalog.
func TestProbeConfigExclusions(t *testing.T) {
	fe := newFakeExec(func(argv []string) ([]byte, []byte, int, error) {
		t.Errorf("exec must not be called: %v", argv)
		return nil, nil, 1, nil
	})
	f := router.Fleet{
		Machines: []string{"-bad", "mac-a", ""},
		Disabled: []string{"mac-a"},
		Exec:     fe.Run,
	}
	out := f.Probe(context.Background())
	wantCandidate(t, out, 0, "-bad", router.StateUnsupported, nil, router.ReasonUnknownMachine)
	wantCandidate(t, out, 1, "mac-a", router.StateDisabled, nil, router.ReasonDisabledByConfig)
	wantCandidate(t, out, 2, "", router.StateUnsupported, nil, router.ReasonUnknownMachine)
	if got := fe.argvs(); len(got) != 0 {
		t.Errorf("exec calls = %v, want none", got)
	}

	fe2 := newFakeExec(func(argv []string) ([]byte, []byte, int, error) {
		return agentsJSON(), nil, 0, nil
	})
	f2 := router.Fleet{
		Machines: []string{"-bad", router.Local, "mac-a"},
		Disabled: []string{"mac-a"},
		Exec:     fe2.Run,
	}
	out2 := f2.Probe(context.Background())
	wantCandidate(t, out2, 0, "-bad", router.StateUnsupported, nil, router.ReasonUnknownMachine)
	wantCandidate(t, out2, 1, router.Local, router.StateAvailable, countPtr(0), "")
	wantCandidate(t, out2, 2, "mac-a", router.StateDisabled, nil, router.ReasonDisabledByConfig)
	calls := fe2.argvs()
	if len(calls) != 1 || !equalArgv(calls[0], []string{"agent", "list"}) {
		t.Errorf("exec calls = %v, want exactly the local agent list (no catalog)", calls)
	}
}

// TestProbeCatalogErrors: a failed catalog marks every remaining non-local
// label unavailable with the mapped reason, and no agent-list probe runs.
func TestProbeCatalogErrors(t *testing.T) {
	oversize := make([]byte, router.MaxProbeOutput+1)
	cases := []struct {
		name   string
		stdout []byte
		code   int
		err    error
		reason string
	}{
		{"herdr unavailable", nil, 0, fmt.Errorf("spawn failed: %w", router.ErrHerdrUnavailable), router.ReasonHerdrUnavailable},
		{"probe timeout", nil, 0, fmt.Errorf("bound expired: %w", router.ErrProbeTimeout), router.ReasonCatalogUnavailable},
		{"other error", nil, 0, errors.New("boom"), router.ReasonCatalogUnavailable},
		{"non-zero exit", []byte("{}"), 3, nil, router.ReasonCatalogUnavailable},
		{"malformed output", []byte(`{"id":"cli:machine:list","result":{}}`), 0, nil, router.ReasonCatalogMalformed},
		{"oversize output", oversize, 0, nil, router.ReasonCatalogMalformed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fe := newFakeExec(func(argv []string) ([]byte, []byte, int, error) {
				if equalArgv(argv, []string{"machine", "list", "--json"}) {
					return tc.stdout, nil, tc.code, tc.err
				}
				t.Errorf("agent-list probe must not run after a catalog failure: %v", argv)
				return nil, nil, 1, nil
			})
			f := router.Fleet{Machines: []string{"mac-a", "mac-b"}, Exec: fe.Run}
			out := f.Probe(context.Background())
			wantCandidate(t, out, 0, "mac-a", router.StateUnavailable, nil, tc.reason)
			wantCandidate(t, out, 1, "mac-b", router.StateUnavailable, nil, tc.reason)
			if got := fe.argvs(); len(got) != 1 {
				t.Errorf("exec calls = %d, want exactly the catalog", len(got))
			}
		})
	}
}

// TestProbeCatalogErrorMixedFleet: a failed catalog marks the non-local
// labels unavailable; local needs no catalog and keeps its own probe.
func TestProbeCatalogErrorMixedFleet(t *testing.T) {
	fe := newFakeExec(func(argv []string) ([]byte, []byte, int, error) {
		if equalArgv(argv, []string{"machine", "list", "--json"}) {
			return nil, nil, 0, errors.New("boom")
		}
		return agentsJSON(agent{pane: "p1", status: "idle", name: namePtr("orchestrator")}), nil, 0, nil
	})
	f := router.Fleet{Machines: []string{router.Local, "mac-a"}, Exec: fe.Run}
	out := f.Probe(context.Background())
	wantCandidate(t, out, 0, router.Local, router.StateAvailable, countPtr(1), "")
	wantCandidate(t, out, 1, "mac-a", router.StateUnavailable, nil, router.ReasonCatalogUnavailable)
	var agentCalls [][]string
	for _, c := range fe.argvs() {
		if !equalArgv(c, []string{"machine", "list", "--json"}) {
			agentCalls = append(agentCalls, c)
		}
	}
	if len(agentCalls) != 1 || !equalArgv(agentCalls[0], []string{"agent", "list"}) {
		t.Errorf("agent-list calls = %v, want the local probe only", agentCalls)
	}
}

// TestProbeAgentListErrors: a failed, timed-out or malformed agent-list
// probe is unavailable with the mapped reason; a non-zero exit with valid
// JSON stdout is never inferred as zero load.
func TestProbeAgentListErrors(t *testing.T) {
	validList := agentsJSON(
		agent{pane: "p1", status: "idle", name: namePtr("orchestrator")},
		agent{pane: "p2", status: "idle", name: namePtr("orchestrator-2")},
	)
	oversize := make([]byte, router.MaxProbeOutput+1)
	cases := []struct {
		name   string
		stdout []byte
		code   int
		err    error
		reason string
	}{
		{"probe timeout", validList, 0, fmt.Errorf("bound expired: %w", router.ErrProbeTimeout), router.ReasonProbeTimeout},
		{"herdr unavailable", validList, 0, fmt.Errorf("spawn failed: %w", router.ErrHerdrUnavailable), router.ReasonHerdrUnavailable},
		{"other error", validList, 0, errors.New("boom"), router.ReasonProbeFailed},
		{"non-zero with valid json", validList, 3, nil, router.ReasonProbeFailed},
		{"herdr error object", []byte(`{"id":"cli:agent:list","error":{"code":"server_not_running","message":"down"}}`), 0, nil, router.ReasonProbeMalformed},
		{"missing agents", []byte(`{"id":"cli:agent:list","result":{}}`), 0, nil, router.ReasonProbeMalformed},
		{"oversize", oversize, 0, nil, router.ReasonProbeMalformed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fe := newFakeExec(func(argv []string) ([]byte, []byte, int, error) {
				if equalArgv(argv, []string{"machine", "list", "--json"}) {
					return catalogStdout(catEntry{id: "1", label: "mac-a", enabled: true}), nil, 0, nil
				}
				return tc.stdout, nil, tc.code, tc.err
			})
			f := router.Fleet{Machines: []string{"mac-a"}, Exec: fe.Run}
			out := f.Probe(context.Background())
			wantCandidate(t, out, 0, "mac-a", router.StateUnavailable, nil, tc.reason)
		})
	}
}

// TestParseCatalogValid: the verified shape, with the extra fields the
// real CLI prints ignored, and the empty array.
func TestParseCatalogValid(t *testing.T) {
	got, err := router.ParseCatalog([]byte(`[{"id":"e6f8","label":"win-a","target":"t","session":"default","enabled":true,"selected":false}]`))
	if err != nil {
		t.Fatalf("ParseCatalog: %v", err)
	}
	want := []router.CatalogEntry{{ID: "e6f8", Label: "win-a", Enabled: true}}
	if len(got) != 1 || got[0] != want[0] {
		t.Errorf("ParseCatalog = %+v, want %+v", got, want)
	}

	empty, err := router.ParseCatalog([]byte(`[]`))
	if err != nil {
		t.Fatalf("empty array: %v", err)
	}
	if len(empty) != 0 {
		t.Errorf("empty array = %v, want no entries", empty)
	}

	multi, err := router.ParseCatalog(catalogStdout(
		catEntry{id: "1", label: "mac-a", enabled: true},
		catEntry{id: "2", label: "win.a_1-2", enabled: false},
	))
	if err != nil {
		t.Fatalf("ParseCatalog: %v", err)
	}
	if len(multi) != 2 || multi[0] != (router.CatalogEntry{ID: "1", Label: "mac-a", Enabled: true}) || multi[1] != (router.CatalogEntry{ID: "2", Label: "win.a_1-2", Enabled: false}) {
		t.Errorf("ParseCatalog = %+v, want the two entries in order", multi)
	}
}

// TestParseCatalogErrors: anything that is not an array of {id,label,
// enabled} objects is an error.
func TestParseCatalogErrors(t *testing.T) {
	cases := []struct {
		name string
		raw  string
	}{
		{"invalid json", `{`},
		{"truncated", `[{"id":"a"}`},
		{"object not array", `{"id":"a","label":"l","enabled":true}`},
		{"string not array", `"x"`},
		{"number not array", `3`},
		{"null not array", `null`},
		{"whitespace only", `   `},
		{"element number", `[1]`},
		{"element string", `["x"]`},
		{"element null", `[null]`},
		{"missing id", `[{"label":"l","enabled":true}]`},
		{"missing label", `[{"id":"i","enabled":true}]`},
		{"missing enabled", `[{"id":"i","label":"l"}]`},
		{"id number", `[{"id":1,"label":"l","enabled":true}]`},
		{"id null", `[{"id":null,"label":"l","enabled":true}]`},
		{"id object", `[{"id":{},"label":"l","enabled":true}]`},
		{"label number", `[{"id":"i","label":1,"enabled":true}]`},
		{"label null", `[{"id":"i","label":null,"enabled":true}]`},
		{"label bool", `[{"id":"i","label":true,"enabled":true}]`},
		{"enabled string", `[{"id":"i","label":"l","enabled":"yes"}]`},
		{"enabled null", `[{"id":"i","label":"l","enabled":null}]`},
		{"trailing text", `[{"id":"i","label":"l","enabled":true}] x`},
		{"trailing object", `[{"id":"i","label":"l","enabled":true}] {}`},
		{"trailing closing bracket", `[{"id":"i","label":"l","enabled":true}] ]`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := router.ParseCatalog([]byte(tc.raw)); err == nil {
				t.Fatalf("ParseCatalog(%q) = ok, want error", tc.raw)
			}
		})
	}
}

// TestCountOrchestratorsNames: the counted names, whatever their status;
// the brief's whole name table.
func TestCountOrchestratorsNames(t *testing.T) {
	raw := agentsJSON(
		agent{pane: "p1", status: "idle", name: namePtr("orchestrator")},
		agent{pane: "p2", status: "working", name: namePtr("orchestrator-2")},
		agent{pane: "p3", status: "blocked", name: namePtr("orchestrator-10")},
		agent{pane: "p4", status: "done", name: namePtr("orchestrator-0")},
		agent{pane: "p5", status: "unknown", name: namePtr("orchestrator2")},
		agent{pane: "p6", status: "idle", name: namePtr("orchestrator-x")},
		agent{pane: "p7", status: "working", name: namePtr("sub-orchestrator")},
		agent{pane: "p8", status: "idle", name: namePtr("build")},
		agent{pane: "p9", status: "working", name: namePtr("build-2")},
		agent{pane: "p10", status: "idle", nullName: true},
		agent{pane: "p11", status: "idle"},
	)
	if got, err := router.CountOrchestrators(raw, "orchestrator"); err != nil || got != 3 {
		t.Errorf("CountOrchestrators(orchestrator) = %d, %v; want 3", got, err)
	}
	if got, err := router.CountOrchestrators(raw, "build"); err != nil || got != 2 {
		t.Errorf("CountOrchestrators(build) = %d, %v; want 2", got, err)
	}
}

// TestCountOrchestratorsStatusIrrelevant: every one of the five statuses
// is counted.
func TestCountOrchestratorsStatusIrrelevant(t *testing.T) {
	var agents []agent
	for i, s := range []string{"idle", "working", "blocked", "done", "unknown"} {
		agents = append(agents, agent{pane: string(rune('a' + i)), status: s, name: namePtr("orchestrator")})
	}
	if got, err := router.CountOrchestrators(agentsJSON(agents...), "orchestrator"); err != nil || got != 5 {
		t.Errorf("CountOrchestrators = %d, %v; want 5", got, err)
	}
}

// TestCountOrchestratorsEmptyAgents: an empty agents array is valid and
// returns 0.
func TestCountOrchestratorsEmptyAgents(t *testing.T) {
	got, err := router.CountOrchestrators([]byte(`{"id":"cli:agent:list","result":{"agents":[]}}`), "orchestrator")
	if err != nil || got != 0 {
		t.Errorf("CountOrchestrators = %d, %v; want 0, nil", got, err)
	}
}

// TestCountOrchestratorsErrors: every malformed shape is an error.
func TestCountOrchestratorsErrors(t *testing.T) {
	cases := []struct {
		name string
		raw  string
	}{
		{"invalid json", `{`},
		{"top array", `[1]`},
		{"top string", `"x"`},
		{"top number", `3`},
		{"top null", `null`},
		{"trailing data", `{"result":{"agents":[]}} x`},
		{"trailing closing brace", `{"result":{"agents":[]}} }`},
		{"error key", `{"id":"cli:agent:list","error":{"code":"server_not_running","message":"down"}}`},
		{"error key null", `{"error":null}`},
		{"result missing", `{"id":"cli:agent:list"}`},
		{"result array", `{"result":[]}`},
		{"result string", `{"result":"x"}`},
		{"result null", `{"result":null}`},
		{"result number", `{"result":1}`},
		{"agents missing", `{"result":{}}`},
		{"agents object", `{"result":{"agents":{}}}`},
		{"agents string", `{"result":{"agents":"x"}}`},
		{"agents null", `{"result":{"agents":null}}`},
		{"agent number", `{"result":{"agents":[1]}}`},
		{"agent string", `{"result":{"agents":["x"]}}`},
		{"agent null", `{"result":{"agents":[null]}}`},
		{"pane missing", `{"result":{"agents":[{"agent_status":"idle"}]}}`},
		{"pane number", `{"result":{"agents":[{"pane_id":1,"agent_status":"idle"}]}}`},
		{"pane null", `{"result":{"agents":[{"pane_id":null,"agent_status":"idle"}]}}`},
		{"pane empty", `{"result":{"agents":[{"pane_id":"","agent_status":"idle"}]}}`},
		{"status missing", `{"result":{"agents":[{"pane_id":"p1"}]}}`},
		{"status number", `{"result":{"agents":[{"pane_id":"p1","agent_status":1}]}}`},
		{"status null", `{"result":{"agents":[{"pane_id":"p1","agent_status":null}]}}`},
		{"status unknown value", `{"result":{"agents":[{"pane_id":"p1","agent_status":"running"}]}}`},
		{"name number", `{"result":{"agents":[{"pane_id":"p1","agent_status":"idle","name":1}]}}`},
		{"name bool", `{"result":{"agents":[{"pane_id":"p1","agent_status":"idle","name":true}]}}`},
		{"name array", `{"result":{"agents":[{"pane_id":"p1","agent_status":"idle","name":[]}]}}`},
		{"name object", `{"result":{"agents":[{"pane_id":"p1","agent_status":"idle","name":{}}]}}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := router.CountOrchestrators([]byte(tc.raw), "orchestrator"); err == nil {
				t.Fatalf("CountOrchestrators(%q) = ok, want error", tc.raw)
			}
		})
	}
}

// TestProbeTimeouts: every Exec call gets its own context whose deadline
// is the probe timeout (20 s default, or the smaller parent deadline).
func TestProbeTimeouts(t *testing.T) {
	cases := []struct {
		name     string
		timeout  time.Duration
		parent   time.Duration // 0 = no parent deadline
		want     time.Duration
		machines []string
	}{
		{"default", 0, 0, 20 * time.Second, []string{router.Local}},
		{"configured", 3 * time.Second, 0, 3 * time.Second, []string{router.Local}},
		{"negative falls back", -1 * time.Second, 0, 20 * time.Second, []string{router.Local}},
		{"parent deadline wins", 30 * time.Second, 1500 * time.Millisecond, 1500 * time.Millisecond, []string{router.Local}},
		{"catalog and agent list each bounded", 7 * time.Second, 0, 7 * time.Second, []string{router.Local, "mac-a"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fe := newFakeExec(func(argv []string) ([]byte, []byte, int, error) {
				if equalArgv(argv, []string{"machine", "list", "--json"}) {
					return catalogStdout(catEntry{id: "1", label: "mac-a", enabled: true}), nil, 0, nil
				}
				return agentsJSON(), nil, 0, nil
			})
			f := router.Fleet{Machines: tc.machines, Timeout: tc.timeout, Exec: fe.Run}
			ctx := context.Background()
			if tc.parent > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, tc.parent)
				defer cancel()
			}
			f.Probe(ctx)
			deadlines := fe.deadlineList()
			if len(deadlines) == 0 {
				t.Fatalf("no exec calls recorded")
			}
			for i, d := range deadlines {
				if d < 0 {
					t.Errorf("call %d has no deadline, want ~%s", i, tc.want)
				} else if d < tc.want-time.Second/2 || d > tc.want+time.Second/2 {
					t.Errorf("call %d deadline = %v, want ~%v", i, d, tc.want)
				}
			}
		})
	}
}
