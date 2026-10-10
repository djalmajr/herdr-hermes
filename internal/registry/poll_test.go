package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/djalmajr/herdr-hermes/internal/router"
)

// pollTestClock is the fixed instant the poll tests inject, in a
// non-UTC zone (the project never uses Z offsets).
var pollTestClock = time.Date(2026, 10, 10, 12, 0, 0, 0, time.FixedZone("x", -3*3600))

// fakeExec records every argv and the deadline left on its context under
// a mutex, and answers from fn, the pattern of the router tests.
type fakeExec struct {
	mu        sync.Mutex
	calls     [][]string
	deadlines []time.Duration
	fn        func(argv []string) ([]byte, int, error)
}

func newFakeExec(fn func(argv []string) ([]byte, int, error)) *fakeExec {
	return &fakeExec{fn: fn}
}

// Run implements router.Exec.
func (f *fakeExec) Run(ctx context.Context, argv []string) ([]byte, []byte, int, error) {
	dl, ok := ctx.Deadline()
	var left time.Duration
	if ok {
		left = time.Until(dl)
	} else {
		left = -1
	}
	f.mu.Lock()
	f.calls = append(f.calls, append([]string(nil), argv...))
	f.deadlines = append(f.deadlines, left)
	f.mu.Unlock()
	stdout, code, err := f.fn(argv)
	return stdout, nil, code, err
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

// isArgv reports whether argv is the subcommand argv with an optional
// --machine prefix.
func isArgv(argv []string, sub ...string) bool {
	if len(argv) >= 2 && argv[0] == "--machine" {
		argv = argv[2:]
	}
	return equalArgv(argv, sub)
}

// machineOf returns the machine a poll argv is for (Local without the
// --machine prefix).
func machineOf(argv []string) string {
	if len(argv) >= 2 && argv[0] == "--machine" {
		return argv[1]
	}
	return router.Local
}

// Reduced Herdr sample shapes: no real labels, paths or tool names.
const wsListSample = `{"id":"cli:workspace:list","result":{"type":"workspace_list","workspaces":[{"active_tab_id":"w1:t1","agent_status":"working","focused":false,"label":"hm-example-impl-x1","number":1,"pane_count":2,"tab_count":1,"workspace_id":"w1"}]}}`

const agentListSample = `{"id":"cli:agent:list","result":{"agents":[{"agent":"x","agent_status":"blocked","cwd":"/somewhere","name":"orchestrator","pane_id":"w1:p1","tab_id":"w1:t1","terminal_title":"secret title","workspace_id":"w1"}]}}`

const statusServerSample = "status: running\nversion: 0.9.1\nendpoint_compatible: yes\n"

// goodExec answers every poll call with the valid fixtures.
func goodExec(t *testing.T) *fakeExec {
	t.Helper()
	return newFakeExec(func(argv []string) ([]byte, int, error) {
		switch {
		case isArgv(argv, "workspace", "list"):
			return []byte(wsListSample), 0, nil
		case isArgv(argv, "agent", "list"):
			return []byte(agentListSample), 0, nil
		case isArgv(argv, "status", "server"):
			return []byte(statusServerSample), 0, nil
		}
		t.Errorf("unexpected argv %v", argv)
		return nil, 1, nil
	})
}

// TestPollArgv: the local poll runs the three herdr calls without
// --machine, a remote label prefixes each call in the same order, and a
// label that fails LabelPattern is refused before any Exec call.
// Mutation captured: removing the LabelPattern check in Poll makes the
// bad-label case run the Exec.
func TestPollArgv(t *testing.T) {
	p := Poller{Now: func() time.Time { return pollTestClock }}

	fe := goodExec(t)
	p.Exec = fe.Run
	got := p.Poll(context.Background(), router.Local)
	if !got.OK {
		t.Fatalf("local poll OK = false: %q", got.Motivo)
	}
	want := [][]string{{"workspace", "list"}, {"agent", "list"}, {"status", "server"}}
	gotCalls := fe.argvs()
	if len(gotCalls) != len(want) {
		t.Fatalf("exec calls = %v, want %v", gotCalls, want)
	}
	for i := range want {
		if !equalArgv(gotCalls[i], want[i]) {
			t.Errorf("call %d = %v, want %v", i, gotCalls[i], want[i])
		}
	}
	if !got.At.Equal(pollTestClock) {
		t.Errorf("At = %v, want the injected clock", got.At)
	}
	if got.HerdrVersion != "0.9.1" {
		t.Errorf("HerdrVersion = %q, want 0.9.1", got.HerdrVersion)
	}
	if len(got.Workspaces) != 1 || got.Workspaces[0] != (PolledWorkspace{ID: "w1", Label: "hm-example-impl-x1", AgentStatus: "working"}) {
		t.Errorf("Workspaces = %+v", got.Workspaces)
	}
	if len(got.Panes) != 1 || got.Panes[0] != (PolledPane{PaneID: "w1:p1", WorkspaceID: "w1", Name: "orchestrator", AgentStatus: "blocked"}) {
		t.Errorf("Panes = %+v", got.Panes)
	}

	fe2 := goodExec(t)
	p.Exec = fe2.Run
	got2 := p.Poll(context.Background(), "node-b")
	if !got2.OK {
		t.Fatalf("remote poll OK = false: %q", got2.Motivo)
	}
	want2 := [][]string{
		{"--machine", "node-b", "workspace", "list"},
		{"--machine", "node-b", "agent", "list"},
		{"--machine", "node-b", "status", "server"},
	}
	gotCalls2 := fe2.argvs()
	if len(gotCalls2) != len(want2) {
		t.Fatalf("exec calls = %v, want %v", gotCalls2, want2)
	}
	for i := range want2 {
		if !equalArgv(gotCalls2[i], want2[i]) {
			t.Errorf("call %d = %v, want %v", i, gotCalls2[i], want2[i])
		}
	}

	fe3 := newFakeExec(func(argv []string) ([]byte, int, error) {
		t.Errorf("exec must not run for a bad label: %v", argv)
		return nil, 1, nil
	})
	p.Exec = fe3.Run
	got3 := p.Poll(context.Background(), "-x")
	if got3.OK {
		t.Errorf("bad label OK = true, want false")
	}
	if got3.Motivo != router.ReasonUnknownMachine {
		t.Errorf("bad label Motivo = %q, want %q", got3.Motivo, router.ReasonUnknownMachine)
	}
	if len(fe3.argvs()) != 0 {
		t.Errorf("exec calls = %v, want none", fe3.argvs())
	}
	if !got3.At.Equal(pollTestClock) {
		t.Errorf("At = %v, want the injected clock", got3.At)
	}
}

// TestPollMapping: every failure class maps to its reason code; a
// workspace-list failure skips the agent list; a status-server failure
// leaves the version empty but the poll ok; At comes from the injected
// clock.
// Mutation captured: swapping the probe_timeout and probe_failed arms of
// the poll run mapping makes the timeout case fail.
func TestPollMapping(t *testing.T) {
	oversize := make([]byte, router.MaxProbeOutput+1)
	cases := []struct {
		name string
		sub  []string
		out  []byte
		code int
		err  error
		want string
	}{
		{"workspace timeout", []string{"workspace", "list"}, nil, 0, fmt.Errorf("bound expired: %w", router.ErrProbeTimeout), router.ReasonProbeTimeout},
		{"workspace unavailable", []string{"workspace", "list"}, nil, 0, fmt.Errorf("spawn failed: %w", router.ErrHerdrUnavailable), router.ReasonHerdrUnavailable},
		{"workspace other error", []string{"workspace", "list"}, nil, 0, errors.New("boom"), router.ReasonProbeFailed},
		{"workspace non-zero exit", []string{"workspace", "list"}, []byte(wsListSample), 3, nil, router.ReasonProbeFailed},
		{"workspace oversize", []string{"workspace", "list"}, oversize, 0, nil, router.ReasonProbeMalformed},
		{"workspace invalid json", []string{"workspace", "list"}, []byte(`{`), 0, nil, router.ReasonProbeMalformed},
		{"workspace herdr error", []string{"workspace", "list"}, []byte(`{"id":"cli:workspace:list","error":{"code":"server_not_running","message":"down"}}`), 0, nil, router.ReasonProbeMalformed},
		{"agent oversize", []string{"agent", "list"}, oversize, 0, nil, router.ReasonProbeMalformed},
		{"agent unknown status", []string{"agent", "list"}, []byte(`{"id":"cli:agent:list","result":{"agents":[{"pane_id":"w1:p1","agent_status":"running","name":"orchestrator"}]}}`), 0, nil, router.ReasonProbeMalformed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fe := newFakeExec(func(argv []string) ([]byte, int, error) {
				if isArgv(argv, tc.sub...) {
					return tc.out, tc.code, tc.err
				}
				switch {
				case isArgv(argv, "workspace", "list"):
					return []byte(wsListSample), 0, nil
				case isArgv(argv, "agent", "list"):
					return []byte(agentListSample), 0, nil
				case isArgv(argv, "status", "server"):
					return []byte(statusServerSample), 0, nil
				}
				t.Errorf("unexpected argv %v", argv)
				return nil, 1, nil
			})
			p := Poller{Exec: fe.Run, Now: func() time.Time { return pollTestClock }}
			got := p.Poll(context.Background(), router.Local)
			if got.OK {
				t.Fatalf("poll OK = true, want false")
			}
			if got.Motivo != tc.want {
				t.Errorf("Motivo = %q, want %q", got.Motivo, tc.want)
			}
			if len(got.Workspaces) != 0 || len(got.Panes) != 0 {
				t.Errorf("lists not empty on failure: %+v %+v", got.Workspaces, got.Panes)
			}
			if !got.At.Equal(pollTestClock) {
				t.Errorf("At = %v, want the injected clock", got.At)
			}
			if equalArgv(tc.sub, []string{"workspace", "list"}) {
				for _, c := range fe.argvs() {
					if isArgv(c, "agent", "list") {
						t.Errorf("agent list ran after a workspace-list failure: %v", c)
					}
				}
			}
		})
	}

	// A status-server failure in any way only leaves the version empty.
	fe := newFakeExec(func(argv []string) ([]byte, int, error) {
		if isArgv(argv, "status", "server") {
			return []byte(statusServerSample), 2, nil
		}
		switch {
		case isArgv(argv, "workspace", "list"):
			return []byte(wsListSample), 0, nil
		case isArgv(argv, "agent", "list"):
			return []byte(agentListSample), 0, nil
		}
		t.Errorf("unexpected argv %v", argv)
		return nil, 1, nil
	})
	p := Poller{Exec: fe.Run, Now: func() time.Time { return pollTestClock }}
	got := p.Poll(context.Background(), "node-b")
	if !got.OK {
		t.Fatalf("poll OK = false, want true: %q", got.Motivo)
	}
	if got.HerdrVersion != "" {
		t.Errorf("HerdrVersion = %q, want empty after a status-server failure", got.HerdrVersion)
	}
	if got.Motivo != "" {
		t.Errorf("Motivo = %q, want empty", got.Motivo)
	}
	if len(got.Workspaces) != 1 || len(got.Panes) != 1 {
		t.Errorf("lists = %+v, %+v; want the parsed entries", got.Workspaces, got.Panes)
	}
}

// TestPollTimeouts: every herdr call runs under its own context with the
// configured timeout (20 s default) or the smaller parent deadline.
// Mutation captured: dropping the WithTimeout wrapper around one Exec
// call leaves that call without a deadline.
func TestPollTimeouts(t *testing.T) {
	cases := []struct {
		name    string
		timeout time.Duration
		parent  time.Duration // 0 = no parent deadline
		want    time.Duration
	}{
		{"default", 0, 0, 20 * time.Second},
		{"configured", 3 * time.Second, 0, 3 * time.Second},
		{"negative falls back", -1 * time.Second, 0, 20 * time.Second},
		{"parent deadline wins", 30 * time.Second, 1500 * time.Millisecond, 1500 * time.Millisecond},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fe := goodExec(t)
			p := Poller{Exec: fe.Run, Timeout: tc.timeout}
			ctx := context.Background()
			if tc.parent > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, tc.parent)
				defer cancel()
			}
			p.Poll(ctx, router.Local)
			deadlines := fe.deadlineList()
			if len(deadlines) != 3 {
				t.Fatalf("exec calls = %d, want 3", len(deadlines))
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

// TestPollAll: the machines are polled concurrently — the fake blocks
// each workspace list until every machine has started its own, with a
// bounded wait so the test cannot hang — and the results come back in
// input order.
// Mutation captured: running the machines one at a time (no per-machine
// goroutine) lets the first machine hit the barrier bound and come back
// probe_timeout.
func TestPollAll(t *testing.T) {
	machines := []string{router.Local, "node-b", "node-c"}
	labelByMachine := map[string]string{
		router.Local: "hm-example-impl-x1",
		"node-b":     "hm-example-impl-x2",
		"node-c":     "hm-example-impl-x3",
	}
	var barrierMu sync.Mutex
	started := map[string]bool{}
	allStarted := make(chan struct{})
	var closeOnce sync.Once

	fe := newFakeExec(func(argv []string) ([]byte, int, error) {
		if isArgv(argv, "workspace", "list") {
			m := machineOf(argv)
			barrierMu.Lock()
			started[m] = true
			n := len(started)
			barrierMu.Unlock()
			if n < len(machines) {
				select {
				case <-allStarted:
				case <-time.After(2 * time.Second):
					return nil, 0, fmt.Errorf("barrier not reached: %w", router.ErrProbeTimeout)
				}
			} else {
				closeOnce.Do(func() { close(allStarted) })
			}
		}
		switch {
		case isArgv(argv, "workspace", "list"):
			label := labelByMachine[machineOf(argv)]
			raw := fmt.Sprintf(`{"id":"cli:workspace:list","result":{"type":"workspace_list","workspaces":[{"agent_status":"working","label":%q,"workspace_id":"w1"}]}}`, label)
			return []byte(raw), 0, nil
		case isArgv(argv, "agent", "list"):
			return []byte(agentListSample), 0, nil
		case isArgv(argv, "status", "server"):
			return []byte(statusServerSample), 0, nil
		}
		t.Errorf("unexpected argv %v", argv)
		return nil, 1, nil
	})
	p := Poller{Exec: fe.Run, Now: func() time.Time { return pollTestClock }}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	out := p.PollAll(ctx, machines)
	if len(out) != len(machines) {
		t.Fatalf("results = %d, want %d", len(out), len(machines))
	}
	for i, r := range out {
		if r.Maquina != machines[i] {
			t.Errorf("result %d Maquina = %q, want %q (input order)", i, r.Maquina, machines[i])
		}
		if !r.OK {
			t.Errorf("result %d OK = false: %q", i, r.Motivo)
		}
		wantLabel := labelByMachine[machines[i]]
		if len(r.Workspaces) != 1 || r.Workspaces[0].Label != wantLabel {
			t.Errorf("result %d Workspaces = %+v, want label %q", i, r.Workspaces, wantLabel)
		}
	}
	if got := p.PollAll(ctx, nil); len(got) != 0 {
		t.Errorf("PollAll(nil) = %v, want empty", got)
	}
}

// TestParseWorkspaceList: the verified shape parses and keeps only id,
// label and agent_status; every rejection rule of the design is an
// error.
// Mutation captured: treating an empty workspace_id as valid makes the
// "workspace_id empty" case pass.
func TestParseWorkspaceList(t *testing.T) {
	valid := []struct {
		name string
		raw  string
		want []PolledWorkspace
	}{
		{"sample", wsListSample, []PolledWorkspace{{ID: "w1", Label: "hm-example-impl-x1", AgentStatus: "working"}}},
		{"empty workspaces", `{"id":"cli:workspace:list","result":{"type":"workspace_list","workspaces":[]}}`, []PolledWorkspace{}},
		{"missing label", `{"result":{"type":"workspace_list","workspaces":[{"agent_status":"idle","workspace_id":"w2"}]}}`, []PolledWorkspace{{ID: "w2", Label: "", AgentStatus: "idle"}}},
	}
	for _, tc := range valid {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseWorkspaceList([]byte(tc.raw))
			if err != nil {
				t.Fatalf("ParseWorkspaceList: %v", err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("workspaces = %+v, want %+v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("workspace %d = %+v, want %+v", i, got[i], tc.want[i])
				}
			}
		})
	}

	t.Run("all five statuses", func(t *testing.T) {
		raw := `{"result":{"type":"workspace_list","workspaces":[`
		for i, s := range []string{"idle", "working", "blocked", "done", "unknown"} {
			if i > 0 {
				raw += ","
			}
			raw += fmt.Sprintf(`{"agent_status":%q,"workspace_id":"w%d"}`, s, i)
		}
		raw += "]}}"
		got, err := ParseWorkspaceList([]byte(raw))
		if err != nil {
			t.Fatalf("ParseWorkspaceList: %v", err)
		}
		if len(got) != 5 {
			t.Fatalf("workspaces = %+v, want 5", got)
		}
		for i, s := range []string{"idle", "working", "blocked", "done", "unknown"} {
			if got[i].AgentStatus != s {
				t.Errorf("workspace %d status = %q, want %q", i, got[i].AgentStatus, s)
			}
		}
	})

	invalid := []struct {
		name string
		raw  string
	}{
		{"invalid json", `{`},
		{"top array", `[]`},
		{"top string", `"x"`},
		{"top number", `3`},
		{"top null", `null`},
		{"trailing text", `{"result":{"type":"workspace_list","workspaces":[]}} x`},
		{"trailing brace", `{"result":{"type":"workspace_list","workspaces":[]}} }`},
		{"error key", `{"id":"cli:workspace:list","error":{"code":"server_not_running","message":"down"}}`},
		{"error key null", `{"error":null}`},
		{"result missing", `{"id":"cli:workspace:list"}`},
		{"result array", `{"result":[]}`},
		{"result string", `{"result":"x"}`},
		{"result null", `{"result":null}`},
		{"result number", `{"result":1}`},
		{"workspaces missing", `{"result":{"type":"workspace_list"}}`},
		{"workspaces object", `{"result":{"workspaces":{}}}`},
		{"workspaces string", `{"result":{"workspaces":"x"}}`},
		{"workspaces null", `{"result":{"workspaces":null}}`},
		{"element number", `{"result":{"workspaces":[1]}}`},
		{"element string", `{"result":{"workspaces":["x"]}}`},
		{"element null", `{"result":{"workspaces":[null]}}`},
		{"workspace_id missing", `{"result":{"workspaces":[{"agent_status":"idle"}]}}`},
		{"workspace_id empty", `{"result":{"workspaces":[{"agent_status":"idle","workspace_id":""}]}}`},
		{"workspace_id number", `{"result":{"workspaces":[{"agent_status":"idle","workspace_id":1}]}}`},
		{"workspace_id null", `{"result":{"workspaces":[{"agent_status":"idle","workspace_id":null}]}}`},
		{"agent_status missing", `{"result":{"workspaces":[{"workspace_id":"w1"}]}}`},
		{"agent_status number", `{"result":{"workspaces":[{"workspace_id":"w1","agent_status":1}]}}`},
		{"agent_status null", `{"result":{"workspaces":[{"workspace_id":"w1","agent_status":null}]}}`},
		{"agent_status unknown value", `{"result":{"workspaces":[{"workspace_id":"w1","agent_status":"running"}]}}`},
		{"label number", `{"result":{"workspaces":[{"workspace_id":"w1","agent_status":"idle","label":1}]}}`},
		{"label null", `{"result":{"workspaces":[{"workspace_id":"w1","agent_status":"idle","label":null}]}}`},
		{"label array", `{"result":{"workspaces":[{"workspace_id":"w1","agent_status":"idle","label":[]}]}}`},
	}
	for _, tc := range invalid {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ParseWorkspaceList([]byte(tc.raw)); err == nil {
				t.Fatalf("ParseWorkspaceList(%q) = ok, want error", tc.raw)
			}
		})
	}
}

// TestParseAgentList: the same shape rules as the router agent list plus
// workspace_id, and the parsed result never carries cwd, titles or
// terminal fields.
// Mutation captured: accepting an agent_status outside the five makes
// the "status unknown value" case pass.
func TestParseAgentList(t *testing.T) {
	t.Run("sample keeps only the allowlisted fields", func(t *testing.T) {
		got, err := ParseAgentList([]byte(agentListSample))
		if err != nil {
			t.Fatalf("ParseAgentList: %v", err)
		}
		if len(got) != 1 {
			t.Fatalf("panes = %+v, want 1", got)
		}
		if got[0] != (PolledPane{PaneID: "w1:p1", WorkspaceID: "w1", Name: "orchestrator", AgentStatus: "blocked"}) {
			t.Errorf("pane = %+v", got[0])
		}
		// The round-trip JSON must carry no cwd, title or other field.
		data, err := json.Marshal(got)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		for _, leak := range []string{"/somewhere", "secret", "terminal_title", "cwd"} {
			if strings.Contains(string(data), leak) {
				t.Errorf("parsed panes leak %q: %s", leak, data)
			}
		}
	})

	valid := []struct {
		name string
		raw  string
		want []PolledPane
	}{
		{"empty agents", `{"id":"cli:agent:list","result":{"agents":[]}}`, []PolledPane{}},
		{"name null", `{"result":{"agents":[{"pane_id":"w1:p1","agent_status":"idle","name":null,"workspace_id":"w1"}]}}`, []PolledPane{{PaneID: "w1:p1", WorkspaceID: "w1", Name: "", AgentStatus: "idle"}}},
		{"name missing", `{"result":{"agents":[{"pane_id":"w1:p1","agent_status":"idle","workspace_id":"w1"}]}}`, []PolledPane{{PaneID: "w1:p1", WorkspaceID: "w1", Name: "", AgentStatus: "idle"}}},
		{"workspace_id missing", `{"result":{"agents":[{"pane_id":"w1:p1","agent_status":"idle","name":"x"}]}}`, []PolledPane{{PaneID: "w1:p1", WorkspaceID: "", Name: "x", AgentStatus: "idle"}}},
		{"workspace_id empty", `{"result":{"agents":[{"pane_id":"w1:p1","agent_status":"idle","name":"x","workspace_id":""}]}}`, []PolledPane{{PaneID: "w1:p1", WorkspaceID: "", Name: "x", AgentStatus: "idle"}}},
	}
	for _, tc := range valid {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseAgentList([]byte(tc.raw))
			if err != nil {
				t.Fatalf("ParseAgentList: %v", err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("panes = %+v, want %+v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("pane %d = %+v, want %+v", i, got[i], tc.want[i])
				}
			}
		})
	}

	t.Run("all five statuses", func(t *testing.T) {
		raw := `{"result":{"agents":[`
		for i, s := range []string{"idle", "working", "blocked", "done", "unknown"} {
			if i > 0 {
				raw += ","
			}
			raw += fmt.Sprintf(`{"pane_id":"w1:p%d","agent_status":%q,"name":"x","workspace_id":"w1"}`, i+1, s)
		}
		raw += "]}}"
		if got, err := ParseAgentList([]byte(raw)); err != nil || len(got) != 5 {
			t.Errorf("ParseAgentList = %+v, %v; want 5 panes", got, err)
		}
	})

	invalid := []struct {
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
		{"status missing", `{"result":{"agents":[{"pane_id":"w1:p1"}]}}`},
		{"status number", `{"result":{"agents":[{"pane_id":"w1:p1","agent_status":1}]}}`},
		{"status null", `{"result":{"agents":[{"pane_id":"w1:p1","agent_status":null}]}}`},
		{"status unknown value", `{"result":{"agents":[{"pane_id":"w1:p1","agent_status":"running"}]}}`},
		{"name number", `{"result":{"agents":[{"pane_id":"w1:p1","agent_status":"idle","name":1}]}}`},
		{"name bool", `{"result":{"agents":[{"pane_id":"w1:p1","agent_status":"idle","name":true}]}}`},
		{"name array", `{"result":{"agents":[{"pane_id":"w1:p1","agent_status":"idle","name":[]}]}}`},
		{"name object", `{"result":{"agents":[{"pane_id":"w1:p1","agent_status":"idle","name":{}}]}}`},
		{"workspace_id number", `{"result":{"agents":[{"pane_id":"w1:p1","agent_status":"idle","workspace_id":1}]}}`},
		{"workspace_id null", `{"result":{"agents":[{"pane_id":"w1:p1","agent_status":"idle","workspace_id":null}]}}`},
	}
	for _, tc := range invalid {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ParseAgentList([]byte(tc.raw)); err == nil {
				t.Fatalf("ParseAgentList(%q) = ok, want error", tc.raw)
			}
		})
	}
}

// TestParseServerVersion: the first version line wins; its trimmed value
// must match the version pattern, else the result is empty.
// Mutation captured: widening the version pattern (for example allowing
// spaces) makes the "value with space" case return a value.
func TestParseServerVersion(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want string
	}{
		{"sample", statusServerSample, "0.9.1"},
		{"first line wins", "version: 1.0.0\nversion: 2.0.0\n", "1.0.0"},
		{"extra spaces", "status: running\nversion:   0.9.1\n", "0.9.1"},
		{"crlf lines", "status: running\r\nversion: 0.9.1\r\n", "0.9.1"},
		{"value with space", "version: 0.9.1 extra\n", ""},
		{"value too long", "version: " + strings.Repeat("a", 41) + "\n", ""},
		{"value at the limit", "version: " + strings.Repeat("a", 40) + "\n", strings.Repeat("a", 40)},
		{"value bad character", "version: 0.9.1_\n", ""},
		{"no version line", "status: running\nendpoint_compatible: yes\n", ""},
		{"empty output", "", ""},
		{"version prefix only", "version:\n", ""},
		{"first line bad value", "version: 0.9.1 extra\nversion: 0.9.1\n", ""},
		{"similar line", "herdr_version: 0.9.1\n", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ParseServerVersion([]byte(tc.raw)); got != tc.want {
				t.Errorf("ParseServerVersion(%q) = %q, want %q", tc.raw, got, tc.want)
			}
		})
	}
}
