package notify

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

// TestParseWorkspaceEventShapes: every accepted shape (socket subscription,
// event-stream and plugin nested/flat), created and closed, with the
// discarded fields (agent kinds, titles, tokens, worktree, counts).
func TestParseWorkspaceEventShapes(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		hook string
		want WorkspaceEvent
	}{
		{
			name: "socket subscription created",
			raw:  `{"event":"workspace.created","data":{"type":"workspace_created","workspace":{"workspace_id":"w2","label":"job-1","title":"a title","agent_kind":"agent-a","tokens":7,"worktree":"/tmp/wt","counts":{"open":1}}}}`,
			want: WorkspaceEvent{Event: WorkspaceCreated, Workspace: "w2", Label: "job-1"},
		},
		{
			name: "socket subscription created envelope name only",
			raw:  `{"event":"workspace.created","data":{"workspace":{"workspace_id":"w2","label":"job-1"}}}`,
			want: WorkspaceEvent{Event: WorkspaceCreated, Workspace: "w2", Label: "job-1"},
		},
		{
			name: "socket subscription closed envelope wins over data type",
			raw:  `{"event":"workspace.created","data":{"type":"workspace_closed","workspace_id":"w2","label":"job-1"}}`,
			want: WorkspaceEvent{Event: WorkspaceCreated, Workspace: "w2", Label: "job-1"},
		},
		{
			name: "socket subscription closed workspace null",
			raw:  `{"event":"workspace.closed","data":{"type":"workspace_closed","workspace_id":"w2","workspace":null}}`,
			want: WorkspaceEvent{Event: WorkspaceClosed, Workspace: "w2", Label: ""},
		},
		{
			name: "socket subscription closed with workspace object",
			raw:  `{"event":"workspace.closed","data":{"type":"workspace_closed","workspace":{"workspace_id":"w2","label":"job-1"}}}`,
			want: WorkspaceEvent{Event: WorkspaceClosed, Workspace: "w2", Label: "job-1"},
		},
		{
			name: "event stream created",
			raw:  `{"type":"workspace_created","workspace":{"workspace_id":"w2","label":"job-1","extra":{"a":1}}}`,
			want: WorkspaceEvent{Event: WorkspaceCreated, Workspace: "w2", Label: "job-1"},
		},
		{
			name: "event stream closed workspace null",
			raw:  `{"type":"workspace_closed","workspace_id":"w3","workspace":null}`,
			want: WorkspaceEvent{Event: WorkspaceClosed, Workspace: "w3", Label: ""},
		},
		{
			name: "event stream closed with workspace",
			raw:  `{"type":"workspace_closed","workspace":{"workspace_id":"w3","label":"job-1"}}`,
			want: WorkspaceEvent{Event: WorkspaceClosed, Workspace: "w3", Label: "job-1"},
		},
		{
			name: "plugin nested with hook created",
			raw:  `{"workspace":{"label":"job-1","workspace_id":"w2"}}`,
			hook: "workspace.created",
			want: WorkspaceEvent{Event: WorkspaceCreated, Workspace: "w2", Label: "job-1"},
		},
		{
			name: "plugin nested with hook closed",
			raw:  `{"workspace":{"label":"job-1","workspace_id":"w2"}}`,
			hook: "workspace.closed",
			want: WorkspaceEvent{Event: WorkspaceClosed, Workspace: "w2", Label: "job-1"},
		},
		{
			name: "plugin flat with hook created",
			raw:  `{"label":"job-1","workspace_id":"w2"}`,
			hook: "workspace.created",
			want: WorkspaceEvent{Event: WorkspaceCreated, Workspace: "w2", Label: "job-1"},
		},
		{
			name: "plugin flat closed without label",
			raw:  `{"workspace_id":"w2"}`,
			hook: "workspace.closed",
			want: WorkspaceEvent{Event: WorkspaceClosed, Workspace: "w2", Label: ""},
		},
		{
			name: "plugin flat closed with empty label",
			raw:  `{"label":"","workspace_id":"w2"}`,
			hook: "workspace.closed",
			want: WorkspaceEvent{Event: WorkspaceClosed, Workspace: "w2", Label: ""},
		},
		{
			name: "data level workspace id and label",
			raw:  `{"event":"workspace.closed","data":{"workspace_id":"w4","label":"job-1","workspace":null}}`,
			want: WorkspaceEvent{Event: WorkspaceClosed, Workspace: "w4", Label: "job-1"},
		},
		{
			name: "workspace id wins over data level",
			raw:  `{"type":"workspace_created","workspace_id":"w-other","workspace":{"workspace_id":"w2","label":"job-1"}}`,
			want: WorkspaceEvent{Event: WorkspaceCreated, Workspace: "w2", Label: "job-1"},
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := ParseWorkspaceEvent([]byte(c.raw), c.hook)
			if err != nil {
				t.Fatalf("err = %v, want nil", err)
			}
			if got != c.want {
				t.Fatalf("got = %+v, want %+v", got, c.want)
			}
		})
	}
}

// TestParseWorkspaceEventOtherEvent: an envelope, data type or hook that
// names another event is ErrNotWorkspace.
func TestParseWorkspaceEventOtherEvent(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		hook string
	}{
		{"envelope other event", `{"event":"pane.exited","data":{"type":"workspace_created","workspace":{"workspace_id":"w2","label":"job-1"}}}`, ""},
		{"envelope other underscored event", `{"event":"pane_agent_status_changed","data":{"workspace":{"workspace_id":"w2","label":"job-1"}}}`, ""},
		{"data type other event", `{"type":"pane_agent_status_changed","workspace":{"workspace_id":"w2","label":"job-1"}}`, ""},
		{"hook other event", `{"label":"job-1","workspace_id":"w2"}`, "pane.exited"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			_, err := ParseWorkspaceEvent([]byte(c.raw), c.hook)
			if err == nil {
				t.Fatal("err = nil, want ErrNotWorkspace")
			}
			if !errors.Is(err, ErrNotWorkspace) {
				t.Fatalf("err = %v, want ErrNotWorkspace", err)
			}
		})
	}
}

// TestParseWorkspaceEventInvalid: malformed JSON, missing names and
// invalid fields are plain errors, not ErrNotWorkspace.
func TestParseWorkspaceEventInvalid(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		hook string
	}{
		{"not json", `not json at all`, ""},
		{"json array", `[1,2]`, ""},
		{"json string", `"w2"`, ""},
		{"json number", `7`, ""},
		{"json null", `null`, ""},
		{"empty", ``, ""},
		{"no name at all", `{"workspace":{"workspace_id":"w2","label":"job-1"}}`, ""},
		{"empty object no name", `{}`, ""},
		{"event name not a string", `{"event":42,"data":{"workspace":{"workspace_id":"w2","label":"job-1"}}}`, ""},
		{"type not a string", `{"type":42,"workspace":{"workspace_id":"w2","label":"job-1"}}`, ""},
		{"envelope with no data", `{"event":"workspace.created"}`, ""},
		{"envelope with null data", `{"event":"workspace.created","data":null}`, ""},
		{"envelope with string data", `{"event":"workspace.created","data":"w2"}`, ""},
		{"invalid workspace id", `{"type":"workspace_created","workspace":{"workspace_id":"bad ws","label":"job-1"}}`, ""},
		{"workspace id too long", `{"type":"workspace_created","workspace":{"workspace_id":"` + strings.Repeat("w", 65) + `","label":"job-1"}}`, ""},
		{"missing workspace id", `{"type":"workspace_created","workspace":{"label":"job-1"}}`, ""},
		{"workspace id not a string", `{"type":"workspace_created","workspace":{"workspace_id":7,"label":"job-1"}}`, ""},
		{"created without label", `{"type":"workspace_created","workspace":{"workspace_id":"w2"}}`, ""},
		{"created with empty label", `{"type":"workspace_created","workspace":{"workspace_id":"w2","label":""}}`, ""},
		{"created without label via hook", `{"workspace_id":"w2"}`, "workspace.created"},
		{"label not a string", `{"type":"workspace_created","workspace":{"workspace_id":"w2","label":7}}`, ""},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			_, err := ParseWorkspaceEvent([]byte(c.raw), c.hook)
			if err == nil {
				t.Fatal("err = nil, want error")
			}
			if errors.Is(err, ErrNotWorkspace) {
				t.Fatalf("err = %v, must not be ErrNotWorkspace", err)
			}
		})
	}
}

// TestProjectWorkspaceTable: both classes, the field contract, the
// per-workspace N in the source id and the stable ids.
func TestProjectWorkspaceTable(t *testing.T) {
	in := WorkspaceInput{Event: WorkspaceCreated, Workspace: "w2", N: 1, JobID: "job-1", Projeto: "example-org/example-repo"}
	n, err := ProjectWorkspace(in, fullRegistry(), fixedNow)
	if err != nil || n == nil {
		t.Fatalf("err = %v, n = %v; want projected", err, n)
	}
	if n.Class != ClassWorkspaceOpened {
		t.Errorf("class = %q, want %q", n.Class, ClassWorkspaceOpened)
	}
	wantEscs(t, n.Escalations)
	if n.SourceKind != SourceWorkspace {
		t.Errorf("source kind = %q, want %q", n.SourceKind, SourceWorkspace)
	}
	if n.SourceID != "workspace:w2:1:opened" {
		t.Errorf("source id = %q, want workspace:w2:1:opened", n.SourceID)
	}
	if n.ID != "n103ac05f9abaf550d1e8" {
		t.Errorf("id = %q, want n103ac05f9abaf550d1e8", n.ID)
	}
	if n.JobID != "job-1" || n.Projeto != "example-org/example-repo" {
		t.Errorf("job/projeto = %q/%q", n.JobID, n.Projeto)
	}
	if n.Workspace != "w2" {
		t.Errorf("workspace = %q, want w2", n.Workspace)
	}
	if n.Pane != "" || n.Status != "" || n.EventTipo != "" {
		t.Errorf("job event fields leaked into workspace: %+v", n)
	}
	if n.SourceTS != "" || n.ObservedAt != fixedNowTS || n.PersistedAt != "" {
		t.Errorf("timestamps = %q/%q/%q", n.SourceTS, n.ObservedAt, n.PersistedAt)
	}
	if len(n.Deliveries) != 2 {
		t.Fatalf("deliveries = %d, want 2 (owner, orchestrator, no coordinator)", len(n.Deliveries))
	}
	if n.Deliveries[0].Ref != "local/w1:p3" || n.Deliveries[0].State != StatePending || n.Deliveries[0].Roles[0] != RoleOwner {
		t.Errorf("owner delivery = %+v, want pending local/w1:p3", n.Deliveries[0])
	}
	if n.Deliveries[1].Ref != "owner-agent" || n.Deliveries[1].State != StatePending || n.Deliveries[1].Roles[0] != RoleOrchestrator {
		t.Errorf("orchestrator delivery = %+v, want pending owner-agent", n.Deliveries[1])
	}

	closed := WorkspaceInput{Event: WorkspaceClosed, Workspace: "w2", N: 1, JobID: "job-1", Projeto: "example-org/example-repo"}
	n2, err := ProjectWorkspace(closed, fullRegistry(), fixedNow)
	if err != nil || n2 == nil {
		t.Fatalf("err = %v, n = %v; want projected", err, n2)
	}
	if n2.Class != ClassWorkspaceClosed {
		t.Errorf("class = %q, want %q", n2.Class, ClassWorkspaceClosed)
	}
	if n2.SourceID != "workspace:w2:1:closed" || n2.ID != "na1541303e2b6e5b0c126" {
		t.Errorf("source/id = %q/%q, want workspace:w2:1:closed / na1541303e2b6e5b0c126", n2.SourceID, n2.ID)
	}
	// A later reopen of the same workspace id (a higher N) is distinct.
	n3, err := ProjectWorkspace(WorkspaceInput{Event: WorkspaceCreated, Workspace: "w2", N: 3, JobID: "job-1", Projeto: "example-org/example-repo"}, fullRegistry(), fixedNow)
	if err != nil || n3 == nil {
		t.Fatalf("err = %v, n = %v; want projected", err, n3)
	}
	if n3.SourceID != "workspace:w2:3:opened" || n3.ID != "nc790d9e182fa54f51948" {
		t.Errorf("source/id = %q/%q, want workspace:w2:3:opened / nc790d9e182fa54f51948", n3.SourceID, n3.ID)
	}
	// Stable ids: the same input gives the same id; opened, closed and a
	// different N are distinct.
	again, err := ProjectWorkspace(in, fullRegistry(), fixedNow)
	if err != nil || again.ID != n.ID {
		t.Fatalf("replay id = %q, err = %v; want %q", again.ID, err, n.ID)
	}
	if n.ID == n2.ID || n.ID == n3.ID {
		t.Fatal("distinct transitions mapped to the same id")
	}
}

// TestProjectWorkspaceEmptyJob: a registered workspace without a job is
// allowed: the job token is omitted and the orchestrator delivery is
// no_route as usual.
func TestProjectWorkspaceEmptyJob(t *testing.T) {
	n, err := ProjectWorkspace(WorkspaceInput{Event: WorkspaceCreated, Workspace: "w2", N: 1, Projeto: "example-org/example-repo"}, fullRegistry(), fixedNow)
	if err != nil || n == nil {
		t.Fatalf("err = %v, n = %v; want projected (an empty job is allowed)", err, n)
	}
	if n.JobID != "" {
		t.Errorf("job id = %q, want empty", n.JobID)
	}
	wantEscs(t, n.Escalations)
	if len(n.Deliveries) != 2 {
		t.Fatalf("deliveries = %d, want 2", len(n.Deliveries))
	}
	if n.Deliveries[0].Ref != "local/w1:p3" || n.Deliveries[0].State != StatePending {
		t.Errorf("owner delivery = %+v, want pending local/w1:p3", n.Deliveries[0])
	}
	if n.Deliveries[1].State != StateNoRoute || n.Deliveries[1].Roles[0] != RoleOrchestrator {
		t.Errorf("orchestrator delivery = %+v, want no_route", n.Deliveries[1])
	}
}

// TestProjectWorkspaceErrors: an event that is not created/closed, a
// workspace id that fails the pattern, a transition number below 1 and a
// non-empty job id that fails the contract id rule are errors.
func TestProjectWorkspaceErrors(t *testing.T) {
	cases := []struct {
		name string
		in   WorkspaceInput
	}{
		{"event empty", WorkspaceInput{Workspace: "w2", N: 1, JobID: "job-1"}},
		{"event other", WorkspaceInput{Event: "reopened", Workspace: "w2", N: 1, JobID: "job-1"}},
		{"workspace empty", WorkspaceInput{Event: WorkspaceCreated, Workspace: "", N: 1, JobID: "job-1"}},
		{"workspace with space", WorkspaceInput{Event: WorkspaceCreated, Workspace: "bad ws", N: 1, JobID: "job-1"}},
		{"workspace with colon", WorkspaceInput{Event: WorkspaceCreated, Workspace: "w1:p2", N: 1, JobID: "job-1"}},
		{"workspace too long", WorkspaceInput{Event: WorkspaceCreated, Workspace: strings.Repeat("w", 65), N: 1, JobID: "job-1"}},
		{"n zero", WorkspaceInput{Event: WorkspaceCreated, Workspace: "w2", N: 0, JobID: "job-1"}},
		{"n negative", WorkspaceInput{Event: WorkspaceCreated, Workspace: "w2", N: -1, JobID: "job-1"}},
		{"job id with space", WorkspaceInput{Event: WorkspaceCreated, Workspace: "w2", N: 1, JobID: "bad job!"}},
		{"job id with slash", WorkspaceInput{Event: WorkspaceCreated, Workspace: "w2", N: 1, JobID: "a/b"}},
		{"job id too long", WorkspaceInput{Event: WorkspaceCreated, Workspace: "w2", N: 1, JobID: strings.Repeat("j", 65)}},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			n, err := ProjectWorkspace(c.in, fullRegistry(), fixedNow)
			if err == nil {
				t.Fatalf("err = nil, want error (n = %+v)", n)
			}
			if n != nil {
				t.Fatalf("n = %+v, want nil", n)
			}
		})
	}
}

// TestProjectWorkspaceRouting: missing owner adds missing_owner and the
// coordinator; an invalid projeto is dropped and routed as missing; an
// invalid workspace is dropped; there are no class escalations and no
// self guard.
func TestProjectWorkspaceRouting(t *testing.T) {
	in := func() WorkspaceInput {
		return WorkspaceInput{Event: WorkspaceCreated, Workspace: "w2", N: 1, JobID: "job-1", Projeto: "example-org/example-repo"}
	}
	t.Run("invalid projeto dropped and routed as missing", func(t *testing.T) {
		c := in()
		c.Projeto = "not-a-repo"
		n, err := ProjectWorkspace(c, fullRegistry(), fixedNow)
		if err != nil || n == nil {
			t.Fatalf("err = %v, n = %v; want projected", err, n)
		}
		if n.Projeto != "" {
			t.Errorf("projeto = %q, want empty (invalid projeto dropped)", n.Projeto)
		}
		wantEscs(t, n.Escalations, EscMissingOwner)
		if len(n.Deliveries) != 3 {
			t.Fatalf("deliveries = %d, want 3", len(n.Deliveries))
		}
		if n.Deliveries[0].State != StateNoRoute || n.Deliveries[0].Roles[0] != RoleOwner {
			t.Errorf("owner delivery = %+v, want no_route [owner]", n.Deliveries[0])
		}
		if n.Deliveries[1].State != StatePending || n.Deliveries[1].Ref != "owner-agent" {
			t.Errorf("orchestrator delivery = %+v, want pending owner-agent", n.Deliveries[1])
		}
		if n.Deliveries[2].State != StatePending || n.Deliveries[2].Ref != "coord-agent" {
			t.Errorf("coordinator delivery = %+v, want pending coord-agent", n.Deliveries[2])
		}
	})
	t.Run("orchestrator missing is no_route without escalation", func(t *testing.T) {
		c := in()
		c.JobID = "job-2"
		n, err := ProjectWorkspace(c, fullRegistry(), fixedNow)
		if err != nil || n == nil {
			t.Fatalf("err = %v, n = %v; want projected", err, n)
		}
		wantEscs(t, n.Escalations)
		if len(n.Deliveries) != 2 {
			t.Fatalf("deliveries = %d, want 2 (no coordinator)", len(n.Deliveries))
		}
		if n.Deliveries[1].State != StateNoRoute || n.Deliveries[1].Roles[0] != RoleOrchestrator {
			t.Errorf("orchestrator delivery = %+v, want no_route", n.Deliveries[1])
		}
	})
	t.Run("no self guard: a pane-shaped recipient is pending", func(t *testing.T) {
		reg := fullRegistry()
		reg.Owners["example-org/example-repo"] = "w1:p2"
		c := in()
		c.Workspace = "w1"
		n, err := ProjectWorkspace(c, reg, fixedNow)
		if err != nil || n == nil {
			t.Fatalf("err = %v, n = %v; want projected", err, n)
		}
		if n.Deliveries[0].State != StatePending || n.Deliveries[0].Ref != "w1:p2" {
			t.Errorf("owner delivery = %+v, want pending w1:p2 (no self guard)", n.Deliveries[0])
		}
	})
}

// TestProjectWorkspaceCanaryNeverLeaked: a canary projeto is dropped at
// projection and never appears in the notification JSON nor in a rendered
// line; a canary workspace is rejected outright.
func TestProjectWorkspaceCanaryNeverLeaked(t *testing.T) {
	canaries := []string{
		"CANARY-SECRET-1",
		"/Users/someone/private/path",
	}
	in := WorkspaceInput{
		Event:     WorkspaceCreated,
		Workspace: "w2",
		N:         1,
		JobID:     "job-1",
		Projeto:   "CANARY-SECRET-1", // invalid repo: dropped
	}
	n, err := ProjectWorkspace(in, fullRegistry(), fixedNow)
	if err != nil || n == nil {
		t.Fatalf("err = %v, n = %v; want projected", err, n)
	}
	if n.Projeto != "" {
		t.Fatalf("dropped identifier kept on the notification: %+v", n)
	}
	marshaled, err := json.Marshal(n)
	if err != nil {
		t.Fatalf("marshal notification: %v", err)
	}
	for _, c := range canaries {
		if strings.Contains(string(marshaled), c) {
			t.Errorf("json.Marshal(n) contains canary %q:\n%s", c, marshaled)
		}
	}
	for i, d := range n.Deliveries {
		line := Render(n, d)
		for _, c := range canaries {
			if strings.Contains(line, c) {
				t.Errorf("rendered delivery %d contains canary %q:\n%s", i, c, line)
			}
		}
		if strings.Contains(line, " project=") {
			t.Errorf("rendered delivery %d keeps a project token:\n%s", i, line)
		}
	}
	// a canary workspace is rejected, never dropped.
	if _, err := ProjectWorkspace(WorkspaceInput{Event: WorkspaceCreated, Workspace: "/Users/someone/private/path", N: 1, JobID: "job-1"}, fullRegistry(), fixedNow); err == nil {
		t.Fatal("canary workspace: err = nil, want error")
	}
}
