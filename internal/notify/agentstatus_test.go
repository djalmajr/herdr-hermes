package notify

import (
	"errors"
	"testing"
)

// TestParseAgentStatusEventShapes: the three accepted shapes; unknown
// fields (agent kind, title, display name, state labels) are discarded.
func TestParseAgentStatusEventShapes(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want AgentStatusEvent
	}{
		{
			name: "socket subscription line",
			raw:  `{"event":"pane.agent_status_changed","data":{"pane_id":"w1:p2","workspace_id":"w1","agent_status":"working","agent":"agent-a","title":"t","display_agent":"agent-a","state_labels":{"l":"x"}}}`,
			want: AgentStatusEvent{Pane: "w1:p2", Workspace: "w1", Status: "working"},
		},
		{
			name: "event stream schema form",
			raw:  `{"event":"pane_agent_status_changed","data":{"type":"pane_agent_status_changed","pane_id":"w1:p2","workspace_id":"w1","agent_status":"blocked","agent":"agent-a"}}`,
			want: AgentStatusEvent{Pane: "w1:p2", Workspace: "w1", Status: "blocked"},
		},
		{
			name: "flat payload without type",
			raw:  `{"pane_id":"w1:p3","workspace_id":"w1","agent_status":"done"}`,
			want: AgentStatusEvent{Pane: "w1:p3", Workspace: "w1", Status: "done"},
		},
		{
			name: "flat payload with type",
			raw:  `{"type":"pane_agent_status_changed","pane_id":"w1:p3","workspace_id":"w1","agent_status":"idle"}`,
			want: AgentStatusEvent{Pane: "w1:p3", Workspace: "w1", Status: "idle"},
		},
		{
			name: "flat payload extra unknown fields",
			raw:  `{"type":"pane_agent_status_changed","pane_id":"w1:p3","workspace_id":"w1","agent_status":"unknown","zzz_extra":1}`,
			want: AgentStatusEvent{Pane: "w1:p3", Workspace: "w1", Status: "unknown"},
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := ParseAgentStatusEvent([]byte(c.raw))
			if err != nil {
				t.Fatalf("err = %v, want nil", err)
			}
			if got != c.want {
				t.Fatalf("got = %+v, want %+v", got, c.want)
			}
		})
	}
}

// TestParseAgentStatusEventStatuses: the five status names are accepted.
func TestParseAgentStatusEventStatuses(t *testing.T) {
	for _, status := range []string{"idle", "working", "blocked", "done", "unknown"} {
		status := status
		t.Run(status, func(t *testing.T) {
			raw := `{"pane_id":"w1:p2","workspace_id":"w1","agent_status":"` + status + `"}`
			got, err := ParseAgentStatusEvent([]byte(raw))
			if err != nil {
				t.Fatalf("err = %v, want nil", err)
			}
			if got.Status != status {
				t.Fatalf("status = %q, want %q", got.Status, status)
			}
		})
	}
}

// TestParseAgentStatusEventOtherEvent: an envelope (or flat type) that
// names another event is ErrNotAgentStatus, not a generic error.
func TestParseAgentStatusEventOtherEvent(t *testing.T) {
	cases := []struct {
		name string
		raw  string
	}{
		{"dotted other event", `{"event":"pane.exited","data":{"pane_id":"w1:p2","workspace_id":"w1","agent_status":"working"}}`},
		{"underscored other event", `{"event":"workspace_created","data":{"type":"workspace_created","pane_id":"w1:p2","workspace_id":"w1","agent_status":"working"}}`},
		{"data type mismatch", `{"event":"pane.agent_status_changed","data":{"type":"pane_agent_terminated","pane_id":"w1:p2","workspace_id":"w1","agent_status":"working"}}`},
		{"flat type mismatch", `{"type":"pane_agent_terminated","pane_id":"w1:p2","workspace_id":"w1","agent_status":"working"}`},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			_, err := ParseAgentStatusEvent([]byte(c.raw))
			if err == nil {
				t.Fatal("err = nil, want ErrNotAgentStatus")
			}
			if !errors.Is(err, ErrNotAgentStatus) {
				t.Fatalf("err = %v, want ErrNotAgentStatus", err)
			}
		})
	}
}

// TestParseAgentStatusEventInvalid: invalid fields are plain errors, not
// ErrNotAgentStatus.
func TestParseAgentStatusEventInvalid(t *testing.T) {
	cases := []struct {
		name string
		raw  string
	}{
		{"invalid pane id", `{"pane_id":"no-colon","workspace_id":"w1","agent_status":"working"}`},
		{"pane id with space", `{"pane_id":"w1: p2","workspace_id":"w1","agent_status":"working"}`},
		{"invalid workspace id", `{"pane_id":"w1:p2","workspace_id":"bad ws","agent_status":"working"}`},
		{"invalid status", `{"pane_id":"w1:p2","workspace_id":"w1","agent_status":"crashed"}`},
		{"empty status", `{"pane_id":"w1:p2","workspace_id":"w1","agent_status":""}`},
		{"missing status", `{"pane_id":"w1:p2","workspace_id":"w1"}`},
		{"missing pane", `{"workspace_id":"w1","agent_status":"working"}`},
		{"missing workspace", `{"pane_id":"w1:p2","agent_status":"working"}`},
		{"pane not a string", `{"pane_id":7,"workspace_id":"w1","agent_status":"working"}`},
		{"event with no data", `{"event":"pane.agent_status_changed","data":null}`},
		{"event with string data", `{"event":"pane.agent_status_changed","data":"w1:p2"}`},
		{"event name not a string", `{"event":42,"data":{"pane_id":"w1:p2","workspace_id":"w1","agent_status":"working"}}`},
		{"type not a string", `{"type":42,"pane_id":"w1:p2","workspace_id":"w1","agent_status":"working"}`},
		{"not json", `not json at all`},
		{"json array", `[1,2,3]`},
		{"json string", `"w1:p2"`},
		{"json number", `7`},
		{"json null", `null`},
		{"empty", ``},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			_, err := ParseAgentStatusEvent([]byte(c.raw))
			if err == nil {
				t.Fatal("err = nil, want error")
			}
			if errors.Is(err, ErrNotAgentStatus) {
				t.Fatalf("err = %v, must not be ErrNotAgentStatus", err)
			}
		})
	}
}

// TestParseAgentStatusEventIsolatedFields: a valid envelope keeps only the
// allowlisted fields; the discarded fields must not influence the result.
func TestParseAgentStatusEventIsolatedFields(t *testing.T) {
	raw1 := `{"event":"pane.agent_status_changed","data":{"pane_id":"w1:p2","workspace_id":"w1","agent_status":"blocked"}}`
	raw2 := `{"event":"pane.agent_status_changed","data":{"pane_id":"w1:p2","workspace_id":"w1","agent_status":"blocked","agent":"agent-a","title":"some title","display_agent":"agent-a","state_labels":{"a":"b"}}}`
	g1, err1 := ParseAgentStatusEvent([]byte(raw1))
	g2, err2 := ParseAgentStatusEvent([]byte(raw2))
	if err1 != nil || err2 != nil {
		t.Fatalf("err = %v / %v, want nil", err1, err2)
	}
	if g1 != g2 {
		t.Fatalf("discarded fields changed the result: %+v vs %+v", g1, g2)
	}
}
