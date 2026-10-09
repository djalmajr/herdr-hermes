package plugin_test

import (
	"strings"
	"testing"

	"github.com/djalmajr/herdr-hermes/internal/plugin"
)

func TestParseEvent(t *testing.T) {
	cases := []struct {
		name   string
		raw    string
		label  string
		wsID   string
		wantOK bool
	}{
		{"nested shape", `{"workspace":{"label":"job-J1","workspace_id":"ws-1"}}`, "job-J1", "ws-1", true},
		{"flat shape", `{"label":"job-J1","workspace_id":"ws-2"}`, "job-J1", "ws-2", true},
		{"nested wins over flat", `{"label":"flat","workspace_id":"flat-ws","workspace":{"label":"job-N","workspace_id":"ws-n"}}`, "job-N", "ws-n", true},
		{"nested label only", `{"workspace":{"label":"job-J1"}}`, "job-J1", "", true},
		{"flat label only", `{"label":"agent-x"}`, "agent-x", "", true},
		{"nested empty label falls back to flat", `{"label":"job-F","workspace":{"label":""}}`, "job-F", "", true},
		{"nested workspace_id not string", `{"workspace":{"label":"job-J1","workspace_id":7}}`, "job-J1", "", true},
		{"empty", ``, ``, ``, false},
		{"not json", `workspace created`, ``, ``, false},
		{"json array", `["label"]`, ``, ``, false},
		{"json null", `null`, ``, ``, false},
		{"object without label", `{"foo":1}`, ``, ``, false},
		{"nested workspace not object", `{"workspace":"job-J1"}`, ``, ``, false},
		{"label not string", `{"label":3}`, ``, ``, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			label, wsID, ok := plugin.ParseEvent(tc.raw)
			if ok != tc.wantOK || label != tc.label || wsID != tc.wsID {
				t.Fatalf("ParseEvent(%q) = (%q, %q, %v), want (%q, %q, %v)", tc.raw, label, wsID, ok, tc.label, tc.wsID, tc.wantOK)
			}
		})
	}
}

func TestJobIDFromLabel(t *testing.T) {
	cases := []struct {
		label string
		id    string
		ok    bool
	}{
		{"job-J1", "J1", true},
		{"job-a.b_c-9", "a.b_c-9", true},
		{"job-" + strings.Repeat("a", 64), strings.Repeat("a", 64), true},
		{"job-", "", false},
		{"job-" + strings.Repeat("a", 65), "", false},
		{"agent-J1", "", false},
		{"J1", "", false},
		{"", "", false},
		{"job-bad id", "", false},
		{"job-bad/slash", "", false},
	}
	for _, tc := range cases {
		t.Run("label "+tc.label, func(t *testing.T) {
			id, ok := plugin.JobIDFromLabel(tc.label)
			if ok != tc.ok || id != tc.id {
				t.Fatalf("JobIDFromLabel(%q) = (%q, %v), want (%q, %v)", tc.label, id, ok, tc.id, tc.ok)
			}
		})
	}
}
