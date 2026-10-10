package registry

import (
	"strings"
	"testing"
)

// TestParseWorkspaceName: the design's examples, plus the boundary cases,
// table-driven.
// Mutation captured: accepting a zero or a second type index (or dropping
// the [a-z0-9] token rule) breaks the unresolved rows.
func TestParseWorkspaceName(t *testing.T) {
	cases := []struct {
		label  string
		want   WorkspaceName
		wantOK bool
	}{
		{label: "hm-herdr-hermes-impl-dja229", want: WorkspaceName{Agente: "herdr-hermes", Trabalho: "impl", Ref: "dja229"}, wantOK: true},
		{label: "hm-herdr-soho-fix-pr64-2", want: WorkspaceName{Agente: "herdr-soho", Trabalho: "fix", Ref: "pr64-2"}, wantOK: true},
		{label: "hm-herdr-hermes-impl-dja238-241", want: WorkspaceName{Agente: "herdr-hermes", Trabalho: "impl", Ref: "dja238-241"}, wantOK: true},
		{label: "hm-a-review-impl-x"},
		{label: "hm-pinar"},
		{label: "hm-Pinar-impl-x"},
		{label: "hm-a-plan-b", want: WorkspaceName{Agente: "a", Trabalho: "plan", Ref: "b"}, wantOK: true},
		{label: "hm-a-impl-x-impl-y"},
		{label: "hm-impl-x"},
		{label: "hm-a-impl"},
		{label: "hm-"},
		{label: "hm-a--impl-x"},
		{label: "hm-a.review-impl-x"},
		{label: "job-123"},
		{label: "personal-workspace"},
	}
	for _, tc := range cases {
		got, ok := ParseWorkspaceName(tc.label)
		if ok != tc.wantOK {
			t.Errorf("ParseWorkspaceName(%q).ok = %v, want %v", tc.label, ok, tc.wantOK)
			continue
		}
		if ok && got != tc.want {
			t.Errorf("ParseWorkspaceName(%q) = %+v, want %+v", tc.label, got, tc.want)
		}
	}
}

// TestJobIDFromWorkspace: only a job-<id> label with a contract id
// resolves.
// Mutation captured: a missing contract id pattern check fails the long
// and bad-character rows.
func TestJobIDFromWorkspace(t *testing.T) {
	cases := []struct {
		label  string
		want   string
		wantOK bool
	}{
		{label: "job-abc-123", want: "abc-123", wantOK: true},
		{label: "job-ABC.123", want: "ABC.123", wantOK: true},
		{label: "job-" + strings.Repeat("a", 64), want: strings.Repeat("a", 64), wantOK: true},
		{label: "job-"},
		{label: "job-" + strings.Repeat("a", 65)},
		{label: "job-abc#" + strings.Repeat("a", 60)},
		{label: "job-abc-123-", want: "abc-123-", wantOK: true},
		{label: "job-abc!"},
		{label: "hm-job-123"},
		{label: "work-x"},
	}
	for _, tc := range cases {
		got, ok := JobIDFromWorkspace(tc.label)
		if ok != tc.wantOK || (ok && got != tc.want) {
			t.Errorf("JobIDFromWorkspace(%q) = (%q, %v), want (%q, %v)", tc.label, got, ok, tc.want, tc.wantOK)
		}
	}
}

// TestRefBase: only a trailing one-digit collision suffix from 2 to 9 is
// dropped.
// Mutation captured: a suffix pattern of -[0-9]+ turns the range
// "x238-241" into "x238".
func TestRefBase(t *testing.T) {
	for ref, want := range map[string]string{"pr64-2": "pr64", "dja245-9": "dja245", "dja238-241": "dja238-241", "dja194-p2": "dja194-p2", "pr4-win": "pr4-win", "x-1": "x-1", "card12": "card12", "-2": "-2"} {
		if got := RefBase(ref); got != want {
			t.Errorf("RefBase(%q) = %q, want %q", ref, got, want)
		}
	}
}
