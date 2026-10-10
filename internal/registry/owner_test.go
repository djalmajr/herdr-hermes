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
