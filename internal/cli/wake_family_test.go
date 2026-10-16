package cli

import (
	"strings"
	"testing"

	"github.com/djalmajr/nau/internal/jobapi"
)

// TestWakeNewFamily: the renamed backend (soho) sets the SOHO_JOB_*
// variables; wake accepts the current family and records the event
// exactly like the legacy family.
func TestWakeNewFamily(t *testing.T) {
	dir := t.TempDir()
	setConfig(t, dir, map[string]string{"machine_label": "m1"})
	vars := map[string]string{
		jobapi.EnvJobIDCurrent:             "job-31",
		jobapi.EnvJobSeqCurrent:            "1",
		jobapi.EnvJobEventCurrent:          `{"seq":1}`,
		jobapi.EnvJobIdempotencyKeyCurrent: "job-31:1",
	}
	stdout, stderr, exit := runCLIStdin(t, dir, wakeEvent(1), vars, "wake")
	if exit != 0 {
		t.Fatalf("wake (current family) exit = %d, stderr %q", exit, stderr)
	}
	if stdout != `{"seq":1,"duplicado":false,"enviados":0,"pendentes":1}
` {
		t.Fatalf("wake (current family) stdout = %q", stdout)
	}
	lines := outboxLines(t, dir)
	if len(lines) != 1 || !strings.Contains(lines[0], `"idempotency_key":"job-31:1"`) || !strings.Contains(lines[0], `"job_id":"job-31"`) {
		t.Fatalf("outbox lines = %v, want the job-31 event recorded", lines)
	}
	// A repeat of the same event deduplicates, as with the legacy family.
	stdout, stderr, exit = runCLIStdin(t, dir, wakeEvent(1), vars, "wake")
	if exit != 0 || !strings.Contains(stdout, `"duplicado":true`) {
		t.Fatalf("wake (current family) retry: exit %d stdout %q", exit, stdout)
	}
}

// TestWakeNewFamilyErrorsNameTheNewVariables: a malformed event under
// the current family reports the SOHO_JOB_* variable that is missing or
// inconsistent, never the legacy one.
func TestWakeNewFamilyErrorsNameTheNewVariables(t *testing.T) {
	cases := []struct {
		name string
		vars map[string]string
		want string
	}{
		{"missing id", map[string]string{jobapi.EnvJobSeqCurrent: "1"}, "SOHO_JOB_ID"},
		{"seq not a number", map[string]string{jobapi.EnvJobIDCurrent: "job-1", jobapi.EnvJobSeqCurrent: "x"}, "SOHO_JOB_SEQ"},
		{"seq mismatch", map[string]string{jobapi.EnvJobIDCurrent: "job-1", jobapi.EnvJobSeqCurrent: "2"}, "SOHO_JOB_SEQ"},
		{"idempotency mismatch", map[string]string{jobapi.EnvJobIDCurrent: "job-1", jobapi.EnvJobIdempotencyKeyCurrent: "job-1:9"}, "SOHO_JOB_IDEMPOTENCY_KEY"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			setConfig(t, dir, map[string]string{"machine_label": "m1"})
			stdout, stderr, exit := runCLIStdin(t, dir, wakeEvent(1), tc.vars, "wake")
			if exit != 2 {
				t.Fatalf("exit = %d, want 2 (stdout %q)", exit, stdout)
			}
			if !strings.Contains(stderr, tc.want) {
				t.Errorf("stderr %q must name %s", stderr, tc.want)
			}
			if len(outboxLines(t, dir)) != 0 {
				t.Errorf("a refused event must write no record")
			}
		})
	}
}

// TestWakeMixedFamiliesOnlyNewWins: an event whose environment carries
// both the current and the legacy job variables is read from the
// current family only — the legacy variables are ignored, whatever they
// say, and the event is recorded with the current family's identity.
func TestWakeMixedFamiliesOnlyNewWins(t *testing.T) {
	dir := t.TempDir()
	setConfig(t, dir, map[string]string{"machine_label": "m1"})
	vars := map[string]string{
		// The current family carries the true identity...
		jobapi.EnvJobIDCurrent:             "job-31",
		jobapi.EnvJobSeqCurrent:            "1",
		jobapi.EnvJobEventCurrent:          `{"seq":1}`,
		jobapi.EnvJobIdempotencyKeyCurrent: "job-31:1",
		// ...and the legacy family carries a conflicting one that must
		// be ignored.
		jobapi.EnvJobID:             "job-other",
		jobapi.EnvJobSeq:            "9",
		jobapi.EnvJobEvent:          `{"seq":9}`,
		jobapi.EnvJobIdempotencyKey: "job-other:9",
	}
	stdout, stderr, exit := runCLIStdin(t, dir, wakeEvent(1), vars, "wake")
	if exit != 0 {
		t.Fatalf("wake (mixed families) exit = %d, stderr %q", exit, stderr)
	}
	if stdout != `{"seq":1,"duplicado":false,"enviados":0,"pendentes":1}
` {
		t.Fatalf("wake (mixed families) stdout = %q, want the current family's event recorded", stdout)
	}
	lines := outboxLines(t, dir)
	if len(lines) != 1 || !strings.Contains(lines[0], `"idempotency_key":"job-31:1"`) || !strings.Contains(lines[0], `"job_id":"job-31"`) {
		t.Fatalf("outbox lines = %v, want the current family's job-31 event, not the legacy job-other", lines)
	}
}
