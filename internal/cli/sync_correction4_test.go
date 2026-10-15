package cli

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/djalmajr/nau/internal/outbox"
)

// TestSyncBackfilledOldTerminalDoesNotRestartGrace: the outbox accepts
// out-of-order events: wake appends the latest terminal at once and the
// sync backfills the earlier records after the grace of the current
// terminal is already over. The backfilled older terminal must not
// restart the grace, which counts only from terminal_seen_at: the sync at
// +120m only repairs the gap and the syncs at +121, +150 and +180m skip
// the job, so a single backend query happens in total.
func TestSyncBackfilledOldTerminalDoesNotRestartGrace(t *testing.T) {
	exe, fakeDir := installFakeSoho(t,
		capsRule(capsJSON, 0),
		eventsRule("J1", 0, terminalExitLine(1, "7")+"\n"+eventLine(2, "unblocked", "")+"\n"+terminalExitLine(3, "0"), trailer(3, "done"), 0),
		eventsRule("J1", 3, "", trailer(3, "done"), 0),
	)
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, exe, "machine-a")
	start := postTerminalTestNow()
	// The job was already terminal: its anchor was written when the
	// current cycle entered the terminal state, at the reference instant.
	seedJob(t, cfgDir, "J1", "org/repo", 0, "done")
	s, err := outbox.Open(outbox.StateDir(cfgDir), outbox.Options{Now: func() time.Time { return start }})
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	err = s.UpdateJobs(func(js *outbox.Jobs) error {
		js.Jobs["J1"].TerminalSeenAt = start.Format(outbox.TSLayout)
		return nil
	})
	if err != nil {
		t.Fatalf("seed anchor: %v", err)
	}
	// The same durable append wake uses for the latest terminal; the
	// earlier events were missed and only the sync stores them.
	if _, _, err := s.AppendJobEvent("machine-a", "org/repo", "J1", json.RawMessage(terminalExitLine(3, "0"))); err != nil {
		t.Fatalf("append the latest terminal: %v", err)
	}
	runs := []struct {
		minutes int
		want    string
	}{
		{120, `{"jobs":1,"novos":2,"enviados":0,"pendentes":3}`},
		{121, `{"jobs":0,"novos":0,"enviados":0,"pendentes":3}`},
		{150, `{"jobs":0,"novos":0,"enviados":0,"pendentes":3}`},
		{180, `{"jobs":0,"novos":0,"enviados":0,"pendentes":3}`},
	}
	for _, r := range runs {
		stdout := syncAt(t, cfgDir, start.Add(time.Duration(r.minutes)*time.Minute))
		if stdout != r.want+"\n" {
			t.Fatalf("sync at +%dm stdout = %q, want %q", r.minutes, stdout, r.want)
		}
	}
	// Only the gap repair queried the backend: the backfilled older
	// terminal did not restart the already expired grace.
	if got := eventsSinceArgs(t, fakeDir); len(got) != 1 || got[0] != "0" {
		t.Fatalf("events --since args = %v, want [0] (only the gap repair; the expired grace of the current terminal was not restarted)", got)
	}
	// The valid anchor of the current cycle is preserved by the backfill.
	if j := loadJob(t, cfgDir, "J1"); j.TerminalSeenAt != start.Format(outbox.TSLayout) {
		t.Fatalf("terminal_seen_at = %q, want %q (the backfill must not rewrite a valid anchor)", j.TerminalSeenAt, start.Format(outbox.TSLayout))
	}
}

// TestSyncOutOfOrderTailClosesByEventSeq: the close tail is durable before
// the terminal record that has the lower event seq (wake appended the
// workspace_closed at once; the sync stores the terminal while backfilling
// the gap). The tail's event seq is above the terminal's, so the tail
// closes the job even though it is physically stored before the terminal
// record: the gap repair is the only backend query.
func TestSyncOutOfOrderTailClosesByEventSeq(t *testing.T) {
	exe, fakeDir := installFakeSoho(t,
		capsRule(capsJSON, 0),
		eventsRule("J1", 0, terminalExitLine(1, "0"), trailer(2, "done"), 0),
		eventsRule("J1", 2, "", trailer(2, "done"), 0),
	)
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, exe, "machine-a")
	start := postTerminalTestNow()
	seedJob(t, cfgDir, "J1", "org/repo", 0, "done")
	s, err := outbox.Open(outbox.StateDir(cfgDir), outbox.Options{Now: func() time.Time { return start }})
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	// The workspace_closed of the same cycle, appended by wake before the
	// terminal record is backfilled: it is stored first and its event seq
	// is above the terminal's.
	if _, _, err := s.AppendJobEvent("machine-a", "org/repo", "J1", json.RawMessage(eventLine(2, "workspace_closed", ""))); err != nil {
		t.Fatalf("append the close tail: %v", err)
	}
	stdout := syncAt(t, cfgDir, start)
	if stdout != `{"jobs":1,"novos":1,"enviados":0,"pendentes":2}`+"\n" {
		t.Fatalf("gap repair stdout = %q", stdout)
	}
	stdout = syncAt(t, cfgDir, start.Add(time.Minute))
	if stdout != `{"jobs":0,"novos":0,"enviados":0,"pendentes":2}`+"\n" {
		t.Fatalf("second sync stdout = %q, want the job skipped (the durable tail closes it)", stdout)
	}
	if got := eventsSinceArgs(t, fakeDir); len(got) != 1 || got[0] != "0" {
		t.Fatalf("events --since args = %v, want [0] (the tail stored before the terminal still closes the job by its event seq)", got)
	}
}

// TestSyncTrailerOnlyTerminalAfterStoredTerminal: the job terminates
// (terminal record and blocked trailer), comes back to running, and its
// new terminal state appears only in the trailer, after an older terminal
// record is already stored. The new cycle rewrites terminal_seen_at at its
// own first observation and gets a fresh grace: the sync one minute after
// the new terminal still queries the backend, so a workspace_closed of the
// new cycle arriving inside that grace is queried and stored; four backend
// queries in total.
func TestSyncTrailerOnlyTerminalAfterStoredTerminal(t *testing.T) {
	exe, fakeDir := installFakeSoho(t,
		capsRule(capsJSON, 0),
		eventsRule("J1", 0, terminalExitLine(1, "7"), trailer(1, "blocked"), 0),
		eventsRule("J1", 1, eventLine(2, "unblocked", ""), trailer(2, "running"), 0),
		eventsRule("J1", 2, "", trailer(2, "done"), 0),
	)
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, exe, "machine-a")
	seedJob(t, cfgDir, "J1", "org/repo", 0, "running")
	start := postTerminalTestNow()
	runs := []struct {
		minutes int
		want    string
	}{
		{0, `{"jobs":1,"novos":1,"enviados":0,"pendentes":1}`},
		{5, `{"jobs":1,"novos":1,"enviados":0,"pendentes":2}`},
		{120, `{"jobs":1,"novos":0,"enviados":0,"pendentes":2}`},
		{121, `{"jobs":1,"novos":0,"enviados":0,"pendentes":2}`},
	}
	for _, r := range runs {
		stdout := syncAt(t, cfgDir, start.Add(time.Duration(r.minutes)*time.Minute))
		if stdout != r.want+"\n" {
			t.Fatalf("sync at +%dm stdout = %q, want %q", r.minutes, stdout, r.want)
		}
	}
	// The +121m sync queried the backend inside the new cycle's grace,
	// counted from the rewritten terminal_seen_at.
	if got := eventsSinceArgs(t, fakeDir); len(got) != 4 || got[0] != "0" || got[1] != "1" || got[2] != "2" || got[3] != "2" {
		t.Fatalf("events --since args = %v, want [0 1 2 2] (the +121m sync queried inside the new grace)", got)
	}
	// The anchor is the new cycle's first observation and did not move
	// while the job stayed terminal.
	wantAnchor := start.Add(120 * time.Minute).Format(outbox.TSLayout)
	if j := loadJob(t, cfgDir, "J1"); j.Estado != "done" || j.TerminalSeenAt != wantAnchor {
		t.Fatalf("estado = %q, terminal_seen_at = %q, want done and %q (the new cycle's anchor)", j.Estado, j.TerminalSeenAt, wantAnchor)
	}
}
