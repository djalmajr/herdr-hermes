package cli

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/djalmajr/nau/internal/outbox"
)

// terminalExitLine is a terminal event line carrying the exit ref.
func terminalExitLine(seq int, exit string) string {
	return strings.TrimSuffix(eventLine(seq, "terminal", ""), "}") + `,"refs":{"exit":"` + exit + `"}}`
}

// TestSyncSecondTerminalGetsNewGrace: a job that terminated with a
// blocking exit, came back with unblocked/running and terminated again
// gets a fresh post-terminal grace: the sync one minute after the second
// terminal still queries the backend and stores the workspace_closed of
// the new cycle (the terminal of the earlier cycle does not expire the
// new grace), and once the workspace_closed is durable the next sync
// stops. Four records and four queries in total.
func TestSyncSecondTerminalGetsNewGrace(t *testing.T) {
	exe, fakeDir := installFakeSoho(t,
		capsRule(capsJSON, 0),
		eventsRule("J1", 0, terminalExitLine(1, "7"), trailer(0, "blocked"), 0),
		eventsRule("J1", 1, eventLine(2, "unblocked", ""), trailer(2, "running"), 0),
		eventsRule("J1", 2, terminalExitLine(3, "0"), trailer(3, "done"), 0),
		eventsRule("J1", 3, eventLine(4, "workspace_closed", ""), trailer(4, "done"), 0),
	)
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, exe, "machine-a")
	seedJob(t, cfgDir, "J1", "org/repo", 0, "running")
	start := postTerminalTestNow()
	runs := []struct {
		minutes int
		want    string
		estado  string
	}{
		{0, `{"jobs":1,"novos":1,"enviados":0,"pendentes":1}`, "blocked"},
		{5, `{"jobs":1,"novos":1,"enviados":0,"pendentes":2}`, "running"},
		{120, `{"jobs":1,"novos":1,"enviados":0,"pendentes":3}`, "done"},
		{121, `{"jobs":1,"novos":1,"enviados":0,"pendentes":4}`, "done"},
		{122, `{"jobs":0,"novos":0,"enviados":0,"pendentes":4}`, "done"},
	}
	for _, r := range runs {
		stdout := syncAt(t, cfgDir, start.Add(time.Duration(r.minutes)*time.Minute))
		if stdout != r.want+"\n" {
			t.Fatalf("sync at +%dm stdout = %q, want %q", r.minutes, stdout, r.want)
		}
		if j := loadJob(t, cfgDir, "J1"); j.Estado != r.estado {
			t.Fatalf("sync at +%dm: estado = %q, want %q", r.minutes, j.Estado, r.estado)
		}
	}
	recs := readOutboxRecords(t, cfgDir)
	if len(recs) != 4 {
		t.Fatalf("stored records = %d, want 4 (terminal, unblocked, terminal, workspace_closed)", len(recs))
	}
	var tipos []string
	for _, r := range recs {
		var ev struct {
			Tipo string `json:"tipo"`
		}
		if err := json.Unmarshal(r.Dados, &ev); err != nil {
			t.Fatalf("record dados: %v", err)
		}
		tipos = append(tipos, ev.Tipo)
	}
	if strings.Join(tipos, " ") != "terminal unblocked terminal workspace_closed" {
		t.Fatalf("stored tipos = %v, want [terminal unblocked terminal workspace_closed]", tipos)
	}
	if got := eventsSinceArgs(t, fakeDir); len(got) != 4 || got[0] != "0" || got[1] != "1" || got[2] != "2" || got[3] != "3" {
		t.Fatalf("events --since args = %v, want [0 1 2 3] (the +121m sync queried the backend inside the new grace and the +122m one stopped)", got)
	}
	wantAnchor := start.Add(120 * time.Minute).Format(outbox.TSLayout)
	if j := loadJob(t, cfgDir, "J1"); j.TerminalSeenAt != wantAnchor {
		t.Fatalf("terminal_seen_at = %q, want %q (the new cycle's anchor)", j.TerminalSeenAt, wantAnchor)
	}
}

// TestSyncInvalidAnchorRewrittenAndStops: a stored terminal_seen_at that
// does not parse is absent: the invalid anchor never expires the grace, so
// the job keeps being queried, and the first update rewrites the anchor
// with the clock of that update; one hour after the rewrite the job stops
// being queried, so a bad format can never loop the sync forever.
func TestSyncInvalidAnchorRewrittenAndStops(t *testing.T) {
	exe, fakeDir := installFakeSoho(t,
		capsRule(capsJSON, 0),
		eventsRule("J1", 0, "", trailer(0, "done"), 0),
	)
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, exe, "machine-a")
	start := postTerminalTestNow()
	s, err := outbox.Open(outbox.StateDir(cfgDir), outbox.Options{Now: func() time.Time { return start }})
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	err = s.UpdateJobs(func(js *outbox.Jobs) error {
		js.Jobs["J1"] = &outbox.Job{ID: "J1", Projeto: "org/repo", Estado: "done", TerminalSeenAt: "invalid-anchor", UpdatedAt: start.Format(outbox.TSLayout)}
		return nil
	})
	if err != nil {
		t.Fatalf("seed job: %v", err)
	}
	runs := []struct {
		minutes int
		want    string
	}{
		{0, `{"jobs":1,"novos":0,"enviados":0,"pendentes":0}`},
		{30, `{"jobs":1,"novos":0,"enviados":0,"pendentes":0}`},
		{60, `{"jobs":0,"novos":0,"enviados":0,"pendentes":0}`},
		{90, `{"jobs":0,"novos":0,"enviados":0,"pendentes":0}`},
		{120, `{"jobs":0,"novos":0,"enviados":0,"pendentes":0}`},
	}
	for _, r := range runs {
		stdout := syncAt(t, cfgDir, start.Add(time.Duration(r.minutes)*time.Minute))
		if stdout != r.want+"\n" {
			t.Fatalf("sync at +%dm stdout = %q, want %q", r.minutes, stdout, r.want)
		}
	}
	if got := eventsSinceArgs(t, fakeDir); len(got) != 2 || got[0] != "0" || got[1] != "0" {
		t.Fatalf("events --since args = %v, want [0 0] (only the t=0 and +30m syncs queried the backend; the grace ended at +60m)", got)
	}
	wantAnchor := start.Format(outbox.TSLayout)
	if j := loadJob(t, cfgDir, "J1"); j.TerminalSeenAt != wantAnchor {
		t.Fatalf("terminal_seen_at = %q, want %q (the invalid anchor was rewritten on the first query)", j.TerminalSeenAt, wantAnchor)
	}
}

// TestSyncLeakedAnchorOnRunningJob: a stale terminal_seen_at left on a
// running job is not inherited when the job goes terminal: the
// non-terminal -> terminal update rewrites the anchor with the clock of
// that update, so the grace of the new cycle has not started and the sync
// one minute later still queries the backend.
func TestSyncLeakedAnchorOnRunningJob(t *testing.T) {
	exe, fakeDir := installFakeSoho(t,
		capsRule(capsJSON, 0),
		eventsRule("J1", 0, "", trailer(0, "done"), 0),
	)
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, exe, "machine-a")
	start := postTerminalTestNow()
	s, err := outbox.Open(outbox.StateDir(cfgDir), outbox.Options{Now: func() time.Time { return start }})
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	stale := start.Add(-2 * time.Hour).Format(outbox.TSLayout)
	err = s.UpdateJobs(func(js *outbox.Jobs) error {
		js.Jobs["J1"] = &outbox.Job{ID: "J1", Projeto: "org/repo", Estado: "running", TerminalSeenAt: stale, UpdatedAt: start.Format(outbox.TSLayout)}
		return nil
	})
	if err != nil {
		t.Fatalf("seed job: %v", err)
	}
	want := `{"jobs":1,"novos":0,"enviados":0,"pendentes":0}`
	stdout := syncAt(t, cfgDir, start)
	if stdout != want+"\n" {
		t.Fatalf("sync at t=0 stdout = %q, want %q", stdout, want)
	}
	// The transition rewrites the anchor even though one was stored.
	if j := loadJob(t, cfgDir, "J1"); j.Estado != "done" || j.TerminalSeenAt != start.Format(outbox.TSLayout) {
		t.Fatalf("after the terminal trailer: estado = %q, terminal_seen_at = %q, want done and the clock of the update", j.Estado, j.TerminalSeenAt)
	}
	stdout = syncAt(t, cfgDir, start.Add(time.Minute))
	if stdout != want+"\n" {
		t.Fatalf("sync at +1m stdout = %q, want %q (the new cycle's grace had not expired)", stdout, want)
	}
	if got := eventsSinceArgs(t, fakeDir); len(got) != 2 || got[0] != "0" || got[1] != "0" {
		t.Fatalf("events --since args = %v, want [0 0] (the stale anchor did not expire the new cycle)", got)
	}
}
