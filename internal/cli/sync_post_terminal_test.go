package cli

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/djalmajr/nau/internal/outbox"
)

// postTerminalTestNow matches the fixed clock runHermes injects; the
// seeded instants in this file are relative to it.
var postTerminalTestNow = func() time.Time { return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC) }

// TestSyncPostTerminalTailStored: the backend publishes folder_retained
// and workspace_closed after the terminal event; the first sync stores
// the terminal, the second sync still queries the terminal job (the
// post-terminal grace) and stores the close tail: three records in total;
// after the workspace_closed is durable, a third sync does not query the
// backend (the folder_retained alone does not close the tail).
func TestSyncPostTerminalTailStored(t *testing.T) {
	exe, fakeDir := installFakeSoho(t,
		capsRule(capsJSON, 0),
		eventsRule("J1", 0, eventLine(1, "terminal", ""), trailer(1, "done"), 0),
		eventsRule("J1", 1, eventLine(2, "folder_retained", "")+"\n"+eventLine(3, "workspace_closed", ""), trailer(3, "done"), 0),
	)
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, exe, "machine-a")
	seedJob(t, cfgDir, "J1", "org/repo", 0, "running")
	stdout, _, exit := runHermes(t, cfgDir, false, "", "sync", "--job", "J1")
	if exit != 0 {
		t.Fatalf("first sync: exit = %d, stdout %q", exit, stdout)
	}
	if stdout != `{"jobs":1,"novos":1,"enviados":0,"pendentes":1}`+"\n" {
		t.Fatalf("first sync stdout = %q", stdout)
	}
	// The close tail arrives in the next answer: the second sync queries
	// the terminal job and stores both events.
	stdout, _, exit = runHermes(t, cfgDir, false, "", "sync", "--job", "J1")
	if exit != 0 {
		t.Fatalf("second sync: exit = %d, stdout %q", exit, stdout)
	}
	if stdout != `{"jobs":1,"novos":2,"enviados":0,"pendentes":3}`+"\n" {
		t.Fatalf("second sync stdout = %q", stdout)
	}
	if got := eventsSinceArgs(t, fakeDir); len(got) != 2 || got[0] != "0" || got[1] != "1" {
		t.Fatalf("events --since args = %v, want [0 1] (the second sync queried the backend)", got)
	}
	recs := readOutboxRecords(t, cfgDir)
	if len(recs) != 3 {
		t.Fatalf("stored records = %d, want 3 (terminal, folder_retained, workspace_closed)", len(recs))
	}
	for i, r := range recs {
		if r.IdempotencyKey != "J1:"+intTo(i+1) {
			t.Fatalf("record %d key = %q, want J1:%d", i+1, r.IdempotencyKey, i+1)
		}
	}
	var tipos []string
	for i, r := range recs {
		var ev struct {
			Tipo string `json:"tipo"`
		}
		if err := json.Unmarshal(r.Dados, &ev); err != nil {
			t.Fatalf("record %d dados: %v", i+1, err)
		}
		tipos = append(tipos, ev.Tipo)
	}
	if len(tipos) != 3 || tipos[0] != "terminal" || tipos[1] != "folder_retained" || tipos[2] != "workspace_closed" {
		t.Fatalf("stored tipos = %v, want [terminal folder_retained workspace_closed]", tipos)
	}
	// After the workspace_closed is durable, a third sync does not query
	// the backend.
	stdout, _, exit = runHermes(t, cfgDir, false, "", "sync", "--job", "J1")
	if exit != 0 {
		t.Fatalf("third sync: exit = %d, stdout %q", exit, stdout)
	}
	if stdout != `{"jobs":0,"novos":0,"enviados":0,"pendentes":3}`+"\n" {
		t.Fatalf("third sync stdout = %q", stdout)
	}
	if got := eventsSinceArgs(t, fakeDir); len(got) != 2 {
		t.Fatalf("events --since args = %v, want [0 1] (the third sync did not query the backend)", got)
	}
}

// TestSyncPostTerminalTailDurableSkips: a terminal job without a gap whose
// outbox already holds a workspace_closed record is not queried: the
// workspace_closed closes the close tail and comes out once.
func TestSyncPostTerminalTailDurableSkips(t *testing.T) {
	exe, fakeDir := installFakeSoho(t, capsRule(capsJSON, 0))
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, exe, "machine-a")
	s, err := outbox.Open(outbox.StateDir(cfgDir), outbox.Options{Now: postTerminalTestNow})
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	for _, line := range []string{eventLine(1, "terminal", ""), eventLine(2, "workspace_closed", "")} {
		if _, appended, err := s.AppendJobEvent("machine-a", "org/repo", "J1", json.RawMessage(line)); err != nil || !appended {
			t.Fatalf("seed: appended=%v err=%v, want true,nil", appended, err)
		}
	}
	err = s.UpdateJobs(func(js *outbox.Jobs) error {
		js.Jobs["J1"].Estado = "done"
		return nil
	})
	if err != nil {
		t.Fatalf("seed estado: %v", err)
	}
	stdout, _, exit := runHermes(t, cfgDir, false, "", "sync")
	if exit != 0 {
		t.Fatalf("sync: exit = %d, stdout %q", exit, stdout)
	}
	if stdout != `{"jobs":0,"novos":0,"enviados":0,"pendentes":2}`+"\n" {
		t.Fatalf("stdout = %q", stdout)
	}
	if n := len(eventsSinceArgs(t, fakeDir)); n != 0 {
		t.Fatalf("events calls = %d, want 0 (the durable workspace_closed closes the tail)", n)
	}
}

// TestSyncPostTerminalGraceOverSkips: a terminal job without a gap and
// without a close tail is not queried once the post-terminal grace is
// over: the age of the stored terminal record (or, when the state came
// only from the trailer and no terminal record is stored, of the job's
// terminal_seen_at) reaches syncPostTerminalGrace; the job's updated_at
// no longer anchors the grace. The instants come from the injected clock,
// never from the backend.
func TestSyncPostTerminalGraceOverSkips(t *testing.T) {
	// More than syncPostTerminalGrace (1 h) before the test clock.
	old := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	t.Run("terminal record older than the grace", func(t *testing.T) {
		exe, fakeDir := installFakeSoho(t, capsRule(capsJSON, 0))
		cfgDir := t.TempDir()
		setSohoConfig(t, cfgDir, exe, "machine-a")
		s, err := outbox.Open(outbox.StateDir(cfgDir), outbox.Options{Now: func() time.Time { return old }})
		if err != nil {
			t.Fatalf("open state: %v", err)
		}
		if _, appended, err := s.AppendJobEvent("machine-a", "org/repo", "J1", json.RawMessage(eventLine(1, "terminal", ""))); err != nil || !appended {
			t.Fatalf("seed: appended=%v err=%v, want true,nil", appended, err)
		}
		err = s.UpdateJobs(func(js *outbox.Jobs) error {
			js.Jobs["J1"].Estado = "done"
			return nil
		})
		if err != nil {
			t.Fatalf("seed estado: %v", err)
		}
		stdout, _, exit := runHermes(t, cfgDir, false, "", "sync")
		if exit != 0 {
			t.Fatalf("sync: exit = %d, stdout %q", exit, stdout)
		}
		if stdout != `{"jobs":0,"novos":0,"enviados":0,"pendentes":1}`+"\n" {
			t.Fatalf("stdout = %q", stdout)
		}
		if n := len(eventsSinceArgs(t, fakeDir)); n != 0 {
			t.Fatalf("events calls = %d, want 0 (the terminal record is older than the grace)", n)
		}
	})

	t.Run("no terminal record and no anchor: updated_at no longer counts", func(t *testing.T) {
		exe, fakeDir := installFakeSoho(t, capsRule(capsJSON, 0), eventsRule("J1", 0, "", trailer(0, "done"), 0))
		cfgDir := t.TempDir()
		setSohoConfig(t, cfgDir, exe, "machine-a")
		s, err := outbox.Open(outbox.StateDir(cfgDir), outbox.Options{})
		if err != nil {
			t.Fatalf("open state: %v", err)
		}
		err = s.UpdateJobs(func(js *outbox.Jobs) error {
			js.Jobs["J1"] = &outbox.Job{ID: "J1", Projeto: "org/repo", Estado: "done", UpdatedAt: old.Format(outbox.TSLayout)}
			return nil
		})
		if err != nil {
			t.Fatalf("seed job: %v", err)
		}
		stdout, _, exit := runHermes(t, cfgDir, false, "", "sync")
		if exit != 0 {
			t.Fatalf("sync: exit = %d, stdout %q", exit, stdout)
		}
		if stdout != `{"jobs":1,"novos":0,"enviados":0,"pendentes":0}`+"\n" {
			t.Fatalf("stdout = %q", stdout)
		}
		if n := len(eventsSinceArgs(t, fakeDir)); n != 1 {
			t.Fatalf("events calls = %d, want 1 (the updated_at no longer anchors the grace)", n)
		}
		if j := loadJob(t, cfgDir, "J1"); j.TerminalSeenAt != postTerminalTestNow().Format(outbox.TSLayout) {
			t.Fatalf("terminal_seen_at = %q, want %q (written on the first query)", j.TerminalSeenAt, postTerminalTestNow().Format(outbox.TSLayout))
		}
	})
}

// TestSyncPostTerminalClosedSkips (control): a Closed job is skipped by
// the Closed flag alone, even with a fresh terminal record and no close
// tail: no job events call.
func TestSyncPostTerminalClosedSkips(t *testing.T) {
	exe, fakeDir := installFakeSoho(t, capsRule(capsJSON, 0))
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, exe, "machine-a")
	s, err := outbox.Open(outbox.StateDir(cfgDir), outbox.Options{Now: postTerminalTestNow})
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	if _, appended, err := s.AppendJobEvent("machine-a", "org/repo", "J1", json.RawMessage(eventLine(1, "terminal", ""))); err != nil || !appended {
		t.Fatalf("seed: appended=%v err=%v, want true,nil", appended, err)
	}
	err = s.UpdateJobs(func(js *outbox.Jobs) error {
		js.Jobs["J1"].Estado = "closed"
		js.Jobs["J1"].Closed = true
		return nil
	})
	if err != nil {
		t.Fatalf("seed job: %v", err)
	}
	stdout, _, exit := runHermes(t, cfgDir, false, "", "sync")
	if exit != 0 {
		t.Fatalf("sync: exit = %d, stdout %q", exit, stdout)
	}
	if stdout != `{"jobs":0,"novos":0,"enviados":0,"pendentes":1}`+"\n" {
		t.Fatalf("stdout = %q", stdout)
	}
	if n := len(eventsSinceArgs(t, fakeDir)); n != 0 {
		t.Fatalf("events calls = %d, want 0 (the Closed flag alone skips the job)", n)
	}
}

// syncAt runs one `sync --job J1` with the given injected clock and
// returns the stdout line.
func syncAt(t *testing.T, cfgDir string, now time.Time) string {
	t.Helper()
	var stdout, stderr bytes.Buffer
	env := Env{
		Stdin:     strings.NewReader(""),
		Stdout:    &stdout,
		Stderr:    &stderr,
		Getenv:    getenvFor(false),
		Environ:   func() []string { return fakeChildEnv },
		ConfigDir: cfgDir,
		Now:       func() time.Time { return now },
		Sleep:     sleepCtx,
	}
	if code := Run([]string{"sync", "--job", "J1"}, env); code != 0 {
		t.Fatalf("sync: exit = %d, stdout %q stderr %q", code, stdout.String(), stderr.String())
	}
	return stdout.String()
}

// loadJob returns one job from the state dir's jobs.json.
func loadJob(t *testing.T, cfgDir, id string) *outbox.Job {
	t.Helper()
	s, err := outbox.Open(outbox.StateDir(cfgDir), outbox.Options{ReadOnly: true})
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	jobs, err := s.LoadJobs()
	if err != nil {
		t.Fatalf("LoadJobs: %v", err)
	}
	j := jobs.Jobs[id]
	if j == nil {
		t.Fatalf("job %s not tracked", id)
	}
	return j
}

// TestSyncPostTerminalSplitCloseTail: the backend publishes the close tail
// in separate answers, minutes apart (terminal, then folder_retained, then
// workspace_closed); every sync inside the post-terminal grace keeps
// querying the job and stores every event, so the workspace_closed that
// arrives after the folder_retained is never lost; once the
// workspace_closed is durable, a fourth sync does not query the backend.
func TestSyncPostTerminalSplitCloseTail(t *testing.T) {
	exe, fakeDir := installFakeSoho(t,
		capsRule(capsJSON, 0),
		eventsRule("J1", 0, eventLine(1, "terminal", ""), trailer(1, "done"), 0),
		eventsRule("J1", 1, eventLine(2, "folder_retained", ""), trailer(2, "done"), 0),
		eventsRule("J1", 2, eventLine(3, "workspace_closed", ""), trailer(3, "done"), 0),
	)
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, exe, "machine-a")
	seedJob(t, cfgDir, "J1", "org/repo", 0, "running")
	for i, want := range []string{
		`{"jobs":1,"novos":1,"enviados":0,"pendentes":1}`,
		`{"jobs":1,"novos":1,"enviados":0,"pendentes":2}`,
		`{"jobs":1,"novos":1,"enviados":0,"pendentes":3}`,
	} {
		stdout, _, exit := runHermes(t, cfgDir, false, "", "sync", "--job", "J1")
		if exit != 0 {
			t.Fatalf("sync %d: exit = %d, stdout %q", i+1, exit, stdout)
		}
		if stdout != want+"\n" {
			t.Fatalf("sync %d stdout = %q, want %q", i+1, stdout, want)
		}
	}
	// The close tail is split across the answers: all three events are
	// stored and every answer was queried.
	recs := readOutboxRecords(t, cfgDir)
	if len(recs) != 3 {
		t.Fatalf("stored records = %d, want 3 (terminal, folder_retained, workspace_closed)", len(recs))
	}
	var tipos []string
	for i, r := range recs {
		var ev struct {
			Tipo string `json:"tipo"`
		}
		if err := json.Unmarshal(r.Dados, &ev); err != nil {
			t.Fatalf("record %d dados: %v", i+1, err)
		}
		tipos = append(tipos, ev.Tipo)
	}
	if len(tipos) != 3 || tipos[0] != "terminal" || tipos[1] != "folder_retained" || tipos[2] != "workspace_closed" {
		t.Fatalf("stored tipos = %v, want [terminal folder_retained workspace_closed]", tipos)
	}
	if got := eventsSinceArgs(t, fakeDir); len(got) != 3 || got[0] != "0" || got[1] != "1" || got[2] != "2" {
		t.Fatalf("events --since args = %v, want [0 1 2] (every answer was queried)", got)
	}
	// Once the workspace_closed is durable, a fourth sync does not query
	// the backend.
	stdout, _, exit := runHermes(t, cfgDir, false, "", "sync", "--job", "J1")
	if exit != 0 {
		t.Fatalf("fourth sync: exit = %d, stdout %q", exit, stdout)
	}
	if stdout != `{"jobs":0,"novos":0,"enviados":0,"pendentes":3}`+"\n" {
		t.Fatalf("fourth sync stdout = %q", stdout)
	}
	if got := eventsSinceArgs(t, fakeDir); len(got) != 3 {
		t.Fatalf("events --since args = %v, want [0 1 2] (the fourth sync did not query the backend)", got)
	}
}

// TestSyncPostTerminalTrailerOnlyAnchorDoesNotSlide: a job whose terminal
// state comes only from the events trailer (no stored terminal record) is
// anchored at its first terminal observation, terminal_seen_at: syncs at
// +30 and +60 minutes still query the backend; at +90 and +120 minutes,
// one hour after the first observation, they do not; the anchor does not
// move between the queries (the job's updated_at does, but it no longer
// counts).
func TestSyncPostTerminalTrailerOnlyAnchorDoesNotSlide(t *testing.T) {
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
		js.Jobs["J1"] = &outbox.Job{ID: "J1", Projeto: "org/repo", Estado: "done", UpdatedAt: start.Format(outbox.TSLayout)}
		return nil
	})
	if err != nil {
		t.Fatalf("seed job: %v", err)
	}
	for _, minutes := range []int{30, 60, 90, 120} {
		want := `{"jobs":1,"novos":0,"enviados":0,"pendentes":0}`
		if minutes >= 90 {
			want = `{"jobs":0,"novos":0,"enviados":0,"pendentes":0}`
		}
		stdout := syncAt(t, cfgDir, start.Add(time.Duration(minutes)*time.Minute))
		if stdout != want+"\n" {
			t.Fatalf("sync at +%dm stdout = %q, want %q", minutes, stdout, want)
		}
	}
	if got := eventsSinceArgs(t, fakeDir); len(got) != 2 || got[0] != "0" || got[1] != "0" {
		t.Fatalf("events --since args = %v, want [0 0] (only +30 and +60 queried the backend)", got)
	}
	// The anchor was written once, at the first terminal observation, and
	// did not move while the job stayed terminal.
	anchor := start.Add(30 * time.Minute).Format(outbox.TSLayout)
	if j := loadJob(t, cfgDir, "J1"); j.TerminalSeenAt != anchor {
		t.Fatalf("terminal_seen_at = %q, want %q (one hour from the first observation)", j.TerminalSeenAt, anchor)
	}
}

// TestSyncPostTerminalLegacyJobGainsAnchor: a job written before
// terminal_seen_at existed — terminal state, no stored terminal record,
// no anchor — is still queried while terminal, and the first query writes
// the anchor, so the grace counts from the first observation and the job
// is not queried forever.
func TestSyncPostTerminalLegacyJobGainsAnchor(t *testing.T) {
	exe, fakeDir := installFakeSoho(t,
		capsRule(capsJSON, 0),
		eventsRule("J1", 0, "", trailer(0, "done"), 0),
	)
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, exe, "machine-a")
	old := postTerminalTestNow().Add(-24 * time.Hour)
	s, err := outbox.Open(outbox.StateDir(cfgDir), outbox.Options{Now: func() time.Time { return old }})
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	err = s.UpdateJobs(func(js *outbox.Jobs) error {
		js.Jobs["J1"] = &outbox.Job{ID: "J1", Projeto: "org/repo", Estado: "done", UpdatedAt: old.Format(outbox.TSLayout)}
		return nil
	})
	if err != nil {
		t.Fatalf("seed job: %v", err)
	}
	stdout, _, exit := runHermes(t, cfgDir, false, "", "sync")
	if exit != 0 {
		t.Fatalf("sync: exit = %d, stdout %q", exit, stdout)
	}
	if stdout != `{"jobs":1,"novos":0,"enviados":0,"pendentes":0}`+"\n" {
		t.Fatalf("sync stdout = %q", stdout)
	}
	if n := len(eventsSinceArgs(t, fakeDir)); n != 1 {
		t.Fatalf("events calls = %d, want 1 (a job without anchor is queried)", n)
	}
	if j := loadJob(t, cfgDir, "J1"); j.TerminalSeenAt != postTerminalTestNow().Format(outbox.TSLayout) {
		t.Fatalf("terminal_seen_at = %q, want %q (written on the first query)", j.TerminalSeenAt, postTerminalTestNow().Format(outbox.TSLayout))
	}
}

// TestSyncPostTerminalBackToNonTerminalClearsAnchor: a job that was
// terminal and then comes back to a non-terminal estado (an unblocked
// after a terminal with a blocking exit) has its terminal_seen_at
// cleared, so a later terminal observation starts a fresh grace.
func TestSyncPostTerminalBackToNonTerminalClearsAnchor(t *testing.T) {
	exe, fakeDir := installFakeSoho(t,
		capsRule(capsJSON, 0),
		eventsRule("J1", 0, eventLine(1, "terminal", ""), trailer(1, "done"), 0),
		eventsRule("J1", 1, "", trailer(1, "running"), 0),
	)
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, exe, "machine-a")
	seedJob(t, cfgDir, "J1", "org/repo", 0, "running")
	stdout, _, exit := runHermes(t, cfgDir, false, "", "sync", "--job", "J1")
	if exit != 0 {
		t.Fatalf("first sync: exit = %d, stdout %q", exit, stdout)
	}
	if stdout != `{"jobs":1,"novos":1,"enviados":0,"pendentes":1}`+"\n" {
		t.Fatalf("first sync stdout = %q", stdout)
	}
	j := loadJob(t, cfgDir, "J1")
	if j.Estado != "done" || j.TerminalSeenAt != postTerminalTestNow().Format(outbox.TSLayout) {
		t.Fatalf("after the terminal trailer: estado = %q, terminal_seen_at = %q, want done and the anchor", j.Estado, j.TerminalSeenAt)
	}
	// The job comes back to a non-terminal estado: the anchor is cleared.
	stdout, _, exit = runHermes(t, cfgDir, false, "", "sync", "--job", "J1")
	if exit != 0 {
		t.Fatalf("second sync: exit = %d, stdout %q", exit, stdout)
	}
	if stdout != `{"jobs":1,"novos":0,"enviados":0,"pendentes":1}`+"\n" {
		t.Fatalf("second sync stdout = %q", stdout)
	}
	if j := loadJob(t, cfgDir, "J1"); j.Estado != "running" || j.TerminalSeenAt != "" {
		t.Fatalf("after the non-terminal trailer: estado = %q, terminal_seen_at = %q, want running and no anchor", j.Estado, j.TerminalSeenAt)
	}
	if got := eventsSinceArgs(t, fakeDir); len(got) != 2 || got[0] != "0" || got[1] != "1" {
		t.Fatalf("events --since args = %v, want [0 1]", got)
	}
}
