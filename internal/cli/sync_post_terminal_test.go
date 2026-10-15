package cli

import (
	"encoding/json"
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
// after the folder_retained is durable, a third sync does not query the
// backend.
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
	// After the folder_retained is durable, a third sync does not query
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
// outbox already holds a folder_retained record is not queried: the
// folder_retained closes the close tail and comes out once.
func TestSyncPostTerminalTailDurableSkips(t *testing.T) {
	exe, fakeDir := installFakeSoho(t, capsRule(capsJSON, 0))
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, exe, "machine-a")
	s, err := outbox.Open(outbox.StateDir(cfgDir), outbox.Options{Now: postTerminalTestNow})
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	for _, line := range []string{eventLine(1, "terminal", ""), eventLine(2, "folder_retained", "")} {
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
		t.Fatalf("events calls = %d, want 0 (the durable folder_retained closes the tail)", n)
	}
}

// TestSyncPostTerminalGraceOverSkips: a terminal job without a gap and
// without a close tail is not queried once the post-terminal grace is
// over: the age of the stored terminal record (or, when the state came
// only from the trailer and no terminal record is stored, of the job's
// updated_at) exceeds syncPostTerminalGrace. The instants come from the
// injected clock, never from the backend.
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

	t.Run("no terminal record, updated_at older than the grace", func(t *testing.T) {
		exe, fakeDir := installFakeSoho(t, capsRule(capsJSON, 0))
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
		if stdout != `{"jobs":0,"novos":0,"enviados":0,"pendentes":0}`+"\n" {
			t.Fatalf("stdout = %q", stdout)
		}
		if n := len(eventsSinceArgs(t, fakeDir)); n != 0 {
			t.Fatalf("events calls = %d, want 0 (the updated_at is older than the grace)", n)
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
