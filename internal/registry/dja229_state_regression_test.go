package registry

// Regression tests for the registry state fixes C-F1..C-F4: a blocked
// terminal job keeps the duplicate protection and an `unblocked` resumes
// it (C-F1); no `Z` timestamp is ever stored or returned (C-F2); an older
// poll never regresses a newer push's activity or the node contact
// (C-F3); the agente filter applies to both wait sources by the owning
// agent (C-F4).

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/djalmajr/herdr-hermes/internal/jobapi"
	"github.com/djalmajr/herdr-hermes/internal/outbox"
)

// sessionStartRecord is the session start that opens job-1 on org/repo
// branch feat/x, so a branch check has something to conflict with.
func sessionStartRecord() outbox.Record {
	return outbox.Record{Schema: 1, Seq: 1, TS: "2026-10-10T11:00:00-03:00", Tipo: outbox.TipoSession, Maquina: "node-a", JobID: ptr("job-1"), IdempotencyKey: "node-a:1", Dados: json.RawMessage(`{"acao":"start","job":"job-1","projeto":"org/repo","branch":"feat/x"}`)}
}

// jobOf finds the inventory job by its key.
func jobOf(t *testing.T, inv Inventory, chave string) Job {
	t.Helper()
	for _, j := range inv.Jobs {
		if j.Chave == chave {
			return j
		}
	}
	t.Fatalf("inventory job %s missing: %+v", chave, inv.Jobs)
	return Job{}
}

// nodeOf finds the inventory node by its label.
func nodeOf(t *testing.T, inv Inventory, maquina string) Node {
	t.Helper()
	for _, n := range inv.Nodes {
		if n.Maquina == maquina {
			return n
		}
	}
	t.Fatalf("inventory node %s missing: %+v", maquina, inv.Nodes)
	return Node{}
}

// waitOf finds the inventory wait by its job key.
func waitOf(t *testing.T, inv Inventory, chave string) (Wait, bool) {
	t.Helper()
	for _, w := range inv.Esperas {
		if w.Chave != nil && *w.Chave == chave {
			return w, true
		}
	}
	return Wait{}, false
}

// conflictCampo reports whether the result names a conflict on the field.
func conflictCampo(res CheckResult, campo string) bool {
	for _, c := range res.Conflitos {
		if c.Campo == campo {
			return true
		}
	}
	return false
}

// invOf reads the inventory with the test-fatal sugar; no filter is the
// full inventory.
func invOf(t *testing.T, s *Store, f ...Filter) Inventory {
	t.Helper()
	var inv Inventory
	var err error
	if len(f) == 0 {
		inv, err = s.Inventory(Filter{})
	} else {
		inv, err = s.Inventory(f[0])
	}
	if err != nil {
		t.Fatalf("Inventory: %v", err)
	}
	return inv
}

// TestDJA229BlockedJobConflicts: a blocked terminal (exits 7, 11 and 14)
// keeps the duplicate protection of its keys, and a forward `unblocked`
// moves the job back to running (terminal false, saida null, no wait)
// without losing the protection. A done terminal (exit 0) releases the
// keys, and an `unblocked` never reopens it.
func TestDJA229BlockedJobConflicts(t *testing.T) {
	for _, tc := range []struct {
		name      string
		exit      int
		blocked   bool   // the exit keeps the job alive and waiting
		afterTerm string // the check status while the job is terminal
		afterUnb  string // the check status after the unblocked
	}{
		{"exit7", 7, true, CheckDuplicate, CheckDuplicate},
		{"exit11", 11, true, CheckDuplicate, CheckDuplicate},
		{"exit14", 14, true, CheckDuplicate, CheckDuplicate},
		{"exit0", 0, false, CheckOK, CheckOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := openStore(t)
			mustIngest(t, s, sessionStartRecord())
			mustIngest(t, s, recordBase(func(r *outbox.Record) {
				r.Seq = 2
				r.Dados = eventData(1, jobapi.TypeTerminal, "stopped", map[string]any{"exit": tc.exit})
			}))
			// The stored shape is unchanged while the job is terminal.
			j := jobOf(t, invOf(t, s), "job-1")
			if !j.Terminal || j.Saida == nil || *j.Saida != tc.exit {
				t.Fatalf("while terminal: terminal = %v saida = %v, want true and %d", j.Terminal, j.Saida, tc.exit)
			}
			if j.EsperandoHumano != tc.blocked {
				t.Errorf("while terminal: esperando_humano = %v, want %v", j.EsperandoHumano, tc.blocked)
			}
			res, err := s.Check(CheckRequest{Agente: "other", Projeto: "org/repo", Branch: "feat/x"})
			if err != nil {
				t.Fatalf("Check while terminal: %v", err)
			}
			if res.Status != tc.afterTerm {
				t.Fatalf("check while terminal = %q, want %q", res.Status, tc.afterTerm)
			}
			if tc.afterTerm == CheckDuplicate && !conflictCampo(res, CampoBranch) {
				t.Fatalf("check while terminal conflicts = %+v, want the branch", res.Conflitos)
			}
			if res.Claim != nil {
				// Release the claim so the next check only sees the job.
				if err := s.Release(res.Claim.ID); err != nil {
					t.Fatalf("Release: %v", err)
				}
			}
			mustIngest(t, s, recordBase(func(r *outbox.Record) {
				r.Seq = 3
				r.IdempotencyKey = "job-1:2"
				r.Dados = eventData(2, jobapi.TypeUnblocked, "resumed", nil)
			}))
			j = jobOf(t, invOf(t, s), "job-1")
			if j.EsperandoHumano {
				t.Errorf("after the unblocked: esperando_humano = true, want false")
			}
			if tc.blocked {
				if j.Terminal {
					t.Errorf("after the unblocked: terminal = true, want false (back to running)")
				}
				if j.Saida != nil {
					t.Errorf("after the unblocked: saida = %d, want null", *j.Saida)
				}
			} else {
				if !j.Terminal {
					t.Errorf("after the unblocked: terminal = false, want true (never reopened)")
				}
				if j.Saida == nil || *j.Saida != tc.exit {
					t.Errorf("after the unblocked: saida = %v, want %d kept", j.Saida, tc.exit)
				}
			}
			res, err = s.Check(CheckRequest{Agente: "other", Projeto: "org/repo", Branch: "feat/x"})
			if err != nil {
				t.Fatalf("Check after the unblocked: %v", err)
			}
			if res.Status != tc.afterUnb {
				t.Errorf("check after the unblocked = %q, want %q", res.Status, tc.afterUnb)
			}
			if tc.afterUnb == CheckDuplicate && !conflictCampo(res, CampoBranch) {
				t.Errorf("check after the unblocked conflicts = %+v, want the branch", res.Conflitos)
			}
		})
	}
}

// TestDJA229UnblockedReplayIdempotent: replaying the same `unblocked`
// record and an older terminal replay leave the resumed state unchanged.
func TestDJA229UnblockedReplayIdempotent(t *testing.T) {
	s := openStore(t)
	mustIngest(t, s, sessionStartRecord())
	terminal := recordBase(func(r *outbox.Record) {
		r.Seq = 2
		r.Dados = eventData(1, jobapi.TypeTerminal, "stopped", map[string]any{"exit": 7})
	})
	if !mustIngest(t, s, terminal) {
		t.Fatal("terminal: applied = false, want true")
	}
	unblocked := recordBase(func(r *outbox.Record) {
		r.Seq = 3
		r.IdempotencyKey = "job-1:2"
		r.Dados = eventData(2, jobapi.TypeUnblocked, "resumed", nil)
	})
	if !mustIngest(t, s, unblocked) {
		t.Fatal("unblocked: applied = false, want true")
	}
	if f := jobFact(t, s, "node-a", "job-1"); f.terminal != 0 || f.saida != nil || f.esperando != 0 {
		t.Fatalf("resumed row = %+v, want active, non-terminal, not waiting", f)
	}
	// The replays change nothing.
	if mustIngest(t, s, unblocked) {
		t.Error("unblocked replay: applied = true, want false")
	}
	if mustIngest(t, s, terminal) {
		t.Error("terminal replay: applied = true, want false")
	}
	f := jobFact(t, s, "node-a", "job-1")
	if f.terminal != 0 || f.saida != nil || f.esperando != 0 || f.ultimoSeq != 2 {
		t.Errorf("after the replays: row = %+v, want the resumed state unchanged", f)
	}
	j := jobOf(t, invOf(t, s), "job-1")
	if j.Terminal || j.Saida != nil || j.EsperandoHumano {
		t.Errorf("after the replays: job = %+v, want active, non-terminal, not waiting", j)
	}
}

// TestDJA229BlockedStateSurvivesReopen: a blocked terminal row written by
// a normal ingest still conflicts after the store is closed and reopened
// (the protection needs no migration), and an `unblocked` before another
// reopen leaves an active, non-terminal row.
func TestDJA229BlockedStateSurvivesReopen(t *testing.T) {
	path := pathFor(t)
	s, err := Open(path, Options{Now: fixedClock()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	mustIngest(t, s, sessionStartRecord())
	mustIngest(t, s, recordBase(func(r *outbox.Record) {
		r.Seq = 2
		r.Dados = eventData(1, jobapi.TypeTerminal, "stopped", map[string]any{"exit": 7})
	}))
	s.Close()

	s2, err := Open(path, Options{Now: fixedClock()})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	res, err := s2.Check(CheckRequest{Agente: "other", Projeto: "org/repo", Branch: "feat/x"})
	if err != nil {
		t.Fatalf("Check after the reopen: %v", err)
	}
	if res.Status != CheckDuplicate || !conflictCampo(res, CampoBranch) {
		t.Errorf("check after the reopen = %+v, want duplicate on the branch", res)
	}
	mustIngest(t, s2, recordBase(func(r *outbox.Record) {
		r.Seq = 3
		r.IdempotencyKey = "job-1:2"
		r.Dados = eventData(2, jobapi.TypeUnblocked, "resumed", nil)
	}))
	s2.Close()

	s3, err := Open(path, Options{Now: fixedClock()})
	if err != nil {
		t.Fatalf("second reopen: %v", err)
	}
	defer s3.Close()
	j := jobOf(t, invOf(t, s3), "job-1")
	if j.Terminal || j.Saida != nil || j.EsperandoHumano {
		t.Errorf("after the unblocked and another reopen: job = %+v, want active, non-terminal, not waiting", j)
	}
}

// TestDJA229EventTimestampsNumericOffset: a record ts with a `Z` suffix,
// an event ts with a `Z` suffix and an event ts with fractional seconds
// are stored and returned with a numeric offset, never `Z`, at the same
// instant (second precision) as the input.
func TestDJA229EventTimestampsNumericOffset(t *testing.T) {
	s := openStore(t)
	// A Z record ts and a Z event ts on job-1.
	if !mustIngest(t, s, recordBase(func(r *outbox.Record) {
		r.TS = "2026-10-10T15:00:00Z"
		r.Dados = json.RawMessage(`{"seq":1,"ts":"2026-10-10T14:30:00Z","tipo":"question","resumo":"which"}`)
	})) {
		t.Fatal("record 1: applied = false, want true")
	}
	// Fractional seconds and a numeric offset on job-2, at the same
	// instant as job-1's event.
	if !mustIngest(t, s, recordBase(func(r *outbox.Record) {
		r.JobID = ptr("job-2")
		r.IdempotencyKey = "job-2:1"
		r.TS = "2026-10-10T15:00:01Z"
		r.Dados = json.RawMessage(`{"seq":1,"ts":"2026-10-10T17:30:00.5-03:00","tipo":"question","resumo":"which"}`)
	})) {
		t.Fatal("record 2: applied = false, want true")
	}
	inv := invOf(t, s)
	for _, tc := range []struct {
		chave  string
		input  string // the ts as the producer wrote it
		stored string // the canonical numeric-offset form
	}{
		{"job-1", "2026-10-10T14:30:00Z", "2026-10-10T14:30:00+00:00"},
		{"job-2", "2026-10-10T17:30:00.5-03:00", "2026-10-10T17:30:00-03:00"},
	} {
		t.Run(tc.chave, func(t *testing.T) {
			j := jobOf(t, inv, tc.chave)
			if len(j.Eventos) != 1 {
				t.Fatalf("events = %d, want 1", len(j.Eventos))
			}
			if j.UltimaAtividade == nil {
				t.Fatal("ultima_atividade is null, want the event ts")
			}
			in, err := time.Parse(time.RFC3339, tc.input)
			if err != nil {
				t.Fatalf("parse the input ts: %v", err)
			}
			for _, got := range []string{j.Eventos[0].TS, *j.UltimaAtividade} {
				if strings.HasSuffix(got, "Z") {
					t.Errorf("%s: %q ends with Z, want a numeric offset", tc.chave, got)
				}
				if got != tc.stored {
					t.Errorf("%s: stored %q, want %q", tc.chave, got, tc.stored)
				}
				gotT, err := time.Parse(time.RFC3339, got)
				if err != nil {
					t.Fatalf("parse the stored ts %q: %v", got, err)
				}
				if gotT.Unix() != in.Unix() {
					t.Errorf("%s: stored %q is instant %d, want %d (the input instant)", tc.chave, got, gotT.Unix(), in.Unix())
				}
			}
			w, ok := waitOf(t, inv, tc.chave)
			if !ok {
				t.Fatalf("the question wait of %s is missing: %+v", tc.chave, inv.Esperas)
			}
			if strings.HasSuffix(w.Desde, "Z") || w.Desde != tc.stored {
				t.Errorf("%s: wait desde = %q, want %q", tc.chave, w.Desde, tc.stored)
			}
		})
	}
}

// TestDJA229PollDoesNotRegressPush: an ok and a failed poll that started
// before a newer push keep the push's last activity and the node contact,
// while still writing their own poll time and status; a poll at a later
// instant with a different offset string moves both forward (an
// instant, not a string, comparison); a backwards server clock never
// yields a negative idle.
func TestDJA229PollDoesNotRegressPush(t *testing.T) {
	s := openStore(t)
	push := sessionStartRecord()
	push.TS = fixedNow.Format(outbox.TSLayout)
	mustIngest(t, s, push)
	late := fixedNow.Add(-30 * time.Second)
	lateTS := late.Format(outbox.TSLayout)
	assertKept := func(t *testing.T, status string) {
		t.Helper()
		inv := invOf(t, s)
		j := jobOf(t, inv, "job-1")
		if j.OciosoS == nil || *j.OciosoS != 0 {
			t.Errorf("ocioso_s = %v, want 0: the newer push activity must not regress", j.OciosoS)
		}
		if j.UltimaAtividade == nil || *j.UltimaAtividade != fixedNow.Format(outbox.TSLayout) {
			t.Errorf("ultima_atividade = %v, want the push instant kept", j.UltimaAtividade)
		}
		n := nodeOf(t, inv, "node-a")
		if n.UltimoContato == nil || *n.UltimoContato != fixedNow.Format(outbox.TSLayout) {
			t.Errorf("ultimo_contato = %v, want the push contact kept", n.UltimoContato)
		}
		if n.UltimoPoll == nil || *n.UltimoPoll != lateTS {
			t.Errorf("ultimo_poll = %v, want the late poll time %q", n.UltimoPoll, lateTS)
		}
		if n.PollStatus == nil || *n.PollStatus != status {
			t.Errorf("poll_status = %v, want %q", n.PollStatus, status)
		}
	}
	// (a) An ok poll started 30 s before the push.
	p := okPoll("node-a", "", PolledWorkspace{ID: "w1", Label: "job-job-1", AgentStatus: "working"})
	p.At = late
	mustApplyPoll(t, s, p)
	assertKept(t, "ok")
	// (b) A failed poll at the same late time: it still writes its own
	// poll time and status and never touches the contact.
	mustApplyPoll(t, s, PollResult{Maquina: "node-a", At: late, OK: false, Motivo: "probe_timeout"})
	assertKept(t, "probe_timeout")
	// A poll at a later instant, formatted in a zone whose wall clock
	// reads earlier than the stored one: only an instant comparison can
	// move the facts forward.
	z := time.FixedZone("test-11", -11*3600)
	newer := fixedNow.Add(60 * time.Second).In(z)
	np := okPoll("node-a", "", PolledWorkspace{ID: "w1", Label: "job-job-1", AgentStatus: "working"})
	np.At = newer
	mustApplyPoll(t, s, np)
	inv := invOf(t, s)
	j := jobOf(t, inv, "job-1")
	if j.UltimaAtividade == nil || *j.UltimaAtividade != newer.Format(outbox.TSLayout) {
		t.Errorf("after the newer poll: ultima_atividade = %v, want %q", j.UltimaAtividade, newer.Format(outbox.TSLayout))
	}
	n := nodeOf(t, inv, "node-a")
	if n.UltimoContato == nil || *n.UltimoContato != newer.Format(outbox.TSLayout) {
		t.Errorf("after the newer poll: ultimo_contato = %v, want %q", n.UltimoContato, newer.Format(outbox.TSLayout))
	}
	if n.UltimoPoll == nil || *n.UltimoPoll != newer.Format(outbox.TSLayout) {
		t.Errorf("after the newer poll: ultimo_poll = %v, want the poll time", n.UltimoPoll)
	}

	// A backwards server clock: the idle never goes negative and a late
	// push after the clock moved back never regresses the contact.
	t.Run("backwards clock", func(t *testing.T) {
		nowFn, set := movableClock()
		path := pathFor(t)
		s2, err := Open(path, Options{Now: nowFn})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		t.Cleanup(func() { s2.Close() })
		mustIngest(t, s2, push)
		set(-60) // the server clock moves back, before the push instant
		// A second record on the same machine after the clock moved back.
		mustIngest(t, s2, recordBase(func(r *outbox.Record) {
			r.JobID = ptr("job-2")
			r.IdempotencyKey = "job-2:1"
		}))
		inv := invOf(t, s2)
		j := jobOf(t, inv, "job-1")
		if j.OciosoS == nil || *j.OciosoS < 0 {
			t.Errorf("ocioso_s = %v, want >= 0 with a backwards clock", j.OciosoS)
		}
		if j.UltimaAtividade == nil || *j.UltimaAtividade != fixedNow.Format(outbox.TSLayout) {
			t.Errorf("ultima_atividade = %v, want the push instant kept", j.UltimaAtividade)
		}
		n := nodeOf(t, inv, "node-a")
		if n.UltimoContato == nil || *n.UltimoContato != fixedNow.Format(outbox.TSLayout) {
			t.Errorf("ultimo_contato = %v, want the first contact kept after the late push", n.UltimoContato)
		}
	})
}

// TestDJA229AgentFilterWaits: the agente filter applies to both wait
// sources by the owning agent: a pane wait is kept when its workspace
// resolves to a non-closed job of the filtered agent (never by the pane's
// own name), a waiting job is kept only when it belongs to the filtered
// agent, every kept wait key names a job of the filtered jobs list, and
// the unfiltered output is unchanged.
func TestDJA229AgentFilterWaits(t *testing.T) {
	s := openStore(t)
	// Alice's blocked hm workspace, with a pane whose worker is named
	// build (the worker name, not the owner).
	mustApplyPoll(t, s, PollResult{Maquina: "node-a", At: fixedNow, OK: true,
		Workspaces: []PolledWorkspace{{ID: "w1", Label: "hm-alice-impl-x", AgentStatus: "blocked"}},
		Panes:      []PolledPane{{PaneID: "p1", WorkspaceID: "w1", Name: "build", AgentStatus: "blocked"}}})
	// A waiting job owned by alice on another machine, with its question
	// event so the wait has a since.
	if _, err := s.db.Exec(`INSERT INTO jobs(maquina, chave, origem, job_id, agente, projeto, branch, esperando_humano, espera_motivo, ultima_atividade, ultima_atividade_unix, fonte_push)
		VALUES('node-b', 'job-9', 'job', 'job-9', 'alice', 'org/repo', 'feat/y', 1, 'question', '2026-10-10T11:45:00-03:00', ?, 1)`, fixedNow.Unix()-900); err != nil {
		t.Fatalf("seed the waiting job: %v", err)
	}
	if _, err := s.db.Exec(`INSERT INTO events(maquina, chave, seq, ts, tipo, resumo)
		VALUES('node-b', 'job-9', 1, '2026-10-10T11:45:00-03:00', 'question', 'which side')`); err != nil {
		t.Fatalf("seed the waiting job event: %v", err)
	}

	invAll := invOf(t, s)
	if len(invAll.Jobs) != 2 || len(invAll.Esperas) != 2 {
		t.Fatalf("unfiltered: jobs = %d esperas = %d, want 2 and 2", len(invAll.Jobs), len(invAll.Esperas))
	}
	// The unfiltered shape is pinned: the pane wait and the job wait,
	// both keyed to their jobs.
	if w, ok := waitOf(t, invAll, "ws:w1"); !ok || w.Tipo != EsperaPane || w.Agente == nil || *w.Agente != "build" || w.Desde != fixedNow.Format(outbox.TSLayout) {
		t.Errorf("unfiltered pane wait = %+v (ok %v), want the build pane waiting since the poll", w, ok)
	}
	if w, ok := waitOf(t, invAll, "job-9"); !ok || w.Tipo != "question" || w.Desde != "2026-10-10T11:45:00-03:00" {
		t.Errorf("unfiltered job wait = %+v (ok %v), want alice's question", w, ok)
	}

	invAlice := invOf(t, s, Filter{Agente: "alice"})
	if len(invAlice.Jobs) != 2 {
		t.Fatalf("alice: jobs = %d, want 2 (both are hers)", len(invAlice.Jobs))
	}
	if len(invAlice.Esperas) != 2 {
		t.Fatalf("alice: esperas = %d, want 2 (the pane wait by its job owner and the job wait)", len(invAlice.Esperas))
	}
	chaves := make(map[string]bool, len(invAlice.Jobs))
	for _, j := range invAlice.Jobs {
		chaves[j.Chave] = true
	}
	for _, w := range invAlice.Esperas {
		if w.Chave == nil || !chaves[*w.Chave] {
			t.Errorf("alice: wait %+v names a key outside the filtered jobs", w)
		}
	}
	// The pane wait is kept by the job's owner; its agente stays the
	// pane's worker name.
	if w, ok := waitOf(t, invAlice, "ws:w1"); !ok || w.Agente == nil || *w.Agente != "build" {
		t.Errorf("alice: pane wait = %+v (ok %v), want it kept with the pane name", w, ok)
	}

	// bob owns nothing: zero jobs, zero waits.
	invBob := invOf(t, s, Filter{Agente: "bob"})
	if len(invBob.Jobs) != 0 || len(invBob.Esperas) != 0 {
		t.Errorf("bob: jobs = %d esperas = %d, want 0 and 0", len(invBob.Jobs), len(invBob.Esperas))
	}
	// build is the pane worker name, not an owner: filtering by it keeps
	// nothing.
	invBuild := invOf(t, s, Filter{Agente: "build"})
	if len(invBuild.Jobs) != 0 || len(invBuild.Esperas) != 0 {
		t.Errorf("build: jobs = %d esperas = %d, want 0 and 0 (the pane name is not the owner)", len(invBuild.Jobs), len(invBuild.Esperas))
	}
}
