package registry

// Shared seed and dump helpers of the registry tests, the retention
// options validation and the session-keys invariant. The jobs and
// sessions kinds have no count cap (DJA-275): any new row is admitted
// and no row is evicted to make room for another; the ended retention
// sweep bounds the ended rows, and the high-count warn signals a
// population approaching the bound that can fail the jobs client. The
// stores use the existing fixed clock; large populations are seeded
// with direct SQL in one transaction.

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/djalmajr/nau/internal/outbox"
)

const (
	retDay       = 24 * 3600
	retSeed1Day  = retDay
	retSeed8Day  = 8 * retDay
	retSeed31Day = 31 * retDay
)

// retStore opens a fresh store with the given options and closes it on
// cleanup.
func retStore(t *testing.T, opts Options) *Store {
	t.Helper()
	if opts.Now == nil {
		opts.Now = fixedClock()
	}
	s, err := Open(pathFor(t), opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// retEndedJob seeds one ended job row (with one event) at the given
// age, in one transaction; like every real row it carries its last
// activity.
func retEndedJob(t *testing.T, s *Store, maquina, chave string, age int64) {
	t.Helper()
	if _, err := s.db.Exec(`INSERT INTO jobs(maquina, chave, origem, job_id, encerrado, encerrado_unix, ultima_atividade, ultima_atividade_unix)
		VALUES(?,?, 'job', ?, 1, ?, ?, ?)`, maquina, chave, chave, fixedNow.Unix()-age, fixedNow.Format(outbox.TSLayout), fixedNow.Unix()-age); err != nil {
		t.Fatalf("seed the ended job %s: %v", chave, err)
	}
	if _, err := s.db.Exec(`INSERT INTO events(maquina, chave, seq, ts, tipo, resumo)
		VALUES(?, ?, 1, '2026-01-01T00:00:00-03:00', 'note', 'x')`, maquina, chave); err != nil {
		t.Fatalf("seed the event of %s: %v", chave, err)
	}
}

// retEndedSessao seeds one ended sessao row (with one event) at the
// given age; a negative age is a future-dated row. Like every real row
// it carries its last activity.
func retEndedSessao(t *testing.T, s *Store, maquina, chave string, age int64) {
	t.Helper()
	if _, err := s.db.Exec(`INSERT INTO jobs(maquina, chave, origem, encerrado, encerrado_unix, ultima_atividade, ultima_atividade_unix)
		VALUES(?,?, 'sessao', 1, ?, ?, ?)`, maquina, chave, fixedNow.Unix()-age, fixedNow.Format(outbox.TSLayout), fixedNow.Unix()-age); err != nil {
		t.Fatalf("seed the ended session %s: %v", chave, err)
	}
	if _, err := s.db.Exec(`INSERT INTO events(maquina, chave, seq, ts, tipo, resumo)
		VALUES(?, ?, 1, '2026-01-01T00:00:00-03:00', 'note', 'x')`, maquina, chave); err != nil {
		t.Fatalf("seed the event of %s: %v", chave, err)
	}
}

// retActiveJob seeds one active job row with its last activity set.
func retActiveJob(t *testing.T, s *Store, maquina, chave string) {
	t.Helper()
	if _, err := s.db.Exec(`INSERT INTO jobs(maquina, chave, origem, job_id, ultima_atividade, ultima_atividade_unix) VALUES(?,?, 'job', ?, ?, ?)`,
		maquina, chave, chave, fixedNow.Format(outbox.TSLayout), fixedNow.Unix()); err != nil {
		t.Fatalf("seed the active job %s: %v", chave, err)
	}
}

// retJobEventRec builds one job_event record for the named job.
func retJobEventRec(jobID string, seq int64, tipo string) outbox.Record {
	return recordBase(func(r *outbox.Record) {
		r.JobID = ptr(jobID)
		r.IdempotencyKey = fmt.Sprintf("%s:%d", jobID, seq)
		r.Dados = eventData(seq, tipo, "x", nil)
	})
}

// retWantRefusal checks the ErrInvalid refusal and its exact text.
func retWantRefusal(t *testing.T, err error, kind string) {
	t.Helper()
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("err = %v, want the ErrInvalid refusal", err)
	}
	if got := strings.TrimPrefix(err.Error(), ErrInvalid.Error()+": "); got != kind+" limit reached" {
		t.Fatalf("refusal text = %q, want %q limit reached", got, kind)
	}
}

// retDump is the full state of every user table: one line per row with
// the table name and every column, in the primary-key order.
func retDump(t *testing.T, s *Store) map[string][]string {
	t.Helper()
	tables := map[string]string{
		"nodes":    `SELECT * FROM nodes ORDER BY maquina`,
		"jobs":     `SELECT * FROM jobs ORDER BY maquina, chave`,
		"events":   `SELECT * FROM events ORDER BY maquina, chave, seq`,
		"waits":    `SELECT * FROM waits ORDER BY maquina, pane`,
		"claims":   `SELECT * FROM claims ORDER BY id`,
		"policies": `SELECT * FROM policies ORDER BY escopo, valor`,
		"folders":  `SELECT * FROM folders ORDER BY maquina, caminho`,
	}
	out := make(map[string][]string, len(tables))
	for table, query := range tables {
		rows, err := s.db.Query(query)
		if err != nil {
			t.Fatalf("dump %s: %v", table, err)
		}
		cols, err := rows.Columns()
		if err != nil {
			rows.Close()
			t.Fatalf("dump %s columns: %v", table, err)
		}
		for rows.Next() {
			vals := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				rows.Close()
				t.Fatalf("dump %s scan: %v", table, err)
			}
			parts := make([]string, 0, len(cols)+1)
			parts = append(parts, table)
			for _, v := range vals {
				switch x := v.(type) {
				case nil:
					parts = append(parts, "\\N")
				case int64:
					parts = append(parts, fmt.Sprintf("%d", x))
				case []byte:
					parts = append(parts, string(x))
				default:
					parts = append(parts, fmt.Sprintf("%v", x))
				}
			}
			out[table] = append(out[table], strings.Join(parts, "\x1f"))
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			t.Fatalf("dump %s: %v", table, err)
		}
		rows.Close()
	}
	return out
}

// rowsEqual compares two row dumps exactly.
func rowsEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// retCountKind counts the retained rows of one kind.
func retCountKind(t *testing.T, s *Store, kind string) int {
	t.Helper()
	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM jobs WHERE ` + kindPred(kind)).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", kind, err)
	}
	return n
}

// TestRetentionOptions: a zero EndedRetention means the default 30-day
// retention; retentions outside whole days 7..30 make Open fail with an
// ErrInvalid wrap. Each opened store is closed by a cleanup before its
// TempDir is removed.
func TestRetentionOptions(t *testing.T) {
	for _, tc := range []struct {
		name    string
		opts    Options
		wantErr bool
	}{
		{name: "defaults zero", opts: Options{}, wantErr: false},
		{name: "retention 7 days", opts: Options{EndedRetention: 7 * 24 * time.Hour}, wantErr: false},
		{name: "retention 30 days", opts: Options{EndedRetention: 30 * 24 * time.Hour}, wantErr: false},
		{name: "retention 30 days explicit", opts: Options{EndedRetention: 720 * time.Hour}, wantErr: false},
		{name: "retention 6 days", opts: Options{EndedRetention: 6 * 24 * time.Hour}, wantErr: true},
		{name: "retention 31 days", opts: Options{EndedRetention: 31 * 24 * time.Hour}, wantErr: true},
		{name: "retention negative", opts: Options{EndedRetention: -time.Hour}, wantErr: true},
		{name: "retention not whole days", opts: Options{EndedRetention: 8*24*time.Hour + time.Hour}, wantErr: true},
		{name: "retention not whole days small", opts: Options{EndedRetention: 7*24*time.Hour + time.Second}, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := Open(pathFor(t), tc.opts)
			if s != nil {
				t.Cleanup(func() {
					if err := s.Close(); err != nil {
						t.Errorf("Close: %v", err)
					}
				})
			}
			if tc.wantErr {
				if !errors.Is(err, ErrInvalid) {
					t.Fatalf("Open = %v, want an ErrInvalid wrap", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Open = %v, want ok", err)
			}
		})
	}
}

// TestSessionKeysNeverHoldEvents: a sessao row cannot hold events rows
// through any admission or update path, which the inventory bound relies
// on to size the sessions rows without event rings. A job_event record's
// key is its job_id, which the contract id rule (no ':' or '#') cannot
// address with a sessao key; a session record that names a job validates
// that job with the same rule and targets the job row; a dispatch, amend
// or decision record names its job through the same rule; a poll
// workspace label reduces to a job id or a ws:<id> key, never a sessao
// key; and only a job event inserts an events row. The test tries every
// path with hostile inputs and asserts every session key keeps zero
// events rows.
func TestSessionKeysNeverHoldEvents(t *testing.T) {
	s := openStore(t)
	// A real session row, so a stray event could land on its key.
	if !mustIngest(t, s, sessaoRec("2026-10-10T11:00:00-03:00", "start", "card-9", "feat/x", "", "")) {
		t.Fatal("session start: applied = false")
	}
	// (1) A job_event whose job_id is shaped like a session key is
	// refused by the id rule: it cannot address a sessao row.
	if _, err := s.Ingest(recordBase(func(r *outbox.Record) {
		r.JobID = ptr("sessao:x#y")
		r.IdempotencyKey = "sessao:x#y:1"
	})); !errors.Is(err, ErrInvalid) {
		t.Fatalf("job_event with job_id sessao:x#y: err = %v, want the id-rule refusal", err)
	}
	// (2) A session record that names a job shaped like a session key is
	// refused the same way...
	if _, err := s.Ingest(sessaoRec("2026-10-10T11:00:30-03:00", "update", "", "feat/x", "", "sessao:org/repo#feat")); !errors.Is(err, ErrInvalid) {
		t.Fatalf("session record with job sessao:org/repo#feat: err = %v, want the id-rule refusal", err)
	}
	// ...while a session record that names a real job lands on the job
	// row (the jobs kind), never on a session row.
	if !mustIngest(t, s, sessaoRec("2026-10-10T11:05:00-03:00", "update", "", "feat/y", "", "job-42")) {
		t.Fatal("session update of job-42: applied = false")
	}
	if f := jobFact(t, s, "node-a", "job-42"); !f.exists {
		t.Errorf("job-42 = %+v, want the named job row touched", f)
	}
	if f := jobFact(t, s, "node-a", "sessao:example-org/example-repo#feat/y"); f.exists {
		t.Errorf("session row for the named job = %+v, want none", f)
	}
	// (3) A dispatch, amend or decision with a session-shaped job id is
	// refused by the same rule, and none of them writes events.
	for _, tipo := range []string{outbox.TipoDispatch, outbox.TipoAmend, outbox.TipoDecision} {
		rec := recordBase(func(r *outbox.Record) {
			r.Tipo = tipo
			r.JobID = ptr("sessao:x#y")
			r.IdempotencyKey = "node-a:sess-shape"
		})
		if _, err := s.Ingest(rec); !errors.Is(err, ErrInvalid) {
			t.Fatalf("%s with job_id sessao:x#y: err = %v, want the id-rule refusal", tipo, err)
		}
	}
	// (4) A poll workspace that looks like a session key lands on a
	// ws:<id> row, never on a sessao row, and the poll writes no events.
	mustApplyPoll(t, s, okPoll("node-b", "",
		PolledWorkspace{ID: "sessao:org/repo#feat", Label: "hm-impl-1", AgentStatus: "working"}))
	if f := jobFact(t, s, "node-b", "sessao:org/repo#feat"); f.exists {
		t.Errorf("session-shaped row from the poll = %+v, want a ws: row only", f)
	}
	if f := jobFact(t, s, "node-b", "ws:sessao:org/repo#feat"); !f.exists {
		t.Errorf("ws:sessao:org/repo#feat = %+v, want the workspace row", f)
	}
	// Every session key keeps zero events rows.
	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM events WHERE chave LIKE 'sessao:%'`).Scan(&n); err != nil {
		t.Fatalf("count session events: %v", err)
	}
	if n != 0 {
		t.Fatalf("events rows for session keys = %d, want 0", n)
	}
	if f := jobFact(t, s, "node-a", "sessao:example-org/example-repo#feat/x"); !f.exists {
		t.Errorf("the session row = %+v, want untouched by the hostile paths", f)
	}
}
