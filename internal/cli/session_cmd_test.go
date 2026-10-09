package cli

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/djalmajr/herdr-hermes/internal/outbox"
)

func sessionRecord(t *testing.T, dir string, n int) outbox.Record {
	t.Helper()
	lines := outboxLines(t, dir)
	if len(lines) != n {
		t.Fatalf("outbox has %d lines, want %d:\n%s", len(lines), n, strings.Join(lines, "\n"))
	}
	var rec outbox.Record
	if err := json.Unmarshal([]byte(lines[n-1]), &rec); err != nil {
		t.Fatalf("record %d: %v", n, err)
	}
	return rec
}

func sessionsFile(t *testing.T, dir string) outbox.Sessions {
	t.Helper()
	data, err := os.ReadFile(dir + "/state/sessions.json")
	if err != nil {
		t.Fatalf("read sessions.json: %v", err)
	}
	var ss outbox.Sessions
	if err := json.Unmarshal(data, &ss); err != nil {
		t.Fatalf("sessions.json: %v", err)
	}
	return ss
}

// TestSession covers criterion 9 for sessions: records shaped as specified,
// estado cut at 280 code points, no person field, a repeated start updates
// sessions.json and still appends a record, targeting and closing rules.
func TestSession(t *testing.T) {
	dir := t.TempDir()
	setConfig(t, dir, map[string]string{"machine_label": "m1"})

	// start creates the entry and appends the record.
	stdout, stderr, exit := runCLIStdin(t, dir, "", nil, "session", "start", "--projeto", "org/repo", "--branch", "b1", "--card", "c1", "--job", "job-1")
	if exit != 0 {
		t.Fatalf("session start exit = %d, stderr %q", exit, stderr)
	}
	if stdout != `{"seq":1}
` {
		t.Fatalf("session start stdout = %q, want {\"seq\":1}", stdout)
	}
	ss := sessionsFile(t, dir)
	e, ok := ss.Sessions["org/repo@b1"]
	if !ok || e.Branch != "b1" || e.Card != "c1" || e.Job != "job-1" || e.StartedAt == "" || e.UpdatedAt == "" {
		t.Fatalf("sessions.json = %+v, want org/repo@b1 with the given fields", ss)
	}
	rec := sessionRecord(t, dir, 1)
	if rec.Tipo != outbox.TipoSession || rec.Maquina != "m1" || rec.Projeto != "org/repo" {
		t.Fatalf("record = %+v", rec)
	}
	if rec.JobID == nil || *rec.JobID != "job-1" {
		t.Fatalf("record job_id = %v, want job-1", rec.JobID)
	}
	var dados map[string]any
	if err := json.Unmarshal(rec.Dados, &dados); err != nil {
		t.Fatal(err)
	}
	wantDados := map[string]any{"acao": "start", "projeto": "org/repo", "branch": "b1", "card": "c1", "job": "job-1"}
	assertDados(t, dados, wantDados)
	assertNoPersonField(t, rec)

	// A repeated start updates the entry (card replaced, branch and job
	// kept, started_at kept) and still appends a record.
	stdout, stderr, exit = runCLIStdin(t, dir, "", nil, "session", "start", "--projeto", "org/repo", "--branch", "b1", "--card", "c2")
	if exit != 0 || stdout != `{"seq":2}
` {
		t.Fatalf("repeated start: exit %d stdout %q stderr %q", exit, stdout, stderr)
	}
	ss = sessionsFile(t, dir)
	e = ss.Sessions["org/repo@b1"]
	if e.Card != "c2" || e.Branch != "b1" || e.Job != "job-1" || e.StartedAt == "" {
		t.Fatalf("sessions.json after repeated start = %+v, want the updated entry", e)
	}
	if len(ss.Sessions) != 1 {
		t.Fatalf("sessions.json has %d entries, want 1 (start updates in place)", len(ss.Sessions))
	}
	rec = sessionRecord(t, dir, 2)
	dados = map[string]any{}
	if err := json.Unmarshal(rec.Dados, &dados); err != nil {
		t.Fatal(err)
	}
	assertDados(t, dados, map[string]any{"acao": "start", "projeto": "org/repo", "branch": "b1", "card": "c2", "job": "job-1"})

	// update without --branch targets the most recently updated open
	// session of the projeto. A second session is newer than the first.
	store, err := outbox.Open(outbox.StateDir(dir), outbox.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateSessions(func(s *outbox.Sessions) error {
		s.Sessions["org/repo@b2"] = &outbox.Session{Projeto: "org/repo", Branch: "b2", StartedAt: "2026-01-01T00:00:00+00:00", UpdatedAt: "2026-01-02T00:00:00+00:00"}
		// b1 is older.
		s.Sessions["org/repo@b1"].UpdatedAt = "2026-01-01T00:00:00+00:00"
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	stdout, stderr, exit = runCLIStdin(t, dir, "", nil, "session", "update", "--projeto", "org/repo", "--estado", "working")
	if exit != 0 || stdout != `{"seq":3}
` {
		t.Fatalf("session update exit = %d stdout %q stderr %q", exit, stdout, stderr)
	}
	rec = sessionRecord(t, dir, 3)
	dados = map[string]any{}
	if err := json.Unmarshal(rec.Dados, &dados); err != nil {
		t.Fatal(err)
	}
	assertDados(t, dados, map[string]any{"acao": "update", "projeto": "org/repo", "branch": "b2", "estado": "working"})
	ss = sessionsFile(t, dir)
	if e := ss.Sessions["org/repo@b2"]; e == nil || e.Estado != "working" {
		t.Fatalf("update hit the wrong entry: %+v", ss)
	}
	if e := ss.Sessions["org/repo@b1"]; e == nil || e.Estado != "" {
		t.Fatalf("update touched the older entry: %+v", e)
	}

	// end closes the most recently updated open session (b2, which the
	// update above just touched): the entry is removed and the record
	// carries the merged state.
	stdout, stderr, exit = runCLIStdin(t, dir, "", nil, "session", "end", "--projeto", "org/repo", "--estado", "done")
	if exit != 0 || stdout != `{"seq":4}
` {
		t.Fatalf("session end exit = %d stdout %q stderr %q", exit, stdout, stderr)
	}
	rec = sessionRecord(t, dir, 4)
	dados = map[string]any{}
	if err := json.Unmarshal(rec.Dados, &dados); err != nil {
		t.Fatal(err)
	}
	assertDados(t, dados, map[string]any{"acao": "end", "projeto": "org/repo", "branch": "b2", "estado": "done"})
	ss = sessionsFile(t, dir)
	if _, ok := ss.Sessions["org/repo@b2"]; ok {
		t.Fatalf("session end left the entry open: %+v", ss)
	}
	if len(ss.Sessions) != 1 {
		t.Fatalf("sessions.json after end = %+v, want only b1", ss)
	}

	// update --branch targets that branch explicitly.
	stdout, stderr, exit = runCLIStdin(t, dir, "", nil, "session", "update", "--projeto", "org/repo", "--branch", "b1", "--pr", "https://example.invalid/pull/7")
	if exit != 0 || stdout != `{"seq":5}
` {
		t.Fatalf("session update --branch exit = %d stdout %q stderr %q", exit, stdout, stderr)
	}
	rec = sessionRecord(t, dir, 5)
	dados = map[string]any{}
	if err := json.Unmarshal(rec.Dados, &dados); err != nil {
		t.Fatal(err)
	}
	assertDados(t, dados, map[string]any{"acao": "update", "projeto": "org/repo", "branch": "b1", "card": "c2", "job": "job-1", "pr": "https://example.invalid/pull/7"})

	// end with no open session creates and closes: the file stays empty
	// and the record is still appended.
	stdout, stderr, exit = runCLIStdin(t, dir, "", nil, "session", "end", "--projeto", "org/other", "--pr", "https://example.invalid/pull/8")
	if exit != 0 || stdout != `{"seq":6}
` {
		t.Fatalf("session end without open session: exit %d stdout %q stderr %q", exit, stdout, stderr)
	}
	rec = sessionRecord(t, dir, 6)
	dados = map[string]any{}
	if err := json.Unmarshal(rec.Dados, &dados); err != nil {
		t.Fatal(err)
	}
	assertDados(t, dados, map[string]any{"acao": "end", "projeto": "org/other", "pr": "https://example.invalid/pull/8"})
	if rec.JobID != nil {
		t.Fatalf("record job_id = %v, want null", *rec.JobID)
	}
	ss = sessionsFile(t, dir)
	if len(ss.Sessions) != 1 || ss.Sessions["org/other@"] != nil {
		t.Fatalf("sessions.json after end of an unknown session = %+v, want only b1", ss)
	}

	// estado is cut at 280 code points, by runes (multi-byte safe).
	long := strings.Repeat("é", 281) + strings.Repeat("a", 100)
	stdout, stderr, exit = runCLIStdin(t, dir, "", nil, "session", "update", "--projeto", "org/repo", "--branch", "b1", "--estado", long)
	if exit != 0 {
		t.Fatalf("session update --estado exit = %d, stderr %q", exit, stderr)
	}
	rec = sessionRecord(t, dir, 7)
	var dadosStr struct {
		Estado string `json:"estado"`
	}
	if err := json.Unmarshal(rec.Dados, &dadosStr); err != nil {
		t.Fatal(err)
	}
	if len([]rune(dadosStr.Estado)) != 280 {
		t.Fatalf("estado cut = %d code points, want 280", len([]rune(dadosStr.Estado)))
	}
	if !strings.HasPrefix(dadosStr.Estado, strings.Repeat("é", 280)) {
		t.Fatalf("estado cut split a rune or cut the wrong end: %q…", dadosStr.Estado[:20])
	}

	// Usage errors exit 2.
	cases := [][]string{
		{"session"},
		{"session", "bogus", "--projeto", "org/repo"},
		{"session", "start"},
		{"session", "start", "--projeto", "org"},
		{"session", "start", "--projeto", "or g/repo"},
		{"session", "start", "--projeto", "a/b/c"},
		{"session", "start", "--projeto", "org/repo", "--estado", "x"},
		{"session", "update", "--projeto", "org/repo", "--job", "j1"},
		{"session", "end", "--projeto", "org/repo", "--branch", "b1"},
		{"session", "start", "--projeto", "org/repo", "--branch"},
		{"session", "start", "--projeto", "org/repo", "--bogus", "x"},
	}
	for _, args := range cases {
		stdout, stderr, exit := runCLIStdin(t, dir, "", nil, args...)
		if exit != 2 || !strings.Contains(stderr, "herdr-hermes") || !strings.Contains(stdout, `"status":"2"`) {
			t.Errorf("%v: exit %d stderr %q stdout %q", args, exit, stderr, stdout)
		}
	}
	// machine_label empty exits 2.
	dirEmpty := t.TempDir()
	if _, _, exit := runCLIStdin(t, dirEmpty, "", nil, "session", "start", "--projeto", "org/repo"); exit != 2 {
		t.Errorf("session with empty machine_label: exit %d, want 2", exit)
	}
}

// TestSessionEndAppendFailureKeepsFields: when the outbox is not writable,
// session end fails before it removes the open session, so a retry after
// restoring the mode writes the end record with branch, card and job and
// closes the session.
func TestSessionEndAppendFailureKeepsFields(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("chmod 0444 does not prevent writes on Windows")
	}
	dir := t.TempDir()
	setConfig(t, dir, map[string]string{"machine_label": "m1"})
	stdout, stderr, exit := runCLIStdin(t, dir, "", nil, "session", "start", "--projeto", "org/repo", "--branch", "b1", "--card", "c1", "--job", "job-1")
	if exit != 0 {
		t.Fatalf("session start exit = %d, stderr %q", exit, stderr)
	}
	outboxPath := filepath.Join(dir, "state", "outbox.jsonl")
	if err := os.Chmod(outboxPath, 0o444); err != nil {
		t.Fatalf("chmod 0444: %v", err)
	}
	defer os.Chmod(outboxPath, 0o600)
	stdout, stderr, exit = runCLIStdin(t, dir, "", nil, "session", "end", "--projeto", "org/repo", "--estado", "done")
	if exit != 2 {
		t.Fatalf("session end with a read-only outbox: exit = %d, want 2 (stdout %q stderr %q)", exit, stdout, stderr)
	}
	ss := sessionsFile(t, dir)
	e := ss.Sessions["org/repo@b1"]
	if e == nil || e.Branch != "b1" || e.Card != "c1" || e.Job != "job-1" {
		t.Fatalf("sessions.json after the failed end = %+v, want the open session kept (branch b1, card c1, job job-1)", ss)
	}
	// Restore the mode: the retry appends the end record and closes the
	// session.
	if err := os.Chmod(outboxPath, 0o600); err != nil {
		t.Fatalf("chmod 0600: %v", err)
	}
	stdout, stderr, exit = runCLIStdin(t, dir, "", nil, "session", "end", "--projeto", "org/repo", "--estado", "done")
	if exit != 0 {
		t.Fatalf("session end retry exit = %d, stderr %q", exit, stderr)
	}
	rec := sessionRecord(t, dir, 2)
	var dados map[string]any
	if err := json.Unmarshal(rec.Dados, &dados); err != nil {
		t.Fatal(err)
	}
	assertDados(t, dados, map[string]any{"acao": "end", "projeto": "org/repo", "branch": "b1", "card": "c1", "job": "job-1", "estado": "done"})
	if rec.JobID == nil || *rec.JobID != "job-1" {
		t.Fatalf("end record job_id = %v, want job-1", rec.JobID)
	}
	ss = sessionsFile(t, dir)
	if len(ss.Sessions) != 0 {
		t.Fatalf("sessions.json after the retry = %+v, want the session closed", ss)
	}
}

// assertDados requires the dados object to have exactly the given keys and
// values.
func assertDados(t *testing.T, got map[string]any, want map[string]any) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("dados = %v, want exactly %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("dados[%s] = %v, want %v (dados %v)", k, got[k], v, got)
		}
	}
}

// assertNoPersonField requires the record to carry no person field, at any
// level: the receiver derives the person from the authenticated key.
func assertNoPersonField(t *testing.T, rec outbox.Record) {
	t.Helper()
	full := string(recordJSON(rec))
	for _, bad := range []string{`"pessoa"`, `"user"`, `"autor"`, `"email"`, `"nome"`} {
		if strings.Contains(full, bad) {
			t.Fatalf("record carries a person field %s: %s", bad, full)
		}
	}
	var top map[string]any
	if err := json.Unmarshal(recordJSON(rec), &top); err != nil {
		t.Fatal(err)
	}
	if _, ok := top["job_id"]; !ok {
		t.Fatalf("record lost its job_id field: %v", top)
	}
}

// recordJSON is the full record line (top-level fields plus dados) as JSON.
func recordJSON(rec outbox.Record) []byte {
	b, err := json.Marshal(rec)
	if err != nil {
		return []byte(`{}`)
	}
	return b
}
