package outbox

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func jobEvent(seq int) json.RawMessage {
	return json.RawMessage(`{"seq":` + strconv.Itoa(seq) + `,"ts":"2026-01-02T03:04:05-03:00","tipo":"question","resumo":"which base?"}`)
}

// TestDedupeJobEventOnce: the same (job_id, seq) appended twice (as the wake
// hook and as sync would both do it) yields exactly one record, and the
// retry converges (appended=false, no new line).
func TestDedupeJobEventOnce(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir)
	rec, appended, err := s.AppendJobEvent(testMaquina, testProjeto, "TASK-1", jobEvent(5))
	if err != nil {
		t.Fatalf("AppendJobEvent: %v", err)
	}
	if !appended {
		t.Fatal("first append reported appended=false")
	}
	if rec.IdempotencyKey != "TASK-1:5" {
		t.Errorf("idempotency key = %q, want TASK-1:5", rec.IdempotencyKey)
	}
	if rec.Seq != 1 {
		t.Errorf("seq = %d, want 1", rec.Seq)
	}
	if _, appended, err := s.AppendJobEvent(testMaquina, testProjeto, "TASK-1", jobEvent(5)); err != nil || appended {
		t.Fatalf("retry: appended=%v err=%v, want false,nil", appended, err)
	}
	lines, last, err := s.Read(0)
	if err != nil || len(lines) != 1 || last != 1 {
		t.Fatalf("Read = %d lines, last %d, err %v; want one record", len(lines), last, err)
	}
	// A later event appends and updates the cursor in jobs.json.
	if _, appended, err := s.AppendJobEvent(testMaquina, testProjeto, "TASK-1", jobEvent(6)); err != nil || !appended {
		t.Fatalf("event 6: appended=%v err=%v", appended, err)
	}
	if _, appended, err := s.AppendJobEvent(testMaquina, testProjeto, "TASK-1", jobEvent(6)); err != nil || appended {
		t.Fatalf("event 6 retry: appended=%v err=%v", appended, err)
	}
	jobs, err := s.LoadJobs()
	if err != nil {
		t.Fatal(err)
	}
	j := jobs.Jobs["TASK-1"]
	if j == nil || j.LastEventSeq != 6 || j.Projeto != testProjeto {
		t.Fatalf("jobs.json = %+v", j)
	}
	// An out-of-order older event is refused by last_event_seq.
	if _, appended, err := s.AppendJobEvent(testMaquina, testProjeto, "TASK-1", jobEvent(5)); err != nil || appended {
		t.Fatalf("old event 5: appended=%v err=%v", appended, err)
	}
	// A different job with the same event seq appends its own record.
	if _, appended, err := s.AppendJobEvent(testMaquina, testProjeto, "TASK-2", jobEvent(5)); err != nil || !appended {
		t.Fatalf("TASK-2 event 5: appended=%v err=%v", appended, err)
	}
	if lines, last, err := s.Read(0); err != nil || len(lines) != 3 || last != 3 {
		t.Fatalf("Read = %d lines, last %d, err %v; want 3 records", len(lines), last, err)
	}
}

// TestDedupeKeyFormats: job_event records use <job_id>:<event seq>; every
// other record type uses <maquina>:<outbox seq>.
func TestDedupeKeyFormats(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir)
	rec, err := s.Append(Record{Tipo: TipoDispatch, Maquina: testMaquina, Projeto: testProjeto, Dados: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	if rec.IdempotencyKey != testMaquina+":1" {
		t.Errorf("dispatch key = %q, want machine-a:1", rec.IdempotencyKey)
	}
	for _, tipo := range []string{TipoAmend, TipoSession, TipoDecision} {
		rec, err := s.Append(Record{Tipo: tipo, Maquina: testMaquina, Projeto: testProjeto, Dados: json.RawMessage(`{}`)})
		if err != nil {
			t.Fatal(err)
		}
		want := testMaquina + ":" + strconv.Itoa(int(rec.Seq))
		if rec.IdempotencyKey != want {
			t.Errorf("%s key = %q, want %q", tipo, rec.IdempotencyKey, want)
		}
	}
	jrec, _, err := s.AppendJobEvent(testMaquina, testProjeto, "TASK-9", jobEvent(42))
	if err != nil {
		t.Fatal(err)
	}
	if jrec.IdempotencyKey != "TASK-9:42" {
		t.Errorf("job_event key = %q, want TASK-9:42", jrec.IdempotencyKey)
	}
	// The raw line keeps the key and the original event bytes.
	data, err := os.ReadFile(s.outboxPath())
	if err != nil {
		t.Fatal(err)
	}
	lastLine := strings.TrimSuffix(string(data), "\n")
	if idx := strings.LastIndexByte(lastLine, '\n'); idx >= 0 {
		lastLine = lastLine[idx+1:]
	}
	if !strings.Contains(lastLine, `"idempotency_key":"TASK-9:42"`) {
		t.Errorf("last line = %s", lastLine)
	}
	if !strings.Contains(lastLine, `"tipo":"question"`) || !strings.Contains(lastLine, `"resumo":"which base?"`) {
		t.Errorf("dados lost event fields: %s", lastLine)
	}
	var r Record
	if err := json.Unmarshal([]byte(lastLine), &r); err != nil {
		t.Fatal(err)
	}
	if r.JobID == nil || *r.JobID != "TASK-9" {
		t.Errorf("job_id = %v, want TASK-9", r.JobID)
	}
}

// TestDedupeCrashBetweenAppendAndJobsUpdate: a crash right after the outbox
// append but before the jobs.json update leaves a stale jobs.json; a retry
// must find the record by its idempotency key and not duplicate it.
func TestDedupeCrashBetweenAppendAndJobsUpdate(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir)
	// First append succeeds and tracks the job at seq 3.
	for i := 1; i <= 3; i++ {
		if _, _, err := s.AppendJobEvent(testMaquina, testProjeto, "TASK-7", jobEvent(i)); err != nil {
			t.Fatalf("event %d: %v", i, err)
		}
	}
	// Simulate the crash for event 4: write the outbox line by hand (as the
	// append did) but leave jobs.json at last_event_seq=3.
	key := "TASK-7:4"
	line := `{"schema":1,"seq":4,"ts":"2026-01-02T03:04:05-03:00","tipo":"job_event","maquina":"` + testMaquina + `","projeto":"` + testProjeto + `","job_id":"TASK-7","idempotency_key":"` + key + `","dados":` + string(jobEvent(4)) + `}
`
	data, err := os.ReadFile(s.outboxPath())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(s.outboxPath(), append(data, []byte(line)...), 0o600); err != nil {
		t.Fatal(err)
	}
	jobs, err := s.LoadJobs()
	if err != nil {
		t.Fatal(err)
	}
	if got := jobs.Jobs["TASK-7"].LastEventSeq; got != 3 {
		t.Fatalf("precondition: jobs.json last_event_seq = %d, want 3", got)
	}
	// Retry after the "crash": no duplicate, and the cursor catches up.
	rec, appended, err := s.AppendJobEvent(testMaquina, testProjeto, "TASK-7", jobEvent(4))
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if appended {
		t.Fatal("retry appended a duplicate record")
	}
	if rec.Seq != 4 || rec.IdempotencyKey != key {
		t.Errorf("retry returned %+v, want the existing record", rec)
	}
	lines, last, err := s.Read(0)
	if err != nil || len(lines) != 4 || last != 4 {
		t.Fatalf("Read = %d lines, last %d, err %v; want 4 records", len(lines), last, err)
	}
	jobs, err = s.LoadJobs()
	if err != nil {
		t.Fatal(err)
	}
	if got := jobs.Jobs["TASK-7"].LastEventSeq; got != 4 {
		t.Errorf("jobs.json last_event_seq = %d after retry, want 4 (recovered)", got)
	}
}

// TestDedupeUnknownJobTracked: an event for a job unknown to jobs.json is
// tracked on the fly (interactive jobs not started through the bridge).
func TestDedupeUnknownJobTracked(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir)
	if _, _, err := s.AppendJobEvent(testMaquina, testProjeto, "TASK-88", jobEvent(1)); err != nil {
		t.Fatal(err)
	}
	jobs, err := s.LoadJobs()
	if err != nil {
		t.Fatal(err)
	}
	j := jobs.Jobs["TASK-88"]
	if j == nil {
		t.Fatal("unknown job was not tracked in jobs.json")
	}
	if j.LastEventSeq != 1 || j.Projeto != testProjeto || j.Closed {
		t.Fatalf("job = %+v", j)
	}
}

// TestDedupeNoSeqSkipped: a mix of appends, dedupe skips and job events
// keeps the outbox seq consecutive with no gaps.
func TestDedupeNoSeqSkipped(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir)
	if _, _, err := s.AppendJobEvent(testMaquina, testProjeto, "T-1", jobEvent(1)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.AppendJobEvent(testMaquina, testProjeto, "T-1", jobEvent(1)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append(Record{Tipo: TipoSession, Maquina: testMaquina, Projeto: testProjeto, Dados: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.AppendJobEvent(testMaquina, testProjeto, "T-1", jobEvent(2)); err != nil {
		t.Fatal(err)
	}
	lines, last, err := s.Read(0)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[int64]bool{}
	for _, l := range lines {
		var r Record
		if err := json.Unmarshal(l, &r); err != nil {
			t.Fatal(err)
		}
		if seen[r.Seq] {
			t.Fatalf("duplicate seq %d", r.Seq)
		}
		seen[r.Seq] = true
	}
	for seq := int64(1); seq <= int64(len(lines)); seq++ {
		if !seen[seq] {
			t.Fatalf("seq %d skipped (have %d records, last %d)", seq, len(lines), last)
		}
	}
	if len(lines) != 3 || last != 3 {
		t.Fatalf("got %d records (last %d), want 3 (skips must not consume a seq)", len(lines), last)
	}
}

// TestDedupeInvalidEventSeq: an event without a positive integer seq is an
// error and appends nothing.
func TestDedupeInvalidEventSeq(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir)
	for _, bad := range []json.RawMessage{
		[]byte(`{"seq":0,"tipo":"question"}`),
		[]byte(`{"seq":-3,"tipo":"question"}`),
		[]byte(`{"seq":"4","tipo":"question"}`),
		[]byte(`{}`),
		[]byte(`not json`),
	} {
		if _, appended, err := s.AppendJobEvent(testMaquina, testProjeto, "T-9", bad); err == nil || appended {
			t.Errorf("event %s: err=%v appended=%v, want error", bad, err, appended)
		}
	}
	if lines, _, err := s.Read(0); err != nil || len(lines) != 0 {
		t.Fatalf("Read = %d lines, err %v; want none", len(lines), err)
	}
	if _, err := os.Stat(filepath.Join(dir, "jobs.json")); !os.IsNotExist(err) {
		t.Errorf("jobs.json created by an invalid event: %v", err)
	}
}
