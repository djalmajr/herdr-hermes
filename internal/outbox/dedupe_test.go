package outbox

import (
	"bytes"
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

// oddEvent is one event with unusual spacing and a field order different
// from the compact canonical form; its bytes must round-trip unchanged
// into the stored record's dados.
func oddEvent(seq int) json.RawMessage {
	return json.RawMessage(`{"tipo":"question",  "seq":` + strconv.Itoa(seq) + ` , "resumo":"e ` + strconv.Itoa(seq) + `  spaced"}`)
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
	// A later event appends; the cursor is the contiguous stored prefix,
	// still 0 while events 1 to 4 are missing.
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
	if j == nil || j.LastEventSeq != 0 || j.Projeto != testProjeto {
		t.Fatalf("jobs.json = %+v, want last_event_seq 0 (the contiguous stored prefix)", j)
	}
	// An out-of-order older event is refused by its idempotency key.
	if _, appended, err := s.AppendJobEvent(testMaquina, testProjeto, "TASK-1", jobEvent(5)); err != nil || appended {
		t.Fatalf("old event 5: appended=%v err=%v", appended, err)
	}
	// Filling the gap makes the cursor catch up to the stored prefix.
	for i := 1; i <= 4; i++ {
		if _, appended, err := s.AppendJobEvent(testMaquina, testProjeto, "TASK-1", jobEvent(i)); err != nil || !appended {
			t.Fatalf("event %d: appended=%v err=%v", i, appended, err)
		}
	}
	jobs, err = s.LoadJobs()
	if err != nil {
		t.Fatal(err)
	}
	if j := jobs.Jobs["TASK-1"]; j == nil || j.LastEventSeq != 6 {
		t.Fatalf("jobs.json = %+v, want last_event_seq 6 once the gap is filled", j)
	}
	// A different job with the same event seq appends its own record.
	if _, appended, err := s.AppendJobEvent(testMaquina, testProjeto, "TASK-2", jobEvent(5)); err != nil || !appended {
		t.Fatalf("TASK-2 event 5: appended=%v err=%v", appended, err)
	}
	if lines, last, err := s.Read(0); err != nil || len(lines) != 7 || last != 7 {
		t.Fatalf("Read = %d lines, last %d, err %v; want 7 records", len(lines), last, err)
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

// TestDedupeMultilineEventOneLine: an event delivered as pretty-printed
// multi-line JSON with a trailing newline (as the wake hook passes raw
// stdin) must store one line in the outbox: its dados is the compact
// form, and a single-line event with a trailing newline keeps its bytes
// minus the newline.
func TestDedupeMultilineEventOneLine(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir)
	pretty := json.RawMessage("{\n  \"tipo\": \"question\",\n  \"seq\": 1,\n  \"resumo\": \"ship it?\"\n}\n")
	if _, appended, err := s.AppendJobEvent(testMaquina, testProjeto, "J4", pretty); err != nil || !appended {
		t.Fatalf("event 1: appended=%v err=%v, want true,nil", appended, err)
	}
	single := json.RawMessage(`{"seq":2,"tipo":"accepted","resumo":"go"}` + "\n")
	if _, appended, err := s.AppendJobEvent(testMaquina, testProjeto, "J4", single); err != nil || !appended {
		t.Fatalf("event 2: appended=%v err=%v, want true,nil", appended, err)
	}
	data, err := os.ReadFile(s.outboxPath())
	if err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(string(data), "\n"); n != 2 {
		t.Fatalf("outbox file has %d newlines, want exactly 2 (one per record): %q", n, data)
	}
	lines, last, err := s.Read(0)
	if err != nil || len(lines) != 2 || last != 2 {
		t.Fatalf("Read = %d lines, last %d, err %v; want 2 lines", len(lines), last, err)
	}
	var r1, r2 Record
	if err := json.Unmarshal(lines[0], &r1); err != nil {
		t.Fatalf("line 1 is not a record: %v", err)
	}
	if err := json.Unmarshal(lines[1], &r2); err != nil {
		t.Fatalf("line 2 is not a record: %v", err)
	}
	if !bytes.Equal(r1.Dados, []byte(`{"tipo":"question","seq":1,"resumo":"ship it?"}`)) {
		t.Fatalf("record 1 dados = %s, want the compact form", r1.Dados)
	}
	if !bytes.Equal(r2.Dados, []byte(`{"seq":2,"tipo":"accepted","resumo":"go"}`)) {
		t.Fatalf("record 2 dados = %s, want the event without the trailing newline", r2.Dados)
	}
}

// TestDedupeGapWakeThenLower: a wake for the higher seq stores the record
// but leaves last_event_seq at the contiguous stored prefix (0 when event
// 1 is absent); the lower events the sync then appends are not counted as
// duplicates, the retries converge, and the outbox holds exactly one
// record per key with the original event bytes in dados.
func TestDedupeGapWakeThenLower(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir)
	// Event 3 arrives first (the wake hook fired before sync stored 1 and 2).
	ev3 := oddEvent(3)
	rec3, appended, err := s.AppendJobEvent(testMaquina, testProjeto, "J1", ev3)
	if err != nil || !appended {
		t.Fatalf("event 3: appended=%v err=%v, want true,nil", appended, err)
	}
	if rec3.IdempotencyKey != "J1:3" || rec3.Seq != 1 {
		t.Fatalf("event 3 = %+v, want key J1:3, outbox seq 1", rec3)
	}
	if !bytes.Equal(rec3.Dados, ev3) {
		t.Fatalf("event 3 dados = %s, want the original bytes %s", rec3.Dados, ev3)
	}
	jobs, err := s.LoadJobs()
	if err != nil {
		t.Fatal(err)
	}
	if j := jobs.Jobs["J1"]; j == nil || j.LastEventSeq != 0 {
		t.Fatalf("after event 3: job = %+v, want last_event_seq 0 (event 1 absent)", j)
	}
	// The sync fetches 1 and 2: neither is a duplicate.
	ev1, ev2 := oddEvent(1), oddEvent(2)
	for _, ev := range []json.RawMessage{ev1, ev2} {
		rec, appended, err := s.AppendJobEvent(testMaquina, testProjeto, "J1", ev)
		if err != nil || !appended {
			t.Fatalf("lower event %s: appended=%v err=%v, want true,nil", ev, appended, err)
		}
		if !bytes.Equal(rec.Dados, ev) {
			t.Fatalf("event %s dados = %s, want the original bytes", ev, rec.Dados)
		}
	}
	jobs, err = s.LoadJobs()
	if err != nil {
		t.Fatal(err)
	}
	if j := jobs.Jobs["J1"]; j == nil || j.LastEventSeq != 3 {
		t.Fatalf("after events 1 and 2: job = %+v, want last_event_seq 3", j)
	}
	// The retries converge: none appends, and the outbox holds one record
	// per key with the original bytes, in order 3, 1, 2.
	for _, ev := range []json.RawMessage{ev1, ev2, oddEvent(3)} {
		if _, appended, err := s.AppendJobEvent(testMaquina, testProjeto, "J1", ev); err != nil || appended {
			t.Fatalf("retry %s: appended=%v err=%v, want false,nil", ev, appended, err)
		}
	}
	lines, last, err := s.Read(0)
	if err != nil || len(lines) != 3 || last != 3 {
		t.Fatalf("Read = %d lines, last %d, err %v; want 3 records", len(lines), last, err)
	}
	wantKeys := []string{"J1:3", "J1:1", "J1:2"}
	wantDados := map[string]json.RawMessage{"J1:3": ev3, "J1:1": ev1, "J1:2": ev2}
	counts := map[string]int{}
	for i, l := range lines {
		var r Record
		if err := json.Unmarshal(l, &r); err != nil {
			t.Fatalf("line %d: %v", i+1, err)
		}
		if r.IdempotencyKey != wantKeys[i] {
			t.Fatalf("record %d key = %q, want %q (outbox order 3, 1, 2)", i+1, r.IdempotencyKey, wantKeys[i])
		}
		if r.Seq != int64(i+1) {
			t.Fatalf("record %d outbox seq = %d, want %d", i+1, r.Seq, i+1)
		}
		if !bytes.Equal(r.Dados, wantDados[r.IdempotencyKey]) {
			t.Fatalf("record %d dados = %s, want the original bytes %s", i+1, r.Dados, wantDados[r.IdempotencyKey])
		}
		counts[r.IdempotencyKey]++
	}
	for _, k := range wantKeys {
		if counts[k] != 1 {
			t.Fatalf("key %s stored %d times, want exactly 1", k, counts[k])
		}
	}
}

// TestDedupeStaleCursorNeverSkips: a last_event_seq higher than anything
// stored must never make a missing record count as a duplicate; the cursor
// is the contiguous stored prefix, so it is lowered to 0 while event 1 is
// absent and catches up when the gap fills.
func TestDedupeStaleCursorNeverSkips(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir)
	if err := s.UpdateJobs(func(js *Jobs) error {
		js.Jobs["J2"] = &Job{ID: "J2", Projeto: testProjeto, LastEventSeq: 5}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// jobs.json says 5 but the outbox has no record for the job: event 2
	// is stored, and the cursor is the contiguous stored prefix (0).
	ev := oddEvent(2)
	rec, appended, err := s.AppendJobEvent(testMaquina, testProjeto, "J2", ev)
	if err != nil || !appended {
		t.Fatalf("event 2: appended=%v err=%v, want true,nil (a stale cursor never skips)", appended, err)
	}
	if rec.IdempotencyKey != "J2:2" {
		t.Fatalf("event 2 key = %q, want J2:2", rec.IdempotencyKey)
	}
	if !bytes.Equal(rec.Dados, ev) {
		t.Fatalf("event 2 dados = %s, want the original bytes", rec.Dados)
	}
	jobs, err := s.LoadJobs()
	if err != nil {
		t.Fatal(err)
	}
	if j := jobs.Jobs["J2"]; j == nil || j.LastEventSeq != 0 {
		t.Fatalf("after event 2: job = %+v, want last_event_seq 0 (event 1 missing)", j)
	}
	// Filling the gap: event 1 stores and the cursor is 2.
	if _, appended, err := s.AppendJobEvent(testMaquina, testProjeto, "J2", oddEvent(1)); err != nil || !appended {
		t.Fatalf("event 1: appended=%v err=%v, want true,nil", appended, err)
	}
	jobs, err = s.LoadJobs()
	if err != nil {
		t.Fatal(err)
	}
	if j := jobs.Jobs["J2"]; j == nil || j.LastEventSeq != 2 {
		t.Fatalf("after event 1: job = %+v, want last_event_seq 2", j)
	}
	if lines, _, err := s.Read(0); err != nil || len(lines) != 2 {
		t.Fatalf("Read = %d lines, err %v; want 2 records", len(lines), err)
	}
}

// TestDedupeCrashHealsPrefix: a crash between the outbox append and the
// jobs.json write leaves records without bookkeeping; the next call for
// any seq of the job recomputes the contiguous stored prefix and writes
// it, and retries return the existing record without duplicating.
func TestDedupeCrashHealsPrefix(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir)
	// Records 1 and 2 are durable; then simulate the crash by rewriting
	// jobs.json back to 0 (the write after the appends never happened).
	for i := 1; i <= 2; i++ {
		if _, appended, err := s.AppendJobEvent(testMaquina, testProjeto, "J3", oddEvent(i)); err != nil || !appended {
			t.Fatalf("event %d: appended=%v err=%v, want true,nil", i, appended, err)
		}
	}
	jobsData, err := json.Marshal(&Jobs{Jobs: map[string]*Job{"J3": {ID: "J3", Projeto: testProjeto}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteFileAtomic(s.jobsPath(), jobsData); err != nil {
		t.Fatal(err)
	}
	// The retry for event 2 finds the existing record and heals the cursor
	// to the contiguous stored prefix (2).
	rec, appended, err := s.AppendJobEvent(testMaquina, testProjeto, "J3", oddEvent(2))
	if err != nil || appended {
		t.Fatalf("event 2 retry: appended=%v err=%v, want false,nil", appended, err)
	}
	if rec.IdempotencyKey != "J3:2" || rec.Seq != 2 {
		t.Fatalf("retry = %+v, want the existing record", rec)
	}
	jobs, err := s.LoadJobs()
	if err != nil {
		t.Fatal(err)
	}
	if j := jobs.Jobs["J3"]; j == nil || j.LastEventSeq != 2 {
		t.Fatalf("after the retry: job = %+v, want last_event_seq 2 (healed)", j)
	}
	// Event 3 stores and the cursor is 3.
	if _, appended, err := s.AppendJobEvent(testMaquina, testProjeto, "J3", oddEvent(3)); err != nil || !appended {
		t.Fatalf("event 3: appended=%v err=%v, want true,nil", appended, err)
	}
	jobs, err = s.LoadJobs()
	if err != nil {
		t.Fatal(err)
	}
	if j := jobs.Jobs["J3"]; j == nil || j.LastEventSeq != 3 {
		t.Fatalf("after event 3: job = %+v, want last_event_seq 3", j)
	}
	// A gap opened by a wake of 5 must not count as stored: the cursor
	// stays at the contiguous stored prefix (3) while 4 is missing, and a
	// retry of 5 heals it back to 3 instead of the event's own seq.
	if _, appended, err := s.AppendJobEvent(testMaquina, testProjeto, "J3", oddEvent(5)); err != nil || !appended {
		t.Fatalf("event 5: appended=%v err=%v, want true,nil", appended, err)
	}
	if _, appended, err := s.AppendJobEvent(testMaquina, testProjeto, "J3", oddEvent(5)); err != nil || appended {
		t.Fatalf("event 5 retry: appended=%v err=%v, want false,nil", appended, err)
	}
	jobs, err = s.LoadJobs()
	if err != nil {
		t.Fatal(err)
	}
	if j := jobs.Jobs["J3"]; j == nil || j.LastEventSeq != 3 {
		t.Fatalf("after the event 5 retry: job = %+v, want last_event_seq 3 (event 4 missing)", j)
	}
	if lines, _, err := s.Read(0); err != nil || len(lines) != 4 {
		t.Fatalf("Read = %d lines, err %v; want 4 records", len(lines), err)
	}
}

// TestOutboxJobEventCursor: JobEventCursor reports, per job, the
// contiguous stored prefix and the highest stored event seq without
// writing: an empty outbox, a contiguous run, a holed run, a foreign job
// and non job_event records.
func TestOutboxJobEventCursor(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir)
	for _, id := range []string{"J1", "J2"} {
		prefix, maxStored, err := s.JobEventCursor(id)
		if err != nil || prefix != 0 || maxStored != 0 {
			t.Fatalf("%s on an empty outbox: prefix=%d max=%d err=%v, want 0,0,nil", id, prefix, maxStored, err)
		}
	}
	// A dispatch record of J1 is not a job event.
	id := "J1"
	if _, err := s.Append(Record{Tipo: TipoDispatch, Maquina: testMaquina, Projeto: testProjeto, JobID: &id, Dados: json.RawMessage(`{"id":"J1"}`)}); err != nil {
		t.Fatal(err)
	}
	// J1 stores 1, 2 and 4 (a hole at 3); J2 stores 1 and 2.
	for _, seq := range []int{1, 2, 4} {
		if _, _, err := s.AppendJobEvent(testMaquina, testProjeto, "J1", jobEvent(seq)); err != nil {
			t.Fatal(err)
		}
	}
	for _, seq := range []int{1, 2} {
		if _, _, err := s.AppendJobEvent(testMaquina, testProjeto, "J2", jobEvent(seq)); err != nil {
			t.Fatal(err)
		}
	}
	prefix, maxStored, err := s.JobEventCursor("J1")
	if err != nil || prefix != 2 || maxStored != 4 {
		t.Fatalf("J1: prefix=%d max=%d err=%v, want 2,4,nil (the hole at 3 stops the prefix)", prefix, maxStored, err)
	}
	prefix, maxStored, err = s.JobEventCursor("J2")
	if err != nil || prefix != 2 || maxStored != 2 {
		t.Fatalf("J2: prefix=%d max=%d err=%v, want 2,2,nil (contiguous)", prefix, maxStored, err)
	}
	// A read-only store answers without taking the lock, and the cursor
	// stays read-only: it wrote nothing.
	before := stateDirFiles(t, dir)
	ro, err := Open(dir, Options{Now: fixedNow, ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	prefix, maxStored, err = ro.JobEventCursor("J1")
	if err != nil || prefix != 2 || maxStored != 4 {
		t.Fatalf("read-only J1: prefix=%d max=%d err=%v, want 2,4,nil", prefix, maxStored, err)
	}
	after := stateDirFiles(t, dir)
	if len(before) != len(after) {
		t.Fatalf("JobEventCursor left %d files, want %d (it wrote nothing)", len(after), len(before))
	}
	for name, b := range before {
		if !bytes.Equal(after[name], b) {
			t.Fatalf("JobEventCursor changed %s (it wrote nothing)", name)
		}
	}
}

// stateDirFiles snapshots the state dir's files: name -> bytes.
func stateDirFiles(t *testing.T, dir string) map[string][]byte {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string][]byte{}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		out[e.Name()] = b
	}
	return out
}
