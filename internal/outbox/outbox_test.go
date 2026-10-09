package outbox

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

const testMaquina = "machine-a"
const testProjeto = "org/repo"

func openStore(t *testing.T, dir string) *Store {
	t.Helper()
	s, err := Open(dir, Options{Now: fixedNow})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return s
}

func fixedNow() time.Time {
	return time.Date(2026, 1, 2, 3, 4, 5, 0, time.FixedZone("", -3*3600))
}

func appendN(t *testing.T, s *Store, n int, tipo string) []Record {
	t.Helper()
	var out []Record
	for i := 0; i < n; i++ {
		rec, err := s.Append(Record{Tipo: tipo, Maquina: testMaquina, Projeto: testProjeto, Dados: json.RawMessage(`{"n":` + strconv.Itoa(i) + `}`)})
		if err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
		out = append(out, rec)
	}
	return out
}

// TestOutboxSeqConsecutiveGoroutines: several goroutines append concurrently
// and the seqs come out consecutive with no duplicates or gaps.
func TestOutboxSeqConsecutiveGoroutines(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir)
	const workers = 8
	const perWorker = 5
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := appendNS(t, s, perWorker); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	lines, last, err := s.Read(0)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if want := workers * perWorker; len(lines) != want {
		t.Fatalf("Read returned %d lines, want %d", len(lines), want)
	}
	seen := map[int64]bool{}
	for _, l := range lines {
		var r Record
		if err := json.Unmarshal(l, &r); err != nil {
			t.Fatalf("line is not a record: %v", err)
		}
		if seen[r.Seq] {
			t.Fatalf("duplicate seq %d", r.Seq)
		}
		seen[r.Seq] = true
	}
	for seq := int64(1); seq <= int64(workers*perWorker); seq++ {
		if !seen[seq] {
			t.Fatalf("seq %d missing", seq)
		}
	}
	if last != int64(workers*perWorker) {
		t.Errorf("last = %d, want %d", last, workers*perWorker)
	}
}

// appendNS is the same as appendN but returns its errors instead of
// fatalling (for use inside goroutines).
func appendNS(t *testing.T, s *Store, n int) ([]Record, error) {
	var out []Record
	for i := 0; i < n; i++ {
		rec, err := s.Append(Record{Tipo: TipoDispatch, Maquina: testMaquina, Projeto: testProjeto, Dados: json.RawMessage(`{"n":` + strconv.Itoa(i) + `}`)})
		if err != nil {
			return out, err
		}
		out = append(out, rec)
	}
	return out, nil
}

// TestOutboxHelperProcess is the child of the two-process test. Re-executed
// with "-- <dir> <n> <startfile>": it writes a ready marker, blocks until
// the parent creates <startfile> (so both children are inside the lock
// region before either appends), then appends n records and stops.
func TestOutboxHelperProcess(t *testing.T) {
	dir, n, startFile, ok := helperArgs()
	if !ok {
		t.Skip("not a helper process invocation")
	}
	s, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("child Open: %v", err)
	}
	ready := startFile + ".ready-" + strconv.Itoa(os.Getpid())
	if err := os.WriteFile(ready, []byte("ready"), 0o644); err != nil {
		t.Fatalf("child ready marker: %v", err)
	}
	for {
		if _, err := os.Stat(startFile); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	for i := 0; i < n; i++ {
		if _, err := s.Append(Record{Tipo: TipoDispatch, Maquina: testMaquina, Projeto: testProjeto, Dados: json.RawMessage(`{"child":true}`)}); err != nil {
			t.Fatalf("child Append: %v", err)
		}
	}
}

// helperArgs extracts the arguments after the "--" separator, if any.
func helperArgs() (dir string, n int, startFile string, ok bool) {
	for i, a := range os.Args {
		if a == "--" {
			if len(os.Args) != i+4 {
				return "", 0, "", false
			}
			n, err := strconv.Atoi(os.Args[i+2])
			if err != nil {
				return "", 0, "", false
			}
			return os.Args[i+1], n, os.Args[i+3], true
		}
	}
	return "", 0, "", false
}

// TestOutboxSeqConsecutiveProcesses: two OS processes that are both blocked
// inside the lock region at the same time append through the lock file and
// the seqs come out consecutive with no gap or duplicate.
func TestOutboxSeqConsecutiveProcesses(t *testing.T) {
	if testing.Short() {
		t.Skip("re-executes the test binary")
	}
	dir := t.TempDir()
	const perProcess = 25
	startFile := filepath.Join(dir, "start")
	cmds := make([]*exec.Cmd, 2)
	outBufs := make([]*bytes.Buffer, 2)
	for p := 0; p < 2; p++ {
		outBufs[p] = &bytes.Buffer{}
		cmds[p] = exec.Command(os.Args[0],
			"-test.run=TestOutboxHelperProcess",
			"--", dir, strconv.Itoa(perProcess), startFile)
		cmds[p].Stdout = outBufs[p]
		cmds[p].Stderr = outBufs[p]
		if err := cmds[p].Start(); err != nil {
			t.Fatalf("start process %d: %v", p, err)
		}
	}
	// Both children write a ready marker before they start polling the
	// start file; wait until both are inside the block so the append
	// windows genuinely overlap.
	bothReady := func() bool {
		for p := 0; p < 2; p++ {
			if _, err := os.Stat(startFile + ".ready-" + strconv.Itoa(cmds[p].Process.Pid)); err != nil {
				return false
			}
		}
		return true
	}
	deadline := time.Now().Add(15 * time.Second)
	for !bothReady() {
		if time.Now().After(deadline) {
			t.Fatalf("children did not reach the start-file block:\nchild0: %s\nchild1: %s", outBufs[0].String(), outBufs[1].String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	// Release both at once.
	if err := os.WriteFile(startFile, []byte("go"), 0o644); err != nil {
		t.Fatal(err)
	}
	for p := 0; p < 2; p++ {
		if err := cmds[p].Wait(); err != nil {
			t.Fatalf("process %d: %v\noutput: %s", p, err, outBufs[p].String())
		}
	}
	// Both processes are done: the parent appends one more record.
	s := openStore(t, dir)
	rec, err := s.Append(Record{Tipo: TipoDispatch, Maquina: testMaquina, Projeto: testProjeto, Dados: json.RawMessage(`{"parent":true}`)})
	if err != nil {
		t.Fatalf("parent Append: %v", err)
	}
	lines, last, err := s.Read(0)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if want := 2*perProcess + 1; len(lines) != want {
		t.Fatalf("Read returned %d lines, want %d", len(lines), want)
	}
	seen := map[int64]bool{}
	for _, l := range lines {
		var r Record
		if err := json.Unmarshal(l, &r); err != nil {
			t.Fatalf("line is not a record: %v", err)
		}
		seen[r.Seq] = true
	}
	for seq := int64(1); seq <= int64(2*perProcess+1); seq++ {
		if !seen[seq] {
			t.Fatalf("seq %d missing (skipped or duplicated under the lock)", seq)
		}
	}
	if rec.Seq != int64(2*perProcess+1) || last != int64(2*perProcess+1) {
		t.Errorf("parent seq/last = %d/%d, want %d", rec.Seq, last, 2*perProcess+1)
	}
}

// installSyncCounter replaces the store's fsync with a counting wrapper
// that still performs the real sync, and returns the counter.
func installSyncCounter(s *Store) *int {
	n := 0
	s.syncFile = func(f *os.File) error {
		n++
		return f.Sync()
	}
	return &n
}

// TestOutboxFsyncBeforeReturn proves the append path syncs the outbox file
// before returning (counting hook) and that right after Append returns the
// line is visible through a fresh file handle.
func TestOutboxFsyncBeforeReturn(t *testing.T) {
	s := openStore(t, t.TempDir())
	cnt := installSyncCounter(s)
	rec, err := s.Append(Record{Tipo: TipoDispatch, Maquina: testMaquina, Projeto: testProjeto, Dados: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if *cnt < 1 {
		t.Fatalf("append path did not sync the outbox file before returning (syncs seen: %d)", *cnt)
	}
	f, err := os.Open(s.outboxPath())
	if err != nil {
		t.Fatalf("reopen outbox: %v", err)
	}
	defer f.Close()
	data, err := io.ReadAll(f)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	lines := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
	if len(lines) != 1 {
		t.Fatalf("file has %d lines, want 1", len(lines))
	}
	var r Record
	if err := json.Unmarshal([]byte(lines[0]), &r); err != nil {
		t.Fatalf("line: %v", err)
	}
	if r.Seq != rec.Seq || r.Seq != 1 {
		t.Errorf("seq = %d, want 1", r.Seq)
	}
}

// TestOutboxWriteFileAtomicSyncs proves WriteFileAtomic syncs the temp file
// before the rename, via the counting hook (exported signature unchanged).
func TestOutboxWriteFileAtomicSyncs(t *testing.T) {
	n := 0
	old := atomicWriteSync
	atomicWriteSync = func(f *os.File) error {
		n++
		return f.Sync()
	}
	t.Cleanup(func() { atomicWriteSync = old })
	path := filepath.Join(t.TempDir(), "f.json")
	if err := WriteFileAtomic(path, []byte(`{"a":1}`)); err != nil {
		t.Fatalf("WriteFileAtomic: %v", err)
	}
	if n < 1 {
		t.Fatalf("WriteFileAtomic did not sync the temp file before rename (syncs seen: %d)", n)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if string(data) != `{"a":1}` {
		t.Errorf("content = %s", data)
	}
}

// TestOutboxLastPush: last_push lives in cursor.json next to delivered_seq
// and updates to one preserve the other.
func TestOutboxLastPush(t *testing.T) {
	s := openStore(t, t.TempDir())
	lp, err := s.LastPush()
	if err != nil || lp != nil {
		t.Fatalf("LastPush on empty store = %+v, %v; want nil, nil", lp, err)
	}
	if err := s.SetLastPush(PushResult{Status: "ok", Code: 200}); err != nil {
		t.Fatalf("SetLastPush: %v", err)
	}
	lp, err = s.LastPush()
	if err != nil || lp == nil {
		t.Fatalf("LastPush = %+v, %v; want the recorded result", lp, err)
	}
	if lp.Status != "ok" || lp.Code != 200 || lp.TS == "" {
		t.Errorf("LastPush = %+v; want status ok, code 200, ts filled", lp)
	}
	if want := s.now().Format(TSLayout); lp.TS != want {
		t.Errorf("TS = %q; want %q", lp.TS, want)
	}
	if d, err := s.DeliveredSeq(); err != nil || d != 0 {
		t.Errorf("DeliveredSeq = %d, %v; want 0", d, err)
	}
	// Moving the delivered cursor must preserve last_push.
	if err := s.SetDeliveredSeq(7); err != nil {
		t.Fatalf("SetDeliveredSeq: %v", err)
	}
	lp2, err := s.LastPush()
	if err != nil || lp2 == nil || *lp2 != *lp {
		t.Fatalf("LastPush after SetDeliveredSeq = %+v, %v; want the same result", lp2, err)
	}
	// Recording a new push result must preserve delivered_seq.
	if err := s.SetLastPush(PushResult{TS: "2026-10-08T12:00:00-03:00", Status: "http_503", Code: 503}); err != nil {
		t.Fatalf("SetLastPush (2): %v", err)
	}
	if d, err := s.DeliveredSeq(); err != nil || d != 7 {
		t.Errorf("DeliveredSeq after second SetLastPush = %d, %v; want 7", d, err)
	}
	lp3, err := s.LastPush()
	if err != nil || lp3 == nil || lp3.Status != "http_503" || lp3.Code != 503 || lp3.TS != "2026-10-08T12:00:00-03:00" {
		t.Fatalf("LastPush (2) = %+v, %v; want the http_503 result", lp3, err)
	}
	// File shape: both keys, no Z timestamp.
	data, err := os.ReadFile(filepath.Join(s.dir, "cursor.json"))
	if err != nil {
		t.Fatalf("cursor.json: %v", err)
	}
	if !bytes.Contains(data, []byte(`"delivered_seq":7`)) || !bytes.Contains(data, []byte(`"last_push"`)) {
		t.Errorf("cursor.json = %s; want both keys", data)
	}
	if bytes.Contains(data, []byte(`Z"`)) {
		t.Errorf("cursor.json has a UTC Z timestamp: %s", data)
	}
}

// TestOutboxReadSince: --since filtering, and since beyond last returns
// nothing while still reporting the last seq.
func TestOutboxReadSince(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir)
	appendN(t, s, 3, TipoDispatch)

	lines, last, err := s.Read(0)
	if err != nil || len(lines) != 3 || last != 3 {
		t.Fatalf("Read(0) = %d lines, last %d, err %v", len(lines), last, err)
	}
	lines, last, err = s.Read(1)
	if err != nil || len(lines) != 2 || last != 3 {
		t.Fatalf("Read(1) = %d lines, last %d, err %v", len(lines), last, err)
	}
	var r Record
	if err := json.Unmarshal(lines[0], &r); err != nil {
		t.Fatalf("line: %v", err)
	}
	if r.Seq != 2 {
		t.Errorf("first line seq = %d, want 2", r.Seq)
	}
	lines, last, err = s.Read(3)
	if err != nil || len(lines) != 0 || last != 3 {
		t.Fatalf("Read(3) = %d lines, last %d, err %v", len(lines), last, err)
	}
	lines, last, err = s.Read(100)
	if err != nil || len(lines) != 0 || last != 3 {
		t.Fatalf("Read(100) = %d lines, last %d, err %v", len(lines), last, err)
	}
	// Absent file: empty, no error.
	empty, err := Open(t.TempDir(), Options{ReadOnly: true})
	if err != nil {
		t.Fatalf("Open absent: %v", err)
	}
	if lines, last, err := empty.Read(0); err != nil || len(lines) != 0 || last != 0 {
		t.Fatalf("Read absent = %v, %d, %v", lines, last, err)
	}
}

// fakeClock is the injected clock: Sleep records the requested duration and
// advances (or drifts backwards by drift) the visible now.
type fakeClock struct {
	mu    sync.Mutex
	now   time.Time
	slept time.Duration
	drift time.Duration
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC)}
}

func (f *fakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *fakeClock) Sleep(ctx context.Context, d time.Duration) error {
	f.mu.Lock()
	f.slept += d
	f.now = f.now.Add(d + f.drift)
	f.mu.Unlock()
	// Short real delay so concurrent writers can interleave.
	real := 20 * time.Millisecond
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(real):
		return nil
	}
}

func (f *fakeClock) totalSleep() time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.slept
}

// TestOutboxWaitNewRecord: the long-poll returns as soon as a new record
// lands.
func TestOutboxWaitNewRecord(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir)
	appendN(t, s, 2, TipoDispatch)
	go func() {
		time.Sleep(60 * time.Millisecond)
		if _, err := appendNS(t, s, 1); err != nil {
			t.Errorf("append from goroutine: %v", err)
		}
	}()
	clock := newFakeClock()
	found, last, err := s.Wait(context.Background(), 2, 5*time.Second, clock)
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if !found {
		t.Fatal("Wait timed out, want a new record")
	}
	if last != 3 {
		t.Errorf("last = %d, want 3", last)
	}
}

// TestOutboxWaitTimeout: with nothing new, the long-poll returns after the
// bound and the total sleep never exceeds it.
func TestOutboxWaitTimeout(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir)
	appendN(t, s, 1, TipoDispatch)
	clock := newFakeClock()
	found, last, err := s.Wait(context.Background(), 1, 600*time.Millisecond, clock)
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if found {
		t.Fatal("Wait returned a record, want timeout")
	}
	if last != 1 {
		t.Errorf("last = %d, want 1", last)
	}
	if slept := clock.totalSleep(); slept > 600*time.Millisecond || slept < 400*time.Millisecond {
		t.Errorf("total sleep = %v, want within (400ms, 600ms]", slept)
	}
	// --wait 0 never sleeps.
	clock = newFakeClock()
	found, _, err = s.Wait(context.Background(), 1, 0, clock)
	if err != nil || found || clock.totalSleep() != 0 {
		t.Errorf("Wait(max=0) = %v, %v, slept %v", found, err, clock.totalSleep())
	}
}

// TestOutboxWaitBackwardsClock: a clock that moves backwards on every sleep
// never ends the wait early and never lets it exceed the bound; the bound is
// the sum of the slept durations, not now - start.
func TestOutboxWaitBackwardsClock(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir)
	appendN(t, s, 1, TipoDispatch)
	clock := newFakeClock()
	clock.drift = -time.Hour // the visible clock jumps backwards after each sleep
	startVisible := clock.Now()
	found, _, err := s.Wait(context.Background(), 1, 600*time.Millisecond, clock)
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if found {
		t.Fatal("Wait returned a record, want timeout")
	}
	if slept := clock.totalSleep(); slept > 600*time.Millisecond || slept < 400*time.Millisecond {
		t.Errorf("total sleep = %v, want within (400ms, 600ms]", slept)
	}
	if !clock.Now().Before(startVisible) {
		t.Error("clock did not go backwards (test misconfiguration)")
	}
}

// TestOutboxWaitContextCancel: a cancelled context ends the wait.
func TestOutboxWaitContextCancel(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	_, _, err := s.Wait(ctx, 0, time.Minute, newFakeClock())
	if err == nil {
		t.Fatal("Wait returned without error after context cancel")
	}
}

// TestOutboxTimestampNoZ: ts has an explicit offset and never a trailing Z,
// including when the zone is UTC.
func TestOutboxTimestampNoZ(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir)
	rec, err := s.Append(Record{Tipo: TipoDispatch, Maquina: testMaquina, Projeto: testProjeto, Dados: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if rec.TS != "2026-01-02T03:04:05-03:00" {
		t.Errorf("ts = %q, want 2026-01-02T03:04:05-03:00", rec.TS)
	}
	if strings.Contains(rec.TS, "Z") {
		t.Errorf("ts %q contains Z", rec.TS)
	}
	// UTC zone: the explicit-offset layout prints +00:00, never Z.
	utc, err := Open(t.TempDir(), Options{Now: func() time.Time {
		return time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC)
	}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	rec, err = utc.Append(Record{Tipo: TipoDispatch, Maquina: testMaquina, Projeto: testProjeto, Dados: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatalf("Append utc: %v", err)
	}
	if rec.TS != "2026-01-02T12:00:00+00:00" {
		t.Errorf("utc ts = %q, want 2026-01-02T12:00:00+00:00", rec.TS)
	}
	if strings.Contains(rec.TS, "Z") {
		t.Errorf("utc ts %q contains Z", rec.TS)
	}
}

// TestOutboxFileModes: state dir 0700, data files 0600 (POSIX only).
func TestOutboxFileModes(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mode asserts are POSIX-only")
	}
	dir := t.TempDir()
	s := openStore(t, dir)
	appendN(t, s, 1, TipoDispatch)
	if err := s.SetDeliveredSeq(1); err != nil {
		t.Fatalf("SetDeliveredSeq: %v", err)
	}
	if err := s.UpdateJobs(func(j *Jobs) error {
		j.Jobs["TASK-1"] = &Job{ID: "TASK-1", Projeto: testProjeto}
		return nil
	}); err != nil {
		t.Fatalf("UpdateJobs: %v", err)
	}
	if err := s.UpdateSessions(func(ss *Sessions) error {
		ss.Sessions["org/repo\tmain"] = &Session{Projeto: testProjeto, Branch: "main"}
		return nil
	}); err != nil {
		t.Fatalf("UpdateSessions: %v", err)
	}
	mode := func(p string) os.FileMode {
		fi, err := os.Stat(p)
		if err != nil {
			t.Fatalf("stat %s: %v", p, err)
		}
		return fi.Mode().Perm()
	}
	if got := mode(dir); got != 0o700 {
		t.Errorf("state dir mode = %o, want 700", got)
	}
	for _, f := range []string{"outbox.jsonl", "cursor.json", "jobs.json", "sessions.json"} {
		if got := mode(filepath.Join(dir, f)); got != 0o600 {
			t.Errorf("%s mode = %o, want 600", f, got)
		}
	}
}

// TestOutboxTornLineRepaired: a torn final line (a crash mid-write) is
// ignored by Read and truncated before the next append, which continues the
// seq from the last complete line.
func TestOutboxTornLineRepaired(t *testing.T) {
	dir := t.TempDir()
	complete := `{"schema":1,"seq":1,"ts":"2026-01-02T03:04:05-03:00","tipo":"dispatch","maquina":"machine-a","projeto":"org/repo","job_id":null,"idempotency_key":"machine-a:1","dados":{}}
`
	torn := `{"schema":1,"seq":2,"ts":"2026-01-02T03:04:05-03:00","tipo":"d`
	if err := os.WriteFile(filepath.Join(dir, "outbox.jsonl"), []byte(complete+torn), 0o600); err != nil {
		t.Fatal(err)
	}
	// Read ignores the torn line.
	s := openStore(t, dir)
	lines, last, err := s.Read(0)
	if err != nil || len(lines) != 1 || last != 1 {
		t.Fatalf("Read before repair = %d lines, last %d, err %v", len(lines), last, err)
	}
	// The next append repairs the file and continues the seq.
	rec, err := s.Append(Record{Tipo: TipoDispatch, Maquina: testMaquina, Projeto: testProjeto, Dados: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if rec.Seq != 2 {
		t.Errorf("seq = %d, want 2", rec.Seq)
	}
	data, err := os.ReadFile(filepath.Join(dir, "outbox.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	fileLines := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
	if len(fileLines) != 2 {
		t.Fatalf("file has %d lines after repair, want 2 (the torn line must be truncated):\n%s", len(fileLines), data)
	}
	for i, line := range fileLines {
		var r Record
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			t.Fatalf("line %d is not a complete record after repair: %v", i+1, err)
		}
		if i == 0 && r.Seq != 1 || i == 1 && r.Seq != 2 {
			t.Fatalf("line %d seq = %d, want %d", i+1, r.Seq, i+1)
		}
	}
	if !bytes.HasSuffix(data, []byte("\n")) {
		t.Errorf("file does not end with a newline after repair")
	}
	if lines, last, err := s.Read(0); err != nil || len(lines) != 2 || last != 2 {
		t.Fatalf("Read after repair = %d lines, last %d, err %v", len(lines), last, err)
	}
	// A file whose only content is torn: it is truncated to nothing.
	dir2 := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir2, "outbox.jsonl"), []byte(`{"schema":1,"seq":1`), 0o600); err != nil {
		t.Fatal(err)
	}
	s2 := openStore(t, dir2)
	rec, err = s2.Append(Record{Tipo: TipoDispatch, Maquina: testMaquina, Projeto: testProjeto, Dados: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatalf("Append on all-torn file: %v", err)
	}
	if rec.Seq != 1 {
		t.Errorf("seq = %d, want 1", rec.Seq)
	}
}

// TestOutboxReadOnlyNoFiles: a read-only store never creates the state
// directory or any file.
func TestOutboxReadOnlyNoFiles(t *testing.T) {
	cfg := t.TempDir()
	state := filepath.Join(cfg, "state")
	s, err := Open(state, Options{ReadOnly: true})
	if err != nil {
		t.Fatalf("Open read-only: %v", err)
	}
	if lines, last, err := s.Read(0); err != nil || len(lines) != 0 || last != 0 {
		t.Fatalf("Read = %v, %d, %v", lines, last, err)
	}
	if seq, err := s.DeliveredSeq(); err != nil || seq != 0 {
		t.Fatalf("DeliveredSeq = %d, %v", seq, err)
	}
	if _, err := os.Stat(state); !os.IsNotExist(err) {
		t.Fatalf("state dir was created: %v", err)
	}
	if entries, err := os.ReadDir(cfg); err != nil || len(entries) != 0 {
		t.Fatalf("config dir not empty: %v, %v", entries, err)
	}
}

// TestOutboxCursor: cursor.json is written atomically, is monotonic, and a
// missing cursor reads as 0.
func TestOutboxCursor(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir)
	if seq, err := s.DeliveredSeq(); err != nil || seq != 0 {
		t.Fatalf("DeliveredSeq absent = %d, %v", seq, err)
	}
	if err := s.SetDeliveredSeq(5); err != nil {
		t.Fatalf("SetDeliveredSeq(5): %v", err)
	}
	if seq, err := s.DeliveredSeq(); err != nil || seq != 5 {
		t.Fatalf("DeliveredSeq = %d, %v", seq, err)
	}
	data, err := os.ReadFile(s.cursorPath())
	if err != nil {
		t.Fatal(err)
	}
	var c struct {
		DeliveredSeq int64 `json:"delivered_seq"`
	}
	if err := json.Unmarshal(data, &c); err != nil {
		t.Fatalf("cursor.json: %v", err)
	}
	if c.DeliveredSeq != 5 {
		t.Errorf("cursor.json = %v", c)
	}
	// Never moves backwards.
	if err := s.SetDeliveredSeq(3); err != nil {
		t.Fatalf("SetDeliveredSeq(3): %v", err)
	}
	if seq, err := s.DeliveredSeq(); err != nil || seq != 5 {
		t.Fatalf("DeliveredSeq after lower value = %d, %v", seq, err)
	}
	// Advances again.
	if err := s.SetDeliveredSeq(7); err != nil {
		t.Fatalf("SetDeliveredSeq(7): %v", err)
	}
	if seq, err := s.DeliveredSeq(); err != nil || seq != 7 {
		t.Fatalf("DeliveredSeq = %d, %v", seq, err)
	}
}

// TestOutboxJobsSessions: jobs.json and sessions.json round-trip through
// Update/Load.
func TestOutboxJobsSessions(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir)
	jobs, err := s.LoadJobs()
	if err != nil || len(jobs.Jobs) != 0 {
		t.Fatalf("LoadJobs = %v, %v", jobs, err)
	}
	if err := s.UpdateJobs(func(j *Jobs) error {
		j.Jobs["TASK-1"] = &Job{ID: "TASK-1", Projeto: testProjeto, Estado: "running", CreatedAt: "2026-01-02T03:04:05-03:00", UpdatedAt: "2026-01-02T03:04:05-03:00"}
		return nil
	}); err != nil {
		t.Fatalf("UpdateJobs: %v", err)
	}
	jobs, err = s.LoadJobs()
	if err != nil {
		t.Fatal(err)
	}
	j := jobs.Jobs["TASK-1"]
	if j == nil || j.ID != "TASK-1" || j.Projeto != testProjeto || j.Estado != "running" {
		t.Fatalf("job = %+v", j)
	}
	data, err := os.ReadFile(s.jobsPath())
	if err != nil {
		t.Fatal(err)
	}
	var outer struct {
		Jobs map[string]*Job `json:"jobs"`
	}
	if err := json.Unmarshal(data, &outer); err != nil || outer.Jobs["TASK-1"] == nil {
		t.Fatalf("jobs.json = %s", data)
	}
	if err := s.UpdateSessions(func(ss *Sessions) error {
		ss.Sessions["org/repo\tmain"] = &Session{Projeto: testProjeto, Branch: "main", Card: "CARD-1", StartedAt: "2026-01-02T03:04:05-03:00", UpdatedAt: "2026-01-02T03:04:05-03:00"}
		return nil
	}); err != nil {
		t.Fatalf("UpdateSessions: %v", err)
	}
	ss, err := s.LoadSessions()
	if err != nil {
		t.Fatal(err)
	}
	ses := ss.Sessions["org/repo\tmain"]
	if ses == nil || ses.Projeto != testProjeto || ses.Branch != "main" || ses.Card != "CARD-1" {
		t.Fatalf("session = %+v", ses)
	}
}

// TestOutboxFriction: friction lines are appended as "<ts>\t<cmd>\t<msg>"
// and stay one line even with newlines in the message.
func TestOutboxFriction(t *testing.T) {
	dir := t.TempDir()
	// Best effort in a directory that cannot be created: no panic.
	Friction(filepath.Join(dir, "missing"), "cmd", "msg")
	Friction(dir, "push", "line1\nline2\ttab")
	data, err := os.ReadFile(filepath.Join(dir, "friction.log"))
	if err != nil {
		t.Fatalf("friction.log: %v", err)
	}
	lines := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
	if len(lines) != 1 {
		t.Fatalf("friction.log has %d lines, want 1", len(lines))
	}
	fields := strings.Split(lines[0], "\t")
	if len(fields) != 3 || fields[1] != "push" || fields[2] != "line1 line2 tab" {
		t.Fatalf("friction line = %q", lines[0])
	}
	if strings.Contains(fields[0], "Z") {
		t.Errorf("friction ts %q has Z", fields[0])
	}
}

// TestOutboxWriteFileAtomic: the rename leaves no temp files behind and the
// file has mode 0600.
func TestOutboxWriteFileAtomic(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mode asserts are POSIX-only")
	}
	dir := t.TempDir()
	p := filepath.Join(dir, "target.json")
	if err := WriteFileAtomic(p, []byte(`{"a":1}`)); err != nil {
		t.Fatalf("WriteFileAtomic: %v", err)
	}
	if err := WriteFileAtomic(p, []byte(`{"a":2}`)); err != nil {
		t.Fatalf("WriteFileAtomic overwrite: %v", err)
	}
	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != `{"a":2}` {
		t.Fatalf("content = %s", data)
	}
	fi, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Errorf("mode = %o, want 600", fi.Mode().Perm())
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.Name() != "target.json" {
			t.Errorf("leftover file %q", e.Name())
		}
	}
}
