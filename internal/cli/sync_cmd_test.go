package cli

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/djalmajr/herdr-hermes/internal/jobapi"
	"github.com/djalmajr/herdr-hermes/internal/outbox"
	"github.com/djalmajr/herdr-hermes/internal/testutil/fakesoho"
)

const capsJSON = `{"schema":1,"worker_collaboration":1,"ephemeral_job":1,"job_events":1}`

// capsRule is the fake rule for `capabilities --json`.
func capsRule(line string, code int) fakesoho.Rule {
	return fakesoho.Rule{Argv: []string{"capabilities", "--json"}, Stdout: line + "\n", Code: code}
}

// eventsRule is the fake rule for one `job events` call; events is a
// newline-separated list without a trailing newline (the rule adds it),
// followed by the trailer line.
func eventsRule(id string, since int, events, trailer string, code int) fakesoho.Rule {
	out := trailer
	if events != "" {
		out = events + "\n" + trailer
	}
	return fakesoho.Rule{
		Argv:   []string{"job", "events", "--id", id, "--since", intTo(since)},
		Stdout: out + "\n",
		Code:   code,
	}
}

func intTo(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

// eventLine is one job event line.
func eventLine(seq int, tipo, estado string) string {
	return `{"seq":` + intTo(seq) + `,"ts":"2026-01-02T00:00:0` + intTo(seq%10) + `-03:00","tipo":"` + tipo + `","resumo":"e` + intTo(seq) + `"}`
}

func trailer(ultimo int, estado string) string {
	return `{"eventos":"fim","ultimo_seq":` + intTo(ultimo) + `,"estado":"` + estado + `"}`
}

// runHermesVars is runHermes with the CLI-visible environment variables
// (the wake hook's HERDR_SOHO_JOB_* variables) set.
func runHermesVars(t *testing.T, cfgDir string, vars map[string]string, stdin string, args ...string) (string, string, int) {
	t.Helper()
	var out, errb bytes.Buffer
	env := Env{
		Stdin:     strings.NewReader(stdin),
		Stdout:    &out,
		Stderr:    &errb,
		Getenv:    func(k string) string { return vars[k] },
		Environ:   func() []string { return fakeChildEnv },
		ConfigDir: cfgDir,
		Now:       func() time.Time { return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC) },
		Sleep:     sleepCtx,
	}
	exit := Run(args, env)
	return out.String(), errb.String(), exit
}

// eventsSinceArgs returns the --since argument of every logged
// `job events` call.
func eventsSinceArgs(t *testing.T, fakeDir string) []string {
	t.Helper()
	calls, err := fakesoho.ReadCalls(fakeDir)
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("ReadCalls: %v", err)
	}
	var out []string
	for _, c := range calls {
		if len(c.Argv) >= 2 && c.Argv[0] == "job" && c.Argv[1] == "events" {
			out = append(out, c.Argv[len(c.Argv)-1])
		}
	}
	return out
}

// eventsUpTo is the newline-separated event lines 1..n (tipo accepted).
func eventsUpTo(n int) string {
	var b strings.Builder
	for i := 1; i <= n; i++ {
		if i > 1 {
			b.WriteByte('\n')
		}
		b.WriteString(eventLine(i, "accepted", ""))
	}
	return b.String()
}

// TestSyncAppendsNewEvents: new events are appended in order from
// last_event_seq and the trailer updates the tracked estado.
func TestSyncAppendsNewEvents(t *testing.T) {
	exe, _ := installFakeSoho(t,
		capsRule(capsJSON, 0),
		eventsRule("J1", 0,
			eventLine(1, "accepted", "")+"\n"+eventLine(2, "preparing", ""),
			trailer(2, "running"), 0),
	)
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, exe, "machine-a")
	seedJob(t, cfgDir, "J1", "org/repo", 0, "accepted")
	stdout, _, exit := runHermes(t, cfgDir, false, "", "sync")
	if exit != 0 {
		t.Fatalf("sync: exit = %d, stdout %q", exit, stdout)
	}
	if stdout != `{"jobs":1,"novos":2,"enviados":0,"pendentes":2}`+"\n" {
		t.Fatalf("stdout = %q", stdout)
	}
	recs := readOutboxRecords(t, cfgDir)
	if len(recs) != 2 {
		t.Fatalf("records = %d, want 2", len(recs))
	}
	for i, r := range recs {
		if r.Tipo != outbox.TipoJobEvent || r.JobID == nil || *r.JobID != "J1" {
			t.Errorf("record %d = %+v", i, r)
		}
		if r.IdempotencyKey != "J1:"+intTo(i+1) {
			t.Errorf("record %d key = %q", i, r.IdempotencyKey)
		}
		var ev struct {
			Seq int64 `json:"seq"`
		}
		if err := json.Unmarshal(r.Dados, &ev); err != nil || int(ev.Seq) != i+1 {
			t.Errorf("record %d dados = %s, want seq %d", i, r.Dados, i+1)
		}
		if r.Seq != int64(i+1) {
			t.Errorf("record %d outbox seq = %d, order not preserved", i, r.Seq)
		}
	}
	s, err := outbox.Open(outbox.StateDir(cfgDir), outbox.Options{ReadOnly: true})
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	jobs, err := s.LoadJobs()
	if err != nil {
		t.Fatalf("LoadJobs: %v", err)
	}
	if j := jobs.Jobs["J1"]; j == nil || j.LastEventSeq != 2 || j.Estado != "running" {
		t.Errorf("job J1 = %+v, want last_event_seq 2, estado running", j)
	}
}

// TestSyncTrailerTerminalSkipped: a trailer that reaches a terminal state
// marks the job so the next sync skips it (no events call for it).
func TestSyncTrailerTerminalSkipped(t *testing.T) {
	exe, fakeDir := installFakeSoho(t,
		capsRule(capsJSON, 0),
		eventsRule("J1", 2, eventLine(3, "terminal", ""), trailer(3, "done"), 0),
	)
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, exe, "machine-a")
	// Events 1 and 2 are already durable, so the seeded cursor 2 is the
	// contiguous stored prefix.
	s, err := outbox.Open(outbox.StateDir(cfgDir), outbox.Options{})
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	for i := 1; i <= 2; i++ {
		if _, appended, err := s.AppendJobEvent("machine-a", "org/repo", "J1", json.RawMessage(eventLine(i, "accepted", ""))); err != nil || !appended {
			t.Fatalf("event %d: appended=%v err=%v, want true,nil", i, appended, err)
		}
	}
	seedJob(t, cfgDir, "J1", "org/repo", 2, "running")
	stdout, _, exit := runHermes(t, cfgDir, false, "", "sync", "--job", "J1")
	if exit != 0 {
		t.Fatalf("sync --job: exit = %d, stdout %q", exit, stdout)
	}
	if stdout != `{"jobs":1,"novos":1,"enviados":0,"pendentes":3}`+"\n" {
		t.Fatalf("stdout = %q", stdout)
	}
	s, err = outbox.Open(outbox.StateDir(cfgDir), outbox.Options{ReadOnly: true})
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	jobs, err := s.LoadJobs()
	if err != nil {
		t.Fatalf("LoadJobs: %v", err)
	}
	if j := jobs.Jobs["J1"]; j == nil || j.Estado != "done" || j.LastEventSeq != 3 {
		t.Fatalf("job J1 = %+v, want estado done, last 3", j)
	}
	// The next sync skips the terminal job: no events call for it.
	stdout, _, exit = runHermes(t, cfgDir, false, "", "sync")
	if exit != 0 {
		t.Fatalf("second sync: exit = %d, stdout %q", exit, stdout)
	}
	if stdout != `{"jobs":0,"novos":0,"enviados":0,"pendentes":3}`+"\n" {
		t.Fatalf("second sync stdout = %q", stdout)
	}
	calls, err := fakesoho.ReadCalls(fakeDir)
	if err != nil {
		t.Fatalf("ReadCalls: %v", err)
	}
	var eventsCalls int
	for _, c := range calls {
		if len(c.Argv) >= 2 && c.Argv[0] == "job" && c.Argv[1] == "events" {
			eventsCalls++
		}
	}
	if eventsCalls != 1 {
		t.Errorf("events calls = %d, want 1 (the terminal job was skipped)", eventsCalls)
	}
}

// TestSyncRetryConverges: running sync twice converges; the second run
// appends no duplicate records.
func TestSyncRetryConverges(t *testing.T) {
	exe, _ := installFakeSoho(t,
		capsRule(capsJSON, 0),
		eventsRule("J1", 0,
			eventLine(1, "accepted", "")+"\n"+eventLine(2, "worker_spawned", ""),
			trailer(2, "running"), 0),
		eventsRule("J1", 2, "", trailer(2, "running"), 0),
	)
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, exe, "machine-a")
	seedJob(t, cfgDir, "J1", "org/repo", 0, "accepted")
	stdout, _, exit := runHermes(t, cfgDir, false, "", "sync")
	if exit != 0 {
		t.Fatalf("first sync: exit = %d, stdout %q", exit, stdout)
	}
	if stdout != `{"jobs":1,"novos":2,"enviados":0,"pendentes":2}`+"\n" {
		t.Fatalf("first sync stdout = %q", stdout)
	}
	stdout, _, exit = runHermes(t, cfgDir, false, "", "sync")
	if exit != 0 {
		t.Fatalf("second sync: exit = %d, stdout %q", exit, stdout)
	}
	if stdout != `{"jobs":1,"novos":0,"enviados":0,"pendentes":2}`+"\n" {
		t.Fatalf("second sync stdout = %q", stdout)
	}
	recs := readOutboxRecords(t, cfgDir)
	if len(recs) != 2 {
		t.Fatalf("records after two syncs = %d, want 2 (no duplicates)", len(recs))
	}
	for _, r := range recs {
		if r.Tipo != outbox.TipoJobEvent {
			t.Errorf("record = %+v", r)
		}
	}
}

// TestSyncRecoversGapAfterWake: the wake hook stored event 3 before sync
// stored 1 and 2; the cursor is the contiguous stored prefix (0), so the
// sync refetches from 0 and appends the gap in upstream event order after
// the wake record, with exactly one record per key and the original event
// bytes in dados; a second sync appends nothing.
func TestSyncRecoversGapAfterWake(t *testing.T) {
	exe, fakeDir := installFakeSoho(t,
		capsRule(capsJSON, 0),
		eventsRule("J1", 0,
			eventLine(1, "accepted", "")+"\n"+eventLine(2, "preparing", "")+"\n"+eventLine(3, "question", ""),
			trailer(3, "running"), 0),
		eventsRule("J1", 3, "", trailer(3, "running"), 0),
	)
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, exe, "machine-a")
	seedJob(t, cfgDir, "J1", "org/repo", 0, "accepted")
	// The wake hook stores event 3 directly; the cursor stays 0 because
	// events 1 and 2 were never stored.
	s, err := outbox.Open(outbox.StateDir(cfgDir), outbox.Options{})
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	wakeEv := json.RawMessage(eventLine(3, "question", ""))
	if _, appended, err := s.AppendJobEvent("machine-a", "org/repo", "J1", wakeEv); err != nil || !appended {
		t.Fatalf("wake event 3: appended=%v err=%v, want true,nil", appended, err)
	}
	jobs, err := s.LoadJobs()
	if err != nil {
		t.Fatalf("LoadJobs: %v", err)
	}
	if j := jobs.Jobs["J1"]; j == nil || j.LastEventSeq != 0 {
		t.Fatalf("after the wake: job J1 = %+v, want last_event_seq 0 (events 1 and 2 absent)", j)
	}
	stdout, _, exit := runHermes(t, cfgDir, false, "", "sync")
	if exit != 0 {
		t.Fatalf("sync: exit = %d, stdout %q", exit, stdout)
	}
	if stdout != `{"jobs":1,"novos":2,"enviados":0,"pendentes":3}`+"\n" {
		t.Fatalf("stdout = %q", stdout)
	}
	calls, err := fakesoho.ReadCalls(fakeDir)
	if err != nil {
		t.Fatalf("ReadCalls: %v", err)
	}
	var sinceArgs []string
	for _, c := range calls {
		if len(c.Argv) >= 2 && c.Argv[0] == "job" && c.Argv[1] == "events" {
			sinceArgs = append(sinceArgs, c.Argv[len(c.Argv)-1])
		}
	}
	if len(sinceArgs) != 1 || sinceArgs[0] != "0" {
		t.Fatalf("events --since args = %v, want [0] (the contiguous stored prefix)", sinceArgs)
	}
	recs := readOutboxRecords(t, cfgDir)
	if len(recs) != 3 {
		t.Fatalf("records = %d, want 3 (one per key)", len(recs))
	}
	// Outbox order: the wake record 3 first, then the gap in upstream
	// event order 1, 2; each record keeps the original event bytes.
	wantKeys := []string{"J1:3", "J1:1", "J1:2"}
	wantDados := []string{eventLine(3, "question", ""), eventLine(1, "accepted", ""), eventLine(2, "preparing", "")}
	for i, r := range recs {
		if r.IdempotencyKey != wantKeys[i] {
			t.Fatalf("record %d key = %q, want %q (order 3, 1, 2)", i+1, r.IdempotencyKey, wantKeys[i])
		}
		if r.Seq != int64(i+1) {
			t.Fatalf("record %d outbox seq = %d, want %d", i+1, r.Seq, i+1)
		}
		if !bytes.Equal(r.Dados, []byte(wantDados[i])) {
			t.Fatalf("record %d dados = %s, want the original bytes %s", i+1, r.Dados, wantDados[i])
		}
	}
	jobs, err = s.LoadJobs()
	if err != nil {
		t.Fatalf("LoadJobs: %v", err)
	}
	if j := jobs.Jobs["J1"]; j == nil || j.LastEventSeq != 3 || j.Estado != "running" {
		t.Fatalf("job J1 = %+v, want last_event_seq 3, estado running", j)
	}
	// The second sync asks --since 3 and appends nothing.
	stdout, _, exit = runHermes(t, cfgDir, false, "", "sync")
	if exit != 0 {
		t.Fatalf("second sync: exit = %d, stdout %q", exit, stdout)
	}
	if stdout != `{"jobs":1,"novos":0,"enviados":0,"pendentes":3}`+"\n" {
		t.Fatalf("second sync stdout = %q", stdout)
	}
	if recs := readOutboxRecords(t, cfgDir); len(recs) != 3 {
		t.Fatalf("records after the second sync = %d, want 3 (nothing appended)", len(recs))
	}
}

// TestSyncRetryAfterAppendCrash: a crash between the outbox append and the
// jobs.json write leaves records without bookkeeping; the sync refetches
// from the contiguous stored prefix, appends nothing that is already
// durable, and converges with the healed cursor and no duplicate records.
func TestSyncRetryAfterAppendCrash(t *testing.T) {
	exe, _ := installFakeSoho(t,
		capsRule(capsJSON, 0),
		eventsRule("J1", 2, eventLine(3, "question", ""), trailer(3, "running"), 0),
		eventsRule("J1", 3, "", trailer(3, "running"), 0),
	)
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, exe, "machine-a")
	seedJob(t, cfgDir, "J1", "org/repo", 0, "running")
	// A previous sync appended events 1 and 2 and crashed before the
	// jobs.json write: store the records, then rewrite jobs.json back to 0.
	s, err := outbox.Open(outbox.StateDir(cfgDir), outbox.Options{})
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	for i := 1; i <= 2; i++ {
		if _, appended, err := s.AppendJobEvent("machine-a", "org/repo", "J1", json.RawMessage(eventLine(i, "accepted", ""))); err != nil || !appended {
			t.Fatalf("event %d: appended=%v err=%v, want true,nil", i, appended, err)
		}
	}
	jobsData := `{"jobs":{"J1":{"id":"J1","projeto":"org/repo","last_event_seq":0,"estado":"running"}}}` + "\n"
	if err := os.WriteFile(filepath.Join(outbox.StateDir(cfgDir), "jobs.json"), []byte(jobsData), 0o600); err != nil {
		t.Fatal(err)
	}
	stdout, _, exit := runHermes(t, cfgDir, false, "", "sync")
	if exit != 0 {
		t.Fatalf("sync: exit = %d, stdout %q", exit, stdout)
	}
	if stdout != `{"jobs":1,"novos":1,"enviados":0,"pendentes":3}`+"\n" {
		t.Fatalf("stdout = %q", stdout)
	}
	recs := readOutboxRecords(t, cfgDir)
	if len(recs) != 3 {
		t.Fatalf("records = %d, want 3 (no duplicates)", len(recs))
	}
	counts := map[string]int{}
	for _, r := range recs {
		counts[r.IdempotencyKey]++
	}
	for _, k := range []string{"J1:1", "J1:2", "J1:3"} {
		if counts[k] != 1 {
			t.Fatalf("key %s stored %d times, want exactly 1", k, counts[k])
		}
	}
	jobs, err := s.LoadJobs()
	if err != nil {
		t.Fatalf("LoadJobs: %v", err)
	}
	if j := jobs.Jobs["J1"]; j == nil || j.LastEventSeq != 3 || j.Estado != "running" {
		t.Fatalf("job J1 = %+v, want last_event_seq 3 (healed), estado running", j)
	}
	// The second sync converges: nothing new is appended.
	stdout, _, exit = runHermes(t, cfgDir, false, "", "sync")
	if exit != 0 {
		t.Fatalf("second sync: exit = %d, stdout %q", exit, stdout)
	}
	if stdout != `{"jobs":1,"novos":0,"enviados":0,"pendentes":3}`+"\n" {
		t.Fatalf("second sync stdout = %q", stdout)
	}
	if recs := readOutboxRecords(t, cfgDir); len(recs) != 3 {
		t.Fatalf("records after the second sync = %d, want 3 (no duplicates)", len(recs))
	}
}

// TestSyncFailedEventStopsJob: a failed event line stops the job's event
// loop; the trailer must not be applied (the job stays non-terminal and
// unchanged), and the next sync retries the events.
func TestSyncFailedEventStopsJob(t *testing.T) {
	exe, fakeDir := installFakeSoho(t,
		capsRule(capsJSON, 0),
		fakesoho.Rule{
			Argv:   []string{"job", "events", "--id", "J1", "--since", "0"},
			Stdout: `{"seq":"nope"}` + "\n" + trailer(1, "done") + "\n",
			Code:   0,
		},
	)
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, exe, "machine-a")
	seedJob(t, cfgDir, "J1", "org/repo", 0, "running")
	stdout, _, exit := runHermes(t, cfgDir, false, "", "sync", "--job", "J1")
	if exit != 0 {
		t.Fatalf("first sync: exit = %d, stdout %q", exit, stdout)
	}
	if stdout != `{"jobs":1,"novos":0,"enviados":0,"pendentes":0}`+"\n" {
		t.Fatalf("first sync stdout = %q", stdout)
	}
	s, err := outbox.Open(outbox.StateDir(cfgDir), outbox.Options{ReadOnly: true})
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	jobs, err := s.LoadJobs()
	if err != nil {
		t.Fatalf("LoadJobs: %v", err)
	}
	if j := jobs.Jobs["J1"]; j == nil || j.Estado != "running" || j.LastEventSeq != 0 {
		t.Fatalf("job J1 = %+v, want estado running, last 0 (the terminal trailer was applied)", j)
	}
	logData, err := os.ReadFile(filepath.Join(outbox.StateDir(cfgDir), "friction.log"))
	if err != nil || !strings.Contains(string(logData), "J1") {
		t.Errorf("friction.log = %q, want the J1 failure line", logData)
	}
	// The second sync calls the events again: the job was not marked
	// terminal by the trailer.
	stdout, _, exit = runHermes(t, cfgDir, false, "", "sync")
	if exit != 0 {
		t.Fatalf("second sync: exit = %d, stdout %q", exit, stdout)
	}
	calls, err := fakesoho.ReadCalls(fakeDir)
	if err != nil {
		t.Fatalf("ReadCalls: %v", err)
	}
	var eventsCalls int
	for _, c := range calls {
		if len(c.Argv) >= 2 && c.Argv[0] == "job" && c.Argv[1] == "events" {
			eventsCalls++
		}
	}
	if eventsCalls != 2 {
		t.Errorf("events calls = %d, want 2 (the failed job was retried)", eventsCalls)
	}
}

// TestSyncFailedEventKeepsProgress: events appended before a failed line
// stay (dedupe handles the retry); last_event_seq stays at the last
// appended event, the trailer is not applied, and the next sync retries
// from the unchanged cursor.
func TestSyncFailedEventKeepsProgress(t *testing.T) {
	exe, _ := installFakeSoho(t,
		capsRule(capsJSON, 0),
		fakesoho.Rule{
			Argv:   []string{"job", "events", "--id", "J1", "--since", "0"},
			Stdout: eventLine(1, "accepted", "") + "\n" + `{"seq":"nope"}` + "\n" + trailer(2, "running") + "\n",
			Code:   0,
		},
		eventsRule("J1", 1, eventLine(2, "worker_spawned", ""), trailer(2, "running"), 0),
	)
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, exe, "machine-a")
	seedJob(t, cfgDir, "J1", "org/repo", 0, "accepted")
	stdout, _, exit := runHermes(t, cfgDir, false, "", "sync")
	if exit != 0 {
		t.Fatalf("first sync: exit = %d, stdout %q", exit, stdout)
	}
	if stdout != `{"jobs":1,"novos":1,"enviados":0,"pendentes":1}`+"\n" {
		t.Fatalf("first sync stdout = %q", stdout)
	}
	recs := readOutboxRecords(t, cfgDir)
	if len(recs) != 1 || recs[0].IdempotencyKey != "J1:1" {
		t.Fatalf("records = %+v, want only J1:1", recs)
	}
	s, err := outbox.Open(outbox.StateDir(cfgDir), outbox.Options{ReadOnly: true})
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	jobs, err := s.LoadJobs()
	if err != nil {
		t.Fatalf("LoadJobs: %v", err)
	}
	if j := jobs.Jobs["J1"]; j == nil || j.LastEventSeq != 1 || j.Estado != "accepted" {
		t.Fatalf("job J1 = %+v, want last_event_seq 1, estado accepted (the trailer was applied)", j)
	}
	// The next sync retries from the unchanged cursor: event 2 appends and
	// the trailer is applied only when the run has no failed line.
	stdout, _, exit = runHermes(t, cfgDir, false, "", "sync")
	if exit != 0 {
		t.Fatalf("second sync: exit = %d, stdout %q", exit, stdout)
	}
	if stdout != `{"jobs":1,"novos":1,"enviados":0,"pendentes":2}`+"\n" {
		t.Fatalf("second sync stdout = %q", stdout)
	}
	if recs := readOutboxRecords(t, cfgDir); len(recs) != 2 {
		t.Fatalf("records after retry = %d, want 2 (no duplicates)", len(recs))
	}
	jobs, err = s.LoadJobs()
	if err != nil {
		t.Fatalf("LoadJobs: %v", err)
	}
	if j := jobs.Jobs["J1"]; j == nil || j.LastEventSeq != 2 || j.Estado != "running" {
		t.Fatalf("job J1 after retry = %+v, want last 2, estado running", j)
	}
}

// TestSyncValidEventAfterBadLine: a valid event after the failed line must
// not be appended (the loop breaks, not continues); the outbox holds only
// the event before the bad line, last_event_seq stays there, and the
// trailer is not applied.
func TestSyncValidEventAfterBadLine(t *testing.T) {
	exe, _ := installFakeSoho(t,
		capsRule(capsJSON, 0),
		fakesoho.Rule{
			Argv:   []string{"job", "events", "--id", "J1", "--since", "0"},
			Stdout: eventLine(1, "accepted", "") + "\n" + `{"seq":"nope"}` + "\n" + eventLine(3, "push", "") + "\n" + trailer(3, "done") + "\n",
			Code:   0,
		},
	)
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, exe, "machine-a")
	seedJob(t, cfgDir, "J1", "org/repo", 0, "accepted")
	stdout, _, exit := runHermes(t, cfgDir, false, "", "sync")
	if exit != 0 {
		t.Fatalf("sync: exit = %d, stdout %q", exit, stdout)
	}
	if stdout != `{"jobs":1,"novos":1,"enviados":0,"pendentes":1}`+"\n" {
		t.Fatalf("sync stdout = %q", stdout)
	}
	recs := readOutboxRecords(t, cfgDir)
	if len(recs) != 1 || recs[0].IdempotencyKey != "J1:1" {
		t.Fatalf("records = %+v, want only J1:1 (the valid event after the bad line was appended)", recs)
	}
	s, err := outbox.Open(outbox.StateDir(cfgDir), outbox.Options{ReadOnly: true})
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	jobs, err := s.LoadJobs()
	if err != nil {
		t.Fatalf("LoadJobs: %v", err)
	}
	if j := jobs.Jobs["J1"]; j == nil || j.LastEventSeq != 1 || j.Estado != "accepted" {
		t.Fatalf("job J1 = %+v, want last_event_seq 1, estado accepted", j)
	}
}

// TestSyncCapabilitiesMissing: a capability failure exits 43 with the
// capabilities_missing status; a missing executable does too.
func TestSyncCapabilitiesMissing(t *testing.T) {
	exe, _ := installFakeSoho(t, capsRule(`{"schema":1,"ephemeral_job":1}`, 0))
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, exe, "machine-a")
	stdout, _, exit := runHermes(t, cfgDir, false, "", "sync")
	if exit != 43 {
		t.Fatalf("exit = %d, want 43", exit)
	}
	if !strings.Contains(stdout, `"status":"capabilities_missing"`) {
		t.Errorf("stdout = %q", stdout)
	}
	// A missing executable is also a capability failure.
	cfgDir = t.TempDir()
	setSohoConfig(t, cfgDir, "no-such-herdr-soho-binary-xyz", "machine-a")
	_, _, exit = runHermes(t, cfgDir, false, "", "sync")
	if exit != 43 {
		t.Fatalf("missing binary: exit = %d, want 43", exit)
	}
}

// TestSyncUnknownJob: sync --job with an unknown id exits 3 with the
// not_found line.
func TestSyncUnknownJob(t *testing.T) {
	exe, _ := installFakeSoho(t, capsRule(capsJSON, 0))
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, exe, "machine-a")
	stdout, _, exit := runHermes(t, cfgDir, false, "", "sync", "--job", "nope")
	if exit != 3 {
		t.Fatalf("exit = %d, want 3", exit)
	}
	if stdout != `{"status":"not_found"}`+"\n" {
		t.Errorf("stdout = %q", stdout)
	}
}

// TestSyncPushOnly: --push-only skips the capability check and the event
// sync; it only reports the push outcome.
func TestSyncPushOnly(t *testing.T) {
	// No capabilities rule at all: a capabilities call would exit 127.
	exe, fakeDir := installFakeSoho(t)
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, exe, "machine-a")
	// One pending record.
	s, err := outbox.Open(outbox.StateDir(cfgDir), outbox.Options{})
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	if _, err := s.Append(outbox.Record{
		Tipo: outbox.TipoDispatch, Maquina: "machine-a", Projeto: "org/repo",
		Dados: json.RawMessage(`{"id":"J1"}`),
	}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	stdout, _, exit := runHermes(t, cfgDir, false, "", "sync", "--push-only")
	if exit != 0 {
		t.Fatalf("exit = %d, stdout %q", exit, stdout)
	}
	if stdout != `{"jobs":0,"novos":0,"enviados":0,"pendentes":1}`+"\n" {
		t.Fatalf("stdout = %q", stdout)
	}
	calls, err := fakesoho.ReadCalls(fakeDir)
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("ReadCalls: %v", err)
	}
	if len(calls) != 0 {
		t.Errorf("fake calls = %d, want 0 (capability check skipped)", len(calls))
	}
}

// TestSyncPerJobFailure: a per-job events failure goes to friction and the
// loop continues with the other jobs.
func TestSyncPerJobFailure(t *testing.T) {
	exe, _ := installFakeSoho(t,
		capsRule(capsJSON, 0),
		eventsRule("J1", 0, eventLine(1, "accepted", ""), trailer(1, "running"), 0),
		eventsRule("J2", 0, "", "", 3),
	)
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, exe, "machine-a")
	seedJob(t, cfgDir, "J1", "org/repo", 0, "accepted")
	seedJob(t, cfgDir, "J2", "org/repo", 0, "accepted")
	stdout, _, exit := runHermes(t, cfgDir, false, "", "sync")
	if exit != 0 {
		t.Fatalf("exit = %d, stdout %q", exit, stdout)
	}
	if stdout != `{"jobs":2,"novos":1,"enviados":0,"pendentes":1}`+"\n" {
		t.Fatalf("stdout = %q", stdout)
	}
	if recs := readOutboxRecords(t, cfgDir); len(recs) != 1 {
		t.Errorf("records = %d, want 1 (J1 only)", len(recs))
	}
	logData, err := os.ReadFile(filepath.Join(outbox.StateDir(cfgDir), "friction.log"))
	if err != nil || !strings.Contains(string(logData), "J2") {
		t.Errorf("friction.log = %q, want the J2 failure line", logData)
	}
}

// TestSyncClosedJobSkipped: a closed job is skipped even when --job names
// it.
func TestSyncClosedJobSkipped(t *testing.T) {
	exe, fakeDir := installFakeSoho(t, capsRule(capsJSON, 0))
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, exe, "machine-a")
	s, err := outbox.Open(outbox.StateDir(cfgDir), outbox.Options{})
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	err = s.UpdateJobs(func(js *outbox.Jobs) error {
		js.Jobs["J1"] = &outbox.Job{ID: "J1", Projeto: "org/repo", Estado: "closed", Closed: true}
		return nil
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	stdout, _, exit := runHermes(t, cfgDir, false, "", "sync", "--job", "J1")
	if exit != 0 {
		t.Fatalf("exit = %d, stdout %q", exit, stdout)
	}
	if stdout != `{"jobs":0,"novos":0,"enviados":0,"pendentes":0}`+"\n" {
		t.Fatalf("stdout = %q", stdout)
	}
	calls, err := fakesoho.ReadCalls(fakeDir)
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("ReadCalls: %v", err)
	}
	for _, c := range calls {
		if len(c.Argv) >= 2 && c.Argv[0] == "job" && c.Argv[1] == "events" {
			t.Errorf("events called for a closed job: %v", c.Argv)
		}
	}
}

// TestSyncLegacyHighCursorRefetches: a jobs.json written by a previous
// release holds a high-water last_event_seq while the outbox lacks the
// lower events; the sync asks the contiguous stored prefix (0), stores the
// missing events with their original bytes and converges on the cursor.
func TestSyncLegacyHighCursorRefetches(t *testing.T) {
	exe, fakeDir := installFakeSoho(t,
		capsRule(capsJSON, 0),
		eventsRule("J1", 0,
			eventLine(1, "accepted", "")+"\n"+eventLine(2, "preparing", "")+"\n"+eventLine(3, "question", ""),
			trailer(3, "running"), 0),
		eventsRule("J1", 3, "", trailer(3, "running"), 0),
	)
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, exe, "machine-a")
	// A previous release recorded the high-water cursor 3 while the
	// outbox holds none of the events.
	seedJob(t, cfgDir, "J1", "org/repo", 3, "running")
	stdout, _, exit := runHermes(t, cfgDir, false, "", "sync")
	if exit != 0 {
		t.Fatalf("sync: exit = %d, stdout %q", exit, stdout)
	}
	if stdout != `{"jobs":1,"novos":3,"enviados":0,"pendentes":3}`+"\n" {
		t.Fatalf("stdout = %q", stdout)
	}
	sinceArgs := eventsSinceArgs(t, fakeDir)
	if len(sinceArgs) != 1 || sinceArgs[0] != "0" {
		t.Fatalf("events --since args = %v, want [0] (the contiguous stored prefix, not the legacy cursor 3)", sinceArgs)
	}
	recs := readOutboxRecords(t, cfgDir)
	if len(recs) != 3 {
		t.Fatalf("records = %d, want 3", len(recs))
	}
	wantDados := []string{eventLine(1, "accepted", ""), eventLine(2, "preparing", ""), eventLine(3, "question", "")}
	for i, r := range recs {
		if r.IdempotencyKey != "J1:"+intTo(i+1) {
			t.Fatalf("record %d key = %q, want J1:%d", i+1, r.IdempotencyKey, i+1)
		}
		if r.Seq != int64(i+1) {
			t.Fatalf("record %d outbox seq = %d, want %d", i+1, r.Seq, i+1)
		}
		if !bytes.Equal(r.Dados, []byte(wantDados[i])) {
			t.Fatalf("record %d dados = %s, want the original bytes %s", i+1, r.Dados, wantDados[i])
		}
	}
	s, err := outbox.Open(outbox.StateDir(cfgDir), outbox.Options{ReadOnly: true})
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	jobs, err := s.LoadJobs()
	if err != nil {
		t.Fatalf("LoadJobs: %v", err)
	}
	if j := jobs.Jobs["J1"]; j == nil || j.LastEventSeq != 3 || j.Estado != "running" {
		t.Errorf("job J1 = %+v, want last_event_seq 3, estado running", j)
	}
}

// TestSyncSparseTerminalWakeThenSync: the wake hook stored only the
// terminal event 9 (prefix 0) of a job whose estado is already terminal;
// the sync still fetches from the contiguous stored prefix, stores 1..8 in
// upstream order after the wake record and converges the cursor; a second
// sync skips the job because the gap is closed.
func TestSyncSparseTerminalWakeThenSync(t *testing.T) {
	exe, fakeDir := installFakeSoho(t,
		capsRule(capsJSON, 0),
		eventsRule("J1", 0, eventsUpTo(9), trailer(9, "done"), 0),
	)
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, exe, "machine-a")
	// The job is tracked and already terminal in jobs.json (an earlier
	// sync applied the terminal estado), while its events were never
	// stored.
	seedJob(t, cfgDir, "J1", "org/repo", 0, "done")
	wakeStdin := eventLine(9, "terminal", "")
	stdout, stderr, exit := runHermesVars(t, cfgDir, map[string]string{
		jobapi.EnvJobID:             "J1",
		jobapi.EnvJobSeq:            "9",
		jobapi.EnvJobEvent:          wakeStdin,
		jobapi.EnvJobIdempotencyKey: "J1:9",
	}, wakeStdin, "wake")
	if exit != 0 {
		t.Fatalf("wake: exit = %d, stdout %q stderr %q", exit, stdout, stderr)
	}
	if stdout != `{"seq":1,"duplicado":false,"enviados":0,"pendentes":1}`+"\n" {
		t.Fatalf("wake stdout = %q", stdout)
	}
	stdout, _, exit = runHermes(t, cfgDir, false, "", "sync")
	if exit != 0 {
		t.Fatalf("sync: exit = %d, stdout %q", exit, stdout)
	}
	if stdout != `{"jobs":1,"novos":8,"enviados":0,"pendentes":9}`+"\n" {
		t.Fatalf("stdout = %q", stdout)
	}
	sinceArgs := eventsSinceArgs(t, fakeDir)
	if len(sinceArgs) != 1 || sinceArgs[0] != "0" {
		t.Fatalf("events --since args = %v, want [0] (the contiguous stored prefix)", sinceArgs)
	}
	recs := readOutboxRecords(t, cfgDir)
	if len(recs) != 9 {
		t.Fatalf("records = %d, want 9 (one per key 1..9)", len(recs))
	}
	// The wake record 9 first, then the gap in upstream event order 1..8.
	wantKeys := []string{"J1:9"}
	wantDados := []string{wakeStdin}
	for i := 1; i <= 8; i++ {
		wantKeys = append(wantKeys, "J1:"+intTo(i))
		wantDados = append(wantDados, eventLine(i, "accepted", ""))
	}
	for i, r := range recs {
		if r.IdempotencyKey != wantKeys[i] {
			t.Fatalf("record %d key = %q, want %q (9 stored first, then 1..8)", i+1, r.IdempotencyKey, wantKeys[i])
		}
		if r.Seq != int64(i+1) {
			t.Fatalf("record %d outbox seq = %d, want %d", i+1, r.Seq, i+1)
		}
		if !bytes.Equal(r.Dados, []byte(wantDados[i])) {
			t.Fatalf("record %d dados = %s, want the original bytes %s", i+1, r.Dados, wantDados[i])
		}
	}
	s, err := outbox.Open(outbox.StateDir(cfgDir), outbox.Options{ReadOnly: true})
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	jobs, err := s.LoadJobs()
	if err != nil {
		t.Fatalf("LoadJobs: %v", err)
	}
	if j := jobs.Jobs["J1"]; j == nil || j.LastEventSeq != 9 || j.Estado != "done" {
		t.Fatalf("job J1 = %+v, want last_event_seq 9, estado done", j)
	}
	// The gap is closed and the job is terminal: the second sync skips
	// it, with no job events call for it.
	stdout, _, exit = runHermes(t, cfgDir, false, "", "sync")
	if exit != 0 {
		t.Fatalf("second sync: exit = %d, stdout %q", exit, stdout)
	}
	if stdout != `{"jobs":0,"novos":0,"enviados":0,"pendentes":9}`+"\n" {
		t.Fatalf("second sync stdout = %q", stdout)
	}
	if n := len(eventsSinceArgs(t, fakeDir)); n != 1 {
		t.Errorf("events calls = %d, want 1 (the second sync skipped the gapless terminal job)", n)
	}
}

// TestSyncClosedJobWithGapStillSynced: a closed job with a stored event
// above the contiguous stored prefix is still synced for the gap: the sync
// stores the missing events and the job stays closed.
func TestSyncClosedJobWithGapStillSynced(t *testing.T) {
	exe, fakeDir := installFakeSoho(t,
		capsRule(capsJSON, 0),
		fakesoho.Rule{Argv: []string{"job", "close", "--id", "J1"}, Stdout: `{"status":"closed"}` + "\n", Code: 0},
		eventsRule("J1", 0, eventsUpTo(9), trailer(9, "done"), 0),
	)
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, exe, "machine-a")
	seedJob(t, cfgDir, "J1", "org/repo", 0, "running")
	wakeStdin := eventLine(9, "terminal", "")
	stdout, stderr, exit := runHermesVars(t, cfgDir, map[string]string{
		jobapi.EnvJobID:             "J1",
		jobapi.EnvJobSeq:            "9",
		jobapi.EnvJobEvent:          wakeStdin,
		jobapi.EnvJobIdempotencyKey: "J1:9",
	}, wakeStdin, "wake")
	if exit != 0 {
		t.Fatalf("wake: exit = %d, stdout %q stderr %q", exit, stdout, stderr)
	}
	stdout, _, exit = runHermes(t, cfgDir, false, "", "job", "close", "--id", "J1")
	if exit != 0 {
		t.Fatalf("job close: exit = %d, stdout %q", exit, stdout)
	}
	s, err := outbox.Open(outbox.StateDir(cfgDir), outbox.Options{ReadOnly: true})
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	jobs, err := s.LoadJobs()
	if err != nil {
		t.Fatalf("LoadJobs: %v", err)
	}
	if j := jobs.Jobs["J1"]; j == nil || !j.Closed {
		t.Fatalf("job J1 after close = %+v, want Closed true", j)
	}
	stdout, _, exit = runHermes(t, cfgDir, false, "", "sync")
	if exit != 0 {
		t.Fatalf("sync: exit = %d, stdout %q", exit, stdout)
	}
	if stdout != `{"jobs":1,"novos":8,"enviados":0,"pendentes":9}`+"\n" {
		t.Fatalf("stdout = %q", stdout)
	}
	sinceArgs := eventsSinceArgs(t, fakeDir)
	if len(sinceArgs) != 1 || sinceArgs[0] != "0" {
		t.Fatalf("events --since args = %v, want [0] (the contiguous stored prefix)", sinceArgs)
	}
	recs := readOutboxRecords(t, cfgDir)
	if len(recs) != 9 {
		t.Fatalf("records = %d, want 9 (one per key 1..9)", len(recs))
	}
	wantKeys := []string{"J1:9"}
	wantDados := []string{wakeStdin}
	for i := 1; i <= 8; i++ {
		wantKeys = append(wantKeys, "J1:"+intTo(i))
		wantDados = append(wantDados, eventLine(i, "accepted", ""))
	}
	for i, r := range recs {
		if r.IdempotencyKey != wantKeys[i] {
			t.Fatalf("record %d key = %q, want %q (9 stored first, then 1..8)", i+1, r.IdempotencyKey, wantKeys[i])
		}
		if !bytes.Equal(r.Dados, []byte(wantDados[i])) {
			t.Fatalf("record %d dados = %s, want the original bytes %s", i+1, r.Dados, wantDados[i])
		}
	}
	jobs, err = s.LoadJobs()
	if err != nil {
		t.Fatalf("LoadJobs: %v", err)
	}
	if j := jobs.Jobs["J1"]; j == nil || !j.Closed || j.LastEventSeq != 9 || j.Estado != "done" {
		t.Fatalf("job J1 = %+v, want still closed, last_event_seq 9, estado done", j)
	}
}

// TestSyncTerminalLegacyCursorStillSynced: a terminal job whose recorded
// cursor (5) sits above the contiguous stored prefix (only event 5 is
// stored) is synced from the prefix and stores the missing events 1..4.
func TestSyncTerminalLegacyCursorStillSynced(t *testing.T) {
	exe, fakeDir := installFakeSoho(t,
		capsRule(capsJSON, 0),
		eventsRule("J1", 0,
			eventLine(1, "accepted", "")+"\n"+eventLine(2, "preparing", "")+"\n"+eventLine(3, "question", "")+"\n"+eventLine(4, "push", ""),
			trailer(5, "done"), 0),
	)
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, exe, "machine-a")
	// Only event 5 is stored (as a crash or a previous release left it),
	// and jobs.json records the high-water cursor 5 with the terminal
	// estado.
	s, err := outbox.Open(outbox.StateDir(cfgDir), outbox.Options{})
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	if _, appended, err := s.AppendJobEvent("machine-a", "org/repo", "J1", json.RawMessage(eventLine(5, "terminal", ""))); err != nil || !appended {
		t.Fatalf("event 5: appended=%v err=%v, want true,nil", appended, err)
	}
	err = s.UpdateJobs(func(js *outbox.Jobs) error {
		js.Jobs["J1"].LastEventSeq = 5
		js.Jobs["J1"].Estado = "done"
		return nil
	})
	if err != nil {
		t.Fatalf("seed cursor: %v", err)
	}
	stdout, _, exit := runHermes(t, cfgDir, false, "", "sync")
	if exit != 0 {
		t.Fatalf("sync: exit = %d, stdout %q", exit, stdout)
	}
	if stdout != `{"jobs":1,"novos":4,"enviados":0,"pendentes":5}`+"\n" {
		t.Fatalf("stdout = %q", stdout)
	}
	sinceArgs := eventsSinceArgs(t, fakeDir)
	if len(sinceArgs) != 1 || sinceArgs[0] != "0" {
		t.Fatalf("events --since args = %v, want [0] (the contiguous stored prefix, not the recorded cursor 5)", sinceArgs)
	}
	recs := readOutboxRecords(t, cfgDir)
	if len(recs) != 5 {
		t.Fatalf("records = %d, want 5 (one per key 1..5)", len(recs))
	}
	wantKeys := []string{"J1:5", "J1:1", "J1:2", "J1:3", "J1:4"}
	wantDados := []string{
		eventLine(5, "terminal", ""), eventLine(1, "accepted", ""),
		eventLine(2, "preparing", ""), eventLine(3, "question", ""), eventLine(4, "push", ""),
	}
	for i, r := range recs {
		if r.IdempotencyKey != wantKeys[i] {
			t.Fatalf("record %d key = %q, want %q (5 stored first, then 1..4)", i+1, r.IdempotencyKey, wantKeys[i])
		}
		if !bytes.Equal(r.Dados, []byte(wantDados[i])) {
			t.Fatalf("record %d dados = %s, want the original bytes %s", i+1, r.Dados, wantDados[i])
		}
	}
	sro, err := outbox.Open(outbox.StateDir(cfgDir), outbox.Options{ReadOnly: true})
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	jobs, err := sro.LoadJobs()
	if err != nil {
		t.Fatalf("LoadJobs: %v", err)
	}
	if j := jobs.Jobs["J1"]; j == nil || j.LastEventSeq != 5 || j.Estado != "done" {
		t.Fatalf("job J1 = %+v, want last_event_seq 5, estado done", j)
	}
}

// TestSyncTerminalNoGapSkipped (control): a terminal job whose stored
// events cover its recorded cursor (no gap) is still skipped: no job
// events call for it.
func TestSyncTerminalNoGapSkipped(t *testing.T) {
	exe, fakeDir := installFakeSoho(t, capsRule(capsJSON, 0))
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, exe, "machine-a")
	s, err := outbox.Open(outbox.StateDir(cfgDir), outbox.Options{})
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	for i := 1; i <= 3; i++ {
		if _, appended, err := s.AppendJobEvent("machine-a", "org/repo", "J1", json.RawMessage(eventLine(i, "accepted", ""))); err != nil || !appended {
			t.Fatalf("event %d: appended=%v err=%v, want true,nil", i, appended, err)
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
	if stdout != `{"jobs":0,"novos":0,"enviados":0,"pendentes":3}`+"\n" {
		t.Fatalf("stdout = %q", stdout)
	}
	if n := len(eventsSinceArgs(t, fakeDir)); n != 0 {
		t.Fatalf("events calls = %d, want 0 (no gap: the terminal job is skipped)", n)
	}
}

// TestSyncBadFlags: unknown flags, a missing --job value and a repeated
// flag exit 2.
func TestSyncBadFlags(t *testing.T) {
	exe, _ := installFakeSoho(t, capsRule(capsJSON, 0))
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, exe, "machine-a")
	for _, args := range [][]string{
		{"sync", "--bogus"},
		{"sync", "--job"},
		{"sync", "--job", "A", "--job", "B"},
		{"sync", "--push-only", "--push-only"},
		{"sync", "--job", "A", "extra"},
	} {
		_, _, exit := runHermes(t, cfgDir, false, "", args...)
		if exit != 2 {
			t.Errorf("%v: exit = %d, want 2", args, exit)
		}
	}
}

// TestSyncEmptyLabel: sync writes records, so without a machine_label it
// refuses with exit 2 before the capability check writes anything; a
// --push-only run still works.
func TestSyncEmptyLabel(t *testing.T) {
	exe, _ := installFakeSoho(t, capsRule(capsJSON, 0))
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, exe, "")
	_, _, exit := runHermes(t, cfgDir, false, "", "sync")
	if exit != 2 {
		t.Fatalf("sync without label: exit = %d, want 2", exit)
	}
	_, _, exit = runHermes(t, cfgDir, false, "", "sync", "--push-only")
	if exit != 0 {
		t.Fatalf("sync --push-only without label: exit = %d, want 0", exit)
	}
}
