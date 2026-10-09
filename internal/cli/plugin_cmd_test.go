package cli

// The CLI-level plugin tests for the slice-4b entry points (the manifest
// gate TestManifest lives in internal/plugin).

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/djalmajr/herdr-hermes/internal/cred"
	"github.com/djalmajr/herdr-hermes/internal/outbox"
	"github.com/djalmajr/herdr-hermes/internal/plugin"
	"github.com/djalmajr/herdr-hermes/internal/testutil/fakesoho"
)

var pluginTestNow = func() time.Time { return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC) }

// eventsRuleAnySince matches every `job events --id <id>` call (any
// --since) with the same single event plus trailer; the outbox deduplicates
// by (job_id, event seq), so repeated syncs stay idempotent.
func eventsRuleAnySince(id string) fakesoho.Rule {
	return fakesoho.Rule{
		Argv:       []string{"job", "events", "--id", id},
		ArgvPrefix: true,
		Stdout:     eventLine(1, "status", "working") + "\n" + trailer(1, "running") + "\n",
		Code:       0,
	}
}

// runPluginEvent runs `plugin event` with the two Herdr event variables set.
func runPluginEvent(t *testing.T, cfgDir, event, eventJSON string) (string, string, int) {
	t.Helper()
	var out, errb bytes.Buffer
	env := Env{
		Stdin:  strings.NewReader(""),
		Stdout: &out,
		Stderr: &errb,
		Getenv: func(k string) string {
			switch k {
			case plugin.EnvEvent:
				return event
			case plugin.EnvEventJSON:
				return eventJSON
			}
			return ""
		},
		Environ:   func() []string { return fakeChildEnv },
		ConfigDir: cfgDir,
		Now:       pluginTestNow,
		Sleep:     sleepCtx,
	}
	exit := Run([]string{"plugin", "event"}, env)
	return out.String(), errb.String(), exit
}

// readFakeCalls parses the fake herdr-soho call log of the install dir.
func readFakeCalls(t *testing.T, fakeDir string) []fakesoho.Call {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(fakeDir, "fake-herdr-soho.calls.jsonl"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatalf("read fake call log: %v", err)
	}
	var calls []fakesoho.Call
	for _, line := range bytes.Split(bytes.TrimSpace(data), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var c fakesoho.Call
		if err := json.Unmarshal(line, &c); err != nil {
			t.Fatalf("call log line %q: %v", line, err)
		}
		calls = append(calls, c)
	}
	return calls
}

// jobsInState loads the tracked jobs of a config dir.
func jobsInState(t *testing.T, cfgDir string) map[string]*outbox.Job {
	t.Helper()
	s, err := outbox.Open(outbox.StateDir(cfgDir), outbox.Options{ReadOnly: true})
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	jobs, err := s.LoadJobs()
	if err != nil {
		t.Fatalf("load jobs: %v", err)
	}
	return jobs.Jobs
}

// stateDirExists reports whether the state dir was created.
func stateDirExists(t *testing.T, cfgDir string) bool {
	t.Helper()
	_, err := os.Stat(outbox.StateDir(cfgDir))
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	t.Fatalf("stat state dir: %v", err)
	return false
}

func TestPluginHooksEvent(t *testing.T) {
	t.Run("created tracks and syncs", func(t *testing.T) {
		exe, fakeDir := installFakeSoho(t, capsRule(capsJSON, 0), eventsRuleAnySince("J1"))
		cfgDir := t.TempDir()
		setSohoConfig(t, cfgDir, exe, "machine-a")
		stdout, _, exit := runPluginEvent(t, cfgDir, "workspace.created", `{"workspace":{"label":"job-J1","workspace_id":"ws-1"}}`)
		if exit != 0 {
			t.Fatalf("plugin event created: exit %d, stdout %q", exit, stdout)
		}
		jobs := jobsInState(t, cfgDir)
		if len(jobs) != 1 {
			t.Fatalf("tracked jobs = %v, want exactly J1", jobs)
		}
		if j := jobs["J1"]; j == nil || j.Projeto != "" || j.Estado != "running" || j.LastEventSeq != 1 {
			t.Fatalf("job J1 = %+v, want open, empty projeto, estado running, last_event_seq 1", j)
		}
		recs := readOutboxRecords(t, cfgDir)
		if len(recs) != 1 || recs[0].Tipo != "job_event" || recs[0].JobID == nil || *recs[0].JobID != "J1" {
			t.Fatalf("outbox records = %+v, want one J1 job_event", recs)
		}
		var sawEvents bool
		for _, c := range readFakeCalls(t, fakeDir) {
			if strings.Join(c.Argv, " ") == "job events --id J1 --since 0" {
				sawEvents = true
			}
		}
		if !sawEvents {
			t.Fatalf("the fake never received `job events --id J1 --since 0`")
		}
	})

	t.Run("flat shape", func(t *testing.T) {
		exe, _ := installFakeSoho(t, capsRule(capsJSON, 0), eventsRuleAnySince("J1"))
		cfgDir := t.TempDir()
		setSohoConfig(t, cfgDir, exe, "machine-a")
		_, _, exit := runPluginEvent(t, cfgDir, "workspace.created", `{"label":"job-J1","workspace_id":"ws-1"}`)
		if exit != 0 {
			t.Fatalf("plugin event created (flat): exit %d", exit)
		}
		if j := jobsInState(t, cfgDir)["J1"]; j == nil {
			t.Fatal("flat shape: J1 not tracked")
		}
		if n := len(readOutboxRecords(t, cfgDir)); n != 1 {
			t.Fatalf("flat shape: outbox records = %d, want 1", n)
		}
	})

	t.Run("closed tracked job final sync", func(t *testing.T) {
		exe, _ := installFakeSoho(t, capsRule(capsJSON, 0), eventsRuleAnySince("J1"))
		cfgDir := t.TempDir()
		setSohoConfig(t, cfgDir, exe, "machine-a")
		seedJob(t, cfgDir, "J1", "org/repo", 0, "accepted")
		stdout, _, exit := runPluginEvent(t, cfgDir, "workspace.closed", `{"workspace":{"label":"job-J1","workspace_id":"ws-1"}}`)
		if exit != 0 {
			t.Fatalf("plugin event closed: exit %d, stdout %q", exit, stdout)
		}
		if n := len(readOutboxRecords(t, cfgDir)); n != 1 {
			t.Fatalf("outbox records = %d, want 1 (the final sync event)", n)
		}
	})

	t.Run("closed untracked job ignored", func(t *testing.T) {
		exe, fakeDir := installFakeSoho(t, capsRule(capsJSON, 0))
		cfgDir := t.TempDir()
		setSohoConfig(t, cfgDir, exe, "machine-a")
		stdout, _, exit := runPluginEvent(t, cfgDir, "workspace.closed", `{"workspace":{"label":"job-J9","workspace_id":"ws-9"}}`)
		if exit != 0 {
			t.Fatalf("plugin event closed (untracked): exit %d, stdout %q", exit, stdout)
		}
		if stateDirExists(t, cfgDir) {
			t.Fatal("untracked close wrote state; want nothing written")
		}
		if calls := readFakeCalls(t, fakeDir); len(calls) != 0 {
			t.Fatalf("untracked close ran herdr-soho %v; want no calls", calls)
		}
	})

	t.Run("other workspace label ignored", func(t *testing.T) {
		exe, fakeDir := installFakeSoho(t, capsRule(capsJSON, 0))
		cfgDir := t.TempDir()
		setSohoConfig(t, cfgDir, exe, "machine-a")
		stdout, _, exit := runPluginEvent(t, cfgDir, "workspace.created", `{"workspace":{"label":"agent-x","workspace_id":"ws-x"}}`)
		if exit != 0 {
			t.Fatalf("plugin event (other label): exit %d, stdout %q", exit, stdout)
		}
		if stateDirExists(t, cfgDir) {
			t.Fatal("other label wrote state; want nothing written")
		}
		if calls := readFakeCalls(t, fakeDir); len(calls) != 0 {
			t.Fatalf("other label ran herdr-soho %v; want no calls", calls)
		}
	})

	t.Run("unknown shape friction", func(t *testing.T) {
		cfgDir := t.TempDir()
		stdout, _, exit := runPluginEvent(t, cfgDir, "workspace.created", "not json")
		if exit != 0 {
			t.Fatalf("plugin event (unknown shape): exit %d, stdout %q", exit, stdout)
		}
		friction, err := os.ReadFile(filepath.Join(outbox.StateDir(cfgDir), "friction.log"))
		if err != nil {
			t.Fatalf("friction.log: %v", err)
		}
		if !strings.Contains(string(friction), "event: unknown event JSON shape") {
			t.Fatalf("friction = %q, want the unknown-shape line", friction)
		}
	})

	t.Run("empty json friction", func(t *testing.T) {
		cfgDir := t.TempDir()
		stdout, _, exit := runPluginEvent(t, cfgDir, "workspace.created", "")
		if exit != 0 {
			t.Fatalf("plugin event (empty json): exit %d, stdout %q", exit, stdout)
		}
		friction, err := os.ReadFile(filepath.Join(outbox.StateDir(cfgDir), "friction.log"))
		if err != nil || !strings.Contains(string(friction), "event: unknown event JSON shape") {
			t.Fatalf("friction = %q (%v), want the unknown-shape line", friction, err)
		}
	})

	t.Run("other event name ignored", func(t *testing.T) {
		exe, fakeDir := installFakeSoho(t, capsRule(capsJSON, 0))
		cfgDir := t.TempDir()
		setSohoConfig(t, cfgDir, exe, "machine-a")
		stdout, _, exit := runPluginEvent(t, cfgDir, "workspace.updated", `{"workspace":{"label":"job-J1","workspace_id":"ws-1"}}`)
		if exit != 0 {
			t.Fatalf("plugin event (other event): exit %d, stdout %q", exit, stdout)
		}
		if stateDirExists(t, cfgDir) {
			t.Fatal("other event name wrote state; want nothing written")
		}
		if calls := readFakeCalls(t, fakeDir); len(calls) != 0 {
			t.Fatalf("other event name ran herdr-soho %v; want no calls", calls)
		}
	})

	t.Run("nowrite skipped", func(t *testing.T) {
		cfgDir := t.TempDir()
		var out, errb bytes.Buffer
		env := Env{
			Stdin:  strings.NewReader(""),
			Stdout: &out,
			Stderr: &errb,
			Getenv: func(k string) string {
				switch k {
				case nowriteVar:
					return "1"
				case plugin.EnvEvent:
					return "workspace.created"
				case plugin.EnvEventJSON:
					return `{"workspace":{"label":"job-J1","workspace_id":"ws-1"}}`
				}
				return ""
			},
			Environ:   func() []string { return fakeChildEnv },
			ConfigDir: cfgDir,
			Now:       pluginTestNow,
			Sleep:     sleepCtx,
		}
		exit := Run([]string{"plugin", "event"}, env)
		if exit != 0 {
			t.Fatalf("plugin event under NOWRITE: exit %d, want 0", exit)
		}
		if out.String() != `{"status":"skipped","motivo":"HERDR_HERMES_NOWRITE=1"}`+"\n" {
			t.Fatalf("plugin event under NOWRITE stdout = %q", out.String())
		}
		assertDirEmpty(t, cfgDir)
	})

	t.Run("created then closed keeps one job", func(t *testing.T) {
		exe, _ := installFakeSoho(t, capsRule(capsJSON, 0), eventsRuleAnySince("J1"))
		cfgDir := t.TempDir()
		setSohoConfig(t, cfgDir, exe, "machine-a")
		if _, _, exit := runPluginEvent(t, cfgDir, "workspace.created", `{"workspace":{"label":"job-J1","workspace_id":"ws-1"}}`); exit != 0 {
			t.Fatalf("created: exit %d", exit)
		}
		if _, _, exit := runPluginEvent(t, cfgDir, "workspace.closed", `{"workspace":{"label":"job-J1","workspace_id":"ws-1"}}`); exit != 0 {
			t.Fatalf("closed: exit %d", exit)
		}
		jobs := jobsInState(t, cfgDir)
		if len(jobs) != 1 || jobs["J1"] == nil {
			t.Fatalf("tracked jobs = %v, want exactly J1", jobs)
		}
		if n := len(readOutboxRecords(t, cfgDir)); n != 1 {
			t.Fatalf("outbox records = %d, want 1 (deduplicated)", n)
		}
	})
}

func TestPluginHooksStartup(t *testing.T) {
	t.Run("happy path", func(t *testing.T) {
		exe, _ := installFakeSoho(t, capsRule(capsJSON, 0), eventsRuleAnySince("J1"))
		cfgDir := t.TempDir()
		setSohoConfig(t, cfgDir, exe, "machine-a")
		seedJob(t, cfgDir, "J1", "org/repo", 0, "accepted")
		stdout, _, exit := runHermes(t, cfgDir, false, "", "plugin", "startup")
		if exit != 0 {
			t.Fatalf("plugin startup: exit %d, stdout %q", exit, stdout)
		}
		if stdout != `{"jobs":1,"novos":1,"enviados":0,"pendentes":1}`+"\n" {
			t.Fatalf("plugin startup stdout = %q", stdout)
		}
	})

	t.Run("herdr-soho missing always exit 0", func(t *testing.T) {
		cfgDir := t.TempDir()
		missing := filepath.Join(t.TempDir(), "no-such-herdr-soho")
		setSohoConfig(t, cfgDir, missing, "machine-a")
		seedJob(t, cfgDir, "J1", "org/repo", 0, "accepted")
		stdout, _, exit := runHermes(t, cfgDir, false, "", "plugin", "startup")
		if exit != 0 {
			t.Fatalf("plugin startup (missing soho): exit %d, stdout %q", exit, stdout)
		}
		if !strings.HasPrefix(strings.TrimSpace(stdout), `{"status":"skipped"`) {
			t.Fatalf("plugin startup stdout = %q, want the skipped line", stdout)
		}
		friction, err := os.ReadFile(filepath.Join(outbox.StateDir(cfgDir), "friction.log"))
		if err != nil || !strings.Contains(string(friction), "startup:") {
			t.Fatalf("friction = %q (%v), want a startup friction line", friction, err)
		}
	})

	t.Run("nowrite skipped", func(t *testing.T) {
		cfgDir := t.TempDir()
		stdout, _, exit := runHermes(t, cfgDir, true, "", "plugin", "startup")
		if exit != 0 {
			t.Fatalf("plugin startup under NOWRITE: exit %d, want 0", exit)
		}
		if stdout != `{"status":"skipped","motivo":"HERDR_HERMES_NOWRITE=1"}`+"\n" {
			t.Fatalf("plugin startup under NOWRITE stdout = %q", stdout)
		}
		assertDirEmpty(t, cfgDir)
	})
}

func TestPluginHooksBridgeStatus(t *testing.T) {
	t.Run("fresh", func(t *testing.T) {
		cfgDir := t.TempDir()
		stdout, _, exit := runHermes(t, cfgDir, false, "", "plugin", "bridge", "status")
		if exit != 0 {
			t.Fatalf("plugin bridge status: exit %d, stdout %q", exit, stdout)
		}
		if stdout != `{"jobs_abertos":0,"pendentes":0,"ultimo_push":null,"key_configured":false}`+"\n" {
			t.Fatalf("plugin bridge status stdout = %q", stdout)
		}
		assertDirEmpty(t, cfgDir) // read-only: creates nothing
	})

	t.Run("counts and last push", func(t *testing.T) {
		cfgDir := t.TempDir()
		seedJob(t, cfgDir, "J1", "org/repo", 0, "accepted") // open
		seedJob(t, cfgDir, "J2", "org/repo", 0, "done")     // terminal: not open
		s, err := outbox.Open(outbox.StateDir(cfgDir), outbox.Options{})
		if err != nil {
			t.Fatalf("open state: %v", err)
		}
		for i := 0; i < 3; i++ {
			if _, err := s.Append(outbox.Record{Tipo: "session", Maquina: "m1", Projeto: "org/repo", Dados: json.RawMessage(`{"acao":"start"}`)}); err != nil {
				t.Fatalf("append record %d: %v", i, err)
			}
		}
		if err := s.SetLastPush(outbox.PushResult{TS: "2026-01-02T03:04:05-03:00", Status: "ok", Code: 200}); err != nil {
			t.Fatalf("set last_push: %v", err)
		}
		if err := (&cred.FileStore{Path: filepath.Join(cfgDir, "credentials")}).Set("sk-test-bridge-status"); err != nil {
			t.Fatalf("store key: %v", err)
		}
		stdout, _, exit := runHermes(t, cfgDir, false, "", "plugin", "bridge", "status")
		if exit != 0 {
			t.Fatalf("plugin bridge status: exit %d, stdout %q", exit, stdout)
		}
		want := `{"jobs_abertos":1,"pendentes":3,"ultimo_push":{"ts":"2026-01-02T03:04:05-03:00","status":"ok","code":200},"key_configured":true}` + "\n"
		if stdout != want {
			t.Fatalf("plugin bridge status stdout = %q, want %q", stdout, want)
		}
	})
}

func TestPluginHooksBridgeSync(t *testing.T) {
	t.Run("counts then sync", func(t *testing.T) {
		exe, _ := installFakeSoho(t, capsRule(capsJSON, 0), eventsRuleAnySince("J1"))
		cfgDir := t.TempDir()
		setSohoConfig(t, cfgDir, exe, "machine-a")
		seedJob(t, cfgDir, "J1", "org/repo", 0, "accepted")
		stdout, stderr, exit := runHermes(t, cfgDir, false, "", "plugin", "bridge", "sync")
		if exit != 0 {
			t.Fatalf("plugin bridge sync: exit %d, stdout %q stderr %q", exit, stdout, stderr)
		}
		if stdout != `{"jobs":1,"novos":1,"enviados":0,"pendentes":1}`+"\n" {
			t.Fatalf("plugin bridge sync stdout = %q", stdout)
		}
		if !strings.Contains(stderr, "1 open") || !strings.Contains(stderr, "0 pending") {
			t.Fatalf("plugin bridge sync stderr = %q, want the open-job and pending-record counts", stderr)
		}
	})

	t.Run("auth missing exit 40", func(t *testing.T) {
		exe, _ := installFakeSoho(t, capsRule(capsJSON, 0), eventsRuleAnySince("J1"))
		cfgDir := t.TempDir()
		setSohoConfig(t, cfgDir, exe, "machine-a")
		setConfig(t, cfgDir, map[string]string{"dispatcher_url": "https://127.0.0.1:1"})
		seedJob(t, cfgDir, "J1", "org/repo", 0, "accepted")
		stdout, _, exit := runHermes(t, cfgDir, false, "", "plugin", "bridge", "sync")
		if exit != 40 {
			t.Fatalf("plugin bridge sync (no key): exit %d, want 40 (stdout %q)", exit, stdout)
		}
		if stdout != `{"jobs":1,"novos":1,"enviados":0,"pendentes":1,"status":"auth_missing"}`+"\n" {
			t.Fatalf("plugin bridge sync (no key) stdout = %q", stdout)
		}
	})

	t.Run("nowrite refused", func(t *testing.T) {
		cfgDir := t.TempDir()
		stdout, _, exit := runHermes(t, cfgDir, true, "", "plugin", "bridge", "sync")
		if exit != 2 {
			t.Fatalf("plugin bridge sync under NOWRITE: exit %d, want 2", exit)
		}
		if stdout != "{\"status\":\"nowrite\",\"motivo\":\"HERDR_HERMES_NOWRITE=1\"}\n" {
			t.Fatalf("plugin bridge sync under NOWRITE stdout = %q", stdout)
		}
		assertDirEmpty(t, cfgDir)
	})
}

func TestPluginBadUsage(t *testing.T) {
	for _, args := range [][]string{
		{"plugin"},
		{"plugin", "bogus"},
		{"plugin", "startup", "extra"},
		{"plugin", "event", "extra"},
		{"plugin", "bridge"},
		{"plugin", "bridge", "bogus"},
		{"plugin", "bridge", "status", "extra"},
		{"plugin", "bridge", "sync", "extra"},
	} {
		dir := t.TempDir()
		stdout, stderr, exit := runCLI(t, dir, false, args...)
		if exit != 2 {
			t.Fatalf("%v: exit %d, want 2", args, exit)
		}
		if !strings.Contains(stderr, "usage:") {
			t.Errorf("%v: stderr %q has no usage text", args, stderr)
		}
		if !strings.HasPrefix(stdout, `{"status":"2"`) {
			t.Errorf("%v: stdout %q, want the error line with status 2", args, stdout)
		}
	}
}
