package cli

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/djalmajr/herdr-hermes/internal/outbox"
)

// seedCLIOutbox appends n dispatch records to the store under dir.
func seedCLIOutbox(t *testing.T, dir string, n int) *outbox.Store {
	t.Helper()
	s, err := outbox.Open(outbox.StateDir(dir), outbox.Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 1; i <= n; i++ {
		if _, err := s.Append(outbox.Record{
			Tipo: outbox.TipoDispatch, Maquina: "machine-a", Projeto: "org/repo",
			Dados: []byte(`{"i":` + itoaCLIOutbox(i) + `}`),
		}); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	return s
}

func itoaCLIOutbox(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for x := n; x > 0; x /= 10 {
		b = append([]byte{byte('0' + x%10)}, b...)
	}
	return string(b)
}

// TestPushCmd covers the push command exit codes and output: 0 with the
// counts (status omitted), 40 auth_missing, 41 auth_rejected, 42
// unreachable (with retries through the injected sleep) and push_disabled.
func TestPushCmd(t *testing.T) {
	// push disabled (no dispatcher_url): exit 0, status push_disabled.
	dir := t.TempDir()
	setConfig(t, dir, map[string]string{"machine_label": "m1"})
	seedCLIOutbox(t, dir, 2)
	stdout, stderr, exit := runCLIStdin(t, dir, "", nil, "push")
	if exit != 0 {
		t.Fatalf("push exit = %d, stderr %q", exit, stderr)
	}
	if stdout != `{"enviados":0,"pendentes":2,"status":"push_disabled"}
` {
		t.Fatalf("push disabled stdout = %q", stdout)
	}

	// No key: exit 40 auth_missing.
	dir2 := t.TempDir()
	setConfig(t, dir2, map[string]string{"machine_label": "m1", "dispatcher_url": "http://127.0.0.1:1"})
	seedCLIOutbox(t, dir2, 2)
	stdout, stderr, exit = runCLIStdin(t, dir2, "", nil, "push")
	if exit != 40 {
		t.Fatalf("push without key exit = %d, want 40 (stderr %q)", exit, stderr)
	}
	if stdout != `{"enviados":0,"pendentes":2,"status":"auth_missing"}
` {
		t.Fatalf("push without key stdout = %q", stdout)
	}

	// Key stored, dispatcher 200: exit 0, no status member.
	loginAssert(t, dir2)
	srv := newCLIPushServer(t, 200)
	setConfig(t, dir2, map[string]string{"dispatcher_url": srv.URL})
	stdout, stderr, exit = runCLIStdin(t, dir2, "", nil, "push")
	if exit != 0 {
		t.Fatalf("push ok exit = %d, stderr %q", exit, stderr)
	}
	if stdout != `{"enviados":2,"pendentes":0}
` {
		t.Fatalf("push ok stdout = %q, want the counts without a status", stdout)
	}
	if n := srv.count(); n != 2 {
		t.Fatalf("dispatcher saw %d requests, want 2", n)
	}

	// Dispatcher 401: exit 41 auth_rejected, nothing delivered.
	srv401 := newCLIPushServer(t, 401)
	dir3 := t.TempDir()
	setConfig(t, dir3, map[string]string{"machine_label": "m1", "dispatcher_url": srv401.URL})
	loginAssert(t, dir3)
	seedCLIOutbox(t, dir3, 1)
	stdout, stderr, exit = runCLIStdin(t, dir3, "", nil, "push")
	if exit != 41 {
		t.Fatalf("push 401 exit = %d, want 41 (stderr %q)", exit, stderr)
	}
	if stdout != `{"enviados":0,"pendentes":1,"status":"auth_rejected"}
` {
		t.Fatalf("push 401 stdout = %q", stdout)
	}

	// Dispatcher 500: exit 42 unreachable, exactly 4 attempts (1 + 3
	// retries) through the injected (instant) sleep.
	srv500 := newCLIPushServer(t, 500)
	dir4 := t.TempDir()
	setConfig(t, dir4, map[string]string{"machine_label": "m1", "dispatcher_url": srv500.URL})
	loginAssert(t, dir4)
	seedCLIOutbox(t, dir4, 1)
	var sleeps int
	stdout, stderr, exit = runCLIStdinSleep(t, dir4, "", nil, func(_ context.Context, _ time.Duration) error {
		sleeps++
		return nil
	}, "push")
	if exit != 42 {
		t.Fatalf("push 500 exit = %d, want 42 (stderr %q)", exit, stderr)
	}
	if stdout != `{"enviados":0,"pendentes":1,"status":"unreachable"}
` {
		t.Fatalf("push 500 stdout = %q", stdout)
	}
	if n := srv500.count(); n != 4 {
		t.Fatalf("dispatcher saw %d requests, want 4 (1 + 3 retries)", n)
	}
	if sleeps != 3 {
		t.Fatalf("backoff slept %d times, want exactly 3 (10s/30s/90s injected)", sleeps)
	}
	// The friction line on the stop carries the status code and the
	// outcome, never the key.
	data, err := os.ReadFile(dir4 + "/state/friction.log")
	if err != nil {
		t.Fatalf("friction.log: %v", err)
	}
	if strings.Count(string(data), "\n") != 1 || !strings.Contains(string(data), "status 500 outcome unreachable") {
		t.Fatalf("friction.log = %q, want one line with the status code and outcome", data)
	}
	if strings.Contains(string(data), sentinelKey) {
		t.Fatalf("friction.log leaked the key: %q", data)
	}
	// cursor.json last_push records the outcome; the outbox pull still
	// shows the pending record.
	data, err = os.ReadFile(dir4 + "/state/cursor.json")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"status":"unreachable"`) || !strings.Contains(string(data), `"code":500`) {
		t.Fatalf("cursor.json = %q, want last_push unreachable/500", data)
	}
	stdout, stderr, exit = runCLIStdin(t, dir4, "", nil, "outbox")
	if exit != 0 || !strings.Contains(stdout, `"tipo":"dispatch"`) || !strings.Contains(stdout, `"entregue_seq":0`) {
		t.Fatalf("outbox after a failed push: exit %d stdout %q", exit, stdout)
	}
	// A bad URL the client refuses: exit 42 rejected.
	dir5 := t.TempDir()
	setConfig(t, dir5, map[string]string{"machine_label": "m1", "dispatcher_url": "http://example.invalid/x"})
	loginAssert(t, dir5)
	seedCLIOutbox(t, dir5, 1)
	stdout, stderr, exit = runCLIStdin(t, dir5, "", nil, "push")
	if exit != 42 || stdout != `{"enviados":0,"pendentes":1,"status":"rejected"}
` {
		t.Fatalf("push with a refused URL: exit %d stdout %q stderr %q", exit, stdout, stderr)
	}
	// Extra args are bad usage.
	if _, _, exit := runCLIStdin(t, dir, "", nil, "push", "extra"); exit != 2 {
		t.Errorf("push with extra args: exit %d, want 2", exit)
	}
}

// loginAssert stores the sentinel key in dir and fails the test when the
// login is not the configured success line.
func loginAssert(t *testing.T, dir string) {
	t.Helper()
	out, errOut, exit := runCLIStdin(t, dir, sentinelKey+"\n", nil, "auth", "login", "--key", "-")
	if exit != 0 || out != `{"configured":true,"store":"file"}
` {
		t.Fatalf("auth login in push test: exit %d stdout %q stderr %q", exit, out, errOut)
	}
}
