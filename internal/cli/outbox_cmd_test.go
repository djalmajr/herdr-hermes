package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/djalmajr/herdr-hermes/internal/outbox"
)

// seedOutbox writes n dispatch records into the state directory and returns
// the store.
func seedOutbox(t *testing.T, configDir string, n int) *outbox.Store {
	t.Helper()
	s, err := outbox.Open(outbox.StateDir(configDir), outbox.Options{})
	if err != nil {
		t.Fatalf("seed Open: %v", err)
	}
	for i := 0; i < n; i++ {
		if _, err := s.Append(outbox.Record{
			Tipo: outbox.TipoDispatch, Maquina: "machine-a", Projeto: "org/repo",
			Dados: json.RawMessage(`{"i":` + itoa(i) + `}`),
		}); err != nil {
			t.Fatalf("seed Append: %v", err)
		}
	}
	return s
}

func TestOutboxCmd(t *testing.T) {
	dir := t.TempDir()
	seedOutbox(t, dir, 3)
	stdout, _, exit := runCLI(t, dir, false, "outbox")
	if exit != 0 {
		t.Fatalf("outbox exit = %d", exit)
	}
	lines := strings.Split(strings.TrimSuffix(stdout, "\n"), "\n")
	if len(lines) != 4 {
		t.Fatalf("outbox printed %d lines:\n%s", len(lines), stdout)
	}
	var first outbox.Record
	if err := json.Unmarshal([]byte(lines[0]), &first); err != nil {
		t.Fatalf("first line %q: %v", lines[0], err)
	}
	if first.Seq != 1 || first.Schema != 1 {
		t.Errorf("first record = %+v", first)
	}
	if !strings.Contains(lines[0], `"idempotency_key":"machine-a:1"`) {
		t.Errorf("first line = %q", lines[0])
	}
	if lines[3] != `{"outbox":"fim","ultimo_seq":3,"entregue_seq":0}` {
		t.Errorf("trailer = %q", lines[3])
	}

	// --since filters; beyond the last seq the trailer is printed alone.
	stdout, _, exit = runCLI(t, dir, false, "outbox", "--since", "2")
	if exit != 0 {
		t.Fatalf("outbox --since 2 exit = %d", exit)
	}
	lines = strings.Split(strings.TrimSuffix(stdout, "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("outbox --since 2 lines = %v", lines)
	}
	if !strings.HasPrefix(lines[0], `{"schema":1,"seq":3,`) {
		t.Errorf("line after since = %q", lines[0])
	}
	if lines[1] != `{"outbox":"fim","ultimo_seq":3,"entregue_seq":0}` {
		t.Errorf("trailer = %q", lines[1])
	}
	stdout, _, exit = runCLI(t, dir, false, "outbox", "--since", "100")
	if exit != 0 {
		t.Fatalf("outbox --since 100 exit = %d", exit)
	}
	if stdout != `{"outbox":"fim","ultimo_seq":3,"entregue_seq":0}
` {
		t.Errorf("outbox --since 100 = %q, want trailer only", stdout)
	}

	// entregue_seq follows the cursor.
	s, _ := outbox.Open(outbox.StateDir(dir), outbox.Options{})
	if err := s.SetDeliveredSeq(2); err != nil {
		t.Fatalf("SetDeliveredSeq: %v", err)
	}
	stdout, _, exit = runCLI(t, dir, false, "outbox")
	if exit != 0 {
		t.Fatalf("outbox exit = %d", exit)
	}
	if lines := strings.Split(strings.TrimSuffix(stdout, "\n"), "\n"); lines[len(lines)-1] != `{"outbox":"fim","ultimo_seq":3,"entregue_seq":2}` {
		t.Errorf("trailer with cursor = %q", stdout)
	}

	// Invalid flags exit 2.
	for _, args := range [][]string{
		{"--wait", "600001"},
		{"--wait", "-1"},
		{"--wait", "abc"},
		{"--since", "-1"},
		{"--since", "abc"},
		{"--since"},
		{"--bogus"},
	} {
		stdout, stderr, exit := runCLI(t, dir, false, append([]string{"outbox"}, args...)...)
		if exit != 2 || !strings.Contains(stderr, "commands:") || !strings.Contains(stdout, `"status":"2"`) {
			t.Errorf("outbox %v: exit %d, stderr %q, stdout %q", args, exit, stderr, stdout)
		}
	}
}

// TestOutboxCmdLongPoll wires Env.Sleep into the long-poll: with nothing new
// and --wait set, the command sleeps through the bound via the injected
// clock (the total requested sleep never exceeds the bound) and ends with
// the trailer.
func TestOutboxCmdLongPoll(t *testing.T) {
	dir := t.TempDir()
	seedOutbox(t, dir, 1)
	var out bytes.Buffer
	var sleeps []time.Duration
	env := Env{
		Stdin:     strings.NewReader(""),
		Stdout:    &out,
		Stderr:    &bytes.Buffer{},
		Getenv:    getenvFor(false),
		ConfigDir: dir,
		Now:       func() time.Time { return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC) },
		Sleep: func(_ context.Context, d time.Duration) error {
			sleeps = append(sleeps, d)
			return nil
		},
	}
	exit := Run([]string{"outbox", "--since", "1", "--wait", "400"}, env)
	if exit != 0 {
		t.Fatalf("outbox --wait exit = %d", exit)
	}
	want := "{\"outbox\":\"fim\",\"ultimo_seq\":1,\"entregue_seq\":0}\n"
	if out.String() != want {
		t.Errorf("outbox --wait stdout = %q, want %q", out.String(), want)
	}
	if len(sleeps) == 0 {
		t.Fatal("the long-poll did not use Env.Sleep")
	}
	var total time.Duration
	for _, d := range sleeps {
		total += d
		if d > 200*time.Millisecond {
			t.Errorf("single sleep %v exceeds the poll interval", d)
		}
	}
	if total != 400*time.Millisecond {
		t.Errorf("total sleep = %v, want exactly the 400ms bound (no overshoot)", total)
	}
}
