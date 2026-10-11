package registry

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

// djaP3NodeCapAssert checks the whole refusal of the MaxNodes+1-th
// machine: the ErrInvalid refusal with its exact text, the node count
// unchanged at MaxNodes, and exactly one alert line carrying the counts
// only, never the machine label.
func djaP3NodeCapAssert(t *testing.T, s *Store, path string, err error, lines *[]string, maquina string) {
	t.Helper()
	if err == nil {
		t.Fatalf("ApplyPoll %s of the %d+1-th machine = nil, want the nodes refusal", path, MaxNodes)
	}
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("ApplyPoll %s error = %v, want the ErrInvalid refusal", path, err)
	}
	if err.Error() != "registry: invalid input: nodes limit reached" {
		t.Fatalf("ApplyPoll %s error = %q, want the exact refusal text", path, err.Error())
	}
	var n int
	if scanErr := s.db.QueryRow(`SELECT COUNT(*) FROM nodes`).Scan(&n); scanErr != nil {
		t.Fatalf("count nodes: %v", scanErr)
	}
	if n != MaxNodes {
		t.Fatalf("nodes = %d, want %d", n, MaxNodes)
	}
	if len(*lines) != 1 {
		t.Fatalf("ApplyPoll %s alert lines = %d (%v), want exactly one", path, len(*lines), *lines)
	}
	want := fmt.Sprintf("registry: capacity: nodes limit reached (stored total %d, limit %d)", MaxNodes, MaxNodes)
	if (*lines)[0] != want {
		t.Fatalf("ApplyPoll %s alert line = %q, want %q", path, (*lines)[0], want)
	}
	if strings.Contains((*lines)[0], maquina) {
		t.Fatalf("ApplyPoll %s alert line = %q, must not carry the machine label", path, (*lines)[0])
	}
}

// TestDJA229P3NodeAlertDatabaseErrorOk: a database error of admitNewNode
// on the ok path (the nodes table missing) is returned to the caller and
// writes no capacity alert line: only the real nodes-cap refusal alerts.
func TestDJA229P3NodeAlertDatabaseErrorOk(t *testing.T) {
	s := retStore(t, Options{})
	if _, err := s.db.Exec(`DROP TABLE nodes`); err != nil {
		t.Fatalf("drop the nodes table: %v", err)
	}
	lines := setLogf(t)
	err := s.ApplyPoll(okPoll("node-a", ""))
	if err == nil {
		t.Fatal("ApplyPoll of the ok poll = nil, want the database error")
	}
	if errors.Is(err, ErrInvalid) {
		t.Fatalf("ApplyPoll error = %v, a database error is not the ErrInvalid cap", err)
	}
	if len(*lines) != 0 {
		t.Fatalf("alert lines on a database error = %v, want none: only the nodes cap alerts", *lines)
	}
}

// TestDJA229P3NodeAlertDatabaseErrorFailed: the failed-poll path is the
// same: a database error of admitNewNode is returned to the caller and
// writes no capacity alert line.
func TestDJA229P3NodeAlertDatabaseErrorFailed(t *testing.T) {
	s := retStore(t, Options{})
	if _, err := s.db.Exec(`DROP TABLE nodes`); err != nil {
		t.Fatalf("drop the nodes table: %v", err)
	}
	lines := setLogf(t)
	err := s.ApplyPoll(PollResult{Maquina: "node-a", At: fixedNow, OK: false, Motivo: "probe_failed"})
	if err == nil {
		t.Fatal("ApplyPoll of the failed poll = nil, want the database error")
	}
	if errors.Is(err, ErrInvalid) {
		t.Fatalf("ApplyPoll error = %v, a database error is not the ErrInvalid cap", err)
	}
	if len(*lines) != 0 {
		t.Fatalf("alert lines on a database error = %v, want none: only the nodes cap alerts", *lines)
	}
}

// TestDJA229P3NodeAlertPositiveControl: at the cap, the ok and the
// failed poll of a known node still apply with no alert, and the
// MaxNodes+1-th machine is refused on both paths with the exact refusal
// text, the count unchanged and one counts-only alert line each.
func TestDJA229P3NodeAlertPositiveControl(t *testing.T) {
	s := retStore(t, Options{})
	for i := 0; i < MaxNodes; i++ {
		if err := s.SetNode(NodeUpdate{Maquina: fmt.Sprintf("n%02d", i)}); err != nil {
			t.Fatalf("SetNode %d: %v", i, err)
		}
	}
	lines := setLogf(t)
	mustApplyPoll(t, s, okPoll("n00", "",
		PolledWorkspace{ID: "w1", Label: "hm-example-impl-c1", AgentStatus: "working"}))
	mustApplyPoll(t, s, PollResult{Maquina: "n00", At: fixedNow, OK: false, Motivo: "probe_failed"})
	if len(*lines) != 0 {
		t.Fatalf("alert lines for the known node at the cap = %v, want none", *lines)
	}
	lines = setLogf(t)
	err := s.ApplyPoll(okPoll("node-x", "",
		PolledWorkspace{ID: "w1", Label: "hm-example-impl-c1", AgentStatus: "working"}))
	djaP3NodeCapAssert(t, s, "ok", err, lines, "node-x")
	lines = setLogf(t)
	err = s.ApplyPoll(PollResult{Maquina: "node-x", At: fixedNow, OK: false, Motivo: "probe_failed"})
	djaP3NodeCapAssert(t, s, "failed", err, lines, "node-x")
}
