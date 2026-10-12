package registry

import (
	"context"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/djalmajr/nau/internal/router"
)

// djaP3ErrWorkspaceList and djaP3ErrAgentList are the exact refusal
// texts of the raw UTF-8 guard of the two parsers.
const (
	djaP3ErrWorkspaceList = "registry: workspace list is not valid UTF-8"
	djaP3ErrAgentList     = "registry: agent list is not valid UTF-8"
)

// djaP3Later is a fixed instant after pollTestClock, in the same
// explicit-offset zone, for the failed poll applied to the seeded store.
var djaP3Later = pollTestClock.Add(60 * time.Second)

// djaP3Byte renders one raw byte for injection into the fixtures: the
// conversion must keep the byte, not encode it as a UTF-8 code point.
func djaP3Byte(b byte) string { return string([]byte{b}) }

// djaP3WSRow builds one workspace element; extra carries an unknown
// key or nested object after agent_status.
func djaP3WSRow(id, label, status, extra string) string {
	return `{"workspace_id":"` + id + `","label":"` + label + `","agent_status":"` + status + `"` + extra + `}`
}

// djaP3WSRaw wraps the workspace elements in the raw answer shape.
func djaP3WSRaw(rows string) string {
	return `{"id":"1","result":{"type":"workspace_list","workspaces":[` + rows + `]}}`
}

// djaP3AgentRow builds one agent element; extra carries an unknown key
// or nested object after agent_status.
func djaP3AgentRow(pane, name, ws, status, extra string) string {
	return `{"pane_id":"` + pane + `","name":"` + name + `","workspace_id":"` + ws + `","agent_status":"` + status + `"` + extra + `}`
}

// djaP3AgentRaw wraps the agent elements in the raw answer shape.
func djaP3AgentRaw(rows string) string {
	return `{"id":"1","result":{"agents":[` + rows + `]}}`
}

// djaP3AssertRefused checks the whole-answer refusal: the error is the
// exact decided text and the result is nil.
func djaP3AssertRefused(t *testing.T, name string, raw string, parse func([]byte) (int, error), wantErr string) {
	t.Helper()
	n, err := parse([]byte(raw))
	if err == nil {
		t.Fatalf("%s accepted the raw answer with invalid bytes: %d rows", name, n)
	}
	if err.Error() != wantErr {
		t.Fatalf("%s error = %q, want %q", name, err.Error(), wantErr)
	}
	if n != 0 {
		t.Fatalf("%s result = %d rows, want nil", name, n)
	}
}

// TestDJA229P3PollUTF8RefusedPositions: an invalid byte 0xff or 0xfe at
// each position of the raw answer refuses the whole answer with the
// exact text: an identity, a name, a status, an unknown key name, the
// value of an unknown nested key.
func TestDJA229P3PollUTF8RefusedPositions(t *testing.T) {
	for _, b := range []byte{0xff, 0xfe} {
		byteName := hex.EncodeToString([]byte{b})
		t.Run("workspace list "+byteName, func(t *testing.T) {
			cases := []struct {
				name string
				raw  string
			}{
				{"workspace_id", djaP3WSRaw(djaP3WSRow("w"+djaP3Byte(b)+"1", "l1", "idle", ""))},
				{"label", djaP3WSRaw(djaP3WSRow("w1", "l"+djaP3Byte(b), "idle", ""))},
				{"agent_status", djaP3WSRaw(djaP3WSRow("w1", "l1", "i"+djaP3Byte(b)+"dle", ""))},
				{"unknown key name", djaP3WSRaw(djaP3WSRow("w1", "l1", "idle", `,"x`+djaP3Byte(b)+`y":"z"`))},
				{"unknown nested value", djaP3WSRaw(djaP3WSRow("w1", "l1", "idle", `,"meta":{"cwd":"c`+djaP3Byte(b)+`1"}`))},
			}
			for _, c := range cases {
				t.Run(c.name, func(t *testing.T) {
					djaP3AssertRefused(t, "ParseWorkspaceList "+c.name, c.raw,
						func(raw []byte) (int, error) { ws, err := ParseWorkspaceList(raw); return len(ws), err },
						djaP3ErrWorkspaceList)
				})
			}
		})
		t.Run("agent list "+byteName, func(t *testing.T) {
			cases := []struct {
				name string
				raw  string
			}{
				{"pane_id", djaP3AgentRaw(djaP3AgentRow("p"+djaP3Byte(b)+"1", "n1", "w1", "blocked", ""))},
				{"name", djaP3AgentRaw(djaP3AgentRow("p1", "n"+djaP3Byte(b)+"1", "w1", "blocked", ""))},
				{"workspace_id", djaP3AgentRaw(djaP3AgentRow("p1", "n1", "w"+djaP3Byte(b)+"1", "blocked", ""))},
				{"agent_status", djaP3AgentRaw(djaP3AgentRow("p1", "n1", "w1", "b"+djaP3Byte(b)+"locked", ""))},
				{"unknown key name", djaP3AgentRaw(djaP3AgentRow("p1", "n1", "w1", "blocked", `,"x`+djaP3Byte(b)+`y":"z"`))},
				{"unknown nested value", djaP3AgentRaw(djaP3AgentRow("p1", "n1", "w1", "blocked", `,"meta":{"cwd":"c`+djaP3Byte(b)+`1"}`))},
			}
			for _, c := range cases {
				t.Run(c.name, func(t *testing.T) {
					djaP3AssertRefused(t, "ParseAgentList "+c.name, c.raw,
						func(raw []byte) (int, error) { panes, err := ParseAgentList(raw); return len(panes), err },
						djaP3ErrAgentList)
				})
			}
		})
	}
}

// TestDJA229P3PollUTF8RefusedMixedRows: three rows where only one carries
// the invalid byte, as the first row and as the last row, refuse the
// whole answer for both parsers.
func TestDJA229P3PollUTF8RefusedMixedRows(t *testing.T) {
	validWS := []string{
		`{"workspace_id":"w1","label":"l1","agent_status":"idle"}`,
		`{"workspace_id":"w2","label":"l2","agent_status":"working"}`,
		`{"workspace_id":"w3","label":"l3","agent_status":"blocked"}`,
	}
	validAG := []string{
		`{"pane_id":"p1","name":"n1","workspace_id":"w1","agent_status":"idle"}`,
		`{"pane_id":"p2","name":"n2","workspace_id":"w2","agent_status":"working"}`,
		`{"pane_id":"p3","name":"n3","workspace_id":"w3","agent_status":"blocked"}`,
	}
	for _, b := range []byte{0xff, 0xfe} {
		byteName := hex.EncodeToString([]byte{b})
		badWS := djaP3WSRow("w"+djaP3Byte(b), "lx", "idle", "")
		badAG := djaP3AgentRow("p"+djaP3Byte(b), "nx", "w1", "idle", "")
		t.Run("workspace list "+byteName, func(t *testing.T) {
			for _, c := range []struct {
				name string
				rows []string
			}{
				{"first row", []string{badWS, validWS[1], validWS[2]}},
				{"last row", []string{validWS[0], validWS[1], badWS}},
			} {
				t.Run(c.name, func(t *testing.T) {
					djaP3AssertRefused(t, "ParseWorkspaceList mixed", djaP3WSRaw(strings.Join(c.rows, ",")),
						func(raw []byte) (int, error) { ws, err := ParseWorkspaceList(raw); return len(ws), err },
						djaP3ErrWorkspaceList)
				})
			}
		})
		t.Run("agent list "+byteName, func(t *testing.T) {
			for _, c := range []struct {
				name string
				rows []string
			}{
				{"first row", []string{badAG, validAG[1], validAG[2]}},
				{"last row", []string{validAG[0], validAG[1], badAG}},
			} {
				t.Run(c.name, func(t *testing.T) {
					djaP3AssertRefused(t, "ParseAgentList mixed", djaP3AgentRaw(strings.Join(c.rows, ",")),
						func(raw []byte) (int, error) { panes, err := ParseAgentList(raw); return len(panes), err },
						djaP3ErrAgentList)
				})
			}
		})
	}
}

// djaP3AssertFailedPoll checks the failed poll of a machine whose raw
// answer carried invalid bytes: OK false, the probe_malformed reason, no
// lists, and the agent list run or not run as expected.
func djaP3AssertFailedPoll(t *testing.T, res PollResult, fe *fakeExec, agentRan bool) {
	t.Helper()
	if res.OK {
		t.Fatalf("Poll OK = true with the invalid raw answer, want the failed poll (%q)", res.Motivo)
	}
	if res.Motivo != router.ReasonProbeMalformed {
		t.Fatalf("Motivo = %q, want %q", res.Motivo, router.ReasonProbeMalformed)
	}
	if res.Workspaces != nil || res.Panes != nil {
		t.Fatalf("lists of the failed poll = %v / %v, want nil", res.Workspaces, res.Panes)
	}
	ran := false
	for _, argv := range fe.argvs() {
		if isArgv(argv, "agent", "list") {
			ran = true
		}
	}
	if ran != agentRan {
		t.Fatalf("agent list ran = %v, want %v (calls: %v)", ran, agentRan, fe.argvs())
	}
}

// TestDJA229P3PollUTF8PollerRefusal: a raw answer with an invalid byte
// fails the whole poll of the machine with probe_malformed; when the
// workspace list is invalid the agent list is not run at all.
func TestDJA229P3PollUTF8PollerRefusal(t *testing.T) {
	bad := byte(0xff)
	wsBad := djaP3WSRaw(djaP3WSRow("w"+djaP3Byte(bad), "l1", "idle", ""))
	agBad := djaP3AgentRaw(djaP3AgentRow("p"+djaP3Byte(bad), "n1", "w1", "blocked", ""))
	t.Run("invalid workspace list", func(t *testing.T) {
		fe := newFakeExec(func(argv []string) ([]byte, int, error) {
			switch {
			case isArgv(argv, "workspace", "list"):
				return []byte(wsBad), 0, nil
			case isArgv(argv, "agent", "list"):
				t.Errorf("agent list was run although the workspace list was refused")
				return []byte(agentListSample), 0, nil
			case isArgv(argv, "status", "server"):
				return []byte(statusServerSample), 0, nil
			}
			t.Errorf("unexpected argv %v", argv)
			return nil, 1, nil
		})
		p := Poller{Exec: fe.Run, Now: func() time.Time { return djaP3Later }}
		res := p.Poll(context.Background(), "node-a")
		djaP3AssertFailedPoll(t, res, fe, false)
	})
	t.Run("invalid agent list", func(t *testing.T) {
		fe := newFakeExec(func(argv []string) ([]byte, int, error) {
			switch {
			case isArgv(argv, "workspace", "list"):
				return []byte(wsListSample), 0, nil
			case isArgv(argv, "agent", "list"):
				return []byte(agBad), 0, nil
			case isArgv(argv, "status", "server"):
				return []byte(statusServerSample), 0, nil
			}
			t.Errorf("unexpected argv %v", argv)
			return nil, 1, nil
		})
		p := Poller{Exec: fe.Run, Now: func() time.Time { return djaP3Later }}
		res := p.Poll(context.Background(), "node-a")
		djaP3AssertFailedPoll(t, res, fe, true)
	})
}

// TestDJA229P3PollUTF8FailedPollApply: the failed poll of a machine whose
// raw answer carried invalid bytes applies the failed-poll behavior on
// the seeded store: the workspace row and the blocked pane wait stay,
// the node status is probe_malformed, the contact does not move and no
// new workspace or wait row appears.
func TestDJA229P3PollUTF8FailedPollApply(t *testing.T) {
	bad := byte(0xff)
	wsBad := djaP3WSRaw(djaP3WSRow("w"+djaP3Byte(bad), "l1", "idle", ""))
	fe := newFakeExec(func(argv []string) ([]byte, int, error) {
		switch {
		case isArgv(argv, "workspace", "list"):
			return []byte(wsBad), 0, nil
		case isArgv(argv, "agent", "list"):
			t.Errorf("agent list was run although the workspace list was refused")
			return []byte(agentListSample), 0, nil
		case isArgv(argv, "status", "server"):
			return []byte(statusServerSample), 0, nil
		}
		t.Errorf("unexpected argv %v", argv)
		return nil, 1, nil
	})
	p := Poller{Exec: fe.Run, Now: func() time.Time { return djaP3Later }}
	res := p.Poll(context.Background(), "node-a")
	djaP3AssertFailedPoll(t, res, fe, false)

	s := openStore(t)
	seed := okPoll("node-a", "1.2.3",
		PolledWorkspace{ID: "w1", Label: "hm-example-impl-x1", AgentStatus: "working"})
	seed.Panes = []PolledPane{{PaneID: "p1", WorkspaceID: "w1", Name: "orchestrator", AgentStatus: "blocked"}}
	mustApplyPoll(t, s, seed)
	if f := jobFact(t, s, "node-a", "ws:w1"); !f.exists {
		t.Fatalf("seed job row = %+v, want the hm workspace row", f)
	}
	exists, _, _ := waitFact(t, s, "node-a", "p1")
	if !exists {
		t.Fatal("the seeded blocked pane wait is missing")
	}
	var contact string
	if err := s.db.QueryRow(`SELECT ultimo_contato FROM nodes WHERE maquina = 'node-a'`).Scan(&contact); err != nil {
		t.Fatalf("read ultimo_contato: %v", err)
	}
	if err := s.ApplyPoll(res); err != nil {
		t.Fatalf("ApplyPoll of the failed poll: %v", err)
	}
	var status string
	if err := s.db.QueryRow(`SELECT poll_status FROM nodes WHERE maquina = 'node-a'`).Scan(&status); err != nil {
		t.Fatalf("read poll_status: %v", err)
	}
	if status != router.ReasonProbeMalformed {
		t.Errorf("poll_status = %q, want %q", status, router.ReasonProbeMalformed)
	}
	var contactAfter string
	if err := s.db.QueryRow(`SELECT ultimo_contato FROM nodes WHERE maquina = 'node-a'`).Scan(&contactAfter); err != nil {
		t.Fatalf("read ultimo_contato after: %v", err)
	}
	if contactAfter != contact {
		t.Errorf("ultimo_contato = %q, want it unchanged (%q): the failed poll must not move the contact", contactAfter, contact)
	}
	if f := jobFact(t, s, "node-a", "ws:w1"); !f.exists {
		t.Fatalf("workspace row after the failed poll = %+v, want it kept", f)
	}
	exists, _, _ = waitFact(t, s, "node-a", "p1")
	if !exists {
		t.Error("the blocked pane wait was lost by the failed poll")
	}
	var jobs, waits int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM jobs WHERE maquina = 'node-a'`).Scan(&jobs); err != nil {
		t.Fatalf("count jobs: %v", err)
	}
	if jobs != 1 {
		t.Errorf("job rows of node-a = %d, want 1: no new workspace row", jobs)
	}
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM waits WHERE maquina = 'node-a'`).Scan(&waits); err != nil {
		t.Fatalf("count waits: %v", err)
	}
	if waits != 1 {
		t.Errorf("wait rows of node-a = %d, want 1: no new wait row", waits)
	}
}

// TestDJA229P3PollUTF8Accepted: the raw-byte guard changes nothing for
// valid answers: multibyte identities as raw UTF-8 and as \u escapes,
// the escaped and the literal U+FFFD, the ASCII control escape, the HTML
// characters and the bounded long label all decode to the same values as
// before the change.
func TestDJA229P3PollUTF8Accepted(t *testing.T) {
	t.Run("multibyte raw and escapes", func(t *testing.T) {
		raw := djaP3WSRaw(
			`{"workspace_id":"ação","label":"日本-café-😀","agent_status":"idle"},` +
				`{"workspace_id":"a\u00e7\u00e3o","label":"\u65e5\u672c-c\u0061f\u00e9-\ud83d\ude00","agent_status":"working"}`)
		ws, err := ParseWorkspaceList([]byte(raw))
		if err != nil {
			t.Fatalf("ParseWorkspaceList of the valid multibyte answer: %v", err)
		}
		if len(ws) != 2 {
			t.Fatalf("workspaces = %d, want 2", len(ws))
		}
		if ws[0].ID != "ação" || ws[0].Label != "日本-café-😀" || ws[0].AgentStatus != "idle" {
			t.Fatalf("raw row = %+v, want the decoded multibyte values", ws[0])
		}
		if ws[1].ID != ws[0].ID || ws[1].Label != ws[0].Label || ws[1].AgentStatus != "working" {
			t.Fatalf("escape row = %+v, want the same values as the raw row", ws[1])
		}
		agRaw := djaP3AgentRaw(
			`{"pane_id":"p-ação","name":"n-日本-😀","workspace_id":"w-\u65e5\u672c","agent_status":"blocked"}`)
		panes, err := ParseAgentList([]byte(agRaw))
		if err != nil {
			t.Fatalf("ParseAgentList of the valid multibyte answer: %v", err)
		}
		if len(panes) != 1 {
			t.Fatalf("agents = %d, want 1", len(panes))
		}
		if panes[0].PaneID != "p-ação" || panes[0].Name != "n-日本-😀" || panes[0].WorkspaceID != "w-日本" || panes[0].AgentStatus != "blocked" {
			t.Fatalf("agent row = %+v, want the decoded multibyte values", panes[0])
		}
	})
	t.Run("replacement character controls and html", func(t *testing.T) {
		// The escaped U+FFFD and the literal U+FFFD (bytes EF BF BD) are
		// both valid JSON of valid UTF-8 and decode to the same rune.
		escaped := djaP3WSRaw(`{"workspace_id":"w1","label":"a\uFFFD","agent_status":"idle"}`)
		literal := djaP3WSRaw(`{"workspace_id":"w1","label":"a` + "\uFFFD" + `","agent_status":"idle"}`)
		escapedWS, err := ParseWorkspaceList([]byte(escaped))
		if err != nil {
			t.Fatalf("ParseWorkspaceList of the escaped U+FFFD: %v", err)
		}
		literalWS, err := ParseWorkspaceList([]byte(literal))
		if err != nil {
			t.Fatalf("ParseWorkspaceList of the literal U+FFFD: %v", err)
		}
		if escapedWS[0].Label != "a\uFFFD" || literalWS[0].Label != "a\uFFFD" || escapedWS[0].Label != literalWS[0].Label {
			t.Fatalf("labels = %q / %q, want a + U+FFFD for both", escapedWS[0].Label, literalWS[0].Label)
		}
		controls := djaP3WSRaw(`{"workspace_id":"w2","label":"a\u0001b","agent_status":"idle"}`)
		controlsWS, err := ParseWorkspaceList([]byte(controls))
		if err != nil {
			t.Fatalf("ParseWorkspaceList of the control escape: %v", err)
		}
		if controlsWS[0].Label != "a\u0001b" {
			t.Fatalf("control label = %q, want a U+0001 b", controlsWS[0].Label)
		}
		html := djaP3AgentRaw(`{"pane_id":"p2","name":"<a>&b</a>","workspace_id":"w2","agent_status":"idle"}`)
		htmlPanes, err := ParseAgentList([]byte(html))
		if err != nil {
			t.Fatalf("ParseAgentList of the HTML characters: %v", err)
		}
		if htmlPanes[0].Name != "<a>&b</a>" {
			t.Fatalf("html name = %q, want <a>&b</a>", htmlPanes[0].Name)
		}
	})
	t.Run("long label bounded", func(t *testing.T) {
		label := normLongString('a', 2000)
		raw := djaP3WSRaw(`{"workspace_id":"w1","label":"` + label + `","agent_status":"idle"}`)
		ws, err := ParseWorkspaceList([]byte(raw))
		if err != nil {
			t.Fatalf("ParseWorkspaceList of the 2000-byte label: %v", err)
		}
		want := boundPolledString(label, MaxPolledStringBytes)
		if ws[0].Label != want || len(ws[0].Label) != MaxPolledStringBytes {
			t.Fatalf("label = %q (len %d), want the %d-byte bound with the marker", normHead(ws[0].Label), len(ws[0].Label), MaxPolledStringBytes)
		}
		if !strings.HasSuffix(ws[0].Label, normMarker(label)) {
			t.Fatalf("label = %q, want the marker of the original 2000 bytes", normHead(ws[0].Label))
		}
	})
}

// TestDJA229P3PollUTF8UnpairedSurrogatesCurrentBehavior documents the
// current behavior the raw-byte guard does not change: an escaped
// unpaired surrogate is valid JSON made of ASCII bytes, so utf8.Valid
// does not see it and the Go decoder turns each one into U+FFFD; the
// answer is accepted and decodes to the replacement character, while a
// valid escaped pair decodes to the astral rune.
func TestDJA229P3PollUTF8UnpairedSurrogatesCurrentBehavior(t *testing.T) {
	raw := djaP3WSRaw(`{"workspace_id":"w\ud800","label":"l\udbff","agent_status":"idle"}`)
	ws, err := ParseWorkspaceList([]byte(raw))
	if err != nil {
		t.Fatalf("the unpaired surrogate escapes are valid JSON of ASCII bytes and must be accepted: %v", err)
	}
	if len(ws) != 1 || ws[0].ID != "w\uFFFD" || ws[0].Label != "l\uFFFD" {
		t.Fatalf("unpaired escapes = %+v, want U+FFFD for each escape", ws)
	}
	pair := djaP3WSRaw(`{"workspace_id":"w\ud83d\ude00","label":"l","agent_status":"idle"}`)
	pairWS, err := ParseWorkspaceList([]byte(pair))
	if err != nil {
		t.Fatalf("ParseWorkspaceList of the escaped pair: %v", err)
	}
	if pairWS[0].ID != "w\U0001F600" {
		t.Fatalf("escaped pair = %q, want U+1F600", pairWS[0].ID)
	}
}

// TestDJA229P3PollUTF8DirectPathControl: a PollResult built in Go,
// bypassing the parsers, keeps the distinction of the invalid-UTF-8
// strings that the parsers now refuse whole: each value is bounded with
// the marker of its own original bytes, so "bad\xff" and "bad\xfe" stay
// distinct.
func TestDJA229P3PollUTF8DirectPathControl(t *testing.T) {
	one, err := normalizePollResult(PollResult{Maquina: "node-a", At: fixedNow, OK: true,
		Workspaces: []PolledWorkspace{{ID: "bad\xff", Label: "l", AgentStatus: "idle"}}})
	if err != nil {
		t.Fatalf("normalizePollResult of bad\xff: %v", err)
	}
	two, err := normalizePollResult(PollResult{Maquina: "node-a", At: fixedNow, OK: true,
		Workspaces: []PolledWorkspace{{ID: "bad\xfe", Label: "l", AgentStatus: "idle"}}})
	if err != nil {
		t.Fatalf("normalizePollResult of bad\xfe: %v", err)
	}
	if one.Workspaces[0].ID == two.Workspaces[0].ID {
		t.Fatalf("the two invalid-UTF-8 ids collapsed to one value: %q", one.Workspaces[0].ID)
	}
	if !strings.HasSuffix(one.Workspaces[0].ID, normMarker("bad\xff")) {
		t.Fatalf("direct bad\xff = %q, want the marker of its original bytes", normHead(one.Workspaces[0].ID))
	}
	if !strings.HasSuffix(two.Workspaces[0].ID, normMarker("bad\xfe")) {
		t.Fatalf("direct bad\xfe = %q, want the marker of its original bytes", normHead(two.Workspaces[0].ID))
	}
}
