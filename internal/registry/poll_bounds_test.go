package registry

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"unicode/utf8"
)

// normLongString is n repetitions of one rune, a string of predictable
// byte length.
func normLongString(r rune, n int) string {
	return strings.Repeat(string(r), n)
}

// normRune is an alias of normLongString used where the name reads as a
// rune repetition.
func normRune(r string, n int) string {
	return strings.Repeat(r, n)
}

// normMarker is the exact truncation marker of the original bytes of s,
// computed against crypto/sha256 in the test.
func normMarker(s string) string {
	sum := sha256.Sum256([]byte(s))
	return "[truncated:" + hex.EncodeToString(sum[:]) + "]"
}

// normHead shortens a value for a failure message.
func normHead(s string) string {
	if len(s) > 48 {
		return s[:48] + "..."
	}
	return s
}

// normAssertBounded checks the invariants of one bounded result: the
// length bound, the valid UTF-8, the marker of the original bytes when
// the input was bounded, and the idempotence of bounding it again.
func normAssertBounded(t *testing.T, s string, limit int, got string) {
	t.Helper()
	if len(got) > limit {
		t.Fatalf("bound of a %d-byte input at limit %d = %d bytes, want <= %d", len(s), limit, len(got), limit)
	}
	if !utf8.ValidString(got) {
		t.Fatalf("bound of a %d-byte input at limit %d is not valid UTF-8: %q", len(s), limit, normHead(got))
	}
	if len(s) > limit || !utf8.ValidString(s) {
		if !strings.HasSuffix(got, normMarker(s)) {
			t.Fatalf("bound of a %d-byte input does not end with the marker of the original bytes: %q", len(s), normHead(got))
		}
	}
	if again := boundPolledString(got, limit); again != got {
		t.Fatalf("bounding twice changed the result: %q -> %q", normHead(got), normHead(again))
	}
}

// normCase is one input with the limit its bound is measured at.
type normCase struct {
	name  string
	s     string
	limit int
}

// normCases gathers the inputs of the other bound tests: the limit
// boundaries at both limits, the rune-straddling cuts, the invalid
// inputs, the shared-prefix pair and the verbatim specials.
func normCases() []normCase {
	cases := []normCase{}
	for _, limit := range []int{MaxPolledStringBytes, MaxPolledPaneBytes} {
		cases = append(cases,
			normCase{"limit-1", normLongString('a', limit-1), limit},
			normCase{"limit", normLongString('a', limit), limit},
			normCase{"limit+1", normLongString('a', limit+1), limit},
			normCase{"empty", "", limit},
		)
	}
	return append(cases,
		normCase{"two-byte straddle", normRune("\u00e9", 60), 115},
		normCase{"three-byte straddle", normRune("\u20ac", 40), 116},
		normCase{"four-byte straddle", normRune("\U0001F600", 30), 117},
		normCase{"mixed straddle", normRune("\u00e9", 61) + normRune("\u20ac", 60), MaxPolledPaneBytes},
		normCase{"invalid within", "aaaaaaaaaa\xffaaaaaaaaa", MaxPolledStringBytes},
		normCase{"invalid inside cut", "\xff" + normLongString('a', 200), MaxPolledPaneBytes},
		normCase{"invalid past cut", normLongString('a', 299) + "\xff", MaxPolledPaneBytes},
		normCase{"shared prefix one", normLongString('a', 99999) + "b", MaxPolledStringBytes},
		normCase{"shared prefix two", normLongString('a', 99999) + "c", MaxPolledStringBytes},
		normCase{"specials within", "\x00\x01\x02<&>\u2028", MaxPolledStringBytes},
		normCase{"specials over", "\u2028" + normLongString('a', 300), MaxPolledPaneBytes},
	)
}

// normJSONRoundTrip marshals s and checks the unmarshal gives it back.
func normJSONRoundTrip(t *testing.T, s string) {
	t.Helper()
	b, err := json.Marshal(s)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	var back string
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	if back != s {
		t.Fatalf("JSON round-trip = %q, want %q", normHead(back), normHead(s))
	}
}

// normPollClone deep-copies p so a later comparison proves the caller's
// value was not written.
func normPollClone(p PollResult) PollResult {
	c := p
	c.Workspaces = append([]PolledWorkspace(nil), p.Workspaces...)
	c.Panes = append([]PolledPane(nil), p.Panes...)
	return c
}

// normTableCounts counts the rows of every user table of the store.
func normTableCounts(t *testing.T, s *Store) map[string]int {
	t.Helper()
	out := map[string]int{}
	for _, name := range tableNames(t, s) {
		out[name] = tableCount(t, s, name)
	}
	return out
}

// normNodeVersion reads the stored herdr_version of one node.
func normNodeVersion(t *testing.T, s *Store, maquina string) *string {
	t.Helper()
	var ver sql.NullString
	if err := s.db.QueryRow(`SELECT herdr_version FROM nodes WHERE maquina = ?`, maquina).Scan(&ver); err != nil {
		t.Fatalf("herdr_version: %v", err)
	}
	if !ver.Valid {
		return nil
	}
	return &ver.String
}

// normNoLeak reports when a returned field keeps a fragment of the
// nested unknown content of the parsed answer.
func normNoLeak(t *testing.T, fields []string, fragments []string) {
	t.Helper()
	for _, frag := range fragments {
		for _, f := range fields {
			if strings.Contains(f, frag) {
				t.Fatalf("a returned field keeps the nested content %q: %q", normHead(frag), normHead(f))
			}
		}
	}
}

// TestNormBoundPolledStringLimits: at each limit the bound is exact:
// limit-1 and limit are returned unchanged, limit+1 is cut to the limit
// with the marker of the original bytes, and the result is valid UTF-8.
func TestNormBoundPolledStringLimits(t *testing.T) {
	for _, limit := range []int{MaxPolledStringBytes, MaxPolledPaneBytes} {
		short := normLongString('a', limit-1)
		if got := boundPolledString(short, limit); got != short {
			t.Fatalf("limit %d: a valid %d-byte input changed, want unchanged", limit, limit-1)
		}
		exact := normLongString('a', limit)
		if got := boundPolledString(exact, limit); got != exact {
			t.Fatalf("limit %d: a valid %d-byte input changed, want unchanged", limit, limit)
		}
		long := normLongString('a', limit+1)
		got := boundPolledString(long, limit)
		normAssertBounded(t, long, limit, got)
		want := normLongString('a', limit-truncationMarkerLen) + normMarker(long)
		if got != want {
			t.Fatalf("limit %d: bound of %d bytes = %q, want %d chars + the marker", limit, len(long), normHead(got), limit-truncationMarkerLen)
		}
		if got := boundPolledString("", limit); got != "" {
			t.Fatalf("limit %d: the empty string = %q, want unchanged", limit, got)
		}
	}
}

// TestNormBoundPolledStringRuneBoundary: a cut that falls inside a 2-, 3-
// or 4-byte rune leaves the prefix on a rune boundary: the whole runes
// before the cut plus the marker of the original bytes.
func TestNormBoundPolledStringRuneBoundary(t *testing.T) {
	cases := []struct {
		name   string
		rune   string
		limit  int
		input  string
		prefix string
	}{
		{"two-byte", "\u00e9", 115, normRune("\u00e9", 60), normRune("\u00e9", 19)},
		{"three-byte", "\u20ac", 116, normRune("\u20ac", 40), normRune("\u20ac", 13)},
		{"four-byte", "\U0001F600", 117, normRune("\U0001F600", 30), normRune("\U0001F600", 10)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if (c.limit-truncationMarkerLen)%len(c.rune) == 0 {
				t.Fatalf("case %s: the cut does not straddle a rune, fix the fixture", c.name)
			}
			got := boundPolledString(c.input, c.limit)
			normAssertBounded(t, c.input, c.limit, got)
			if got != c.prefix+normMarker(c.input) {
				t.Fatalf("bound = %q, want %d whole runes + the marker of the original", normHead(got), utf8.RuneCountInString(c.prefix))
			}
		})
	}
	// At the pane limit a mixed 2- and 3-byte cut also stops on a
	// boundary.
	s := normRune("\u00e9", 61) + normRune("\u20ac", 60)
	got := boundPolledString(s, MaxPolledPaneBytes)
	normAssertBounded(t, s, MaxPolledPaneBytes, got)
	want := normRune("\u00e9", 61) + normRune("\u20ac", 19) + normMarker(s)
	if got != want {
		t.Fatalf("the mixed cut at %d = %q, want 61 two-byte + 19 three-byte runes + the marker", MaxPolledPaneBytes, normHead(got))
	}
}

// TestNormBoundPolledStringInvalidUTF8: an invalid input gets the marker
// with the digest of the original bytes; the replaced byte carries
// U+FFFD inside the prefix, and the prefix still ends on a rune
// boundary.
func TestNormBoundPolledStringInvalidUTF8(t *testing.T) {
	// Invalid within the limit: the whole valid form plus the marker.
	s1 := "aaaaaaaaaa" + "\xff" + "aaaaaaaaa" // 20 bytes
	got := boundPolledString(s1, MaxPolledStringBytes)
	normAssertBounded(t, s1, MaxPolledStringBytes, got)
	want := "aaaaaaaaaa" + "\ufffd" + "aaaaaaaaa" + normMarker(s1)
	if got != want {
		t.Fatalf("bound = %q, want the U+FFFD form of the input + the marker of the original bytes", normHead(got))
	}
	// Invalid with the U+FFFD inside the cut.
	s2 := "\xff" + normLongString('a', 200)
	got = boundPolledString(s2, MaxPolledPaneBytes)
	normAssertBounded(t, s2, MaxPolledPaneBytes, got)
	want = "\ufffd" + normLongString('a', 177) + normMarker(s2)
	if got != want {
		t.Fatalf("bound = %q, want the U+FFFD + 177 chars + the marker of the original bytes", normHead(got))
	}
	// Invalid with the U+FFFD past the cut: the prefix is clean.
	s3 := normLongString('a', 299) + "\xff"
	got = boundPolledString(s3, MaxPolledPaneBytes)
	normAssertBounded(t, s3, MaxPolledPaneBytes, got)
	want = normLongString('a', 180) + normMarker(s3)
	if got != want {
		t.Fatalf("bound = %q, want 180 chars + the marker of the original bytes", normHead(got))
	}
}

// TestNormBoundPolledStringIdempotent: bounding twice equals bounding
// once for every case the other bound tests exercise.
func TestNormBoundPolledStringIdempotent(t *testing.T) {
	for _, c := range normCases() {
		once := boundPolledString(c.s, c.limit)
		twice := boundPolledString(once, c.limit)
		if once != twice {
			t.Fatalf("case %s: bounding twice = %q, want the first bound kept", c.name, normHead(twice))
		}
	}
}

// TestNormBoundPolledStringSharedPrefix: two inputs that share every
// byte but the last get different results: the marker carries the digest
// of the whole original, checked against crypto/sha256.
func TestNormBoundPolledStringSharedPrefix(t *testing.T) {
	base := normLongString('a', 99999)
	s1 := base + "b"
	s2 := base + "c"
	got1 := boundPolledString(s1, MaxPolledStringBytes)
	got2 := boundPolledString(s2, MaxPolledStringBytes)
	normAssertBounded(t, s1, MaxPolledStringBytes, got1)
	normAssertBounded(t, s2, MaxPolledStringBytes, got2)
	if got1 == got2 {
		t.Fatal("the shared-prefix pair collapsed to one result, want distinct digests")
	}
	sum1 := sha256.Sum256([]byte(s1))
	want1 := normLongString('a', MaxPolledStringBytes-truncationMarkerLen) + "[truncated:" + hex.EncodeToString(sum1[:]) + "]"
	if got1 != want1 {
		t.Fatalf("bound of input one = %q, want 948 chars + the sha256 of its 100000 original bytes", normHead(got1))
	}
}

// TestNormBoundPolledStringVerbatim: control characters, the HTML
// characters and U+2028 are kept as they are within the limit, the
// bounded result keeps them inside the prefix, and the result survives a
// JSON round-trip.
func TestNormBoundPolledStringVerbatim(t *testing.T) {
	s := "\x00\x01\x02<&>\u2028"
	if got := boundPolledString(s, MaxPolledStringBytes); got != s {
		t.Fatalf("specials within the limit = %q, want unchanged", got)
	}
	normJSONRoundTrip(t, s)
	long := "\u2028" + normLongString('a', 300)
	got := boundPolledString(long, MaxPolledPaneBytes)
	normAssertBounded(t, long, MaxPolledPaneBytes, got)
	if !strings.HasPrefix(got, "\u2028") {
		t.Fatalf("the prefix lost the U+2028: %q", normHead(got))
	}
	if strings.Contains(got, normLongString('a', 300)) {
		t.Fatalf("the bounded result kept the whole 300-char input: %q", normHead(got))
	}
	normJSONRoundTrip(t, got)
}

// TestNormParseBounded: the parsers return bounded fields: a 100000-char
// field comes back at its limit with the marker, and the nested unknown
// objects (a 1 MiB metadata object, cwd, title and sessions) appear in
// no returned field.
func TestNormParseBounded(t *testing.T) {
	mega := normLongString('m', 1024*1024)
	nested := `,"metadata":{"k":"` + mega + `"},"cwd":"` + normLongString('c', 100000) + `","title":"long-title","sessions":[{"id":"` + mega + `"}]`
	wsRaw := `{"id":"1","result":{"type":"workspace_list","workspaces":[{"workspace_id":"` +
		normLongString('a', 100000) + `","label":"` + normLongString('b', 100000) +
		`","agent_status":"idle"` + nested + `}]}}`
	ws, err := ParseWorkspaceList([]byte(wsRaw))
	if err != nil {
		t.Fatalf("ParseWorkspaceList: %v", err)
	}
	if len(ws) != 1 {
		t.Fatalf("workspaces = %d, want 1", len(ws))
	}
	boundedID := boundPolledString(normLongString('a', 100000), MaxPolledStringBytes)
	if ws[0].ID != boundedID || len(ws[0].ID) != MaxPolledStringBytes {
		t.Fatalf("workspace_id = %q (len %d), want the %d-byte bound with the marker", normHead(ws[0].ID), len(ws[0].ID), MaxPolledStringBytes)
	}
	if !strings.HasSuffix(ws[0].ID, normMarker(normLongString('a', 100000))) {
		t.Fatalf("workspace_id = %q, want the marker of the original 100000 bytes", normHead(ws[0].ID))
	}
	boundedLabel := boundPolledString(normLongString('b', 100000), MaxPolledStringBytes)
	if ws[0].Label != boundedLabel || len(ws[0].Label) != MaxPolledStringBytes {
		t.Fatalf("label = %q (len %d), want the %d-byte bound with the marker", normHead(ws[0].Label), len(ws[0].Label), MaxPolledStringBytes)
	}
	if ws[0].AgentStatus != "idle" {
		t.Fatalf("agent_status = %q, want idle", ws[0].AgentStatus)
	}
	normNoLeak(t, []string{ws[0].ID, ws[0].Label, ws[0].AgentStatus},
		[]string{"metadata", "cwd", "long-title", normLongString('m', 100), normLongString('c', 100)})

	agRaw := `{"id":"1","result":{"agents":[{"pane_id":"` +
		normLongString('a', 100000) + `","workspace_id":"` + normLongString('b', 100000) +
		`","name":"` + normLongString('n', 100000) +
		`","agent_status":"blocked"` + nested + `}]}}`
	panes, err := ParseAgentList([]byte(agRaw))
	if err != nil {
		t.Fatalf("ParseAgentList: %v", err)
	}
	if len(panes) != 1 {
		t.Fatalf("agents = %d, want 1", len(panes))
	}
	boundedPane := boundPolledString(normLongString('a', 100000), MaxPolledPaneBytes)
	if panes[0].PaneID != boundedPane || len(panes[0].PaneID) != MaxPolledPaneBytes {
		t.Fatalf("pane_id = %q (len %d), want the %d-byte bound with the marker", normHead(panes[0].PaneID), len(panes[0].PaneID), MaxPolledPaneBytes)
	}
	boundedName := boundPolledString(normLongString('n', 100000), MaxPolledPaneBytes)
	if panes[0].Name != boundedName || len(panes[0].Name) != MaxPolledPaneBytes {
		t.Fatalf("name = %q (len %d), want the %d-byte bound with the marker", normHead(panes[0].Name), len(panes[0].Name), MaxPolledPaneBytes)
	}
	boundedWSB := boundPolledString(normLongString('b', 100000), MaxPolledStringBytes)
	if panes[0].WorkspaceID != boundedWSB || len(panes[0].WorkspaceID) != MaxPolledStringBytes {
		t.Fatalf("workspace_id = %q (len %d), want the %d-byte bound with the marker", normHead(panes[0].WorkspaceID), len(panes[0].WorkspaceID), MaxPolledStringBytes)
	}
	if panes[0].AgentStatus != "blocked" {
		t.Fatalf("agent_status = %q, want blocked", panes[0].AgentStatus)
	}
	normNoLeak(t, []string{panes[0].PaneID, panes[0].WorkspaceID, panes[0].Name, panes[0].AgentStatus},
		[]string{"metadata", "cwd", "long-title", normLongString('m', 100), normLongString('c', 100)})
}

// TestNormApplyPollDirectBounded: a PollResult built in Go, bypassing the
// parsers, is stored bounded; the caller's value is not written; a
// truncated job- label is stored as no row and a truncated hm- label as a
// ws: row with a null owner and a null job id; and a pane whose workspace
// id was truncated still resolves to the workspace job in the inventory.
func TestNormApplyPollDirectBounded(t *testing.T) {
	s := openStore(t)
	longID := normLongString('a', 100000)
	hmLabel := "hm-" + normLongString('z', 100000)
	jobLabel := "job-" + normLongString('j', 100000)
	longPane := normLongString('p', 100000)
	longName := normLongString('n', 100000)
	p := PollResult{
		Maquina:      "node-a",
		At:           fixedNow,
		OK:           true,
		Motivo:       normLongString('m', 100000),
		HerdrVersion: "bad version with a space",
		Workspaces:   []PolledWorkspace{{ID: longID, Label: hmLabel, AgentStatus: "idle"}, {ID: "wjob", Label: jobLabel, AgentStatus: "idle"}},
		Panes:        []PolledPane{{PaneID: longPane, WorkspaceID: longID, Name: longName, AgentStatus: "blocked"}},
	}
	clone := normPollClone(p)
	mustApplyPoll(t, s, p)
	if !reflect.DeepEqual(p, clone) {
		t.Fatal("the caller's PollResult was written by ApplyPoll")
	}
	boundedID := boundPolledString(longID, MaxPolledStringBytes)
	boundedLabel := boundPolledString(hmLabel, MaxPolledStringBytes)
	boundedPane := boundPolledString(longPane, MaxPolledPaneBytes)
	boundedName := boundPolledString(longName, MaxPolledPaneBytes)
	key := "ws:" + boundedID
	if n := tableCount(t, s, "jobs"); n != 1 {
		t.Fatalf("job rows = %d, want 1: the truncated job- label must not create a row", n)
	}
	f := jobFact(t, s, "node-a", key)
	if !f.exists {
		t.Fatalf("the ws: row for the truncated hm workspace is missing")
	}
	if f.agente != nil || f.trabalho != nil || f.ref != nil || f.fonte != nil {
		t.Errorf("truncated hm label owner = (%v, %v, %v, %v), want all null", f.agente, f.trabalho, f.ref, f.fonte)
	}
	if f.workspace == nil || *f.workspace != boundedLabel {
		t.Errorf("workspace = %v, want the bounded label", f.workspace)
	}
	var jobID, wsID sql.NullString
	if err := s.db.QueryRow(`SELECT job_id, workspace_id FROM jobs WHERE maquina = 'node-a' AND chave = ?`, key).Scan(&jobID, &wsID); err != nil {
		t.Fatalf("job_id/workspace_id: %v", err)
	}
	if jobID.Valid {
		t.Errorf("job_id = %q, want null: a truncated label never forges a job id", jobID.String)
	}
	if !wsID.Valid || wsID.String != boundedID {
		t.Errorf("workspace_id = %q, want the bounded id", wsID.String)
	}
	var waitWS, waitName sql.NullString
	if err := s.db.QueryRow(`SELECT workspace_id, agente FROM waits WHERE maquina = 'node-a' AND pane = ?`, boundedPane).Scan(&waitWS, &waitName); err != nil {
		t.Fatalf("wait for the bounded pane id: %v", err)
	}
	if !waitWS.Valid || waitWS.String != boundedID {
		t.Errorf("wait workspace_id = %q, want the bounded id", waitWS.String)
	}
	if !waitName.Valid || waitName.String != boundedName {
		t.Errorf("wait name = %q, want the bounded name", waitName.String)
	}
	if v := normNodeVersion(t, s, "node-a"); v != nil {
		t.Errorf("herdr_version = %q, want null for the bad version", *v)
	}
	inv, err := s.Inventory(Filter{Maquina: "node-a"})
	if err != nil {
		t.Fatalf("Inventory: %v", err)
	}
	found := false
	for _, w := range inv.Esperas {
		if w.Tipo == EsperaPane && w.Pane != nil && *w.Pane == boundedPane && w.Chave != nil && *w.Chave == key {
			found = true
		}
	}
	if !found {
		t.Errorf("no pane wait resolved to %q in Inventory().Esperas (esperas = %d)", key, len(inv.Esperas))
	}
}

// TestNormApplyPollDirectVersion: a HerdrVersion outside the parser's
// pattern is dropped: a bad first poll stores null and a later bad poll
// never clears the stored version.
func TestNormApplyPollDirectVersion(t *testing.T) {
	s := openStore(t)
	mustApplyPoll(t, s, PollResult{Maquina: "node-a", At: fixedNow, OK: true, HerdrVersion: "bad version with a space"})
	if v := normNodeVersion(t, s, "node-a"); v != nil {
		t.Fatalf("herdr_version = %q, want null after the bad version", *v)
	}
	mustApplyPoll(t, s, PollResult{Maquina: "node-a", At: fixedNow, OK: true, HerdrVersion: "9.9.9"})
	if v := normNodeVersion(t, s, "node-a"); v == nil || *v != "9.9.9" {
		t.Fatalf("herdr_version = %v, want 9.9.9", v)
	}
	mustApplyPoll(t, s, PollResult{Maquina: "node-a", At: fixedNow, OK: true, HerdrVersion: "still bad"})
	if v := normNodeVersion(t, s, "node-a"); v == nil || *v != "9.9.9" {
		t.Fatalf("herdr_version = %v after the bad poll, want 9.9.9 kept", v)
	}
}

// TestNormApplyPollDirectFailedMotivo: a failed poll stores its bounded
// Motivo and drops its lists: the workspaces and panes it carries are
// stored nowhere, and the caller's slices are kept.
func TestNormApplyPollDirectFailedMotivo(t *testing.T) {
	s := openStore(t)
	p := PollResult{
		Maquina:      "node-a",
		At:           fixedNow,
		OK:           false,
		Motivo:       normLongString('x', 100000),
		Workspaces:   []PolledWorkspace{{ID: "w1", Label: "hm-example-impl-x1", AgentStatus: "idle"}},
		Panes:        []PolledPane{{PaneID: "p1", WorkspaceID: "w1", Name: "n1", AgentStatus: "blocked"}},
		HerdrVersion: "bad version",
	}
	clone := normPollClone(p)
	mustApplyPoll(t, s, p)
	if !reflect.DeepEqual(p, clone) {
		t.Fatal("the caller's PollResult was written by ApplyPoll")
	}
	bounded := boundPolledString(normLongString('x', 100000), MaxPolledStringBytes)
	var status sql.NullString
	if err := s.db.QueryRow(`SELECT poll_status FROM nodes WHERE maquina = 'node-a'`).Scan(&status); err != nil {
		t.Fatalf("poll_status: %v", err)
	}
	if !status.Valid || status.String != bounded {
		t.Errorf("poll_status = %q (len %d), want the %d-byte bounded Motivo", normHead(status.String), len(status.String), MaxPolledStringBytes)
	}
	if n := tableCount(t, s, "jobs"); n != 0 {
		t.Errorf("job rows = %d, want 0: a failed poll drops its lists", n)
	}
	if n := tableCount(t, s, "waits"); n != 0 {
		t.Errorf("waits = %d, want 0: a failed poll drops its lists", n)
	}
}

// TestNormNormalizeDropsFailedLists: the copy of a failed poll drops its
// lists and the caller's lists are kept.
func TestNormNormalizeDropsFailedLists(t *testing.T) {
	p := PollResult{
		Maquina:      "node-a",
		At:           fixedNow,
		OK:           false,
		Motivo:       "probe_timeout",
		Workspaces:   []PolledWorkspace{{ID: "w1", Label: "l", AgentStatus: "idle"}},
		Panes:        []PolledPane{{PaneID: "p1", WorkspaceID: "w1", Name: "n1", AgentStatus: "blocked"}},
		HerdrVersion: "bad version",
	}
	clone := normPollClone(p)
	out, err := normalizePollResult(p)
	if err != nil {
		t.Fatalf("normalizePollResult: %v", err)
	}
	if out.Workspaces != nil || out.Panes != nil {
		t.Fatalf("the failed poll's copy kept its lists: %d workspaces, %d panes", len(out.Workspaces), len(out.Panes))
	}
	if out.HerdrVersion != "" {
		t.Errorf("HerdrVersion = %q, want dropped on the copy", out.HerdrVersion)
	}
	if !reflect.DeepEqual(p, clone) {
		t.Fatal("the caller's PollResult was written by normalizePollResult")
	}
}

// TestNormFailedPollIgnoresInvalidLists: a failed poll records its
// failure whatever its lists carry: the lists are neither validated nor
// applied, the bounded Motivo is stored, the node's jobs go obsolete,
// and the caller's value is kept.
func TestNormFailedPollIgnoresInvalidLists(t *testing.T) {
	s := openStore(t)
	mustApplyPoll(t, s, okPoll("node-a", "1.2.3",
		PolledWorkspace{ID: "w1", Label: "hm-example-impl-x1", AgentStatus: "working"}))
	p := PollResult{
		Maquina:      "node-a",
		At:           fixedNow,
		OK:           false,
		Motivo:       normLongString('x', 100000),
		HerdrVersion: "bad version",
		Workspaces:   []PolledWorkspace{{ID: "", Label: "hm-example-impl-x1", AgentStatus: "sleeping"}, {ID: "w9", Label: "hm-second-impl-ref2", AgentStatus: "idle"}},
		Panes:        []PolledPane{{PaneID: "p9", WorkspaceID: "w9", Name: "n9", AgentStatus: "bogus"}},
	}
	clone := normPollClone(p)
	if err := s.ApplyPoll(p); err != nil {
		t.Fatalf("ApplyPoll of the failed poll = %v, want nil: the lists of a failed poll are never validated", err)
	}
	if !reflect.DeepEqual(p, clone) {
		t.Fatal("the caller's PollResult was written by ApplyPoll")
	}
	bounded := boundPolledString(normLongString('x', 100000), MaxPolledStringBytes)
	var status string
	if err := s.db.QueryRow(`SELECT poll_status FROM nodes WHERE maquina = 'node-a'`).Scan(&status); err != nil {
		t.Fatalf("poll_status: %v", err)
	}
	if status != bounded {
		t.Errorf("poll_status = %q (len %d), want the %d-byte bounded Motivo", normHead(status), len(status), MaxPolledStringBytes)
	}
	if n := tableCount(t, s, "jobs"); n != 1 {
		t.Errorf("job rows = %d, want 1: a failed poll writes no jobs from its lists", n)
	}
	if f := jobFact(t, s, "node-a", "ws:w1"); !f.exists || f.obsoleto != 1 {
		t.Errorf("ws:w1 = (exists %v, obsoleto %d), want the known row marked obsolete", f.exists, f.obsoleto)
	}
	if f := jobFact(t, s, "node-a", "ws:w9"); f.exists {
		t.Error("a workspace from the failed poll's lists became a job row")
	}
	if n := tableCount(t, s, "waits"); n != 0 {
		t.Errorf("waits = %d, want 0: a failed poll writes no waits", n)
	}
	if v := normNodeVersion(t, s, "node-a"); v == nil || *v != "1.2.3" {
		t.Errorf("herdr_version = %v, want the stored version kept", v)
	}
}

// TestNormApplyPollInvalid: an empty workspace id, an empty pane id and
// an unknown agent status are ErrInvalid on the direct path, the
// database is unchanged, and the caller's value is kept.
func TestNormApplyPollInvalid(t *testing.T) {
	cases := []struct {
		name string
		p    PollResult
	}{
		{
			name: "empty workspace id",
			p: PollResult{Maquina: "node-a", At: fixedNow, OK: true,
				Workspaces: []PolledWorkspace{{ID: "", Label: "hm-example-impl-x1", AgentStatus: "idle"}}},
		},
		{
			name: "empty pane id",
			p: PollResult{Maquina: "node-a", At: fixedNow, OK: true,
				Panes: []PolledPane{{PaneID: "", WorkspaceID: "w1", Name: "n1", AgentStatus: "blocked"}}},
		},
		{
			name: "unknown workspace status",
			p: PollResult{Maquina: "node-a", At: fixedNow, OK: true,
				Workspaces: []PolledWorkspace{{ID: "w1", Label: "hm-example-impl-x1", AgentStatus: "sleeping"}}},
		},
		{
			name: "unknown pane status",
			p: PollResult{Maquina: "node-a", At: fixedNow, OK: true,
				Panes: []PolledPane{{PaneID: "p1", WorkspaceID: "w1", Name: "n1", AgentStatus: "sleeping"}}},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := openStore(t)
			before := normTableCounts(t, s)
			clone := normPollClone(c.p)
			err := s.ApplyPoll(c.p)
			if !errors.Is(err, ErrInvalid) {
				t.Fatalf("ApplyPoll = %v, want an ErrInvalid wrap", err)
			}
			after := normTableCounts(t, s)
			if !reflect.DeepEqual(before, after) {
				t.Fatalf("the database changed on refusal: before = %v, after = %v", before, after)
			}
			if !reflect.DeepEqual(c.p, clone) {
				t.Fatal("the caller's PollResult was written on the error path")
			}
		})
	}
}
