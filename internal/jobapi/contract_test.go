package jobapi

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// findDoc walks up from this test file to the module root (marked by go.mod)
// and returns the vendored contract document.
func findDoc(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return filepath.Join(dir, "docs", "upstream", "job-contract.md")
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("go.mod not found walking up from %s", dir)
		}
		dir = parent
	}
}

func splitRow(row string) []string {
	cells := strings.Split(row, "|")
	// A pipe-delimited row starts and ends with an empty split.
	if len(cells) >= 2 && cells[0] == "" {
		cells = cells[1:]
	}
	if len(cells) >= 1 && cells[len(cells)-1] == "" {
		cells = cells[:len(cells)-1]
	}
	out := make([]string, 0, len(cells))
	for _, c := range cells {
		out = append(out, strings.TrimSpace(c))
	}
	return out
}

var separatorCell = regexp.MustCompile(`^:?-{3,}:?$`)

func isSeparatorRow(cells []string) bool {
	if len(cells) == 0 {
		return false
	}
	for _, c := range cells {
		if !separatorCell.MatchString(c) {
			return false
		}
	}
	return true
}

// wakeModeForCell maps the "wakes the dispatcher" column to a WakeMode.
func wakeModeForCell(cell string) (WakeMode, bool) {
	switch strings.ToLower(cell) {
	case "no":
		return WakeNo, true
	case "yes", "yes, immediately":
		return WakeYes, true
	case "only on failure":
		return WakeOnFailure, true
	default:
		return WakeNo, false
	}
}

// docEventRow is one data row of the event-type table.
type docEventRow struct {
	tipo  string
	wakes string
}

// extractEventRows reads the event-type table (header first cell "type").
func extractEventRows(doc string) []docEventRow {
	var rows []docEventRow
	inTable := false
	for _, line := range strings.Split(doc, "\n") {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, "|") {
			inTable = false
			continue
		}
		cells := splitRow(trimmed)
		if !inTable {
			if len(cells) > 0 && cells[0] == "type" {
				inTable = true
			}
			continue
		}
		if isSeparatorRow(cells) || len(cells) < 3 {
			continue
		}
		tipo := strings.Trim(cells[0], "`")
		rows = append(rows, docEventRow{tipo: tipo, wakes: cells[2]})
	}
	return rows
}

// eventTableDiff parses the event-type table of the given document and lists
// every mismatch against the vendored constants.
func eventTableDiff(doc string) []string {
	var diff []string
	rows := extractEventRows(doc)
	if len(rows) != len(EventTypes) {
		diff = append(diff, fmt.Sprintf("event table row count: doc has %d, vendored has %d", len(rows), len(EventTypes)))
	}
	seen := map[string]bool{}
	for i := 0; i < len(rows) && i < len(EventTypes); i++ {
		seen[rows[i].tipo] = true
		if rows[i].tipo != EventTypes[i] {
			diff = append(diff, fmt.Sprintf("event type at position %d: doc has %q, vendored has %q", i, rows[i].tipo, EventTypes[i]))
		}
		mode, ok := wakeModeForCell(rows[i].wakes)
		if !ok {
			diff = append(diff, fmt.Sprintf("unrecognized wake cell %q for type %q", rows[i].wakes, rows[i].tipo))
			continue
		}
		if got := WakesDispatcher(rows[i].tipo); got != mode {
			diff = append(diff, fmt.Sprintf("wake mode for %q: doc says %q, vendored WakesDispatcher says %v", rows[i].tipo, rows[i].wakes, got))
		}
	}
	for _, tipo := range EventTypes {
		if !seen[tipo] {
			diff = append(diff, fmt.Sprintf("vendored event type %q is missing from the doc table", tipo))
		}
	}
	return diff
}

var splitCode = regexp.MustCompile(`\d+`)

// exitCodesDiff parses the "### Exit codes" table of the given document and
// lists every mismatch against the vendored exit codes. The "11 / 14" row is
// two codes.
func exitCodesDiff(doc string) []string {
	var diff []string
	var codes []int
	inSection := false
	inTable := false
	for _, line := range strings.Split(doc, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "### ") {
			inSection = strings.TrimPrefix(trimmed, "### ") == "Exit codes"
			inTable = false
			continue
		}
		if !inSection {
			continue
		}
		if !strings.HasPrefix(trimmed, "|") {
			inTable = false
			continue
		}
		cells := splitRow(trimmed)
		if !inTable {
			if len(cells) > 0 && cells[0] == "code" {
				inTable = true
			}
			continue
		}
		if isSeparatorRow(cells) || len(cells) < 2 {
			continue
		}
		for _, n := range splitCode.FindAllString(cells[0], -1) {
			code, err := strconv.Atoi(n)
			if err != nil {
				diff = append(diff, fmt.Sprintf("unparseable exit code %q", n))
				continue
			}
			codes = append(codes, code)
		}
	}
	if len(codes) != len(ExitCodes) {
		diff = append(diff, fmt.Sprintf("exit code count: doc has %d, vendored has %d", len(codes), len(ExitCodes)))
	}
	for i := 0; i < len(codes) && i < len(ExitCodes); i++ {
		if codes[i] != ExitCodes[i] {
			diff = append(diff, fmt.Sprintf("exit code at position %d: doc has %d, vendored has %d", i, codes[i], ExitCodes[i]))
		}
	}
	return diff
}

// TestContractEventTable parses the vendored document's event-type table and
// requires it to equal the vendored constants and wake set.
func TestContractEventTable(t *testing.T) {
	doc, err := os.ReadFile(findDoc(t))
	if err != nil {
		t.Fatalf("read contract document: %v", err)
	}
	diff := eventTableDiff(string(doc))
	if len(diff) != 0 {
		t.Errorf("vendored event constants drift from docs/upstream/job-contract.md:\n  %s", strings.Join(diff, "\n  "))
	}
}

// TestContractWakeSet requires the wake set to be exactly the documented one,
// including that unknown types never wake the dispatcher.
func TestContractWakeSet(t *testing.T) {
	doc, err := os.ReadFile(findDoc(t))
	if err != nil {
		t.Fatalf("read contract document: %v", err)
	}
	for _, r := range extractEventRows(string(doc)) {
		want, ok := wakeModeForCell(r.wakes)
		if !ok {
			t.Errorf("unrecognized wake cell %q for %q", r.wakes, r.tipo)
			continue
		}
		if got := WakesDispatcher(r.tipo); got != want {
			t.Errorf("WakesDispatcher(%q) = %v, doc says %q", r.tipo, got, r.wakes)
		}
	}
	for _, tipo := range []string{"", "wakes_everything", "cleanup", "note"} {
		if got := WakesDispatcher(tipo); got != WakeNo {
			t.Errorf("WakesDispatcher(%q) = %v, want WakeNo", tipo, got)
		}
	}
}

// TestContractExitCodes parses the vendored document's exit-code table
// (including the "11 / 14" row as two codes) and requires equality with the
// vendored constants.
func TestContractExitCodes(t *testing.T) {
	doc, err := os.ReadFile(findDoc(t))
	if err != nil {
		t.Fatalf("read contract document: %v", err)
	}
	diff := exitCodesDiff(string(doc))
	if len(diff) != 0 {
		t.Errorf("vendored exit codes drift from docs/upstream/job-contract.md:\n  %s", strings.Join(diff, "\n  "))
	}
}

// TestContractDriftDetected proves the comparison above fails when the
// document changes: a mutated copy of the doc (one extra event row, one
// changed wake cell, one removed exit code) must be reported as drift.
func TestContractDriftDetected(t *testing.T) {
	doc, err := os.ReadFile(findDoc(t))
	if err != nil {
		t.Fatalf("read contract document: %v", err)
	}
	original := string(doc)

	// Insert a fabricated row inside the event-type table, before the first
	// data row.
	anchor := "| `accepted` | supervisor | no | `start` validated |"
	mutated := strings.Replace(original, anchor, "| `fabricated_type` | supervisor | yes | never happens |\n"+anchor, 1)
	if mutated == original {
		t.Fatal("mutation did not change the doc (anchor missing)")
	}
	if len(eventTableDiff(mutated)) == 0 {
		t.Error("added event row was not detected as drift")
	}

	flipped := strings.Replace(original,
		"| `push` | supervisor | only on failure | push succeeded or was rejected |",
		"| `push` | supervisor | no | push succeeded or was rejected |", 1)
	if flipped == original {
		t.Fatal("mutation did not change the doc (anchor missing)")
	}
	if len(eventTableDiff(flipped)) == 0 {
		t.Error("changed wake cell was not detected as drift")
	}

	removed := strings.Replace(original, "| 9 | job `timeout` |\n", "", 1)
	if removed == original {
		t.Fatal("mutation did not change the doc (anchor missing)")
	}
	if len(exitCodesDiff(removed)) == 0 {
		t.Error("removed exit code was not detected as drift")
	}
}

// TestContractDecode is lenient about unknown fields and unknown types.
func TestContractDecode(t *testing.T) {
	event, err := DecodeEvent([]byte(`{"seq":7,"ts":"2026-01-01T12:00:00-03:00","tipo":"unknown_future_type","resumo":"short text","refs":{"nova":"x"},"extra_top_level":123}`))
	if err != nil {
		t.Fatalf("DecodeEvent: %v", err)
	}
	if event.Seq != 7 || event.Tipo != "unknown_future_type" || event.Resumo != "short text" || event.TS != "2026-01-01T12:00:00-03:00" {
		t.Errorf("DecodeEvent = %+v", event)
	}
	if event.Refs["nova"] != "x" {
		t.Errorf("refs = %v", event.Refs)
	}
	decision, err := DecodeEvent([]byte(`{"seq":17,"ts":"2026-01-01T12:00:00-03:00","tipo":"decision","resumo":"r","escopo":"global","motivo":"m"}`))
	if err != nil {
		t.Fatalf("DecodeEvent decision: %v", err)
	}
	if decision.Escopo != "global" || decision.Motivo != "m" {
		t.Errorf("decision event = %+v", decision)
	}
	if _, err := DecodeEvent([]byte(`{not json`)); err == nil {
		t.Error("DecodeEvent accepted invalid JSON")
	}
}

// TestContractLimits pins the vendored input limits and names.
func TestContractLimits(t *testing.T) {
	if StdinCapStart != 256*1024 || StdinCapAmend != 64*1024 || StdinCapSend != 16*1024 {
		t.Errorf("stdin caps = %d/%d/%d", StdinCapStart, StdinCapAmend, StdinCapSend)
	}
	if MaxWaitMS != 600000 {
		t.Errorf("MaxWaitMS = %d", MaxWaitMS)
	}
	for id, ok := range map[string]bool{
		"A":                     true,
		"TASK-123-7f3a":         true,
		"abc-123_X.y-z":         true,
		strings.Repeat("x", 65): false,
		"bad id":                false,
		"":                      false,
	} {
		if got := ValidID(id); got != ok {
			t.Errorf("ValidID(%q) = %v, want %v", id, got, ok)
		}
	}
	for repo, ok := range map[string]bool{
		"org/repo":      true,
		"example-org/x": true,
		"org/a/b":       false,
		"org/":          false,
		"/repo":         false,
		"no_slash":      false,
		"org/bad repo":  false,
	} {
		if got := ValidRepo(repo); got != ok {
			t.Errorf("ValidRepo(%q) = %v, want %v", repo, got, ok)
		}
	}
	for base, ok := range map[string]bool{
		"main":        true,
		"release/1.0": true,
		"a/../b":      false,
		"weird..dots": false,
		"":            false,
	} {
		if got := ValidBase(base); got != ok {
			t.Errorf("ValidBase(%q) = %v, want %v", base, got, ok)
		}
	}
	if EnvJobID != "HERDR_SOHO_JOB_ID" || EnvJobSeq != "HERDR_SOHO_JOB_SEQ" ||
		EnvJobEvent != "HERDR_SOHO_JOB_EVENT" || EnvJobIdempotencyKey != "HERDR_SOHO_JOB_IDEMPOTENCY_KEY" {
		t.Error("wake hook environment names drifted")
	}
	if CapEphemeralJob != "ephemeral_job" || CapJobEvents != "job_events" {
		t.Error("capability keys drifted")
	}
}
