package cli

import (
	"encoding/json"
	"strings"
	"testing"
)

func decisionRecord(t *testing.T, dir string, n int) map[string]any {
	t.Helper()
	lines := outboxLines(t, dir)
	if len(lines) != n {
		t.Fatalf("outbox has %d lines, want %d", len(lines), n)
	}
	var rec struct {
		Tipo           string          `json:"tipo"`
		Maquina        string          `json:"maquina"`
		Projeto        string          `json:"projeto"`
		JobID          *string         `json:"job_id"`
		IdempotencyKey string          `json:"idempotency_key"`
		Dados          json.RawMessage `json:"dados"`
	}
	if err := json.Unmarshal([]byte(lines[n-1]), &rec); err != nil {
		t.Fatalf("record: %v", err)
	}
	if rec.Tipo != "decision" || rec.Maquina != "m1" || rec.Projeto != "org/repo" {
		t.Fatalf("record envelope = %+v", rec)
	}
	var dados map[string]any
	if err := json.Unmarshal(rec.Dados, &dados); err != nil {
		t.Fatalf("dados: %v", err)
	}
	return dados
}

// TestDecision covers criterion 9 for decisions: the record shape, the
// 280-code-point cuts (multi-byte safe), the escopo limit, the resumo
// sources (positional or stdin) and the absence of person fields.
func TestDecision(t *testing.T) {
	dir := t.TempDir()
	setConfig(t, dir, map[string]string{"machine_label": "m1"})

	// Positional resumo.
	stdout, stderr, exit := runCLIStdin(t, dir, "", nil, "decision", "--projeto", "org/repo", "--escopo", "global", "--motivo", "why", "the resumo")
	if exit != 0 {
		t.Fatalf("decision exit = %d, stderr %q", exit, stderr)
	}
	if stdout != `{"seq":1}
` {
		t.Fatalf("decision stdout = %q, want {\"seq\":1}", stdout)
	}
	dados := decisionRecord(t, dir, 1)
	if len(dados) != 3 || dados["resumo"] != "the resumo" || dados["motivo"] != "why" || dados["escopo"] != "global" {
		t.Fatalf("dados = %v, want exactly resumo/motivo/escopo", dados)
	}

	// resumo from stdin (with '-' and a trailing newline trimmed).
	stdout, stderr, exit = runCLIStdin(t, dir, "resumo from stdin\n", nil, "decision", "--projeto", "org/repo", "--escopo", "projeto", "--motivo", "m2", "-")
	if exit != 0 || stdout != `{"seq":2}
` {
		t.Fatalf("decision from stdin: exit %d stdout %q stderr %q", exit, stdout, stderr)
	}
	dados = decisionRecord(t, dir, 2)
	if dados["resumo"] != "resumo from stdin" || dados["escopo"] != "projeto" {
		t.Fatalf("dados = %v", dados)
	}

	// --job sets the record job_id; a bad id is refused.
	stdout, stderr, exit = runCLIStdin(t, dir, "", nil, "decision", "--projeto", "org/repo", "--escopo", "global", "--motivo", "m3", "--job", "job-2", "r3")
	if exit != 0 {
		t.Fatalf("decision --job exit = %d, stderr %q", exit, stderr)
	}
	lines := outboxLines(t, dir)
	if !strings.Contains(lines[2], `"job_id":"job-2"`) {
		t.Fatalf("record with --job = %q", lines[2])
	}
	if _, _, exit := runCLIStdin(t, dir, "", nil, "decision", "--projeto", "org/repo", "--escopo", "global", "--motivo", "m4", "--job", "bad id", "r4"); exit != 2 {
		t.Errorf("decision with a bad --job: exit %d, want 2", exit)
	}

	// Cuts at 280 code points, by runes.
	longResumo := strings.Repeat("a", 270) + strings.Repeat("é", 20) // 290 code points
	longMotivo := strings.Repeat("ç", 290)
	stdout, stderr, exit = runCLIStdin(t, dir, "", nil, "decision", "--projeto", "org/repo", "--escopo", "global", "--motivo", longMotivo, longResumo)
	if exit != 0 {
		t.Fatalf("decision long exit = %d, stderr %q", exit, stderr)
	}
	dados = decisionRecord(t, dir, 4)
	resumo, _ := dados["resumo"].(string)
	motivo, _ := dados["motivo"].(string)
	if len([]rune(resumo)) != 280 || !strings.HasPrefix(resumo, strings.Repeat("a", 270)) {
		t.Fatalf("resumo cut = %d code points (prefix %q), want 280 starting with the 270 a's", len([]rune(resumo)), resumo[:10])
	}
	if len([]rune(motivo)) != 280 || !strings.HasPrefix(motivo, strings.Repeat("ç", 280)) {
		t.Fatalf("motivo cut = %d code points, want 280 ç's (no split rune)", len([]rune(motivo)))
	}
	if !strings.HasPrefix(resumo, strings.Repeat("a", 270)+strings.Repeat("é", 10)) {
		t.Fatalf("resumo cut the wrong end: %q", resumo[:275])
	}

	// Usage errors exit 2.
	cases := [][]string{
		{"decision"},
		{"decision", "--projeto", "org/repo", "--escopo", "global", "--motivo", "m"},
		{"decision", "--escopo", "global", "--motivo", "m", "r"},
		{"decision", "--projeto", "org/repo", "--motivo", "m", "r"},
		{"decision", "--projeto", "org/repo", "--escopo", "team", "--motivo", "m", "r"},
		{"decision", "--projeto", "org/repo", "--escopo", "global", "r"},
		{"decision", "--projeto", "org/repo", "--escopo", "global", "--motivo", "m", "r", "extra"},
		{"decision", "--projeto", "org/repo", "--escopo", "global", "--motivo", "m", "-", "r"},
		{"decision", "--projeto", "org", "--escopo", "global", "--motivo", "m", "r"},
		{"decision", "--projeto", "org/repo", "--escopo", "global", "--motivo", "m", "--bogus"},
	}
	for _, args := range cases {
		stdout, stderr, exit := runCLIStdin(t, dir, "", nil, args...)
		if exit != 2 || !strings.Contains(stderr, "herdr-hermes") || !strings.Contains(stdout, `"status":"2"`) {
			t.Errorf("%v: exit %d stderr %q stdout %q", args, exit, stderr, stdout)
		}
	}
	// The resumo from stdin over the 16 KiB cap exits 2; exactly at the cap
	// is accepted.
	over := strings.Repeat("x", 16*1024+1)
	if _, _, exit := runCLIStdin(t, dir, over, nil, "decision", "--projeto", "org/repo", "--escopo", "global", "--motivo", "m", "-"); exit != 2 {
		t.Error("decision resumo from stdin over 16 KiB must exit 2")
	}
	exact := strings.Repeat("x", 16*1024-1) + "\n" // 16 KiB with the trimmed newline
	stdout, stderr, exit = runCLIStdin(t, dir, exact, nil, "decision", "--projeto", "org/repo", "--escopo", "global", "--motivo", "m", "-")
	if exit != 0 {
		t.Errorf("decision resumo at exactly 16 KiB: exit %d stderr %q", exit, stderr)
	}
	// Empty resumo after the trim exits 2.
	if _, _, exit := runCLIStdin(t, dir, "\n", nil, "decision", "--projeto", "org/repo", "--escopo", "global", "--motivo", "m", "-"); exit != 2 {
		t.Error("decision with an empty resumo must exit 2")
	}
	// No person field at any level of the record.
	lines = outboxLines(t, dir)
	for _, l := range lines {
		for _, bad := range []string{`"pessoa"`, `"user"`, `"autor"`, `"email"`, `"nome"`} {
			if strings.Contains(l, bad) {
				t.Fatalf("record carries a person field %s: %s", bad, l)
			}
		}
	}
	// machine_label empty exits 2.
	dirEmpty := t.TempDir()
	if _, _, exit := runCLIStdin(t, dirEmpty, "", nil, "decision", "--projeto", "org/repo", "--escopo", "global", "--motivo", "m", "r"); exit != 2 {
		t.Errorf("decision with empty machine_label: exit %d, want 2", exit)
	}
}
