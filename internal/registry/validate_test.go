package registry

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// TestNormalizePR: the accepted forms reduce to the decimal number;
// leading zeros, zero itself, ten digits, a non-number or a URL that does
// not end in /pull/<n> are refused.
// Mutation captured: dropping the leading-zero or digit-count rule fails
// the 067, 0 and 1234567890 rows.
func TestNormalizePR(t *testing.T) {
	cases := []struct {
		in      string
		want    string
		wantErr bool
	}{
		{in: "67", want: "67"},
		{in: "#67", want: "67"},
		{in: "https://github.com/o/r/pull/67", want: "67"},
		{in: "https://github.com/o/r/pull/67/files", want: "67"},
		{in: "https://github.com/o/r/pull/67/", want: "67"},
		{in: "https://github.com/o/r/pull/67#issuecomment-1", want: "67"},
		{in: "https://o/r/pull/123456789", want: "123456789"},
		{in: "067", wantErr: true},
		{in: "#067", wantErr: true},
		{in: "0", wantErr: true},
		{in: "1234567890", wantErr: true},
		{in: "https://o/r/pull/0", wantErr: true},
		{in: "https://o/r/pull/067", wantErr: true},
		{in: "abc", wantErr: true},
		{in: "#abc", wantErr: true},
		{in: "67 ", wantErr: true},
		{in: "", wantErr: true},
		{in: "#", wantErr: true},
		{in: "https://o/r/issues/67", wantErr: true},
		{in: "https://o/r/pull/", wantErr: true},
		{in: "https://o/r/pull/67x", wantErr: true},
	}
	for _, tc := range cases {
		got, err := normalizePR(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("normalizePR(%q) = %q, want an error", tc.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("normalizePR(%q): %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("normalizePR(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestNormalizeCard: the card is upper-cased; a missing hyphen, a leading
// digit or a too-long part is refused.
// Mutation captured: skipping the pattern check passes dja229 through.
func TestNormalizeCard(t *testing.T) {
	cases := []struct {
		in      string
		want    string
		wantErr bool
	}{
		{in: "dja-229", want: "DJA-229"},
		{in: "DJA-229", want: "DJA-229"},
		{in: "a-1", want: "A-1"},
		{in: "abcdefghijklmnop-1", want: "ABCDEFGHIJKLMNOP-1"},
		{in: "DJA229", wantErr: true},
		{in: "1ja-229", wantErr: true},
		{in: "dja-", wantErr: true},
		{in: "-dja-229", wantErr: true},
		{in: "dja-22999999999", wantErr: true},
		{in: "abcdefghijklmnopr-1", wantErr: true},
		{in: "", wantErr: true},
	}
	for _, tc := range cases {
		got, err := normalizeCard(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("normalizeCard(%q) = %q, want an error", tc.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("normalizeCard(%q): %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("normalizeCard(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestNormalizeBranch: slashes and dots are allowed, but not a dot run or
// a leading hyphen.
// Mutation captured: dropping the .. check passes ../x.
func TestNormalizeBranch(t *testing.T) {
	cases := []struct {
		in      string
		wantErr bool
	}{
		{in: "feat/x"},
		{in: "release/1.0"},
		{in: "x.y/z"},
		{in: strings.Repeat("a", 200)},
		{in: "../x", wantErr: true},
		{in: "-x", wantErr: true},
		{in: "feat..x", wantErr: true},
		{in: strings.Repeat("a", 201), wantErr: true},
		{in: "", wantErr: true},
		{in: "feat/x y", wantErr: true},
	}
	for _, tc := range cases {
		_, err := normalizeBranch(tc.in)
		if tc.wantErr && err == nil {
			t.Errorf("normalizeBranch(%q): want an error", tc.in)
		}
		if !tc.wantErr && err != nil {
			t.Errorf("normalizeBranch(%q): %v", tc.in, err)
		}
	}
}

// TestNormalizeProjeto: exactly two well-formed parts.
// Mutation captured: accepting three parts passes acme/repo/extra.
func TestNormalizeProjeto(t *testing.T) {
	cases := []struct {
		in      string
		wantErr bool
	}{
		{in: "acme/repo"},
		{in: "Acme/Repo_1.x-y"},
		{in: strings.Repeat("a", 100) + "/" + strings.Repeat("b", 100)},
		{in: "acme", wantErr: true},
		{in: "acme/repo/extra", wantErr: true},
		{in: "/repo", wantErr: true},
		{in: "acme/", wantErr: true},
		{in: "acme/repo#1", wantErr: true},
		{in: strings.Repeat("a", 101) + "/repo", wantErr: true},
		{in: "", wantErr: true},
	}
	for _, tc := range cases {
		_, err := normalizeProjeto(tc.in)
		if tc.wantErr && err == nil {
			t.Errorf("normalizeProjeto(%q): want an error", tc.in)
		}
		if !tc.wantErr && err != nil {
			t.Errorf("normalizeProjeto(%q): %v", tc.in, err)
		}
	}
}

// TestValidateAgente and friends: the field rules of the shared helpers.
// Mutation captured: a widened agente pattern passes Dja.
func TestValidateAgente(t *testing.T) {
	ok := []string{"dja", "a-1", "a"}
	for _, a := range ok {
		if err := validateAgente(a); err != nil {
			t.Errorf("validateAgente(%q): %v", a, err)
		}
	}
	bad := []string{"", "Dja", "-x", "x_y", strings.Repeat("a", 65), "a a"}
	for _, a := range bad {
		if err := validateAgente(a); !errors.Is(err, ErrInvalid) {
			t.Errorf("validateAgente(%q) = %v, want ErrInvalid", a, err)
		}
	}
}

func TestValidateMachine(t *testing.T) {
	for _, m := range []string{"local", "mac-a", "1node", "a.b-c_d"} {
		if err := validateMachine(m); err != nil {
			t.Errorf("validateMachine(%q): %v", m, err)
		}
	}
	for _, m := range []string{"", "-x", "_x", "ma chine", strings.Repeat("a", 65)} {
		if err := validateMachine(m); !errors.Is(err, ErrInvalid) {
			t.Errorf("validateMachine(%q) = %v, want ErrInvalid", m, err)
		}
	}
}

func TestValidateWorkspace(t *testing.T) {
	for _, w := range []string{"hm-a-impl-1", "job-123", "x"} {
		if err := validateWorkspace(w); err != nil {
			t.Errorf("validateWorkspace(%q): %v", w, err)
		}
	}
	for _, w := range []string{"", "-x", "a b", strings.Repeat("a", 65)} {
		if err := validateWorkspace(w); !errors.Is(err, ErrInvalid) {
			t.Errorf("validateWorkspace(%q) = %v, want ErrInvalid", w, err)
		}
	}
}

func TestValidateJobID(t *testing.T) {
	for _, id := range []string{"abc-123", "A.1_-x", strings.Repeat("a", 64)} {
		if err := validateJobID(id); err != nil {
			t.Errorf("validateJobID(%q): %v", id, err)
		}
	}
	for _, id := range []string{"", strings.Repeat("a", 65), "ab cd"} {
		if err := validateJobID(id); !errors.Is(err, ErrInvalid) {
			t.Errorf("validateJobID(%q) = %v, want ErrInvalid", id, err)
		}
	}
}

// TestValidateTTL: 0 keeps the default; only 60..86400 seconds are
// accepted.
// Mutation captured: bounds set to 59..86401 pass the ttl_s 59 and 86401
// rows.
func TestValidateTTL(t *testing.T) {
	cases := []struct {
		in      int
		want    time.Duration
		wantErr bool
	}{
		{in: 0, want: DefaultClaimTTL},
		{in: 60, want: time.Minute},
		{in: 86400, want: MaxClaimTTL},
		{in: 59, wantErr: true},
		{in: 86401, wantErr: true},
		{in: -1, wantErr: true},
	}
	for _, tc := range cases {
		got, err := validateTTL(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("validateTTL(%d) = %v, want an error", tc.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("validateTTL(%d): %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("validateTTL(%d) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

// TestValidateRunners: at most 32 unique well-formed names.
// Mutation captured: a missing duplicate check passes [a, a].
func TestValidateRunners(t *testing.T) {
	if err := validateRunners(nil); err != nil {
		t.Errorf("validateRunners(nil): %v", err)
	}
	if err := validateRunners([]string{"a", "b_1", "c-2.d"}); err != nil {
		t.Errorf("validateRunners(3 good): %v", err)
	}
	good32 := make([]string, 32)
	for i := range good32 {
		good32[i] = "r" + string(rune('a'+i/26)) + string(rune('a'+i%26))
	}
	if err := validateRunners(good32); err != nil {
		t.Errorf("validateRunners(32 good): %v", err)
	}
	bad := [][]string{
		{"a", "a"},
		append(append([]string{}, good32...), "extra"),
		{"Runner-1"},
		{"-x"},
		{strings.Repeat("a", 33)},
	}
	for _, r := range bad {
		if err := validateRunners(r); !errors.Is(err, ErrInvalid) {
			t.Errorf("validateRunners(%v) = %v, want ErrInvalid", r, err)
		}
	}
}

// TestValidatePolicyScope: the valor is checked with the rule of its
// scope, the card upper-cased.
// Mutation captured: a missing card upper-casing stores dja-229 as is.
func TestValidatePolicyScope(t *testing.T) {
	cases := []struct {
		escopo, valor, want string
		wantErr             bool
	}{
		{escopo: "projeto", valor: "acme/repo", want: "acme/repo"},
		{escopo: "agente", valor: "dja", want: "dja"},
		{escopo: "card", valor: "dja-229", want: "DJA-229"},
		{escopo: "card", valor: "DJA-229", want: "DJA-229"},
		{escopo: "bogus", valor: "x", wantErr: true},
		{escopo: "card", valor: "DJA229", wantErr: true},
		{escopo: "agente", valor: "DJA", wantErr: true},
		{escopo: "projeto", valor: "acme", wantErr: true},
		{escopo: "agente", valor: "", wantErr: true},
	}
	for _, tc := range cases {
		escopo, valor, err := validatePolicyScope(tc.escopo, tc.valor)
		if tc.wantErr {
			if err == nil {
				t.Errorf("validatePolicyScope(%q, %q): want an error", tc.escopo, tc.valor)
			}
			continue
		}
		if err != nil {
			t.Errorf("validatePolicyScope(%q, %q): %v", tc.escopo, tc.valor, err)
			continue
		}
		if escopo != tc.escopo || valor != tc.want {
			t.Errorf("validatePolicyScope(%q, %q) = (%q, %q), want (%q, %q)", tc.escopo, tc.valor, escopo, valor, tc.escopo, tc.want)
		}
	}
}

// TestValidateFolder: the folder record field bounds.
// Mutation captured: a 1025-byte caminho bound set to 1024 passes the long
// path row.
func TestValidateFolder(t *testing.T) {
	if err := validateFolder("data/report", "delete after review", nil); err != nil {
		t.Errorf("validateFolder(good): %v", err)
	}
	if err := validateFolder("data/report", "x", ptr(int64(-1))); !errors.Is(err, ErrInvalid) {
		t.Errorf("validateFolder(negative size) = %v, want ErrInvalid", err)
	}
	n := int64(10)
	if err := validateFolder("data/report", "x", &n); err != nil {
		t.Errorf("validateFolder(size 10): %v", err)
	}
	bad := []struct {
		caminho, pedido string
		size            *int64
	}{
		{caminho: strings.Repeat("a", 1025), pedido: "x"},
		{caminho: "data\nreport", pedido: "x"},
		{caminho: "data\rreport", pedido: "x"},
		{caminho: "data\x00report", pedido: "x"},
		{caminho: "", pedido: "x"},
		{caminho: "data", pedido: ""},
		{caminho: "data", pedido: strings.Repeat("a", 281)},
		{caminho: "data", pedido: "a\x00b"},
	}
	for _, tc := range bad {
		if err := validateFolder(tc.caminho, tc.pedido, tc.size); !errors.Is(err, ErrInvalid) {
			t.Errorf("validateFolder(%q, %q) = %v, want ErrInvalid", tc.caminho, tc.pedido, err)
		}
	}
}

// TestPrintableFact: node facts are capped in code points and free of
// control characters.
// Mutation captured: dropping the control check passes "a\tb".
func TestPrintableFact(t *testing.T) {
	if err := printableFact("soho_version", strings.Repeat("v", 120), 120); err != nil {
		t.Errorf("printableFact(120 chars): %v", err)
	}
	if err := printableFact("soho_version", "1.2.3", 120); err != nil {
		t.Errorf("printableFact(1.2.3): %v", err)
	}
	bad := []struct {
		field, value string
		max          int
	}{
		{"soho_version", strings.Repeat("v", 121), 120},
		{"manutencao_motivo", strings.Repeat("m", 281), 280},
		{"soho_version", "1.2\t3", 120},
		{"manutencao_motivo", "m\n", 280},
	}
	for _, tc := range bad {
		if err := printableFact(tc.field, tc.value, tc.max); !errors.Is(err, ErrInvalid) {
			t.Errorf("printableFact(%q, %q) = %v, want ErrInvalid", tc.field, tc.value, err)
		}
	}
}

func TestCodePointLimit(t *testing.T) {
	if err := codePointLimit("motivo", strings.Repeat("e", 280), 280); err != nil {
		t.Errorf("codePointLimit(280): %v", err)
	}
	if err := codePointLimit("motivo", strings.Repeat("é", 281), 280); !errors.Is(err, ErrInvalid) {
		t.Errorf("codePointLimit(281 code points): want ErrInvalid")
	}
}

func ptr[T any](v T) *T { return &v }
