package registry

import (
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/djalmajr/herdr-hermes/internal/router"
)

// Field rules of the shared validation helpers. maquina reuses the router
// label rule; every other field keeps its own.
var (
	agentePattern      = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,63}$`)
	cardPattern        = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9]{0,15}-[0-9]{1,9}$`)
	projetoPartPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,100}$`)
	branchPattern      = regexp.MustCompile(`^[A-Za-z0-9._/-]{1,200}$`)
	workspacePattern   = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)
	jobIDPattern       = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)
	runnerPattern      = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,31}$`)
	digitsPattern      = regexp.MustCompile(`^[0-9]+$`)
	claimIDPattern     = regexp.MustCompile(`^[0-9a-f]{32}$`)
)

// invalidErr wraps ErrInvalid with the field and the rule only; the
// message is safe to show to the caller.
func invalidErr(field, rule string) error {
	return fmt.Errorf("%w: %s %s", ErrInvalid, field, rule)
}

func validateAgente(agente string) error {
	if agente == "" {
		return invalidErr("agente", "required")
	}
	if !agentePattern.MatchString(agente) {
		return invalidErr("agente", "bad format")
	}
	return nil
}

func validateMachine(maquina string) error {
	if !router.LabelPattern.MatchString(maquina) {
		return invalidErr("maquina", "bad format")
	}
	return nil
}

// normalizeCard returns the card upper-cased for storage.
func normalizeCard(card string) (string, error) {
	if !cardPattern.MatchString(card) {
		return "", invalidErr("card", "bad format")
	}
	return strings.ToUpper(card), nil
}

func normalizeProjeto(projeto string) (string, error) {
	parts := strings.Split(projeto, "/")
	if len(parts) != 2 || !projetoPartPattern.MatchString(parts[0]) || !projetoPartPattern.MatchString(parts[1]) {
		return "", invalidErr("projeto", "bad format")
	}
	return projeto, nil
}

func normalizeBranch(branch string) (string, error) {
	// The character class allows dots, so the dot run is checked apart.
	if !branchPattern.MatchString(branch) || strings.HasPrefix(branch, "-") || strings.Contains(branch, "..") {
		return "", invalidErr("branch", "bad format")
	}
	return branch, nil
}

// normalizePR reduces an accepted pull request form (67, #67, or a URL
// ending in /pull/67 optionally followed by / or #...) to its decimal
// number: 1..9 digits, no leading zero, greater than zero.
func normalizePR(pr string) (string, error) {
	n := pr
	if strings.HasPrefix(n, "#") {
		n = n[1:]
	}
	if i := strings.Index(n, "/pull/"); i >= 0 {
		n = n[i+len("/pull/"):]
		if j := strings.IndexAny(n, "/#"); j >= 0 {
			n = n[:j]
		}
	}
	if !digitsPattern.MatchString(n) || len(n) > 9 || strings.HasPrefix(n, "0") {
		return "", invalidErr("pr", "bad format")
	}
	return n, nil
}

func validateWorkspace(workspace string) error {
	if !workspacePattern.MatchString(workspace) {
		return invalidErr("workspace", "bad format")
	}
	return nil
}

func validateJobID(jobID string) error {
	if !jobIDPattern.MatchString(jobID) {
		return invalidErr("job_id", "bad format")
	}
	return nil
}

// validateTTL: 0 keeps the default; otherwise only 60..86400 seconds.
func validateTTL(seconds int) (time.Duration, error) {
	if seconds == 0 {
		return DefaultClaimTTL, nil
	}
	d := time.Duration(seconds) * time.Second
	if d < MinClaimTTL || d > MaxClaimTTL {
		return 0, invalidErr("ttl_s", "out of range")
	}
	return d, nil
}

func validateRunners(runners []string) error {
	if len(runners) > 32 {
		return invalidErr("runners", "too many")
	}
	seen := make(map[string]bool, len(runners))
	for _, r := range runners {
		if !runnerPattern.MatchString(r) {
			return invalidErr("runners", "bad format")
		}
		if seen[r] {
			return invalidErr("runners", "duplicate")
		}
		seen[r] = true
	}
	return nil
}

// validatePolicyScope checks the scope and returns the stored form of the
// valor: the card upper-cased, the other scopes as given.
func validatePolicyScope(escopo, valor string) (string, string, error) {
	switch escopo {
	case EscopoProjeto:
		parts := strings.Split(valor, "/")
		if len(parts) != 2 || !projetoPartPattern.MatchString(parts[0]) || !projetoPartPattern.MatchString(parts[1]) {
			return "", "", invalidErr("valor", "bad format")
		}
		return escopo, valor, nil
	case EscopoAgente:
		if !agentePattern.MatchString(valor) {
			return "", "", invalidErr("valor", "bad format")
		}
		return escopo, valor, nil
	case EscopoCard:
		if !cardPattern.MatchString(valor) {
			return "", "", invalidErr("valor", "bad format")
		}
		return escopo, strings.ToUpper(valor), nil
	}
	return "", "", invalidErr("escopo", "unknown scope")
}

// validateFolder checks the folder record fields; nothing on disk is ever
// touched.
func validateFolder(caminho, pedido string, tamanho *int64) error {
	if err := validateFolderPath(caminho); err != nil {
		return err
	}
	if !utf8.ValidString(pedido) || utf8.RuneCountInString(pedido) < 1 || utf8.RuneCountInString(pedido) > 280 || strings.ContainsRune(pedido, 0) {
		return invalidErr("pedido", "bad format")
	}
	if tamanho != nil && *tamanho < 0 {
		return invalidErr("tamanho_bytes", "negative")
	}
	return nil
}

func validateFolderPath(caminho string) error {
	if len(caminho) < 1 || len(caminho) > 1024 || !utf8.ValidString(caminho) || strings.ContainsAny(caminho, "\x00\r\n") {
		return invalidErr("caminho", "bad format")
	}
	return nil
}

// codePointLimit caps a text field in code points; empty is allowed.
func codePointLimit(field, value string, max int) error {
	if utf8.RuneCountInString(value) > max {
		return invalidErr(field, "too long")
	}
	return nil
}

// printableFact checks a node fact: capped in code points, no control
// characters.
func printableFact(field, value string, max int) error {
	if err := codePointLimit(field, value, max); err != nil {
		return err
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return invalidErr(field, "control character")
		}
	}
	return nil
}

// nullStr is nil for the empty string, so an absent field stores as NULL.
func nullStr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
