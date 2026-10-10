package registry

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"sort"
	"strings"
	"time"
)

// checkNorm is the validated and normalized input of one Check.
type checkNorm struct {
	agente    string
	maquina   string
	card      string
	projeto   string
	branch    string
	pr        string
	workspace string
	ref       string
	ttl       time.Duration
}

// normalizeCheckRequest applies the Check rules: the agente is required,
// at least one key is present, branch and pr need a projeto, and an hm
// workspace must belong to the request's own agente.
func normalizeCheckRequest(req CheckRequest) (checkNorm, error) {
	var n checkNorm
	if err := validateAgente(req.Agente); err != nil {
		return n, err
	}
	n.agente = req.Agente
	if req.Maquina != "" {
		if err := validateMachine(req.Maquina); err != nil {
			return n, err
		}
		n.maquina = req.Maquina
	}
	if req.Card == "" && req.Workspace == "" && req.Branch == "" && req.PR == "" {
		return n, invalidErr("check", "needs card, workspace, branch or pr")
	}
	if req.Branch != "" && req.Projeto == "" {
		return n, invalidErr("branch", "needs projeto")
	}
	if req.PR != "" && req.Projeto == "" {
		return n, invalidErr("pr", "needs projeto")
	}
	if req.Card != "" {
		card, err := normalizeCard(req.Card)
		if err != nil {
			return n, err
		}
		n.card = card
	}
	if req.Projeto != "" {
		projeto, err := normalizeProjeto(req.Projeto)
		if err != nil {
			return n, err
		}
		n.projeto = projeto
	}
	if req.Branch != "" {
		branch, err := normalizeBranch(req.Branch)
		if err != nil {
			return n, err
		}
		n.branch = branch
	}
	if req.PR != "" {
		pr, err := normalizePR(req.PR)
		if err != nil {
			return n, err
		}
		n.pr = pr
	}
	if req.Workspace != "" {
		if err := validateWorkspace(req.Workspace); err != nil {
			return n, err
		}
		n.workspace = req.Workspace
		if name, ok := ParseWorkspaceName(req.Workspace); ok {
			if name.Agente != n.agente {
				return n, invalidErr("workspace", "owner mismatch")
			}
			// The hm name joins the check by its (agente, ref) key.
			n.ref = name.Ref
		}
	}
	ttl, err := validateTTL(req.TTLS)
	if err != nil {
		return n, err
	}
	n.ttl = ttl
	return n, nil
}

// Check is the mandatory pre-dispatch check: one immediate transaction
// that deletes the expired claims, collects every hold, active job and
// live claim overlapping the request, and records a claim when nothing
// conflicts.
func (s *Store) Check(req CheckRequest) (CheckResult, error) {
	n, err := normalizeCheckRequest(req)
	if err != nil {
		return CheckResult{}, err
	}
	var result CheckResult
	err = s.tx(func(tx *sql.Tx) error {
		now := s.now()

		// Expired claims stop conflicting and are deleted.
		if _, err := tx.Exec(`DELETE FROM claims WHERE expira_unix <= ?`, now.Unix()); err != nil {
			return err
		}

		holds, err := s.holdConflicts(tx, n)
		if err != nil {
			return err
		}
		jobConflicts, err := s.jobConflicts(tx, n)
		if err != nil {
			return err
		}
		claimConflicts, err := s.claimConflicts(tx, n)
		if err != nil {
			return err
		}
		conflicts := append(holds, jobConflicts...)
		conflicts = append(conflicts, claimConflicts...)
		sortConflicts(conflicts)
		switch {
		case len(holds) > 0:
			result = CheckResult{Status: CheckHold, Conflitos: conflicts}
			return nil
		case len(conflicts) > 0:
			result = CheckResult{Status: CheckDuplicate, Conflitos: conflicts}
			return nil
		}

		id, err := newClaimID()
		if err != nil {
			return err
		}
		criado := now.Format(time.RFC3339)
		expira := now.Add(n.ttl)
		if _, err := tx.Exec(`INSERT INTO claims(id, agente, maquina, card, projeto, branch, pr, workspace, ref, criado, criado_unix, expira, expira_unix)
			VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			id, n.agente, nullStr(n.maquina), nullStr(n.card), nullStr(n.projeto), nullStr(n.branch), nullStr(n.pr), nullStr(n.workspace), nullStr(n.ref),
			criado, now.Unix(), expira.Format(time.RFC3339), expira.Unix()); err != nil {
			return err
		}
		result = CheckResult{
			Status: CheckOK,
			Claim: &Claim{
				ID:        id,
				Agente:    n.agente,
				Maquina:   nullStr(n.maquina),
				Card:      nullStr(n.card),
				Projeto:   nullStr(n.projeto),
				Branch:    nullStr(n.branch),
				PR:        nullStr(n.pr),
				Workspace: nullStr(n.workspace),
				Ref:       nullStr(n.ref),
				Criado:    criado,
				Expira:    expira.Format(time.RFC3339),
			},
		}
		return nil
	})
	return result, err
}

// holdConflicts finds the hold policies on the request's agente, projeto
// (case-insensitive) and card.
type policyScope struct {
	escopo, valor string
}

func (s *Store) holdConflicts(tx *sql.Tx, n checkNorm) ([]Conflict, error) {
	scopes := []policyScope{{EscopoAgente, strings.ToLower(n.agente)}}
	if n.projeto != "" {
		scopes = append(scopes, policyScope{EscopoProjeto, strings.ToLower(n.projeto)})
	}
	if n.card != "" {
		scopes = append(scopes, policyScope{EscopoCard, strings.ToLower(n.card)})
	}
	var holds []Conflict
	for _, sc := range scopes {
		rows, err := tx.Query(`SELECT valor, motivo FROM policies WHERE hold = 1 AND escopo = ? AND LOWER(valor) = ?`, sc.escopo, sc.valor)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var valor string
			var motivo sql.NullString
			if err := rows.Scan(&valor, &motivo); err != nil {
				rows.Close()
				return nil, err
			}
			c := Conflict{Tipo: ConflitoHold, Campo: sc.escopo, Valor: valor}
			if motivo.Valid {
				c.Motivo = nullStr(motivo.String)
			}
			holds = append(holds, c)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		rows.Close()
	}
	return holds, nil
}

// jobConflicts matches the request's keys against every active job
// (encerrado = 0 and terminal = 0; obsolete ones included: unknown is not
// free). One conflict is reported per matched key, so a job matched on
// several keys yields several.
func (s *Store) jobConflicts(tx *sql.Tx, n checkNorm) ([]Conflict, error) {
	rows, err := tx.Query(`SELECT maquina, chave, agente, workspace, card, projeto, branch, pr, ref
		FROM jobs WHERE encerrado = 0 AND terminal = 0`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Conflict
	for rows.Next() {
		var maquina, chave string
		var agente, workspace, card, projeto, branch, pr, ref sql.NullString
		if err := rows.Scan(&maquina, &chave, &agente, &workspace, &card, &projeto, &branch, &pr, &ref); err != nil {
			return nil, err
		}
		base := func(campo, valor string) Conflict {
			c := Conflict{Tipo: ConflitoJob, Campo: campo, Valor: valor, Maquina: nullStr(maquina), Chave: nullStr(chave)}
			if agente.Valid {
				c.Agente = nullStr(agente.String)
			}
			return c
		}
		if n.workspace != "" && workspace.Valid && workspace.String == n.workspace {
			out = append(out, base(CampoWorkspace, workspace.String))
		}
		if n.card != "" && card.Valid && card.String == n.card {
			out = append(out, base(CampoCard, card.String))
		}
		if n.branch != "" && branch.Valid && branch.String == n.branch && projeto.Valid && strings.EqualFold(projeto.String, n.projeto) {
			out = append(out, base(CampoBranch, branch.String))
		}
		if n.pr != "" && pr.Valid && pr.String == n.pr && projeto.Valid && strings.EqualFold(projeto.String, n.projeto) {
			out = append(out, base(CampoPR, pr.String))
		}
		if n.ref != "" && ref.Valid && agente.Valid && agente.String == n.agente && strings.EqualFold(RefBase(ref.String), RefBase(n.ref)) {
			out = append(out, base(CampoRef, ref.String))
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// claimConflicts matches the request's keys against the live claims left
// after the expiry delete.
func (s *Store) claimConflicts(tx *sql.Tx, n checkNorm) ([]Conflict, error) {
	rows, err := tx.Query(`SELECT id, agente, maquina, card, projeto, branch, pr, workspace, ref FROM claims`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Conflict
	for rows.Next() {
		var id, agente string
		var maquina, card, projeto, branch, pr, workspace, ref sql.NullString
		if err := rows.Scan(&id, &agente, &maquina, &card, &projeto, &branch, &pr, &workspace, &ref); err != nil {
			return nil, err
		}
		base := func(campo, valor string) Conflict {
			c := Conflict{Tipo: ConflitoClaim, Campo: campo, Valor: valor, Chave: nullStr(id), Agente: nullStr(agente)}
			if maquina.Valid {
				c.Maquina = nullStr(maquina.String)
			}
			return c
		}
		if n.workspace != "" && workspace.Valid && workspace.String == n.workspace {
			out = append(out, base(CampoWorkspace, workspace.String))
		}
		if n.card != "" && card.Valid && card.String == n.card {
			out = append(out, base(CampoCard, card.String))
		}
		if n.branch != "" && branch.Valid && branch.String == n.branch && projeto.Valid && strings.EqualFold(projeto.String, n.projeto) {
			out = append(out, base(CampoBranch, branch.String))
		}
		if n.pr != "" && pr.Valid && pr.String == n.pr && projeto.Valid && strings.EqualFold(projeto.String, n.projeto) {
			out = append(out, base(CampoPR, pr.String))
		}
		if n.ref != "" && ref.Valid && strings.EqualFold(RefBase(ref.String), RefBase(n.ref)) && agente == n.agente {
			out = append(out, base(CampoRef, ref.String))
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// conflictRank orders the conflict kinds: holds first, then jobs, then
// claims.
func conflictRank(tipo string) int {
	switch tipo {
	case ConflitoHold:
		return 0
	case ConflitoJob:
		return 1
	default:
		return 2
	}
}

// sortConflicts orders the conflicts deterministically: tipo (hold < job
// < claim), then campo, maquina, chave.
func sortConflicts(conflicts []Conflict) {
	sort.Slice(conflicts, func(i, j int) bool {
		a, b := conflicts[i], conflicts[j]
		if ra, rb := conflictRank(a.Tipo), conflictRank(b.Tipo); ra != rb {
			return ra < rb
		}
		if a.Campo != b.Campo {
			return a.Campo < b.Campo
		}
		if am, bm := nullStrOrEmpty(a.Maquina), nullStrOrEmpty(b.Maquina); am != bm {
			return am < bm
		}
		return nullStrOrEmpty(a.Chave) < nullStrOrEmpty(b.Chave)
	})
}

func nullStrOrEmpty(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

// newClaimID is the claim id: 16 random bytes, lower-case hex.
func newClaimID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
