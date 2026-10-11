package registry

import (
	"context"
	"database/sql"
	"encoding/json"
	"sort"
	"time"

	"github.com/djalmajr/herdr-hermes/internal/outbox"
)

// Inventory reads the whole current state, as the jobs API returns it.
// It is one read transaction so the sections never mix two moments.
func (s *Store) Inventory(f Filter) (Inventory, error) {
	if f.Maquina != "" {
		if err := validateMachine(f.Maquina); err != nil {
			return Inventory{}, err
		}
	}
	if f.Agente != "" {
		if !agentePattern.MatchString(f.Agente) {
			return Inventory{}, invalidErr("agente", "bad format")
		}
	}
	txn, err := s.db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return Inventory{}, err
	}
	defer txn.Rollback()
	now := s.now()
	inv := Inventory{
		Gerado:    now.Format(outbox.TSLayout),
		Nodes:     []Node{},
		Jobs:      []Job{},
		Esperas:   []Wait{},
		Claims:    []Claim{},
		Politicas: []Policy{},
		Pastas:    []Folder{},
	}
	if err := invNodes(txn, f, now, int64(s.onlineWindow/time.Second), &inv); err != nil {
		return Inventory{}, err
	}
	if err := invJobs(txn, f, s.eventsPerJob, now, &inv); err != nil {
		return Inventory{}, err
	}
	if err := invEsperas(txn, f, &inv); err != nil {
		return Inventory{}, err
	}
	if err := invClaims(txn, f, now, &inv); err != nil {
		return Inventory{}, err
	}
	if err := invPoliticas(txn, &inv); err != nil {
		return Inventory{}, err
	}
	if err := invPastas(txn, f, &inv); err != nil {
		return Inventory{}, err
	}
	return inv, txn.Commit()
}

// invNodes lists the machines, sorted by label.
func invNodes(txn *sql.Tx, f Filter, now time.Time, windowSecs int64, inv *Inventory) error {
	q := `SELECT maquina, ultimo_contato, ultimo_contato_unix, ultimo_poll, poll_status, herdr_version, soho_version, runners, manutencao, manutencao_motivo
		FROM nodes`
	var args []any
	if f.Maquina != "" {
		q += ` WHERE maquina = ?`
		args = append(args, f.Maquina)
	}
	q += ` ORDER BY maquina`
	rows, err := txn.Query(q, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var n Node
		var ultimoContato, ultimoPoll, pollStatus, herdrVersion, sohoVersion, runners sql.NullString
		var ultimoContatoUnix, manutencao sql.NullInt64
		var manutencaoMotivo sql.NullString
		if err := rows.Scan(&n.Maquina, &ultimoContato, &ultimoContatoUnix, &ultimoPoll, &pollStatus, &herdrVersion, &sohoVersion, &runners, &manutencao, &manutencaoMotivo); err != nil {
			return err
		}
		n.Online = ultimoContatoUnix.Valid && ultimoContatoUnix.Int64 >= now.Unix()-windowSecs
		n.UltimoContato = nullOf(ultimoContato)
		n.UltimoPoll = nullOf(ultimoPoll)
		n.PollStatus = nullOf(pollStatus)
		n.HerdrVersion = nullOf(herdrVersion)
		n.SohoVersion = nullOf(sohoVersion)
		n.Manutencao = manutencao.Valid && manutencao.Int64 == 1
		n.ManutencaoMotivo = nullOf(manutencaoMotivo)
		// The runners are null when never reported, a list otherwise.
		if runners.Valid {
			var list []string
			if err := json.Unmarshal([]byte(runners.String), &list); err == nil {
				n.Runners = list
			}
		}
		inv.Nodes = append(inv.Nodes, n)
	}
	return rows.Err()
}

// invJobs lists the live jobs, sorted by machine then key, with their
// event ring and the idle seconds.
func invJobs(txn *sql.Tx, f Filter, eventsPerJob int, now time.Time, inv *Inventory) error {
	q := `SELECT maquina, chave, origem, job_id, workspace, workspace_id, agente, agente_fonte, trabalho, ref, card, projeto, branch, pr, fase, herdr_status, terminal, saida, esperando_humano, espera_motivo, ultimo_seq, ultima_atividade, ultima_atividade_unix, obsoleto
		FROM jobs WHERE encerrado = 0`
	var args []any
	if f.Maquina != "" {
		q += ` AND maquina = ?`
		args = append(args, f.Maquina)
	}
	if f.Agente != "" {
		q += ` AND agente = ?`
		args = append(args, f.Agente)
	}
	q += ` ORDER BY maquina, chave`
	rows, err := txn.Query(q, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var j Job
		var origem string
		var terminal, esperando, obsoleto int
		var ultimoSeq int64
		var ultimaUnix sql.NullInt64
		var jobID, workspace, workspaceID, agente, agenteFonte, trabalho, ref, card, projeto, branch, pr, fase, herdrStatus, esperaMotivo, ultima sql.NullString
		var saida sql.NullInt64
		if err := rows.Scan(&j.Maquina, &j.Chave, &origem, &jobID, &workspace, &workspaceID, &agente, &agenteFonte, &trabalho, &ref, &card, &projeto, &branch, &pr, &fase, &herdrStatus, &terminal, &saida, &esperando, &esperaMotivo, &ultimoSeq, &ultima, &ultimaUnix, &obsoleto); err != nil {
			return err
		}
		j.Origem = origem
		j.JobID = nullOf(jobID)
		j.Workspace = nullOf(workspace)
		j.WorkspaceID = nullOf(workspaceID)
		j.Agente = nullOf(agente)
		j.AgenteFonte = nullOf(agenteFonte)
		j.Trabalho = nullOf(trabalho)
		j.Ref = nullOf(ref)
		j.Card = nullOf(card)
		j.Projeto = nullOf(projeto)
		j.Branch = nullOf(branch)
		j.PR = nullOf(pr)
		j.Fase = nullOf(fase)
		j.HerdrStatus = nullOf(herdrStatus)
		j.Terminal = terminal == 1
		if saida.Valid {
			v := int(saida.Int64)
			j.Saida = &v
		}
		j.EsperandoHumano = esperando == 1
		j.EsperaMotivo = nullOf(esperaMotivo)
		j.UltimoSeq = ultimoSeq
		j.UltimaAtividade = nullOf(ultima)
		if ultima.Valid {
			idle := now.Unix() - ultimaUnix.Int64
			if idle < 0 {
				idle = 0
			}
			v := idle
			j.OciosoS = &v
		}
		j.Obsoleto = obsoleto == 1
		j.Eventos = []Event{}
		evRows, err := txn.Query(`SELECT seq, ts, tipo, resumo, refs FROM events WHERE maquina = ? AND chave = ? ORDER BY seq ASC LIMIT ?`,
			j.Maquina, j.Chave, eventsPerJob)
		if err != nil {
			return err
		}
		for evRows.Next() {
			var e Event
			var refs sql.NullString
			if err := evRows.Scan(&e.Seq, &e.TS, &e.Tipo, &e.Resumo, &refs); err != nil {
				evRows.Close()
				return err
			}
			if refs.Valid {
				var m map[string]any
				if err := json.Unmarshal([]byte(refs.String), &m); err == nil {
					e.Refs = m
				}
			}
			j.Eventos = append(j.Eventos, e)
		}
		if err := evRows.Err(); err != nil {
			evRows.Close()
			return err
		}
		evRows.Close()
		inv.Jobs = append(inv.Jobs, j)
	}
	return rows.Err()
}

// invEsperas lists the pane waits and the waiting jobs, sorted by machine
// then since. The agent filter applies to both sources by the owning
// agent, with the same predicate as the jobs list: a pane wait is kept
// when its workspace resolves to a non-closed job of the filtered agent
// (the pane's own name is the worker, not the owner) and a wait whose
// workspace resolves to no job is dropped there; a waiting job is kept
// when it belongs to the filtered agent. Without the filter the output
// is unchanged.
func invEsperas(txn *sql.Tx, f Filter, inv *Inventory) error {
	type esperaRow struct {
		w     Wait
		since int64
	}
	var out []esperaRow
	q := `SELECT maquina, pane, workspace_id, agente, desde, desde_unix FROM waits`
	var args []any
	if f.Maquina != "" {
		q += ` WHERE maquina = ?`
		args = append(args, f.Maquina)
	}
	rows, err := txn.Query(q, args...)
	if err != nil {
		return err
	}
	for rows.Next() {
		var w Wait
		var workspaceID, agente sql.NullString
		var desdeUnix int64
		if err := rows.Scan(&w.Maquina, &w.Pane, &workspaceID, &agente, &w.Desde, &desdeUnix); err != nil {
			rows.Close()
			return err
		}
		w.Tipo = EsperaPane
		w.Agente = nullOf(agente)
		// The pane's job, when its workspace id resolves to one; under
		// the agent filter the resolving job must belong to the filtered
		// agent (never the pane's own name, which is the worker), and a
		// wait that resolves to no job is dropped there, never a
		// dangling key.
		if workspaceID.Valid {
			q2 := `SELECT chave FROM jobs WHERE maquina = ? AND workspace_id = ? AND encerrado = 0`
			args2 := []any{w.Maquina, workspaceID.String}
			if f.Agente != "" {
				q2 += ` AND agente = ?`
				args2 = append(args2, f.Agente)
			}
			var chave string
			if err := txn.QueryRow(q2, args2...).Scan(&chave); err == nil {
				w.Chave = strPtr(chave)
			}
		}
		if f.Agente != "" && w.Chave == nil {
			continue
		}
		out = append(out, esperaRow{w: w, since: desdeUnix})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()

	// The waiting jobs: the facts come from the job's latest stored
	// event, when it has one.
	q = `SELECT maquina, chave, espera_motivo, ultima_atividade FROM jobs WHERE esperando_humano = 1 AND encerrado = 0`
	args = nil
	if f.Maquina != "" {
		q += ` AND maquina = ?`
		args = append(args, f.Maquina)
	}
	if f.Agente != "" {
		q += ` AND agente = ?`
		args = append(args, f.Agente)
	}
	rows, err = txn.Query(q, args...)
	if err != nil {
		return err
	}
	for rows.Next() {
		var w Wait
		var chave, motivo, ultima sql.NullString
		var since time.Time
		var hasSince bool
		if err := rows.Scan(&w.Maquina, &chave, &motivo, &ultima); err != nil {
			rows.Close()
			return err
		}
		w.Chave = strPtr(chave.String)
		var ts, resumo sql.NullString
		var refs sql.NullString
		if err := txn.QueryRow(`SELECT ts, resumo, refs FROM events WHERE maquina = ? AND chave = ? ORDER BY seq DESC LIMIT 1`,
			w.Maquina, chave.String).Scan(&ts, &resumo, &refs); err == nil {
			w.Resumo = nullOf(resumo)
			if refs.Valid {
				var m map[string]any
				if err := json.Unmarshal([]byte(refs.String), &m); err == nil {
					if p, ok := m["pane"].(string); ok {
						w.Pane = strPtr(p)
					}
				}
			}
			if t, err := time.Parse(time.RFC3339, ts.String); err == nil {
				since, hasSince = t, true
				w.Desde = ts.String
			}
		}
		w.Tipo = motivo.String
		if !hasSince {
			if ultima.Valid {
				if t, err := time.Parse(time.RFC3339, ultima.String); err == nil {
					since, hasSince = t, true
					w.Desde = ultima.String
				}
			}
		}
		out = append(out, esperaRow{w: w, since: sinceUnix(since, hasSince)})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()

	sort.Slice(out, func(i, j int) bool {
		if out[i].w.Maquina != out[j].w.Maquina {
			return out[i].w.Maquina < out[j].w.Maquina
		}
		return out[i].since < out[j].since
	})
	for _, e := range out {
		inv.Esperas = append(inv.Esperas, e.w)
	}
	return nil
}

// sinceUnix is the sortable instant of an espera; the zero instant sorts
// first when the since is unknown.
func sinceUnix(t time.Time, ok bool) int64 {
	if !ok {
		return 0
	}
	return t.Unix()
}

// invClaims lists the unexpired claims, sorted by creation.
func invClaims(txn *sql.Tx, f Filter, now time.Time, inv *Inventory) error {
	q := `SELECT id, agente, maquina, card, projeto, branch, pr, workspace, ref, criado, expira FROM claims WHERE expira_unix > ?`
	args := []any{now.Unix()}
	if f.Maquina != "" {
		q += ` AND maquina = ?`
		args = append(args, f.Maquina)
	}
	if f.Agente != "" {
		q += ` AND agente = ?`
		args = append(args, f.Agente)
	}
	q += ` ORDER BY criado_unix`
	rows, err := txn.Query(q, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var c Claim
		var maquina, card, projeto, branch, pr, workspace, ref sql.NullString
		if err := rows.Scan(&c.ID, &c.Agente, &maquina, &card, &projeto, &branch, &pr, &workspace, &ref, &c.Criado, &c.Expira); err != nil {
			return err
		}
		c.Maquina = nullOf(maquina)
		c.Card = nullOf(card)
		c.Projeto = nullOf(projeto)
		c.Branch = nullOf(branch)
		c.PR = nullOf(pr)
		c.Workspace = nullOf(workspace)
		c.Ref = nullOf(ref)
		inv.Claims = append(inv.Claims, c)
	}
	return rows.Err()
}

// invPoliticas lists the holds and priorities, sorted by scope then value.
func invPoliticas(txn *sql.Tx, inv *Inventory) error {
	rows, err := txn.Query(`SELECT escopo, valor, hold, prioridade, motivo, atualizado FROM policies ORDER BY escopo, valor`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var p Policy
		var hold int
		var prioridade sql.NullInt64
		var motivo sql.NullString
		if err := rows.Scan(&p.Escopo, &p.Valor, &hold, &prioridade, &motivo, &p.Atualizado); err != nil {
			return err
		}
		p.Hold = hold == 1
		if prioridade.Valid {
			v := int(prioridade.Int64)
			p.Prioridade = &v
		}
		p.Motivo = nullOf(motivo)
		inv.Politicas = append(inv.Politicas, p)
	}
	return rows.Err()
}

// invPastas lists the retained folders, sorted by machine then path.
func invPastas(txn *sql.Tx, f Filter, inv *Inventory) error {
	q := `SELECT maquina, caminho, tamanho_bytes, pedido, job, criado FROM folders`
	var args []any
	if f.Maquina != "" {
		q += ` WHERE maquina = ?`
		args = append(args, f.Maquina)
	}
	q += ` ORDER BY maquina, caminho`
	rows, err := txn.Query(q, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var p Folder
		var tamanho sql.NullInt64
		var job sql.NullString
		if err := rows.Scan(&p.Maquina, &p.Caminho, &tamanho, &p.Pedido, &job, &p.Criado); err != nil {
			return err
		}
		if tamanho.Valid {
			v := tamanho.Int64
			p.TamanhoBytes = &v
		}
		p.Job = nullOf(job)
		inv.Pastas = append(inv.Pastas, p)
	}
	return rows.Err()
}
