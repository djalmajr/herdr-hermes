package registry

import (
	"database/sql"
	"errors"
	"strings"
	"time"
)

// ApplyPoll applies the outcome of polling one machine, in one
// transaction: the node facts, the workspace rows, the disappeared rows
// and the pane waits.
func (s *Store) ApplyPoll(p PollResult) error {
	if err := validateMachine(p.Maquina); err != nil {
		return err
	}
	return s.tx(func(tx *sql.Tx) error {
		now := s.now()
		// The dead rows leave before the poll lands.
		if err := sweepDead(tx, now); err != nil {
			return err
		}
		at := p.At
		atTS := at.Format(time.RFC3339)
		if p.OK {
			if err := touchPollNode(tx, p.Maquina, p.HerdrVersion, at); err != nil {
				return err
			}
			if err := applyPollWorkspaces(tx, p, at, now); err != nil {
				return err
			}
			return replaceWaits(tx, p, at)
		}
		// A failed poll only ages the machine: its jobs go obsolete,
		// the waits are kept.
		if _, err := tx.Exec(`INSERT INTO nodes(maquina, ultimo_poll, poll_status) VALUES(?,?,?)
			ON CONFLICT(maquina) DO UPDATE SET
				ultimo_poll = excluded.ultimo_poll,
				poll_status = excluded.poll_status`,
			p.Maquina, atTS, p.Motivo); err != nil {
			return err
		}
		_, err := tx.Exec(`UPDATE jobs SET obsoleto = 1 WHERE maquina = ?`, p.Maquina)
		return err
	})
}

// touchPollNode records the poll: the last poll, the status and, when the
// poll was valid, the contact and the version (never cleared by an empty
// one).
func touchPollNode(tx *sql.Tx, maquina, herdrVersion string, at time.Time) error {
	atTS := at.Format(time.RFC3339)
	_, err := tx.Exec(`INSERT INTO nodes(maquina, ultimo_poll, poll_status, ultimo_contato, ultimo_contato_unix, herdr_version)
		VALUES(?, 'ok', 'ok', ?, ?, ?)
		ON CONFLICT(maquina) DO UPDATE SET
			ultimo_poll = excluded.ultimo_poll,
			poll_status = excluded.poll_status,
			ultimo_contato = excluded.ultimo_contato,
			ultimo_contato_unix = excluded.ultimo_contato_unix,
			herdr_version = CASE WHEN excluded.herdr_version IS NULL THEN herdr_version ELSE excluded.herdr_version END`,
		maquina, atTS, at.Unix(), nullStr(herdrVersion))
	return err
}

// workspaceChave reduces a polled workspace label to its job key: the
// contract id of a job-<id> label, "ws:" + the workspace id of an hm-*
// label, empty for the workspaces that are not jobs.
func workspaceChave(label, id string) (key, origem string, ok bool) {
	if jobID, isJob := JobIDFromWorkspace(label); isJob {
		return jobID, OrigemJob, true
	}
	if strings.HasPrefix(label, "hm-") && id != "" {
		return "ws:" + id, OrigemWorkspace, true
	}
	return "", "", false
}

// applyPollWorkspaces upserts one job row per seen workspace, binds the
// matching claims and clears the rows that disappeared.
func applyPollWorkspaces(tx *sql.Tx, p PollResult, at, now time.Time) error {
	atTS := at.Format(time.RFC3339)
	seen := make(map[string]bool, len(p.Workspaces))
	for _, ws := range p.Workspaces {
		chave, origem, ok := workspaceChave(ws.Label, ws.ID)
		if !ok {
			// A personal workspace never becomes a job.
			continue
		}
		row, err := jobRowOf(tx, p.Maquina, chave)
		if err != nil {
			return err
		}
		if row != nil && row.encerrado == 1 {
			// A tombstoned id is skipped.
			continue
		}
		seen[chave] = true
		// The last activity moves when the row is new, the agent is
		// working, or the status differs from the stored one (a stored
		// null differs from any reported status).
		moves := row == nil || ws.AgentStatus == "working" || row.herdrStatus == nil || ws.AgentStatus != *row.herdrStatus
		if row == nil {
			var agente, fonte, trabalho, ref, jobID *string
			if origem == OrigemJob {
				jobID = strPtr(chave)
			} else if n, resolved := ParseWorkspaceName(ws.Label); resolved {
				agente, fonte, trabalho, ref = &n.Agente, strPtr(AgenteFonteNome), &n.Trabalho, &n.Ref
			}
			var ultima *string
			var ultimaUnix int64
			if moves {
				ultima, ultimaUnix = strPtr(atTS), at.Unix()
			}
			if _, err := tx.Exec(`INSERT INTO jobs(maquina, chave, origem, job_id, workspace, workspace_id, agente, agente_fonte, trabalho, ref, herdr_status, fonte_poll, visto_poll, ultima_atividade, ultima_atividade_unix)
				VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
				p.Maquina, chave, origem, jobID, strPtr(ws.Label), strPtr(ws.ID), agente, fonte, trabalho, ref, strPtr(ws.AgentStatus), 1, strPtr(atTS), ultima, ultimaUnix); err != nil {
				return err
			}
		} else {
			var sets []string
			var args []any
			sets = append(sets, "workspace = ?", "workspace_id = ?", "herdr_status = ?", "obsoleto = 0", "visto_poll = ?", "fonte_poll = 1")
			args = append(args, ws.Label, ws.ID, ws.AgentStatus, atTS)
			if moves {
				sets = append(sets, "ultima_atividade = ?", "ultima_atividade_unix = ?")
				args = append(args, atTS, at.Unix())
			}
			args = append(args, p.Maquina, chave)
			if _, err := tx.Exec(`UPDATE jobs SET `+strings.Join(sets, ", ")+" WHERE maquina = ? AND chave = ?", args...); err != nil {
				return err
			}
		}
		if err := bindClaim(tx, p.Maquina, chave, ws.Label, now); err != nil {
			return err
		}
	}
	// The rows the poll no longer sees.
	return dropUnseen(tx, p.Maquina, seen)
}

// bindClaim copies the fields of the newest unexpired claim that names
// the workspace into the row's null fields; the claim's agent fills a
// row that has no owner.
func bindClaim(tx *sql.Tx, maquina, chave, label string, now time.Time) error {
	var card, projeto, branch, pr, agente sql.NullString
	err := tx.QueryRow(`SELECT card, projeto, branch, pr, agente FROM claims
		WHERE workspace = ? AND expira_unix > ? ORDER BY criado_unix DESC LIMIT 1`,
		label, now.Unix()).
		Scan(&card, &projeto, &branch, &pr, &agente)
	if errors.Is(err, errNoRow) {
		return nil // no claim: nothing to bind
	}
	if err != nil {
		return err
	}
	_, err = tx.Exec(`UPDATE jobs SET
		card = COALESCE(card, ?),
		projeto = COALESCE(projeto, ?),
		branch = COALESCE(branch, ?),
		pr = COALESCE(pr, ?),
		agente = CASE WHEN agente IS NULL THEN ? ELSE agente END,
		agente_fonte = CASE WHEN agente IS NULL THEN ? ELSE agente_fonte END
		WHERE maquina = ? AND chave = ?`,
		nullOf(card), nullOf(projeto), nullOf(branch), nullOf(pr), nullOf(agente), strPtr(AgenteFonteClaim), maquina, chave)
	return err
}

// dropUnseen clears the machine's rows that the poll no longer reports:
// the workspace rows and the poll-only job rows are deleted (with their
// events), a pushed job row keeps itself but loses its workspace facts.
// Session rows and tombstones are untouched.
func dropUnseen(tx *sql.Tx, maquina string, seen map[string]bool) error {
	rows, err := tx.Query(`SELECT chave, origem, fonte_push, encerrado FROM jobs WHERE maquina = ?`, maquina)
	if err != nil {
		return err
	}
	type rowState struct {
		chave     string
		origem    string
		fontePush int
		encerrado int
	}
	var all []rowState
	for rows.Next() {
		var r rowState
		if err := rows.Scan(&r.chave, &r.origem, &r.fontePush, &r.encerrado); err != nil {
			rows.Close()
			return err
		}
		all = append(all, r)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	for _, r := range all {
		if seen[r.chave] || r.encerrado == 1 {
			continue
		}
		switch {
		case r.origem == OrigemWorkspace, r.origem == OrigemJob && r.fontePush == 0:
			if _, err := tx.Exec(`DELETE FROM events WHERE maquina = ? AND chave = ?`, maquina, r.chave); err != nil {
				return err
			}
			if _, err := tx.Exec(`DELETE FROM jobs WHERE maquina = ? AND chave = ?`, maquina, r.chave); err != nil {
				return err
			}
		case r.origem == OrigemJob:
			// A pushed job row stays until its cleanup event.
			if _, err := tx.Exec(`UPDATE jobs SET workspace = NULL, workspace_id = NULL, herdr_status = NULL
				WHERE maquina = ? AND chave = ?`, maquina, r.chave); err != nil {
				return err
			}
		}
	}
	return nil
}

// replaceWaits replaces the machine's pane waits by the blocked panes of
// this poll: the since of a pane that was already waiting is kept, a new
// pane starts at the poll time.
func replaceWaits(tx *sql.Tx, p PollResult, at time.Time) error {
	atTS := at.Format(time.RFC3339)
	old, err := tx.Query(`SELECT pane, desde, desde_unix FROM waits WHERE maquina = ?`, p.Maquina)
	if err != nil {
		return err
	}
	type waitState struct {
		desde     string
		desdeUnix int64
	}
	kept := make(map[string]waitState)
	for old.Next() {
		var pane string
		var w waitState
		if err := old.Scan(&pane, &w.desde, &w.desdeUnix); err != nil {
			old.Close()
			return err
		}
		kept[pane] = w
	}
	if err := old.Err(); err != nil {
		old.Close()
		return err
	}
	old.Close()
	if _, err := tx.Exec(`DELETE FROM waits WHERE maquina = ?`, p.Maquina); err != nil {
		return err
	}
	for _, pane := range p.Panes {
		if pane.AgentStatus != "blocked" {
			continue
		}
		desde, desdeUnix := atTS, at.Unix()
		if w, ok := kept[pane.PaneID]; ok {
			desde, desdeUnix = w.desde, w.desdeUnix
		}
		if _, err := tx.Exec(`INSERT INTO waits(maquina, pane, workspace_id, agente, desde, desde_unix) VALUES(?,?,?,?,?,?)`,
			p.Maquina, pane.PaneID, nullStr(pane.WorkspaceID), nullStr(pane.Name), desde, desdeUnix); err != nil {
			return err
		}
	}
	return nil
}
