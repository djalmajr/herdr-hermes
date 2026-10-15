package registry

import (
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/djalmajr/nau/internal/outbox"
)

// ApplyPoll applies the outcome of polling one machine, in one
// transaction: the node facts, the workspace rows, the disappeared rows
// and the pane waits. The request is admitted whole or refused whole:
// a new node past MaxNodes or a waits total past MaxWaits refuses the
// poll and rolls back every write of the transaction, the retention
// sweep included. A refusal writes its alert line after the transaction,
// with the kind and counts only. A confirmed poll runs the high-count
// check after the transaction, outside it.
func (s *Store) ApplyPoll(p PollResult) error {
	if err := validateMachine(p.Maquina); err != nil {
		return err
	}
	p, err := normalizePollResult(p)
	if err != nil {
		return err
	}
	var nodeRefusal bool
	err = s.tx(func(tx *sql.Tx) error {
		now := s.now()
		// The ended rows past the retention leave before the poll
		// lands, and roll back with the rest when the request is
		// refused below.
		if err := sweepEnded(tx, s.endedRetention, now); err != nil {
			return err
		}
		at := p.At
		if p.OK {
			if err := admitNewNode(tx, p.Maquina); err != nil {
				nodeRefusal = errors.Is(err, ErrInvalid)
				return err
			}
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
		if err := admitNewNode(tx, p.Maquina); err != nil {
			nodeRefusal = errors.Is(err, ErrInvalid)
			return err
		}
		atTS := at.Format(outbox.TSLayout)
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
	if err == nil {
		// The high-count check runs after the committed transaction,
		// outside it; it never blocks, undoes or skips the poll.
		s.checkHighCount()
		return nil
	}
	// The refusal alerts are written after the transaction, outside it,
	// with the kind and counts only.
	var waitsErr *waitsRefusal
	switch {
	case errors.As(err, &waitsErr):
		s.alertWaits(waitsErr.storedTotal, waitsErr.snapshotBlocked, MaxWaits)
	case nodeRefusal:
		s.alertNodes(s.nodeCount(), MaxNodes)
	}
	return err
}

// touchPollNode records the poll: the last poll and the status move on
// every ok poll; the contact moves forward only, by the unix column, so
// an older poll never regresses a newer push contact.
func touchPollNode(tx *sql.Tx, maquina, herdrVersion string, at time.Time) error {
	atTS := at.Format(outbox.TSLayout)
	_, err := tx.Exec(`INSERT INTO nodes(maquina, ultimo_poll, poll_status, ultimo_contato, ultimo_contato_unix, herdr_version)
		VALUES(?, ?, 'ok', ?, ?, ?)
		ON CONFLICT(maquina) DO UPDATE SET
			ultimo_poll = excluded.ultimo_poll,
			poll_status = excluded.poll_status,
			ultimo_contato = CASE WHEN ultimo_contato_unix IS NULL OR ultimo_contato_unix < excluded.ultimo_contato_unix THEN excluded.ultimo_contato ELSE ultimo_contato END,
			ultimo_contato_unix = CASE WHEN ultimo_contato_unix IS NULL OR ultimo_contato_unix < excluded.ultimo_contato_unix THEN excluded.ultimo_contato_unix ELSE ultimo_contato_unix END,
			herdr_version = CASE WHEN excluded.herdr_version IS NULL THEN herdr_version ELSE excluded.herdr_version END`,
		maquina, atTS, atTS, at.Unix(), nullStr(herdrVersion))
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

// applyPollWorkspaces upserts one job row per seen workspace,
// re-derives the owner of an hm row from its current label, binds the
// matching claims and clears the rows that disappeared. The snapshot's
// key set is computed first and the disappeared rows are dropped before
// any new row is inserted, so a poll that replaces the rows it no longer
// sees applies the replacement in the same poll. The jobs kind has no
// count cap: every new workspace row is admitted, and a refusal of the
// whole poll (the nodes or the waits cap) rolls everything back, the
// reconciled drop included.
func applyPollWorkspaces(tx *sql.Tx, p PollResult, at, now time.Time) error {
	atTS := at.Format(outbox.TSLayout)
	type wsInfo struct {
		ws     PolledWorkspace
		chave  string
		origem string
	}
	var infos []wsInfo
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
		infos = append(infos, wsInfo{ws: ws, chave: chave, origem: origem})
	}
	// The rows the poll no longer sees leave before the new rows are
	// inserted.
	if err := dropUnseen(tx, p.Maquina, seen); err != nil {
		return err
	}
	for _, info := range infos {
		ws, chave, origem := info.ws, info.chave, info.origem
		row, err := jobRowOf(tx, p.Maquina, chave)
		if err != nil {
			return err
		}
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
			if origem == OrigemWorkspace {
				// The hm row re-derives its owner from the current label:
				// a resolved name sets it, an unparsable one clears the
				// name-derived owner and the ref; the claim binding runs
				// after and may fill a null agente.
				if n, resolved := ParseWorkspaceName(ws.Label); resolved {
					sets = append(sets, "agente = ?", "agente_fonte = ?", "trabalho = ?", "ref = ?")
					args = append(args, n.Agente, AgenteFonteNome, n.Trabalho, n.Ref)
				} else {
					sets = append(sets,
						"trabalho = NULL",
						"ref = NULL",
						"agente = CASE WHEN agente_fonte = ? THEN NULL ELSE agente END",
						"agente_fonte = CASE WHEN agente_fonte = ? THEN NULL ELSE agente_fonte END")
					args = append(args, AgenteFonteNome, AgenteFonteNome)
				}
			}
			if moves {
				// The last activity moves forward only, by the unix
				// column, so a poll that started before a newer push
				// never regresses it.
				sets = append(sets,
					"ultima_atividade = CASE WHEN ultima_atividade_unix IS NULL OR ultima_atividade_unix < ? THEN ? ELSE ultima_atividade END",
					"ultima_atividade_unix = CASE WHEN ultima_atividade_unix IS NULL OR ultima_atividade_unix < ? THEN ? ELSE ultima_atividade_unix END")
				args = append(args, at.Unix(), atTS, at.Unix(), at.Unix())
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
	return nil
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

// waitsRefusal is the whole-poll waits refusal: the ErrInvalid wrap
// "waits limit reached" plus the counts of the post-transaction alert
// line (the stored total and the blocked panes of the snapshot).
type waitsRefusal struct {
	storedTotal     int
	snapshotBlocked int
	err             error
}

func (e *waitsRefusal) Error() string { return e.err.Error() }
func (e *waitsRefusal) Unwrap() error { return e.err }

// replaceWaits replaces the machine's pane waits by the blocked panes
// of this poll: the since of a pane that was already waiting is kept, a
// new pane starts at the poll time. The stored pane waits of every
// machine are bounded by MaxWaits in total: this machine's old waits are
// removed before the count, so a node that replaces its own waits at the
// cap succeeds; a database already over the cap may shrink or stay
// equal, never grow, and a refusal never deletes its rows. Two blocked
// panes with the same bounded pane id in one snapshot refuse the poll
// with the shape error, not the raw primary-key error.
func replaceWaits(tx *sql.Tx, p PollResult, at time.Time) error {
	atTS := at.Format(outbox.TSLayout)
	var storedTotal int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM waits`).Scan(&storedTotal); err != nil {
		return err
	}
	var machineWaits int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM waits WHERE maquina = ?`, p.Maquina).Scan(&machineWaits); err != nil {
		return err
	}
	// The blocked panes of the snapshot; two with the same bounded pane
	// id would collide on the primary key, so they are a shape error.
	blocked := 0
	seen := make(map[string]bool)
	for _, pane := range p.Panes {
		if pane.AgentStatus != "blocked" {
			continue
		}
		if seen[pane.PaneID] {
			return invalidErr("panes", "duplicate pane")
		}
		seen[pane.PaneID] = true
		blocked++
	}
	// The final total: the other machines' waits plus this snapshot's
	// blocked panes, this machine's old waits removed before counting.
	finalTotal := storedTotal - machineWaits + blocked
	if finalTotal > MaxWaits && finalTotal > storedTotal {
		// The whole poll is refused; the alert is written by the caller
		// after the transaction.
		return &waitsRefusal{storedTotal: storedTotal, snapshotBlocked: blocked, err: invalidErr("waits", "limit reached")}
	}
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
