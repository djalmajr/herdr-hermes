package a2astore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aevent"

	"github.com/djalmajr/nau/internal/a2acore"
)

// RecordDecision records a pending decision: first the shape check
// (ValidateDecision, a refusal changes nothing), then the idempotency on
// the id — the same id with the same task, kind and text returns the
// stored decision, other content is a decision_conflict refusal —, then
// the task checks: a missing task is unknown_task, a terminal one
// terminal. The recorded_seq is the record order of the table: the
// maximum stored plus one.
func (s *Store) RecordDecision(ctx context.Context, d a2acore.Decision, now time.Time) (a2acore.Decision, *a2acore.Refusal, error) {
	if r := a2acore.ValidateDecision(d, s.limits); r != nil {
		return a2acore.Decision{}, r, nil
	}
	var out a2acore.Decision
	var refusal *a2acore.Refusal
	err := s.tx(ctx, func(tx *sql.Tx) error {
		var stored a2acore.Decision
		var refusalCode, recorded, applied string
		err := tx.QueryRow(`SELECT decision_id, task_id, kind, text, actor, state, COALESCE(refusal, ''), recorded, COALESCE(applied, '')
			FROM a2a_decisions WHERE decision_id = ?`, d.ID).
			Scan(&stored.ID, &stored.TaskID, &stored.Kind, &stored.Text, &stored.Actor, &stored.State, &refusalCode, &recorded, &applied)
		if err == nil {
			stored.Refusal, stored.RecordedAt, stored.AppliedAt = refusalCode, recorded, applied
			if stored.TaskID == d.TaskID && stored.Kind == d.Kind && stored.Text == d.Text {
				out = stored
				return nil
			}
			refusal = &a2acore.Refusal{Code: a2acore.RefuseDecisionConflict, Detail: "the decision id was recorded with other content"}
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		var state string
		err = tx.QueryRow(`SELECT state FROM a2a_tasks WHERE task_id = ?`, d.TaskID).Scan(&state)
		if errors.Is(err, sql.ErrNoRows) {
			refusal = &a2acore.Refusal{Code: a2acore.RefuseUnknownTask, Detail: "no such task"}
			return nil
		}
		if err != nil {
			return err
		}
		if a2acore.Terminal(state) {
			refusal = &a2acore.Refusal{Code: a2acore.RefuseTerminal, Detail: "the task is terminal"}
			return nil
		}
		var seq int
		if err := tx.QueryRow(`SELECT COALESCE(MAX(recorded_seq), 0) + 1 FROM a2a_decisions`).Scan(&seq); err != nil {
			return err
		}
		nowTS, _ := stamp(now)
		if _, err := tx.Exec(`INSERT INTO a2a_decisions(decision_id, task_id, kind, text, actor, state, refusal, recorded, recorded_seq)
			VALUES(?,?,?,?,?,?,?,?,?)`,
			d.ID, d.TaskID, string(d.Kind), d.Text, d.Actor, a2acore.DecisionPending, nil, nowTS, seq); err != nil {
			return err
		}
		out = a2acore.Decision{
			ID:         d.ID,
			TaskID:     d.TaskID,
			Kind:       d.Kind,
			Text:       d.Text,
			Actor:      d.Actor,
			State:      a2acore.DecisionPending,
			RecordedAt: nowTS,
		}
		return nil
	})
	if err != nil {
		return a2acore.Decision{}, nil, err
	}
	return out, refusal, nil
}

// PendingDecisions lists at most limit pending decisions in record
// order; a limit at or below zero lists none.
func (s *Store) PendingDecisions(ctx context.Context, limit int) ([]a2acore.Decision, error) {
	if limit <= 0 {
		return []a2acore.Decision{}, nil
	}
	rows, err := s.db.Query(`SELECT decision_id, task_id, kind, text, actor, state, COALESCE(refusal, ''), recorded, COALESCE(applied, '')
		FROM a2a_decisions WHERE state = ? ORDER BY recorded_seq ASC, decision_id ASC LIMIT ?`, a2acore.DecisionPending, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var decisions []a2acore.Decision
	for rows.Next() {
		var d a2acore.Decision
		if err := rows.Scan(&d.ID, &d.TaskID, &d.Kind, &d.Text, &d.Actor, &d.State, &d.Refusal, &d.RecordedAt, &d.AppliedAt); err != nil {
			return nil, err
		}
		decisions = append(decisions, d)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return decisions, nil
}

// RefuseDecision marks a pending decision refused with a code; a
// decision that is not pending changes nothing.
func (s *Store) RefuseDecision(ctx context.Context, id, code string, now time.Time) error {
	return s.tx(ctx, func(tx *sql.Tx) error {
		_, err := tx.Exec(`UPDATE a2a_decisions SET state = ?, refusal = ? WHERE decision_id = ? AND state = ?`,
			a2acore.DecisionRefused, code, id, a2acore.DecisionPending)
		return err
	})
}

// ApplyDecisionEvents applies the decision's events to a task without a
// live execution, in one immediate transaction: an already applied
// decision returns the task and no refusal (idempotent), a refused one
// returns a refusal with its code, a missing task refuses with
// unknown_task, a terminal one with terminal, any other state with
// not_awaiting_owner (the refusal is committed and returned); otherwise
// every event is applied in order with a2aevent.ApplyUpdate, the task is
// stored at version+1, the decision is marked applied, and a terminal
// result runs the terminal side effects.
func (s *Store) ApplyDecisionEvents(ctx context.Context, decisionID string, events []a2a.Event, now time.Time) (*a2a.Task, *a2acore.Refusal, error) {
	var task *a2a.Task
	var refusal *a2acore.Refusal
	err := s.tx(ctx, func(tx *sql.Tx) error {
		var taskID, state, refusalCode string
		err := tx.QueryRow(`SELECT task_id, state, COALESCE(refusal, '') FROM a2a_decisions WHERE decision_id = ?`, decisionID).
			Scan(&taskID, &state, &refusalCode)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("a2astore: no such decision %q", decisionID)
		}
		if err != nil {
			return err
		}
		var taskJSON []byte
		var taskState string
		err = tx.QueryRow(`SELECT task_json, state FROM a2a_tasks WHERE task_id = ?`, taskID).Scan(&taskJSON, &taskState)
		if errors.Is(err, sql.ErrNoRows) {
			if _, err := tx.Exec(`UPDATE a2a_decisions SET state = ?, refusal = ? WHERE decision_id = ? AND state = ?`,
				a2acore.DecisionRefused, a2acore.RefuseUnknownTask, decisionID, a2acore.DecisionPending); err != nil {
				return err
			}
			refusal = &a2acore.Refusal{Code: a2acore.RefuseUnknownTask, Detail: "no such task"}
			return nil
		}
		if err != nil {
			return err
		}
		stored, err := unmarshalTask(taskJSON)
		if err != nil {
			return err
		}
		switch {
		case state == a2acore.DecisionApplied:
			task = stored
			return nil
		case state == a2acore.DecisionRefused:
			refusal = &a2acore.Refusal{Code: refusalCode, Detail: "the decision was refused"}
			task = stored
			return nil
		case a2acore.Terminal(taskState):
			if _, err := tx.Exec(`UPDATE a2a_decisions SET state = ?, refusal = ? WHERE decision_id = ?`,
				a2acore.DecisionRefused, a2acore.RefuseTerminal, decisionID); err != nil {
				return err
			}
			refusal = &a2acore.Refusal{Code: a2acore.RefuseTerminal, Detail: "the task is terminal"}
			task = stored
			return nil
		case taskState != a2acore.StateWorking:
			if _, err := tx.Exec(`UPDATE a2a_decisions SET state = ?, refusal = ? WHERE decision_id = ?`,
				a2acore.DecisionRefused, a2acore.RefuseNotAwaitingOwner, decisionID); err != nil {
				return err
			}
			refusal = &a2acore.Refusal{Code: a2acore.RefuseNotAwaitingOwner, Detail: "the task is not awaiting the owner"}
			task = stored
			return nil
		}
		updated := stored
		for _, ev := range events {
			updated, err = a2aevent.ApplyUpdate(updated, ev)
			if err != nil {
				return fmt.Errorf("a2astore: decision event: %w", err)
			}
		}
		body, err := json.Marshal(updated)
		if err != nil {
			return fmt.Errorf("a2astore: task: %w", err)
		}
		newState := NeutralState(updated.Status.State)
		if _, err := tx.Exec(`UPDATE a2a_tasks SET task_json = ?, state = ?, version = version + 1, messages = ?, updated = ?, updated_nano = ?
			WHERE task_id = ?`,
			string(body), newState, len(updated.History), now.Format(tsLayout), now.UnixNano(), taskID); err != nil {
			return err
		}
		if _, err := tx.Exec(`UPDATE a2a_decisions SET state = ?, applied = ? WHERE decision_id = ?`,
			a2acore.DecisionApplied, now.Format(tsLayout), decisionID); err != nil {
			return err
		}
		if a2acore.Terminal(newState) {
			if err := s.terminalSideEffects(tx, taskID); err != nil {
				return err
			}
		}
		task = updated
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return task, refusal, nil
}
