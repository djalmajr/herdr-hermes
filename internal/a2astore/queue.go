package a2astore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/djalmajr/nau/internal/a2acore"
	"github.com/djalmajr/nau/internal/notify"
)

// repointGrace is how old an admission whose task never committed must
// be before a retry of the same message re-points it to a new task. It
// is also how long the admission of a new task keeps a reservation of
// one open-task slot while its task is not yet committed.
const repointGrace = 60 * time.Second

// openTasksSQL counts the committed tasks that are not terminal.
const openTasksSQL = `SELECT COUNT(*) FROM a2a_tasks WHERE state NOT IN (?, ?, ?, ?)`

// reservedTasksSQL counts the live reservations: the distinct tasks of a
// new-task admission younger than repointGrace (received_unix is above
// the bound argument) whose a2a_tasks row does not exist yet.
const reservedTasksSQL = `SELECT COUNT(DISTINCT task_id) FROM a2a_ingress
	WHERE new_task = 1 AND received_unix > ? AND task_id NOT IN (SELECT task_id FROM a2a_tasks)`

// Admit runs, in one immediate transaction and in this order: the
// duplicate check on (subject, message id) — a duplicate whose task
// committed returns that task, one whose task never committed is re-
// pointed at the new task and its deliveries are reset to held —, the
// per (subject, session) fence (CheckFence), the limits of a new task
// (MaxTasksPerContext of the context, then MaxOpenTasks of the store:
// the committed non-terminal tasks plus the live reservations, that is
// the new-task admissions younger than repointGrace whose task is not
// committed yet, each task counted once), then the ingress insert (its
// id is the global admission order), the fence upsert and the planned
// deliveries (new task: the planned state, a continuation's held ones
// become pending, due at once, because the message is already committed
// to the task history). A refusal changes nothing. The reservation
// closes the window between this admission and the SDK task create,
// which is another transaction; an expired reservation stops counting,
// and Create makes the definitive check of the committed tasks.
func (s *Store) Admit(ctx context.Context, a a2acore.Admission) (a2acore.AdmitResult, error) {
	var res a2acore.AdmitResult
	err := s.tx(ctx, func(tx *sql.Tx) error {
		// 1. Duplicate: the store answers it before the fence.
		var existingIngress, existingReceived int64
		var existingTask string
		err := tx.QueryRow(`SELECT ingress_id, task_id, received_unix FROM a2a_ingress WHERE subject = ? AND message_id = ?`,
			a.Envelope.Subject, a.Envelope.MessageID).Scan(&existingIngress, &existingTask, &existingReceived)
		if err == nil {
			var n int
			if err := tx.QueryRow(`SELECT COUNT(*) FROM a2a_tasks WHERE task_id = ?`, existingTask).Scan(&n); err != nil {
				return err
			}
			// A task that committed, or one whose admission is younger
			// than repointGrace (the first request may still be about to
			// create it), answers the retry as a duplicate: only an
			// admission old enough to have been abandoned is re-pointed,
			// so two tasks never share one message.
			if n > 0 || s.now().Unix()-existingReceived < int64(repointGrace/time.Second) {
				res = a2acore.AdmitResult{Duplicate: true, DuplicateTaskID: existingTask}
				return nil
			}
			// An admission whose task never committed: re-point the
			// ingress and its deliveries (the context lives on the
			// ingress row), held again, the fence stands.
			if _, err := tx.Exec(`UPDATE a2a_ingress SET task_id = ?, context_id = ? WHERE ingress_id = ?`,
				a.TaskID, a.ContextID, existingIngress); err != nil {
				return err
			}
			if _, err := tx.Exec(`UPDATE a2a_deliveries SET task_id = ?, state = ?, attempts = 0,
				next_attempt_unix = NULL, lease_until_unix = NULL, first_attempt = NULL, last_attempt = NULL,
				accepted = NULL, accept_status = NULL, last_exit = NULL, last_error = NULL
				WHERE ingress_id = ?`, a.TaskID, a2acore.DeliveryHeld, existingIngress); err != nil {
				return err
			}
			res = a2acore.AdmitResult{IngressID: existingIngress}
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		// 2. Fence.
		var prev a2acore.FenceState
		var epoch, lastSeq int64
		err = tx.QueryRow(`SELECT epoch, last_seq FROM a2a_fences WHERE subject = ? AND session = ?`,
			a.Envelope.Subject, a.Envelope.Session).Scan(&epoch, &lastSeq)
		if err == nil {
			prev = a2acore.FenceState{Found: true, Epoch: epoch, LastSeq: lastSeq}
		} else if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		next, refusal := a2acore.CheckFence(prev, a.Envelope)
		if refusal != nil {
			res = a2acore.AdmitResult{Refusal: refusal}
			return nil
		}
		// 3. The limits of a new task.
		if a.NewTask {
			var contextTasks int
			if err := tx.QueryRow(`SELECT COUNT(*) FROM (
				SELECT task_id FROM a2a_tasks WHERE context_id = ?
				UNION
				SELECT task_id FROM a2a_ingress WHERE context_id = ? AND new_task = 1
				AND task_id NOT IN (SELECT task_id FROM a2a_tasks)
			)`, a.ContextID, a.ContextID).Scan(&contextTasks); err != nil {
				return err
			}
			if contextTasks >= s.limits.MaxTasksPerContext {
				res = a2acore.AdmitResult{Refusal: &a2acore.Refusal{Code: a2acore.RefuseLoopLimit, Detail: "the context is at its task limit"}}
				return nil
			}
			var open, reserved int
			if err := tx.QueryRow(openTasksSQL,
				a2acore.StateCompleted, a2acore.StateCanceled, a2acore.StateFailed, a2acore.StateRejected).Scan(&open); err != nil {
				return err
			}
			if err := tx.QueryRow(reservedTasksSQL, s.now().Unix()-int64(repointGrace/time.Second)).Scan(&reserved); err != nil {
				return err
			}
			if open+reserved >= s.limits.MaxOpenTasks {
				res = a2acore.AdmitResult{Refusal: &a2acore.Refusal{Code: a2acore.RefuseCapacity, Detail: "the open tasks are at their limit"}}
				return nil
			}
		}
		// 4. The fresh admission.
		now := s.now()
		nowTS, nowUnix := stamp(now)
		insert, err := tx.Exec(`INSERT INTO a2a_ingress(subject, message_id, task_id, context_id, project, job, session, epoch, seq, new_task, text_bytes, received, received_unix)
			VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			a.Envelope.Subject, a.Envelope.MessageID, a.TaskID, a.ContextID, a.Envelope.Project, nullStr(a.Envelope.Job),
			a.Envelope.Session, a.Envelope.Epoch, a.Envelope.Seq, boolInt(a.NewTask), a.Envelope.TextBytes, nowTS, nowUnix)
		if err != nil {
			return err
		}
		var ingressID int64
		if ingressID, err = insert.LastInsertId(); err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT INTO a2a_fences(subject, session, epoch, last_seq, updated)
			VALUES(?,?,?,?,?)
			ON CONFLICT(subject, session) DO UPDATE SET epoch = excluded.epoch, last_seq = excluded.last_seq, updated = excluded.updated`,
			a.Envelope.Subject, a.Envelope.Session, next.Epoch, next.LastSeq, nowTS); err != nil {
			return err
		}
		for _, d := range a.Deliveries {
			state := d.State
			var nextUnix any
			if !a.NewTask && state == a2acore.DeliveryHeld {
				// A continuation's message is already in the task
				// history: the notice is due at once.
				state = a2acore.DeliveryPending
				nextUnix = nowUnix
			}
			if _, err := tx.Exec(`INSERT INTO a2a_deliveries(ingress_id, task_id, role, ref, state, attempts, next_attempt_unix, created, created_unix)
				VALUES(?,?,?,?,?,?,?,?,?)`,
				ingressID, a.TaskID, d.Role, nullStr(d.Ref), state, 0, nextUnix, nowTS, nowUnix); err != nil {
				return err
			}
		}
		res = a2acore.AdmitResult{IngressID: ingressID}
		return nil
	})
	if err != nil {
		return a2acore.AdmitResult{}, err
	}
	return res, nil
}

// ClaimDue claims at most limit due deliveries — pending ones whose next
// attempt is at or before now, in-flight ones whose lease has expired —
// ordered by ingress id then delivery id: each becomes in-flight until
// now+lease and counts the attempt. A limit or lease at or below zero
// uses the delivery runner's defaults (32, notify.LeaseDuration).
func (s *Store) ClaimDue(ctx context.Context, now time.Time, limit int, lease time.Duration) ([]a2acore.DeliveryClaim, error) {
	if limit <= 0 {
		limit = 32
	}
	if lease <= 0 {
		lease = notify.LeaseDuration
	}
	nowTS, nowUnix := stamp(now)
	leaseUntil := now.Add(lease).Unix()
	var claims []a2acore.DeliveryClaim
	err := s.tx(ctx, func(tx *sql.Tx) error {
		rows, err := tx.Query(`SELECT d.delivery_id, d.ingress_id, d.task_id, d.role, d.ref, d.attempts,
			i.context_id, i.project, i.job, t.state
			FROM a2a_deliveries d
			JOIN a2a_ingress i ON i.ingress_id = d.ingress_id
			LEFT JOIN a2a_tasks t ON t.task_id = d.task_id
			WHERE (d.state = ? AND (d.next_attempt_unix IS NULL OR d.next_attempt_unix <= ?))
			OR (d.state = ? AND (d.lease_until_unix IS NULL OR d.lease_until_unix < ?))
			ORDER BY d.ingress_id ASC, d.delivery_id ASC
			LIMIT ?`,
			a2acore.DeliveryPending, nowUnix, a2acore.DeliveryInFlight, nowUnix, limit)
		if err != nil {
			return err
		}
		type due struct {
			deliveryID, ingressID int64
			taskID, role          string
			ref, contextID        string
			project, job, state   string
			attempts              int
		}
		var dues []due
		for rows.Next() {
			var d due
			var ref, job, state sql.NullString
			if err := rows.Scan(&d.deliveryID, &d.ingressID, &d.taskID, &d.role, &ref, &d.attempts,
				&d.contextID, &d.project, &job, &state); err != nil {
				rows.Close()
				return err
			}
			d.ref, d.job, d.state = ref.String, job.String, state.String
			dues = append(dues, d)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		for _, d := range dues {
			if _, err := tx.Exec(`UPDATE a2a_deliveries SET state = ?, lease_until_unix = ?, attempts = attempts + 1,
				first_attempt = COALESCE(first_attempt, ?), last_attempt = ? WHERE delivery_id = ?`,
				a2acore.DeliveryInFlight, leaseUntil, nowTS, nowTS, d.deliveryID); err != nil {
				return err
			}
			claims = append(claims, a2acore.DeliveryClaim{
				DeliveryID: d.deliveryID,
				IngressID:  d.ingressID,
				TaskID:     d.taskID,
				ContextID:  d.contextID,
				Project:    d.project,
				Job:        d.job,
				Role:       d.role,
				Ref:        d.ref,
				Attempt:    d.attempts + 1,
				TaskState:  d.state,
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return claims, nil
}

// Complete records the outcome of one claimed attempt with
// a2acore.NextDeliveryState: it applies only when the delivery is still
// in flight with the claimed attempt (a stale claim changes nothing),
// and stores the acceptance, the exit and the error category. A
// delivery whose task became terminal while in flight still records its
// real outcome.
func (s *Store) Complete(ctx context.Context, c a2acore.DeliveryClaim, r notify.SendResult, now time.Time) error {
	return s.tx(ctx, func(tx *sql.Tx) error {
		var state string
		var attempts int
		err := tx.QueryRow(`SELECT state, attempts FROM a2a_deliveries WHERE delivery_id = ?`, c.DeliveryID).
			Scan(&state, &attempts)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if state != a2acore.DeliveryInFlight || attempts != c.Attempt {
			return nil
		}
		newState, next := a2acore.NextDeliveryState(c.Attempt, r, now)
		nowTS, _ := stamp(now)
		var nextUnix, accepted, acceptStatus, lastExit, lastError any
		if !next.IsZero() {
			nextUnix = next.Unix()
		}
		if newState == a2acore.DeliveryAccepted {
			accepted, acceptStatus = nowTS, r.Status
		} else {
			lastError = r.Category
		}
		if r.Exit >= 0 {
			lastExit = r.Exit
		}
		_, err = tx.Exec(`UPDATE a2a_deliveries SET state = ?, next_attempt_unix = ?, lease_until_unix = NULL,
			accepted = ?, accept_status = ?, last_exit = ?, last_error = ? WHERE delivery_id = ?`,
			newState, nextUnix, accepted, acceptStatus, lastExit, lastError, c.DeliveryID)
		return err
	})
}

// Release returns an unsent claim to pending without consuming its
// attempt: a stale claim changes nothing. The attempt timestamps go back
// to what the remaining attempts justify (the first attempt, when the
// released one was the second; none, when it was the only one).
func (s *Store) Release(ctx context.Context, c a2acore.DeliveryClaim) error {
	return s.tx(ctx, func(tx *sql.Tx) error {
		var state string
		var attempts int
		err := tx.QueryRow(`SELECT state, attempts FROM a2a_deliveries WHERE delivery_id = ?`, c.DeliveryID).
			Scan(&state, &attempts)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if state != a2acore.DeliveryInFlight || attempts != c.Attempt {
			return nil
		}
		_, err = tx.Exec(`UPDATE a2a_deliveries SET state = ?, lease_until_unix = NULL, attempts = attempts - 1,
			first_attempt = CASE WHEN attempts = 1 THEN NULL ELSE first_attempt END,
			last_attempt = CASE WHEN attempts = 1 THEN NULL WHEN attempts = 2 THEN first_attempt ELSE last_attempt END
			WHERE delivery_id = ?`, a2acore.DeliveryPending, c.DeliveryID)
		return err
	})
}

// RecoverHeld marks obsolete the held deliveries whose ingress is older
// than grace and whose task never committed (a crash between the
// admission and the task write), and returns how many it marked. In the
// same transaction it deletes the rows of the pending push configs
// whose task has no a2a_tasks row and whose creation
// stamp is older than now minus grace (a stamp that does not parse counts
// as older); they are not part of the returned count. The provider
// handles of those configs are destroyed only after the transaction
// committed: on a transaction error it returns 0 and the error and touches
// no handle, so a later Create of the task still finds a usable config.
// When the commit succeeded and deleting a handle failed it returns the
// committed count of marked deliveries together with an error saying the
// recovery committed and keeping the cause; the failed handle stays in the
// provider, unreferenced (the row is gone and the store keeps no durable
// ledger of handles).
func (s *Store) RecoverHeld(ctx context.Context, now time.Time, grace time.Duration) (int, error) {
	cutoff := now.Add(-grace).Unix()
	var marked int
	var handles []string
	err := s.tx(ctx, func(tx *sql.Tx) error {
		marked, handles = 0, nil
		res, err := tx.Exec(`UPDATE a2a_deliveries SET state = ?
			WHERE state = ?
			AND task_id NOT IN (SELECT task_id FROM a2a_tasks)
			AND ingress_id IN (SELECT ingress_id FROM a2a_ingress WHERE received_unix < ?)`,
			a2acore.DeliveryObsolete, a2acore.DeliveryHeld, cutoff)
		if err != nil {
			return err
		}
		affected, err := res.RowsAffected()
		if err != nil {
			return err
		}
		count := int(affected)
		pending, err := pendingPushRows(tx, `WHERE task_id NOT IN (SELECT task_id FROM a2a_tasks)`)
		if err != nil {
			return err
		}
		var stale []pendingPush
		pushCutoff := now.Add(-grace)
		for _, r := range pending {
			created, perr := time.Parse(tsLayout, r.created)
			if perr != nil || created.Before(pushCutoff) {
				stale = append(stale, r)
			}
		}
		_, refs, err := s.deletePushRows(tx, stale)
		if err != nil {
			return err
		}
		marked, handles = count, refs
		return nil
	})
	if err != nil {
		return 0, err
	}
	if derr := s.PushConfigs().deleteHandles(ctx, handles...); derr != nil {
		return marked, fmt.Errorf("a2astore: held deliveries recovered; deleting a secret handle of a discarded pending push config failed: %w", derr)
	}
	return marked, nil
}
