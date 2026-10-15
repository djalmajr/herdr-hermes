package a2astore

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/djalmajr/nau/internal/a2acore"
)

// TaskView is the neutral view of a stored task, not owner-masked (the
// server and the CLI read it; another subject's SDK Get would refuse it):
// a missing task is a2a.ErrTaskNotFound.
func (s *Store) TaskView(ctx context.Context, id string) (*a2acore.TaskView, error) {
	var view a2acore.TaskView
	var job sql.NullString
	err := s.db.QueryRow(`SELECT task_id, context_id, state, owner, project, job, messages FROM a2a_tasks WHERE task_id = ?`, id).
		Scan(&view.ID, &view.ContextID, &view.State, &view.Owner, &view.Project, &job, &view.Messages)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, a2a.ErrTaskNotFound
	}
	if err != nil {
		return nil, err
	}
	view.Job = job.String
	return &view, nil
}

// Show is the local read of one task, not owner-masked: the task with
// its full history (the message text is data), its inbox item and its
// decisions in record order. A missing task is a2a.ErrTaskNotFound.
func (s *Store) Show(ctx context.Context, id string) (*a2a.Task, *a2acore.InboxItem, []a2acore.Decision, error) {
	var taskJSON []byte
	var row taskRow
	err := s.db.QueryRow(`SELECT task_id, context_id, state, owner, project, COALESCE(job, ''), messages, created, updated, updated_nano, task_json
		FROM a2a_tasks WHERE task_id = ?`, id).
		Scan(&row.taskID, &row.contextID, &row.state, &row.subject, &row.project, &row.job, &row.messages, &row.created, &row.updated, &row.updatedNano, &taskJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil, nil, a2a.ErrTaskNotFound
	}
	if err != nil {
		return nil, nil, nil, err
	}
	task, err := unmarshalTask(taskJSON)
	if err != nil {
		return nil, nil, nil, err
	}
	item, err := s.inboxItem(ctx, row)
	if err != nil {
		return nil, nil, nil, err
	}
	decisions, err := s.decisionsOf(ctx, row.taskID)
	if err != nil {
		return nil, nil, nil, err
	}
	return task, &item, decisions, nil
}

// decisionsOf lists the decisions of one task, every state, in record
// order.
func (s *Store) decisionsOf(ctx context.Context, taskID string) ([]a2acore.Decision, error) {
	rows, err := s.db.Query(`SELECT decision_id, task_id, kind, text, actor, state, COALESCE(refusal, ''), recorded, COALESCE(applied, '')
		FROM a2a_decisions WHERE task_id = ? ORDER BY recorded_seq ASC, decision_id ASC`, taskID)
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

// taskRow is one a2a_tasks row as the inbox reads it.
type taskRow struct {
	taskID      string
	contextID   string
	state       string
	subject     string
	project     string
	job         string
	messages    int
	created     string
	updated     string
	updatedNano int64
}

// inboxItem builds the inbox view of one task row: the deliveries in
// delivery-id order and the pending decision count.
func (s *Store) inboxItem(ctx context.Context, row taskRow) (a2acore.InboxItem, error) {
	item := a2acore.InboxItem{
		TaskID:     row.taskID,
		ContextID:  row.contextID,
		State:      row.state,
		Subject:    row.subject,
		Project:    row.project,
		Job:        row.job,
		Messages:   row.messages,
		Created:    row.created,
		Updated:    row.updated,
		Deliveries: []a2acore.Delivery{},
	}
	deliveries, err := s.deliveriesOf(ctx, row.taskID)
	if err != nil {
		return item, err
	}
	item.Deliveries = deliveries
	var pending int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM a2a_decisions WHERE task_id = ? AND state = ?`, row.taskID, a2acore.DecisionPending).Scan(&pending); err != nil {
		return item, err
	}
	item.PendingDecisions = pending
	return item, nil
}

// deliveriesOf lists the deliveries of one task in delivery-id order.
func (s *Store) deliveriesOf(ctx context.Context, taskID string) ([]a2acore.Delivery, error) {
	rows, err := s.db.Query(`SELECT delivery_id, ingress_id, role, ref, state, attempts, next_attempt_unix, accepted, accept_status, last_exit, last_error
		FROM a2a_deliveries WHERE task_id = ? ORDER BY delivery_id ASC`, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	loc := s.now().Location()
	var deliveries []a2acore.Delivery
	for rows.Next() {
		var d a2acore.Delivery
		var ref, accepted, acceptStatus, lastError sql.NullString
		var nextUnix, lastExit sql.NullInt64
		if err := rows.Scan(&d.DeliveryID, &d.IngressID, &d.Role, &ref, &d.State, &d.Attempts, &nextUnix, &accepted, &acceptStatus, &lastExit, &lastError); err != nil {
			return nil, err
		}
		d.Ref = ref.String
		if nextUnix.Valid {
			d.NextAttemptAt = unixToTS(nextUnix.Int64, loc)
		}
		d.AcceptedAt = accepted.String
		d.AcceptStatus = acceptStatus.String
		if lastExit.Valid {
			exit := int(lastExit.Int64)
			d.LastExit = &exit
		}
		d.LastError = lastError.String
		deliveries = append(deliveries, d)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return deliveries, nil
}

// unixToTS renders a Unix second as the local timestamp text in the
// clock's location (the stored companions of the *_unix columns).
func unixToTS(unix int64, loc *time.Location) string {
	return time.Unix(unix, 0).In(loc).Format(tsLayout)
}
