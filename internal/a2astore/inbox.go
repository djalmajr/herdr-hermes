package a2astore

import (
	"context"

	"github.com/djalmajr/nau/internal/a2acore"
)

// Inbox lists the tasks with their deliveries, newest update first
// (updated_nano, then task id, both descending), filtered by project and
// neutral state: the limit is 100 when zero or less and at most 500.
// The task rows are read and closed before the per-item reads, so the
// single pooled connection never carries two open queries.
func (s *Store) Inbox(ctx context.Context, f a2acore.InboxFilter) ([]a2acore.InboxItem, error) {
	limit := f.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	rows, err := s.db.Query(`SELECT task_id, context_id, state, owner, project, COALESCE(job, ''), messages, created, updated, updated_nano
		FROM a2a_tasks
		WHERE (? = '' OR project = ?) AND (? = '' OR state = ?)
		ORDER BY updated_nano DESC, task_id DESC
		LIMIT ?`, f.Project, f.Project, f.State, f.State, limit)
	if err != nil {
		return nil, err
	}
	var taskRows []taskRow
	for rows.Next() {
		var row taskRow
		if err := rows.Scan(&row.taskID, &row.contextID, &row.state, &row.subject, &row.project, &row.job,
			&row.messages, &row.created, &row.updated, &row.updatedNano); err != nil {
			rows.Close()
			return nil, err
		}
		taskRows = append(taskRows, row)
	}
	errs := rows.Err()
	rows.Close()
	if errs != nil {
		return nil, errs
	}
	items := make([]a2acore.InboxItem, 0, len(taskRows))
	for i := range taskRows {
		item, err := s.inboxItem(ctx, taskRows[i])
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, nil
}
