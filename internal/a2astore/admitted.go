package a2astore

import (
	"context"
	"database/sql"
	"errors"
)

// AdmittedTask returns the task id admitted under the same key Admit
// uses — (subject, message id) — or the empty string when no admission
// carries the message: the early resend answer of the server's
// interceptor answers the admitted continuation with the stored task
// without running it. A SQL error comes back as an error.
func (s *Store) AdmittedTask(ctx context.Context, subject, messageID string) (string, error) {
	var taskID string
	err := s.db.QueryRow(`SELECT task_id FROM a2a_ingress WHERE subject = ? AND message_id = ?`,
		subject, messageID).Scan(&taskID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return taskID, nil
}
