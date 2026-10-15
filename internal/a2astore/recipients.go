package a2astore

import (
	"context"
)

// TaskRecipientRefs lists the distinct, non-null recipient references
// planned for the deliveries of one task, in any role and state: the set
// a reply may come from without a routing re-read. A task with no
// deliveries (or only no-route ones) returns an empty list.
func (s *Store) TaskRecipientRefs(ctx context.Context, taskID string) ([]string, error) {
	rows, err := s.db.Query(`SELECT DISTINCT ref FROM a2a_deliveries WHERE task_id = ? AND ref IS NOT NULL ORDER BY ref ASC`, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var refs []string
	for rows.Next() {
		var ref string
		if err := rows.Scan(&ref); err != nil {
			return nil, err
		}
		refs = append(refs, ref)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return refs, nil
}
