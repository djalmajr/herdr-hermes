package a2astore

import (
	"context"
	"database/sql"
	"errors"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv/push"
)

// ownerRefKey is the context key of the routing reference the push
// config Save records (WithOwnerRef). The value is the non-secret
// routing reference the identity provider asserted for the
// authenticated subject of the call: never a token, the subject id or
// any other provider value.
type ownerRefKey struct{}

// WithOwnerRef stamps the routing reference of the authenticated
// subject on the context so the push config Save of the same call
// records it in the owner_ref column.
func WithOwnerRef(ctx context.Context, ref string) context.Context {
	return context.WithValue(ctx, ownerRefKey{}, ref)
}

// ownerRefFrom returns the stamped reference, "" when the context
// carries no stamp (a Save of a call without an authenticated subject,
// or one of the local operator, whose reference is empty, stores NULL).
func ownerRefFrom(ctx context.Context) string {
	ref, _ := ctx.Value(ownerRefKey{}).(string)
	return ref
}

// PushOwner is the stored owner, the stored project and the recorded
// routing reference of one push config row.
type PushOwner struct {
	Subject string
	Project string
	Ref     string
}

// Owner returns the owner, the stored project and the recorded routing
// reference of one push config. The join with the task table keeps only
// the configs of a committed task, so a missing row or a pending config
// (its task has no a2a_tasks row) is push.ErrPushConfigNotFound.
func (p *PushConfigStore) Owner(ctx context.Context, taskID a2a.TaskID, configID string) (PushOwner, error) {
	var o PushOwner
	err := p.s.db.QueryRowContext(ctx,
		`SELECT t.owner, t.project, COALESCE(c.owner_ref, '')
		 FROM a2a_push_configs c JOIN a2a_tasks t ON t.task_id = c.task_id
		 WHERE c.task_id = ? AND c.config_id = ?`, string(taskID), configID).
		Scan(&o.Subject, &o.Project, &o.Ref)
	if errors.Is(err, sql.ErrNoRows) {
		return PushOwner{}, push.ErrPushConfigNotFound
	}
	if err != nil {
		return PushOwner{}, err
	}
	return o, nil
}

// ensurePushOwnerRef adds the additive owner_ref column of
// a2a_push_configs when the table does not carry it yet: a database
// created before the column gains it in place when the store opens (the
// ALTER runs in the caller's immediate transaction, so without a commit
// the next open redoes it), a second open finds the column and never
// alters the table again, and an old build keeps opening the migrated
// database (the a2a_meta schema value stays 1 and the column is
// nullable).
func ensurePushOwnerRef(tx *sql.Tx) error {
	var n int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('a2a_push_configs') WHERE name = 'owner_ref'`).Scan(&n); err != nil {
		return err
	}
	if n > 0 {
		return nil
	}
	_, err := tx.Exec(`ALTER TABLE a2a_push_configs ADD COLUMN owner_ref TEXT`)
	return err
}
