package a2astore

import (
	"context"
	"database/sql"
	"errors"

	"github.com/djalmajr/nau/internal/a2acore"
)

// Fence is the read-only fence read of one (subject, session): the same
// query as Admit's fence step, without writing and without a transaction.
// A missing row is a2acore.FenceState{Found: false} and no error (the
// session was never seen); a SQL error comes back as an error.
func (s *Store) Fence(ctx context.Context, subject, session string) (a2acore.FenceState, error) {
	var st a2acore.FenceState
	var epoch, lastSeq int64
	err := s.db.QueryRow(`SELECT epoch, last_seq FROM a2a_fences WHERE subject = ? AND session = ?`,
		subject, session).Scan(&epoch, &lastSeq)
	if errors.Is(err, sql.ErrNoRows) {
		return st, nil
	}
	if err != nil {
		return st, err
	}
	return a2acore.FenceState{Found: true, Epoch: epoch, LastSeq: lastSeq}, nil
}
