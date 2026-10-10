package outbox

import "errors"

// ErrReadOnly is returned by WithLock on a read-only store.
var ErrReadOnly = errors.New("outbox: store is read-only")

// WithLock runs fn while holding the process-wide outbox lock (the same
// lock as Append). fn must not call methods that take the lock themselves
// (Append, AppendJobEvent, UpdateJobs, UpdateSessions, SetLastPush, ...);
// Read and the exported read helpers are fine. A read-only store returns
// ErrReadOnly without calling fn.
func (s *Store) WithLock(fn func() error) error {
	if s.readOnly {
		return ErrReadOnly
	}
	l, err := s.lock()
	if err != nil {
		return err
	}
	defer l.unlock()
	return fn()
}
