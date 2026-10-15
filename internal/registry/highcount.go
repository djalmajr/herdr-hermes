package registry

import (
	"database/sql"
	"time"
)

// highCountPeriod is the repetition period of the high-count warn: while
// the retained total stays over the threshold the warn repeats at most
// once per period of the store clock.
const highCountPeriod = 24 * time.Hour

// retainedKindCounts reads the retained rows of one kind, outside a
// transaction, for the high-count check: the total and the active and
// ended rows of the jobs table of the kind.
func (s *Store) retainedKindCounts(kind string) (total, active, ended int, err error) {
	var a, e sql.NullInt64
	err = s.db.QueryRow(`SELECT COUNT(*),
		SUM(CASE WHEN encerrado = 0 THEN 1 ELSE 0 END),
		SUM(CASE WHEN encerrado = 1 THEN 1 ELSE 0 END)
		FROM jobs WHERE `+kindPred(kind)).Scan(&total, &a, &e)
	if err != nil {
		return 0, 0, 0, err
	}
	return total, int(a.Int64), int(e.Int64), nil
}

// checkHighCount runs the high-count check after a confirmed ingest,
// poll or sweep, outside the transaction: the retained total of the jobs
// and sessions kinds over the threshold logs one line with the counts
// only. The repetition rule, on the store clock: the warn fires on the
// transition from at-or-below to over the threshold, repeats at most
// once per 24 h while the total stays over, and re-arms when the total
// drops to or below it. The sampling of the counts, the clock read and
// the state update run under the one alert mutex, held across the
// queries, so a sample is never applied after a sample that started
// after it: a stale at-or-below sample cannot disarm the alert of the
// fresh over-threshold one. A clock that goes backwards only delays the
// repetition; it never fires early and never panics. The check lives in
// memory only, so a restarted serve alerts on its first check over the
// threshold. It never blocks, undoes or skips work and never holds the
// push cursor; the line carries no machine label, key, card or path.
func (s *Store) checkHighCount() {
	s.hcMu.Lock()
	defer s.hcMu.Unlock()
	jobsTotal, jobsActive, jobsEnded, err := s.retainedKindCounts(kindJobs)
	if err != nil {
		return
	}
	sessTotal, sessActive, sessEnded, err := s.retainedKindCounts(kindSessions)
	if err != nil {
		return
	}
	total := jobsTotal + sessTotal
	now := s.now()
	if total <= s.highCountThreshold {
		// At or below the threshold the rule re-arms.
		s.hcAbove = false
		return
	}
	if s.hcAbove && now.Sub(s.hcLastAlert) < highCountPeriod {
		// Still over, and the period is not over: no repeat. A backward
		// clock makes the difference negative, which is also no repeat.
		return
	}
	s.hcAbove = true
	s.hcLastAlert = now
	logf("registry: high count: retained %d (jobs active %d, jobs ended %d, sessions active %d, sessions ended %d), warn above %d",
		total, jobsActive, jobsEnded, sessActive, sessEnded, s.highCountThreshold)
}
