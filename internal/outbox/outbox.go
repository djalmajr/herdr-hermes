// Package outbox implements the durable local outbox: an append-only JSON
// lines file with a monotonic seq derived from the file itself, an OS
// advisory lock on a persistent lock file, the delivery cursor, the tracked
// jobs and sessions files, job-event dedupe and a long-polling reader for
// the dispatcher pull.
package outbox

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// TSLayout is the local time with an explicit offset. The layout forces the
// offset to be printed (as +00:00) even when the zone is UTC, so a timestamp
// never ends with "Z".
const TSLayout = "2006-01-02T15:04:05-07:00"

// Record is one outbox line (schema 1). Field order in JSON is normative.
type Record struct {
	Schema         int             `json:"schema"`
	Seq            int64           `json:"seq"`
	TS             string          `json:"ts"`
	Tipo           string          `json:"tipo"`
	Maquina        string          `json:"maquina"`
	Projeto        string          `json:"projeto"`
	JobID          *string         `json:"job_id"`
	IdempotencyKey string          `json:"idempotency_key"`
	Dados          json.RawMessage `json:"dados"`
}

// Record types.
const (
	TipoJobEvent = "job_event"
	TipoDispatch = "dispatch"
	TipoAmend    = "amend"
	TipoSession  = "session"
	TipoDecision = "decision"
)

// RecordSchema is the outbox record schema version.
const RecordSchema = 1

// Options configures a Store.
type Options struct {
	// Now provides the current time for record timestamps; time.Now when
	// nil.
	Now func() time.Time
	// ReadOnly forbids creating the state directory; a read-only store
	// must not create the directory, the lock file or any other file.
	ReadOnly bool
	// LockTimeout bounds lock acquisition; the default is 10s.
	LockTimeout time.Duration
}

const (
	defaultLockTimeout = 10 * time.Second
	lockPollInterval   = 50 * time.Millisecond
	waitPollInterval   = 200 * time.Millisecond
)

// Clock is the injected clock used by Wait: Now and Sleep. The long-poll
// bound is the sum of the slept durations, so a clock that moves backwards
// never shortens the wait and never lets it exceed the bound.
type Clock interface {
	Now() time.Time
	Sleep(ctx context.Context, d time.Duration) error
}

// Store is an open outbox state directory.
type Store struct {
	dir         string
	now         func() time.Time
	readOnly    bool
	lockTimeout time.Duration
	syncFile    func(*os.File) error // fsync; tests replace it to count calls
}

// LockError reports that the outbox lock was not acquired within the bound.
type LockError struct {
	Timeout time.Duration
}

func (e *LockError) Error() string {
	return "outbox: lock not acquired within " + e.Timeout.String()
}

// Open opens the state directory at dir (<ConfigDir>/state). A non-read-only
// store creates the directory with mode 0700.
func Open(dir string, opts Options) (*Store, error) {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.LockTimeout <= 0 {
		opts.LockTimeout = defaultLockTimeout
	}
	if !opts.ReadOnly {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return nil, err
		}
		if err := os.Chmod(dir, 0o700); err != nil {
			return nil, err
		}
	}
	return &Store{
		dir:         dir,
		now:         opts.Now,
		readOnly:    opts.ReadOnly,
		lockTimeout: opts.LockTimeout,
		syncFile:    (*os.File).Sync,
	}, nil
}

// Dir returns the state directory path.
func (s *Store) Dir() string { return s.dir }

// StateDir returns the canonical state directory path for a configuration
// directory.
func StateDir(configDir string) string {
	return filepath.Join(configDir, "state")
}

func (s *Store) outboxPath() string { return filepath.Join(s.dir, "outbox.jsonl") }
func (s *Store) cursorPath() string { return filepath.Join(s.dir, "cursor.json") }
func (s *Store) jobsPath() string   { return filepath.Join(s.dir, "jobs.json") }
func (s *Store) sessionsPath() string {
	return filepath.Join(s.dir, "sessions.json")
}
func (s *Store) lockPath() string { return filepath.Join(s.dir, "lock") }

// fileLock is one held advisory lock: a per-call open file descriptor. The
// lock lives on the descriptor, not on the Store, so concurrent callers
// each own their own lock and a crashed process never leaves a stale one.
type fileLock struct {
	f *os.File
}

// unlock releases the lock and closes its descriptor.
func (l *fileLock) unlock() {
	_ = lockRelease(l.f.Fd())
	_ = l.f.Close()
}

// lock takes the process-wide advisory lock on the persistent lock file.
// The kernel holds the lock while the descriptor is open, so a crashed
// process never leaves a stale lock.
func (s *Store) lock() (*fileLock, error) {
	f, err := os.OpenFile(s.lockPath(), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := lockAcquire(f.Fd(), s.lockTimeout); err != nil {
		f.Close()
		return nil, err
	}
	return &fileLock{f: f}, nil
}

// readOutbox returns the raw bytes of outbox.jsonl, or nil when absent.
func (s *Store) readOutbox() ([]byte, error) {
	data, err := os.ReadFile(s.outboxPath())
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	return data, err
}

// completeLines splits raw outbox bytes into the lines that end with a
// newline; a torn final line is ignored.
func completeLines(data []byte) [][]byte {
	if len(data) == 0 {
		return nil
	}
	var lines [][]byte
	start := 0
	for i, b := range data {
		if b == '\n' {
			lines = append(lines, data[start:i])
			start = i + 1
		}
	}
	if start < len(data) {
		// Tail without a trailing newline: torn write, ignored.
	}
	return lines
}

// parseSeq extracts the seq field of one outbox line.
func parseSeq(line []byte) (int64, error) {
	var probe struct {
		Seq int64 `json:"seq"`
	}
	if err := json.Unmarshal(line, &probe); err != nil {
		return 0, err
	}
	return probe.Seq, nil
}

// repairTornTail truncates outbox.jsonl to the last newline when it does not
// end with one (a torn write from a crash), and syncs the file.
func (s *Store) repairTornTail() error {
	data, err := s.readOutbox()
	if err != nil || len(data) == 0 {
		return err
	}
	if data[len(data)-1] == '\n' {
		return nil
	}
	last := bytes.LastIndexByte(data, '\n')
	keep := int64(0)
	if last >= 0 {
		keep = int64(last + 1)
	}
	f, err := os.OpenFile(s.outboxPath(), os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := f.Truncate(keep); err != nil {
		return err
	}
	return s.syncFile(f)
}

// appendLocked appends one record under the caller-held lock: it assigns
// seq (last seq in the file + 1), ts, the schema and, for non-job_event
// records, the idempotency key <maquina>:<seq>; then it writes one line and
// syncs the file before returning.
func (s *Store) appendLocked(r Record) (Record, error) {
	if err := s.repairTornTail(); err != nil {
		return r, err
	}
	data, err := s.readOutbox()
	if err != nil {
		return r, err
	}
	seq := int64(0)
	if lines := completeLines(data); len(lines) > 0 {
		if seq, err = parseSeq(lines[len(lines)-1]); err != nil {
			return r, fmt.Errorf("outbox: last line of %s is not a record: %w", s.outboxPath(), err)
		}
	}
	r.Schema = RecordSchema
	r.Seq = seq + 1
	r.TS = s.now().Format(TSLayout)
	if r.Tipo != TipoJobEvent && r.IdempotencyKey == "" {
		r.IdempotencyKey = r.Maquina + ":" + strconv.FormatInt(r.Seq, 10)
	}
	line, err := json.Marshal(&r)
	if err != nil {
		return r, err
	}
	f, err := os.OpenFile(s.outboxPath(), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		return r, err
	}
	defer f.Close()
	if _, err := f.Write(append(line, '\n')); err != nil {
		return r, err
	}
	return r, s.syncFile(f)
}

// Append appends one record and returns it with seq, ts, schema and (for
// non-job_event records) the idempotency key assigned. Job events go through
// AppendJobEvent, which assigns the <job_id>:<seq> key and dedupes.
func (s *Store) Append(r Record) (Record, error) {
	l, err := s.lock()
	if err != nil {
		return r, err
	}
	defer l.unlock()
	return s.appendLocked(r)
}

// findRecordByKeyLocked scans outbox.jsonl for a record with the given
// idempotency key; it returns nil when there is none.
func (s *Store) findRecordByKeyLocked(key string) (*Record, error) {
	data, err := s.readOutbox()
	if err != nil {
		return nil, err
	}
	for _, line := range completeLines(data) {
		var r Record
		if err := json.Unmarshal(line, &r); err != nil {
			continue
		}
		if r.IdempotencyKey == key {
			return &r, nil
		}
	}
	return nil, nil
}

// AppendJobEvent appends one job event. The idempotency key is
// <job_id>:<event seq> and dados carries the original event bytes unchanged.
// It is a no-op (appended=false) when jobs.json already records
// last_event_seq >= event seq for the job, or when a record with the key
// already exists in the outbox (the window between the append and the
// jobs.json update, where a crash must not duplicate). The job is tracked in
// jobs.json when unknown. An event without a positive integer seq is an
// error.
func (s *Store) AppendJobEvent(maquina, projeto, jobID string, event json.RawMessage) (rec Record, appended bool, err error) {
	var probe struct {
		Seq int64 `json:"seq"`
	}
	if err := json.Unmarshal(event, &probe); err != nil {
		return rec, false, fmt.Errorf("outbox: event is not a JSON object: %w", err)
	}
	if probe.Seq <= 0 {
		return rec, false, errors.New("outbox: event seq must be a positive integer")
	}
	key := jobID + ":" + strconv.FormatInt(probe.Seq, 10)

	l, err := s.lock()
	if err != nil {
		return rec, false, err
	}
	defer l.unlock()

	jobs, err := s.loadJobsLocked()
	if err != nil {
		return rec, false, err
	}
	if j, ok := jobs.Jobs[jobID]; ok && j.LastEventSeq >= probe.Seq {
		// Already durable from a previous append; return the existing record
		// when it is present.
		existing, ferr := s.findRecordByKeyLocked(key)
		if ferr != nil {
			return rec, false, ferr
		}
		if existing != nil {
			return *existing, false, nil
		}
		return rec, false, nil
	}
	existing, ferr := s.findRecordByKeyLocked(key)
	if ferr != nil {
		return rec, false, ferr
	}
	if existing != nil {
		// A crash left the outbox record without the jobs.json update: heal
		// the cursor so later retries take the cheap path.
		j := jobs.Jobs[jobID]
		if j == nil {
			ts := s.now().Format(TSLayout)
			j = &Job{ID: jobID, Projeto: projeto, CreatedAt: ts, UpdatedAt: ts}
			jobs.Jobs[jobID] = j
		}
		j.LastEventSeq = probe.Seq
		j.UpdatedAt = s.now().Format(TSLayout)
		if err := s.writeJobsLocked(jobs); err != nil {
			return *existing, false, err
		}
		return *existing, false, nil
	}
	id := jobID
	rec = Record{
		Tipo:           TipoJobEvent,
		Maquina:        maquina,
		Projeto:        projeto,
		JobID:          &id,
		IdempotencyKey: key,
		Dados:          event,
	}
	rec, err = s.appendLocked(rec)
	if err != nil {
		return rec, false, err
	}
	j, ok := jobs.Jobs[jobID]
	if !ok {
		ts := s.now().Format(TSLayout)
		j = &Job{ID: jobID, Projeto: projeto, CreatedAt: ts, UpdatedAt: ts}
		jobs.Jobs[jobID] = j
	}
	j.LastEventSeq = probe.Seq
	j.UpdatedAt = s.now().Format(TSLayout)
	return rec, true, s.writeJobsLocked(jobs)
}

// Read returns the raw lines with seq > since, in file order, ignoring a
// torn final line, plus the last complete line's seq (0 when the file is
// empty or absent).
func (s *Store) Read(since int64) ([][]byte, int64, error) {
	data, err := s.readOutbox()
	if err != nil {
		return nil, 0, err
	}
	var out [][]byte
	var last int64
	for _, line := range completeLines(data) {
		seq, err := parseSeq(line)
		if err != nil {
			continue
		}
		last = seq
		if seq > since {
			out = append(out, line)
		}
	}
	return out, last, nil
}

// Wait long-polls until a record with seq > since exists (found=true) or max
// elapses (found=false), polling the file through the injected clock. The
// bound is the sum of the slept durations, so a backwards-moving clock never
// shortens the wait and the total sleep never exceeds max. A cancelled
// context returns its error.
func (s *Store) Wait(ctx context.Context, since int64, max time.Duration, clock Clock) (bool, int64, error) {
	if clock == nil {
		return false, 0, errors.New("outbox: Wait requires a Clock")
	}
	var elapsed time.Duration
	for {
		if err := ctx.Err(); err != nil {
			return false, 0, err
		}
		lines, last, err := s.Read(since)
		if err != nil {
			return false, 0, err
		}
		if len(lines) > 0 {
			return true, last, nil
		}
		if elapsed >= max {
			return false, last, nil
		}
		d := waitPollInterval
		if rem := max - elapsed; rem < d {
			d = rem
		}
		if err := clock.Sleep(ctx, d); err != nil {
			return false, last, err
		}
		elapsed += d
	}
}

// cursor is the delivery cursor file content. last_push (recorded by the
// push client) travels in the same file.
type cursor struct {
	DeliveredSeq int64       `json:"delivered_seq"`
	LastPush     *PushResult `json:"last_push,omitempty"`
}

// PushResult is the outcome of one push batch attempt (slice 3 writes it).
type PushResult struct {
	TS     string `json:"ts"`
	Status string `json:"status"`
	Code   int    `json:"code"`
}

// cursorLocked reads the cursor file; an absent file is (zero, nil).
func (s *Store) cursorLocked() (cursor, error) {
	var c cursor
	data, err := os.ReadFile(s.cursorPath())
	if errors.Is(err, os.ErrNotExist) {
		return c, nil
	}
	if err != nil {
		return c, err
	}
	if err := json.Unmarshal(data, &c); err != nil {
		return c, fmt.Errorf("outbox: cursor.json: %w", err)
	}
	return c, nil
}

func (s *Store) writeCursorLocked(c cursor) error {
	data, err := json.Marshal(c)
	if err != nil {
		return err
	}
	return WriteFileAtomic(s.cursorPath(), data)
}

// DeliveredSeq returns the delivery cursor (0 when cursor.json is absent).
// It takes no lock, so it is safe for read-only stores.
func (s *Store) DeliveredSeq() (int64, error) {
	c, err := s.cursorLocked()
	if err != nil {
		return 0, err
	}
	return c.DeliveredSeq, nil
}

// LastPush returns the last recorded push result, or nil when absent.
// It takes no lock, so it is safe for read-only stores.
func (s *Store) LastPush() (*PushResult, error) {
	c, err := s.cursorLocked()
	if err != nil {
		return nil, err
	}
	return c.LastPush, nil
}

// SetLastPush records the last push result in cursor.json under the lock
// (atomic write), leaving delivered_seq unchanged; TS is filled with the
// store clock in TSLayout when empty.
func (s *Store) SetLastPush(r PushResult) error {
	l, err := s.lock()
	if err != nil {
		return err
	}
	defer l.unlock()
	c, err := s.cursorLocked()
	if err != nil {
		return err
	}
	if r.TS == "" {
		r.TS = s.now().Format(TSLayout)
	}
	p := r
	c.LastPush = &p
	return s.writeCursorLocked(c)
}

// SetDeliveredSeq moves the delivery cursor forward atomically; it never
// moves backwards (a lower value is a no-op). last_push is preserved.
func (s *Store) SetDeliveredSeq(n int64) error {
	l, err := s.lock()
	if err != nil {
		return err
	}
	defer l.unlock()
	c, err := s.cursorLocked()
	if err != nil {
		return err
	}
	if n <= c.DeliveredSeq {
		return nil
	}
	c.DeliveredSeq = n
	return s.writeCursorLocked(c)
}

// Job is one tracked job in jobs.json.
type Job struct {
	ID           string `json:"id"`
	Projeto      string `json:"projeto"`
	LastEventSeq int64  `json:"last_event_seq"`
	Estado       string `json:"estado"`
	CreatedAt    string `json:"created_at"`
	UpdatedAt    string `json:"updated_at"`
	Closed       bool   `json:"closed"`
}

// Jobs is the jobs.json content.
type Jobs struct {
	Jobs map[string]*Job `json:"jobs"`
}

// Session is one open session in sessions.json.
type Session struct {
	Projeto   string `json:"projeto"`
	Branch    string `json:"branch,omitempty"`
	Card      string `json:"card,omitempty"`
	Job       string `json:"job,omitempty"`
	Estado    string `json:"estado,omitempty"`
	PR        string `json:"pr,omitempty"`
	StartedAt string `json:"started_at"`
	UpdatedAt string `json:"updated_at"`
}

// Sessions is the sessions.json content.
type Sessions struct {
	Sessions map[string]*Session `json:"sessions"`
}

// LoadJobs returns jobs.json, or an empty set when the file is absent.
func (s *Store) LoadJobs() (*Jobs, error) {
	l, err := s.lock()
	if err != nil {
		return nil, err
	}
	defer l.unlock()
	return s.loadJobsLocked()
}

func (s *Store) loadJobsLocked() (*Jobs, error) {
	data, err := os.ReadFile(s.jobsPath())
	if errors.Is(err, os.ErrNotExist) {
		return &Jobs{Jobs: map[string]*Job{}}, nil
	}
	if err != nil {
		return nil, err
	}
	var j Jobs
	if err := json.Unmarshal(data, &j); err != nil {
		return nil, fmt.Errorf("outbox: jobs.json: %w", err)
	}
	if j.Jobs == nil {
		j.Jobs = map[string]*Job{}
	}
	return &j, nil
}

func (s *Store) writeJobsLocked(j *Jobs) error {
	if j.Jobs == nil {
		j.Jobs = map[string]*Job{}
	}
	data, err := json.Marshal(j)
	if err != nil {
		return err
	}
	return WriteFileAtomic(s.jobsPath(), data)
}

// UpdateJobs mutates the jobs file under the lock and writes it atomically.
func (s *Store) UpdateJobs(mutate func(*Jobs) error) error {
	l, err := s.lock()
	if err != nil {
		return err
	}
	defer l.unlock()
	jobs, err := s.loadJobsLocked()
	if err != nil {
		return err
	}
	if err := mutate(jobs); err != nil {
		return err
	}
	return s.writeJobsLocked(jobs)
}

// LoadSessions returns sessions.json, or an empty set when the file is
// absent.
func (s *Store) LoadSessions() (*Sessions, error) {
	l, err := s.lock()
	if err != nil {
		return nil, err
	}
	defer l.unlock()
	return s.loadSessionsLocked()
}

func (s *Store) loadSessionsLocked() (*Sessions, error) {
	data, err := os.ReadFile(s.sessionsPath())
	if errors.Is(err, os.ErrNotExist) {
		return &Sessions{Sessions: map[string]*Session{}}, nil
	}
	if err != nil {
		return nil, err
	}
	var ss Sessions
	if err := json.Unmarshal(data, &ss); err != nil {
		return nil, fmt.Errorf("outbox: sessions.json: %w", err)
	}
	if ss.Sessions == nil {
		ss.Sessions = map[string]*Session{}
	}
	return &ss, nil
}

func (s *Store) writeSessionsLocked(ss *Sessions) error {
	if ss.Sessions == nil {
		ss.Sessions = map[string]*Session{}
	}
	data, err := json.Marshal(ss)
	if err != nil {
		return err
	}
	return WriteFileAtomic(s.sessionsPath(), data)
}

// UpdateSessions mutates the sessions file under the lock and writes it
// atomically.
func (s *Store) UpdateSessions(mutate func(*Sessions) error) error {
	l, err := s.lock()
	if err != nil {
		return err
	}
	defer l.unlock()
	ss, err := s.loadSessionsLocked()
	if err != nil {
		return err
	}
	if err := mutate(ss); err != nil {
		return err
	}
	return s.writeSessionsLocked(ss)
}

// atomicWriteSync is the sync step of WriteFileAtomic; tests replace it to
// count syncs without changing the exported signature.
var atomicWriteSync = func(f *os.File) error { return f.Sync() }

// WriteFileAtomic writes data to path atomically: a temp file in the same
// directory, fsync, then rename. The final file has mode 0600.
func WriteFileAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer func() {
		if tmp != "" {
			_ = os.Remove(tmp)
		}
	}()
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := atomicWriteSync(f); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmp, 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	tmp = ""
	return nil
}

// Friction appends one line "<ts>\t<cmd>\t<msg>" to friction.log in dir.
// Best effort: errors are ignored. Callers must never pass secrets.
func Friction(dir, cmd, msg string) {
	scrub := func(s string) string {
		return sanitize(s)
	}
	line := time.Now().Format(TSLayout) + "\t" + scrub(cmd) + "\t" + scrub(msg) + "\n"
	f, err := os.OpenFile(filepath.Join(dir, "friction.log"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return
	}
	defer f.Close()
	_, _ = f.WriteString(line)
}

// sanitize removes newlines and tabs so one friction line stays one line.
func sanitize(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c == '\n' || c == '\t' || c == '\r' {
			b[i] = ' '
		}
	}
	return string(b)
}
