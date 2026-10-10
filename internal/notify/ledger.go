// Durable notify state: the registry and the ledger live in
// <outbox dir>/notify (registry.json and ledger.json), every mutation runs
// under the outbox lock and re-reads both files inside the lock, and the
// projection cursor advances only with the atomic ledger write, so a
// crash before the write is recovered by the next run.
package notify

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/djalmajr/herdr-hermes/internal/outbox"
)

// ErrNotEnabled is returned by Raise when no registry exists yet.
var ErrNotEnabled = errors.New("notify: no registration; run notify register first")

// retention is how long final notifications are kept in the ledger.
const retention = 30 * 24 * time.Hour

// defaultClaimLimit bounds ClaimDue when the caller passes limit <= 0.
const defaultClaimLimit = 32

// Ledger is the ledger.json content: the outbox projection cursor, the
// notifications, the per-pane agent status state and the per-workspace
// transition state.
type Ledger struct {
	Schema        int                   `json:"schema"`
	ProjectedSeq  int64                 `json:"projected_seq"`
	Notifications []*Notification       `json:"notifications"`
	Panes         map[string]*PaneState `json:"panes"`
	// Workspaces maps a Herdr workspace id to its transition state;
	// the recorded Job resolves a closed event that carries no label. A
	// ledger without the key loads with a non-nil empty map.
	Workspaces map[string]*WorkspaceState `json:"workspaces,omitempty"`
}

// WorkspaceState is the per-workspace transition state: N counts the true
// transitions (created/closed alternations, starting at 1), Job and
// Projeto the source recorded at the last true transition, and Event the
// last true transition.
type WorkspaceState struct {
	Event          string `json:"event"` // created or closed (last true transition)
	N              int64  `json:"n"`
	Job            string `json:"job,omitempty"`
	Projeto        string `json:"projeto,omitempty"`
	LastObservedAt string `json:"last_observed_at"`
}

// PaneState is the per-pane agent status state: N counts the true status
// transitions (starting at 1) and Coalesced the repeated statuses plus
// routine transitions not notified.
type PaneState struct {
	Status         string `json:"status"`
	N              int64  `json:"n"`
	LastObservedAt string `json:"last_observed_at"`
	Coalesced      int64  `json:"coalesced"` // repeated statuses plus routine transitions not notified
}

// ProjectFunc projects one stored job_event outbox record.
type ProjectFunc func(in JobEventInput, reg Registry, now time.Time) (*Notification, bool, error)

// StatusFunc projects one agent status transition.
type StatusFunc func(tr Transition, reg Registry, now time.Time) (*Notification, bool)

// RaiseFunc projects one explicit local raise.
type RaiseFunc func(in RaiseInput, reg Registry, now time.Time) (*Notification, error)

// WorkspaceFunc projects one job workspace event.
type WorkspaceFunc func(in WorkspaceInput, reg Registry, now time.Time) (*Notification, error)

// WorkspaceSummary is the result of one IngestWorkspace call.
type WorkspaceSummary struct {
	Ignored bool // nothing resolved (no registration, no job label,
	// no recorded job); nothing written
	Duplicate bool // the same event as the stored state, or a
	// projection that produced an existing id
	Created        bool // a new notification was appended
	NotificationID string
	N              int64  // the workspace's transition number after the call
	JobID          string // the resolved job ("" when none)
}

// ProjectSummary is the result of one ProjectOutbox run: the counts plus
// the projection cursor after the run.
type ProjectSummary struct {
	Created, Duplicates, Malformed int
	ProjectedSeq                   int64
}

// IngestSummary is the result of one IngestAgentStatus call.
type IngestSummary struct {
	Ignored        bool // pane not watched (or notify disabled); nothing written
	Duplicate      bool // repeated status, or a projection that produced an existing id
	Routine        bool // true transition the projection did not notify
	Created        bool // a new notification was appended
	NotificationID string
	N              int64 // the pane's transition number after the call
}

// Store is the durable notify state of one outbox store.
type Store struct {
	ob  *outbox.Store
	now func() time.Time
}

// Open opens the notify store for one outbox store.
func Open(ob *outbox.Store, now func() time.Time) *Store {
	if now == nil {
		now = time.Now
	}
	return &Store{ob: ob, now: now}
}

// Dir is the notify state directory.
func (s *Store) Dir() string { return filepath.Join(s.ob.Dir(), "notify") }

func (s *Store) registryPath() string { return filepath.Join(s.Dir(), "registry.json") }
func (s *Store) ledgerPath() string   { return filepath.Join(s.Dir(), "ledger.json") }

// Enabled reports whether the registry exists.
func (s *Store) Enabled() bool {
	_, err := os.Stat(s.registryPath())
	return err == nil
}

func (s *Store) ts() string { return s.now().Format(TSLayout) }

func (s *Store) ensureDir() error {
	if err := os.MkdirAll(s.Dir(), 0o700); err != nil {
		return err
	}
	return os.Chmod(s.Dir(), 0o700)
}

func emptyRegistry() Registry {
	return Registry{
		Schema:           LedgerSchema,
		Owners:           map[string]string{},
		Orchestrators:    map[string]string{},
		Watches:          map[string]Watch{},
		WorkspaceWatches: map[string]WorkspaceWatch{},
	}
}

func emptyLedger() *Ledger {
	return &Ledger{
		Schema:        LedgerSchema,
		Notifications: []*Notification{},
		Panes:         map[string]*PaneState{},
		Workspaces:    map[string]*WorkspaceState{},
	}
}

func normalizeRegistry(reg *Registry) {
	if reg.Owners == nil {
		reg.Owners = map[string]string{}
	}
	if reg.Orchestrators == nil {
		reg.Orchestrators = map[string]string{}
	}
	if reg.Watches == nil {
		reg.Watches = map[string]Watch{}
	}
	if reg.WorkspaceWatches == nil {
		reg.WorkspaceWatches = map[string]WorkspaceWatch{}
	}
}

// loadRegistryLocked reads the registry; a missing file is an empty
// registry with non-nil maps and Schema LedgerSchema.
func (s *Store) loadRegistryLocked() (Registry, error) {
	var reg Registry
	data, err := os.ReadFile(s.registryPath())
	if errors.Is(err, os.ErrNotExist) {
		return emptyRegistry(), nil
	}
	if err != nil {
		return emptyRegistry(), err
	}
	if err := json.Unmarshal(data, &reg); err != nil {
		return emptyRegistry(), fmt.Errorf("notify: registry.json: %w", err)
	}
	if reg.Schema == 0 {
		reg.Schema = LedgerSchema
	}
	normalizeRegistry(&reg)
	return reg, nil
}

// LoadRegistry returns the registry, or an empty registry with non-nil
// maps and Schema LedgerSchema when the file is absent.
func (s *Store) LoadRegistry() (Registry, error) { return s.loadRegistryLocked() }

// loadLedgerLocked reads the ledger; a missing file is an empty ledger
// with a non-nil Panes map and a non-nil empty Workspaces map.
func (s *Store) loadLedgerLocked() (*Ledger, error) {
	data, err := os.ReadFile(s.ledgerPath())
	if errors.Is(err, os.ErrNotExist) {
		return emptyLedger(), nil
	}
	if err != nil {
		return nil, err
	}
	var led Ledger
	if err := json.Unmarshal(data, &led); err != nil {
		return nil, fmt.Errorf("notify: ledger.json: %w", err)
	}
	if led.Schema == 0 {
		led.Schema = LedgerSchema
	}
	if led.Panes == nil {
		led.Panes = map[string]*PaneState{}
	}
	if led.Notifications == nil {
		led.Notifications = []*Notification{}
	}
	if led.Workspaces == nil {
		led.Workspaces = map[string]*WorkspaceState{}
	}
	return &led, nil
}

// LoadLedger returns the ledger, or an empty ledger (Panes non-nil,
// Workspaces a non-nil empty map) when the file is absent. It is
// read-only: it takes no lock and creates nothing.
func (s *Store) LoadLedger() (*Ledger, error) { return s.loadLedgerLocked() }

// loadLedgerMutatingLocked loads the ledger for one mutating operation,
// under the outbox lock. A missing ledger file (the registry exists but
// the ledger was never written, or was deleted) is initialized at the
// current last outbox seq and reported as initialized, so the records
// stored before it are never backfilled; the operation writes the ledger
// back when it persists. A ledger file that exists but cannot be parsed
// stays an error, never a silent reset.
func (s *Store) loadLedgerMutatingLocked() (*Ledger, bool, error) {
	_, statErr := os.Stat(s.ledgerPath())
	led, err := s.loadLedgerLocked()
	if err != nil {
		return nil, false, err
	}
	if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
		return nil, false, statErr
	}
	if statErr == nil {
		return led, false, nil
	}
	_, last, err := s.ob.Read(0)
	if err != nil {
		return nil, false, err
	}
	led.ProjectedSeq = last
	return led, true, nil
}

func (s *Store) writeRegistryLocked(reg *Registry) error {
	if reg.Schema == 0 {
		reg.Schema = LedgerSchema
	}
	normalizeRegistry(reg)
	data, err := json.Marshal(reg)
	if err != nil {
		return err
	}
	return outbox.WriteFileAtomic(s.registryPath(), data)
}

func (s *Store) writeLedgerLocked(led *Ledger) error {
	if led.Schema == 0 {
		led.Schema = LedgerSchema
	}
	if led.Panes == nil {
		led.Panes = map[string]*PaneState{}
	}
	if led.Notifications == nil {
		led.Notifications = []*Notification{}
	}
	if led.Workspaces == nil {
		led.Workspaces = map[string]*WorkspaceState{}
	}
	data, err := json.Marshal(led)
	if err != nil {
		return err
	}
	return outbox.WriteFileAtomic(s.ledgerPath(), data)
}

func findNotification(led *Ledger, id string) *Notification {
	for i := range led.Notifications {
		if led.Notifications[i].ID == id {
			return led.Notifications[i]
		}
	}
	return nil
}

// copyNotification is a deep copy of one notification (slices and the
// LastExit pointers included).
func copyNotification(n *Notification) Notification {
	c := *n
	c.Escalations = append([]string(nil), n.Escalations...)
	c.Deliveries = make([]Delivery, len(n.Deliveries))
	for i := range n.Deliveries {
		d := n.Deliveries[i]
		d.Roles = append([]string(nil), d.Roles...)
		if n.Deliveries[i].LastExit != nil {
			e := *n.Deliveries[i].LastExit
			d.LastExit = &e
		}
		c.Deliveries[i] = d
	}
	return c
}

// UpdateRegistry mutates the registry under the outbox lock and writes it
// atomically. When the registry did not exist it is created, and when the
// ledger does not exist yet it is created first — with ProjectedSeq at the
// last complete outbox seq — and only then the registry: there is no
// window in which notifications are enabled without a cursor, and old
// records are not backfilled (the cursor only moves forward from here).
// A later update never resets an existing ledger. Validation is the
// caller's job.
func (s *Store) UpdateRegistry(mutate func(*Registry) error) error {
	return s.ob.WithLock(func() error {
		if err := s.ensureDir(); err != nil {
			return err
		}
		reg, err := s.loadRegistryLocked()
		if err != nil {
			return err
		}
		if err := mutate(&reg); err != nil {
			return err
		}
		// The missing ledger is created before the registry enables
		// notifications: no window with notifications on and no cursor.
		if _, err := os.ReadFile(s.ledgerPath()); err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return err
			}
			_, last, err := s.ob.Read(0)
			if err != nil {
				return err
			}
			led := emptyLedger()
			led.ProjectedSeq = last
			if err := s.writeLedgerLocked(led); err != nil {
				return err
			}
		}
		return s.writeRegistryLocked(&reg)
	})
}

// ProjectOutbox projects the stored job_event records after the ledger's
// projection cursor into notifications, under the outbox lock. Records
// whose tipo is not job_event or that carry no job id are skipped; a
// project error counts Malformed and does not stop the run; a
// notification id already in the ledger counts Duplicates. A missing
// ledger (the registry exists, the ledger was never written or was
// deleted) is initialized at the current last outbox seq and written,
// so the stored records are never backfilled. The cursor
// advances to the last complete outbox seq of the lines read, the ledger
// is written once and only when something changed, and before the write
// the final notifications persisted before the retention window are
// pruned. Without a registry it returns a zero summary and writes
// nothing.
func (s *Store) ProjectOutbox(project ProjectFunc) (ProjectSummary, error) {
	if !s.Enabled() {
		return ProjectSummary{}, nil
	}
	var sum ProjectSummary
	err := s.ob.WithLock(func() error {
		led, initialized, err := s.loadLedgerMutatingLocked()
		if err != nil {
			return err
		}
		reg, err := s.loadRegistryLocked()
		if err != nil {
			return err
		}
		lines, last, err := s.ob.Read(led.ProjectedSeq)
		if err != nil {
			return err
		}
		now := s.now()
		nowTS := now.Format(TSLayout)
		seen := make(map[string]bool, len(led.Notifications))
		for i := range led.Notifications {
			seen[led.Notifications[i].ID] = true
		}
		for _, line := range lines {
			var rec outbox.Record
			if err := json.Unmarshal(line, &rec); err != nil {
				sum.Malformed++
				continue
			}
			if rec.Tipo != outbox.TipoJobEvent || rec.JobID == nil {
				continue
			}
			in := JobEventInput{OutboxSeq: rec.Seq, Projeto: rec.Projeto, JobID: *rec.JobID, Event: rec.Dados}
			n, ok, err := project(in, reg, now)
			if err != nil {
				sum.Malformed++
				continue
			}
			if !ok || n == nil {
				continue
			}
			if seen[n.ID] {
				sum.Duplicates++
				continue
			}
			n.PersistedAt = nowTS
			led.Notifications = append(led.Notifications, n)
			seen[n.ID] = true
			sum.Created++
		}
		changed := sum.Created > 0
		if last > led.ProjectedSeq {
			led.ProjectedSeq = last
			changed = true
		}
		sum.ProjectedSeq = led.ProjectedSeq
		if changed || initialized {
			pruneLedger(led, now)
			if err := s.writeLedgerLocked(led); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return ProjectSummary{}, err
	}
	return sum, nil
}

// pruneLedger drops the final notifications persisted before the
// retention window and reports whether the ledger changed.
func pruneLedger(led *Ledger, now time.Time) bool {
	cutoff := now.Add(-retention)
	kept := make([]*Notification, 0, len(led.Notifications))
	changed := false
	for _, n := range led.Notifications {
		if expired(n, cutoff) {
			changed = true
			continue
		}
		kept = append(kept, n)
	}
	led.Notifications = kept
	return changed
}

// expired reports whether a notification is older than the retention
// window and final (every delivery in a final state).
func expired(n *Notification, cutoff time.Time) bool {
	persisted, err := time.Parse(TSLayout, n.PersistedAt)
	if err != nil {
		return false // never drop what cannot be dated
	}
	if !persisted.Before(cutoff) {
		return false
	}
	for i := range n.Deliveries {
		if !isFinalState(n.Deliveries[i].State) {
			return false
		}
	}
	return true
}

func isFinalState(state string) bool {
	switch state {
	case StateAccepted, StateUncertain, StateRejected, StateExhausted, StateNoRoute, StateSelf:
		return true
	}
	return false
}

// IngestAgentStatus records one Herdr agent status event. A pane not in
// the registry's watches is ignored without any write. A repeated status
// is coalesced; a true transition increments the pane's N, is projected,
// and a routine projection (ok=false) is coalesced too. Every write runs
// under the outbox lock. Without a registry it returns the ignored
// summary and writes nothing.
func (s *Store) IngestAgentStatus(ev AgentStatusEvent, project StatusFunc) (IngestSummary, error) {
	if !s.Enabled() {
		return IngestSummary{Ignored: true}, nil
	}
	var sum IngestSummary
	err := s.ob.WithLock(func() error {
		reg, err := s.loadRegistryLocked()
		if err != nil {
			return err
		}
		watch, watched := reg.Watches[ev.Pane]
		if !watched {
			sum = IngestSummary{Ignored: true}
			return nil
		}
		led, _, err := s.loadLedgerMutatingLocked()
		if err != nil {
			return err
		}
		now := s.now()
		nowTS := now.Format(TSLayout)
		ps := led.Panes[ev.Pane]
		if ps == nil {
			ps = &PaneState{}
			led.Panes[ev.Pane] = ps
		}
		if ps.Status == ev.Status {
			ps.Coalesced++
			ps.LastObservedAt = nowTS
			sum.Duplicate = true
			sum.N = ps.N
		} else {
			previous := ps.Status
			ps.N++
			ps.Status = ev.Status
			ps.LastObservedAt = nowTS
			sum.N = ps.N
			tr := Transition{Pane: ev.Pane, Workspace: ev.Workspace, From: previous, To: ev.Status, N: ps.N, Watch: watch}
			n, ok := project(tr, reg, now)
			switch {
			case !ok || n == nil:
				ps.Coalesced++
				sum.Routine = true
			case findNotification(led, n.ID) != nil:
				sum.Duplicate = true
				sum.NotificationID = n.ID
			default:
				n.PersistedAt = nowTS
				led.Notifications = append(led.Notifications, n)
				sum.Created = true
				sum.NotificationID = n.ID
			}
		}
		if err := s.writeLedgerLocked(led); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return IngestSummary{}, err
	}
	return sum, nil
}

// Raise projects one explicit local raise. A raise whose notification id
// already exists returns the stored notification with created=false: a
// wake-style replay never creates a new notification. Without a registry
// it returns ErrNotEnabled.
func (s *Store) Raise(in RaiseInput, project RaiseFunc) (*Notification, bool, error) {
	if !s.Enabled() {
		return nil, false, ErrNotEnabled
	}
	var n *Notification
	var created bool
	err := s.ob.WithLock(func() error {
		led, _, err := s.loadLedgerMutatingLocked()
		if err != nil {
			return err
		}
		reg, err := s.loadRegistryLocked()
		if err != nil {
			return err
		}
		new, err := project(in, reg, s.now())
		if err != nil {
			return err
		}
		if new == nil {
			return errors.New("notify: raise projection returned no notification")
		}
		if existing := findNotification(led, new.ID); existing != nil {
			c := copyNotification(existing)
			n = &c
			return nil
		}
		new.PersistedAt = s.now().Format(TSLayout)
		led.Notifications = append(led.Notifications, new)
		if err := s.writeLedgerLocked(led); err != nil {
			return err
		}
		c := copyNotification(new)
		n = &c
		created = true
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return n, created, nil
}

// IngestWorkspace records one job workspace event. The source is
// resolved in order: the registry's workspace registration (explicit
// registration wins whatever the label, job and projeto from it), else a
// job-<id> label (labelJob, labelProjeto), else, for a closed event, the
// job recorded when the workspace opened; nothing resolved is ignored
// without any write. The same event as the stored state is a duplicate
// (the observation time is updated, no notification); a true transition
// increments the workspace's N, stores the source, projects
// WorkspaceInput{Event, Workspace, N, JobID, Projeto}, and a new id is
// appended with PersistedAt while an existing one is a duplicate. The
// ledger is written once, under the outbox lock. Without a registry it
// returns the ignored summary and writes nothing.
func (s *Store) IngestWorkspace(ev WorkspaceEvent, labelJob, labelProjeto string, project WorkspaceFunc) (WorkspaceSummary, error) {
	if !s.Enabled() {
		return WorkspaceSummary{Ignored: true}, nil
	}
	var sum WorkspaceSummary
	err := s.ob.WithLock(func() error {
		reg, err := s.loadRegistryLocked()
		if err != nil {
			return err
		}
		led, _, err := s.loadLedgerMutatingLocked()
		if err != nil {
			return err
		}
		now := s.now()
		nowTS := now.Format(TSLayout)
		jobID, projeto := "", ""
		resolved := false
		if w, ok := reg.WorkspaceWatches[ev.Workspace]; ok {
			jobID, projeto = w.Job, w.Projeto
			resolved = true
		} else if labelJob != "" {
			jobID, projeto = labelJob, labelProjeto
			resolved = true
		} else if ev.Event == WorkspaceClosed {
			if st := led.Workspaces[ev.Workspace]; st != nil && st.Job != "" {
				jobID, projeto = st.Job, st.Projeto
				resolved = true
			}
		}
		if !resolved {
			sum = WorkspaceSummary{Ignored: true}
			return nil
		}
		st := led.Workspaces[ev.Workspace]
		if st == nil {
			st = &WorkspaceState{}
			led.Workspaces[ev.Workspace] = st
		}
		if st.Event == ev.Event {
			st.LastObservedAt = nowTS
			sum.Duplicate = true
			sum.N = st.N
			sum.JobID = jobID
			if err := s.writeLedgerLocked(led); err != nil {
				return err
			}
			return nil
		}
		st.N++
		st.Event = ev.Event
		st.Job = jobID
		st.Projeto = projeto
		st.LastObservedAt = nowTS
		sum.N = st.N
		sum.JobID = jobID
		in := WorkspaceInput{Event: ev.Event, Workspace: ev.Workspace, N: st.N, JobID: jobID, Projeto: projeto}
		fresh, err := project(in, reg, now)
		if err != nil {
			return err
		}
		if fresh == nil {
			return errors.New("notify: workspace projection returned no notification")
		}
		sum.NotificationID = fresh.ID
		if existing := findNotification(led, fresh.ID); existing != nil {
			sum.Duplicate = true
		} else {
			fresh.PersistedAt = nowTS
			led.Notifications = append(led.Notifications, fresh)
			sum.Created = true
		}
		if err := s.writeLedgerLocked(led); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return WorkspaceSummary{}, err
	}
	return sum, nil
}

// WorkspaceJob reports the job recorded for one Herdr workspace id
// (Workspaces[workspace].Job when non-empty). It is a read-only lookup:
// it takes no lock and creates nothing.
func (s *Store) WorkspaceJob(workspace string) (string, bool) {
	led, err := s.loadLedgerLocked()
	if err != nil {
		return "", false
	}
	st := led.Workspaces[workspace]
	if st == nil {
		return "", false
	}
	return st.Job, st.Job != ""
}

// Claim is one claimed delivery: the address to send, the roles the
// recipient holds, the attempt number after the claim and a copy of the
// notification for rendering.
type Claim struct {
	NotificationID string
	Index          int // index in Deliveries
	Ref            string
	Roles          []string
	Attempt        int          // Attempts after the claim
	Notification   Notification // copy, for rendering

	// prevFirst and prevLast are the attempt timestamps before the
	// claim, restored by Release.
	prevFirst, prevLast string
}

// ClaimDue claims the due deliveries with the default lease
// (LeaseDuration): the same behavior as ClaimDueLease with the default
// lease. A disabled store claims nothing.
func (s *Store) ClaimDue(limit int) ([]Claim, error) {
	return s.ClaimDueLease(limit, LeaseDuration)
}

// ClaimDueLease claims the due deliveries, in ledger order: pending ones
// whose NextAttemptAt has come (empty means due now) and in-flight ones
// whose lease has expired (the earlier run died). Every claim is marked
// in-flight with a fresh lease of the given duration — a live delivery
// run passes its remaining run budget plus the crash margin, so a live
// claim never becomes due while its run can still send it; a lease of
// 0 or less uses LeaseDuration. Attempts is incremented, the attempt
// timestamps are set, and the ledger is written once. limit <= 0 means
// defaultClaimLimit. A disabled store claims nothing.
func (s *Store) ClaimDueLease(limit int, lease time.Duration) ([]Claim, error) {
	if lease <= 0 {
		lease = LeaseDuration
	}
	if limit <= 0 {
		limit = defaultClaimLimit
	}
	if !s.Enabled() {
		return nil, nil
	}
	var claims []Claim
	err := s.ob.WithLock(func() error {
		led, initialized, err := s.loadLedgerMutatingLocked()
		if err != nil {
			return err
		}
		now := s.now()
		nowTS := now.Format(TSLayout)
		leaseTS := now.Add(lease).Format(TSLayout)
		claims = []Claim{}
	outer:
		for i := range led.Notifications {
			n := led.Notifications[i]
			for j := range n.Deliveries {
				d := &n.Deliveries[j]
				if !dueNow(d, now) {
					continue
				}
				prevFirst, prevLast := d.FirstAttemptAt, d.LastAttemptAt
				d.State = StateInFlight
				d.LeaseUntil = leaseTS
				d.Attempts++
				if d.FirstAttemptAt == "" {
					d.FirstAttemptAt = nowTS
				}
				d.LastAttemptAt = nowTS
				claims = append(claims, Claim{
					NotificationID: n.ID,
					Index:          j,
					Ref:            d.Ref,
					Roles:          append([]string(nil), d.Roles...),
					Attempt:        d.Attempts,
					Notification:   copyNotification(n),
					prevFirst:      prevFirst,
					prevLast:       prevLast,
				})
				if len(claims) >= limit {
					break outer
				}
			}
		}
		if len(claims) > 0 || initialized {
			if err := s.writeLedgerLocked(led); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return claims, nil
}

// dueNow reports whether a delivery is claimable at now. Timestamps are
// compared parsed with the ledger layout; an unparseable deadline or
// lease is treated as expired, so a corrupt entry never stalls a
// delivery.
func dueNow(d *Delivery, now time.Time) bool {
	switch d.State {
	case StatePending:
		if d.NextAttemptAt == "" {
			return true
		}
		t, err := time.Parse(TSLayout, d.NextAttemptAt)
		if err != nil {
			return true
		}
		return !t.After(now)
	case StateInFlight:
		if d.LeaseUntil == "" {
			return true
		}
		t, err := time.Parse(TSLayout, d.LeaseUntil)
		if err != nil {
			return true
		}
		return t.Before(now)
	}
	return false
}

// Complete records the outcome of one claimed attempt. A stale claim
// (the delivery is no longer in flight, or its attempts no longer match
// the claim) is ignored with a nil error: only the owner of the current
// claim decides. Accepted carries the send status word; transient
// outcomes are retried on the schedule and become exhausted at
// MaxAttempts.
func (s *Store) Complete(c Claim, r SendResult) error {
	if !s.Enabled() {
		return nil
	}
	return s.ob.WithLock(func() error {
		led, _, err := s.loadLedgerMutatingLocked()
		if err != nil {
			return err
		}
		n := findNotification(led, c.NotificationID)
		if n == nil || c.Index < 0 || c.Index >= len(n.Deliveries) {
			return nil
		}
		d := &n.Deliveries[c.Index]
		if d.State != StateInFlight || d.Attempts != c.Attempt {
			return nil
		}
		applyOutcome(d, r, s.now())
		return s.writeLedgerLocked(led)
	})
}

// Release returns a claimed delivery that was never sent (the delivery
// run's deadline expired before its send started) to pending, due at
// once, without consuming the attempt: Attempts and the attempt
// timestamps go back to their values before the claim. A stale claim
// (the delivery is no longer in flight with the claimed attempt) is
// ignored.
func (s *Store) Release(c Claim) error {
	if !s.Enabled() {
		return nil
	}
	return s.ob.WithLock(func() error {
		led, _, err := s.loadLedgerMutatingLocked()
		if err != nil {
			return err
		}
		n := findNotification(led, c.NotificationID)
		if n == nil || c.Index < 0 || c.Index >= len(n.Deliveries) {
			return nil
		}
		d := &n.Deliveries[c.Index]
		if d.State != StateInFlight || d.Attempts != c.Attempt {
			return nil
		}
		d.State = StatePending
		d.LeaseUntil = ""
		d.Attempts--
		d.FirstAttemptAt, d.LastAttemptAt = c.prevFirst, c.prevLast
		return s.writeLedgerLocked(led)
	})
}

// applyOutcome moves a claimed delivery to its outcome state.
func applyOutcome(d *Delivery, r SendResult, now time.Time) {
	if r.Exit >= 0 {
		exit := r.Exit
		d.LastExit = &exit
	}
	d.LeaseUntil = ""
	switch r.Outcome {
	case OutcomeAccepted:
		d.State = StateAccepted
		d.AcceptedAt = now.Format(TSLayout)
		d.AcceptStatus = r.Status
		d.LastError = ""
	case OutcomeUncertain:
		d.State = StateUncertain
		d.LastError = r.Category
	case OutcomeRejected:
		d.State = StateRejected
		d.LastError = r.Category
	default: // transient, and any unknown outcome, are retried with backoff
		d.LastError = r.Category
		if d.Attempts >= MaxAttempts {
			d.State = StateExhausted
			return
		}
		d.State = StatePending
		idx := d.Attempts - 1
		if idx >= len(RetrySchedule) {
			idx = len(RetrySchedule) - 1
		}
		d.NextAttemptAt = now.Add(RetrySchedule[idx]).Format(TSLayout)
	}
}

// Ack records the optional recipient processing acknowledgement on the
// delivery of notification id whose Roles contain role and whose state is
// accepted or uncertain. A second Ack keeps the first AckAt. found is
// false when no such delivery exists (unknown id, missing role, or a
// non-final state).
func (s *Store) Ack(id, role string) (bool, error) {
	if !s.Enabled() {
		return false, nil
	}
	var found bool
	err := s.ob.WithLock(func() error {
		led, initialized, err := s.loadLedgerMutatingLocked()
		if err != nil {
			return err
		}
		n := findNotification(led, id)
		if n == nil {
			return nil
		}
		changed := false
		for i := range n.Deliveries {
			d := &n.Deliveries[i]
			if !hasRole(d.Roles, role) {
				continue
			}
			if d.State != StateAccepted && d.State != StateUncertain {
				continue
			}
			found = true
			if d.AckAt == "" {
				d.AckAt = s.now().Format(TSLayout)
				changed = true
			}
		}
		if changed || initialized {
			return s.writeLedgerLocked(led)
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	return found, nil
}

// Retry re-queues, once, the delivery of notification id whose Roles
// contain role and whose state is uncertain or exhausted: it becomes
// pending and due at once. It is the explicit decision of someone who
// checked that the recipient did not get the message (an uncertain
// delivery is never resent automatically). Attempts are kept, so an
// exhausted delivery that fails again is exhausted after this one more
// attempt. found is false when there is no such delivery.
func (s *Store) Retry(id, role string) (bool, error) {
	if !s.Enabled() {
		return false, nil
	}
	var found bool
	err := s.ob.WithLock(func() error {
		led, initialized, err := s.loadLedgerMutatingLocked()
		if err != nil {
			return err
		}
		n := findNotification(led, id)
		if n == nil {
			return nil
		}
		for i := range n.Deliveries {
			d := &n.Deliveries[i]
			if !hasRole(d.Roles, role) || (d.State != StateUncertain && d.State != StateExhausted) {
				continue
			}
			found = true
			d.State = StatePending
			d.NextAttemptAt = s.now().Format(TSLayout)
			d.LeaseUntil = ""
		}
		if found || initialized {
			return s.writeLedgerLocked(led)
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	return found, nil
}

func hasRole(roles []string, role string) bool {
	for _, r := range roles {
		if r == role {
			return true
		}
	}
	return false
}
