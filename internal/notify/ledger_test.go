package notify

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/djalmajr/herdr-hermes/internal/outbox"
)

const (
	testOwnerRef  = "owner-agent"
	testProjetoID = "example-org/example-repo"
)

// ledgerTestClock is an injected clock on a non-UTC zone; the tests move
// it explicitly instead of sleeping.
type ledgerTestClock struct {
	mu  sync.Mutex
	now time.Time
}

func newLedgerTestClock() *ledgerTestClock {
	return &ledgerTestClock{now: time.Date(2026, 10, 9, 12, 0, 0, 0, time.FixedZone("", 2*3600))}
}

func (c *ledgerTestClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *ledgerTestClock) Add(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func (c *ledgerTestClock) TS() string { return c.Now().Format(TSLayout) }

// openOutbox opens a throwaway outbox store with a fixed non-UTC clock
// and a short lock timeout so a wrongly held lock fails fast.
func openOutbox(t *testing.T, dir string) *outbox.Store {
	t.Helper()
	ob, err := outbox.Open(dir, outbox.Options{
		Now: func() time.Time {
			return time.Date(2026, 10, 9, 12, 0, 0, 0, time.FixedZone("", 2*3600))
		},
		LockTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("outbox.Open: %v", err)
	}
	return ob
}

// appendJobEvent appends one job contract event to the outbox.
func appendJobEvent(t *testing.T, ob *outbox.Store, jobID string, seq int64, tipo string) {
	t.Helper()
	ev := fmt.Sprintf(`{"tipo":%q,"seq":%d}`, tipo, seq)
	if _, _, err := ob.AppendJobEvent("machine-a", testProjetoID, jobID, json.RawMessage(ev)); err != nil {
		t.Fatalf("AppendJobEvent %s/%d: %v", jobID, seq, err)
	}
}

// enableNotify registers one owner and writes the registry (creating the
// ledger at the current outbox cursor).
func enableNotify(t *testing.T, s *Store) {
	t.Helper()
	if err := s.UpdateRegistry(func(reg *Registry) error {
		reg.Owners[testProjetoID] = testOwnerRef
		return nil
	}); err != nil {
		t.Fatalf("UpdateRegistry: %v", err)
	}
}

// fakeNotificationID mirrors the design ids: "n" plus the first 20 hex
// digits of sha256(source id).
func fakeNotificationID(src string) string {
	sum := sha256.Sum256([]byte(src))
	return "n" + hex.EncodeToString(sum[:])[:20]
}

// fakeJobNotification is the test projection of one job_event record.
func fakeJobNotification(in JobEventInput, now time.Time) *Notification {
	var ev struct {
		Tipo string `json:"tipo"`
		Seq  int64  `json:"seq"`
	}
	_ = json.Unmarshal(in.Event, &ev)
	src := "job:" + in.JobID + ":" + strconv.FormatInt(in.OutboxSeq, 10)
	return &Notification{
		ID:         fakeNotificationID(src),
		SourceKind: SourceJobEvent,
		SourceID:   src,
		Class:      ClassBlocked,
		JobID:      in.JobID,
		Projeto:    in.Projeto,
		EventTipo:  ev.Tipo,
		EventSeq:   ev.Seq,
		ObservedAt: now.Format(TSLayout),
		Deliveries: []Delivery{{Roles: []string{RoleOwner}, Ref: testOwnerRef, State: StatePending}},
	}
}

// fakeProjectJobEvent always projects the record into one notification.
func fakeProjectJobEvent(in JobEventInput, reg Registry, now time.Time) (*Notification, bool, error) {
	return fakeJobNotification(in, now), true, nil
}

// fakeProjectAgentStatus projects blocked transitions into one
// notification; the other statuses are routine.
func fakeProjectAgentStatus(tr Transition, reg Registry, now time.Time) (*Notification, bool) {
	if tr.To != "blocked" {
		return nil, false
	}
	src := "agent:" + tr.Pane + ":" + strconv.FormatInt(tr.N, 10) + ":" + tr.To
	return &Notification{
		ID:          fakeNotificationID(src),
		SourceKind:  SourceAgentStatus,
		SourceID:    src,
		Class:       ClassAgentBlocked,
		JobID:       tr.Watch.Job,
		Projeto:     tr.Watch.Projeto,
		Pane:        tr.Pane,
		Workspace:   tr.Workspace,
		Status:      tr.To,
		Escalations: []string{EscBlocked},
		ObservedAt:  now.Format(TSLayout),
		Deliveries:  []Delivery{{Roles: []string{RoleOwner}, Ref: testOwnerRef, State: StatePending}},
	}, true
}

// fakeProjectRaise projects one explicit raise.
func fakeProjectRaise(in RaiseInput, reg Registry, now time.Time) (*Notification, error) {
	src := "raise:" + in.ID
	return &Notification{
		ID:         fakeNotificationID(src),
		SourceKind: SourceRaise,
		SourceID:   src,
		Class:      in.Class,
		JobID:      in.JobID,
		Projeto:    in.Projeto,
		ObservedAt: now.Format(TSLayout),
		Deliveries: []Delivery{{Roles: []string{RoleOwner}, Ref: testOwnerRef, State: StatePending}},
	}, nil
}

// assertNoDuplicateDeliveries checks that no (source, recipient) pair
// appears twice in the ledger.
func assertNoDuplicateDeliveries(t *testing.T, led *Ledger) {
	t.Helper()
	seen := map[string]bool{}
	for _, n := range led.Notifications {
		for _, d := range n.Deliveries {
			key := n.SourceID + "/" + d.Ref
			if seen[key] {
				t.Fatalf("duplicate delivery for source %s ref %s", n.SourceID, d.Ref)
			}
			seen[key] = true
		}
	}
}

// overwriteLedger rewrites ledger.json directly (test state setup).
func overwriteLedger(t *testing.T, s *Store, led *Ledger) {
	t.Helper()
	data, err := json.Marshal(led)
	if err != nil {
		t.Fatalf("marshal ledger: %v", err)
	}
	if err := outbox.WriteFileAtomic(filepath.Join(s.Dir(), "ledger.json"), data); err != nil {
		t.Fatalf("write ledger: %v", err)
	}
}

// TestLedgerDisabledWritesNothing: without a registry the projection and
// the ingest write nothing (the notify dir is not created) and Raise
// fails with ErrNotEnabled.
func TestLedgerDisabledWritesNothing(t *testing.T) {
	ob := openOutbox(t, t.TempDir())
	appendJobEvent(t, ob, "job-1", 1, "blocked")
	s := Open(ob, newLedgerTestClock().Now)
	if s.Enabled() {
		t.Fatal("store is enabled without a registry")
	}
	sum, err := s.ProjectOutbox(fakeProjectJobEvent)
	if err != nil || sum != (ProjectSummary{}) {
		t.Fatalf("ProjectOutbox = %+v, %v; want zero summary", sum, err)
	}
	sum2, err := s.IngestAgentStatus(AgentStatusEvent{Pane: "w1:p1", Workspace: "w1", Status: "working"}, fakeProjectAgentStatus)
	if err != nil || sum2 != (IngestSummary{Ignored: true}) {
		t.Fatalf("IngestAgentStatus = %+v, %v; want Ignored summary", sum2, err)
	}
	if _, _, err := s.Raise(RaiseInput{ID: "ra-1", Class: ClassStuck}, fakeProjectRaise); !errors.Is(err, ErrNotEnabled) {
		t.Fatalf("Raise error = %v, want ErrNotEnabled", err)
	}
	sum3, err := s.IngestWorkspace(WorkspaceEvent{Event: WorkspaceCreated, Workspace: "w2", Label: "job-1"}, "job-1", testProjetoID, ProjectWorkspace)
	if err != nil || sum3 != (WorkspaceSummary{Ignored: true}) {
		t.Fatalf("IngestWorkspace = %+v, %v; want the ignored summary", sum3, err)
	}
	if _, err := os.Stat(s.Dir()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("notify dir %s exists, want missing (stat err %v)", s.Dir(), err)
	}
}

// TestLedgerEnableNoBackfill: enabling with an outbox that already has
// records starts the cursor at the last seq (no backfill); later records
// are projected.
func TestLedgerEnableNoBackfill(t *testing.T) {
	ob := openOutbox(t, t.TempDir())
	appendJobEvent(t, ob, "job-1", 1, "blocked")
	appendJobEvent(t, ob, "job-1", 2, "failure")
	appendJobEvent(t, ob, "job-2", 1, "question")
	clk := newLedgerTestClock()
	s := Open(ob, clk.Now)
	if s.Enabled() {
		t.Fatal("store is enabled without a registry")
	}
	if err := s.UpdateRegistry(func(reg *Registry) error {
		reg.Owners[testProjetoID] = testOwnerRef
		return nil
	}); err != nil {
		t.Fatalf("UpdateRegistry: %v", err)
	}
	if !s.Enabled() {
		t.Fatal("store not enabled after UpdateRegistry")
	}
	led, err := s.LoadLedger()
	if err != nil {
		t.Fatalf("LoadLedger: %v", err)
	}
	if led.ProjectedSeq != 3 {
		t.Fatalf("ProjectedSeq = %d, want 3 (no backfill)", led.ProjectedSeq)
	}
	if len(led.Notifications) != 0 {
		t.Fatalf("notifications = %d, want 0 (no backfill)", len(led.Notifications))
	}
	if led.Panes == nil {
		t.Fatal("Panes is nil on a fresh ledger")
	}
	// the old records are not projected.
	sum, err := s.ProjectOutbox(fakeProjectJobEvent)
	if err != nil {
		t.Fatalf("ProjectOutbox: %v", err)
	}
	if sum.Created != 0 || sum.Duplicates != 0 || sum.Malformed != 0 || sum.ProjectedSeq != 3 {
		t.Fatalf("summary = %+v, want no counts and ProjectedSeq 3", sum)
	}
	// a later record is projected.
	appendJobEvent(t, ob, "job-1", 3, "blocked")
	clk.Add(time.Minute)
	sum, err = s.ProjectOutbox(fakeProjectJobEvent)
	if err != nil {
		t.Fatalf("ProjectOutbox: %v", err)
	}
	if sum.Created != 1 || sum.ProjectedSeq != 4 {
		t.Fatalf("summary = %+v, want Created 1, ProjectedSeq 4", sum)
	}
	led, err = s.LoadLedger()
	if err != nil {
		t.Fatalf("LoadLedger: %v", err)
	}
	if len(led.Notifications) != 1 {
		t.Fatalf("notifications = %d, want 1", len(led.Notifications))
	}
	n := led.Notifications[0]
	if n.JobID != "job-1" || n.EventSeq != 3 {
		t.Fatalf("notification = job %s event %d, want job-1 event 3", n.JobID, n.EventSeq)
	}
	if _, err := time.Parse(TSLayout, n.PersistedAt); err != nil {
		t.Fatalf("PersistedAt %q does not parse with TSLayout: %v", n.PersistedAt, err)
	}
	if strings.Contains(n.PersistedAt, "Z") {
		t.Fatalf("PersistedAt %q ends with Z", n.PersistedAt)
	}
	if !strings.HasSuffix(n.PersistedAt, "+02:00") {
		t.Fatalf("PersistedAt %q has no explicit offset", n.PersistedAt)
	}
	// a registry update does not reset an existing ledger cursor.
	if err := s.UpdateRegistry(func(reg *Registry) error {
		reg.Coordinator = "coordinator-agent"
		return nil
	}); err != nil {
		t.Fatalf("UpdateRegistry: %v", err)
	}
	led, err = s.LoadLedger()
	if err != nil {
		t.Fatalf("LoadLedger: %v", err)
	}
	if led.ProjectedSeq != 4 {
		t.Fatalf("ProjectedSeq = %d after a registry update, want 4 (unchanged)", led.ProjectedSeq)
	}
	if len(led.Notifications) != 1 {
		t.Fatalf("notifications = %d after a registry update, want 1", len(led.Notifications))
	}
	reg, err := s.LoadRegistry()
	if err != nil {
		t.Fatalf("LoadRegistry: %v", err)
	}
	if reg.Owners[testProjetoID] != testOwnerRef || reg.Coordinator != "coordinator-agent" || reg.Schema != LedgerSchema {
		t.Fatalf("registry = %+v, want owner and coordinator kept with schema %d", reg, LedgerSchema)
	}
	if reg.Watches == nil || reg.Orchestrators == nil {
		t.Fatal("registry maps are nil")
	}
}

// TestLedgerMissingLedgerNoBackfill: a registry that exists without a
// ledger (the crash state: the registry was written and the ledger
// never was, or the file was deleted) must never backfill the stored
// records: the first mutating operation initializes the cursor at the
// last outbox seq. ProjectOutbox on that state writes the initialized
// ledger and creates 0, and a later appended event is projected. A
// ledger file that exists but cannot be parsed stays an error, never a
// silent reset.
func TestLedgerMissingLedgerNoBackfill(t *testing.T) {
	ob := openOutbox(t, t.TempDir())
	clk := newLedgerTestClock()
	s := Open(ob, clk.Now)
	// the records first: they must not be backfilled later.
	appendJobEvent(t, ob, "job-1", 1, "blocked")
	appendJobEvent(t, ob, "job-1", 2, "failure")
	enableNotify(t, s)
	// the crash state: the registry exists, the ledger was not written.
	if err := os.Remove(filepath.Join(s.Dir(), "ledger.json")); err != nil {
		t.Fatalf("remove ledger: %v", err)
	}
	sum, err := s.ProjectOutbox(fakeProjectJobEvent)
	if err != nil {
		t.Fatalf("ProjectOutbox: %v", err)
	}
	if sum.Created != 0 || sum.Duplicates != 0 || sum.Malformed != 0 || sum.ProjectedSeq != 2 {
		t.Fatalf("summary = %+v, want 0 created and ProjectedSeq 2 (no backfill)", sum)
	}
	led, err := s.LoadLedger()
	if err != nil {
		t.Fatalf("LoadLedger: %v", err)
	}
	if led.ProjectedSeq != 2 || len(led.Notifications) != 0 {
		t.Fatalf("ledger = seq %d with %d notifications, want the initialized cursor 2 and none", led.ProjectedSeq, len(led.Notifications))
	}
	// a later appended event is projected.
	appendJobEvent(t, ob, "job-1", 3, "blocked")
	clk.Add(time.Minute)
	sum, err = s.ProjectOutbox(fakeProjectJobEvent)
	if err != nil {
		t.Fatalf("ProjectOutbox: %v", err)
	}
	if sum.Created != 1 || sum.ProjectedSeq != 3 {
		t.Fatalf("summary = %+v, want Created 1, ProjectedSeq 3", sum)
	}
	// a ledger file that exists but cannot be parsed stays an error.
	if err := os.WriteFile(filepath.Join(s.Dir(), "ledger.json"), []byte("{not-json"), 0o600); err != nil {
		t.Fatalf("write corrupt ledger: %v", err)
	}
	if _, err := s.ProjectOutbox(fakeProjectJobEvent); err == nil {
		t.Fatal("ProjectOutbox on a corrupt ledger: want an error, got nil")
	}
}

// TestLedgerClaimDueLease: ClaimDueLease claims with the given lease (a
// live run passes its remaining budget plus the crash margin), ClaimDue
// keeps the default lease (LeaseDuration) and a non-positive lease falls
// back to it; the long-lease delivery stays unclaimable past the default
// expiry and becomes due again only when its own lease expires.
func TestLedgerClaimDueLease(t *testing.T) {
	ob := openOutbox(t, t.TempDir())
	clk := newLedgerTestClock()
	s := Open(ob, clk.Now)
	enableNotify(t, s)
	appendJobEvent(t, ob, "job-1", 1, "blocked")
	appendJobEvent(t, ob, "job-1", 2, "failure")
	appendJobEvent(t, ob, "job-1", 3, "question")
	if _, err := s.ProjectOutbox(fakeProjectJobEvent); err != nil {
		t.Fatalf("ProjectOutbox: %v", err)
	}
	// claim the three deliveries (ledger order): one with a long lease,
	// one with the default lease, one with a non-positive lease.
	long := 2 * time.Minute
	c1, err := s.ClaimDueLease(1, long)
	if err != nil || len(c1) != 1 || c1[0].Attempt != 1 {
		t.Fatalf("ClaimDueLease = %+v, %v; want one claim with Attempt 1", c1, err)
	}
	c2, err := s.ClaimDue(1)
	if err != nil || len(c2) != 1 || c2[0].Attempt != 1 {
		t.Fatalf("ClaimDue = %+v, %v; want one claim with Attempt 1", c2, err)
	}
	c3, err := s.ClaimDueLease(1, 0)
	if err != nil || len(c3) != 1 || c3[0].Attempt != 1 {
		t.Fatalf("ClaimDueLease(0) = %+v, %v; want one claim with Attempt 1", c3, err)
	}
	led, err := s.LoadLedger()
	if err != nil {
		t.Fatalf("LoadLedger: %v", err)
	}
	leaseUntil := func(id string) time.Time {
		t.Helper()
		for i := range led.Notifications {
			if led.Notifications[i].ID != id {
				continue
			}
			d := led.Notifications[i].Deliveries[0]
			ts, err := time.Parse(TSLayout, d.LeaseUntil)
			if err != nil {
				t.Fatalf("LeaseUntil %q: %v", d.LeaseUntil, err)
			}
			return ts
		}
		t.Fatalf("notification %s not in the ledger", id)
		return time.Time{}
	}
	if got, want := leaseUntil(c1[0].NotificationID), clk.Now().Add(long); !got.Equal(want) {
		t.Fatalf("long-lease LeaseUntil = %s, want %s (the given lease)", got.Format(TSLayout), want.Format(TSLayout))
	}
	for _, c := range []Claim{c2[0], c3[0]} {
		if got, want := leaseUntil(c.NotificationID), clk.Now().Add(LeaseDuration); !got.Equal(want) {
			t.Fatalf("default-lease LeaseUntil = %s, want %s (LeaseDuration)", got.Format(TSLayout), want.Format(TSLayout))
		}
	}
	// past the default lease: the two default-lease deliveries are due
	// again, the long-lease one stays held.
	clk.Add(LeaseDuration + time.Second)
	cs, err := s.ClaimDue(0)
	if err != nil {
		t.Fatalf("ClaimDue: %v", err)
	}
	if len(cs) != 2 {
		t.Fatalf("claims = %+v, want the two default-lease deliveries only", cs)
	}
	byID := map[string]Claim{}
	for _, c := range cs {
		byID[c.NotificationID] = c
	}
	if c, ok := byID[c1[0].NotificationID]; ok {
		t.Fatalf("the long-lease delivery was claimed before its lease expires: %+v", c)
	}
	for _, id := range []string{c2[0].NotificationID, c3[0].NotificationID} {
		if c, ok := byID[id]; !ok || c.Attempt != 2 {
			t.Fatalf("default-lease delivery %s not re-claimed at Attempt 2: %+v", id, cs)
		}
	}
	// past the long lease too: only that delivery is due again; the two
	// re-claimed ones carry a fresh default lease that expires exactly
	// at this clock value, and the expiry check is strict (lease < now).
	clk.Add(long - LeaseDuration)
	cs, err = s.ClaimDue(0)
	if err != nil {
		t.Fatalf("ClaimDue: %v", err)
	}
	if len(cs) != 1 || cs[0].NotificationID != c1[0].NotificationID || cs[0].Attempt != 2 {
		t.Fatalf("claims = %+v, want only the long-lease delivery at Attempt 2", cs)
	}
}

// TestLedgerProjectReplayNoDuplicates: projecting the same records
// several times never creates a duplicate notification or a second
// delivery for one (source, recipient).
func TestLedgerProjectReplayNoDuplicates(t *testing.T) {
	ob := openOutbox(t, t.TempDir())
	clk := newLedgerTestClock()
	s := Open(ob, clk.Now)
	enableNotify(t, s)
	appendJobEvent(t, ob, "job-1", 1, "blocked")
	appendJobEvent(t, ob, "job-1", 2, "failure")
	for i := 0; i < 3; i++ {
		clk.Add(time.Minute)
		if _, err := s.ProjectOutbox(fakeProjectJobEvent); err != nil {
			t.Fatalf("run %d: %v", i+1, err)
		}
	}
	led, err := s.LoadLedger()
	if err != nil {
		t.Fatalf("LoadLedger: %v", err)
	}
	if len(led.Notifications) != 2 {
		t.Fatalf("notifications = %d, want 2", len(led.Notifications))
	}
	assertNoDuplicateDeliveries(t, led)
	for _, n := range led.Notifications {
		if len(n.Deliveries) != 1 || n.Deliveries[0].Ref != testOwnerRef {
			t.Fatalf("%s deliveries = %+v, want exactly one for %s", n.SourceID, n.Deliveries, testOwnerRef)
		}
	}
}

// TestLedgerProjectConcurrentStores: a second store projecting from
// goroutines never duplicates the first store's work.
func TestLedgerProjectConcurrentStores(t *testing.T) {
	ob := openOutbox(t, t.TempDir())
	clk := newLedgerTestClock()
	a := Open(ob, clk.Now)
	enableNotify(t, a)
	appendJobEvent(t, ob, "job-1", 1, "blocked")
	appendJobEvent(t, ob, "job-1", 2, "failure")
	b := Open(ob, clk.Now) // second store on the same outbox dir
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for _, s := range []*Store{a, b, a, b} {
		wg.Add(1)
		go func(s *Store) {
			defer wg.Done()
			if _, err := s.ProjectOutbox(fakeProjectJobEvent); err != nil {
				errs <- err
			}
		}(s)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("ProjectOutbox: %v", err)
		}
	}
	led, err := a.LoadLedger()
	if err != nil {
		t.Fatalf("LoadLedger: %v", err)
	}
	if len(led.Notifications) != 2 {
		t.Fatalf("notifications = %d, want 2", len(led.Notifications))
	}
	assertNoDuplicateDeliveries(t, led)
	if led.ProjectedSeq != 2 {
		t.Fatalf("ProjectedSeq = %d, want 2", led.ProjectedSeq)
	}
}

// TestLedgerLeaseRestart: a claimed delivery that is never completed
// becomes due again once the lease expires; before the expiry it is not
// claimed.
func TestLedgerLeaseRestart(t *testing.T) {
	ob := openOutbox(t, t.TempDir())
	clk := newLedgerTestClock()
	s := Open(ob, clk.Now)
	enableNotify(t, s)
	appendJobEvent(t, ob, "job-1", 1, "blocked")
	if _, err := s.ProjectOutbox(fakeProjectJobEvent); err != nil {
		t.Fatalf("ProjectOutbox: %v", err)
	}
	claims, err := s.ClaimDue(0)
	if err != nil {
		t.Fatalf("ClaimDue: %v", err)
	}
	if len(claims) != 1 || claims[0].Attempt != 1 {
		t.Fatalf("claims = %+v, want one claim with Attempt 1", claims)
	}
	// crash: no Complete. While the lease is valid the delivery is not due.
	clk.Add(LeaseDuration - time.Second)
	claims, err = s.ClaimDue(0)
	if err != nil {
		t.Fatalf("ClaimDue: %v", err)
	}
	if len(claims) != 0 {
		t.Fatalf("claims before the lease expires = %d, want 0", len(claims))
	}
	// after the lease expires the delivery is due again, with Attempt 2.
	clk.Add(2 * time.Second)
	claims, err = s.ClaimDue(0)
	if err != nil {
		t.Fatalf("ClaimDue: %v", err)
	}
	if len(claims) != 1 || claims[0].Attempt != 2 {
		t.Fatalf("claims after the lease expires = %+v, want one claim with Attempt 2", claims)
	}
	if claims[0].Ref != testOwnerRef || len(claims[0].Roles) != 1 || claims[0].Roles[0] != RoleOwner {
		t.Fatalf("claim ref/roles = %s %+v, want %s [%s]", claims[0].Ref, claims[0].Roles, testOwnerRef, RoleOwner)
	}
	// the claim carries an independent copy of the notification for
	// rendering.
	if len(claims[0].Notification.Deliveries) != 1 || claims[0].Notification.Deliveries[0].Ref != testOwnerRef {
		t.Fatalf("claim copy deliveries = %+v, want one for %s", claims[0].Notification.Deliveries, testOwnerRef)
	}
	if claims[0].Notification.Deliveries[0].State != StateInFlight {
		t.Fatalf("claim copy state = %s, want in_flight (post-claim)", claims[0].Notification.Deliveries[0].State)
	}
	if claims[0].Notification.Deliveries[0].FirstAttemptAt == "" {
		t.Fatal("claim copy has no FirstAttemptAt")
	}
}

// TestLedgerRetryScheduleAndExhaustion: transient outcomes follow the
// retry schedule on the injected clock and become exhausted after
// MaxAttempts; an exhausted delivery is never claimed again.
func TestLedgerRetryScheduleAndExhaustion(t *testing.T) {
	ob := openOutbox(t, t.TempDir())
	clk := newLedgerTestClock()
	s := Open(ob, clk.Now)
	enableNotify(t, s)
	appendJobEvent(t, ob, "job-1", 1, "blocked")
	if _, err := s.ProjectOutbox(fakeProjectJobEvent); err != nil {
		t.Fatalf("ProjectOutbox: %v", err)
	}
	deltas := []time.Duration{}
	for attempt := 1; attempt <= MaxAttempts; attempt++ {
		claims, err := s.ClaimDue(0)
		if err != nil {
			t.Fatalf("attempt %d ClaimDue: %v", attempt, err)
		}
		if len(claims) != 1 || claims[0].Attempt != attempt {
			t.Fatalf("attempt %d: claims = %+v, want one claim with Attempt %d", attempt, claims, attempt)
		}
		c := claims[0]
		before := clk.Now()
		if err := s.Complete(c, SendResult{Outcome: OutcomeTransient, Exit: 1, Category: "exit"}); err != nil {
			t.Fatalf("attempt %d Complete: %v", attempt, err)
		}
		led, err := s.LoadLedger()
		if err != nil {
			t.Fatalf("LoadLedger: %v", err)
		}
		d := led.Notifications[0].Deliveries[0]
		if d.LastExit == nil || *d.LastExit != 1 {
			t.Fatalf("attempt %d LastExit = %v, want 1", attempt, d.LastExit)
		}
		if d.LastError != "exit" {
			t.Fatalf("attempt %d LastError = %q, want exit", attempt, d.LastError)
		}
		if d.FirstAttemptAt == "" || d.LastAttemptAt == "" {
			t.Fatalf("attempt %d: attempt timestamps not set (first %q last %q)", attempt, d.FirstAttemptAt, d.LastAttemptAt)
		}
		if attempt == MaxAttempts {
			if d.State != StateExhausted {
				t.Fatalf("state = %s, want exhausted after %d attempts", d.State, MaxAttempts)
			}
			break
		}
		if d.State != StatePending {
			t.Fatalf("attempt %d state = %s, want pending", attempt, d.State)
		}
		next, err := time.Parse(TSLayout, d.NextAttemptAt)
		if err != nil {
			t.Fatalf("NextAttemptAt %q: %v", d.NextAttemptAt, err)
		}
		deltas = append(deltas, next.Sub(before))
		clk.Add(next.Sub(before))
	}
	want := []time.Duration{10 * time.Second, 30 * time.Second, 90 * time.Second, 300 * time.Second, 900 * time.Second}
	if len(deltas) != len(want) {
		t.Fatalf("deltas = %v, want %v", deltas, want)
	}
	for i := range want {
		if deltas[i] != want[i] {
			t.Fatalf("delta %d = %v, want %v", i, deltas[i], want[i])
		}
	}
	// exhausted is final: never claimed again.
	clk.Add(time.Hour)
	claims, err := s.ClaimDue(0)
	if err != nil {
		t.Fatalf("ClaimDue: %v", err)
	}
	if len(claims) != 0 {
		t.Fatalf("claims after exhaustion = %d, want 0", len(claims))
	}
}

// TestLedgerFinalStatesNeverClaimed: accepted, uncertain and rejected
// deliveries are final; only the pending retry is claimed again.
func TestLedgerFinalStatesNeverClaimed(t *testing.T) {
	ob := openOutbox(t, t.TempDir())
	clk := newLedgerTestClock()
	s := Open(ob, clk.Now)
	enableNotify(t, s)
	for i := 0; i < 4; i++ {
		appendJobEvent(t, ob, "job-1", int64(i+1), "blocked")
	}
	if _, err := s.ProjectOutbox(fakeProjectJobEvent); err != nil {
		t.Fatalf("ProjectOutbox: %v", err)
	}
	claims, err := s.ClaimDue(0)
	if err != nil {
		t.Fatalf("ClaimDue: %v", err)
	}
	if len(claims) != 4 {
		t.Fatalf("claims = %d, want 4", len(claims))
	}
	results := []SendResult{
		{Outcome: OutcomeAccepted, Status: "sent", Exit: 0},
		{Outcome: OutcomeUncertain, Exit: 15, Category: "uncertain"},
		{Outcome: OutcomeRejected, Exit: 2, Category: "refused"},
		{Outcome: OutcomeTransient, Exit: 1, Category: "exit"},
	}
	for i, c := range claims {
		if err := s.Complete(c, results[i]); err != nil {
			t.Fatalf("Complete %d: %v", i, err)
		}
	}
	led, err := s.LoadLedger()
	if err != nil {
		t.Fatalf("LoadLedger: %v", err)
	}
	wantStates := []string{StateAccepted, StateUncertain, StateRejected, StatePending}
	for i, ws := range wantStates {
		d := led.Notifications[i].Deliveries[0]
		if d.State != ws {
			t.Fatalf("n%d state = %s, want %s", i, d.State, ws)
		}
		if d.LeaseUntil != "" {
			t.Fatalf("n%d LeaseUntil = %q, want cleared after Complete", i, d.LeaseUntil)
		}
	}
	if got := led.Notifications[0].Deliveries[0].AcceptStatus; got != "sent" {
		t.Fatalf("AcceptStatus = %q, want sent", got)
	}
	if got := led.Notifications[0].Deliveries[0].LastError; got != "" {
		t.Fatalf("accepted LastError = %q, want empty", got)
	}
	if got := led.Notifications[1].Deliveries[0].LastError; got != "uncertain" {
		t.Fatalf("uncertain LastError = %q, want uncertain", got)
	}
	if got := led.Notifications[2].Deliveries[0].LastError; got != "refused" {
		t.Fatalf("rejected LastError = %q, want refused", got)
	}
	// the final states are never claimed again; only the pending retry is
	// claimed, until it exhausts the remaining attempts.
	clk.Add(24 * time.Hour)
	for attempt := 2; attempt <= MaxAttempts; attempt++ {
		cs, err := s.ClaimDue(0)
		if err != nil {
			t.Fatalf("retry %d ClaimDue: %v", attempt, err)
		}
		if len(cs) != 1 || cs[0].NotificationID != led.Notifications[3].ID {
			t.Fatalf("retry %d claims = %+v, want only the pending retry", attempt, cs)
		}
		if err := s.Complete(cs[0], SendResult{Outcome: OutcomeTransient, Exit: 1, Category: "exit"}); err != nil {
			t.Fatalf("retry %d Complete: %v", attempt, err)
		}
		clk.Add(time.Hour)
	}
	// it exhausted: never claimed again.
	claims, err = s.ClaimDue(0)
	if err != nil {
		t.Fatalf("ClaimDue: %v", err)
	}
	if len(claims) != 0 {
		t.Fatalf("claims after exhaustion = %d, want 0", len(claims))
	}
	led, err = s.LoadLedger()
	if err != nil {
		t.Fatalf("LoadLedger: %v", err)
	}
	if got := led.Notifications[3].Deliveries[0].State; got != StateExhausted {
		t.Fatalf("state = %s, want exhausted", got)
	}
}

// TestLedgerStaleCompleteIgnored: completing a re-claimed delivery with
// the old claim is ignored; the current claim decides.
func TestLedgerStaleCompleteIgnored(t *testing.T) {
	ob := openOutbox(t, t.TempDir())
	clk := newLedgerTestClock()
	s := Open(ob, clk.Now)
	enableNotify(t, s)
	appendJobEvent(t, ob, "job-1", 1, "blocked")
	if _, err := s.ProjectOutbox(fakeProjectJobEvent); err != nil {
		t.Fatalf("ProjectOutbox: %v", err)
	}
	claims, err := s.ClaimDue(0)
	if err != nil || len(claims) != 1 {
		t.Fatalf("ClaimDue = %+v, %v; want one claim", claims, err)
	}
	clk.Add(LeaseDuration + time.Second)
	claims2, err := s.ClaimDue(0)
	if err != nil || len(claims2) != 1 || claims2[0].Attempt != 2 {
		t.Fatalf("re-claim = %+v, %v; want one claim with Attempt 2", claims2, err)
	}
	// the stale claim is ignored: the delivery stays in flight.
	if err := s.Complete(claims[0], SendResult{Outcome: OutcomeAccepted, Status: "sent", Exit: 0}); err != nil {
		t.Fatalf("stale Complete: %v", err)
	}
	led, err := s.LoadLedger()
	if err != nil {
		t.Fatalf("LoadLedger: %v", err)
	}
	d := led.Notifications[0].Deliveries[0]
	if d.State != StateInFlight || d.Attempts != 2 {
		t.Fatalf("after stale complete: state = %s attempts = %d, want in_flight/2", d.State, d.Attempts)
	}
	if d.AcceptedAt != "" {
		t.Fatalf("stale complete set AcceptedAt %q", d.AcceptedAt)
	}
	// the current claim completes the delivery.
	if err := s.Complete(claims2[0], SendResult{Outcome: OutcomeAccepted, Status: "queued", Exit: 0}); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	led, err = s.LoadLedger()
	if err != nil {
		t.Fatalf("LoadLedger: %v", err)
	}
	d = led.Notifications[0].Deliveries[0]
	if d.State != StateAccepted || d.AcceptStatus != "queued" {
		t.Fatalf("after current complete: state = %s accept = %q, want accepted/queued", d.State, d.AcceptStatus)
	}
	// a Complete on a final delivery is a no-op.
	if err := s.Complete(claims2[0], SendResult{Outcome: OutcomeRejected, Exit: 2, Category: "refused"}); err != nil {
		t.Fatalf("Complete on final: %v", err)
	}
	led, err = s.LoadLedger()
	if err != nil {
		t.Fatalf("LoadLedger: %v", err)
	}
	if got := led.Notifications[0].Deliveries[0].State; got != StateAccepted {
		t.Fatalf("state = %s after a Complete on a final delivery, want accepted", got)
	}
}

// TestLedgerAgentStatus: first observation, repeated status, routine
// transitions, blocked transitions (created, distinct per N) and an
// unwatched pane ignored without any write.
func TestLedgerAgentStatus(t *testing.T) {
	ob := openOutbox(t, t.TempDir())
	clk := newLedgerTestClock()
	s := Open(ob, clk.Now)
	err := s.UpdateRegistry(func(reg *Registry) error {
		reg.Owners[testProjetoID] = testOwnerRef
		reg.Watches["w1:p2"] = Watch{Pane: "w1:p2", Job: "job-1", Projeto: testProjetoID}
		return nil
	})
	if err != nil {
		t.Fatalf("UpdateRegistry: %v", err)
	}
	var trs []Transition
	proj := func(tr Transition, reg Registry, now time.Time) (*Notification, bool) {
		trs = append(trs, tr)
		return fakeProjectAgentStatus(tr, reg, now)
	}
	ingest := func(pane, status string) IngestSummary {
		t.Helper()
		sum, err := s.IngestAgentStatus(AgentStatusEvent{Pane: pane, Workspace: "w1", Status: status}, proj)
		if err != nil {
			t.Fatalf("IngestAgentStatus %s %s: %v", pane, status, err)
		}
		return sum
	}
	// first observation: routine (working), N=1, coalesced.
	if sum := ingest("w1:p2", "working"); sum.Created || sum.Duplicate || !sum.Routine || sum.N != 1 {
		t.Fatalf("first observation = %+v, want routine N=1", sum)
	}
	// a repeated status: duplicate, coalesced, same N.
	if sum := ingest("w1:p2", "working"); !sum.Duplicate || sum.Routine || sum.N != 1 {
		t.Fatalf("repeated status = %+v, want duplicate N=1", sum)
	}
	// blocked: a true transition with a notification, N=2.
	sum := ingest("w1:p2", "blocked")
	if !sum.Created || sum.Duplicate || sum.Routine || sum.N != 2 || sum.NotificationID == "" {
		t.Fatalf("blocked = %+v, want created N=2 with NotificationID", sum)
	}
	led, err := s.LoadLedger()
	if err != nil {
		t.Fatalf("LoadLedger: %v", err)
	}
	if len(led.Notifications) != 1 {
		t.Fatalf("notifications = %d, want 1", len(led.Notifications))
	}
	n := led.Notifications[0]
	if n.SourceID != "agent:w1:p2:2:blocked" {
		t.Fatalf("SourceID = %s, want agent:w1:p2:2:blocked", n.SourceID)
	}
	if n.Class != ClassAgentBlocked || n.JobID != "job-1" || n.Projeto != testProjetoID || n.Pane != "w1:p2" || n.Workspace != "w1" {
		t.Fatalf("notification = %+v, want blocked fields from the watch", n)
	}
	ps := led.Panes["w1:p2"]
	if ps == nil || ps.Status != "blocked" || ps.N != 2 || ps.Coalesced != 2 {
		t.Fatalf("pane state = %+v, want blocked N=2 coalesced 2", ps)
	}
	if ps.LastObservedAt == "" {
		t.Fatal("pane state has no LastObservedAt")
	}
	// working again after blocked: routine, N=3.
	if sum := ingest("w1:p2", "working"); !sum.Routine || sum.N != 3 {
		t.Fatalf("working again = %+v, want routine N=3", sum)
	}
	// blocked again: a new N and a distinct notification.
	sum = ingest("w1:p2", "blocked")
	if !sum.Created || sum.N != 4 {
		t.Fatalf("blocked again = %+v, want created N=4", sum)
	}
	led, err = s.LoadLedger()
	if err != nil {
		t.Fatalf("LoadLedger: %v", err)
	}
	if len(led.Notifications) != 2 {
		t.Fatalf("notifications = %d, want 2", len(led.Notifications))
	}
	if led.Notifications[1].SourceID != "agent:w1:p2:4:blocked" {
		t.Fatalf("second SourceID = %s, want agent:w1:p2:4:blocked", led.Notifications[1].SourceID)
	}
	if led.Notifications[0].ID == led.Notifications[1].ID {
		t.Fatal("two blocked transitions share one notification id")
	}
	// the transitions carry the previous status.
	wantFrom := []string{"", "working", "blocked", "working"}
	if len(trs) != 4 {
		t.Fatalf("transitions = %d, want 4", len(trs))
	}
	for i, wf := range wantFrom {
		if trs[i].From != wf {
			t.Fatalf("transition %d From = %q, want %q", i, trs[i].From, wf)
		}
		if trs[i].N != int64(i+1) {
			t.Fatalf("transition %d N = %d, want %d", i, trs[i].N, i+1)
		}
		if trs[i].Watch.Pane != "w1:p2" {
			t.Fatalf("transition %d Watch = %+v, want the pane watch", i, trs[i].Watch)
		}
	}
	// an unwatched pane is ignored without any write.
	before, err := os.ReadFile(filepath.Join(s.Dir(), "ledger.json"))
	if err != nil {
		t.Fatalf("read ledger: %v", err)
	}
	if sum := ingest("w9:p9", "blocked"); !sum.Ignored {
		t.Fatalf("unwatched pane = %+v, want ignored", sum)
	}
	after, err := os.ReadFile(filepath.Join(s.Dir(), "ledger.json"))
	if err != nil {
		t.Fatalf("read ledger: %v", err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("unwatched pane changed the ledger file")
	}
	led, err = s.LoadLedger()
	if err != nil {
		t.Fatalf("LoadLedger: %v", err)
	}
	if _, ok := led.Panes["w9:p9"]; ok {
		t.Fatal("unwatched pane got a pane state")
	}
}

// TestLedgerReordering: outbox records are projected in outbox order,
// whatever the event seq order.
func TestLedgerReordering(t *testing.T) {
	ob := openOutbox(t, t.TempDir())
	clk := newLedgerTestClock()
	s := Open(ob, clk.Now)
	enableNotify(t, s)
	// store event 2 before event 1 (a gap fill).
	appendJobEvent(t, ob, "job-1", 2, "failure")
	appendJobEvent(t, ob, "job-1", 1, "blocked")
	var order []int64
	proj := func(in JobEventInput, reg Registry, now time.Time) (*Notification, bool, error) {
		var ev struct {
			Seq int64 `json:"seq"`
		}
		_ = json.Unmarshal(in.Event, &ev)
		order = append(order, ev.Seq)
		return fakeJobNotification(in, now), true, nil
	}
	sum, err := s.ProjectOutbox(proj)
	if err != nil || sum.Created != 2 {
		t.Fatalf("ProjectOutbox = %+v, %v; want Created 2", sum, err)
	}
	if len(order) != 2 || order[0] != 2 || order[1] != 1 {
		t.Fatalf("projection order = %v, want [2 1] (outbox order)", order)
	}
	led, err := s.LoadLedger()
	if err != nil {
		t.Fatalf("LoadLedger: %v", err)
	}
	if len(led.Notifications) != 2 {
		t.Fatalf("notifications = %d, want 2", len(led.Notifications))
	}
	if led.Notifications[0].EventSeq != 2 || led.Notifications[1].EventSeq != 1 {
		t.Fatalf("ledger order = [%d %d], want [2 1]", led.Notifications[0].EventSeq, led.Notifications[1].EventSeq)
	}
}

// TestLedgerMalformedProject: a project error counts Malformed, the
// cursor still advances and the ledger is written.
func TestLedgerMalformedProject(t *testing.T) {
	ob := openOutbox(t, t.TempDir())
	clk := newLedgerTestClock()
	s := Open(ob, clk.Now)
	enableNotify(t, s)
	appendJobEvent(t, ob, "job-1", 1, "blocked")
	proj := func(in JobEventInput, reg Registry, now time.Time) (*Notification, bool, error) {
		return nil, false, errors.New("boom")
	}
	sum, err := s.ProjectOutbox(proj)
	if err != nil {
		t.Fatalf("ProjectOutbox error = %v, want a Malformed count instead", err)
	}
	if sum.Malformed != 1 || sum.Created != 0 || sum.ProjectedSeq != 1 {
		t.Fatalf("summary = %+v, want Malformed 1, ProjectedSeq 1", sum)
	}
	led, err := s.LoadLedger()
	if err != nil {
		t.Fatalf("LoadLedger: %v", err)
	}
	if len(led.Notifications) != 0 {
		t.Fatalf("notifications = %d, want 0", len(led.Notifications))
	}
	if led.ProjectedSeq != 1 {
		t.Fatalf("cursor = %d, want 1 (the ledger was written)", led.ProjectedSeq)
	}
	// the cursor advanced: the same record is not re-projected.
	sum, err = s.ProjectOutbox(fakeProjectJobEvent)
	if err != nil {
		t.Fatalf("ProjectOutbox: %v", err)
	}
	if sum.Malformed != 0 || sum.Created != 0 {
		t.Fatalf("replay summary = %+v, want zero counts (cursor advanced)", sum)
	}
}

// TestLedgerRaiseReplay: a raise with a known id returns the stored
// notification with created=false; a new id creates one.
func TestLedgerRaiseReplay(t *testing.T) {
	ob := openOutbox(t, t.TempDir())
	clk := newLedgerTestClock()
	s := Open(ob, clk.Now)
	enableNotify(t, s)
	in := RaiseInput{ID: "ra-1", Class: ClassCrossProject, Projeto: testProjetoID}
	n1, created, err := s.Raise(in, fakeProjectRaise)
	if err != nil || !created {
		t.Fatalf("Raise = %+v, %v, %v; want created", n1, created, err)
	}
	clk.Add(time.Minute)
	n2, created, err := s.Raise(in, fakeProjectRaise)
	if err != nil || created {
		t.Fatalf("replay = %+v, created %v, %v; want created=false", n2, created, err)
	}
	if n1.ID != n2.ID || n2.PersistedAt != n1.PersistedAt {
		t.Fatalf("replay returned a different notification (%s %q vs %s %q)", n1.ID, n1.PersistedAt, n2.ID, n2.PersistedAt)
	}
	led, err := s.LoadLedger()
	if err != nil {
		t.Fatalf("LoadLedger: %v", err)
	}
	if len(led.Notifications) != 1 {
		t.Fatalf("notifications = %d, want 1", len(led.Notifications))
	}
	n3, created, err := s.Raise(RaiseInput{ID: "ra-2", Class: ClassStuck, JobID: "job-1"}, fakeProjectRaise)
	if err != nil || !created {
		t.Fatalf("second raise = %+v, %v, %v; want created", n3, created, err)
	}
	if n3.ID == n1.ID {
		t.Fatal("a different raise id mapped to the same notification")
	}
}

// TestLedgerAck: Ack sets AckAt once on an accepted or uncertain
// delivery; a second Ack keeps the first timestamp; unknown ids, missing
// roles and non-final states are not found.
func TestLedgerAck(t *testing.T) {
	ob := openOutbox(t, t.TempDir())
	clk := newLedgerTestClock()
	s := Open(ob, clk.Now)
	enableNotify(t, s)
	n, created, err := s.Raise(RaiseInput{ID: "ra-1", Class: ClassStuck, Projeto: testProjetoID}, fakeProjectRaise)
	if err != nil || !created {
		t.Fatalf("Raise = %+v, %v, %v; want created", n, created, err)
	}
	// a pending delivery is not ackable.
	if found, err := s.Ack(n.ID, RoleOwner); found || err != nil {
		t.Fatalf("Ack on pending delivery = %v, %v; want found=false", found, err)
	}
	claims, err := s.ClaimDue(0)
	if err != nil || len(claims) != 1 {
		t.Fatalf("ClaimDue = %+v, %v; want one claim", claims, err)
	}
	if err := s.Complete(claims[0], SendResult{Outcome: OutcomeAccepted, Status: "sent", Exit: 0}); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	found, err := s.Ack(n.ID, RoleOwner)
	if err != nil || !found {
		t.Fatalf("Ack = %v, %v; want found", found, err)
	}
	led, err := s.LoadLedger()
	if err != nil {
		t.Fatalf("LoadLedger: %v", err)
	}
	first := led.Notifications[0].Deliveries[0].AckAt
	if first == "" {
		t.Fatal("AckAt not set")
	}
	// a second Ack keeps the first timestamp.
	clk.Add(time.Hour)
	found, err = s.Ack(n.ID, RoleOwner)
	if err != nil || !found {
		t.Fatalf("second Ack = %v, %v; want found", found, err)
	}
	led, err = s.LoadLedger()
	if err != nil {
		t.Fatalf("LoadLedger: %v", err)
	}
	if got := led.Notifications[0].Deliveries[0].AckAt; got != first {
		t.Fatalf("AckAt = %q after the second Ack, want %q (the first is kept)", got, first)
	}
	// a role the delivery does not hold is not found.
	if found, err := s.Ack(n.ID, RoleOrchestrator); found || err != nil {
		t.Fatalf("Ack with a missing role = %v, %v; want found=false", found, err)
	}
	// an unknown id is not found.
	if found, err := s.Ack("n-missing", RoleOwner); found || err != nil {
		t.Fatalf("Ack unknown id = %v, %v; want found=false", found, err)
	}
}

// TestLedgerPruning: a final notification older than the retention
// window is dropped before the write; an old notification with a
// pending delivery is kept.
func TestLedgerPruning(t *testing.T) {
	ob := openOutbox(t, t.TempDir())
	clk := newLedgerTestClock()
	s := Open(ob, clk.Now)
	enableNotify(t, s)
	appendJobEvent(t, ob, "job-1", 1, "blocked")
	appendJobEvent(t, ob, "job-1", 2, "blocked")
	if _, err := s.ProjectOutbox(fakeProjectJobEvent); err != nil {
		t.Fatalf("ProjectOutbox: %v", err)
	}
	led, err := s.LoadLedger()
	if err != nil {
		t.Fatalf("LoadLedger: %v", err)
	}
	old := led.Notifications[0]
	old.PersistedAt = clk.Now().Add(-31 * 24 * time.Hour).Format(TSLayout)
	old.Deliveries[0].State = StateAccepted
	led.Notifications[0] = old
	pending := led.Notifications[1]
	pending.PersistedAt = clk.Now().Add(-31 * 24 * time.Hour).Format(TSLayout)
	led.Notifications[1] = pending
	overwriteLedger(t, s, led)
	appendJobEvent(t, ob, "job-1", 3, "blocked")
	if _, err := s.ProjectOutbox(fakeProjectJobEvent); err != nil {
		t.Fatalf("ProjectOutbox: %v", err)
	}
	led, err = s.LoadLedger()
	if err != nil {
		t.Fatalf("LoadLedger: %v", err)
	}
	if len(led.Notifications) != 2 {
		t.Fatalf("notifications = %d, want 2 (the old final one was pruned)", len(led.Notifications))
	}
	if led.Notifications[0].EventSeq != 2 || led.Notifications[1].EventSeq != 3 {
		t.Fatalf("ledger order = [%d %d], want [2 3] (the old pending one survived)",
			led.Notifications[0].EventSeq, led.Notifications[1].EventSeq)
	}
}

// TestLedgerWorkspaceRegistered: a registered workspace notifies whatever
// its label, routed by the registration; the label itself is never stored
// in the ledger, and a canary label without a registration resolves
// nothing and writes nothing.
func TestLedgerWorkspaceRegistered(t *testing.T) {
	ob := openOutbox(t, t.TempDir())
	clk := newLedgerTestClock()
	s := Open(ob, clk.Now)
	enableNotify(t, s)
	if err := s.UpdateRegistry(func(reg *Registry) error {
		reg.WorkspaceWatches["w2"] = WorkspaceWatch{Workspace: "w2", Job: "job-1", Projeto: testProjetoID}
		return nil
	}); err != nil {
		t.Fatalf("UpdateRegistry: %v", err)
	}
	// a label without a job- prefix: labelJob is empty, the registration
	// resolves the source.
	sum, err := s.IngestWorkspace(WorkspaceEvent{Event: WorkspaceCreated, Workspace: "w2", Label: "some-label"}, "", "", ProjectWorkspace)
	if err != nil || !sum.Created || sum.Duplicate || sum.Ignored {
		t.Fatalf("created = %+v, %v; want created", sum, err)
	}
	if sum.N != 1 || sum.JobID != "job-1" || sum.NotificationID == "" {
		t.Fatalf("summary = %+v, want N=1 job-1 with NotificationID", sum)
	}
	led, err := s.LoadLedger()
	if err != nil {
		t.Fatalf("LoadLedger: %v", err)
	}
	if len(led.Notifications) != 1 {
		t.Fatalf("notifications = %d, want 1", len(led.Notifications))
	}
	n := led.Notifications[0]
	if n.SourceID != "workspace:w2:1:opened" || n.Class != ClassWorkspaceOpened || n.Workspace != "w2" || n.JobID != "job-1" || n.Projeto != testProjetoID {
		t.Fatalf("notification = %+v, want opened fields from the registration", n)
	}
	if n.Deliveries[0].Ref != testOwnerRef || n.Deliveries[0].State != StatePending || n.Deliveries[0].Roles[0] != RoleOwner {
		t.Fatalf("owner delivery = %+v, want pending %s", n.Deliveries[0], testOwnerRef)
	}
	if n.Deliveries[1].State != StateNoRoute || n.Deliveries[1].Roles[0] != RoleOrchestrator {
		t.Fatalf("orchestrator delivery = %+v, want no_route", n.Deliveries[1])
	}
	// the label itself is never stored in the ledger file.
	raw, err := os.ReadFile(filepath.Join(s.Dir(), "ledger.json"))
	if err != nil {
		t.Fatalf("read ledger: %v", err)
	}
	if strings.Contains(string(raw), "some-label") {
		t.Fatalf("the label leaked into the ledger file: %s", raw)
	}
	// a canary label without a registration resolves nothing: ignored,
	// no write, so nothing stored and nothing to render.
	before, err := os.ReadFile(filepath.Join(s.Dir(), "ledger.json"))
	if err != nil {
		t.Fatalf("read ledger: %v", err)
	}
	sum, err = s.IngestWorkspace(WorkspaceEvent{Event: WorkspaceCreated, Workspace: "w9", Label: "CANARY-SECRET-1"}, "", "", ProjectWorkspace)
	if err != nil || !sum.Ignored {
		t.Fatalf("canary label = %+v, %v; want ignored", sum, err)
	}
	after, err := os.ReadFile(filepath.Join(s.Dir(), "ledger.json"))
	if err != nil {
		t.Fatalf("read ledger: %v", err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("an ignored canary-label event changed the ledger file")
	}
}

// TestLedgerWorkspaceRegistrationWins: an explicit registration wins over
// a job-<id> label.
func TestLedgerWorkspaceRegistrationWins(t *testing.T) {
	ob := openOutbox(t, t.TempDir())
	clk := newLedgerTestClock()
	s := Open(ob, clk.Now)
	enableNotify(t, s)
	if err := s.UpdateRegistry(func(reg *Registry) error {
		reg.WorkspaceWatches["w2"] = WorkspaceWatch{Workspace: "w2", Job: "job-2", Projeto: testProjetoID}
		return nil
	}); err != nil {
		t.Fatalf("UpdateRegistry: %v", err)
	}
	// the label says job-1, the registration says job-2: the
	// registration wins.
	sum, err := s.IngestWorkspace(WorkspaceEvent{Event: WorkspaceCreated, Workspace: "w2", Label: "job-1"}, "job-1", testProjetoID, ProjectWorkspace)
	if err != nil || !sum.Created {
		t.Fatalf("created = %+v, %v; want created", sum, err)
	}
	if sum.JobID != "job-2" {
		t.Fatalf("JobID = %q, want job-2 (registration wins over the job label)", sum.JobID)
	}
	led, err := s.LoadLedger()
	if err != nil {
		t.Fatalf("LoadLedger: %v", err)
	}
	if got := led.Workspaces["w2"]; got == nil || got.Job != "job-2" {
		t.Fatalf("stored state = %+v, want job-2", got)
	}
}

// TestLedgerWorkspaceJobLabel: a job-<id> label without a registration
// resolves the job; a later closed event without a label resolves it from
// the stored state (job and projeto).
func TestLedgerWorkspaceJobLabel(t *testing.T) {
	ob := openOutbox(t, t.TempDir())
	clk := newLedgerTestClock()
	s := Open(ob, clk.Now)
	enableNotify(t, s)
	sum, err := s.IngestWorkspace(WorkspaceEvent{Event: WorkspaceCreated, Workspace: "w2", Label: "job-1"}, "job-1", testProjetoID, ProjectWorkspace)
	if err != nil || !sum.Created || sum.JobID != "job-1" {
		t.Fatalf("created = %+v, %v; want created job-1", sum, err)
	}
	if job, ok := s.WorkspaceJob("w2"); !ok || job != "job-1" {
		t.Fatalf("WorkspaceJob(w2) = (%q, %v), want (job-1, true)", job, ok)
	}
	clk.Add(time.Minute)
	// a closed event without a label: resolved from the stored state.
	sum, err = s.IngestWorkspace(WorkspaceEvent{Event: WorkspaceClosed, Workspace: "w2"}, "", "", ProjectWorkspace)
	if err != nil || !sum.Created || sum.N != 2 || sum.JobID != "job-1" {
		t.Fatalf("closed = %+v, %v; want created N=2 job-1 from the stored state", sum, err)
	}
	led, err := s.LoadLedger()
	if err != nil {
		t.Fatalf("LoadLedger: %v", err)
	}
	if len(led.Notifications) != 2 {
		t.Fatalf("notifications = %d, want 2", len(led.Notifications))
	}
	if led.Notifications[1].SourceID != "workspace:w2:2:closed" || led.Notifications[1].Class != ClassWorkspaceClosed {
		t.Fatalf("closed notification = %+v, want workspace:w2:2:closed", led.Notifications[1])
	}
	// the stored projeto routes the owner of the closed notification.
	if led.Notifications[1].Projeto != testProjetoID {
		t.Fatalf("closed projeto = %q, want %q (from the stored state)", led.Notifications[1].Projeto, testProjetoID)
	}
}

// TestLedgerWorkspaceUnknownIgnored: a closed event of an unknown
// workspace without a label (and a created without a label) is ignored
// without any write.
func TestLedgerWorkspaceUnknownIgnored(t *testing.T) {
	ob := openOutbox(t, t.TempDir())
	clk := newLedgerTestClock()
	s := Open(ob, clk.Now)
	enableNotify(t, s)
	before, err := os.ReadFile(filepath.Join(s.Dir(), "ledger.json"))
	if err != nil {
		t.Fatalf("read ledger: %v", err)
	}
	sum, err := s.IngestWorkspace(WorkspaceEvent{Event: WorkspaceClosed, Workspace: "w9"}, "", "", ProjectWorkspace)
	if err != nil || !sum.Ignored {
		t.Fatalf("unknown closed = %+v, %v; want ignored", sum, err)
	}
	sum, err = s.IngestWorkspace(WorkspaceEvent{Event: WorkspaceCreated, Workspace: "w9", Label: "no-job-label"}, "", "", ProjectWorkspace)
	if err != nil || !sum.Ignored {
		t.Fatalf("unknown created without a job label = %+v, %v; want ignored", sum, err)
	}
	after, err := os.ReadFile(filepath.Join(s.Dir(), "ledger.json"))
	if err != nil {
		t.Fatalf("read ledger: %v", err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("ignored events changed the ledger file")
	}
	if job, ok := s.WorkspaceJob("w9"); ok || job != "" {
		t.Fatalf("WorkspaceJob(w9) = (%q, %v), want (\"\", false)", job, ok)
	}
}

// TestLedgerWorkspaceReopenSequence: created, a replayed created
// (duplicate), closed, created (reopen) and closed give four distinct
// notifications (N 1..4) and one stored state.
func TestLedgerWorkspaceReopenSequence(t *testing.T) {
	ob := openOutbox(t, t.TempDir())
	clk := newLedgerTestClock()
	s := Open(ob, clk.Now)
	enableNotify(t, s)
	if err := s.UpdateRegistry(func(reg *Registry) error {
		reg.WorkspaceWatches["w2"] = WorkspaceWatch{Workspace: "w2", Job: "job-1", Projeto: testProjetoID}
		return nil
	}); err != nil {
		t.Fatalf("UpdateRegistry: %v", err)
	}
	ingest := func(event string) WorkspaceSummary {
		t.Helper()
		sum, err := s.IngestWorkspace(WorkspaceEvent{Event: event, Workspace: "w2", Label: "job-1"}, "job-1", testProjetoID, ProjectWorkspace)
		if err != nil {
			t.Fatalf("IngestWorkspace %s: %v", event, err)
		}
		return sum
	}
	// created N=1.
	if sum := ingest(WorkspaceCreated); !sum.Created || sum.N != 1 {
		t.Fatalf("created = %+v, want created N=1", sum)
	}
	// a replayed created right after created is a duplicate.
	clk.Add(time.Minute)
	if sum := ingest(WorkspaceCreated); !sum.Duplicate || sum.Created || sum.N != 1 {
		t.Fatalf("replayed created = %+v, want duplicate N=1", sum)
	}
	// closed N=2.
	clk.Add(time.Minute)
	if sum := ingest(WorkspaceClosed); !sum.Created || sum.N != 2 {
		t.Fatalf("closed = %+v, want created N=2", sum)
	}
	// created (a reopen) N=3.
	clk.Add(time.Minute)
	if sum := ingest(WorkspaceCreated); !sum.Created || sum.N != 3 {
		t.Fatalf("reopen = %+v, want created N=3", sum)
	}
	// closed N=4.
	clk.Add(time.Minute)
	if sum := ingest(WorkspaceClosed); !sum.Created || sum.N != 4 {
		t.Fatalf("closed again = %+v, want created N=4", sum)
	}
	led, err := s.LoadLedger()
	if err != nil {
		t.Fatalf("LoadLedger: %v", err)
	}
	if len(led.Notifications) != 4 {
		t.Fatalf("notifications = %d, want 4", len(led.Notifications))
	}
	wantIDs := []string{"n103ac05f9abaf550d1e8", "n70ea9d27cfc2e358202b", "nc790d9e182fa54f51948", "n8290185abe4d72ef2f11"}
	wantSources := []string{"workspace:w2:1:opened", "workspace:w2:2:closed", "workspace:w2:3:opened", "workspace:w2:4:closed"}
	seen := map[string]bool{}
	for i, n := range led.Notifications {
		if n.ID != wantIDs[i] || n.SourceID != wantSources[i] {
			t.Fatalf("notification %d = %s/%s, want %s/%s", i+1, n.ID, n.SourceID, wantIDs[i], wantSources[i])
		}
		if seen[n.ID] {
			t.Fatalf("duplicate notification id in the ledger: %s", n.ID)
		}
		seen[n.ID] = true
	}
	st := led.Workspaces["w2"]
	if st == nil || st.N != 4 || st.Event != WorkspaceClosed || st.Job != "job-1" || st.Projeto != testProjetoID {
		t.Fatalf("workspace state = %+v, want N=4 closed job-1/%s", st, testProjetoID)
	}
	if st.LastObservedAt == "" {
		t.Fatal("workspace state has no LastObservedAt")
	}
	// WorkspaceJob is a read-only lookup of the stored job.
	if job, ok := s.WorkspaceJob("w2"); !ok || job != "job-1" {
		t.Fatalf("WorkspaceJob(w2) = (%q, %v), want (job-1, true)", job, ok)
	}
}

// TestLedgerWorkspacesState: loading a ledger without the workspaces key
// (an older file) gives a non-nil empty map; a written state round-trips
// and the key is present in the stored file.
func TestLedgerWorkspacesState(t *testing.T) {
	ob := openOutbox(t, t.TempDir())
	clk := newLedgerTestClock()
	s := Open(ob, clk.Now)
	enableNotify(t, s)
	led, err := s.LoadLedger()
	if err != nil {
		t.Fatalf("LoadLedger: %v", err)
	}
	if led.Workspaces == nil {
		t.Fatal("Workspaces is nil on a fresh ledger")
	}
	// an older ledger file without the key loads with a non-nil map.
	led.Workspaces = nil
	overwriteLedger(t, s, led)
	led, err = s.LoadLedger()
	if err != nil {
		t.Fatalf("LoadLedger: %v", err)
	}
	if led.Workspaces == nil {
		t.Fatal("Workspaces is nil on a ledger file without the key")
	}
	if len(led.Workspaces) != 0 {
		t.Fatalf("Workspaces = %v, want empty", led.Workspaces)
	}
	// after a transition the state is persisted.
	if sum, err := s.IngestWorkspace(WorkspaceEvent{Event: WorkspaceCreated, Workspace: "w2"}, "job-1", testProjetoID, ProjectWorkspace); err != nil || !sum.Created {
		t.Fatalf("IngestWorkspace = %+v, %v; want created", sum, err)
	}
	raw, err := os.ReadFile(filepath.Join(s.Dir(), "ledger.json"))
	if err != nil {
		t.Fatalf("read ledger: %v", err)
	}
	if !strings.Contains(string(raw), `"workspaces"`) {
		t.Fatal("ledger file lacks the workspaces key")
	}
	led, err = s.LoadLedger()
	if err != nil {
		t.Fatalf("LoadLedger: %v", err)
	}
	st := led.Workspaces["w2"]
	if st == nil || st.Event != WorkspaceCreated || st.N != 1 || st.Job != "job-1" || st.Projeto != testProjetoID {
		t.Fatalf("workspace state = %+v, want created N=1 job-1/%s", st, testProjetoID)
	}
	// a second store on the same outbox reads the same state.
	b := Open(ob, clk.Now)
	if job, ok := b.WorkspaceJob("w2"); !ok || job != "job-1" {
		t.Fatalf("WorkspaceJob on a second store = (%q, %v), want (job-1, true)", job, ok)
	}
}

// TestLedgerAgentDoneSequence: done -> working -> done gives two distinct
// done notifications (the per-pane N differs).
func TestLedgerAgentDoneSequence(t *testing.T) {
	ob := openOutbox(t, t.TempDir())
	clk := newLedgerTestClock()
	s := Open(ob, clk.Now)
	err := s.UpdateRegistry(func(reg *Registry) error {
		reg.Owners[testProjetoID] = testOwnerRef
		reg.Watches["w1:p2"] = Watch{Pane: "w1:p2", Job: "job-1", Projeto: testProjetoID}
		return nil
	})
	if err != nil {
		t.Fatalf("UpdateRegistry: %v", err)
	}
	ingest := func(status string) IngestSummary {
		t.Helper()
		sum, err := s.IngestAgentStatus(AgentStatusEvent{Pane: "w1:p2", Workspace: "w1", Status: status}, ProjectAgentStatus)
		if err != nil {
			t.Fatalf("IngestAgentStatus %s: %v", status, err)
		}
		return sum
	}
	// working: routine, N=1.
	if sum := ingest("working"); !sum.Routine || sum.Created || sum.N != 1 {
		t.Fatalf("working = %+v, want routine N=1", sum)
	}
	// done: a true transition with a notification, N=2.
	sum := ingest("done")
	if !sum.Created || sum.Duplicate || sum.Routine || sum.N != 2 || sum.NotificationID == "" {
		t.Fatalf("done = %+v, want created N=2 with NotificationID", sum)
	}
	led, err := s.LoadLedger()
	if err != nil {
		t.Fatalf("LoadLedger: %v", err)
	}
	if len(led.Notifications) != 1 {
		t.Fatalf("notifications = %d, want 1", len(led.Notifications))
	}
	first := led.Notifications[0]
	if first.SourceID != "agent:w1:p2:2:done" || first.Class != ClassAgentDone {
		t.Fatalf("notification = %+v, want agent:w1:p2:2:done agent_done", first)
	}
	// working again: routine, N=3.
	if sum := ingest("working"); !sum.Routine || sum.Created || sum.N != 3 {
		t.Fatalf("working again = %+v, want routine N=3", sum)
	}
	// done again: a new N and a distinct notification.
	sum = ingest("done")
	if !sum.Created || sum.N != 4 {
		t.Fatalf("done again = %+v, want created N=4", sum)
	}
	led, err = s.LoadLedger()
	if err != nil {
		t.Fatalf("LoadLedger: %v", err)
	}
	if len(led.Notifications) != 2 {
		t.Fatalf("notifications = %d, want 2", len(led.Notifications))
	}
	second := led.Notifications[1]
	if second.SourceID != "agent:w1:p2:4:done" || second.ID != "n543f72dfdf8fa0f53703" {
		t.Fatalf("second notification = %+v, want agent:w1:p2:4:done", second)
	}
	if first.ID == second.ID {
		t.Fatal("two done transitions share one notification id")
	}
}

// TestLedgerFileModes: the notify dir is 0700 and its files are 0600
// (POSIX only).
func TestLedgerFileModes(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mode asserts are POSIX-only")
	}
	ob := openOutbox(t, t.TempDir())
	clk := newLedgerTestClock()
	s := Open(ob, clk.Now)
	enableNotify(t, s)
	appendJobEvent(t, ob, "job-1", 1, "blocked")
	if _, err := s.ProjectOutbox(fakeProjectJobEvent); err != nil {
		t.Fatalf("ProjectOutbox: %v", err)
	}
	mode := func(p string) os.FileMode {
		t.Helper()
		fi, err := os.Stat(p)
		if err != nil {
			t.Fatalf("stat %s: %v", p, err)
		}
		return fi.Mode().Perm()
	}
	if got := mode(s.Dir()); got != 0o700 {
		t.Fatalf("notify dir mode = %o, want 700", got)
	}
	for _, f := range []string{"registry.json", "ledger.json"} {
		if got := mode(filepath.Join(s.Dir(), f)); got != 0o600 {
			t.Fatalf("%s mode = %o, want 600", f, got)
		}
	}
}
