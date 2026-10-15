package notify

// ExpeditePending and the read-only PendingRefs: a recipient pane that
// came back pulls the next attempt of its pending deliveries (matching
// ref, future deadline) to the store's clock; everything else — the
// other states, the attempts, the unmatched refs and the already-due
// deadlines — stays untouched, and without a change nothing is written
// (not even the ledger file).

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// seedExpediteLedger overwrites the ledger with one notification whose
// nine deliveries cover every state and deadline class of the
// ExpeditePending contract (all with non-zero Attempts so an untouched
// delivery is distinguishable from a reset).
func seedExpediteLedger(t *testing.T, s *Store) {
	t.Helper()
	now := s.now()
	led := &Ledger{
		Schema:       LedgerSchema,
		ProjectedSeq: 1,
		Notifications: []*Notification{{
			ID:          fakeNotificationID("job:job-1:1"),
			SourceKind:  SourceJobEvent,
			SourceID:    "job:job-1:1",
			Class:       ClassBlocked,
			JobID:       "job-1",
			Projeto:     testProjetoID,
			ObservedAt:  now.Format(TSLayout),
			PersistedAt: now.Format(TSLayout),
			Deliveries: []Delivery{
				// matching ref, future deadline: the only expedited one
				{Roles: []string{RoleOwner}, Ref: "local/w1:p2", State: StatePending, Attempts: 1,
					NextAttemptAt: now.Add(30 * time.Second).Format(TSLayout)},
				// non-matching ref, future deadline: untouched
				{Roles: []string{RoleOrchestrator}, Ref: "local/w1:p9", State: StatePending, Attempts: 1,
					NextAttemptAt: now.Add(30 * time.Second).Format(TSLayout)},
				// matching ref, past deadline: already due, untouched
				{Roles: []string{RoleCoordinator}, Ref: "local/w1:p2", State: StatePending, Attempts: 2,
					NextAttemptAt: now.Add(-5 * time.Second).Format(TSLayout)},
				// matching ref, no deadline: due now, untouched
				{Roles: []string{RoleOwner}, Ref: "local/w1:p2", State: StatePending, Attempts: 3},
				// in flight with a live lease (and a stale future deadline
				// left over from an earlier claim): untouched
				{Roles: []string{RoleOrchestrator}, Ref: "local/w1:p2", State: StateInFlight, Attempts: 1,
					NextAttemptAt:  now.Add(45 * time.Second).Format(TSLayout),
					LeaseUntil:     now.Add(LeaseDuration).Format(TSLayout),
					FirstAttemptAt: now.Format(TSLayout), LastAttemptAt: now.Format(TSLayout)},
				// the final states: untouched
				{Roles: []string{RoleCoordinator}, Ref: "local/w1:p2", State: StateAccepted, Attempts: 1,
					AcceptedAt: now.Format(TSLayout)},
				{Roles: []string{RoleOwner}, Ref: "local/w1:p2", State: StateUncertain, Attempts: 1,
					LastError: "uncertain"},
				{Roles: []string{RoleOrchestrator}, Ref: "local/w1:p2", State: StateRejected, Attempts: 1,
					LastError: "refused"},
				{Roles: []string{RoleCoordinator}, Ref: "local/w1:p2", State: StateExhausted, Attempts: MaxAttempts,
					LastError: "exit"},
			},
		}},
	}
	overwriteLedger(t, s, led)
}

// TestExpeditePending: only the pending delivery with the matching ref
// and the future deadline is pulled to now; the attempts, the other
// states and the unmatched or already-due deadlines stay untouched;
// without a match nothing is written (mtime and content equal).
func TestExpeditePending(t *testing.T) {
	ob := openOutbox(t, t.TempDir())
	clk := newLedgerTestClock()
	s := Open(ob, clk.Now)
	enableNotify(t, s)

	t.Run("only the matching future pending delivery", func(t *testing.T) {
		seedExpediteLedger(t, s)
		before := clk.Now()
		n, err := s.ExpeditePending(func(ref string) bool { return ref == "local/w1:p2" })
		if err != nil {
			t.Fatalf("ExpeditePending: %v", err)
		}
		if n != 1 {
			t.Fatalf("expedited = %d, want 1 (only the matching future pending)", n)
		}
		led, err := s.LoadLedger()
		if err != nil {
			t.Fatalf("LoadLedger: %v", err)
		}
		if len(led.Notifications) != 1 || len(led.Notifications[0].Deliveries) != 9 {
			t.Fatalf("ledger = %+v, want the nine seeded deliveries", led.Notifications)
		}
		ds := led.Notifications[0].Deliveries
		nowTS := before.Format(TSLayout)
		if ds[0].NextAttemptAt != nowTS {
			t.Fatalf("ds[0] NextAttemptAt = %q, want %q (pulled to now)", ds[0].NextAttemptAt, nowTS)
		}
		if ds[0].State != StatePending || ds[0].Attempts != 1 {
			t.Fatalf("ds[0] state/attempts = %s/%d, want pending/1 (state and attempts untouched)", ds[0].State, ds[0].Attempts)
		}
		if ds[1].Ref != "local/w1:p9" || ds[1].NextAttemptAt != before.Add(30*time.Second).Format(TSLayout) || ds[1].Attempts != 1 {
			t.Fatalf("ds[1] = %+v, want the unmatched pending untouched", ds[1])
		}
		if ds[2].NextAttemptAt != before.Add(-5*time.Second).Format(TSLayout) || ds[2].Attempts != 2 {
			t.Fatalf("ds[2] = %+v, want the past-deadline pending untouched", ds[2])
		}
		if ds[3].NextAttemptAt != "" || ds[3].Attempts != 3 {
			t.Fatalf("ds[3] = %+v, want the due-now pending untouched", ds[3])
		}
		if ds[4].State != StateInFlight || ds[4].LeaseUntil != before.Add(LeaseDuration).Format(TSLayout) ||
			ds[4].NextAttemptAt != before.Add(45*time.Second).Format(TSLayout) || ds[4].Attempts != 1 {
			t.Fatalf("ds[4] = %+v, want the in-flight lease untouched", ds[4])
		}
		wantStates := []string{StateAccepted, StateUncertain, StateRejected, StateExhausted}
		for i, ws := range wantStates {
			if ds[5+i].State != ws {
				t.Fatalf("ds[%d] state = %s, want %s untouched", 5+i, ds[5+i].State, ws)
			}
		}
		if ds[5].AcceptedAt != nowTS || ds[6].LastError != "uncertain" || ds[7].LastError != "refused" || ds[8].LastError != "exit" {
			t.Fatalf("final deliveries = %+v %+v %+v %+v, want untouched", ds[5], ds[6], ds[7], ds[8])
		}
	})

	t.Run("every matching future pending delivery", func(t *testing.T) {
		led := &Ledger{
			Schema: LedgerSchema,
			Notifications: []*Notification{{
				ID:          fakeNotificationID("job:job-1:2"),
				SourceKind:  SourceJobEvent,
				SourceID:    "job:job-1:2",
				Class:       ClassBlocked,
				JobID:       "job-1",
				Projeto:     testProjetoID,
				ObservedAt:  clk.TS(),
				PersistedAt: clk.TS(),
				Deliveries: []Delivery{
					{Roles: []string{RoleOwner}, Ref: "local/w1:p2", State: StatePending, Attempts: 1,
						NextAttemptAt: clk.Now().Add(RetrySchedule[1]).Format(TSLayout)},
					{Roles: []string{RoleCoordinator}, Ref: "local/w1:p2", State: StatePending, Attempts: 2,
						NextAttemptAt: clk.Now().Add(RetrySchedule[3]).Format(TSLayout)},
					{Roles: []string{RoleOrchestrator}, Ref: "local/w1:p9", State: StatePending, Attempts: 1,
						NextAttemptAt: clk.Now().Add(RetrySchedule[0]).Format(TSLayout)},
				},
			}},
		}
		overwriteLedger(t, s, led)
		before := clk.Now()
		n, err := s.ExpeditePending(func(ref string) bool { return ref == "local/w1:p2" })
		if err != nil {
			t.Fatalf("ExpeditePending: %v", err)
		}
		if n != 2 {
			t.Fatalf("expedited = %d, want 2 (both matching future pending)", n)
		}
		led2, err := s.LoadLedger()
		if err != nil {
			t.Fatalf("LoadLedger: %v", err)
		}
		ds := led2.Notifications[0].Deliveries
		nowTS := before.Format(TSLayout)
		if ds[0].NextAttemptAt != nowTS || ds[1].NextAttemptAt != nowTS {
			t.Fatalf("matching deadlines = %q %q, want both %q", ds[0].NextAttemptAt, ds[1].NextAttemptAt, nowTS)
		}
		if ds[0].Attempts != 1 || ds[1].Attempts != 2 {
			t.Fatalf("attempts = %d %d, want 1 2 (attempts untouched)", ds[0].Attempts, ds[1].Attempts)
		}
		if ds[2].NextAttemptAt != before.Add(RetrySchedule[0]).Format(TSLayout) {
			t.Fatalf("ds[2] NextAttemptAt = %q, want the unmatched deadline untouched", ds[2].NextAttemptAt)
		}
	})

	t.Run("no match writes nothing", func(t *testing.T) {
		seedExpediteLedger(t, s)
		path := filepath.Join(s.Dir(), "ledger.json")
		before, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read ledger: %v", err)
		}
		beforeInfo, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat ledger: %v", err)
		}
		n, err := s.ExpeditePending(func(ref string) bool { return ref == "local/w9:p9" })
		if err != nil {
			t.Fatalf("ExpeditePending: %v", err)
		}
		if n != 0 {
			t.Fatalf("expedited = %d, want 0 (no match)", n)
		}
		after, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read ledger: %v", err)
		}
		afterInfo, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat ledger: %v", err)
		}
		if !bytes.Equal(before, after) {
			t.Fatal("no match rewrote the ledger file content")
		}
		if !afterInfo.ModTime().Equal(beforeInfo.ModTime()) {
			t.Fatal("no match rewrote the ledger file (mtime changed)")
		}
	})

	t.Run("an absent ledger stays absent without a change", func(t *testing.T) {
		ob3 := openOutbox(t, t.TempDir())
		s3 := Open(ob3, clk.Now)
		enableNotify(t, s3)
		path := filepath.Join(s3.Dir(), "ledger.json")
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			t.Fatalf("remove ledger: %v", err)
		}
		n, err := s3.ExpeditePending(func(ref string) bool { return false })
		if err != nil {
			t.Fatalf("ExpeditePending: %v", err)
		}
		if n != 0 {
			t.Fatalf("expedited = %d, want 0", n)
		}
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("ExpeditePending created %s without a change (err %v)", path, err)
		}
	})

	t.Run("disabled store expedites nothing", func(t *testing.T) {
		ob2 := openOutbox(t, t.TempDir())
		s2 := Open(ob2, clk.Now)
		n, err := s2.ExpeditePending(func(ref string) bool { return true })
		if err != nil {
			t.Fatalf("ExpeditePending: %v", err)
		}
		if n != 0 {
			t.Fatalf("expedited = %d, want 0 (disabled)", n)
		}
		if _, err := os.Stat(s2.Dir()); !os.IsNotExist(err) {
			t.Fatalf("the disabled store created the notify dir %s (err %v)", s2.Dir(), err)
		}
	})

	t.Run("PendingRefs lists only the pending refs", func(t *testing.T) {
		seedExpediteLedger(t, s)
		refs, err := s.PendingRefs()
		if err != nil {
			t.Fatalf("PendingRefs: %v", err)
		}
		want := []string{"local/w1:p2", "local/w1:p9", "local/w1:p2", "local/w1:p2"}
		if len(refs) != len(want) {
			t.Fatalf("PendingRefs = %v, want %v", refs, want)
		}
		for i := range want {
			if refs[i] != want[i] {
				t.Fatalf("PendingRefs[%d] = %q, want %q", i, refs[i], want[i])
			}
		}
	})
}
