package a2astore

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv/taskstore"

	"github.com/djalmajr/nau/internal/a2acore"
)

// openAt opens the store at path on the given clock and limits and closes
// it when the test ends (a second Close of a handle the test already
// closed is harmless).
func openAt(t *testing.T, path string, clk *clock, limits a2acore.Limits) *Store {
	t.Helper()
	s, err := OpenFile(path, Options{Now: clk.Now, Owner: fixedOwner("alice"), Limits: limits})
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// openCapacityPair opens two Store instances (two connections) on one
// fresh file, both with limit as MaxOpenTasks.
func openCapacityPair(t *testing.T, clk *clock, limit int) (*Store, *Store, string) {
	t.Helper()
	limits := a2acore.Limits{MaxOpenTasks: limit}
	first, path := openStore2(t, clk, "alice", limits)
	return first, openAt(t, path, clk, limits), path
}

// admitNew is one new-task admission of the fixture subject: every task
// has its own context, session and message.
func admitNew(s *Store, n string) (a2acore.AdmitResult, error) {
	return s.Admit(context.Background(), admission("alice", "m-"+n, "t"+n, "cx-"+n, "acme/widgets", "s-"+n, 1, 1, true,
		[]a2acore.PlannedDelivery{ownerDelivery("owner/" + n)}))
}

// createTask is the SDK task create that follows an admission.
func createTask(s *Store, n string, state a2a.TaskState) error {
	_, err := s.Create(context.Background(), mkTask("t"+n, "cx-"+n, state, 1))
	return err
}

// openDurable counts the committed non-terminal tasks.
func openDurable(t *testing.T, s *Store) int {
	t.Helper()
	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM a2a_tasks WHERE state NOT IN (?, ?, ?, ?)`,
		a2acore.StateCompleted, a2acore.StateCanceled, a2acore.StateFailed, a2acore.StateRejected).Scan(&n); err != nil {
		t.Fatalf("count open tasks: %v", err)
	}
	return n
}

// requireAdmitted fails unless the admission passed.
func requireAdmitted(t *testing.T, what string, res a2acore.AdmitResult, err error) {
	t.Helper()
	if err != nil || res.Refusal != nil || res.Duplicate || res.IngressID == 0 {
		t.Fatalf("%s: admission = %+v, %v, want admitted", what, res, err)
	}
}

// requireCapacity fails unless the admission was refused for capacity.
func requireCapacity(t *testing.T, what string, res a2acore.AdmitResult, err error) {
	t.Helper()
	if err != nil || refuseCode(res.Refusal) != a2acore.RefuseCapacity {
		t.Errorf("%s: admission = %+v, %v, want the capacity refusal", what, res, err)
	}
}

// requireCapacityRefusalError fails unless err is the fixed capacity
// refusal the Create backstop returns.
func requireCapacityRefusalError(t *testing.T, what string, err error) {
	t.Helper()
	if err == nil {
		t.Errorf("%s: Create succeeded, want the capacity refusal", what)
		return
	}
	if !errors.Is(err, a2a.ErrInvalidParams) || !strings.Contains(err.Error(), "refused: "+a2acore.RefuseCapacity) {
		t.Errorf("%s: Create error = %v, want invalid params with the capacity refusal text", what, err)
	}
}

// TestCapacityHandoffInterleaving pins the admission-to-create window
// on two connections: Admit(X), Admit(Y), Create(X), Create(Y) with a
// limit of one open task. The reservation of the admitted task X counts
// against Y at its admission, so Y is refused there and the durable
// non-terminal tasks never pass the limit.
func TestCapacityHandoffInterleaving(t *testing.T) {
	clk := &clock{t: testNow}
	first, second, _ := openCapacityPair(t, clk, 1)

	resX, err := admitNew(first, "x")
	requireAdmitted(t, "Admit X", resX, err)
	before := queryCount(t, second.db)
	resY, err := admitNew(second, "y")
	requireCapacity(t, "Admit Y while X is admitted", resY, err)
	after := queryCount(t, second.db)
	for table := range before {
		if before[table] != after[table] {
			t.Errorf("the capacity refusal changed %s: %d -> %d", table, before[table], after[table])
		}
	}
	if err := createTask(first, "x", a2a.TaskStateSubmitted); err != nil {
		t.Fatalf("Create X: %v", err)
	}
	if resY.Refusal == nil {
		// Only a Y that passed its admission goes on to create.
		_ = createTask(second, "y", a2a.TaskStateSubmitted)
	}
	if n := openDurable(t, first); n > 1 {
		t.Errorf("durable non-terminal tasks = %d, want at most the limit of 1", n)
	}
}

// TestCapacityDuplicateNotChargedTwice proves a duplicate at the limit
// is answered before the limit and returns the first task, whether the
// first task is still only admitted or already committed.
func TestCapacityDuplicateNotChargedTwice(t *testing.T) {
	clk := &clock{t: testNow}
	s, _ := openStore2(t, clk, "alice", a2acore.Limits{MaxOpenTasks: 1})
	res, err := admitNew(s, "x")
	requireAdmitted(t, "Admit X", res, err)
	for _, phase := range []string{"admitted", "committed"} {
		dup, err := admitNew(s, "x")
		if err != nil || dup.Refusal != nil || !dup.Duplicate || dup.DuplicateTaskID != "tx" {
			t.Fatalf("duplicate while %s = %+v, %v, want the duplicate of tx", phase, dup, err)
		}
		if phase == "admitted" {
			if err := createTask(s, "x", a2a.TaskStateSubmitted); err != nil {
				t.Fatalf("Create X: %v", err)
			}
		}
	}
	if n := openDurable(t, s); n != 1 {
		t.Fatalf("durable open tasks = %d, want 1", n)
	}
}

// TestCapacityContinuationNotCharged proves a continuation (not a new
// task) at the limit is never charged.
func TestCapacityContinuationNotCharged(t *testing.T) {
	clk := &clock{t: testNow}
	s, _ := openStore2(t, clk, "alice", a2acore.Limits{MaxOpenTasks: 1})
	res, err := admitNew(s, "x")
	requireAdmitted(t, "Admit X", res, err)
	// The task awaits the caller (input_required), so the continuation
	// below passes the awaiting-caller gate.
	if err := createTask(s, "x", a2a.TaskStateInputRequired); err != nil {
		t.Fatalf("Create X: %v", err)
	}
	cont, err := s.Admit(context.Background(), admission("alice", "m-x2", "tx", "cx-x", "acme/widgets", "s-x", 1, 2, false, nil))
	if err != nil || cont.Refusal != nil || cont.Duplicate || cont.IngressID == 0 {
		t.Fatalf("continuation at the limit = %+v, %v, want admitted", cont, err)
	}
}

// TestCapacityContinuationReservesNothing proves the admission of a
// continuation never holds a slot: with the continuation's task
// committed and open (one slot of the limit), the reservation count the
// limit uses stays at zero and a new task is still admitted afterwards.
func TestCapacityContinuationReservesNothing(t *testing.T) {
	clk := &clock{t: testNow}
	s, _ := openStore2(t, clk, "alice", a2acore.Limits{MaxOpenTasks: 2})
	// tg is committed and open: it holds one slot of the limit, so the
	// new task below fits only if the continuation held none.
	if err := createTask(s, "g", a2a.TaskStateInputRequired); err != nil {
		t.Fatalf("Create tg: %v", err)
	}
	cont, err := s.Admit(context.Background(), admission("alice", "m-g", "tg", "cx-g", "acme/widgets", "s-g", 1, 1, false, nil))
	if err != nil || cont.Refusal != nil || cont.IngressID == 0 {
		t.Fatalf("continuation of the open task = %+v, %v, want admitted", cont, err)
	}
	var reserved int
	if err := s.db.QueryRow(`SELECT COUNT(DISTINCT task_id) FROM a2a_ingress
		WHERE new_task = 1 AND received_unix > ? AND task_id NOT IN (SELECT task_id FROM a2a_tasks)`,
		clk.t.Unix()-int64(repointGrace/time.Second)).Scan(&reserved); err != nil {
		t.Fatalf("reservation count: %v", err)
	}
	if reserved != 0 {
		t.Fatalf("reservation count = %d, want 0 (a continuation admission holds no slot)", reserved)
	}
	res, err := admitNew(s, "x")
	requireAdmitted(t, "Admit X after a continuation admission", res, err)
}

// TestCapacityTerminalFreesSlot proves each terminal state frees the
// slot of its task for the next new task, whose create is not refused.
func TestCapacityTerminalFreesSlot(t *testing.T) {
	for _, state := range []a2a.TaskState{a2a.TaskStateCompleted, a2a.TaskStateCanceled, a2a.TaskStateFailed, a2a.TaskStateRejected} {
		t.Run(NeutralState(state), func(t *testing.T) {
			clk := &clock{t: testNow}
			s, _ := openStore2(t, clk, "alice", a2acore.Limits{MaxOpenTasks: 1})
			res, err := admitNew(s, "x")
			requireAdmitted(t, "Admit X", res, err)
			if err := createTask(s, "x", a2a.TaskStateSubmitted); err != nil {
				t.Fatalf("Create X: %v", err)
			}
			res, err = admitNew(s, "y")
			requireCapacity(t, "Admit Y while X is open", res, err)
			if _, err := s.Update(context.Background(), &taskstore.UpdateRequest{Task: mkTask("tx", "cx-x", state, 2), PrevVersion: 1}); err != nil {
				t.Fatalf("terminal Update: %v", err)
			}
			res, err = admitNew(s, "y")
			requireAdmitted(t, "Admit Y after X is terminal", res, err)
			if err := createTask(s, "y", a2a.TaskStateSubmitted); err != nil {
				t.Fatalf("Create Y after X is terminal: %v", err)
			}
		})
	}
}

// TestCapacityAbandonedAdmissionReleasesSlot proves a reservation lives
// exactly repointGrace: just before it expires the slot is still held,
// at the grace the abandoned admission frees it (no orphan capacity).
func TestCapacityAbandonedAdmissionReleasesSlot(t *testing.T) {
	clk := &clock{t: testNow}
	s, _ := openStore2(t, clk, "alice", a2acore.Limits{MaxOpenTasks: 1})
	res, err := admitNew(s, "x")
	requireAdmitted(t, "Admit X", res, err)
	clk.Add(repointGrace - time.Second)
	res, err = admitNew(s, "y")
	requireCapacity(t, "Admit Y one second before the reservation expires", res, err)
	clk.Add(time.Second)
	res, err = admitNew(s, "y")
	requireAdmitted(t, "Admit Y once the reservation of the abandoned X expired", res, err)
	if err := createTask(s, "y", a2a.TaskStateSubmitted); err != nil {
		t.Fatalf("Create Y: %v", err)
	}
}

// TestCapacityReservationCountedOnce proves a committed task is charged
// once (its reservation leaves the term when its a2a_tasks row exists)
// and a reservation is charged until it commits or expires.
func TestCapacityReservationCountedOnce(t *testing.T) {
	clk := &clock{t: testNow}
	s, _ := openStore2(t, clk, "alice", a2acore.Limits{MaxOpenTasks: 3})
	for _, n := range []string{"x", "y", "z"} {
		res, err := admitNew(s, n)
		requireAdmitted(t, "Admit "+n, res, err)
		if n == "x" {
			// X commits while Y and Z are only reservations.
			if err := createTask(s, "x", a2a.TaskStateSubmitted); err != nil {
				t.Fatalf("Create X: %v", err)
			}
		}
	}
	res, err := admitNew(s, "w")
	requireCapacity(t, "Admit W with one committed task and two reservations at the limit of 3", res, err)
}

// TestCapacityReopenKeepsLiveReservations proves the reservations are
// durable: a restart keeps a live one counted and does not count an
// expired one.
func TestCapacityReopenKeepsLiveReservations(t *testing.T) {
	clk := &clock{t: testNow}
	limits := a2acore.Limits{MaxOpenTasks: 1}
	s, path := openStore2(t, clk, "alice", limits)
	res, err := admitNew(s, "x")
	requireAdmitted(t, "Admit X", res, err)
	s.Close()

	s2 := openAt(t, path, clk, limits)
	res, err = admitNew(s2, "y")
	requireCapacity(t, "Admit Y after a restart with the reservation of X alive", res, err)
	s2.Close()

	clk.Add(repointGrace)
	s3 := openAt(t, path, clk, limits)
	res, err = admitNew(s3, "y")
	requireAdmitted(t, "Admit Y after a restart with the reservation of X expired", res, err)
}

// TestCreateBackstopRefusesRepointAtCapacity proves the definitive check
// of Create: an abandoned admission whose retry is re-pointed to a new
// task (the re-point runs no limit) is refused when the task is created
// and the store already holds the limit of open tasks. Nothing is
// inserted and the re-pointed deliveries stay held.
func TestCreateBackstopRefusesRepointAtCapacity(t *testing.T) {
	clk := &clock{t: testNow}
	s, _ := openStore2(t, clk, "alice", a2acore.Limits{MaxOpenTasks: 1})
	res, err := admitNew(s, "x")
	requireAdmitted(t, "Admit X", res, err)
	clk.Add(repointGrace)
	res, err = admitNew(s, "y")
	requireAdmitted(t, "Admit Y after the reservation of X expired", res, err)
	if err := createTask(s, "y", a2a.TaskStateSubmitted); err != nil {
		t.Fatalf("Create Y: %v", err)
	}
	// The retry of the abandoned message of X, now with a new task id.
	retry, err := s.Admit(context.Background(), admission("alice", "m-x", "tx2", "cx-x", "acme/widgets", "s-x", 1, 1, true,
		[]a2acore.PlannedDelivery{ownerDelivery("owner/x")}))
	if err != nil || retry.Refusal != nil || retry.Duplicate {
		t.Fatalf("re-point admission = %+v, %v, want the re-point (no limit runs there)", retry, err)
	}
	before := queryCount(t, s.db)
	_, err = s.Create(context.Background(), mkTask("tx2", "cx-x", a2a.TaskStateSubmitted, 1))
	requireCapacityRefusalError(t, "Create of the re-pointed task", err)
	after := queryCount(t, s.db)
	for table := range before {
		if before[table] != after[table] {
			t.Errorf("the refused Create changed %s: %d -> %d", table, before[table], after[table])
		}
	}
	var held int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM a2a_deliveries WHERE task_id = 'tx2' AND state = ?`, a2acore.DeliveryHeld).Scan(&held); err != nil {
		t.Fatalf("count held: %v", err)
	}
	if held != 1 {
		t.Errorf("held deliveries of the refused task = %d, want 1 (still held)", held)
	}
	if n := openDurable(t, s); n != 1 {
		t.Errorf("durable open tasks = %d, want 1", n)
	}
}

// TestCreateBackstopRefusesWithoutAdmission proves the check needs no
// admission row: a Create at the limit is refused, a Create of a task
// already terminal is not (a terminal task takes no slot).
func TestCreateBackstopRefusesWithoutAdmission(t *testing.T) {
	clk := &clock{t: testNow}
	s, _ := openStore2(t, clk, "alice", a2acore.Limits{MaxOpenTasks: 1})
	if err := createTask(s, "x", a2a.TaskStateSubmitted); err != nil {
		t.Fatalf("Create X: %v", err)
	}
	requireCapacityRefusalError(t, "Create Y at the limit", createTask(s, "y", a2a.TaskStateSubmitted))
	if err := createTask(s, "z", a2a.TaskStateCompleted); err != nil {
		t.Errorf("Create of a terminal task at the limit: %v, want it stored", err)
	}
}

// TestCreateBackstopDoesNotCountOwnReservation proves the reservation of
// the task being created is not charged against it: the create that
// follows an admission at the very limit succeeds.
func TestCreateBackstopDoesNotCountOwnReservation(t *testing.T) {
	clk := &clock{t: testNow}
	s, _ := openStore2(t, clk, "alice", a2acore.Limits{MaxOpenTasks: 1})
	res, err := admitNew(s, "x")
	requireAdmitted(t, "Admit X", res, err)
	if err := createTask(s, "x", a2a.TaskStateSubmitted); err != nil {
		t.Fatalf("Create X after its own admission: %v", err)
	}
}
