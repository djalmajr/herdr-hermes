package a2astore

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv/taskstore"

	"github.com/djalmajr/nau/internal/a2acore"
	"github.com/djalmajr/nau/internal/notify"
)

// refuseCode returns the refusal code of a refusal (or a sentinel text
// when there is none).
func refuseCode(r *a2acore.Refusal) string {
	if r == nil {
		return "<none>"
	}
	return r.Code
}

// TestAdmitFresh proves a fresh admission: the ingress row with all its
// correlation, the fence, and the planned deliveries with their states;
// a new task's held deliveries stay held (due only when the task
// commits), a continuation's held ones become pending at once.
func TestAdmitFresh(t *testing.T) {
	clk := &clock{t: testNow}
	s, _ := openTest(t, clk, "alice")
	res, err := s.Admit(context.Background(), admission("alice", "m-1", "t1", "c1", "acme/widgets", "s1", 1, 1, true,
		[]a2acore.PlannedDelivery{ownerDelivery("m1/ws1:p1"), {Role: a2acore.RoleGuardian, Ref: "", State: a2acore.DeliveryNoRoute}}))
	if err != nil {
		t.Fatalf("Admit: %v", err)
	}
	if res.IngressID == 0 || res.Duplicate || res.Refusal != nil {
		t.Fatalf("Admit = %+v, want a fresh ingress", res)
	}
	var subject, messageID, taskID, contextID, project, session, job string
	var epoch, seq, newTask, textBytes int64
	if err := s.db.QueryRow(`SELECT subject, message_id, task_id, context_id, project, COALESCE(job, ''), session, epoch, seq, new_task, text_bytes FROM a2a_ingress`).
		Scan(&subject, &messageID, &taskID, &contextID, &project, &job, &session, &epoch, &seq, &newTask, &textBytes); err != nil {
		t.Fatalf("ingress: %v", err)
	}
	if subject != "alice" || messageID != "m-1" || taskID != "t1" || contextID != "c1" || project != "acme/widgets" ||
		session != "s1" || epoch != 1 || seq != 1 || newTask != 1 || textBytes != 16 {
		t.Fatalf("ingress row = (%s %s %s %s %s %q %s %d %d %d %d)", subject, messageID, taskID, contextID, project, job, session, epoch, seq, newTask, textBytes)
	}
	var fenceEpoch, fenceSeq int64
	if err := s.db.QueryRow(`SELECT epoch, last_seq FROM a2a_fences WHERE subject = 'alice' AND session = 's1'`).Scan(&fenceEpoch, &fenceSeq); err != nil {
		t.Fatalf("fence: %v", err)
	}
	if fenceEpoch != 1 || fenceSeq != 1 {
		t.Fatalf("fence = %d/%d, want 1/1", fenceEpoch, fenceSeq)
	}
	states := deliveryStates(t, s, "t1")
	if states["owner"] != a2acore.DeliveryHeld || states["guardian"] != a2acore.DeliveryNoRoute {
		t.Fatalf("new-task deliveries = %v, want held and no_route", states)
	}
	// The task commits awaiting the caller (input_required): the held
	// delivery of the first message becomes pending, and the
	// continuation below passes the awaiting-caller gate.
	if _, err := s.Create(context.Background(), mkTask("t1", "c1", a2a.TaskStateInputRequired, 1)); err != nil {
		t.Fatalf("Create t1: %v", err)
	}
	// A continuation: held becomes pending, due at once.
	res, err = s.Admit(context.Background(), admission("alice", "m-2", "t1", "c1", "acme/widgets", "s1", 1, 2, false,
		[]a2acore.PlannedDelivery{ownerDelivery("m1/ws1:p1")}))
	if err != nil || res.Refusal != nil || res.IngressID == 0 {
		t.Fatalf("continuation Admit = %+v, %v", res, err)
	}
	states = deliveryStates(t, s, "t1")
	// Both admissions planned an owner delivery: the second one is a
	// different ingress row, so two rows for the role exist; the task
	// commit and the continuation each set their delivery pending,
	// due at once.
	var pending, held int
	if err := s.db.QueryRow(`SELECT SUM(state = ?), SUM(state = ?) FROM a2a_deliveries WHERE task_id = 't1' AND role = 'owner'`,
		a2acore.DeliveryPending, a2acore.DeliveryHeld).Scan(&pending, &held); err != nil {
		t.Fatalf("delivery states: %v", err)
	}
	if pending != 2 || held != 0 {
		t.Fatalf("owner deliveries = pending %d held %d, want 2/0", pending, held)
	}
	var nextUnix int64
	if err := s.db.QueryRow(`SELECT d.next_attempt_unix FROM a2a_deliveries d
		JOIN a2a_ingress i ON i.ingress_id = d.ingress_id
		WHERE d.task_id = 't1' AND d.role = 'owner' AND i.message_id = 'm-2' AND d.state = ?`,
		a2acore.DeliveryPending).Scan(&nextUnix); err != nil {
		t.Fatalf("next_attempt_unix: %v", err)
	}
	if nextUnix != clk.t.Unix() {
		t.Fatalf("continuation next_attempt_unix = %d, want due at once", nextUnix)
	}
}

// TestAdmitDuplicate proves the duplicate rule: with the task row the
// first task is returned, without it the ingress is re-pointed and its
// deliveries reset to held, and the fence never moves.
func TestAdmitDuplicate(t *testing.T) {
	clk := &clock{t: testNow}
	s, _ := openTest(t, clk, "alice")
	first, err := s.Admit(context.Background(), admission("alice", "m-1", "t1", "c1", "acme/widgets", "s1", 1, 1, true,
		[]a2acore.PlannedDelivery{ownerDelivery("m1/ws1:p1")}))
	if err != nil {
		t.Fatalf("Admit: %v", err)
	}
	// Without the task row: re-pointed, deliveries reset, fence intact.
	before := queryCount(t, s.db)
	// A retry while the first request may still create its task is a
	// duplicate of that (not yet committed) task, never a second task.
	if res, err := s.Admit(context.Background(), admission("alice", "m-1", "t2", "c2", "acme/widgets", "s1", 1, 1, true,
		[]a2acore.PlannedDelivery{ownerDelivery("m1/ws1:p1")})); err != nil || !res.Duplicate || res.DuplicateTaskID != "t1" {
		t.Fatalf("young retry Admit = %+v, %v, want a duplicate of t1", res, err)
	}
	clk.Add(repointGrace)
	// Drift the delivery, as a crashed run would have left it.
	if _, err := s.db.Exec(`UPDATE a2a_deliveries SET state = ?, attempts = 3 WHERE ingress_id = ?`, a2acore.DeliveryPending, first.IngressID); err != nil {
		t.Fatalf("drift: %v", err)
	}
	res, err := s.Admit(context.Background(), admission("alice", "m-1", "t2", "c2", "acme/widgets", "s1", 1, 1, true,
		[]a2acore.PlannedDelivery{ownerDelivery("m1/ws1:p1")}))
	if err != nil || res.Refusal != nil || res.Duplicate || res.IngressID != first.IngressID {
		t.Fatalf("re-point Admit = %+v, %v, want the existing ingress", res, err)
	}
	after := queryCount(t, s.db)
	for table := range before {
		if before[table] != after[table] {
			t.Fatalf("re-point changed %s: %d -> %d", table, before[table], after[table])
		}
	}
	var taskID, contextID string
	if err := s.db.QueryRow(`SELECT task_id, context_id FROM a2a_ingress WHERE ingress_id = ?`, first.IngressID).Scan(&taskID, &contextID); err != nil {
		t.Fatalf("ingress: %v", err)
	}
	if taskID != "t2" || contextID != "c2" {
		t.Fatalf("ingress re-point = %s/%s, want t2/c2", taskID, contextID)
	}
	var state string
	var attempts int
	if err := s.db.QueryRow(`SELECT state, attempts FROM a2a_deliveries WHERE ingress_id = ?`, first.IngressID).Scan(&state, &attempts); err != nil {
		t.Fatalf("delivery: %v", err)
	}
	if state != a2acore.DeliveryHeld || attempts != 0 {
		t.Fatalf("delivery after re-point = %s attempts %d, want held 0", state, attempts)
	}
	var fenceSeq int64
	if err := s.db.QueryRow(`SELECT last_seq FROM a2a_fences WHERE subject = 'alice' AND session = 's1'`).Scan(&fenceSeq); err != nil {
		t.Fatalf("fence: %v", err)
	}
	if fenceSeq != 1 {
		t.Fatalf("fence after re-point = %d, want 1 (unchanged)", fenceSeq)
	}
	// With the task row: the first task is returned.
	if _, err := s.Create(context.Background(), mkTask("t2", "c2", a2a.TaskStateSubmitted, 1)); err != nil {
		t.Fatalf("Create: %v", err)
	}
	res, err = s.Admit(context.Background(), admission("alice", "m-1", "t2", "c2", "acme/widgets", "s1", 1, 1, true,
		[]a2acore.PlannedDelivery{ownerDelivery("m1/ws1:p1")}))
	if err != nil || !res.Duplicate || res.DuplicateTaskID != "t2" || res.IngressID != 0 {
		t.Fatalf("duplicate Admit = %+v, %v, want the duplicate of t2", res, err)
	}
}

// TestAdmitFenceRefusals proves the fence refusals (stale epoch, replay,
// out of order, a new epoch restarts at 1) and that a refusal changes
// nothing.
func TestAdmitFenceRefusals(t *testing.T) {
	clk := &clock{t: testNow}
	s, _ := openTest(t, clk, "alice")
	must := func(msgID string, epoch, seq int64) a2acore.AdmitResult {
		t.Helper()
		res, err := s.Admit(context.Background(), admission("alice", msgID, "t1", "c1", "acme/widgets", "s1", epoch, seq, true, nil))
		if err != nil {
			t.Fatalf("Admit %s: %v", msgID, err)
		}
		return res
	}
	// Epoch 2 first, then a stale epoch 1.
	if r := must("m-1", 2, 1); r.Refusal != nil || r.IngressID == 0 {
		t.Fatalf("Admit epoch 2: %+v", r)
	}
	if r := must("m-2", 1, 1); refuseCode(r.Refusal) != a2acore.RefuseStaleEpoch {
		t.Fatalf("stale epoch: %+v, want stale_epoch", r)
	}
	// A replay: the same seq with another message id.
	if r := must("m-3", 2, 2); r.Refusal != nil {
		t.Fatalf("Admit seq 2: %+v", r)
	}
	before := queryCount(t, s.db)
	if r := must("m-4", 2, 2); refuseCode(r.Refusal) != a2acore.RefuseReplay {
		t.Fatalf("replay: %+v, want replay", r)
	}
	// Out of order: a gap.
	if r := must("m-5", 2, 4); refuseCode(r.Refusal) != a2acore.RefuseOutOfOrder {
		t.Fatalf("gap: %+v, want out_of_order", r)
	}
	after := queryCount(t, s.db)
	for table := range before {
		if before[table] != after[table] {
			t.Fatalf("refusals changed %s: %d -> %d", table, before[table], after[table])
		}
	}
	// A new (higher) epoch restarts at 1.
	if r := must("m-6", 3, 2); refuseCode(r.Refusal) != a2acore.RefuseOutOfOrder {
		t.Fatalf("new epoch seq 2: %+v, want out_of_order", r)
	}
	if r := must("m-7", 3, 1); r.Refusal != nil || r.IngressID == 0 {
		t.Fatalf("new epoch seq 1: %+v", r)
	}
	var epoch, lastSeq int64
	if err := s.db.QueryRow(`SELECT epoch, last_seq FROM a2a_fences WHERE subject = 'alice' AND session = 's1'`).Scan(&epoch, &lastSeq); err != nil {
		t.Fatalf("fence: %v", err)
	}
	if epoch != 3 || lastSeq != 1 {
		t.Fatalf("fence = %d/%d, want 3/1", epoch, lastSeq)
	}
	// Another subject on the same session is independent.
	res, err := s.Admit(context.Background(), admission("bob", "m-1", "t9", "c9", "acme/widgets", "s1", 1, 1, true, nil))
	if err != nil || res.Refusal != nil {
		t.Fatalf("bob Admit: %+v, %v", res, err)
	}
}

// TestAdmitLoopLimit proves the context loop limit: the stored tasks of
// the context plus its uncommitted admissions (ingress rows without a
// task row) are counted, and the limit is refused.
func TestAdmitLoopLimit(t *testing.T) {
	clk := &clock{t: testNow}
	s, _ := openStore2(t, clk, "alice", a2acore.Limits{MaxTasksPerContext: 2})
	must := func(session, msgID, taskID string) a2acore.AdmitResult {
		t.Helper()
		res, err := s.Admit(context.Background(), admission("alice", msgID, taskID, "c1", "acme/widgets", session, 1, 1, true, nil))
		if err != nil {
			t.Fatalf("Admit %s: %v", msgID, err)
		}
		return res
	}
	// t1: admitted but never created — its ingress row alone already
	// counts toward the limit.
	if r := must("s1", "m-1", "t1"); r.Refusal != nil {
		t.Fatalf("t1: %+v", r)
	}
	// t2: admitted and created. Without the uncommitted t1 the count
	// would still be 1, so the next admission proves t1 was counted.
	if r := must("s2", "m-2", "t2"); r.Refusal != nil {
		t.Fatalf("t2: %+v", r)
	}
	if _, err := s.Create(context.Background(), mkTask("t2", "c1", a2a.TaskStateSubmitted, 1)); err != nil {
		t.Fatalf("Create t2: %v", err)
	}
	if r := must("s3", "m-3", "t3"); refuseCode(r.Refusal) != a2acore.RefuseLoopLimit {
		t.Fatalf("t3: %+v, want loop_limit (the uncommitted t1 counts)", r)
	}
	// A different context is not limited.
	res, err := s.Admit(context.Background(), admission("alice", "m-4", "t4", "c2", "acme/widgets", "s4", 1, 1, true, nil))
	if err != nil || res.Refusal != nil {
		t.Fatalf("c2 Admit: %+v, %v", res, err)
	}
}

// TestAdmitCapacity proves the open-task capacity: non-terminal tasks
// at the limit refuse a new task, and completing one frees a slot.
func TestAdmitCapacity(t *testing.T) {
	clk := &clock{t: testNow}
	s, _ := openStore2(t, clk, "alice", a2acore.Limits{MaxOpenTasks: 2})
	openTask := func(taskID, msgID, session string) {
		t.Helper()
		res, err := s.Admit(context.Background(), admission("alice", msgID, taskID, "cx-"+taskID, "acme/widgets", session, 1, 1, true, nil))
		if err != nil || res.Refusal != nil {
			t.Fatalf("Admit %s: %+v, %v", taskID, res, err)
		}
		if _, err := s.Create(context.Background(), mkTask(taskID, "cx-"+taskID, a2a.TaskStateSubmitted, 1)); err != nil {
			t.Fatalf("Create %s: %v", taskID, err)
		}
		if _, err := s.Update(context.Background(), &taskstore.UpdateRequest{Task: mkTask(taskID, "cx-"+taskID, a2a.TaskStateWorking, 2), PrevVersion: 1}); err != nil {
			t.Fatalf("Update %s: %v", taskID, err)
		}
	}
	openTask("t1", "m-1", "s1")
	openTask("t2", "m-2", "s2")
	before := queryCount(t, s.db)
	res, err := s.Admit(context.Background(), admission("alice", "m-3", "t3", "cx-t3", "acme/widgets", "s3", 1, 1, true, nil))
	if err != nil || refuseCode(res.Refusal) != a2acore.RefuseCapacity {
		t.Fatalf("capacity Admit: %+v, %v, want capacity", res, err)
	}
	after := queryCount(t, s.db)
	for table := range before {
		if before[table] != after[table] {
			t.Fatalf("capacity refusal changed %s: %d -> %d", table, before[table], after[table])
		}
	}
	// Completing one task frees a slot.
	if _, err := s.Update(context.Background(), &taskstore.UpdateRequest{Task: mkTask("t1", "cx-t1", a2a.TaskStateCompleted, 2), PrevVersion: 2}); err != nil {
		t.Fatalf("complete t1: %v", err)
	}
	res, err = s.Admit(context.Background(), admission("alice", "m-3", "t3", "cx-t3", "acme/widgets", "s3", 1, 1, true, nil))
	if err != nil || res.Refusal != nil || res.IngressID == 0 {
		t.Fatalf("after completion Admit: %+v, %v", res, err)
	}
}

// TestAdmitIngressIDsIncreasing proves the ingress ids are strictly
// increasing in admission order, across subjects and sessions.
func TestAdmitIngressIDsIncreasing(t *testing.T) {
	clk := &clock{t: testNow}
	s, _ := openTest(t, clk, "alice")
	// t1 commits awaiting the caller, so the continuation below (m-2)
	// passes the awaiting-caller gate.
	if _, err := s.Create(context.Background(), mkTask("t1", "c1", a2a.TaskStateInputRequired, 1)); err != nil {
		t.Fatalf("Create t1: %v", err)
	}
	var ids []int64
	for i, in := range []a2acore.Admission{
		admission("alice", "m-1", "t1", "c1", "acme/widgets", "s1", 1, 1, true, nil),
		admission("bob", "m-1", "t2", "c2", "acme/widgets", "s1", 1, 1, true, nil),
		admission("alice", "m-2", "t1", "c1", "acme/widgets", "s1", 1, 2, false, nil),
		admission("carol", "m-1", "t3", "c3", "acme/widgets", "s9", 1, 1, true, nil),
	} {
		res, err := s.Admit(context.Background(), in)
		if err != nil || res.Refusal != nil {
			t.Fatalf("Admit %d: %+v, %v", i, res, err)
		}
		ids = append(ids, res.IngressID)
	}
	for i := 1; i < len(ids); i++ {
		if ids[i] <= ids[i-1] {
			t.Fatalf("ingress ids %v are not strictly increasing", ids)
		}
	}
}

// openStore2 is openStore with a caller-chosen limits.
func openStore2(t *testing.T, clk *clock, owner string, limits a2acore.Limits) (*Store, string) {
	t.Helper()
	path := t.TempDir() + "/registry.db"
	s, err := OpenFile(path, Options{Now: clk.Now, Owner: fixedOwner(owner), Limits: limits})
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s, path
}

// deliveryRow is the stored row of one delivery.
type deliveryRow struct {
	state        string
	attempts     int
	nextAttempt  int64
	hasNext      bool
	leaseUntil   int64
	hasLease     bool
	first, last  string
	accepted     string
	acceptStatus string
	lastExit     int
	hasExit      bool
	lastError    string
}

// readDelivery reads the stored row of one delivery, with its nullable
// columns resolved.
func readDelivery(t *testing.T, s *Store, id int64) deliveryRow {
	t.Helper()
	var (
		d                                              deliveryRow
		next, lse                                      sql.NullInt64
		exit                                           sql.NullInt64
		first, last, accepted, acceptStatus, lastError sql.NullString
	)
	err := s.db.QueryRow(`SELECT state, attempts, next_attempt_unix, lease_until_unix, first_attempt, last_attempt,
		accepted, accept_status, last_exit, last_error
		FROM a2a_deliveries WHERE delivery_id = ?`, id).
		Scan(&d.state, &d.attempts, &next, &lse, &first, &last, &accepted, &acceptStatus, &exit, &lastError)
	if err != nil {
		t.Fatalf("delivery %d: %v", id, err)
	}
	d.first, d.last = first.String, last.String
	d.accepted, d.acceptStatus, d.lastError = accepted.String, acceptStatus.String, lastError.String
	if next.Valid {
		d.hasNext, d.nextAttempt = true, next.Int64
	}
	if lse.Valid {
		d.hasLease, d.leaseUntil = true, lse.Int64
	}
	if exit.Valid {
		d.hasExit, d.lastExit = true, int(exit.Int64)
	}
	return d
}

// TestClaimDue proves the claim rule: pending due and in-flight expired
// are claimed in ingress then delivery order, marked in-flight with a
// fresh lease and counted; a live lease is not due again.
func TestClaimDue(t *testing.T) {
	clk := &clock{t: testNow}
	s, _ := openTest(t, clk, "alice")
	for i, taskID := range []string{"t1", "t2", "t3"} {
		if _, err := s.Admit(context.Background(), admission("alice", fmt.Sprintf("m-%d", i+1), taskID, "cx-"+taskID, "acme/widgets", fmt.Sprintf("s%d", i+1), 1, 1, true,
			[]a2acore.PlannedDelivery{ownerDelivery("m1/ws1:p1")})); err != nil {
			t.Fatalf("Admit %s: %v", taskID, err)
		}
		if _, err := s.Create(context.Background(), mkTask(taskID, "cx-"+taskID, a2a.TaskStateSubmitted, 1)); err != nil {
			t.Fatalf("Create %s: %v", taskID, err)
		}
	}
	claims, err := s.ClaimDue(context.Background(), clk.t, 10, time.Minute)
	if err != nil {
		t.Fatalf("ClaimDue: %v", err)
	}
	if len(claims) != 3 {
		t.Fatalf("ClaimDue = %d claims, want 3", len(claims))
	}
	for i, c := range claims {
		if c.TaskID != fmt.Sprintf("t%d", i+1) || c.Attempt != 1 || c.Role != a2acore.RoleOwner || c.Ref != "m1/ws1:p1" ||
			c.Project != "acme/widgets" || c.ContextID != "cx-t"+fmt.Sprint(i+1) || c.TaskState != a2acore.StateSubmitted {
			t.Fatalf("claim %d = %+v", i, c)
		}
		row := readDelivery(t, s, c.DeliveryID)
		if row.state != a2acore.DeliveryInFlight || row.attempts != 1 || !row.hasLease || row.leaseUntil != clk.t.Add(time.Minute).Unix() {
			t.Fatalf("delivery after claim = %+v", row)
		}
		if row.first != clk.ts() || row.last != clk.ts() {
			t.Fatalf("attempt timestamps = %q/%q, want %q", row.first, row.last, clk.ts())
		}
	}
	// A live lease is not due again.
	claims, err = s.ClaimDue(context.Background(), clk.t, 10, time.Minute)
	if err != nil || len(claims) != 0 {
		t.Fatalf("ClaimDue with live leases = %d, %v, want 0", len(claims), err)
	}
	// A lease expiry makes the delivery due again.
	clk.Add(time.Minute + time.Second)
	claims, err = s.ClaimDue(context.Background(), clk.t, 10, time.Minute)
	if err != nil || len(claims) != 3 {
		t.Fatalf("ClaimDue after lease expiry = %d, %v, want 3", len(claims), err)
	}
	for _, c := range claims {
		if c.Attempt != 2 {
			t.Fatalf("re-claimed attempt = %d, want 2", c.Attempt)
		}
	}
	// The limit caps the claims and keeps the order.
	clk.Add(time.Hour)
	claims, err = s.ClaimDue(context.Background(), clk.t, 1, time.Minute)
	if err != nil || len(claims) != 1 || claims[0].TaskID != "t1" {
		t.Fatalf("limited ClaimDue = %+v, %v, want only t1", claims, err)
	}
}

// TestCompleteOutcomes proves the Complete mapping: accepted,
// uncertain, rejected, transient (backoff and exhaustion), the stale-
// attempt no-op, and the unknown outcome as transient.
func TestCompleteOutcomes(t *testing.T) {
	clk := &clock{t: testNow}
	s, _ := openTest(t, clk, "alice")
	// Four tasks, one delivery each.
	for _, taskID := range []string{"ta", "tb", "tc", "td"} {
		if _, err := s.Admit(context.Background(), admission("alice", taskID+"-m", taskID, "cx-"+taskID, "acme/widgets", taskID, 1, 1, true,
			[]a2acore.PlannedDelivery{ownerDelivery("m1/ws1:p1")})); err != nil {
			t.Fatalf("Admit %s: %v", taskID, err)
		}
		if _, err := s.Create(context.Background(), mkTask(taskID, "cx-"+taskID, a2a.TaskStateSubmitted, 1)); err != nil {
			t.Fatalf("Create %s: %v", taskID, err)
		}
	}
	claims, err := s.ClaimDue(context.Background(), clk.t, 10, time.Minute)
	if err != nil || len(claims) != 4 {
		t.Fatalf("ClaimDue: %v, %d", err, len(claims))
	}
	ctx := context.Background()
	// accepted.
	if err := s.Complete(ctx, claims[0], notify.SendResult{Outcome: notify.OutcomeAccepted, Status: "sent", Exit: 0}, clk.Now()); err != nil {
		t.Fatalf("Complete accepted: %v", err)
	}
	if row := readDelivery(t, s, claims[0].DeliveryID); row.state != a2acore.DeliveryAccepted || row.accepted != clk.ts() ||
		row.acceptStatus != "sent" || !row.hasExit || row.lastExit != 0 || row.lastError != "" {
		t.Fatalf("accepted row = %+v", row)
	}
	// uncertain: final, never claimed again.
	if err := s.Complete(ctx, claims[1], notify.SendResult{Outcome: notify.OutcomeUncertain, Exit: 15, Category: "uncertain"}, clk.Now()); err != nil {
		t.Fatalf("Complete uncertain: %v", err)
	}
	if row := readDelivery(t, s, claims[1].DeliveryID); row.state != a2acore.DeliveryUncertain || row.lastError != "uncertain" {
		t.Fatalf("uncertain row = %+v", row)
	}
	// rejected.
	if err := s.Complete(ctx, claims[2], notify.SendResult{Outcome: notify.OutcomeRejected, Exit: 2, Category: "refused"}, clk.Now()); err != nil {
		t.Fatalf("Complete rejected: %v", err)
	}
	if row := readDelivery(t, s, claims[2].DeliveryID); row.state != a2acore.DeliveryRejected || row.lastError != "refused" || row.lastExit != 2 {
		t.Fatalf("rejected row = %+v", row)
	}
	// transient: back to pending on the schedule.
	if err := s.Complete(ctx, claims[3], notify.SendResult{Outcome: notify.OutcomeTransient, Exit: 1, Category: "unavailable"}, clk.Now()); err != nil {
		t.Fatalf("Complete transient: %v", err)
	}
	if row := readDelivery(t, s, claims[3].DeliveryID); row.state != a2acore.DeliveryPending || !row.hasNext ||
		row.nextAttempt != clk.t.Add(notify.RetrySchedule[0]).Unix() {
		t.Fatalf("transient row = %+v, want pending on the schedule", row)
	}
	// Not due before the scheduled instant.
	if claims, err := s.ClaimDue(ctx, clk.t, 10, time.Minute); err != nil || len(claims) != 0 {
		t.Fatalf("ClaimDue before the backoff = %d, %v, want 0", len(claims), err)
	}
	clk.Add(notify.RetrySchedule[0])
	claims, err = s.ClaimDue(ctx, clk.t, 10, time.Minute)
	if err != nil || len(claims) != 1 || claims[0].Attempt != 2 {
		t.Fatalf("ClaimDue after the backoff = %+v, %v, want the retry at attempt 2", claims, err)
	}
	// The unknown outcome is treated as transient.
	if err := s.Complete(ctx, claims[0], notify.SendResult{Outcome: "unknown-outcome", Exit: 99, Category: "exit"}, clk.Now()); err != nil {
		t.Fatalf("Complete unknown: %v", err)
	}
	if row := readDelivery(t, s, claims[0].DeliveryID); row.state != a2acore.DeliveryPending || row.lastError != "exit" || !row.hasExit || row.lastExit != 99 {
		t.Fatalf("unknown-outcome row = %+v, want pending with the category", row)
	}
	// A stale attempt changes nothing.
	if err := s.Complete(ctx, a2acore.DeliveryClaim{DeliveryID: claims[0].DeliveryID, Attempt: 99}, notify.SendResult{Outcome: notify.OutcomeAccepted, Status: "sent", Exit: 0}, clk.Now()); err != nil {
		t.Fatalf("Complete stale: %v", err)
	}
	if row := readDelivery(t, s, claims[0].DeliveryID); row.state != a2acore.DeliveryPending || row.attempts != 2 {
		t.Fatalf("stale Complete changed the row = %+v", row)
	}
	// A stale release changes nothing either.
	if err := s.Release(ctx, a2acore.DeliveryClaim{DeliveryID: claims[0].DeliveryID, Attempt: 99}); err != nil {
		t.Fatalf("Release stale: %v", err)
	}
	if row := readDelivery(t, s, claims[0].DeliveryID); row.state != a2acore.DeliveryPending || row.attempts != 2 {
		t.Fatalf("stale Release changed the row = %+v", row)
	}
	// Exhaustion: transients up to the last attempt are final.
	for attempt := 3; attempt <= notify.MaxAttempts; attempt++ {
		clk.Add(time.Hour)
		claims, err := s.ClaimDue(ctx, clk.t, 10, time.Minute)
		if err != nil || len(claims) != 1 || claims[0].Attempt != attempt {
			t.Fatalf("ClaimDue for attempt %d = %+v, %v", attempt, claims, err)
		}
		if err := s.Complete(ctx, claims[0], notify.SendResult{Outcome: notify.OutcomeTransient, Exit: 1, Category: "unavailable"}, clk.Now()); err != nil {
			t.Fatalf("Complete attempt %d: %v", attempt, err)
		}
		row := readDelivery(t, s, claims[0].DeliveryID)
		if attempt < notify.MaxAttempts {
			if row.state != a2acore.DeliveryPending {
				t.Fatalf("attempt %d row = %+v, want pending", attempt, row)
			}
		} else if row.state != a2acore.DeliveryExhausted {
			t.Fatalf("attempt %d row = %+v, want exhausted", attempt, row)
		}
	}
	// Exhausted is never claimed again.
	clk.Add(time.Hour)
	if claims, err := s.ClaimDue(ctx, clk.t, 10, time.Minute); err != nil || len(claims) != 0 {
		t.Fatalf("ClaimDue after exhaustion = %d, %v, want 0", len(claims), err)
	}
}

// TestRelease proves Release returns an unsent claim to pending without
// consuming the attempt, and is a no-op on a stale claim.
func TestRelease(t *testing.T) {
	clk := &clock{t: testNow}
	s, _ := openTest(t, clk, "alice")
	if _, err := s.Admit(context.Background(), admission("alice", "m-1", "t1", "c1", "acme/widgets", "s1", 1, 1, true,
		[]a2acore.PlannedDelivery{ownerDelivery("m1/ws1:p1")})); err != nil {
		t.Fatalf("Admit: %v", err)
	}
	if _, err := s.Create(context.Background(), mkTask("t1", "c1", a2a.TaskStateSubmitted, 1)); err != nil {
		t.Fatalf("Create: %v", err)
	}
	ctx := context.Background()
	claims, err := s.ClaimDue(ctx, clk.t, 10, time.Minute)
	if err != nil || len(claims) != 1 {
		t.Fatalf("ClaimDue: %v, %v", claims, err)
	}
	// A stale claim changes nothing.
	if err := s.Release(ctx, a2acore.DeliveryClaim{DeliveryID: claims[0].DeliveryID, Attempt: 2}); err != nil {
		t.Fatalf("Release stale: %v", err)
	}
	if row := readDelivery(t, s, claims[0].DeliveryID); row.state != a2acore.DeliveryInFlight || row.attempts != 1 {
		t.Fatalf("stale Release changed the row = %+v", row)
	}
	// The live claim: back to pending, attempt not consumed, and still
	// due at the same instant.
	if err := s.Release(ctx, claims[0]); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if row := readDelivery(t, s, claims[0].DeliveryID); row.state != a2acore.DeliveryPending || row.attempts != 0 || row.hasLease || row.first != "" || row.last != "" {
		t.Fatalf("released row = %+v, want pending 0 attempts, no lease or timestamps", row)
	}
	// Claimable again at the same attempt.
	claims, err = s.ClaimDue(ctx, clk.t, 10, time.Minute)
	if err != nil || len(claims) != 1 || claims[0].Attempt != 1 {
		t.Fatalf("re-claim after Release = %+v, %v, want attempt 1", claims, err)
	}
	// A second attempt that is released keeps the first attempt.
	if err := s.Complete(ctx, claims[0], notify.SendResult{Outcome: notify.OutcomeTransient, Exit: 1, Category: "unavailable"}, clk.Now()); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	firstAttempt := readDelivery(t, s, claims[0].DeliveryID).first
	clk.Add(notify.RetrySchedule[0])
	claims, err = s.ClaimDue(ctx, clk.t, 10, time.Minute)
	if err != nil || len(claims) != 1 || claims[0].Attempt != 2 {
		t.Fatalf("ClaimDue attempt 2: %+v, %v", claims, err)
	}
	if err := s.Release(ctx, claims[0]); err != nil {
		t.Fatalf("Release attempt 2: %v", err)
	}
	row := readDelivery(t, s, claims[0].DeliveryID)
	if row.state != a2acore.DeliveryPending || row.attempts != 1 || row.last != firstAttempt {
		t.Fatalf("released second attempt = %+v, want pending 1 with the first attempt's timestamp", row)
	}
}

// TestRecoverHeld proves RecoverHeld marks obsolete only the held
// deliveries older than grace whose task never committed.
func TestRecoverHeld(t *testing.T) {
	clk := &clock{t: testNow}
	s, _ := openTest(t, clk, "alice")
	// t1: admitted, never created (crash between the admission and the
	// task write). t2: admitted and created. t3: admitted late, never
	// created (inside the grace at the recover time).
	if _, err := s.Admit(context.Background(), admission("alice", "m-1", "t1", "c1", "acme/widgets", "s1", 1, 1, true, []a2acore.PlannedDelivery{ownerDelivery("m1/ws1:p1")})); err != nil {
		t.Fatalf("Admit t1: %v", err)
	}
	if _, err := s.Admit(context.Background(), admission("alice", "m-2", "t2", "c2", "acme/widgets", "s2", 1, 1, true, []a2acore.PlannedDelivery{ownerDelivery("m1/ws1:p1")})); err != nil {
		t.Fatalf("Admit t2: %v", err)
	}
	if _, err := s.Create(context.Background(), mkTask("t2", "c2", a2a.TaskStateSubmitted, 1)); err != nil {
		t.Fatalf("Create t2: %v", err)
	}
	clk.Add(6 * time.Minute)
	if _, err := s.Admit(context.Background(), admission("alice", "m-3", "t3", "c3", "acme/widgets", "s3", 1, 1, true, []a2acore.PlannedDelivery{ownerDelivery("m1/ws1:p1")})); err != nil {
		t.Fatalf("Admit t3: %v", err)
	}
	marked, err := s.RecoverHeld(context.Background(), clk.Now(), 5*time.Minute)
	if err != nil {
		t.Fatalf("RecoverHeld: %v", err)
	}
	if marked != 1 {
		t.Fatalf("RecoverHeld marked %d, want 1 (only t1 is past the grace)", marked)
	}
	states := map[string]string{}
	rows, err := s.db.Query(`SELECT task_id, state FROM a2a_deliveries`)
	if err != nil {
		t.Fatalf("deliveries: %v", err)
	}
	for rows.Next() {
		var taskID, state string
		if err := rows.Scan(&taskID, &state); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if _, ok := states[taskID]; !ok {
			states[taskID] = state
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	if states["t1"] != a2acore.DeliveryObsolete {
		t.Fatalf("t1 = %q, want obsolete", states["t1"])
	}
	if t3 := states["t3"]; t3 != a2acore.DeliveryHeld {
		t.Fatalf("t3 = %q, want still held (inside the grace)", t3)
	}
	// t2's delivery is pending (its task committed); a second recover
	// marks nothing new.
	if states["t2"] != a2acore.DeliveryPending {
		t.Fatalf("t2 = %q, want pending", states["t2"])
	}
	if marked, err := s.RecoverHeld(context.Background(), clk.Now(), 5*time.Minute); err != nil || marked != 0 {
		t.Fatalf("second RecoverHeld = %d, %v, want 0", marked, err)
	}
}

// TestClaimDuePendingNotBeforeNext proves a pending delivery is not
// claimed before its next attempt instant.
func TestClaimDuePendingNotBeforeNext(t *testing.T) {
	clk := &clock{t: testNow}
	s, _ := openTest(t, clk, "alice")
	if _, err := s.Admit(context.Background(), admission("alice", "m-1", "t1", "c1", "acme/widgets", "s1", 1, 1, true,
		[]a2acore.PlannedDelivery{ownerDelivery("m1/ws1:p1")})); err != nil {
		t.Fatalf("Admit: %v", err)
	}
	if _, err := s.Create(context.Background(), mkTask("t1", "c1", a2a.TaskStateSubmitted, 1)); err != nil {
		t.Fatalf("Create: %v", err)
	}
	// Push the next attempt into the future, like a transient backoff.
	if _, err := s.db.Exec(`UPDATE a2a_deliveries SET next_attempt_unix = ? WHERE task_id = 't1'`, clk.t.Add(time.Hour).Unix()); err != nil {
		t.Fatalf("push: %v", err)
	}
	if claims, err := s.ClaimDue(context.Background(), clk.t, 10, time.Minute); err != nil || len(claims) != 0 {
		t.Fatalf("ClaimDue before the next attempt = %d, %v, want 0", len(claims), err)
	}
	clk.Add(time.Hour)
	if claims, err := s.ClaimDue(context.Background(), clk.t, 10, time.Minute); err != nil || len(claims) != 1 {
		t.Fatalf("ClaimDue at the next attempt = %d, %v, want 1", len(claims), err)
	}
}
