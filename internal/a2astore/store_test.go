package a2astore

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"

	"github.com/djalmajr/nau/internal/a2acore"
	"github.com/djalmajr/nau/internal/registry"
)

// testNow is the fixed start of every test clock: a local time with a
// numeric offset, never Z.
var testNow = time.Date(2026, 3, 4, 5, 6, 7, 0, time.FixedZone("TST", 2*3600))

// clock is the injected test clock.
type clock struct {
	t time.Time
}

// Now implements the Options.Now clock.
func (c *clock) Now() time.Time { return c.t }

// Add advances the clock.
func (c *clock) Add(d time.Duration) { c.t = c.t.Add(d) }

// ts is the stored text form of the current clock instant.
func (c *clock) ts() string { return c.t.Format(tsLayout) }

// fixedOwner is an Options.Owner that always returns owner.
func fixedOwner(owner string) func(context.Context) (string, error) {
	return func(context.Context) (string, error) { return owner, nil }
}

// openTest opens a fresh store in a temp directory on the given clock
// and owner, closing it when the test ends; it returns the path so a
// test can reopen it.
func openTest(t *testing.T, clk *clock, owner string) (*Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "registry.db")
	s, err := OpenFile(path, Options{Now: clk.Now, Owner: fixedOwner(owner)})
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s, path
}

// mkTask is a task fixture with nHistory user messages, the standard
// project metadata and one artifact when nArtifact > 0.
func mkTask(id, contextID string, state a2a.TaskState, nHistory int) *a2a.Task {
	history := make([]*a2a.Message, 0, nHistory)
	for i := 0; i < nHistory; i++ {
		history = append(history, &a2a.Message{ID: fmt.Sprintf("%s-m%d", id, i), Role: a2a.MessageRoleUser})
	}
	task := &a2a.Task{
		ID:        a2a.TaskID(id),
		ContextID: contextID,
		Status:    a2a.TaskStatus{State: state},
		History:   history,
		Metadata:  map[string]any{a2acore.MetaProject: "acme/widgets"},
	}
	return task
}

// withArtifact returns a copy of task with one text artifact.
func withArtifact(task *a2a.Task, id string) *a2a.Task {
	task = cloneTask(task)
	task.Artifacts = []*a2a.Artifact{{
		ID:   a2a.ArtifactID(id),
		Name: "answer",
		Parts: a2a.ContentParts{
			{Content: a2a.Text("the answer text"), MediaType: "text/plain"},
		},
	}}
	return task
}

// cloneTask is a deep copy through the JSON round trip (the store's own
// copy rule), so a fixture mutation never aliases another fixture.
func cloneTask(task *a2a.Task) *a2a.Task {
	cp, err := unmarshalTask(mustJSON(task))
	if err != nil {
		panic(err)
	}
	return cp
}

// admission is an Admission fixture: the envelope of one message and its
// planned deliveries.
func admission(subject, msgID, taskID, contextID, project, session string, epoch, seq int64, newTask bool, deliveries []a2acore.PlannedDelivery) a2acore.Admission {
	return a2acore.Admission{
		Envelope: a2acore.Envelope{
			Subject:   subject,
			MessageID: msgID,
			TaskID:    taskID,
			ContextID: contextID,
			Project:   project,
			Session:   session,
			Epoch:     epoch,
			Seq:       seq,
			TextBytes: 16,
		},
		TaskID:     taskID,
		ContextID:  contextID,
		NewTask:    newTask,
		Deliveries: deliveries,
		ReceivedAt: testNow,
	}
}

// ownerDelivery is the planned owner delivery to ref, held.
func ownerDelivery(ref string) a2acore.PlannedDelivery {
	return a2acore.PlannedDelivery{Role: a2acore.RoleOwner, Ref: ref, State: a2acore.DeliveryHeld}
}

// TestOpenFreshCreatesBothSchemas proves the additive open: a fresh
// path gets the registry tables and the A2A tables, a2a_meta carries
// schema 1, the registry user_version is untouched, and a reopen is
// idempotent.
func TestOpenFreshCreatesBothSchemas(t *testing.T) {
	clk := &clock{t: testNow}
	s, path := openTest(t, clk, "alice")

	for _, table := range []string{"jobs", "events", "nodes", "a2a_meta", "a2a_tasks", "a2a_ingress", "a2a_fences", "a2a_deliveries", "a2a_decisions", "a2a_push_configs"} {
		var n int
		if err := s.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?`, table).Scan(&n); err != nil {
			t.Fatalf("sqlite_master(%s): %v", table, err)
		}
		if n != 1 {
			t.Fatalf("table %s: count %d, want 1", table, n)
		}
	}
	var schema string
	if err := s.db.QueryRow(`SELECT value FROM a2a_meta WHERE key = 'schema'`).Scan(&schema); err != nil {
		t.Fatalf("a2a_meta: %v", err)
	}
	if schema != "1" {
		t.Fatalf("a2a_meta schema = %q, want 1", schema)
	}
	var version int
	if err := s.db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		t.Fatalf("user_version: %v", err)
	}
	if version != registry.SchemaVersion {
		t.Fatalf("user_version = %d, want the registry's %d", version, registry.SchemaVersion)
	}

	// Reopen on the same path: idempotent, nothing duplicated.
	s.Close()
	s2, err := OpenFile(path, Options{Now: clk.Now, Owner: fixedOwner("alice")})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	var tables int
	if err := s2.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name LIKE 'a2a_%'`).Scan(&tables); err != nil {
		t.Fatalf("tables: %v", err)
	}
	if tables != 7 {
		t.Fatalf("a2a tables after reopen = %d, want 7", tables)
	}
	var version2 int
	if err := s2.db.QueryRow(`PRAGMA user_version`).Scan(&version2); err != nil {
		t.Fatalf("user_version: %v", err)
	}
	if version2 != registry.SchemaVersion {
		t.Fatalf("user_version after reopen = %d, want %d", version2, registry.SchemaVersion)
	}
}

// TestOpenRefusesNewerA2ASchema proves a database written by a newer A2A
// schema (a2a_meta schema > 1) is refused, and that the refusal happens
// before this build writes anything else.
func TestOpenRefusesNewerA2ASchema(t *testing.T) {
	clk := &clock{t: testNow}
	s, path := openTest(t, clk, "alice")
	if _, err := s.db.Exec(`UPDATE a2a_meta SET value = '2' WHERE key = 'schema'`); err != nil {
		t.Fatalf("bump: %v", err)
	}
	s.Close()
	if _, err := OpenFile(path, Options{Now: clk.Now, Owner: fixedOwner("alice")}); err == nil {
		t.Fatal("OpenFile on a2a schema 2: no error, want a refusal")
	} else if !strings.Contains(err.Error(), "newer") {
		t.Fatalf("refusal = %v, want a message naming the newer schema", err)
	}
}

// TestStateMappingRoundTrip proves every neutral state round-trips
// through the SDK, and that the states without a neutral form map to
// the empty / unspecified values.
func TestStateMappingRoundTrip(t *testing.T) {
	states := []string{
		a2acore.StateSubmitted, a2acore.StateWorking, a2acore.StateInputRequired,
		a2acore.StateCompleted, a2acore.StateCanceled, a2acore.StateFailed, a2acore.StateRejected,
	}
	for _, st := range states {
		sdk := SDKState(st)
		if NeutralState(sdk) != st {
			t.Errorf("round trip of %q: got %q, want %q", st, NeutralState(sdk), st)
		}
	}
	if got := NeutralState(a2a.TaskStateAuthRequired); got != "" {
		t.Errorf("NeutralState(auth_required) = %q, want empty", got)
	}
	if got := NeutralState(a2a.TaskStateUnspecified); got != "" {
		t.Errorf("NeutralState(unspecified) = %q, want empty", got)
	}
	if got := SDKState(""); got != a2a.TaskStateUnspecified {
		t.Errorf("SDKState(\"\") = %q, want unspecified", got)
	}
	if got := SDKState("no-such-state"); got != a2a.TaskStateUnspecified {
		t.Errorf("SDKState(bogus) = %q, want unspecified", got)
	}
}

// TestDefaultOwner proves the Options.Owner default: the SDK call
// context's authenticated user with a non-empty name, else
// a2a.ErrUnauthenticated.
func TestDefaultOwner(t *testing.T) {
	clk := &clock{t: testNow}
	s, _ := openTest(t, clk, "")
	_ = s

	// No call context.
	if _, err := defaultOwner(context.Background()); !isErr(err, a2a.ErrUnauthenticated) {
		t.Fatalf("no call context: %v, want unauthenticated", err)
	}
	// Unauthenticated user.
	ctx, _ := a2asrv.NewCallContext(context.Background(), nil)
	if _, err := defaultOwner(ctx); !isErr(err, a2a.ErrUnauthenticated) {
		t.Fatalf("unauthenticated user: %v, want unauthenticated", err)
	}
	// Authenticated user with an empty name.
	ctx, cc := a2asrv.NewCallContext(context.Background(), nil)
	cc.User = a2asrv.NewAuthenticatedUser("", nil)
	if _, err := defaultOwner(ctx); !isErr(err, a2a.ErrUnauthenticated) {
		t.Fatalf("empty name: %v, want unauthenticated", err)
	}
	// Authenticated user with a name.
	ctx, cc = a2asrv.NewCallContext(context.Background(), nil)
	cc.User = a2asrv.NewAuthenticatedUser("alice", nil)
	name, err := defaultOwner(ctx)
	if err != nil || name != "alice" {
		t.Fatalf("named user: %q, %v, want alice", name, err)
	}
}

// TestDurabilityAcrossReopen proves the durability point: after an
// admission, a task create and a recorded decision, a close and a
// reopen from the same path see every committed row.
func TestDurabilityAcrossReopen(t *testing.T) {
	clk := &clock{t: testNow}
	s, path := openTest(t, clk, "alice")

	if _, err := s.Admit(context.Background(), admission("alice", "m-1", "t1", "c1", "acme/widgets", "s1", 1, 1, true, []a2acore.PlannedDelivery{ownerDelivery("m1/ws1:p1")})); err != nil {
		t.Fatalf("Admit: %v", err)
	}
	if _, err := s.Create(context.Background(), mkTask("t1", "c1", a2a.TaskStateSubmitted, 1)); err != nil {
		t.Fatalf("Create: %v", err)
	}
	decision := a2acore.Decision{ID: "d-1", TaskID: "t1", Kind: a2acore.DecisionAnswer, Text: "the answer", Actor: "local"}
	if _, refusal, err := s.RecordDecision(context.Background(), decision, clk.Now()); err != nil || refusal != nil {
		t.Fatalf("RecordDecision: %v, %v", refusal, err)
	}
	s.Close()

	s2, err := OpenFile(path, Options{Now: clk.Now, Owner: fixedOwner("alice")})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()

	view, err := s2.TaskView(context.Background(), "t1")
	if err != nil {
		t.Fatalf("TaskView after reopen: %v", err)
	}
	if view.Owner != "alice" || view.Project != "acme/widgets" || view.State != a2acore.StateSubmitted || view.Messages != 1 {
		t.Fatalf("view after reopen = %+v", view)
	}
	got, err := s2.Get(context.Background(), "t1")
	if err != nil {
		t.Fatalf("Get after reopen: %v", err)
	}
	if got.Version != 1 || got.Task.ID != "t1" {
		t.Fatalf("Get after reopen = %+v", got)
	}
	var ingress, fences, deliveries, decisions int
	for _, q := range []struct {
		n     *int
		query string
	}{{&ingress, `SELECT COUNT(*) FROM a2a_ingress`}, {&fences, `SELECT COUNT(*) FROM a2a_fences`}, {&deliveries, `SELECT COUNT(*) FROM a2a_deliveries`}, {&decisions, `SELECT COUNT(*) FROM a2a_decisions`}} {
		if err := s2.db.QueryRow(q.query).Scan(q.n); err != nil {
			t.Fatalf("count: %v", err)
		}
	}
	if ingress != 1 || fences != 1 || deliveries != 1 || decisions != 1 {
		t.Fatalf("rows after reopen: ingress %d, fences %d, deliveries %d, decisions %d, want 1 each", ingress, fences, deliveries, decisions)
	}
	var epoch, lastSeq int64
	if err := s2.db.QueryRow(`SELECT epoch, last_seq FROM a2a_fences WHERE subject = 'alice' AND session = 's1'`).Scan(&epoch, &lastSeq); err != nil {
		t.Fatalf("fence: %v", err)
	}
	if epoch != 1 || lastSeq != 1 {
		t.Fatalf("fence after reopen = epoch %d seq %d, want 1/1", epoch, lastSeq)
	}
	var state string
	if err := s2.db.QueryRow(`SELECT state FROM a2a_deliveries`).Scan(&state); err != nil {
		t.Fatalf("delivery state: %v", err)
	}
	if state != a2acore.DeliveryPending {
		t.Fatalf("delivery state after reopen = %q, want pending (Create released it)", state)
	}
	decisionsList, err := s2.PendingDecisions(context.Background(), 10)
	if err != nil {
		t.Fatalf("PendingDecisions after reopen: %v", err)
	}
	if len(decisionsList) != 1 || decisionsList[0].ID != "d-1" || decisionsList[0].State != a2acore.DecisionPending {
		t.Fatalf("decision after reopen = %+v", decisionsList)
	}
}

// TestTwoHandlesOnOnePath proves a second OpenFile of the same path
// while the first is open works (two handles, busy timeout) and sees
// the committed rows of the first.
func TestTwoHandlesOnOnePath(t *testing.T) {
	clk := &clock{t: testNow}
	s, path := openTest(t, clk, "alice")
	if _, err := s.Create(context.Background(), mkTask("t1", "c1", a2a.TaskStateSubmitted, 1)); err != nil {
		t.Fatalf("Create: %v", err)
	}
	s2, err := OpenFile(path, Options{Now: clk.Now, Owner: fixedOwner("alice")})
	if err != nil {
		t.Fatalf("second OpenFile: %v", err)
	}
	defer s2.Close()
	view, err := s2.TaskView(context.Background(), "t1")
	if err != nil {
		t.Fatalf("second handle TaskView: %v", err)
	}
	if view.ID != "t1" {
		t.Fatalf("second handle view = %+v", view)
	}
	// The second handle may write beside the first.
	decision := a2acore.Decision{ID: "d-1", TaskID: "t1", Kind: a2acore.DecisionAsk, Text: "a question", Actor: "local"}
	if _, refusal, err := s2.RecordDecision(context.Background(), decision, clk.Now()); err != nil || refusal != nil {
		t.Fatalf("second handle RecordDecision: %v, %v", refusal, err)
	}
	decisions, err := s.PendingDecisions(context.Background(), 10)
	if err != nil {
		t.Fatalf("first handle PendingDecisions: %v", err)
	}
	if len(decisions) != 1 || decisions[0].ID != "d-1" {
		t.Fatalf("first handle decisions = %+v", decisions)
	}
}

// isErr reports whether err wraps want.
func isErr(err error, want error) bool {
	return err != nil && (err == want || strings.Contains(err.Error(), want.Error()))
}

// queryCount is a helper for the row-count assertions of the refusal
// tests: the counts of the A2A tables as one map.
func queryCount(t *testing.T, db *sql.DB) map[string]int {
	t.Helper()
	out := map[string]int{}
	rows, err := db.Query(`SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'a2a_%' AND name != 'a2a_meta'`)
	if err != nil {
		t.Fatalf("tables: %v", err)
	}
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("table name: %v", err)
		}
		names = append(names, name)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatalf("tables: %v", err)
	}
	for _, name := range names {
		var n int
		if err := db.QueryRow(`SELECT COUNT(*) FROM ` + name).Scan(&n); err != nil {
			t.Fatalf("count %s: %v", name, err)
		}
		out[name] = n
	}
	return out
}
