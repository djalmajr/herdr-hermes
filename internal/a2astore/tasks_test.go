package a2astore

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/a2aproject/a2a-go/v2/a2asrv/taskstore"

	"github.com/djalmajr/nau/internal/a2acore"
)

// mustJSON is a test helper: marshal or fail.
func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

// TestCreateGetRoundTrip proves the create/get round trip and the deep
// copy rule: mutating a returned task does not change the stored one.
func TestCreateGetRoundTrip(t *testing.T) {
	clk := &clock{t: testNow}
	s, _ := openTest(t, clk, "alice")
	task := withArtifact(mkTask("t1", "c1", a2a.TaskStateSubmitted, 3), "art1")
	version, err := s.Create(context.Background(), task)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if version != 1 {
		t.Fatalf("Create version = %d, want 1", version)
	}
	got, err := s.Get(context.Background(), "t1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Version != 1 || got.User != "alice" || got.Task.ID != "t1" || got.Task.ContextID != "c1" {
		t.Fatalf("Get = %+v", got)
	}
	if len(got.Task.History) != 3 || len(got.Task.Artifacts) != 1 {
		t.Fatalf("Get task = %+v", got.Task)
	}
	// Mutate the returned value.
	got.Task.History[0].ID = "mutated"
	got.Task.Artifacts[0].Name = "mutated"
	got.Task.Metadata["nau.project"] = "mutated"
	again, err := s.Get(context.Background(), "t1")
	if err != nil {
		t.Fatalf("second Get: %v", err)
	}
	if again.Task.History[0].ID != "t1-m0" || again.Task.Artifacts[0].Name != "answer" || again.Task.Metadata[a2acore.MetaProject] != "acme/widgets" {
		t.Fatalf("stored task changed by mutating the copy: %+v", again.Task)
	}
}

// TestCreateValidation proves the Create shape refusals.
func TestCreateValidation(t *testing.T) {
	clk := &clock{t: testNow}
	s, _ := openTest(t, clk, "alice")
	if _, err := s.Create(context.Background(), nil); !errors.Is(err, a2a.ErrInvalidParams) {
		t.Fatalf("nil task: %v, want invalid params", err)
	}
	emptyID := mkTask("t1", "c1", a2a.TaskStateSubmitted, 0)
	emptyID.ID = ""
	if _, err := s.Create(context.Background(), emptyID); !errors.Is(err, a2a.ErrInvalidParams) {
		t.Fatalf("empty id: %v, want invalid params", err)
	}
	emptyCtx := mkTask("t1", "c1", a2a.TaskStateSubmitted, 0)
	emptyCtx.ContextID = ""
	if _, err := s.Create(context.Background(), emptyCtx); !errors.Is(err, a2a.ErrInvalidParams) {
		t.Fatalf("empty context: %v, want invalid params", err)
	}
}

// TestCreateDuplicate proves a duplicate id is
// taskstore.ErrTaskAlreadyExists and changes nothing.
func TestCreateDuplicate(t *testing.T) {
	clk := &clock{t: testNow}
	s, _ := openTest(t, clk, "alice")
	if _, err := s.Create(context.Background(), mkTask("t1", "c1", a2a.TaskStateSubmitted, 1)); err != nil {
		t.Fatalf("Create: %v", err)
	}
	before := queryCount(t, s.db)
	_, err := s.Create(context.Background(), mkTask("t1", "c1", a2a.TaskStateSubmitted, 2))
	if !errors.Is(err, taskstore.ErrTaskAlreadyExists) {
		t.Fatalf("duplicate Create: %v, want already exists", err)
	}
	after := queryCount(t, s.db)
	if before["a2a_tasks"] != after["a2a_tasks"] {
		t.Fatalf("duplicate Create changed the rows: %v -> %v", before, after)
	}
}

// TestCreateProjectResolution proves the project and job come from the
// newest admission of the task, and from the task metadata (or a
// refusal) when the admission never committed.
func TestCreateProjectResolution(t *testing.T) {
	clk := &clock{t: testNow}
	s, _ := openTest(t, clk, "alice")
	// With an admission: the ingress row wins over the metadata.
	if _, err := s.Admit(context.Background(), admission("alice", "m-1", "t1", "c1", "acme/other", "s1", 1, 1, true, nil)); err != nil {
		t.Fatalf("Admit: %v", err)
	}
	if _, err := s.Create(context.Background(), mkTask("t1", "c1", a2a.TaskStateSubmitted, 1)); err != nil {
		t.Fatalf("Create: %v", err)
	}
	view, err := s.TaskView(context.Background(), "t1")
	if err != nil {
		t.Fatalf("TaskView: %v", err)
	}
	if view.Project != "acme/other" {
		t.Fatalf("project = %q, want the admission's acme/other", view.Project)
	}
	// Without an admission and without a project in the metadata: refused.
	noProject := mkTask("t2", "c1", a2a.TaskStateSubmitted, 1)
	noProject.Metadata = nil
	if _, err := s.Create(context.Background(), noProject); !errors.Is(err, a2a.ErrInvalidParams) {
		t.Fatalf("Create without project: %v, want invalid params", err)
	}
	// Without an admission, the metadata project is used.
	if _, err := s.Create(context.Background(), mkTask("t3", "c1", a2a.TaskStateSubmitted, 1)); err != nil {
		t.Fatalf("Create: %v", err)
	}
	view3, err := s.TaskView(context.Background(), "t3")
	if err != nil {
		t.Fatalf("TaskView: %v", err)
	}
	if view3.Project != "acme/widgets" {
		t.Fatalf("project = %q, want the metadata's acme/widgets", view3.Project)
	}
}

// TestGetUpdateOwnerMasking proves another subject's task is
// a2a.ErrTaskNotFound from Get and Update, and so is a missing one.
func TestGetUpdateOwnerMasking(t *testing.T) {
	clk := &clock{t: testNow}
	var owner = "alice"
	path := t.TempDir() + "/registry.db"
	s, err := OpenFile(path, Options{Now: clk.Now, Owner: func(context.Context) (string, error) { return owner, nil }})
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer s.Close()
	if _, err := s.Create(context.Background(), mkTask("t1", "c1", a2a.TaskStateSubmitted, 1)); err != nil {
		t.Fatalf("Create: %v", err)
	}
	owner = "bob"
	if _, err := s.Get(context.Background(), "t1"); !errors.Is(err, a2a.ErrTaskNotFound) {
		t.Fatalf("Get by another subject: %v, want task not found", err)
	}
	update := &taskstore.UpdateRequest{Task: mkTask("t1", "c1", a2a.TaskStateWorking, 1), PrevVersion: 1}
	if _, err := s.Update(context.Background(), update); !errors.Is(err, a2a.ErrTaskNotFound) {
		t.Fatalf("Update by another subject: %v, want task not found", err)
	}
	// Missing task.
	if _, err := s.Get(context.Background(), "nope"); !errors.Is(err, a2a.ErrTaskNotFound) {
		t.Fatalf("Get missing: %v, want task not found", err)
	}
	if _, err := s.Update(context.Background(), update); !errors.Is(err, a2a.ErrTaskNotFound) {
		t.Fatalf("Update missing: %v, want task not found", err)
	}
}

// TestUpdateVersioning proves the optimistic versioning: a matching
// PrevVersion advances, a stale one is
// taskstore.ErrConcurrentModification and TaskVersionMissing skips the
// check.
func TestUpdateVersioning(t *testing.T) {
	clk := &clock{t: testNow}
	s, _ := openTest(t, clk, "alice")
	if _, err := s.Create(context.Background(), mkTask("t1", "c1", a2a.TaskStateSubmitted, 1)); err != nil {
		t.Fatalf("Create: %v", err)
	}
	// The matching version advances to 2.
	v, err := s.Update(context.Background(), &taskstore.UpdateRequest{Task: mkTask("t1", "c1", a2a.TaskStateWorking, 2), PrevVersion: 1})
	if err != nil {
		t.Fatalf("Update v1: %v", err)
	}
	if v != 2 {
		t.Fatalf("Update version = %d, want 2", v)
	}
	// A stale version is refused and changes nothing.
	before := queryCount(t, s.db)
	var storedVersion int
	if err := s.db.QueryRow(`SELECT version FROM a2a_tasks WHERE task_id = 't1'`).Scan(&storedVersion); err != nil {
		t.Fatalf("version: %v", err)
	}
	_, err = s.Update(context.Background(), &taskstore.UpdateRequest{Task: mkTask("t1", "c1", a2a.TaskStateWorking, 2), PrevVersion: 1})
	if !errors.Is(err, taskstore.ErrConcurrentModification) {
		t.Fatalf("stale Update: %v, want concurrent modification", err)
	}
	after := queryCount(t, s.db)
	if before["a2a_tasks"] != after["a2a_tasks"] || storedVersion != 2 {
		t.Fatalf("stale Update changed the row (version now %d): %v -> %v", storedVersion, before, after)
	}
	// TaskVersionMissing skips the check.
	v, err = s.Update(context.Background(), &taskstore.UpdateRequest{Task: mkTask("t1", "c1", a2a.TaskStateInputRequired, 3), PrevVersion: taskstore.TaskVersionMissing})
	if err != nil {
		t.Fatalf("Update missing version: %v", err)
	}
	if v != 3 {
		t.Fatalf("version = %d, want 3", v)
	}
	got, err := s.Get(context.Background(), "t1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Version != 3 || got.Task.Status.State != a2a.TaskStateInputRequired {
		t.Fatalf("Get = %+v", got)
	}
}

// TestListFilters proves the List contract: owner isolation, the
// context/status/timestamp filters, the page size bounds, the page token
// round trip and its refusal, the history length and the artifact
// rule.
func TestListFilters(t *testing.T) {
	clk := &clock{t: testNow}
	var owner = "alice"
	path := t.TempDir() + "/registry.db"
	s, err := OpenFile(path, Options{Now: clk.Now, Owner: func(context.Context) (string, error) { return owner, nil }})
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer s.Close()
	// Three tasks of alice in three update instants, one of bob.
	if _, err := s.Create(context.Background(), withArtifact(mkTask("t1", "c1", a2a.TaskStateWorking, 3), "art1")); err != nil {
		t.Fatalf("Create t1: %v", err)
	}
	clk.Add(time.Second)
	if _, err := s.Create(context.Background(), mkTask("t2", "c1", a2a.TaskStateCompleted, 1)); err != nil {
		t.Fatalf("Create t2: %v", err)
	}
	clk.Add(time.Second)
	if _, err := s.Create(context.Background(), mkTask("t3", "c2", a2a.TaskStateWorking, 1)); err != nil {
		t.Fatalf("Create t3: %v", err)
	}
	clk.Add(time.Second)
	owner = "bob"
	if _, err := s.Create(context.Background(), mkTask("tb", "c1", a2a.TaskStateWorking, 1)); err != nil {
		t.Fatalf("Create tb: %v", err)
	}
	owner = "alice"

	list := func(req *a2a.ListTasksRequest) *a2a.ListTasksResponse {
		t.Helper()
		resp, err := s.List(context.Background(), req)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		return resp
	}
	ids := func(resp *a2a.ListTasksResponse) []string {
		out := make([]string, 0, len(resp.Tasks))
		for _, task := range resp.Tasks {
			out = append(out, string(task.ID))
		}
		return out
	}
	// Unfiltered: alice's three, newest update first.
	resp := list(&a2a.ListTasksRequest{})
	if len(resp.Tasks) != 3 || resp.TotalSize != 3 || resp.PageSize != 50 {
		t.Fatalf("List = tasks %v total %d size %d, want t3,t2,t1 total 3 size 50", ids(resp), resp.TotalSize, resp.PageSize)
	}
	if got := ids(resp); got[0] != "t3" || got[1] != "t2" || got[2] != "t1" {
		t.Fatalf("order = %v, want [t3 t2 t1]", got)
	}
	// Context filter.
	resp = list(&a2a.ListTasksRequest{ContextID: "c1"})
	if got := ids(resp); len(got) != 2 || got[0] != "t2" || got[1] != "t1" {
		t.Fatalf("context c1 = %v, want [t2 t1]", got)
	}
	// Status filter.
	resp = list(&a2a.ListTasksRequest{Status: a2a.TaskStateCompleted})
	if got := ids(resp); len(got) != 1 || got[0] != "t2" {
		t.Fatalf("status completed = %v, want [t2]", got)
	}
	// A status without a neutral form matches nothing.
	resp = list(&a2a.ListTasksRequest{Status: a2a.TaskStateAuthRequired})
	if len(resp.Tasks) != 0 {
		t.Fatalf("status auth required = %v, want none", ids(resp))
	}
	// Status timestamp filter: t2 carries a timestamp, the others not.
	ts := time.Date(2026, 3, 4, 5, 7, 0, 0, time.UTC)
	stamped := mkTask("t4", "c3", a2a.TaskStateWorking, 1)
	stamped.Status.Timestamp = &ts
	if _, err := s.Create(context.Background(), stamped); err != nil {
		t.Fatalf("Create t4: %v", err)
	}
	resp = list(&a2a.ListTasksRequest{ContextID: "c3", StatusTimestampAfter: &ts})
	if got := ids(resp); len(got) != 1 || got[0] != "t4" {
		t.Fatalf("timestamp >= = %v, want [t4]", got)
	}
	after := ts.Add(time.Hour)
	resp = list(&a2a.ListTasksRequest{ContextID: "c3", StatusTimestampAfter: &after})
	if len(resp.Tasks) != 0 {
		t.Fatalf("timestamp after = %v, want none", ids(resp))
	}
	// Page size bounds.
	if _, err := s.List(context.Background(), &a2a.ListTasksRequest{PageSize: 101}); !errors.Is(err, a2a.ErrInvalidRequest) {
		t.Fatalf("page size 101: %v, want invalid request", err)
	}
	if _, err := s.List(context.Background(), &a2a.ListTasksRequest{PageSize: -1}); !errors.Is(err, a2a.ErrInvalidRequest) {
		t.Fatalf("page size -1: %v, want invalid request", err)
	}
	// Page token round trip: four tasks of alice, two per page.
	resp = list(&a2a.ListTasksRequest{PageSize: 2})
	if len(resp.Tasks) != 2 || resp.NextPageToken == "" || ids(resp)[0] != "t4" || ids(resp)[1] != "t3" {
		t.Fatalf("page 1 = %v token %q, want t4, t3 and a token", ids(resp), resp.NextPageToken)
	}
	resp = list(&a2a.ListTasksRequest{PageSize: 2, PageToken: resp.NextPageToken})
	if len(resp.Tasks) != 2 || resp.NextPageToken != "" || ids(resp)[0] != "t2" || ids(resp)[1] != "t1" {
		t.Fatalf("page 2 = %v token %q, want t2, t1 and no token (last page)", ids(resp), resp.NextPageToken)
	}
	// A token on the last page has no successor: the token of the
	// second page is already ""; reusing a consumed token returns the
	// tail.
	resp = list(&a2a.ListTasksRequest{PageSize: 3, PageToken: pageTokenFor("-1", "t1")})
	if len(resp.Tasks) != 0 {
		t.Fatalf("token before the first row = %v, want none", ids(resp))
	}
	// Bad tokens are a parse error.
	bad := []string{"garbage", pageTokenFor("not-a-number", "t1"), pageTokenFor("123", ""), pageTokenFor("", "t1")}
	for _, token := range bad {
		if _, err := s.List(context.Background(), &a2a.ListTasksRequest{PageToken: token}); !errors.Is(err, a2a.ErrParseError) {
			t.Fatalf("bad token %q: %v, want parse error", token, err)
		}
	}
	// History length: nil keeps up to 100 (here all 3), 0 keeps none,
	// 1 keeps the last one.
	resp = list(&a2a.ListTasksRequest{PageSize: 100})
	if len(resp.Tasks) != 4 || string(resp.Tasks[3].ID) != "t1" {
		t.Fatalf("history base page = %v, want t1 last (newest first)", ids(resp))
	}
	if len(resp.Tasks[3].History) != 3 {
		t.Fatalf("history nil = %d, want 3", len(resp.Tasks[3].History))
	}
	zero := 0
	resp = list(&a2a.ListTasksRequest{PageSize: 100, HistoryLength: &zero})
	if len(resp.Tasks[3].History) != 0 {
		t.Fatalf("history 0 = %d, want 0", len(resp.Tasks[3].History))
	}
	one := 1
	resp = list(&a2a.ListTasksRequest{PageSize: 100, HistoryLength: &one})
	if len(resp.Tasks[3].History) != 1 || resp.Tasks[3].History[0].ID != "t1-m2" {
		t.Fatalf("history 1 = %v, want the last message", resp.Tasks[3].History)
	}
	// Artifacts dropped unless requested.
	resp = list(&a2a.ListTasksRequest{ContextID: "c1", PageSize: 100})
	for _, task := range resp.Tasks {
		if string(task.ID) == "t1" && len(task.Artifacts) != 0 {
			t.Fatalf("artifacts not dropped: %+v", task.Artifacts)
		}
	}
	resp = list(&a2a.ListTasksRequest{ContextID: "c1", PageSize: 100, IncludeArtifacts: true})
	var found bool
	for _, task := range resp.Tasks {
		if string(task.ID) == "t1" {
			found = len(task.Artifacts) == 1
		}
	}
	if !found {
		t.Fatal("artifacts missing with IncludeArtifacts")
	}
}

// TestListUnauthenticated proves List without an authenticated subject
// is a2a.ErrUnauthenticated: a failing owner and the default owner
// without a call context.
func TestListUnauthenticated(t *testing.T) {
	clk := &clock{t: testNow}
	s, _ := openTest(t, clk, "")
	_, err := s.List(context.Background(), &a2a.ListTasksRequest{})
	if !errors.Is(err, a2a.ErrUnauthenticated) {
		t.Fatalf("List with an empty owner: %v, want unauthenticated", err)
	}
	// The default owner: a store opened without an Owner option uses the
	// SDK call context.
	path := t.TempDir() + "/registry.db"
	s2, err := OpenFile(path, Options{Now: clk.Now})
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer s2.Close()
	_, err = s2.List(context.Background(), &a2a.ListTasksRequest{})
	if !errors.Is(err, a2a.ErrUnauthenticated) {
		t.Fatalf("List without a call context: %v, want unauthenticated", err)
	}
	// The default owner: a call context with an authenticated user works.
	ctx, cc := a2asrv.NewCallContext(context.Background(), nil)
	cc.User = a2asrv.NewAuthenticatedUser("alice", nil)
	if _, err := s2.Create(ctx, mkTask("t1", "c1", a2a.TaskStateSubmitted, 1)); err != nil {
		t.Fatalf("Create: %v", err)
	}
	resp, err := s2.List(ctx, &a2a.ListTasksRequest{})
	if err != nil || len(resp.Tasks) != 1 {
		t.Fatalf("List with a call context user: %v, %v", resp, err)
	}
}

// pageTokenFor is a test page token built from raw parts (not the
// store's encoder): "<nano>_<task id>".
func pageTokenFor(nano, taskID string) string {
	return base64.URLEncoding.EncodeToString([]byte(nano + "_" + taskID))
}
