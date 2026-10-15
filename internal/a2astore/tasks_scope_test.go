package a2astore

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv/taskstore"

	"github.com/djalmajr/nau/internal/a2acore"
)

// tsOnly is a visibility function that shows exactly the given projects.
func tsOnly(projects ...string) func(string) bool {
	return func(p string) bool {
		for _, v := range projects {
			if v == p {
				return true
			}
		}
		return false
	}
}

// tsCreate stores one task in a project (the task metadata decides the
// stored project column when no admission exists), advancing the clock
// so the update order is the creation order.
func tsCreate(t *testing.T, s *Store, clk *clock, id, project string) {
	t.Helper()
	task := mkTask(id, "ctx-"+id, a2a.TaskStateWorking, 2)
	task.Metadata = map[string]any{a2acore.MetaProject: project}
	if _, err := s.Create(context.Background(), withArtifact(task, "art-"+id)); err != nil {
		t.Fatalf("Create %s: %v", id, err)
	}
	clk.Add(time.Second)
}

// tsIDs is the task ids of a list response, in order.
func tsIDs(res *a2a.ListTasksResponse) []a2a.TaskID {
	ids := make([]a2a.TaskID, 0, len(res.Tasks))
	for _, task := range res.Tasks {
		ids = append(ids, task.ID)
	}
	return ids
}

// TestTaskScopeGetVisibleFiltersByColumn: a row whose stored project is
// not visible is task-not-found, the same error as a missing task.
func TestTaskScopeGetVisibleFiltersByColumn(t *testing.T) {
	clk := &clock{t: testNow}
	s, _ := openTest(t, clk, "alice")
	tsCreate(t, s, clk, "t-secret", "org/secret")
	tsCreate(t, s, clk, "t-public", "org/public")
	ctx := context.Background()
	visible := tsOnly("org/public")

	if got, err := s.GetVisible(ctx, "t-public", visible); err != nil || got.Task.ID != "t-public" || got.User != "alice" || got.Version != 1 {
		t.Fatalf("GetVisible visible = %+v, %v", got, err)
	}
	_, errHidden := s.GetVisible(ctx, "t-secret", visible)
	_, errMissing := s.GetVisible(ctx, "t-missing", visible)
	if !errors.Is(errHidden, a2a.ErrTaskNotFound) {
		t.Fatalf("GetVisible hidden err = %v, want task not found", errHidden)
	}
	if errHidden.Error() != errMissing.Error() || !errors.Is(errMissing, a2a.ErrTaskNotFound) {
		t.Errorf("hidden err %q differs from missing err %q", errHidden, errMissing)
	}
	// The visible function receives the stored project.
	var seen []string
	if _, err := s.GetVisible(ctx, "t-secret", func(p string) bool { seen = append(seen, p); return true }); err != nil {
		t.Fatalf("GetVisible recording: %v", err)
	}
	if len(seen) != 1 || seen[0] != "org/secret" {
		t.Errorf("visible called with %v, want [org/secret]", seen)
	}
}

// TestTaskScopeOwnerMaskingStaysFirst: another subject's task is task
// not found whatever the visibility function says, and the function is
// not consulted for it.
func TestTaskScopeOwnerMaskingStaysFirst(t *testing.T) {
	clk := &clock{t: testNow}
	s, path := openTest(t, clk, "alice")
	tsCreate(t, s, clk, "t-1", "org/public")
	bob, err := OpenFile(path, Options{Now: clk.Now, Owner: fixedOwner("bob")})
	if err != nil {
		t.Fatalf("open bob: %v", err)
	}
	t.Cleanup(func() { bob.Close() })
	called := false
	_, err = bob.GetVisible(context.Background(), "t-1", func(string) bool { called = true; return true })
	if !errors.Is(err, a2a.ErrTaskNotFound) {
		t.Errorf("another subject's GetVisible err = %v, want task not found", err)
	}
	if called {
		t.Errorf("visible was consulted for another subject's task")
	}
	res, err := bob.ListVisible(context.Background(), &a2a.ListTasksRequest{}, func(string) bool { return true })
	if err != nil || len(res.Tasks) != 0 || res.TotalSize != 0 {
		t.Errorf("another subject's ListVisible = %+v, %v, want none", res, err)
	}
}

// TestTaskScopeColumnDecidesNotMetadata: an Update whose task JSON
// carries another nau.project value does not change visibility.
func TestTaskScopeColumnDecidesNotMetadata(t *testing.T) {
	clk := &clock{t: testNow}
	s, _ := openTest(t, clk, "alice")
	tsCreate(t, s, clk, "t-secret", "org/secret")
	tsCreate(t, s, clk, "t-public", "org/public")
	ctx := context.Background()

	for id, forged := range map[a2a.TaskID]string{"t-secret": "org/public", "t-public": "org/secret"} {
		stored, err := s.Get(ctx, id)
		if err != nil {
			t.Fatalf("Get %s: %v", id, err)
		}
		stored.Task.Metadata[a2acore.MetaProject] = forged
		if _, err := s.Update(ctx, &taskstore.UpdateRequest{Task: stored.Task, PrevVersion: stored.Version}); err != nil {
			t.Fatalf("Update %s: %v", id, err)
		}
	}
	visible := tsOnly("org/public")
	if _, err := s.GetVisible(ctx, "t-secret", visible); !errors.Is(err, a2a.ErrTaskNotFound) {
		t.Errorf("t-secret with forged public metadata err = %v, want task not found", err)
	}
	if got, err := s.GetVisible(ctx, "t-public", visible); err != nil || got.Task.Metadata[a2acore.MetaProject] != "org/secret" {
		t.Errorf("t-public with forged secret metadata = %+v, %v, want visible", got, err)
	}
	res, err := s.ListVisible(ctx, &a2a.ListTasksRequest{}, visible)
	if err != nil {
		t.Fatalf("ListVisible: %v", err)
	}
	if ids := tsIDs(res); len(ids) != 1 || ids[0] != "t-public" || res.TotalSize != 1 {
		t.Errorf("ListVisible ids = %v total = %d, want only t-public", ids, res.TotalSize)
	}
}

// TestTaskScopeListVisibleCountAndPaging: invisible rows are dropped
// before the timestamp filter, the TotalSize count and the page cut.
func TestTaskScopeListVisibleCountAndPaging(t *testing.T) {
	clk := &clock{t: testNow}
	s, _ := openTest(t, clk, "alice")
	// Oldest to newest: v1, h1, v2, h2, v3 (h = hidden); newest first
	// the order is v3, h2, v2, h1, v1.
	tsCreate(t, s, clk, "v1", "org/public")
	tsCreate(t, s, clk, "h1", "org/secret")
	tsCreate(t, s, clk, "v2", "org/public")
	tsCreate(t, s, clk, "h2", "org/secret")
	tsCreate(t, s, clk, "v3", "org/public")
	ctx := context.Background()
	visible := tsOnly("org/public")

	res, err := s.ListVisible(ctx, &a2a.ListTasksRequest{}, visible)
	if err != nil {
		t.Fatalf("ListVisible: %v", err)
	}
	if ids := tsIDs(res); len(ids) != 3 || ids[0] != "v3" || ids[1] != "v2" || ids[2] != "v1" || res.TotalSize != 3 || res.NextPageToken != "" {
		t.Fatalf("ListVisible = %v total %d token %q, want v3 v2 v1 total 3", ids, res.TotalSize, res.NextPageToken)
	}

	// Page size 1: every page holds one visible task, TotalSize stays
	// the visible count, and the last page has no token.
	var pages []a2a.TaskID
	req := &a2a.ListTasksRequest{PageSize: 1}
	for i := 0; i < 10; i++ {
		page, err := s.ListVisible(ctx, req, visible)
		if err != nil {
			t.Fatalf("ListVisible page %d: %v", i, err)
		}
		if page.TotalSize != 3 {
			t.Errorf("page %d TotalSize = %d, want 3", i, page.TotalSize)
		}
		pages = append(pages, tsIDs(page)...)
		if page.NextPageToken == "" {
			break
		}
		next := *req
		next.PageToken = page.NextPageToken
		req = &next
	}
	if len(pages) != 3 || pages[0] != "v3" || pages[1] != "v2" || pages[2] != "v1" {
		t.Errorf("paged ids = %v, want v3 v2 v1", pages)
	}

	// A page boundary that falls before hidden rows resumes at the next visible one.
	first, err := s.ListVisible(ctx, &a2a.ListTasksRequest{PageSize: 2}, visible)
	if err != nil {
		t.Fatalf("ListVisible first: %v", err)
	}
	if ids := tsIDs(first); len(ids) != 2 || first.NextPageToken == "" {
		t.Fatalf("first page = %v token %q, want 2 tasks and a token", ids, first.NextPageToken)
	}
	second, err := s.ListVisible(ctx, &a2a.ListTasksRequest{PageSize: 2, PageToken: first.NextPageToken}, visible)
	if err != nil {
		t.Fatalf("ListVisible second: %v", err)
	}
	if ids := tsIDs(second); len(ids) != 1 || ids[0] != "v1" || second.NextPageToken != "" || second.TotalSize != 3 {
		t.Errorf("second page = %v token %q total %d, want v1 only", ids, second.NextPageToken, second.TotalSize)
	}
}

// TestTaskScopeListVisibleBeforeTimestampFilter: the hidden rows are
// dropped before the StatusTimestampAfter filter; the filter still
// applies to the visible rows.
func TestTaskScopeListVisibleBeforeTimestampFilter(t *testing.T) {
	clk := &clock{t: testNow}
	s, _ := openTest(t, clk, "alice")
	ctx := context.Background()
	stamped := func(id, project string, ts time.Time) {
		task := mkTask(id, "ctx-"+id, a2a.TaskStateWorking, 1)
		task.Metadata = map[string]any{a2acore.MetaProject: project}
		task.Status.Timestamp = &ts
		if _, err := s.Create(ctx, task); err != nil {
			t.Fatalf("Create %s: %v", id, err)
		}
		clk.Add(time.Second)
	}
	old := testNow.Add(-time.Hour)
	recent := testNow.Add(time.Hour)
	stamped("v-old", "org/public", old)
	stamped("v-new", "org/public", recent)
	stamped("h-new", "org/secret", recent)
	after := testNow
	res, err := s.ListVisible(ctx, &a2a.ListTasksRequest{StatusTimestampAfter: &after}, tsOnly("org/public"))
	if err != nil {
		t.Fatalf("ListVisible: %v", err)
	}
	if ids := tsIDs(res); len(ids) != 1 || ids[0] != "v-new" || res.TotalSize != 1 {
		t.Errorf("ListVisible = %v total %d, want only v-new", ids, res.TotalSize)
	}
}

// TestTaskScopeListVisibleHistoryArtifactsFilters: the history length,
// artifacts, context and status filters keep working on visible rows
// and never surface a hidden task.
func TestTaskScopeListVisibleHistoryArtifactsFilters(t *testing.T) {
	clk := &clock{t: testNow}
	s, _ := openTest(t, clk, "alice")
	tsCreate(t, s, clk, "v1", "org/public")
	tsCreate(t, s, clk, "h1", "org/secret")
	ctx := context.Background()
	visible := tsOnly("org/public")
	one := 1
	res, err := s.ListVisible(ctx, &a2a.ListTasksRequest{HistoryLength: &one, IncludeArtifacts: true}, visible)
	if err != nil {
		t.Fatalf("ListVisible: %v", err)
	}
	if len(res.Tasks) != 1 || res.Tasks[0].ID != "v1" || len(res.Tasks[0].History) != 1 || len(res.Tasks[0].Artifacts) != 1 {
		t.Errorf("ListVisible = %+v, want v1 with 1 history message and its artifact", res.Tasks)
	}
	res, err = s.ListVisible(ctx, &a2a.ListTasksRequest{ContextID: "ctx-h1"}, visible)
	if err != nil || len(res.Tasks) != 0 || res.TotalSize != 0 {
		t.Errorf("ListVisible by the hidden context = %+v, %v, want none", res, err)
	}
	res, err = s.ListVisible(ctx, &a2a.ListTasksRequest{Status: a2a.TaskStateWorking}, visible)
	if err != nil || len(res.Tasks) != 1 || res.Tasks[0].ID != "v1" {
		t.Errorf("ListVisible by status = %+v, %v, want only v1", res, err)
	}
}

// TestTaskScopeNilVisibleKeepsBehavior: a nil visibility function is no
// project filter, and Get and List are the nil forms.
func TestTaskScopeNilVisibleKeepsBehavior(t *testing.T) {
	clk := &clock{t: testNow}
	s, _ := openTest(t, clk, "alice")
	tsCreate(t, s, clk, "t-secret", "org/secret")
	tsCreate(t, s, clk, "t-public", "org/public")
	ctx := context.Background()
	for _, id := range []a2a.TaskID{"t-secret", "t-public"} {
		viaNil, err := s.GetVisible(ctx, id, nil)
		if err != nil {
			t.Fatalf("GetVisible nil %s: %v", id, err)
		}
		plain, err := s.Get(ctx, id)
		if err != nil {
			t.Fatalf("Get %s: %v", id, err)
		}
		if viaNil.Task.ID != plain.Task.ID || viaNil.Version != plain.Version || viaNil.User != plain.User {
			t.Errorf("GetVisible nil %+v differs from Get %+v", viaNil, plain)
		}
	}
	viaNil, err := s.ListVisible(ctx, &a2a.ListTasksRequest{}, nil)
	if err != nil {
		t.Fatalf("ListVisible nil: %v", err)
	}
	plain, err := s.List(ctx, &a2a.ListTasksRequest{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	a, b := tsIDs(viaNil), tsIDs(plain)
	if len(a) != 2 || len(a) != len(b) || a[0] != b[0] || a[1] != b[1] || viaNil.TotalSize != 2 || plain.TotalSize != 2 {
		t.Errorf("ListVisible nil = %v total %d, List = %v total %d, want the same two tasks", a, viaNil.TotalSize, b, plain.TotalSize)
	}
	if _, err := s.ListVisible(ctx, nil, nil); !errors.Is(err, a2a.ErrInvalidRequest) {
		t.Errorf("ListVisible nil request err = %v, want invalid request", err)
	}
}
