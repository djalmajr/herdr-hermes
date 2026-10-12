package a2aserver

import (
	"context"
	"errors"
	"net/http"
	"regexp"
	"strings"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv/taskstore"

	"github.com/djalmajr/nau/internal/a2acore"
	"github.com/djalmajr/nau/internal/a2astore"
)

// f2PushToken is the obvious fixture push token that must never leak.
const f2PushToken = "fixture-push-token"

// f2Fix is one partial-grant fixture: a subject that owns org/secret
// and org/public, a task in each (S and P), a push config with the
// fixture token on each, and a second subject that owns org/other.
type f2Fix struct {
	e       *idfxEnv
	tok     string
	c       *Client
	tokB    string
	cB      *Client
	s, p    *a2a.Task
	pushS   *a2a.PushConfig
	pushP   *a2a.PushConfig
	foreign *a2a.Task
}

// f2Base builds the environment and the two clients with no task.
func f2Base(t *testing.T) *f2Fix {
	t.Helper()
	e := newIdfxEnv(t, nil)
	e.routes.set("owner-ref-a", "org/secret", "org/public")
	e.routes.set("owner-ref-b", "org/other")
	f := &f2Fix{e: e}
	f.tok = e.ids.add(t, "svc-a", "owner-ref-a")
	f.tokB = e.ids.add(t, "svc-b", "owner-ref-b")
	f.c = e.client(f.tok)
	f.cB = e.client(f.tokB)
	return f
}

// f2Setup builds the full fixture; routing still grants both projects.
func f2Setup(t *testing.T) *f2Fix {
	t.Helper()
	f := f2Base(t)
	f.s = sendTask(t, f.c, idfxSend("org/secret", "s-1", "m-1"))
	f.p = sendTask(t, f.c, idfxSend("org/public", "s-2", "m-2"))
	f.foreign = sendTask(t, f.cB, idfxSend("org/other", "s-3", "m-3"))
	f.pushS = f2CreatePush(t, f.c, f.s)
	f.pushP = f2CreatePush(t, f.c, f.p)
	return f
}

// f2CreatePush registers one push config with the fixture token.
func f2CreatePush(t *testing.T, c *Client, task *a2a.Task) *a2a.PushConfig {
	t.Helper()
	saved, err := c.client.CreateTaskPushConfig(ctxT(t), &a2a.PushConfig{
		TaskID: task.ID,
		URL:    "http://127.0.0.1:9/hook",
		Token:  f2PushToken,
	})
	if err != nil {
		t.Fatalf("create push config: %v", err)
	}
	return saved
}

// f2Revoke drops org/secret from the subject's routing, keeping
// org/public.
func (f *f2Fix) f2Revoke() { f.e.routes.set("owner-ref-a", "org/public") }

// f2Restore gives org/secret back.
func (f *f2Fix) f2Restore() { f.e.routes.set("owner-ref-a", "org/secret", "org/public") }

// f2Ops is one named task operation on a task as one client.
type f2Op struct {
	name string
	run  func(t *testing.T, c *Client, task *a2a.Task, cfg *a2a.PushConfig) error
}

// f2TaskOps is the task-scoped operation set: get, cancel and the four
// push-config methods.
func f2TaskOps() []f2Op {
	return []f2Op{
		{"get", func(t *testing.T, c *Client, task *a2a.Task, _ *a2a.PushConfig) error {
			_, err := c.Get(ctxT(t), string(task.ID))
			return err
		}},
		{"cancel", func(t *testing.T, c *Client, task *a2a.Task, _ *a2a.PushConfig) error {
			_, err := c.Cancel(ctxT(t), string(task.ID))
			return err
		}},
		{"push create", func(t *testing.T, c *Client, task *a2a.Task, _ *a2a.PushConfig) error {
			_, err := c.client.CreateTaskPushConfig(ctxT(t), &a2a.PushConfig{TaskID: task.ID, URL: "http://127.0.0.1:9/other", Token: f2PushToken})
			return err
		}},
		{"push get", func(t *testing.T, c *Client, task *a2a.Task, cfg *a2a.PushConfig) error {
			got, err := c.client.GetTaskPushConfig(ctxT(t), &a2a.GetTaskPushConfigRequest{TaskID: task.ID, ID: cfg.ID})
			if err == nil && got == nil {
				return errors.New("nil push config without error")
			}
			return err
		}},
		{"push list", func(t *testing.T, c *Client, task *a2a.Task, _ *a2a.PushConfig) error {
			_, err := c.client.ListTaskPushConfigs(ctxT(t), &a2a.ListTaskPushConfigRequest{TaskID: task.ID})
			return err
		}},
		{"push delete", func(t *testing.T, c *Client, task *a2a.Task, cfg *a2a.PushConfig) error {
			return c.client.DeleteTaskPushConfig(ctxT(t), &a2a.DeleteTaskPushConfigRequest{TaskID: task.ID, ID: cfg.ID})
		}},
	}
}

// f2ListIDs pages through the whole list with the given page size and
// returns every task id, the first page's TotalSize and the last
// NextPageToken.
func f2ListIDs(t *testing.T, c *Client, base a2a.ListTasksRequest) (ids []a2a.TaskID, total int, last string) {
	t.Helper()
	req := base
	for i := 0; i < 20; i++ {
		res, err := c.client.ListTasks(ctxT(t), &req)
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		if i == 0 {
			total = res.TotalSize
		}
		for _, task := range res.Tasks {
			ids = append(ids, task.ID)
		}
		last = res.NextPageToken
		if last == "" {
			return ids, total, last
		}
		req.PageToken = last
	}
	t.Fatalf("list did not end within 20 pages")
	return nil, 0, ""
}

// f2StampRE matches the second-resolution error timestamp of a body.
var f2StampRE = regexp.MustCompile(`"timestamp":"[^"]*"`)

// f2Stamp blanks the error timestamp so two bodies built in different
// seconds still compare.
func f2Stamp(body string) string { return f2StampRE.ReplaceAllString(body, `"timestamp":""`) }

func f2Has(ids []a2a.TaskID, id a2a.TaskID) bool {
	for _, got := range ids {
		if got == id {
			return true
		}
	}
	return false
}

// TestPartialGrantRemovedProjectTaskIsNotFound: every task-scoped
// operation on a task in the removed project is task-not-found for the
// revoked owner, exactly as for another subject, and the raw status of
// a task GET is the same.
func TestPartialGrantRemovedProjectTaskIsNotFound(t *testing.T) {
	for _, op := range f2TaskOps() {
		t.Run(op.name, func(t *testing.T) {
			f := f2Setup(t)
			f.f2Revoke()
			if err := op.run(t, f.c, f.s, f.pushS); !errors.Is(err, a2a.ErrTaskNotFound) {
				t.Errorf("revoked owner %s err = %v, want task not found", op.name, err)
			}
			if err := op.run(t, f.cB, f.s, f.pushS); !errors.Is(err, a2a.ErrTaskNotFound) {
				t.Errorf("second subject %s err = %v, want task not found", op.name, err)
			}
		})
	}
	t.Run("raw status", func(t *testing.T) {
		f := f2Setup(t)
		f.f2Revoke()
		own := "Bearer " + f.tok
		other := "Bearer " + f.tokB
		path := "/tasks/" + string(f.s.ID)
		ownStatus, ownBody := f.e.rawGet(path, &own)
		otherStatus, otherBody := f.e.rawGet(path, &other)
		if ownStatus != otherStatus || ownStatus != http.StatusNotFound {
			t.Errorf("raw GET status revoked owner = %d, second subject = %d, want both 404", ownStatus, otherStatus)
		}
		if f2Stamp(ownBody) != f2Stamp(otherBody) {
			t.Errorf("raw GET body differs: owner %q, second subject %q", ownBody, otherBody)
		}
		missing, _ := f.e.rawGet("/tasks/no-such-task", &own)
		if missing != ownStatus {
			t.Errorf("missing task status = %d, removed-project status = %d, want equal", missing, ownStatus)
		}
	})
}

// TestPartialGrantList: the removed project's task never appears in a
// list, TotalSize counts only visible tasks and paging never yields it.
func TestPartialGrantList(t *testing.T) {
	f := f2Setup(t)
	// A third visible task created after S so that S sits between two
	// visible ones in the update order.
	p2 := sendTask(t, f.c, idfxSend("org/public", "s-4", "m-4"))
	f.f2Revoke()

	ids, total, last := f2ListIDs(t, f.c, a2a.ListTasksRequest{})
	if f2Has(ids, f.s.ID) {
		t.Errorf("list returned the removed-project task %s", f.s.ID)
	}
	if total != 2 || len(ids) != 2 || !f2Has(ids, f.p.ID) || !f2Has(ids, p2.ID) {
		t.Errorf("list ids = %v total = %d, want exactly P and P2 with total 2", ids, total)
	}
	if last != "" {
		t.Errorf("last next page token = %q, want empty", last)
	}

	ids, total, last = f2ListIDs(t, f.c, a2a.ListTasksRequest{PageSize: 1})
	if f2Has(ids, f.s.ID) {
		t.Errorf("paged list returned the removed-project task %s", f.s.ID)
	}
	if total != 2 || len(ids) != 2 {
		t.Errorf("paged list ids = %v total = %d, want 2 pages of the visible tasks", ids, total)
	}
	if last != "" {
		t.Errorf("paged list ended with token %q, want empty", last)
	}

	zero, ten := 0, 10
	for name, req := range map[string]a2a.ListTasksRequest{
		"history 0":  {HistoryLength: &zero},
		"history 10": {HistoryLength: &ten},
		"artifacts":  {IncludeArtifacts: true},
		"both":       {HistoryLength: &ten, IncludeArtifacts: true, PageSize: 1},
	} {
		ids, total, _ := f2ListIDs(t, f.c, req)
		if f2Has(ids, f.s.ID) || total != 2 {
			t.Errorf("%s: ids = %v total = %d, want S absent and total 2", name, ids, total)
		}
	}

	// A filter by the context of S matches nothing for the revoked owner.
	res, err := f.c.client.ListTasks(ctxT(t), &a2a.ListTasksRequest{ContextID: f.s.ContextID})
	if err != nil {
		t.Fatalf("list by context: %v", err)
	}
	if len(res.Tasks) != 0 || res.TotalSize != 0 {
		t.Errorf("list by the context of S = %d tasks, total %d, want none", len(res.Tasks), res.TotalSize)
	}
}

// TestPartialGrantNoPushSecretAfterRevocation: no response body after
// the revocation carries the push token, on the raw routes or through
// the SDK.
func TestPartialGrantNoPushSecretAfterRevocation(t *testing.T) {
	f := f2Setup(t)
	f.f2Revoke()
	own := "Bearer " + f.tok
	other := "Bearer " + f.tokB
	base := "/tasks/" + string(f.s.ID)
	for _, path := range []string{
		base,
		base + "/pushNotificationConfigs",
		base + "/pushNotificationConfigs/" + f.pushS.ID,
		"/tasks",
	} {
		status, body := f.e.rawGet(path, &own)
		if strings.Contains(body, f2PushToken) {
			t.Errorf("GET %s body carries the push token: %s", path, body)
		}
		if path != "/tasks" {
			otherStatus, otherBody := f.e.rawGet(path, &other)
			if status != http.StatusNotFound || status != otherStatus || f2Stamp(body) != f2Stamp(otherBody) {
				t.Errorf("GET %s: owner %d %q, second subject %d %q, want identical 404", path, status, body, otherStatus, otherBody)
			}
		}
	}
	if got, err := f.c.client.GetTaskPushConfig(ctxT(t), &a2a.GetTaskPushConfigRequest{TaskID: f.s.ID, ID: f.pushS.ID}); err == nil {
		t.Errorf("push get succeeded with token %q", got.Token)
	}
	if got, err := f.c.client.ListTaskPushConfigs(ctxT(t), &a2a.ListTaskPushConfigRequest{TaskID: f.s.ID}); err == nil {
		for _, pc := range got {
			if pc.Token == f2PushToken {
				t.Errorf("push list returned the push token")
			}
		}
		t.Errorf("push list succeeded with %d configs", len(got))
	}
}

// TestPartialGrantContinuation: a continuation of a task whose project
// left the grant is task-not-found and consumes no fence.
func TestPartialGrantContinuation(t *testing.T) {
	// Only S is created here: idfxAsk waits for no other live execution.
	f := f2Base(t)
	f.s = sendTask(t, f.c, idfxSend("org/secret", "s-1", "m-1"))
	idfxAsk(t, f.e, f.c, string(f.s.ID), "d-1")
	f.f2Revoke()
	in := SendInput{
		TaskID: string(f.s.ID), ContextID: f.s.ContextID,
		Session: "s-1", Epoch: 1, Seq: 2, MessageID: "m-5", Text: "continuation",
	}
	if _, err := f.c.Send(ctxT(t), in); !errors.Is(err, a2a.ErrTaskNotFound) {
		t.Fatalf("continuation err = %v, want task not found", err)
	}
	if _, err := f.cB.Send(ctxT(t), in); !errors.Is(err, a2a.ErrTaskNotFound) {
		t.Errorf("second subject continuation err = %v, want task not found", err)
	}
	f.f2Restore()
	in.MessageID = "m-6"
	res, err := f.c.Send(ctxT(t), in)
	if err != nil {
		t.Fatalf("continuation after restore: %v", err)
	}
	got, ok := res.(*a2a.Task)
	if !ok || got.ID != f.s.ID || got.Status.State != a2a.TaskStateWorking {
		t.Errorf("continuation after restore = %T %+v, want the same task working (fence not consumed)", res, got)
	}
}

// f2Subscribe runs one subscribe and returns the event and error of
// its first element.
func f2Subscribe(t *testing.T, c *Client, id a2a.TaskID) (a2a.Event, error) {
	t.Helper()
	for ev, err := range c.client.SubscribeToTask(ctxT(t), &a2a.SubscribeToTaskRequest{ID: id}) {
		return ev, err
	}
	return nil, errors.New("subscribe yielded nothing")
}

// TestPartialGrantSubscribe: subscribe on a removed-project task, on a
// granted task and on a foreign task is the same error class and
// carries no task data.
func TestPartialGrantSubscribe(t *testing.T) {
	f := f2Setup(t)
	f.f2Revoke()
	var first error
	for name, id := range map[string]a2a.TaskID{"removed project": f.s.ID, "granted": f.p.ID, "foreign": f.foreign.ID, "missing": "no-such-task"} {
		ev, err := f2Subscribe(t, f.c, id)
		if err == nil {
			t.Fatalf("%s: subscribe succeeded with %v", name, ev)
		}
		if ev != nil {
			t.Errorf("%s: subscribe returned task data %v", name, ev)
		}
		if !errors.Is(err, a2a.ErrUnsupportedOperation) {
			t.Errorf("%s: subscribe err = %v, want unsupported operation", name, err)
		}
		if first == nil {
			first = err
		} else if err.Error() != first.Error() {
			t.Errorf("%s: subscribe err text %q differs from %q", name, err.Error(), first.Error())
		}
	}
}

// TestPartialGrantAllowedProjectControls: the still-granted project
// keeps working after the revocation.
func TestPartialGrantAllowedProjectControls(t *testing.T) {
	f := f2Setup(t)
	f.f2Revoke()
	got, err := f.c.Get(ctxT(t), string(f.p.ID))
	if err != nil || got.ID != f.p.ID {
		t.Fatalf("get P = %+v, %v", got, err)
	}
	ids, total, _ := f2ListIDs(t, f.c, a2a.ListTasksRequest{})
	if !f2Has(ids, f.p.ID) || total != 1 || len(ids) != 1 {
		t.Errorf("list ids = %v total = %d, want only P", ids, total)
	}
	created, err := f.c.client.CreateTaskPushConfig(ctxT(t), &a2a.PushConfig{TaskID: f.p.ID, URL: "http://127.0.0.1:9/second", Token: f2PushToken})
	if err != nil {
		t.Fatalf("push create on P: %v", err)
	}
	if pc, err := f.c.client.GetTaskPushConfig(ctxT(t), &a2a.GetTaskPushConfigRequest{TaskID: f.p.ID, ID: f.pushP.ID}); err != nil || pc.ID != f.pushP.ID {
		t.Errorf("push get on P = %+v, %v", pc, err)
	}
	if list, err := f.c.client.ListTaskPushConfigs(ctxT(t), &a2a.ListTaskPushConfigRequest{TaskID: f.p.ID}); err != nil || len(list) != 2 {
		t.Errorf("push list on P = %d configs, %v, want 2", len(list), err)
	}
	if err := f.c.client.DeleteTaskPushConfig(ctxT(t), &a2a.DeleteTaskPushConfigRequest{TaskID: f.p.ID, ID: created.ID}); err != nil {
		t.Errorf("push delete on P: %v", err)
	}
	cancelled, err := f.c.Cancel(ctxT(t), string(f.p.ID))
	if err != nil || cancelled.Status.State != a2a.TaskStateCanceled {
		t.Errorf("cancel P = %+v, %v, want canceled", cancelled, err)
	}
}

// TestPartialGrantRestoreRestoresAccess: giving the project back gives
// the task back.
func TestPartialGrantRestoreRestoresAccess(t *testing.T) {
	f := f2Setup(t)
	f.f2Revoke()
	if _, err := f.c.Get(ctxT(t), string(f.s.ID)); !errors.Is(err, a2a.ErrTaskNotFound) {
		t.Fatalf("get while revoked err = %v, want task not found (setup)", err)
	}
	f.f2Restore()
	if got, err := f.c.Get(ctxT(t), string(f.s.ID)); err != nil || got.ID != f.s.ID {
		t.Errorf("get after restore = %+v, %v", got, err)
	}
	ids, total, _ := f2ListIDs(t, f.c, a2a.ListTasksRequest{})
	if !f2Has(ids, f.s.ID) || !f2Has(ids, f.p.ID) || total != 2 {
		t.Errorf("list after restore ids = %v total = %d, want S and P", ids, total)
	}
	if pc, err := f.c.client.GetTaskPushConfig(ctxT(t), &a2a.GetTaskPushConfigRequest{TaskID: f.s.ID, ID: f.pushS.ID}); err != nil || pc.Token != f2PushToken {
		t.Errorf("push get after restore = %+v, %v, want the owner's own config", pc, err)
	}
	if list, err := f.c.client.ListTaskPushConfigs(ctxT(t), &a2a.ListTaskPushConfigRequest{TaskID: f.s.ID}); err != nil || len(list) != 1 {
		t.Errorf("push list after restore = %d configs, %v, want 1", len(list), err)
	}
}

// TestPartialGrantAnyProjectSeesOnlyOwnTasks: AnyProject means all of
// the subject's own tasks, never another subject's: the local operator
// and a guardian reference.
func TestPartialGrantAnyProjectSeesOnlyOwnTasks(t *testing.T) {
	f := f2Setup(t)
	f.e.routes.guardian = "guardian-ref"
	tokG := f.e.ids.add(t, "svc-g", "guardian-ref")
	f.e.routes.set("owner-ref-l", "org/local")
	cases := []struct {
		name   string
		client *Client
	}{
		{"local operator", f.e.client("")},
		{"guardian", f.e.client(tokG)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			own := sendTask(t, tc.client, idfxSend("org/local", "s-"+strings.ReplaceAll(tc.name, " ", "-"), "m-"+strings.ReplaceAll(tc.name, " ", "-")))
			if got, err := tc.client.Get(ctxT(t), string(own.ID)); err != nil || got.ID != own.ID {
				t.Fatalf("own task get = %+v, %v", got, err)
			}
			for _, other := range []*a2a.Task{f.s, f.p, f.foreign} {
				if _, err := tc.client.Get(ctxT(t), string(other.ID)); !errors.Is(err, a2a.ErrTaskNotFound) {
					t.Errorf("get of another subject's task %s err = %v, want task not found", other.ID, err)
				}
				if _, err := tc.client.Cancel(ctxT(t), string(other.ID)); !errors.Is(err, a2a.ErrTaskNotFound) {
					t.Errorf("cancel of another subject's task %s err = %v, want task not found", other.ID, err)
				}
			}
			ids, total, _ := f2ListIDs(t, tc.client, a2a.ListTasksRequest{})
			if len(ids) != 1 || ids[0] != own.ID || total != 1 {
				t.Errorf("list ids = %v total = %d, want only the subject's own task %s", ids, total, own.ID)
			}
		})
	}
}

// TestPartialGrantScopedStoreFailsClosedWithoutScope: without a project
// scope on the context Get is task-not-found and List is
// unauthenticated, while Create and Update delegate unchanged.
func TestPartialGrantScopedStoreFailsClosedWithoutScope(t *testing.T) {
	store, _ := openTestStore(t, a2astore.Options{Owner: func(context.Context) (string, error) { return "svc-a", nil }})
	scoped := scopedTaskStore{store: store}
	ctx := context.Background()
	task := &a2a.Task{
		ID: "f2-task", ContextID: "f2-ctx",
		Status:   a2a.TaskStatus{State: a2a.TaskStateWorking},
		Metadata: map[string]any{a2acore.MetaProject: "org/public"},
	}
	version, err := scoped.Create(ctx, task)
	if err != nil || version != 1 {
		t.Fatalf("Create = %d, %v, want 1 nil", version, err)
	}
	if _, err := scoped.Create(ctx, task); !errors.Is(err, taskstore.ErrTaskAlreadyExists) {
		t.Errorf("duplicate Create err = %v, want the store's already exists", err)
	}
	updated := *task
	updated.Status.State = a2a.TaskStateInputRequired
	version, err = scoped.Update(ctx, &taskstore.UpdateRequest{Task: &updated, PrevVersion: 1})
	if err != nil || version != 2 {
		t.Fatalf("Update = %d, %v, want 2 nil", version, err)
	}
	if _, err := scoped.Update(ctx, &taskstore.UpdateRequest{Task: &updated, PrevVersion: 1}); !errors.Is(err, taskstore.ErrConcurrentModification) {
		t.Errorf("stale Update err = %v, want the store's concurrent modification", err)
	}

	if _, err := scoped.Get(ctx, "f2-task"); !errors.Is(err, a2a.ErrTaskNotFound) {
		t.Errorf("Get without scope err = %v, want task not found", err)
	}
	if res, err := scoped.List(ctx, &a2a.ListTasksRequest{}); !errors.Is(err, a2a.ErrUnauthenticated) || res != nil {
		t.Errorf("List without scope = %+v, %v, want unauthenticated", res, err)
	}

	// With a scope the same calls read through the store.
	scopedCtx := withProjectScope(ctx, func(p string) bool { return p == "org/public" })
	if got, err := scoped.Get(scopedCtx, "f2-task"); err != nil || got.Version != 2 || got.Task.Status.State != a2a.TaskStateInputRequired {
		t.Errorf("Get with scope = %+v, %v, want the updated task", got, err)
	}
	if res, err := scoped.List(scopedCtx, &a2a.ListTasksRequest{}); err != nil || len(res.Tasks) != 1 {
		t.Errorf("List with scope = %+v, %v, want one task", res, err)
	}
	hidden := withProjectScope(ctx, func(string) bool { return false })
	if _, err := scoped.Get(hidden, "f2-task"); !errors.Is(err, a2a.ErrTaskNotFound) {
		t.Errorf("Get with an empty scope err = %v, want task not found", err)
	}
	if res, err := scoped.List(hidden, &a2a.ListTasksRequest{}); err != nil || len(res.Tasks) != 0 || res.TotalSize != 0 {
		t.Errorf("List with an empty scope = %+v, %v, want none", res, err)
	}
}
