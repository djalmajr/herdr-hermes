package a2aserver

import (
	"context"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv/taskstore"

	"github.com/djalmajr/nau/internal/a2astore"
)

// projectScopeKeyType is the context key of the per-request project
// scope.
type projectScopeKeyType struct{}

// withProjectScope returns ctx carrying visible, the per-request
// predicate that says whether a task of one stored project may be seen
// by the authenticated subject of the request. The interceptor stamps
// it after authentication and method authorization, on every request.
func withProjectScope(ctx context.Context, visible func(project string) bool) context.Context {
	return context.WithValue(ctx, projectScopeKeyType{}, visible)
}

// projectScopeFrom returns the project scope of ctx; ok is false when
// the context carries none (a call that did not pass the interceptor).
func projectScopeFrom(ctx context.Context) (func(project string) bool, bool) {
	v, ok := ctx.Value(projectScopeKeyType{}).(func(project string) bool)
	return v, ok && v != nil
}

// scopedTaskStore is the task store the SDK sees: reads (Get and List,
// which the SDK also reaches for cancel, continuation and every
// push-config method) are limited to the projects of the request scope,
// so a task in a project that left the subject's grant is
// indistinguishable from a task that does not exist. Without a scope on
// the context it fails closed. Create and Update are the store's own.
type scopedTaskStore struct{ store *a2astore.Store }

var _ taskstore.Store = scopedTaskStore{}

// Create implements taskstore.Store.
func (s scopedTaskStore) Create(ctx context.Context, task *a2a.Task) (taskstore.TaskVersion, error) {
	return s.store.Create(ctx, task)
}

// Update implements taskstore.Store.
func (s scopedTaskStore) Update(ctx context.Context, req *taskstore.UpdateRequest) (taskstore.TaskVersion, error) {
	return s.store.Update(ctx, req)
}

// Get implements taskstore.Store: without a scope it is
// a2a.ErrTaskNotFound.
func (s scopedTaskStore) Get(ctx context.Context, id a2a.TaskID) (*taskstore.StoredTask, error) {
	visible, ok := projectScopeFrom(ctx)
	if !ok {
		return nil, a2a.ErrTaskNotFound
	}
	return s.store.GetVisible(ctx, id, visible)
}

// List implements taskstore.Store: without a scope it is
// a2a.ErrUnauthenticated.
func (s scopedTaskStore) List(ctx context.Context, req *a2a.ListTasksRequest) (*a2a.ListTasksResponse, error) {
	visible, ok := projectScopeFrom(ctx)
	if !ok {
		return nil, a2a.ErrUnauthenticated
	}
	return s.store.ListVisible(ctx, req, visible)
}
