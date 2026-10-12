package a2acore

import (
	"errors"
	"sync"
	"testing"
)

type fakeRouteSource struct {
	mu            sync.Mutex
	owner         map[string]string
	ownerErr      error
	guardian      string
	guardianErr   error
	guardianCalls int
}

func (f *fakeRouteSource) OwnerRef(project string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.ownerErr != nil {
		return "", f.ownerErr
	}
	return f.owner[project], nil
}

func (f *fakeRouteSource) GuardianRef() (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.guardianCalls++
	if f.guardianErr != nil {
		return "", f.guardianErr
	}
	return f.guardian, nil
}

var (
	errOwnerSource = errors.New("owner source failed")
	errGuardianSrc = errors.New("guardian source failed")
)

func TestRoute(t *testing.T) {
	t.Run("owner present", func(t *testing.T) {
		src := &fakeRouteSource{owner: map[string]string{"org/repo": "o1/w1:p1"}, guardian: "g1/w1:p1"}
		got, err := Route(src, "org/repo", "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := []PlannedDelivery{{Role: RoleOwner, Ref: "o1/w1:p1", State: DeliveryHeld}}
		if len(got) != len(want) || got[0] != want[0] {
			t.Fatalf("got %+v, want %+v", got, want)
		}
		src.mu.Lock()
		calls := src.guardianCalls
		src.mu.Unlock()
		if calls != 0 {
			t.Fatalf("guardian consulted %d times when an owner exists", calls)
		}
	})
	t.Run("owner present is self when it is the caller", func(t *testing.T) {
		src := &fakeRouteSource{owner: map[string]string{"org/repo": "m1/w1:p1"}}
		got, err := Route(src, "org/repo", "m1/w1:p1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := []PlannedDelivery{{Role: RoleOwner, Ref: "m1/w1:p1", State: DeliverySelf}}
		if len(got) != 1 || got[0] != want[0] {
			t.Fatalf("got %+v, want %+v", got, want)
		}
	})
	t.Run("owner missing guardian present", func(t *testing.T) {
		src := &fakeRouteSource{owner: map[string]string{}, guardian: "g1/w1:p1"}
		got, err := Route(src, "org/repo", "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := []PlannedDelivery{
			{Role: RoleOwner, Ref: "", State: DeliveryNoRoute},
			{Role: RoleGuardian, Ref: "g1/w1:p1", State: DeliveryHeld},
		}
		if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
			t.Fatalf("got %+v, want %+v", got, want)
		}
	})
	t.Run("owner missing guardian is self when it is the caller", func(t *testing.T) {
		src := &fakeRouteSource{owner: map[string]string{}, guardian: "m1/w1:p1"}
		got, err := Route(src, "org/repo", "m1/w1:p1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := []PlannedDelivery{
			{Role: RoleOwner, Ref: "", State: DeliveryNoRoute},
			{Role: RoleGuardian, Ref: "m1/w1:p1", State: DeliverySelf},
		}
		if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
			t.Fatalf("got %+v, want %+v", got, want)
		}
	})
	t.Run("both missing", func(t *testing.T) {
		src := &fakeRouteSource{owner: map[string]string{}}
		got, err := Route(src, "org/repo", "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := []PlannedDelivery{
			{Role: RoleOwner, Ref: "", State: DeliveryNoRoute},
			{Role: RoleGuardian, Ref: "", State: DeliveryNoRoute},
		}
		if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
			t.Fatalf("got %+v, want %+v", got, want)
		}
	})
	t.Run("owner source error propagated", func(t *testing.T) {
		src := &fakeRouteSource{ownerErr: errOwnerSource, guardian: "g1/w1:p1"}
		got, err := Route(src, "org/repo", "")
		if !errors.Is(err, errOwnerSource) {
			t.Fatalf("want owner source error, got %v", err)
		}
		if got != nil {
			t.Fatalf("want nil deliveries on error, got %+v", got)
		}
	})
	t.Run("guardian source error propagated", func(t *testing.T) {
		src := &fakeRouteSource{owner: map[string]string{}, guardianErr: errGuardianSrc}
		got, err := Route(src, "org/repo", "")
		if !errors.Is(err, errGuardianSrc) {
			t.Fatalf("want guardian source error, got %v", err)
		}
		if got != nil {
			t.Fatalf("want nil deliveries on error, got %+v", got)
		}
	})
	t.Run("unknown project routes through the guardian", func(t *testing.T) {
		src := &fakeRouteSource{owner: map[string]string{"org/repo": "o1/w1:p1"}, guardian: "g1/w1:p1"}
		got, err := Route(src, "other/repo", "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(got) != 2 || got[0].Role != RoleOwner || got[0].Ref != "" || got[0].State != DeliveryNoRoute {
			t.Fatalf("unexpected plan %+v", got)
		}
	})
}
