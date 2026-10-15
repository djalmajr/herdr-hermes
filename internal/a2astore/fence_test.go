package a2astore

import (
	"context"
	"reflect"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/djalmajr/nau/internal/a2acore"
)

// TestFenceRead proves the read-only fence read the server's interceptor
// uses for its pre-evaluation: a missing row is Found: false with no
// error; after an admission the Epoch and LastSeq are the admitted ones;
// and the read itself writes nothing (the a2a_ table counts are
// unchanged before and after).
func TestFenceRead(t *testing.T) {
	clk := &clock{t: testNow}
	s, _ := openTest(t, clk, "alice")
	ctx := context.Background()

	// No row yet: Found is false and there is no error.
	st, err := s.Fence(ctx, "alice", "s1")
	if err != nil {
		t.Fatalf("Fence fresh = %v, want no error", err)
	}
	if st.Found || st.Epoch != 0 || st.LastSeq != 0 {
		t.Fatalf("Fence fresh = %+v, want Found: false", st)
	}

	// The task commits awaiting the caller so the continuation below passes
	// the awaiting-caller gate; two admissions advance the fence to epoch 2,
	// last_seq 2.
	if res, err := s.Admit(ctx, admission("alice", "m-1", "t1", "c1", "acme/widgets", "s1", 2, 1, true,
		[]a2acore.PlannedDelivery{ownerDelivery("m1/ws1:p1")})); err != nil || res.Refusal != nil {
		t.Fatalf("first Admit = %+v, %v, want a fresh ingress", res, err)
	}
	if _, err := s.Create(ctx, mkTask("t1", "c1", a2a.TaskStateInputRequired, 1)); err != nil {
		t.Fatalf("Create t1: %v", err)
	}
	if res, err := s.Admit(ctx, admission("alice", "m-2", "t1", "c1", "acme/widgets", "s1", 2, 2, false,
		[]a2acore.PlannedDelivery{ownerDelivery("m1/ws1:p1")})); err != nil || res.Refusal != nil {
		t.Fatalf("second Admit = %+v, %v, want a fresh ingress", res, err)
	}

	// The read returns the admitted epoch and sequence, and writes
	// nothing: the table counts are identical before and after.
	before := queryCount(t, s.db)
	st, err = s.Fence(ctx, "alice", "s1")
	if err != nil {
		t.Fatalf("Fence after admissions = %v, want no error", err)
	}
	if !st.Found || st.Epoch != 2 || st.LastSeq != 2 {
		t.Fatalf("Fence after admissions = %+v, want Found epoch 2 last_seq 2", st)
	}
	after := queryCount(t, s.db)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("the fence read wrote something: before %v after %v", before, after)
	}
}
