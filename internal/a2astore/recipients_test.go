package a2astore

import (
	"context"
	"reflect"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/djalmajr/nau/internal/a2acore"
)

// TestTaskRecipientRefs proves the recipient-reference read of one task:
// the distinct non-null refs of its deliveries, in any role and state,
// deduplicated and without the empty (no-route) refs; a task with no
// deliveries returns an empty list.
func TestTaskRecipientRefs(t *testing.T) {
	clk := &clock{t: testNow}

	t.Run("two admitted messages with different routings", func(t *testing.T) {
		s, _ := openTest(t, clk, "alice")
		// The first message is a fresh task planned to an owner and a
		// guardian at distinct refs.
		res, err := s.Admit(context.Background(), admission("alice", "m-1", "t-recip", "c-recip", "acme/widgets", "s-recip", 1, 1, true, []a2acore.PlannedDelivery{
			ownerDelivery("ref-a"),
			{Role: a2acore.RoleGuardian, Ref: "ref-b", State: a2acore.DeliveryHeld},
		}))
		if err != nil || res.Refusal != nil {
			t.Fatalf("first Admit = %+v, %v", res, err)
		}
		// The task is created awaiting the caller so the second message
		// is admitted as a continuation.
		if _, err := s.Create(context.Background(), &a2a.Task{
			ID: a2a.TaskID("t-recip"), ContextID: "c-recip",
			Status: a2a.TaskStatus{State: a2a.TaskStateInputRequired},
		}); err != nil {
			t.Fatalf("Create: %v", err)
		}
		// The second message is a continuation re-planned to the same
		// owner ref plus a no-route guardian (an empty ref).
		res, err = s.Admit(context.Background(), admission("alice", "m-2", "t-recip", "c-recip", "acme/widgets", "s-recip", 1, 2, false, []a2acore.PlannedDelivery{
			ownerDelivery("ref-a"),
			{Role: a2acore.RoleGuardian, Ref: "", State: a2acore.DeliveryHeld},
		}))
		if err != nil || res.Refusal != nil {
			t.Fatalf("second Admit = %+v, %v", res, err)
		}
		refs, err := s.TaskRecipientRefs(context.Background(), "t-recip")
		if err != nil {
			t.Fatalf("TaskRecipientRefs: %v", err)
		}
		if !reflect.DeepEqual(refs, []string{"ref-a", "ref-b"}) {
			t.Fatalf("TaskRecipientRefs = %v, want [ref-a ref-b] (deduplicated, without the empty ref)", refs)
		}
	})

	t.Run("a task without deliveries returns empty", func(t *testing.T) {
		s, _ := openTest(t, clk, "alice")
		res, err := s.Admit(context.Background(), admission("alice", "m-1", "t-none", "c-none", "acme/widgets", "s-none", 1, 1, true, nil))
		if err != nil || res.Refusal != nil {
			t.Fatalf("Admit = %+v, %v", res, err)
		}
		refs, err := s.TaskRecipientRefs(context.Background(), "t-none")
		if err != nil {
			t.Fatalf("TaskRecipientRefs: %v", err)
		}
		if len(refs) != 0 {
			t.Fatalf("TaskRecipientRefs = %v, want empty", refs)
		}
	})
}
