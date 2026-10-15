package a2astore

import (
	"context"
	"testing"

	"github.com/djalmajr/nau/internal/a2acore"
)

// TestAdmittedTask: the read of the task admitted under the same key
// Admit uses — (subject, message id): no admission is the empty string
// with no error, an admitted message is its task id, and a store error
// (a closed store) comes back as an error.
func TestAdmittedTask(t *testing.T) {
	clk := &clock{t: testNow}
	s, _ := openTest(t, clk, "alice")
	// openTest's cleanup closes the store at the end; Close is idempotent
	// enough for the closed-store case to run before it.
	ctx := context.Background()

	// No admission carries the message: the empty string and no error.
	if got, err := s.AdmittedTask(ctx, "alice", "m-1"); got != "" || err != nil {
		t.Fatalf("AdmittedTask without a row = %q, %v, want the empty string and nil", got, err)
	}

	// A fresh admission carries it: the task id of the admission.
	res, err := s.Admit(ctx, admission("alice", "m-1", "t1", "c1", "acme/widgets", "s1", 1, 1, true,
		[]a2acore.PlannedDelivery{ownerDelivery("m1/ws1:p1")}))
	if err != nil {
		t.Fatalf("Admit: %v", err)
	}
	if res.Duplicate || res.Refusal != nil {
		t.Fatalf("Admit = %+v, want a fresh ingress", res)
	}
	if got, err := s.AdmittedTask(ctx, "alice", "m-1"); got != "t1" || err != nil {
		t.Fatalf("AdmittedTask after the admission = %q, %v, want t1 and nil", got, err)
	}
	// The key is the subject and the message id: another subject does
	// not read the admission, neither does the same subject under
	// another message id.
	if got, err := s.AdmittedTask(ctx, "bob", "m-1"); got != "" || err != nil {
		t.Fatalf("AdmittedTask for another subject = %q, %v, want the empty string and nil", got, err)
	}
	if got, err := s.AdmittedTask(ctx, "alice", "m-2"); got != "" || err != nil {
		t.Fatalf("AdmittedTask for another message id = %q, %v, want the empty string and nil", got, err)
	}

	// A closed store is a SQL error, not the no-admission answer.
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got, err := s.AdmittedTask(ctx, "alice", "m-1"); err == nil {
		t.Fatalf("AdmittedTask on the closed store = %q, nil, want an error", got)
	}
}
