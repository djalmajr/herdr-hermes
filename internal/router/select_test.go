package router_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/djalmajr/herdr-hermes/internal/router"
)

func countPtr(n int) *int { return &n }

func avail(m string, n *int) router.Candidate {
	return router.Candidate{Machine: m, State: router.StateAvailable, Orchestrators: n}
}

func nonAvail(m string, s router.State, r string) router.Candidate {
	return router.Candidate{Machine: m, State: s, Reason: r}
}

// TestSelectLeastLoad: with no request the available machine with the
// fewest orchestrators wins.
func TestSelectLeastLoad(t *testing.T) {
	cands := []router.Candidate{
		avail("mac-a", countPtr(2)),
		avail("mac-b", countPtr(1)),
		avail("mac-c", countPtr(3)),
		nonAvail("mac-d", router.StateUnavailable, router.ReasonProbeTimeout),
	}
	res, err := router.Select(cands, "")
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	if res.Machine != "mac-b" || res.Motivo != router.MotivoLeastLoad || res.Orchestrators != 1 || res.Requested != "" {
		t.Errorf("Select = %+v, want mac-b least_load 1 with empty requested", res)
	}
	if len(res.Candidates) != 4 || res.Candidates[0].Machine != "mac-a" || res.Candidates[3].Machine != "mac-d" {
		t.Errorf("candidates = %+v, want the input in configured order", res.Candidates)
	}
}

// TestSelectTieFirstConfigured: on a tie the first machine in configured
// order wins.
func TestSelectTieFirstConfigured(t *testing.T) {
	cands := []router.Candidate{
		avail("mac-a", countPtr(1)),
		avail("mac-b", countPtr(1)),
		avail("mac-c", countPtr(4)),
	}
	res, err := router.Select(cands, "")
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	if res.Machine != "mac-a" || res.Motivo != router.MotivoLeastLoad || res.Orchestrators != 1 {
		t.Errorf("Select = %+v, want mac-a least_load 1", res)
	}
}

// TestSelectRequestedWinsDespiteGreaterLoad: an available requested
// machine wins even when another machine has fewer orchestrators.
func TestSelectRequestedWinsDespiteGreaterLoad(t *testing.T) {
	cands := []router.Candidate{
		avail("mac-a", countPtr(5)),
		avail("mac-b", countPtr(1)),
	}
	res, err := router.Select(cands, "mac-a")
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	if res.Machine != "mac-a" || res.Motivo != router.MotivoRequested || res.Orchestrators != 5 || res.Requested != "mac-a" {
		t.Errorf("Select = %+v, want mac-a requested 5", res)
	}
}

// TestSelectRequestedUnavailableFallsBack: a requested machine that is
// unavailable, disabled or unsupported loses to the least loaded
// available machine, with motivo fallback.
func TestSelectRequestedUnavailableFallsBack(t *testing.T) {
	requested := []router.Candidate{
		nonAvail("mac-a", router.StateUnavailable, router.ReasonProbeTimeout),
		nonAvail("mac-b", router.StateDisabled, router.ReasonDisabledByConfig),
		nonAvail("mac-c", router.StateUnsupported, router.ReasonUnknownMachine),
	}
	for _, req := range requested {
		cands := []router.Candidate{
			avail("mac-x", countPtr(3)),
			req,
			avail("mac-y", countPtr(2)),
		}
		res, err := router.Select(cands, req.Machine)
		if err != nil {
			t.Errorf("requested %s: err = %v, want nil", req.Machine, err)
			continue
		}
		if res.Machine != "mac-y" || res.Motivo != router.MotivoFallback || res.Orchestrators != 2 || res.Requested != req.Machine {
			t.Errorf("requested %s: got %+v, want mac-y fallback 2", req.Machine, res)
		}
	}
}

// TestSelectRequestedUnknown: a label that is not configured is an error;
// the result keeps every candidate in configured order.
func TestSelectRequestedUnknown(t *testing.T) {
	cands := []router.Candidate{
		avail("mac-a", countPtr(1)),
		nonAvail("mac-b", router.StateDisabled, router.ReasonDisabledByConfig),
	}
	res, err := router.Select(cands, "ghost")
	if !errors.Is(err, router.ErrUnknownRequested) {
		t.Fatalf("err = %v, want ErrUnknownRequested", err)
	}
	if res.Machine != "" || res.Motivo != "" || res.Requested != "ghost" {
		t.Errorf("result = %+v, want empty machine and ghost requested", res)
	}
	if !reflect.DeepEqual(res.Candidates, cands) {
		t.Errorf("candidates = %+v, want the input kept", res.Candidates)
	}
}

// TestSelectNoneAvailable: when no machine is available the result keeps
// the candidates and names no machine; it covers a configured-but-
// unavailable request and an empty fleet too.
func TestSelectNoneAvailable(t *testing.T) {
	cands := []router.Candidate{
		nonAvail("mac-a", router.StateUnavailable, router.ReasonProbeFailed),
		nonAvail("mac-b", router.StateDisabled, router.ReasonDisabledByConfig),
	}
	res, err := router.Select(cands, "")
	if !errors.Is(err, router.ErrNoneAvailable) {
		t.Fatalf("err = %v, want ErrNoneAvailable", err)
	}
	if res.Machine != "" || len(res.Candidates) != 2 || res.Requested != "" {
		t.Errorf("result = %+v, want empty machine and kept candidates", res)
	}
	res2, err2 := router.Select(cands, "mac-b")
	if !errors.Is(err2, router.ErrNoneAvailable) {
		t.Fatalf("requested mac-b: err = %v, want ErrNoneAvailable", err2)
	}
	if res2.Machine != "" || res2.Requested != "mac-b" || len(res2.Candidates) != 2 {
		t.Errorf("result = %+v, want empty machine and kept candidates", res2)
	}
	res3, err3 := router.Select(nil, "")
	if !errors.Is(err3, router.ErrNoneAvailable) {
		t.Fatalf("empty fleet: err = %v, want ErrNoneAvailable", err3)
	}
	if res3.Machine != "" || len(res3.Candidates) != 0 {
		t.Errorf("empty fleet result = %+v, want empty machine and no candidates", res3)
	}
}

// TestSelectNilCountIgnored: an available candidate whose count was never
// established is treated as not available and is never inferred as zero.
func TestSelectNilCountIgnored(t *testing.T) {
	cands := []router.Candidate{
		avail("mac-a", nil),
		avail("mac-b", countPtr(2)),
	}
	res, err := router.Select(cands, "")
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	if res.Machine != "mac-b" || res.Motivo != router.MotivoLeastLoad || res.Orchestrators != 2 {
		t.Errorf("Select = %+v, want mac-b least_load 2", res)
	}
	res2, err2 := router.Select(cands, "mac-a")
	if err2 != nil {
		t.Fatalf("requested mac-a: err = %v, want nil", err2)
	}
	if res2.Machine != "mac-b" || res2.Motivo != router.MotivoFallback || res2.Orchestrators != 2 {
		t.Errorf("requested mac-a: got %+v, want mac-b fallback 2", res2)
	}
	res3, err3 := router.Select([]router.Candidate{avail("mac-a", nil)}, "")
	if !errors.Is(err3, router.ErrNoneAvailable) {
		t.Fatalf("nil-count only: err = %v, want ErrNoneAvailable", err3)
	}
	if res3.Machine != "" || res3.Motivo != "" || len(res3.Candidates) != 1 {
		t.Errorf("nil-count only result = %+v, want empty machine and kept candidates", res3)
	}
}

// TestSelectCandidatesAreACopy: the result never aliases the input, on
// the success and the error paths.
func TestSelectCandidatesAreACopy(t *testing.T) {
	input := []router.Candidate{avail("mac-a", countPtr(1))}
	res, err := router.Select(input, "")
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	res.Candidates[0].Machine = "tampered"
	res.Candidates[0].Orchestrators = countPtr(99)
	if input[0].Machine != "mac-a" || *input[0].Orchestrators != 1 {
		t.Errorf("input mutated through the result: %+v", input)
	}

	fresh := []router.Candidate{avail("mac-a", countPtr(1))}
	res2, err2 := router.Select(fresh, "")
	if err2 != nil {
		t.Fatalf("Select: %v", err2)
	}
	fresh[0].Machine = "tampered"
	if res2.Candidates[0].Machine != "mac-a" {
		t.Errorf("result mutated through the input: %+v", res2.Candidates)
	}

	res3, err3 := router.Select(fresh, "ghost")
	if !errors.Is(err3, router.ErrUnknownRequested) {
		t.Fatalf("err path: err = %v, want ErrUnknownRequested", err3)
	}
	if !reflect.DeepEqual(res3.Candidates, fresh) {
		t.Errorf("error-path candidates = %+v, want a copy of the input", res3.Candidates)
	}
	res3.Candidates[0].Machine = "tampered-again"
	if fresh[0].Machine != "tampered" {
		t.Errorf("input mutated through the error-path result: %+v", fresh)
	}
}
