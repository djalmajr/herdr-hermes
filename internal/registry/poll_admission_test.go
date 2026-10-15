package registry

// Tests of the whole-request atomic admission of the registry (RC1,
// RC2/SR2, SR1): a poll is admitted whole or refused whole. A poll that
// replaces the rows it no longer sees applies the replacement in the
// same poll (the drop runs before the inserts); a poll that cannot be
// admitted whole (the nodes or the waits cap) is refused and rolls back
// every write of the transaction, the retention sweep included; the
// stored pane waits are capped at MaxWaits across every node; and the
// 65th machine is refused on the poll path. The jobs kind has no count
// cap (DJA-275): every new workspace row is admitted. Each refused
// path leaves the full dump (all seven tables) and the caller's input
// value unchanged.

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/djalmajr/nau/internal/jobapi"
	"github.com/djalmajr/nau/internal/outbox"
	"github.com/djalmajr/nau/internal/router"
)

// admBlockedPanes builds n blocked panes with the given id prefix.
func admBlockedPanes(prefix string, n int) []PolledPane {
	out := make([]PolledPane, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, PolledPane{
			PaneID:      fmt.Sprintf("%s-%04d", prefix, i),
			WorkspaceID: "w1",
			Name:        "agent-a",
			AgentStatus: "blocked",
		})
	}
	return out
}

// admWaitsPoll builds an OK poll of the machine carrying only the given
// blocked panes.
func admWaitsPoll(maquina string, panes []PolledPane) PollResult {
	p := okPoll(maquina, "")
	p.Panes = panes
	return p
}

// admSeedWaits seeds n waits rows of one machine with the given pane id
// prefix, in one transaction.
func admSeedWaits(t *testing.T, s *Store, maquina, prefix string, n int) {
	t.Helper()
	tx, err := s.db.Begin()
	if err != nil {
		t.Fatalf("begin the waits seed: %v", err)
	}
	for i := 0; i < n; i++ {
		if _, err := tx.Exec(`INSERT INTO waits(maquina, pane, workspace_id, agente, desde, desde_unix)
			VALUES(?,?,?,?,?,?)`,
			maquina, fmt.Sprintf("%s-%05d", prefix, i), "w1", "agent-a",
			fixedNow.Format(outbox.TSLayout), fixedNow.Unix()); err != nil {
			tx.Rollback()
			t.Fatalf("seed wait %d: %v", i, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit the waits seed: %v", err)
	}
}

// admMarshalInput is the input value's bytes for the unchanged-input
// assertion.
func admMarshalInput(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal the input: %v", err)
	}
	return b
}

// admInputUnchanged asserts the request value survived the call
// byte-for-byte: the apply never writes the caller's value.
func admInputUnchanged(t *testing.T, v any, beforeB []byte) {
	t.Helper()
	afterB, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal the input after the call: %v", err)
	}
	if !bytes.Equal(afterB, beforeB) {
		t.Errorf("the input value changed: before %s, after %s", beforeB, afterB)
	}
}

// admWantInvalid checks the ErrInvalid refusal and its exact text.
func admWantInvalid(t *testing.T, err error, text string) {
	t.Helper()
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("err = %v, want the ErrInvalid refusal", err)
	}
	if got := strings.TrimPrefix(err.Error(), ErrInvalid.Error()+": "); got != text {
		t.Fatalf("refusal text = %q, want %q", got, text)
	}
}

// admCheckRefusal checks the ErrInvalid refusal of the given kind with
// its exact text and the full dump equality: every table of the dump is
// byte-identical to before.
func admCheckRefusal(t *testing.T, s *Store, err error, kind string, before map[string][]string) {
	t.Helper()
	retWantRefusal(t, err, kind)
	after := retDump(t, s)
	for table, rows := range before {
		if !rowsEqual(after[table], rows) {
			t.Errorf("table %s changed by the refused request:\nbefore: %v\nafter: %v", table, rows, after[table])
		}
	}
}

// admAgentListBody builds one `agent list` body of n blocked panes under
// the router output cap: the pane ids are five digits up to 100000 and
// six digits after, which is the longest population that fits.
func admAgentListBody(n int) []byte {
	var b strings.Builder
	b.WriteString(`{"id":"1","result":{"agents":[`)
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(`{"pane_id":"`)
		if i < 100000 {
			fmt.Fprintf(&b, "p%05d", i)
		} else {
			fmt.Fprintf(&b, "p%06d", i)
		}
		b.WriteString(`","agent_status":"blocked"}`)
	}
	b.WriteString(`]}}`)
	return []byte(b.String())
}

// TestAdmPollReplacementAppliesInSamePoll: a poll that replaces the
// poll-only rows it no longer sees applies the replacement in the same
// poll, because the rows the snapshot drops leave before the new rows
// are inserted; rows of another machine and active pushed jobs are
// never touched. The jobs kind has no count cap (DJA-275): every new
// workspace row is admitted.
func TestAdmPollReplacementAppliesInSamePoll(t *testing.T) {
	s := retStore(t, Options{})
	mustApplyPoll(t, s, okPoll("node-a", "",
		PolledWorkspace{ID: "w1", Label: "hm-agent-impl-c1", AgentStatus: "working"}))
	if f := jobFact(t, s, "node-a", "ws:w1"); !f.exists || f.encerrado != 0 {
		t.Fatalf("seed ws:w1 = %+v, want the live poll row", f)
	}
	retActiveJob(t, s, "node-b", "job-b1")
	p := okPoll("node-a", "",
		PolledWorkspace{ID: "w2", Label: "hm-agent-impl-c2", AgentStatus: "working"})
	inB := admMarshalInput(t, p)
	if err := s.ApplyPoll(p); err != nil {
		t.Fatalf("ApplyPoll of the replacement = %v, want admitted in the same poll", err)
	}
	if f := jobFact(t, s, "node-a", "ws:w1"); f.exists {
		t.Errorf("ws:w1 = %+v, want dropped (the poll no longer lists it)", f)
	}
	if f := jobFact(t, s, "node-a", "ws:w2"); !f.exists || f.encerrado != 0 {
		t.Errorf("ws:w2 = %+v, want admitted in the same poll", f)
	}
	if f := jobFact(t, s, "node-b", "job-b1"); !f.exists || f.encerrado != 0 {
		t.Errorf("job-b1 = %+v, want untouched (another machine's row)", f)
	}
	if n := tableCount(t, s, "jobs"); n != 2 {
		t.Errorf("jobs rows = %d, want 2 (one dropped, one inserted, the other machine's row kept)", n)
	}
	res, err := s.Check(CheckRequest{Agente: "agent", Workspace: "hm-agent-impl-c2"})
	if err != nil {
		t.Fatalf("Check of the new workspace: %v", err)
	}
	if res.Status != CheckDuplicate {
		t.Errorf("check of the workspace the poll just reported = %s, want duplicate", res.Status)
	}
	admInputUnchanged(t, p, inB)
}

// TestAdmPollRefusalRollsBackReconciledRows: a snapshot that replaces
// ws:w1 and is then refused late (waits over the cap, or a duplicate
// blocked pane id) leaves ws:w1 and every table unchanged: the
// reconciled drop and the admitted row roll back with the refusal.
func TestAdmPollRefusalRollsBackReconciledRows(t *testing.T) {
	t.Run("waits over the cap", func(t *testing.T) {
		s := retStore(t, Options{})
		mustApplyPoll(t, s, okPoll("node-a", "",
			PolledWorkspace{ID: "w1", Label: "hm-agent-impl-c1", AgentStatus: "working"}))
		before := retDump(t, s)
		p := okPoll("node-a", "",
			PolledWorkspace{ID: "w2", Label: "hm-agent-impl-c2", AgentStatus: "working"})
		p.Panes = admBlockedPanes("pa", MaxWaits+5)
		inB := admMarshalInput(t, p)
		err := s.ApplyPoll(p)
		admCheckRefusal(t, s, err, "waits", before)
		if f := jobFact(t, s, "node-a", "ws:w1"); !f.exists {
			t.Errorf("ws:w1 = %+v, want the reconciled drop rolled back", f)
		}
		if f := jobFact(t, s, "node-a", "ws:w2"); f.exists {
			t.Errorf("ws:w2 = %+v, want the admitted row rolled back", f)
		}
		if n := tableCount(t, s, "waits"); n != 0 {
			t.Errorf("waits rows = %d, want 0 (nothing was stored)", n)
		}
		admInputUnchanged(t, p, inB)
	})
	t.Run("duplicate blocked pane id", func(t *testing.T) {
		s := retStore(t, Options{})
		mustApplyPoll(t, s, okPoll("node-a", "",
			PolledWorkspace{ID: "w1", Label: "hm-agent-impl-c1", AgentStatus: "working"}))
		before := retDump(t, s)
		p := okPoll("node-a", "",
			PolledWorkspace{ID: "w2", Label: "hm-agent-impl-c2", AgentStatus: "working"})
		p.Panes = []PolledPane{
			{PaneID: "w1:p1", WorkspaceID: "w1", Name: "agent-a", AgentStatus: "blocked"},
			{PaneID: "w1:p1", WorkspaceID: "w1", Name: "agent-b", AgentStatus: "blocked"},
		}
		inB := admMarshalInput(t, p)
		err := s.ApplyPoll(p)
		admWantInvalid(t, err, "panes duplicate pane")
		after := retDump(t, s)
		for table, rows := range before {
			if !rowsEqual(after[table], rows) {
				t.Errorf("table %s changed by the refused poll:\nbefore: %v\nafter: %v", table, rows, after[table])
			}
		}
		if f := jobFact(t, s, "node-a", "ws:w1"); !f.exists {
			t.Errorf("ws:w1 = %+v, want still present", f)
		}
		admInputUnchanged(t, p, inB)
	})
}

// TestAdmPollNodeCap: the 65th distinct machine is refused on the poll
// path the same way SetNode refuses it, ok poll and failed poll alike,
// and an update of a known node at the cap still applies.
func TestAdmPollNodeCap(t *testing.T) {
	s := retStore(t, Options{})
	for i := 0; i < MaxNodes; i++ {
		if err := s.SetNode(NodeUpdate{Maquina: fmt.Sprintf("n%02d", i)}); err != nil {
			t.Fatalf("SetNode %d: %v", i, err)
		}
	}
	// Updates of a known node at the cap still apply: the ok poll moves
	// the facts and the failed poll ages them.
	mustApplyPoll(t, s, okPoll("n00", "",
		PolledWorkspace{ID: "w1", Label: "hm-agent-impl-c1", AgentStatus: "working"}))
	if f := jobFact(t, s, "n00", "ws:w1"); !f.exists || f.encerrado != 0 {
		t.Fatalf("known node workspace = %+v, want the ok poll admitted at the cap", f)
	}
	mustApplyPoll(t, s, PollResult{Maquina: "n00", At: fixedNow, OK: false, Motivo: "probe_timeout"})
	var status string
	if err := s.db.QueryRow(`SELECT poll_status FROM nodes WHERE maquina = 'n00'`).Scan(&status); err != nil {
		t.Fatalf("poll_status: %v", err)
	}
	if status != "probe_timeout" {
		t.Errorf("poll_status of the known node = %q, want the failed poll applied at the cap", status)
	}
	// The 65th label refuses the whole request.
	before := retDump(t, s)
	lines := setLogf(t)
	p := okPoll("poll-extra", "",
		PolledWorkspace{ID: "w1", Label: "hm-agent-impl-c1", AgentStatus: "working"})
	inB := admMarshalInput(t, p)
	err := s.ApplyPoll(p)
	var n int
	if scanErr := s.db.QueryRow(`SELECT COUNT(*) FROM nodes`).Scan(&n); scanErr != nil {
		t.Fatalf("count nodes: %v", scanErr)
	}
	if err == nil || !strings.Contains(err.Error(), "nodes limit reached") {
		t.Fatalf("ApplyPoll of the 65th machine = %v, nodes = %d, want nodes limit reached and %d nodes", err, n, MaxNodes)
	}
	if n != MaxNodes {
		t.Errorf("nodes = %d, want %d", n, MaxNodes)
	}
	if f := jobFact(t, s, "poll-extra", "ws:w1"); f.exists {
		t.Errorf("workspace of the refused machine = %+v, want no row", f)
	}
	after := retDump(t, s)
	for table, rows := range before {
		if !rowsEqual(after[table], rows) {
			t.Errorf("table %s changed by the refused poll:\nbefore: %v\nafter: %v", table, rows, after[table])
		}
	}
	if len(*lines) != 1 {
		t.Fatalf("alert lines = %d, want exactly one (%v)", len(*lines), *lines)
	}
	line := (*lines)[0]
	if !strings.Contains(line, "nodes limit reached") || !strings.Contains(line, "stored total 64") || !strings.Contains(line, "limit 64") {
		t.Errorf("alert line = %q, want the nodes counts only", line)
	}
	if strings.Contains(line, "poll-extra") {
		t.Errorf("alert line = %q, must not carry the machine label", line)
	}
	admInputUnchanged(t, p, inB)
	// The failed poll of the same new machine is refused the same way.
	before = retDump(t, s)
	pf := PollResult{Maquina: "poll-extra", At: fixedNow, OK: false, Motivo: "probe_failed"}
	inF := admMarshalInput(t, pf)
	err = s.ApplyPoll(pf)
	var n2 int
	if scanErr := s.db.QueryRow(`SELECT COUNT(*) FROM nodes`).Scan(&n2); scanErr != nil {
		t.Fatalf("count nodes after: %v", scanErr)
	}
	if err == nil || !strings.Contains(err.Error(), "nodes limit reached") {
		t.Fatalf("failed ApplyPoll of the 65th machine = %v, nodes = %d, want nodes limit reached", err, n2)
	}
	if n2 != MaxNodes {
		t.Errorf("nodes = %d, want %d", n2, MaxNodes)
	}
	after = retDump(t, s)
	for table, rows := range before {
		if !rowsEqual(after[table], rows) {
			t.Errorf("table %s changed by the refused failed poll:\nbefore: %v\nafter: %v", table, rows, after[table])
		}
	}
	admInputUnchanged(t, pf, inF)
}

// TestAdmWaitsCap: the stored pane waits are bounded at MaxWaits in
// total across every node: the global totals of 999 and 1000 are
// admitted, 1001 is refused; a node that replaces its own waits at the
// cap succeeds because its old rows are removed first; and a legacy
// database already over the cap may shrink or stay equal, never grow,
// with its rows never deleted by a refusal.
func TestAdmWaitsCap(t *testing.T) {
	t.Run("global totals", func(t *testing.T) {
		s := retStore(t, Options{})
		admSeedWaits(t, s, "m-a", "s1", 10)
		admSeedWaits(t, s, "m-b", "s2", MaxWaits-10-1)
		// final = 999 - 10 + 10 = 999: admitted.
		mustApplyPoll(t, s, admWaitsPoll("m-a", admBlockedPanes("pa", 10)))
		if n := tableCount(t, s, "waits"); n != MaxWaits-1 {
			t.Fatalf("waits = %d, want %d (the total under the cap fits)", n, MaxWaits-1)
		}
		// final = 1000 - 10 + 10 = 1000: admitted, exactly at the cap.
		admSeedWaits(t, s, "m-b", "s3", 1)
		mustApplyPoll(t, s, admWaitsPoll("m-a", admBlockedPanes("pb", 10)))
		if n := tableCount(t, s, "waits"); n != MaxWaits {
			t.Fatalf("waits = %d, want the cap kept (1000 is not over 1000)", n)
		}
		// final = 1000 - 10 + 11 = 1001: refused whole.
		before := retDump(t, s)
		p := admWaitsPoll("m-a", admBlockedPanes("pc", 11))
		inB := admMarshalInput(t, p)
		err := s.ApplyPoll(p)
		admCheckRefusal(t, s, err, "waits", before)
		if n := tableCount(t, s, "waits"); n != MaxWaits {
			t.Errorf("waits = %d, want %d (the refusal stored nothing)", n, MaxWaits)
		}
		admInputUnchanged(t, p, inB)
	})
	t.Run("own replacement at the cap", func(t *testing.T) {
		s := retStore(t, Options{})
		admSeedWaits(t, s, "m-a", "sa", MaxWaits)
		// The node's own old rows are removed before counting: the
		// replacement at the cap succeeds.
		mustApplyPoll(t, s, admWaitsPoll("m-a", admBlockedPanes("sb", MaxWaits)))
		if n := tableCount(t, s, "waits"); n != MaxWaits {
			t.Fatalf("waits = %d, want the cap kept (the old rows left room)", n)
		}
		// One pane past the cap with only its own rows: refused whole.
		before := retDump(t, s)
		p := admWaitsPoll("m-a", admBlockedPanes("sc", MaxWaits+1))
		inB := admMarshalInput(t, p)
		err := s.ApplyPoll(p)
		admCheckRefusal(t, s, err, "waits", before)
		if n := tableCount(t, s, "waits"); n != MaxWaits {
			t.Errorf("waits = %d, want %d (the refusal never deletes)", n, MaxWaits)
		}
		admInputUnchanged(t, p, inB)
	})
	t.Run("legacy over the cap", func(t *testing.T) {
		s := retStore(t, Options{})
		admSeedWaits(t, s, "m-b", "legacy", 1200)
		// Growth of another machine: final = 1200 + 10 > 1200: refused.
		before := retDump(t, s)
		p := admWaitsPoll("m-a", admBlockedPanes("lg", 10))
		inB := admMarshalInput(t, p)
		err := s.ApplyPoll(p)
		admCheckRefusal(t, s, err, "waits", before)
		if n := tableCount(t, s, "waits"); n != 1200 {
			t.Errorf("waits = %d, want 1200 (a refusal never deletes the rows)", n)
		}
		admInputUnchanged(t, p, inB)
		// Shrink: final = 1200 - 1200 + 1100 = 1100: admitted although
		// still over the cap.
		mustApplyPoll(t, s, admWaitsPoll("m-b", admBlockedPanes("sh", 1100)))
		if n := tableCount(t, s, "waits"); n != 1100 {
			t.Fatalf("waits = %d, want 1100 (a legacy shrink is allowed)", n)
		}
		// Equal: final = 1100: admitted.
		mustApplyPoll(t, s, admWaitsPoll("m-b", admBlockedPanes("eq", 1100)))
		if n := tableCount(t, s, "waits"); n != 1100 {
			t.Errorf("waits = %d, want 1100 (a legacy total staying equal is allowed)", n)
		}
		// Growth again: final = 1101 > 1100: refused.
		before = retDump(t, s)
		p = admWaitsPoll("m-b", admBlockedPanes("gr", 1101))
		inB = admMarshalInput(t, p)
		err = s.ApplyPoll(p)
		admCheckRefusal(t, s, err, "waits", before)
		if n := tableCount(t, s, "waits"); n != 1100 {
			t.Errorf("waits = %d, want 1100 (the refusal stored nothing)", n)
		}
		admInputUnchanged(t, p, inB)
	})
	t.Run("alert hygiene", func(t *testing.T) {
		s := retStore(t, Options{})
		admSeedWaits(t, s, "m-b", "hy", MaxWaits)
		lines := setLogf(t)
		p := admWaitsPoll("m-a", admBlockedPanes("hz", 1))
		err := s.ApplyPoll(p)
		retWantRefusal(t, err, "waits")
		if len(*lines) != 1 {
			t.Fatalf("alert lines = %d, want exactly one (%v)", len(*lines), *lines)
		}
		line := (*lines)[0]
		for _, want := range []string{"waits limit reached", "stored total 1000", "snapshot blocked 1", "limit 1000"} {
			if !strings.Contains(line, want) {
				t.Errorf("alert line = %q, want the count %q", line, want)
			}
		}
		for _, leak := range []string{"m-a", "m-b", "hz-0000"} {
			if strings.Contains(line, leak) {
				t.Errorf("alert line = %q, must not carry %q", line, leak)
			}
		}
	})
}

// TestAdmWaitsLargeAgentListRefusedWhole: the SR1 reproduction. An
// `agent list` body under the router output cap, with about 180000
// blocked panes, is refused whole by the waits bound: the pre-fix source
// stored every row and the composed inventory broke the 256 MiB bound.
func TestAdmWaitsLargeAgentListRefusedWhole(t *testing.T) {
	const panes = 178480 // the longest unique population that fits the cap
	body := admAgentListBody(panes)
	if len(body) > router.MaxProbeOutput {
		t.Fatalf("body = %d bytes, want at most the router cap %d", len(body), router.MaxProbeOutput)
	}
	list, err := ParseAgentList(body)
	if err != nil {
		t.Fatalf("ParseAgentList: %v", err)
	}
	if len(list) != panes {
		t.Fatalf("parsed panes = %d, want %d", len(list), panes)
	}
	s := openStore(t)
	before := retDump(t, s)
	p := PollResult{Maquina: "node-a", At: fixedNow, OK: true, Panes: list}
	inB := admMarshalInput(t, p)
	err = s.ApplyPoll(p)
	var n int
	if scanErr := s.db.QueryRow(`SELECT COUNT(*) FROM waits`).Scan(&n); scanErr != nil {
		t.Fatalf("count waits: %v", scanErr)
	}
	if err == nil {
		t.Fatalf("ApplyPoll of the %d-blocked-panes poll = <nil>, waits rows stored = %d, want the whole poll refused with waits limit reached", len(list), n)
	}
	retWantRefusal(t, err, "waits")
	if n != 0 {
		t.Errorf("waits rows = %d, want 0 (the refused poll stored nothing)", n)
	}
	after := retDump(t, s)
	for table, rows := range before {
		if !rowsEqual(after[table], rows) {
			t.Errorf("table %s changed by the refused poll:\nbefore: %v\nafter: %v", table, rows, after[table])
		}
	}
	admInputUnchanged(t, p, inB)
}

// TestAdmAdmissionConcurrencyAtCap: two Store values on one file with
// parallel goroutines issuing polls and ingests end with every cap
// respected and only valid refusals; the refused requests rolled back
// nothing half-applied. The jobs kind has no count cap (DJA-275): every
// ingest is admitted, and only the polls over the waits cap are refused.
// Run with -race.
func TestAdmAdmissionConcurrencyAtCap(t *testing.T) {
	path := pathFor(t)
	s1, err := Open(path, Options{Now: fixedClock()})
	if err != nil {
		t.Fatalf("Open store 1: %v", err)
	}
	s2, err := Open(path, Options{Now: fixedClock()})
	if err != nil {
		t.Fatalf("Open store 2: %v", err)
	}
	t.Cleanup(func() { s1.Close(); s2.Close() })
	// Waits: 990 seeded rows of a machine no poll touches; every poll
	// adds 15 blocked panes, which would push the total to 1005.
	admSeedWaits(t, s1, "m-seed", "seed", 990)
	stores := []*Store{s1, s2}
	var mu sync.Mutex
	var wg sync.WaitGroup
	var errs []error
	record := func(err error) {
		mu.Lock()
		errs = append(errs, err)
		mu.Unlock()
	}
	// Six ingests of distinct new jobs: the kind has no cap, so every
	// one is admitted, across the two stores.
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rec := retJobEventRec(fmt.Sprintf("job-p%d", i), 1, jobapi.TypeAccepted)
			applied, err := stores[i%2].Ingest(rec)
			if err == nil && !applied {
				err = fmt.Errorf("ingest %d: applied = false with no error", i)
			}
			record(err)
		}(i)
	}
	// Four polls of distinct new machines with 15 blocked panes each,
	// which push the waits total past the cap.
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			p := admWaitsPoll(fmt.Sprintf("m-poll%d", i), admBlockedPanes(fmt.Sprintf("mp%d", i), 15))
			record(stores[i%2].ApplyPoll(p))
		}(i)
	}
	wg.Wait()
	refusals := 0
	for _, err := range errs {
		if err == nil {
			continue
		}
		refusals++
		if !errors.Is(err, ErrInvalid) {
			t.Errorf("unexpected refusal class: %v", err)
		}
	}
	// Every ingest was admitted and no poll row survived: every poll was
	// refused whole on the waits cap.
	if n := tableCount(t, s1, "jobs"); n != 6 {
		t.Errorf("jobs rows = %d, want 6 (every ingest was admitted: the kind has no cap)", n)
	}
	if n := tableCount(t, s1, "waits"); n != 990 {
		t.Errorf("waits = %d, want 990 (every poll was refused whole)", n)
	}
	var pollRows int
	if err := s1.db.QueryRow(`SELECT COUNT(*) FROM jobs WHERE maquina LIKE 'm-poll%'`).Scan(&pollRows); err != nil {
		t.Fatalf("count poll rows: %v", err)
	}
	if pollRows != 0 {
		t.Errorf("poll workspace rows = %d, want 0 (the refused polls applied nothing)", pollRows)
	}
	if n := tableCount(t, s1, "nodes"); n > MaxNodes {
		t.Errorf("nodes = %d, want at most the cap %d", n, MaxNodes)
	}
	if refusals != 4 {
		t.Errorf("refusals = %d, want 4 (the four over-cap polls)", refusals)
	}
}
