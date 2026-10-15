package registry

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/djalmajr/nau/internal/jobapi"
	"github.com/djalmajr/nau/internal/outbox"
)

// dispatchRec is a dispatch record with a controllable ts.
func dispatchRec(maquina, jobID, ts string) outbox.Record {
	return outbox.Record{
		Schema: 1, Seq: 9, TS: ts,
		Tipo: outbox.TipoDispatch, Maquina: maquina,
		JobID: ptr(jobID), IdempotencyKey: maquina + ":9",
		Dados: json.RawMessage(`{}`),
	}
}

// TestInventoryOnlineWindow: a node is online when its last contact is
// within the window.
// Mutation captured: a >= swapped with > (or a missing unix comparison)
// flips the boundary row.
func TestInventoryOnlineWindow(t *testing.T) {
	nowFn, set := movableClock()
	s := clockStore(t, nowFn)
	mustApplyPoll(t, s, okPoll("node-a", ""))
	set(180) // exactly one default window later
	inv, err := s.Inventory(Filter{})
	if err != nil {
		t.Fatalf("Inventory: %v", err)
	}
	if len(inv.Nodes) != 1 {
		t.Fatalf("nodes = %d, want 1", len(inv.Nodes))
	}
	if !inv.Nodes[0].Online {
		t.Errorf("node online = false at the window boundary, want true (inclusive)")
	}
	set(181)
	inv, err = s.Inventory(Filter{})
	if err != nil {
		t.Fatalf("Inventory: %v", err)
	}
	if inv.Nodes[0].Online {
		t.Errorf("node online = true past the window, want false")
	}
}

// TestInventoryOciosoS: ocioso_s is now minus the last activity in whole
// seconds, clamped at zero, null when the job never had activity.
// Mutation captured: a missing clamp (or a zero written instead of null)
// fails a row.
func TestInventoryOciosoS(t *testing.T) {
	nowFn, set := movableClock()
	s := clockStore(t, nowFn)
	mustIngest(t, s, dispatchRec("node-a", "job-9", "2026-10-10T12:00:00-03:00"))
	mustIngest(t, s, recordBase(func(r *outbox.Record) {
		r.JobID = ptr("job-10")
		r.IdempotencyKey = "job-10:1"
		r.Dados = json.RawMessage(`{"seq":1,"ts":"2026-10-10T13:00:00-03:00","tipo":"note","resumo":"future"}`)
	}))
	if _, err := s.db.Exec(`INSERT INTO jobs(maquina, chave, origem) VALUES('node-a','job-null','job')`); err != nil {
		t.Fatalf("seed the activity-less job: %v", err)
	}
	set(300) // now is 12:05
	inv, err := s.Inventory(Filter{})
	if err != nil {
		t.Fatalf("Inventory: %v", err)
	}
	var byChave = map[string]*Job{}
	for i := range inv.Jobs {
		byChave[inv.Jobs[i].Chave] = &inv.Jobs[i]
	}
	if j := byChave["job-9"]; j == nil || j.OciosoS == nil || *j.OciosoS != 300 {
		t.Errorf("job-9 ocioso_s = %v, want 300 (12:05 - 12:00)", j)
	}
	if j := byChave["job-10"]; j == nil || j.OciosoS == nil || *j.OciosoS != 0 {
		t.Errorf("job-10 ocioso_s = %v, want 0 (clamped, future activity)", j)
	}
	if j := byChave["job-null"]; j == nil || j.OciosoS != nil {
		t.Errorf("job-null ocioso_s = %v, want null", j)
	}
}

// TestInventorySorting: every list comes out sorted.
// Mutation captured: a dropped ORDER BY (or the wrong key) fails the
// order assertions.
func TestInventorySorting(t *testing.T) {
	s := openStore(t)
	mustIngest(t, s, recordBase(func(r *outbox.Record) { r.JobID = ptr("job-b"); r.IdempotencyKey = "job-b:1" }))
	mustIngest(t, s, recordBase(func(r *outbox.Record) { r.Maquina = "node-b"; r.IdempotencyKey = "job-1:1" }))
	mustIngest(t, s, recordBase(func(r *outbox.Record) { r.Maquina = "node-b"; r.JobID = ptr("job-a"); r.IdempotencyKey = "job-a:1" }))
	if err := s.SetPolicy(PolicyInput{Escopo: "card", Valor: "zz-1", Hold: true}); err != nil {
		t.Fatalf("SetPolicy: %v", err)
	}
	if err := s.SetPolicy(PolicyInput{Escopo: "agente", Valor: "a", Hold: true}); err != nil {
		t.Fatalf("SetPolicy: %v", err)
	}
	if err := s.SetFolder(FolderInput{Maquina: "node-b", Caminho: "/data/2", Pedido: "x"}); err != nil {
		t.Fatalf("SetFolder: %v", err)
	}
	if err := s.SetFolder(FolderInput{Maquina: "node-a", Caminho: "/data/1", Pedido: "x"}); err != nil {
		t.Fatalf("SetFolder: %v", err)
	}
	inv, err := s.Inventory(Filter{})
	if err != nil {
		t.Fatalf("Inventory: %v", err)
	}
	var nodes, jobs, pastas []string
	for _, n := range inv.Nodes {
		nodes = append(nodes, n.Maquina)
	}
	for _, j := range inv.Jobs {
		jobs = append(jobs, j.Maquina+"/"+j.Chave)
	}
	for _, p := range inv.Pastas {
		pastas = append(pastas, p.Maquina+"/"+p.Caminho)
	}
	if !equalStrings(nodes, []string{"node-a", "node-b"}) {
		t.Errorf("nodes = %v, want sorted by maquina", nodes)
	}
	if !equalStrings(jobs, []string{"node-a/job-b", "node-b/job-1", "node-b/job-a"}) {
		t.Errorf("jobs = %v, want maquina then chave", jobs)
	}
	if !equalStrings(pastas, []string{"node-a//data/1", "node-b//data/2"}) {
		t.Errorf("pastas = %v, want maquina then caminho", pastas)
	}
	var politicas []string
	for _, p := range inv.Politicas {
		politicas = append(politicas, p.Escopo+"/"+p.Valor)
	}
	if !equalStrings(politicas, []string{"agente/a", "card/ZZ-1"}) {
		t.Errorf("politicas = %v, want escopo then valor", politicas)
	}
}

// TestInventoryFilters: maquina keeps the machine's rows, agente keeps the
// agent's jobs and claims, invalid filters are refused.
// Mutation captured: a filter applied to the wrong table (or skipped)
// fails the counts.
func TestInventoryFilters(t *testing.T) {
	s := openStore(t)
	mustIngest(t, s, recordBase(nil)) // node-a job-1, no agent
	mustApplyPoll(t, s, okPoll("node-a", "",
		PolledWorkspace{ID: "1", Label: "hm-dja-impl-9", AgentStatus: "working"}))
	res, err := s.Check(CheckRequest{Agente: "dja", Maquina: "node-a", Card: "dja-229"})
	if err != nil || res.Status != CheckOK {
		t.Fatalf("Check: %v %v", res, err)
	}
	inv, err := s.Inventory(Filter{Maquina: "node-a"})
	if err != nil {
		t.Fatalf("Inventory: %v", err)
	}
	if len(inv.Nodes) != 1 || len(inv.Jobs) != 2 || inv.Nodes[0].Maquina != "node-a" {
		t.Errorf("maquina filter kept %+v, want only the node-a rows", inv)
	}
	if len(inv.Claims) != 1 || inv.Claims[0].Maquina == nil || *inv.Claims[0].Maquina != "node-a" {
		t.Errorf("maquina filter claims = %+v, want the node-a claim", inv.Claims)
	}
	inv, err = s.Inventory(Filter{Agente: "dja"})
	if err != nil {
		t.Fatalf("Inventory: %v", err)
	}
	if len(inv.Jobs) != 1 || inv.Jobs[0].Chave != "ws:1" {
		t.Errorf("agente filter jobs = %+v, want the dja job only", inv.Jobs)
	}
	if len(inv.Claims) != 1 {
		t.Errorf("agente filter claims = %d, want the dja claim", len(inv.Claims))
	}
	inv, err = s.Inventory(Filter{Agente: "dja2"})
	if err != nil {
		t.Fatalf("Inventory: %v", err)
	}
	if len(inv.Jobs) != 0 || len(inv.Claims) != 0 {
		t.Errorf("agente filter kept %d jobs %d claims, want none", len(inv.Jobs), len(inv.Claims))
	}
	for _, f := range []Filter{{Maquina: "bad maquina"}, {Agente: "UPPER"}} {
		if _, err := s.Inventory(f); !errors.Is(err, ErrInvalid) {
			t.Errorf("Inventory(%+v) = %v, want ErrInvalid", f, err)
		}
	}
}

// TestInventoryWaits: pane waits and waiting jobs both appear, sorted by
// maquina then desde.
// Mutation captured: a waits source dropped (or the desde taken from the
// wrong column) fails the list.
func TestInventoryWaits(t *testing.T) {
	s := openStore(t)
	mustIngest(t, s, recordBase(func(r *outbox.Record) {
		r.Dados = eventData(1, jobapi.TypeQuestion, "which one", map[string]any{"pane": "p9"})
	}))
	p := okPoll("node-a", "")
	p.Panes = []PolledPane{{PaneID: "w1:p1", WorkspaceID: "w1", Name: "agent-a", AgentStatus: "blocked"}}
	mustApplyPoll(t, s, p)
	inv, err := s.Inventory(Filter{})
	if err != nil {
		t.Fatalf("Inventory: %v", err)
	}
	if len(inv.Esperas) != 2 {
		t.Fatalf("esperas = %+v, want two (the job wait and the pane wait)", inv.Esperas)
	}
	// The job wait (11:30) is older than the pane wait (12:00).
	if inv.Esperas[0].Tipo != EsperaQuestion || inv.Esperas[1].Tipo != EsperaPane {
		t.Errorf("esperas order = [%s %s], want the job wait first", inv.Esperas[0].Tipo, inv.Esperas[1].Tipo)
	}
	jw := inv.Esperas[0]
	if jw.Chave == nil || *jw.Chave != "job-1" || jw.Pane == nil || *jw.Pane != "p9" || jw.Resumo == nil || *jw.Resumo != "which one" {
		t.Errorf("job wait = %+v, want the chave, pane and resumo of the latest event", jw)
	}
	if jw.Desde != "2026-10-10T11:30:00-03:00" {
		t.Errorf("job wait desde = %q, want the event ts", jw.Desde)
	}
	pw := inv.Esperas[1]
	if pw.Chave != nil || pw.Pane == nil || *pw.Pane != "w1:p1" || pw.Desde != "2026-10-10T12:00:00-03:00" {
		t.Errorf("pane wait = %+v, want the pane facts", pw)
	}
}

// TestInventoryExpiredClaimsHidden: only unexpired claims are listed.
// Mutation captured: a claims read without the expiry predicate shows the
// expired one.
func TestInventoryExpiredClaimsHidden(t *testing.T) {
	s := openStore(t)
	_, err := s.db.Exec(`INSERT INTO claims(id, agente, maquina, criado, criado_unix, expira, expira_unix)
		VALUES(?, 'dja', 'node-a', '2026-10-10T00:00:00-03:00', -3600, '2026-10-10T01:00:00-03:00', -3540)`,
		strings.Repeat("0", 32))
	if err != nil {
		t.Fatalf("seed the expired claim: %v", err)
	}
	_, err = s.db.Exec(`INSERT INTO claims(id, agente, maquina, criado, criado_unix, expira, expira_unix)
		VALUES(?, 'dja', 'node-a', '2026-10-10T12:00:00-03:00', ?, '2026-10-10T13:00:00-03:00', ?)`,
		strings.Repeat("1", 32), fixedNow.Unix(), fixedNow.Unix()+3600)
	if err != nil {
		t.Fatalf("seed the live claim: %v", err)
	}
	inv, err := s.Inventory(Filter{})
	if err != nil {
		t.Fatalf("Inventory: %v", err)
	}
	if len(inv.Claims) != 1 || inv.Claims[0].ID != strings.Repeat("1", 32) {
		t.Errorf("claims = %+v, want only the live one", inv.Claims)
	}
}

// TestInventoryJSONShapes: every list is [] (never null) and the node
// runners are null when never reported.
// Mutation captured: a nil slice (or runners written as an empty list)
// changes the JSON text.
func TestInventoryJSONShapes(t *testing.T) {
	s := openStore(t)
	inv, err := s.Inventory(Filter{})
	if err != nil {
		t.Fatalf("Inventory: %v", err)
	}
	b, err := json.Marshal(inv)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, key := range []string{`"nodes":[]`, `"jobs":[]`, `"esperas":[]`, `"claims":[]`, `"politicas":[]`, `"pastas":[]`} {
		if !strings.Contains(string(b), key) {
			t.Errorf("JSON %s, want an empty list not null: %s", key, b)
		}
	}
	// A dispatched job has no events: "eventos":[].
	mustIngest(t, s, dispatchRec("node-a", "job-9", "2026-10-10T12:00:00-03:00"))
	inv, err = s.Inventory(Filter{})
	if err != nil {
		t.Fatalf("Inventory: %v", err)
	}
	b, _ = json.Marshal(inv)
	if !strings.Contains(string(b), `"eventos":[]`) {
		t.Errorf("JSON events for the fresh job, want []: %s", b)
	}
	// Runners: null when never reported, a list when reported.
	if err := s.SetNode(NodeUpdate{Maquina: "node-b", Runners: []string{"a"}}); err != nil {
		t.Fatalf("SetNode: %v", err)
	}
	inv, err = s.Inventory(Filter{})
	if err != nil {
		t.Fatalf("Inventory: %v", err)
	}
	b, _ = json.Marshal(inv)
	var doc struct {
		Nodes []struct {
			Maquina string   `json:"maquina"`
			Runners []string `json:"runners"`
		} `json:"nodes"`
	}
	if err := json.Unmarshal(b, &doc); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, n := range doc.Nodes {
		switch n.Maquina {
		case "node-a":
			if n.Runners != nil {
				t.Errorf("node-a runners = %v, want null (never reported)", n.Runners)
			}
		case "node-b":
			if n.Runners == nil || len(n.Runners) != 1 || n.Runners[0] != "a" {
				t.Errorf("node-b runners = %v, want [a]", n.Runners)
			}
		}
	}
}

// TestTwoNodeInventory: two polled nodes, a pushed job event on one, a
// claim bound to the workspace of the other; the inventory lists both and
// the check for the bound card is a duplicate on the bound machine.
// Mutation captured: a claim binding skipped (or a merge that ignores the
// machine) leaves the check free.
func TestTwoNodeInventory(t *testing.T) {
	s := openStore(t)
	// The card is claimed for node-b's workspace.
	res, err := s.Check(CheckRequest{Agente: "example", Maquina: "node-b", Card: "example-229", Workspace: "hm-example-review-x2"})
	if err != nil || res.Status != CheckOK {
		t.Fatalf("Check: %v %v", res, err)
	}
	// node-a: one hm workspace, plus a pushed job event.
	mustApplyPoll(t, s, okPoll("node-a", "0.9.1",
		PolledWorkspace{ID: "1", Label: "hm-example-impl-x1", AgentStatus: "working"}))
	mustIngest(t, s, recordBase(func(r *outbox.Record) { r.JobID = ptr("job-9"); r.IdempotencyKey = "job-9:1" }))
	// node-b: the claimed workspace.
	mustApplyPoll(t, s, okPoll("node-b", "0.9.1",
		PolledWorkspace{ID: "2", Label: "hm-example-review-x2", AgentStatus: "working"}))

	inv, err := s.Inventory(Filter{})
	if err != nil {
		t.Fatalf("Inventory: %v", err)
	}
	var nodes, jobs []string
	for _, n := range inv.Nodes {
		nodes = append(nodes, n.Maquina)
		if !n.Online {
			t.Errorf("node %s online = false, want both nodes online", n.Maquina)
		}
	}
	for _, j := range inv.Jobs {
		jobs = append(jobs, j.Maquina+"/"+j.Chave)
	}
	if !equalStrings(nodes, []string{"node-a", "node-b"}) {
		t.Errorf("nodes = %v, want both", nodes)
	}
	if !equalStrings(jobs, []string{"node-a/job-9", "node-a/ws:1", "node-b/ws:2"}) {
		t.Errorf("jobs = %v, want the rows of both machines", jobs)
	}
	// The check for the card already used on node-b is a duplicate there
	// (the bound claim and the bound row both conflict; both are on
	// node-b).
	res2, err := s.Check(CheckRequest{Agente: "other", Card: "example-229"})
	if err != nil {
		t.Fatalf("second Check: %v", err)
	}
	if res2.Status != CheckDuplicate || len(res2.Conflitos) == 0 {
		t.Fatalf("status = %q conflitos = %+v, want the duplicate conflicts", res2.Status, res2.Conflitos)
	}
	var jobConflict *Conflict
	for i := range res2.Conflitos {
		c := res2.Conflitos[i]
		if c.Maquina == nil || *c.Maquina != "node-b" {
			t.Errorf("conflict = %+v, want maquina node-b", c)
		}
		if c.Tipo == ConflitoJob && c.Campo == CampoCard {
			jobConflict = &c
		}
	}
	if jobConflict == nil {
		t.Errorf("conflicts = %+v, want the node-b card job conflict", res2.Conflitos)
	}
}

// TestInventoryWorstCaseBound: a math model, not a measurement of a
// stored database: one measured row of each table, with every kept value
// at its length bound and made of characters JSON escapes to six bytes,
// multiplied by the table's reference count, stays under MaxInventoryBytes, the most
// the jobs client reads. The jobs and sessions kinds have no row cap
// (DJA-275), so the model uses the reference populations (1000 rows
// each) and an inventory larger than the bound fails the jobs client,
// the high-count warn being the advance signal, not a guarantee. The
// worst strings are at the byte limit: such a
// value is wider, escaped, than any truncated value (whose 76-byte marker
// is ASCII), so the model uses at-limit control characters and checks one
// truncated value is not wider. The jobs list holds the active rows of
// both kinds (the poll workspaces are the jobs kind). The
// modeled state is refJobs jobs-kind rows, per row the larger of the
// event-bearing row (its full event ring plus a workspace_id at
// MaxPolledStringBytes, taken through ApplyPoll of a job-<id> workspace
// of the same job) and the worst hm- workspace row (label and id at the
// bound), each with its wait; refSessions sessions-kind rows at their
// field bounds; MaxWaits pane waits, each with the pane id and the name
// at MaxPolledPaneBytes and a workspace id resolving to a ws: key at the
// bound; and MaxNodes nodes, the measured one with its poll_status at
// MaxPolledStringBytes. A sessao row never carries an event ring: a
// job_event record's key is its job_id, which the contract id rule (no
// ':' or '#') cannot address with a sessao key, and no other record type
// or the poll writes events (TestSessionKeysNeverHoldEvents proves every
// path with hostile inputs), so a uniform worst case would size a state
// that cannot exist.
func TestInventoryWorstCaseBound(t *testing.T) {
	const refJobs = 1000
	const refSessions = 1000
	s := openStore(t)
	// A value at its byte limit of control characters is the worst
	// escaped width; a truncated value of the same limit is narrower.
	atLimit, _ := json.Marshal(strings.Repeat("\x01", MaxPolledStringBytes))
	truncated, _ := json.Marshal(boundPolledString(strings.Repeat("\x01", 2*MaxPolledStringBytes), MaxPolledStringBytes))
	if len(truncated) > len(atLimit) {
		t.Fatalf("a truncated value (%d bytes escaped) is wider than the at-limit value (%d)", len(truncated), len(atLimit))
	}
	refs := map[string]any{}
	for _, k := range EventRefsKeys {
		refs[k] = strings.Repeat("<", MaxRefValueBytes)
	}
	for i := int64(1); i <= int64(DefaultEventsPerJob)+1; i++ {
		tipo := "n" + strings.Repeat("x", 63) // the longest type an event may carry
		if i == int64(DefaultEventsPerJob)+1 {
			tipo = jobapi.TypeQuestion
		}
		b, _ := json.Marshal(map[string]any{
			"seq": i, "ts": "2026-10-10T11:30:00.123456789-03:00", "tipo": tipo,
			"resumo": strings.Repeat("\x01", MaxResumo+1), "refs": refs,
		})
		mustIngest(t, s, recordBase(func(r *outbox.Record) {
			r.Seq = i
			r.IdempotencyKey = fmt.Sprintf("job-1:%d", i)
			r.Dados = b
		}))
	}
	esc := strings.Repeat("\x01", 280)
	if err := s.SetPolicy(PolicyInput{Escopo: EscopoProjeto, Valor: strings.Repeat("a", 100) + "/" + strings.Repeat("b", 100), Hold: true, Prioridade: ptr(-1000), Motivo: esc}); err != nil {
		t.Fatalf("SetPolicy: %v", err)
	}
	if err := s.SetFolder(FolderInput{Maquina: "node-a", Caminho: strings.Repeat("\x01", 1024), TamanhoBytes: ptr(int64(1) << 62), Pedido: "2026-10-10T11:30:00.123456789-03:00", Job: strings.Repeat("j", 64)}); err != nil {
		t.Fatalf("SetFolder: %v", err)
	}
	runners := make([]string, 32)
	for i := range runners {
		runners[i] = fmt.Sprintf("%02d", i) + strings.Repeat("r", 30)
	}
	on := true
	if err := s.SetNode(NodeUpdate{Maquina: "node-a", Manutencao: &on, ManutencaoMotivo: ptr(strings.Repeat("~", 280)), SohoVersion: ptr(strings.Repeat("~", 120)), Runners: runners}); err != nil {
		t.Fatalf("SetNode: %v", err)
	}
	if _, err := s.Check(CheckRequest{Agente: strings.Repeat("a", 64), Maquina: "node-b", Card: "A" + strings.Repeat("b", 15) + "-123456789", Projeto: strings.Repeat("c", 100) + "/" + strings.Repeat("d", 100), Branch: strings.Repeat("e", 200), PR: "123456789", Workspace: "w" + strings.Repeat("x", 63)}); err != nil {
		t.Fatalf("Check: %v", err)
	}
	// The worst sessions-kind row: a sessao row with every text field at
	// its bound and no events (impossible for a sessao row, measured at
	// its own fields only).
	sessProjeto := strings.Repeat("a", 100) + "/" + strings.Repeat("b", 100)
	sessBranch := strings.Repeat("e", 200)
	if !mustIngest(t, s, recordBase(func(r *outbox.Record) {
		r.Tipo = outbox.TipoSession
		r.TS = "2026-10-10T11:30:00-03:00"
		r.IdempotencyKey = "node-a:sess-worst"
		r.Projeto = sessProjeto
		r.Dados = json.RawMessage(`{"acao":"start","projeto":"` + sessProjeto + `","branch":"` + sessBranch + `","card":"Abcdefghijklmno-123456789","pr":"123456789"}`)
	})) {
		t.Fatal("worst sessions row: applied = false")
	}
	// The poll-side worst rows: the job-<id> workspace of the event-
	// bearing job takes its workspace_id to the poll bound, the worst
	// hm- workspace takes its label and id to the bound, the blocked
	// pane's workspace id resolves to the hm row's ws: key, and the
	// failed poll leaves the node's poll_status at the bound.
	idJob := strings.Repeat("\x01", MaxPolledStringBytes)
	idHm := strings.Repeat("\x02", MaxPolledStringBytes)
	if err := s.ApplyPoll(PollResult{Maquina: "node-a", At: fixedNow, OK: true,
		Workspaces: []PolledWorkspace{
			{ID: idJob, Label: "job-job-1", AgentStatus: "working"},
			{ID: idHm, Label: "hm-" + strings.Repeat("\x03", MaxPolledStringBytes-3), AgentStatus: "idle"},
		},
		Panes: []PolledPane{
			{PaneID: strings.Repeat("\x01", MaxPolledPaneBytes), Name: strings.Repeat("\x02", MaxPolledPaneBytes), WorkspaceID: idHm, AgentStatus: "blocked"},
		},
	}); err != nil {
		t.Fatalf("ApplyPoll(ok): %v", err)
	}
	if err := s.ApplyPoll(PollResult{Maquina: "node-a", At: fixedNow, OK: false, Motivo: strings.Repeat("\x04", MaxPolledStringBytes)}); err != nil {
		t.Fatalf("ApplyPoll(failed): %v", err)
	}
	inv, err := s.Inventory(Filter{})
	if err != nil {
		t.Fatalf("Inventory: %v", err)
	}
	if len(inv.Jobs) != 3 || len(inv.Esperas) != 2 || len(inv.Politicas) != 1 || len(inv.Pastas) != 1 || len(inv.Claims) != 1 || len(inv.Nodes) == 0 {
		t.Fatalf("inventory rows = %d jobs, %d esperas, %d politicas, %d pastas, %d claims, %d nodes; want the job, hm and session rows, the job and pane waits, and one of the rest", len(inv.Jobs), len(inv.Esperas), len(inv.Politicas), len(inv.Pastas), len(inv.Claims), len(inv.Nodes))
	}
	if n := len(inv.Jobs[0].Eventos); n != DefaultEventsPerJob {
		t.Fatalf("events kept = %d, want %d", n, DefaultEventsPerJob)
	}
	var sessRow, hmRow *Job
	var jobWait, paneWait *Wait
	for i := range inv.Jobs {
		switch inv.Jobs[i].Origem {
		case OrigemSessao:
			sessRow = &inv.Jobs[i]
		case OrigemWorkspace:
			hmRow = &inv.Jobs[i]
		}
	}
	for i := range inv.Esperas {
		switch inv.Esperas[i].Tipo {
		case EsperaQuestion:
			jobWait = &inv.Esperas[i]
		case EsperaPane:
			paneWait = &inv.Esperas[i]
		}
	}
	if sessRow == nil || hmRow == nil || jobWait == nil || paneWait == nil {
		t.Fatalf("inventory = %+v, want the session row, the hm row and both waits", inv.Jobs)
	}
	if paneWait.Chave == nil || !strings.HasPrefix(*paneWait.Chave, "ws:") {
		t.Fatalf("pane wait chave = %v, want the resolved ws: key at the bound", paneWait.Chave)
	}
	size := func(v any) int {
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		return len(b) + 1
	}
	// Per jobs-kind row the model takes the larger of the event-bearing
	// row and the worst hm- row, each with its wait; the pane waits run
	// at MaxWaits; the measured node carries its poll_status at the
	// bound.
	jobsRow := size(inv.Jobs[0])
	if h := size(hmRow); h > jobsRow {
		jobsRow = h
	}
	total := 1024 +
		refJobs*(jobsRow+size(*jobWait)) +
		refSessions*(size(sessRow)+size(*jobWait)) +
		MaxPolicies*size(inv.Politicas[0]) +
		MaxFolders*size(inv.Pastas[0]) +
		MaxClaims*size(inv.Claims[0]) +
		MaxNodes*size(inv.Nodes[0]) +
		MaxWaits*size(*paneWait)
	if total > MaxInventoryBytes {
		t.Fatalf("worst-case inventory model = %d bytes, want at most MaxInventoryBytes (%d)", total, MaxInventoryBytes)
	}
	t.Logf("worst-case inventory model: %d bytes of %d (headroom %d)", total, MaxInventoryBytes, MaxInventoryBytes-total)
}
