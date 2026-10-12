package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/djalmajr/nau/internal/outbox"
)

// countingWriter counts the encoded bytes without holding the whole JSON
// in memory: the composed state is bigger than MaxInventoryBytes only if
// the encoding overflows, so the count is the proof.
type countingWriter struct{ n int }

func (w *countingWriter) Write(p []byte) (int, error) {
	w.n += len(p)
	return len(p), nil
}

var _ io.Writer = (*countingWriter)(nil)

// boundVals holds the worst stored value of every kept column: control
// characters (JSON escapes each byte to six bytes) at the column's byte
// bound, or the bound in ASCII when the column's grammar admits no
// control characters. The stored timestamps are the TSLayout form (25
// bytes); the event and record ts inputs are the 35-byte RFC 3339 form.
type boundVals struct {
	tsIn      string // 35-byte record/event ts input
	ts        string // 25-byte stored TSLayout form
	mach      [64]string
	pubJob    string
	jobID     func(n int) string
	jobRef    string
	agente    string
	card      string
	proj      string
	branch    string
	pr        string
	tipo      string
	resumo    string
	refs      map[string]any
	refsJSON  string
	ws1024    string
	ws1024Alt string
	pane      func(n int) string
	name      string
	herdr     string
	soho      string
	maint     string
	runners   []string
	claim     func(n int) string
	ws        string
	cref      string
	pol       func(n int) string
	path      func(n int) string
	job64     string
}

// newBoundVals builds the worst stored values at the fixed clock.
func newBoundVals() boundVals {
	v := boundVals{}
	v.tsIn = "2026-10-10T11:30:00.123456789-03:00"
	v.ts = "2026-10-10T11:30:00-03:00"
	for i := 0; i < MaxNodes; i++ {
		v.mach[i] = fmt.Sprintf("m%02d", i) + strings.Repeat("a", 60)
	}
	v.pubJob = "j" + strings.Repeat("x", 59) // 60 chars: the claim's workspace "job-<id>" stays within its 64
	v.jobID = func(n int) string { return fmt.Sprintf("j%062d", n) + "x" }
	v.jobRef = v.jobID(1000)
	v.agente = strings.Repeat("a", 64)
	v.card = "A" + strings.Repeat("b", 15) + "-123456789"
	v.proj = strings.Repeat("a", 100) + "/" + strings.Repeat("b", 100)
	v.branch = strings.Repeat("e", 200)
	v.pr = "123456789"
	v.tipo = "n" + strings.Repeat("x", 63)
	v.resumo = strings.Repeat("\x01", MaxResumo)
	v.refs = map[string]any{}
	for _, k := range EventRefsKeys {
		v.refs[k] = strings.Repeat("\x01", MaxRefValueBytes)
	}
	b, err := json.Marshal(v.refs)
	if err != nil {
		panic(err)
	}
	v.refsJSON = string(b) // the stored refs text, as reduceRefs writes it
	v.ws1024 = strings.Repeat("\x01", MaxPolledStringBytes)
	v.ws1024Alt = strings.Repeat("\x03", MaxPolledStringBytes)
	v.pane = func(n int) string { return strings.Repeat("\x01", MaxPolledPaneBytes-2) + fmt.Sprintf("%02d", n) }
	v.name = strings.Repeat("\x02", MaxPolledPaneBytes)
	v.herdr = "0." + strings.Repeat("1", 38)
	v.soho = strings.Repeat("s", 120)
	v.maint = strings.Repeat("m", 280)
	v.runners = make([]string, 32)
	for i := range v.runners {
		v.runners[i] = fmt.Sprintf("r%031d", i)
	}
	v.claim = func(n int) string { return fmt.Sprintf("%032x", n+1) }
	v.ws = "w" + strings.Repeat("x", 63)
	v.cref = strings.Repeat("f", 50)
	v.pol = func(n int) string { return strings.Repeat("a", 100) + "/" + fmt.Sprintf("%0100d", n+1) }
	v.path = func(n int) string { return strings.Repeat("\x01", 1020) + fmt.Sprintf("%04d", n) }
	v.job64 = v.jobID(2000)
	return v
}

// TestInventoryComposedStateBound: one database at every cap at once, with
// every value at its worst escaped bound, encoded the way the server
// encodes it, stays under MaxInventoryBytes. The public paths seed one row
// of each table and every pane wait, so the real bounds apply; the bulk
// rows go in through direct SQL inserts of the same stored values (the
// public path would take too long: 999 jobs through Ingest with their
// 20 events each, 999 sessions through Ingest, 999 claims through Check,
// 999 policies through SetPolicy, 999 folders through SetFolder and 62
// nodes through SetNode). The state overcounts on purpose, as the model:
// every jobs-kind row carries a full event ring at the field bounds,
// though the poll workspace rows cannot hold one, and the public job's
// claim-bound facts and the poll's workspace facts are on the same row.
func TestInventoryComposedStateBound(t *testing.T) {
	s := openStore(t)
	v := newBoundVals()
	now := fixedNow
	nowUnix := now.Unix()
	m0, m1 := v.mach[0], v.mach[1]

	// One jobs row through Ingest: 20 events at the field bounds.
	pubTS := "2026-10-10T12:00:00-03:00"
	for i := int64(1); i <= DefaultEventsPerJob; i++ {
		b, _ := json.Marshal(map[string]any{
			"seq": i, "ts": v.tsIn, "tipo": v.tipo,
			"resumo": v.resumo, "refs": v.refs,
		})
		if !mustIngest(t, s, outbox.Record{
			Schema: 1, Seq: i, TS: pubTS,
			Tipo: outbox.TipoJobEvent, Maquina: m0,
			JobID: ptr(v.pubJob), IdempotencyKey: v.pubJob + ":" + fmt.Sprint(i),
			Dados: b,
		}) {
			t.Fatalf("event %d not applied", i)
		}
	}
	// One policy, folder and node through the operator paths.
	if err := s.SetPolicy(PolicyInput{Escopo: EscopoProjeto, Valor: v.pol(0), Hold: true, Prioridade: ptr(-1000), Motivo: v.resumo}); err != nil {
		t.Fatalf("SetPolicy: %v", err)
	}
	if err := s.SetFolder(FolderInput{Maquina: m0, Caminho: v.path(0), TamanhoBytes: ptr(int64(1) << 62), Pedido: v.resumo, Job: v.job64}); err != nil {
		t.Fatalf("SetFolder: %v", err)
	}
	on := true
	if err := s.SetNode(NodeUpdate{Maquina: m0, Manutencao: &on, ManutencaoMotivo: ptr(v.maint), SohoVersion: ptr(v.soho), Runners: v.runners}); err != nil {
		t.Fatalf("SetNode: %v", err)
	}
	// One claim through Check, bound to the public job by its workspace
	// label (the claim's facts fill the row's null fields at the poll).
	res, err := s.Check(CheckRequest{Agente: v.agente, Maquina: m0, Card: v.card, Projeto: v.proj, Branch: v.branch, PR: v.pr, Workspace: "job-" + v.pubJob})
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if res.Status != CheckOK {
		t.Fatalf("check status = %q (conflitos %+v), want ok", res.Status, res.Conflitos)
	}
	// One sessions row through Ingest, after the check: its card, branch
	// and project are the same worst values as the claim's, so an earlier
	// session would make the check a duplicate.
	if !mustIngest(t, s, outbox.Record{
		Schema: 1, Seq: 1, TS: pubTS,
		Tipo: outbox.TipoSession, Maquina: m0,
		IdempotencyKey: "sess:1", Projeto: v.proj,
		Dados: json.RawMessage(fmt.Sprintf(`{"acao":"start","projeto":%q,"branch":%q,"card":%q,"pr":%q}`, v.proj, v.branch, v.card, v.pr)),
	}) {
		t.Fatal("session not applied")
	}
	// Every pane wait through ApplyPoll: MaxWaits blocked panes across
	// two nodes, the pane id and the name at the pane bound, the
	// workspace id at the string bound. The public job's workspace facts
	// come from the same poll.
	waitsPoll := func(m string, first, n int) PollResult {
		var panes []PolledPane
		for i := 0; i < n; i++ {
			panes = append(panes, PolledPane{PaneID: v.pane(first + i), WorkspaceID: v.ws1024, Name: v.name, AgentStatus: "blocked"})
		}
		p := PollResult{Maquina: m, At: now, OK: true, Panes: panes}
		if m == m0 {
			p.Workspaces = []PolledWorkspace{{ID: v.ws1024, Label: "job-" + v.pubJob, AgentStatus: "working"}}
		}
		return p
	}
	const perNode = MaxWaits / 2
	if err := s.ApplyPoll(waitsPoll(m0, 0, perNode)); err != nil {
		t.Fatalf("ApplyPoll(%s): %v", m0, err)
	}
	if err := s.ApplyPoll(waitsPoll(m1, perNode, perNode)); err != nil {
		t.Fatalf("ApplyPoll(%s): %v", m1, err)
	}

	// The bulk rows: direct SQL inserts of the same stored values. One
	// transaction so the 20,000 events commit once.
	tx, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := tx.Exec(sql, args...); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}
	ph := func(n int) string { return strings.Repeat("?,", n-1) + "?" }
	jobCols := `INSERT INTO jobs(maquina, chave, origem, job_id, workspace, workspace_id, agente, agente_fonte, trabalho, ref, card, projeto, branch, pr, fase, herdr_status, terminal, saida, esperando_humano, espera_motivo, ultimo_seq, ultima_atividade, ultima_atividade_unix, obsoleto, fonte_push) VALUES(` + ph(25) + `)`
	evCols := `INSERT INTO events(maquina, chave, seq, ts, tipo, resumo, refs) VALUES(` + ph(7) + `)`
	evStmt, err := tx.Prepare(evCols)
	if err != nil {
		t.Fatalf("prepare events: %v", err)
	}
	// The bulk jobs-kind rows: 998 pushed jobs across the remaining nodes
	// and one worst poll workspace row on the second node. Every row
	// carries its full event ring at the field bounds (the overcount: the
	// poll workspace rows cannot hold events).
	for n := 0; n < MaxJobs-1; n++ {
		m := m1
		if n > 0 {
			m = v.mach[2+n%62]
		}
		id := v.jobID(1000 + n)
		if n > 0 {
			exec(jobCols, m, id, OrigemJob, id, v.ws1024, v.ws1024, v.agente, AgenteFonteClaim, nil, strings.Repeat("f", 54), v.card, v.proj, v.branch, v.pr, v.tipo, "working", 0, nil, 0, nil, int64(DefaultEventsPerJob), v.ts, nowUnix, 1, 1)
		} else {
			// The worst poll workspace row: the ws: key and the label at
			// the bound, the name-derived owner and ref at the label's
			// worst split (the label is 3 + owner + 1 + type + 1 + ref).
			wsLabel := "hm-" + strings.Repeat("a", 1004) + "-impl-" + strings.Repeat("r", 10)
			exec(jobCols, m1, "ws:"+v.ws1024, OrigemWorkspace, nil, wsLabel, v.ws1024, strings.Repeat("a", 1004), AgenteFonteNome, "impl", strings.Repeat("f", 10), nil, nil, nil, nil, v.tipo, "idle", 0, nil, 0, nil, int64(DefaultEventsPerJob), v.ts, nowUnix, 1, 1)
		}
		chave := id
		if n == 0 {
			chave = "ws:" + v.ws1024
		}
		for seq := 1; seq <= DefaultEventsPerJob; seq++ {
			if _, err := evStmt.Exec(m, chave, seq, v.ts, v.tipo, v.resumo, v.refsJSON); err != nil {
				t.Fatalf("event insert: %v", err)
			}
		}
	}
	sessCols := `INSERT INTO jobs(maquina, chave, origem, job_id, card, branch, pr, projeto, fonte_push, sessao_ts_unix, sessao_seq, encerrado, ultima_atividade, ultima_atividade_unix) VALUES(` + ph(14) + `)`
	for n := 0; n < MaxSessions-1; n++ {
		m := v.mach[1+n%63]
		// The key is the row's own projeto and branch, as the public
		// path builds it; the first projeto part carries a unique run so
		// the key is unique per machine at the same bound.
		proj := strings.Repeat("a", 97) + fmt.Sprintf("%03d", n) + "/" + strings.Repeat("b", 100)
		exec(sessCols, m, "sessao:"+proj+"#"+v.branch, OrigemSessao, nil, v.card, v.branch, v.pr, proj, 1, nowUnix, int64(n+1), 0, v.ts, nowUnix)
	}
	claimCols := `INSERT INTO claims(id, agente, maquina, card, projeto, branch, pr, workspace, ref, criado, criado_unix, expira, expira_unix) VALUES(` + ph(13) + `)`
	expira := now.Add(time.Hour).Format(outbox.TSLayout)
	for n := 0; n < MaxClaims-1; n++ {
		exec(claimCols, v.claim(1000+n), v.agente, v.mach[n%64], v.card, v.proj, v.branch, v.pr, v.ws, v.cref, v.ts, nowUnix, expira, nowUnix+3600)
	}
	polCols := `INSERT INTO policies(escopo, valor, hold, prioridade, motivo, atualizado) VALUES(` + ph(6) + `)`
	for n := 0; n < MaxPolicies-1; n++ {
		exec(polCols, EscopoProjeto, v.pol(1000+n), 1, -1000, v.resumo, v.ts)
	}
	foldCols := `INSERT INTO folders(maquina, caminho, tamanho_bytes, pedido, job, criado) VALUES(` + ph(6) + `)`
	for n := 0; n < MaxFolders-1; n++ {
		exec(foldCols, v.mach[2+n%62], v.path(1000+n), int64(1)<<62, v.resumo, v.job64, v.ts)
	}
	nodeCols := `INSERT INTO nodes(maquina, ultimo_contato, ultimo_contato_unix, ultimo_poll, poll_status, herdr_version, soho_version, runners, manutencao, manutencao_motivo) VALUES(` + ph(10) + `)`
	for n := 2; n < MaxNodes; n++ {
		exec(nodeCols, v.mach[n], v.ts, nowUnix, v.ts, v.ws1024, v.herdr, v.soho, runnersJSON(v.runners), 1, v.maint)
	}
	if err := evStmt.Close(); err != nil {
		t.Fatalf("close events: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// The two polled nodes age through a failed poll: the poll_status at
	// the bound and the jobs of the nodes marked obsolete.
	if err := s.ApplyPoll(PollResult{Maquina: m0, At: now, OK: false, Motivo: strings.Repeat("\x05", MaxPolledStringBytes)}); err != nil {
		t.Fatalf("ApplyPoll(failed %s): %v", m0, err)
	}
	if err := s.ApplyPoll(PollResult{Maquina: m1, At: now, OK: false, Motivo: strings.Repeat("\x06", MaxPolledStringBytes)}); err != nil {
		t.Fatalf("ApplyPoll(failed %s): %v", m1, err)
	}

	inv, err := s.Inventory(Filter{})
	if err != nil {
		t.Fatalf("Inventory: %v", err)
	}
	if len(inv.Nodes) != MaxNodes || len(inv.Jobs) != MaxJobs+MaxSessions || len(inv.Esperas) != MaxWaits || len(inv.Claims) != MaxClaims || len(inv.Politicas) != MaxPolicies || len(inv.Pastas) != MaxFolders {
		t.Fatalf("inventory rows = %d nodes, %d jobs, %d esperas, %d claims, %d politicas, %d pastas; want every table at its cap", len(inv.Nodes), len(inv.Jobs), len(inv.Esperas), len(inv.Claims), len(inv.Politicas), len(inv.Pastas))
	}
	evTotal := 0
	for i := range inv.Jobs {
		if inv.Jobs[i].Origem != OrigemSessao {
			evTotal += len(inv.Jobs[i].Eventos)
		}
	}
	if evTotal != MaxJobs*DefaultEventsPerJob {
		t.Fatalf("jobs-kind events = %d, want %d", evTotal, MaxJobs*DefaultEventsPerJob)
	}
	// The server encodes the inventory with json.Marshal plus the newline;
	// the encoder writes the same bytes to the counting writer without
	// holding the whole answer in memory.
	var w countingWriter
	if err := json.NewEncoder(&w).Encode(inv); err != nil {
		t.Fatalf("encode: %v", err)
	}
	if w.n > MaxInventoryBytes {
		t.Fatalf("composed inventory = %d bytes, want at most MaxInventoryBytes (%d)", w.n, MaxInventoryBytes)
	}
	t.Logf("composed inventory: %d bytes of %d (headroom %d)", w.n, MaxInventoryBytes, MaxInventoryBytes-w.n)
}

// runnersJSON is the stored runners list, as SetNode stores it.
func runnersJSON(runners []string) string {
	b, err := json.Marshal(runners)
	if err != nil {
		panic(err)
	}
	return string(b)
}
