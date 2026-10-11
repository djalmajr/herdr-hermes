package registry

import (
	"errors"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
)

var claimIDRe = regexp.MustCompile(`^[0-9a-f]{32}$`)

// insertJob writes one jobs row directly (Ingest and ApplyPoll are a later
// slice; the check tests need active rows without them).
func insertJob(t *testing.T, s *Store, m map[string]any) {
	t.Helper()
	type kv struct {
		col string
		v   any
	}
	var kvs []kv
	for k, v := range m {
		kvs = append(kvs, kv{k, v})
	}
	sort.Slice(kvs, func(i, j int) bool { return kvs[i].col < kvs[j].col })
	cols := make([]string, len(kvs))
	args := make([]any, len(kvs))
	for i, e := range kvs {
		cols[i] = e.col
		args[i] = e.v
	}
	query := "INSERT INTO jobs(" + strings.Join(cols, ",") + ") VALUES(" + strings.Repeat("?,", len(cols)-1) + "?)"
	if _, err := s.db.Exec(query, args...); err != nil {
		t.Fatalf("insertJob(%v): %v", m, err)
	}
}

// strEq reports whether a pointer holds the wanted string.
func strEq(p *string, want string) bool { return p != nil && *p == want }

// TestCheck: a free request gets a claim; every key of the request finds
// its own conflict; terminal and tombstoned jobs never conflict; expired
// claims are deleted; live claims of another request conflict.
// Mutation captured: a claim that skips the expired-claim delete passes an
// expired claim as free; a key match dropped from the job comparison (or a
// terminal=0 condition missing from the query) leaves its duplicate row
// returning ok.
func TestCheck(t *testing.T) {
	t.Run("ok free request", func(t *testing.T) {
		s := openStore(t)
		res, err := s.Check(CheckRequest{Agente: "dja", Maquina: "mac-a", Card: "dja-229"})
		if err != nil {
			t.Fatalf("Check: %v", err)
		}
		if res.Status != CheckOK || res.Claim == nil {
			t.Fatalf("status = %q claim = %+v, want ok with a claim", res.Status, res.Claim)
		}
		c := res.Claim
		if !claimIDRe.MatchString(c.ID) {
			t.Errorf("claim id %q, want 32 lower-case hex", c.ID)
		}
		if c.Agente != "dja" || !strEq(c.Card, "DJA-229") || !strEq(c.Maquina, "mac-a") || c.Workspace != nil || c.Projeto != nil {
			t.Errorf("claim = %+v, want dja DJA-229 mac-a with the unset fields null", c)
		}
		// The fixed clock: criado at 12:00, expira one hour later, both in
		// the injected zone, never Z.
		if c.Criado != "2026-10-10T12:00:00-03:00" || c.Expira != "2026-10-10T13:00:00-03:00" {
			t.Errorf("claim criado = %q expira = %q, want the fixed clock and the default ttl", c.Criado, c.Expira)
		}
		var expiraUnix int64
		if err := s.db.QueryRow(`SELECT expira_unix FROM claims WHERE id = ?`, c.ID).Scan(&expiraUnix); err != nil {
			t.Fatalf("claim row: %v", err)
		}
		if expiraUnix != fixedNow.Add(DefaultClaimTTL).Unix() {
			t.Errorf("expira_unix = %d, want %d", expiraUnix, fixedNow.Add(DefaultClaimTTL).Unix())
		}
	})

	t.Run("ok hm workspace", func(t *testing.T) {
		s := openStore(t)
		res, err := s.Check(CheckRequest{Agente: "dja", Workspace: "hm-dja-impl-42"})
		if err != nil {
			t.Fatalf("Check: %v", err)
		}
		if res.Status != CheckOK || res.Claim == nil {
			t.Fatalf("status = %q, want ok", res.Status)
		}
		if !strEq(res.Claim.Workspace, "hm-dja-impl-42") || !strEq(res.Claim.Ref, "42") || res.Claim.Card != nil {
			t.Errorf("claim = %+v, want the workspace and the hm ref", res.Claim)
		}
	})

	t.Run("duplicate per key", func(t *testing.T) {
		cases := []struct {
			name    string
			job     map[string]any
			req     CheckRequest
			wantKey [2]string
		}{
			{
				name:    "workspace",
				job:     map[string]any{"maquina": "mac-a", "chave": "ws:1", "origem": "workspace", "workspace": "hm-dja-impl-42"},
				req:     CheckRequest{Agente: "dja", Workspace: "hm-dja-impl-42"},
				wantKey: [2]string{"workspace", "hm-dja-impl-42"},
			},
			{
				name:    "card",
				job:     map[string]any{"maquina": "mac-a", "chave": "job-1", "origem": "job", "card": "DJA-229"},
				req:     CheckRequest{Agente: "dja", Card: "dja-229"},
				wantKey: [2]string{"card", "DJA-229"},
			},
			{
				name:    "branch projeto case",
				job:     map[string]any{"maquina": "mac-a", "chave": "job-2", "origem": "job", "projeto": "Acme/Repo", "branch": "feat/x"},
				req:     CheckRequest{Agente: "dja", Projeto: "acme/repo", Branch: "feat/x"},
				wantKey: [2]string{"branch", "feat/x"},
			},
			{
				name:    "pr url form",
				job:     map[string]any{"maquina": "mac-a", "chave": "job-3", "origem": "job", "projeto": "acme/repo", "pr": "67"},
				req:     CheckRequest{Agente: "dja", Projeto: "ACME/REPO", PR: "https://example.org/acme/repo/pull/67/files"},
				wantKey: [2]string{"pr", "67"},
			},
			{
				name:    "ref two hm workspaces",
				job:     map[string]any{"maquina": "mac-a", "chave": "ws:2", "origem": "workspace", "workspace": "hm-dja-impl-42", "agente": "dja", "ref": "42"},
				req:     CheckRequest{Agente: "dja", Workspace: "hm-dja-review-42"},
				wantKey: [2]string{"ref", "42"},
			},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				s := openStore(t)
				insertJob(t, s, tc.job)
				res, err := s.Check(tc.req)
				if err != nil {
					t.Fatalf("Check: %v", err)
				}
				if res.Status != CheckDuplicate {
					t.Fatalf("status = %q, want duplicate", res.Status)
				}
				if res.Claim != nil {
					t.Fatalf("claim = %+v, want none", res.Claim)
				}
				if len(res.Conflitos) != 1 {
					t.Fatalf("conflitos = %+v, want exactly one", res.Conflitos)
				}
				c := res.Conflitos[0]
				if c.Tipo != ConflitoJob || c.Campo != tc.wantKey[0] || c.Valor != tc.wantKey[1] {
					t.Errorf("conflict = %+v, want job %s %s", c, tc.wantKey[0], tc.wantKey[1])
				}
				if !strEq(c.Maquina, "mac-a") {
					t.Errorf("conflict maquina = %v, want mac-a", c.Maquina)
				}
				if !strEq(c.Chave, tc.job["chave"].(string)) {
					t.Errorf("conflict chave = %v, want %v", c.Chave, tc.job["chave"])
				}
			})
		}
	})

	t.Run("same job several keys", func(t *testing.T) {
		s := openStore(t)
		insertJob(t, s, map[string]any{"maquina": "mac-a", "chave": "job-9", "origem": "job", "card": "DJA-229", "projeto": "acme/repo", "branch": "feat/x"})
		res, err := s.Check(CheckRequest{Agente: "dja", Card: "dja-229", Projeto: "acme/repo", Branch: "feat/x"})
		if err != nil {
			t.Fatalf("Check: %v", err)
		}
		if res.Status != CheckDuplicate {
			t.Fatalf("status = %q, want duplicate", res.Status)
		}
		var campos []string
		for _, c := range res.Conflitos {
			campos = append(campos, c.Campo)
		}
		sort.Strings(campos)
		if !equalStrings(campos, []string{"branch", "card"}) {
			t.Errorf("conflict campos = %v, want one per matched key", campos)
		}
	})

	t.Run("terminal and tombstoned never conflict", func(t *testing.T) {
		cases := []struct {
			name string
			job  map[string]any
		}{
			{name: "terminal", job: map[string]any{"maquina": "mac-a", "chave": "job-4", "origem": "job", "card": "DJA-229", "terminal": 1}},
			{name: "tombstoned", job: map[string]any{"maquina": "mac-a", "chave": "job-5", "origem": "job", "card": "DJA-229", "encerrado": 1}},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				s := openStore(t)
				insertJob(t, s, tc.job)
				res, err := s.Check(CheckRequest{Agente: "dja", Card: "dja-229"})
				if err != nil {
					t.Fatalf("Check: %v", err)
				}
				if res.Status != CheckOK || res.Claim == nil {
					t.Fatalf("status = %q, want ok for a %s job", res.Status, tc.name)
				}
			})
		}
	})

	t.Run("expired claim deleted and free", func(t *testing.T) {
		s := openStore(t)
		_, err := s.db.Exec(`INSERT INTO claims(id, agente, card, criado, criado_unix, expira, expira_unix)
			VALUES(?, 'dja', 'DJA-229', '2026-10-10T00:00:00-03:00', -3600, '2026-10-10T01:00:00-03:00', -3540)`,
			strings.Repeat("0", 32))
		if err != nil {
			t.Fatalf("insert the expired claim: %v", err)
		}
		res, err := s.Check(CheckRequest{Agente: "dja", Card: "dja-229"})
		if err != nil {
			t.Fatalf("Check: %v", err)
		}
		if res.Status != CheckOK {
			t.Fatalf("status = %q, want ok: an expired claim never conflicts", res.Status)
		}
		var count int
		if err := s.db.QueryRow(`SELECT COUNT(*) FROM claims WHERE id != ?`, res.Claim.ID).Scan(&count); err != nil {
			t.Fatalf("claim count: %v", err)
		}
		if count != 0 {
			t.Errorf("expired claim rows left = %d, want 0 (deleted by the check)", count)
		}
	})

	t.Run("claim of another request", func(t *testing.T) {
		s := openStore(t)
		_, err := s.db.Exec(`INSERT INTO claims(id, agente, maquina, card, criado, criado_unix, expira, expira_unix)
			VALUES(?, 'dja', 'mac-b', 'DJA-229', ?, ?, ?, ?)`,
			strings.Repeat("1", 32), "2026-10-10T12:00:00-03:00", fixedNow.Unix(), "2026-10-10T13:00:00-03:00", fixedNow.Unix()+3600)
		if err != nil {
			t.Fatalf("insert the claim: %v", err)
		}
		res, err := s.Check(CheckRequest{Agente: "dja", Maquina: "mac-a", Card: "dja-229"})
		if err != nil {
			t.Fatalf("Check: %v", err)
		}
		if res.Status != CheckDuplicate || len(res.Conflitos) != 1 {
			t.Fatalf("status = %q conflitos = %+v, want one claim conflict", res.Status, res.Conflitos)
		}
		c := res.Conflitos[0]
		if c.Tipo != ConflitoClaim || !strEq(c.Chave, strings.Repeat("1", 32)) || !strEq(c.Agente, "dja") || !strEq(c.Maquina, "mac-b") {
			t.Errorf("conflict = %+v, want the claim id, agente and maquina", c)
		}
	})
}

// TestCheckHolds: a hold on the agent, the project (other case) or the
// card blocks with the hold conflicts first; a priority-only policy never
// blocks.
// Mutation captured: a hold query that skips hold = 1 (or the LOWER on
// the project scope) lets the priority row or the case-different project
// hold through.
func TestCheckHolds(t *testing.T) {
	seed := func(t *testing.T, s *Store, escopo, valor string, hold int, prio *int, motivo *string) {
		t.Helper()
		if _, err := s.db.Exec(`INSERT INTO policies(escopo, valor, hold, prioridade, motivo, atualizado) VALUES(?,?,?,?,?,?)`,
			escopo, valor, hold, prio, motivo, "2026-10-10T12:00:00-03:00"); err != nil {
			t.Fatalf("seed the policy: %v", err)
		}
	}

	t.Run("agent hold", func(t *testing.T) {
		s := openStore(t)
		m := "hold the agent"
		seed(t, s, "agente", "dja", 1, nil, &m)
		res, err := s.Check(CheckRequest{Agente: "dja", Card: "dja-229"})
		if err != nil {
			t.Fatalf("Check: %v", err)
		}
		if res.Status != CheckHold || len(res.Conflitos) != 1 {
			t.Fatalf("status = %q, want hold with one conflict", res.Status)
		}
		c := res.Conflitos[0]
		if c.Tipo != ConflitoHold || c.Campo != CampoAgente || c.Valor != "dja" || !strEq(c.Motivo, m) {
			t.Errorf("conflict = %+v, want the agent hold with its motivo", c)
		}
	})

	t.Run("project hold other case", func(t *testing.T) {
		s := openStore(t)
		seed(t, s, "projeto", "Acme/Repo", 1, nil, nil)
		res, err := s.Check(CheckRequest{Agente: "dja", Projeto: "acme/repo", Branch: "feat/x"})
		if err != nil {
			t.Fatalf("Check: %v", err)
		}
		if res.Status != CheckHold || len(res.Conflitos) != 1 {
			t.Fatalf("status = %q, want hold with one conflict", res.Status)
		}
		c := res.Conflitos[0]
		if c.Tipo != ConflitoHold || c.Campo != CampoProjeto || c.Valor != "Acme/Repo" {
			t.Errorf("conflict = %+v, want the project hold with the stored case", c)
		}
	})

	t.Run("card hold", func(t *testing.T) {
		s := openStore(t)
		seed(t, s, "card", "DJA-229", 1, nil, nil)
		res, err := s.Check(CheckRequest{Agente: "dja", Card: "dja-229"})
		if err != nil {
			t.Fatalf("Check: %v", err)
		}
		if res.Status != CheckHold || len(res.Conflitos) != 1 {
			t.Fatalf("status = %q, want hold with one conflict", res.Status)
		}
		if res.Conflitos[0].Campo != CampoCard || res.Conflitos[0].Valor != "DJA-229" {
			t.Errorf("conflict = %+v, want the card hold", res.Conflitos[0])
		}
	})

	t.Run("holds first with a job conflict", func(t *testing.T) {
		s := openStore(t)
		seed(t, s, "agente", "dja", 1, nil, nil)
		insertJob(t, s, map[string]any{"maquina": "mac-a", "chave": "job-7", "origem": "job", "card": "DJA-229"})
		res, err := s.Check(CheckRequest{Agente: "dja", Card: "dja-229"})
		if err != nil {
			t.Fatalf("Check: %v", err)
		}
		if res.Status != CheckHold || len(res.Conflitos) != 2 {
			t.Fatalf("status = %q conflitos = %+v, want hold with both conflicts", res.Status, res.Conflitos)
		}
		if res.Conflitos[0].Tipo != ConflitoHold || res.Conflitos[1].Tipo != ConflitoJob {
			t.Errorf("conflict order = [%s %s], want the hold first", res.Conflitos[0].Tipo, res.Conflitos[1].Tipo)
		}
	})

	t.Run("priority only never blocks", func(t *testing.T) {
		s := openStore(t)
		p := 7
		m := "preferred first"
		seed(t, s, "agente", "dja", 0, &p, &m)
		res, err := s.Check(CheckRequest{Agente: "dja", Card: "dja-229"})
		if err != nil {
			t.Fatalf("Check: %v", err)
		}
		if res.Status != CheckOK || res.Claim == nil {
			t.Fatalf("status = %q, want ok: a priority-only policy never blocks", res.Status)
		}
	})
}

// TestCheckConcurrent: sixteen checks of the same card on one store leave
// exactly one claim; the same through two stores on one file leaves
// exactly one claim across both.
// Mutation captured: the check running in two transactions (or without the
// immediate lock) lets two winners through.
func TestCheckConcurrent(t *testing.T) {
	req := CheckRequest{Agente: "dja", Card: "dja-229"}

	run := func(t *testing.T, n int, store func() *Store) int {
		t.Helper()
		var wg sync.WaitGroup
		var mu sync.Mutex
		oks := 0
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				res, err := store().Check(req)
				if err != nil {
					mu.Lock()
					defer mu.Unlock()
					t.Errorf("Check: %v", err)
					return
				}
				mu.Lock()
				if res.Status == CheckOK {
					oks++
				}
				mu.Unlock()
			}()
		}
		wg.Wait()
		return oks
	}

	t.Run("one store", func(t *testing.T) {
		s := openStore(t)
		if oks := run(t, 16, func() *Store { return s }); oks != 1 {
			t.Fatalf("ok claims = %d, want exactly 1 of 16", oks)
		}
	})

	t.Run("two stores one file", func(t *testing.T) {
		path := mustStorePath(t)
		s1, err := Open(path, Options{Now: fixedClock()})
		if err != nil {
			t.Fatalf("Open s1: %v", err)
		}
		defer s1.Close()
		s2, err := Open(path, Options{Now: fixedClock()})
		if err != nil {
			t.Fatalf("Open s2: %v", err)
		}
		defer s2.Close()
		total := run(t, 8, func() *Store { return s1 })
		total += run(t, 8, func() *Store { return s2 })
		if total != 1 {
			t.Fatalf("ok claims = %d, want exactly 1 of 16 across two stores", total)
		}
	})
}

func mustStorePath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "registry.db")
}

// TestCheckInvalid: the refused requests, every error wrapping ErrInvalid.
// Mutation captured: a missing owner-equality check passes the wrong-owner
// hm workspace through.
func TestCheckInvalid(t *testing.T) {
	s := openStore(t)
	cases := []struct {
		name string
		req  CheckRequest
	}{
		{name: "no agente", req: CheckRequest{Card: "dja-229"}},
		{name: "no key", req: CheckRequest{Agente: "dja"}},
		{name: "branch without projeto", req: CheckRequest{Agente: "dja", Branch: "feat/x"}},
		{name: "pr without projeto", req: CheckRequest{Agente: "dja", PR: "#67"}},
		{name: "ttl 59", req: CheckRequest{Agente: "dja", Card: "dja-229", TTLS: 59}},
		{name: "ttl 86401", req: CheckRequest{Agente: "dja", Card: "dja-229", TTLS: 86401}},
		{name: "hm workspace owner mismatch", req: CheckRequest{Agente: "other", Workspace: "hm-dja-impl-42"}},
		{name: "bad maquina", req: CheckRequest{Agente: "dja", Maquina: "bad maquina", Card: "dja-229"}},
		{name: "bad workspace", req: CheckRequest{Agente: "dja", Workspace: "hm-dja impl-42"}},
		{name: "bad card", req: CheckRequest{Agente: "dja", Card: "dja229"}},
		{name: "bad pr", req: CheckRequest{Agente: "dja", Projeto: "acme/repo", PR: "067"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.Check(tc.req)
			if !errors.Is(err, ErrInvalid) {
				t.Fatalf("Check = %v, want ErrInvalid", err)
			}
		})
	}
}

// TestCheckRefCollisionSuffix: a workspace re-opened under its collision
// name (hm-<agent>-<type>-<ref>-2) is the same work as the plain ref, in
// both directions, while a ref whose last part is not a one-digit
// collision suffix stays distinct.
// Mutation captured: comparing the raw refs instead of RefBase lets
// hm-example-impl-x245 pass while hm-example-impl-x245-2 is active.
func TestCheckRefCollisionSuffix(t *testing.T) {
	cases := []struct {
		name     string
		stored   string
		request  string
		conflict bool
	}{
		{"active -2, plain request", "x245-2", "hm-example-review-x245", true},
		{"active plain, -3 request", "x245", "hm-example-fix-x245-3", true},
		{"range is not a suffix", "x238-241", "hm-example-impl-x238", false},
		{"part is not a suffix", "x194-p2", "hm-example-impl-x194", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := openStore(t)
			insertJob(t, s, map[string]any{"maquina": "node-a", "chave": "ws:w1", "origem": "workspace", "workspace": "hm-example-impl-" + tc.stored, "agente": "example", "ref": tc.stored})
			res, err := s.Check(CheckRequest{Agente: "example", Workspace: tc.request})
			if err != nil {
				t.Fatalf("Check: %v", err)
			}
			if got := res.Status == CheckDuplicate; got != tc.conflict {
				t.Fatalf("status = %q, want conflict %v (%+v)", res.Status, tc.conflict, res.Conflitos)
			}
		})
	}
}
