package registry

import (
	"database/sql"
	"errors"
	"strings"
	"testing"
)

var unexpiredClaimSQL = `INSERT INTO claims(id, agente, maquina, criado, criado_unix, expira, expira_unix) VALUES(?, 'dja', 'mac-b', ?, ?, ?, ?)`

func seedClaim(t *testing.T, s *Store, id string) {
	t.Helper()
	if _, err := s.db.Exec(unexpiredClaimSQL, id, "2026-10-10T12:00:00-03:00", fixedNow.Unix(), "2026-10-10T13:00:00-03:00", fixedNow.Unix()+3600); err != nil {
		t.Fatalf("seed the claim: %v", err)
	}
}

// TestRelease: an unknown or expired id is ErrNotFound; a malformed id is
// ErrInvalid; a live claim is deleted.
// Mutation captured: a Release that ignores the expiry predicate deletes
// the expired claim instead of reporting ErrNotFound.
// Mutation captured: a Release that ignores the expiry predicate deletes
// the expired claim and leaves it unreported.
func TestRelease(t *testing.T) {
	s := openStore(t)

	if err := s.Release("not-an-id"); !errors.Is(err, ErrInvalid) {
		t.Errorf("Release(malformed) = %v, want ErrInvalid", err)
	}
	if err := s.Release(strings.Repeat("A", 32)); !errors.Is(err, ErrInvalid) {
		t.Errorf("Release(upper-case hex) = %v, want ErrInvalid", err)
	}
	if err := s.Release(strings.Repeat("0", 31)); !errors.Is(err, ErrInvalid) {
		t.Errorf("Release(31 hex) = %v, want ErrInvalid", err)
	}
	if err := s.Release(strings.Repeat("9", 32)); !errors.Is(err, ErrNotFound) {
		t.Errorf("Release(unknown) = %v, want ErrNotFound", err)
	}

	if _, err := s.db.Exec(`INSERT INTO claims(id, agente, criado, criado_unix, expira, expira_unix) VALUES(?, 'dja', ?, ?, ?, ?)`,
		strings.Repeat("2", 32), "2026-10-10T00:00:00-03:00", -3600, "2026-10-10T01:00:00-03:00", -3540); err != nil {
		t.Fatalf("seed the expired claim: %v", err)
	}
	if err := s.Release(strings.Repeat("2", 32)); !errors.Is(err, ErrNotFound) {
		t.Errorf("Release(expired) = %v, want ErrNotFound", err)
	}

	seedClaim(t, s, strings.Repeat("3", 32))
	if err := s.Release(strings.Repeat("3", 32)); err != nil {
		t.Fatalf("Release(live): %v", err)
	}
	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM claims WHERE expira_unix > ?`, fixedNow.Unix()).Scan(&n); err != nil {
		t.Fatalf("claim count: %v", err)
	}
	if n != 0 {
		t.Errorf("unexpired claims left = %d, want 0", n)
	}
}

func policyRow(t *testing.T, s *Store, escopo, valor string) (hold int, prio sql.NullInt64, motivo sql.NullString, atualizado string) {
	t.Helper()
	if err := s.db.QueryRow(`SELECT hold, prioridade, motivo, atualizado FROM policies WHERE escopo = ? AND valor = ?`, escopo, valor).
		Scan(&hold, &prio, &motivo, &atualizado); err != nil {
		t.Fatalf("policy row: %v", err)
	}
	return
}

// TestPolicy: SetPolicy validates and upserts, ClearPolicy deletes or
// reports ErrNotFound.
// Mutation captured: an upsert that updates atualizado to a stale value,
// or that skips the card upper-casing, fails the row assertions.
func TestPolicy(t *testing.T) {
	s := openStore(t)

	// Nothing to record is refused.
	if err := s.SetPolicy(PolicyInput{Escopo: "agente", Valor: "dja"}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("SetPolicy(hold=false, no priority) = %v, want ErrInvalid", err)
	}
	// The scope rules apply to the valor.
	for _, in := range []PolicyInput{
		{Escopo: "bogus", Valor: "x", Hold: true},
		{Escopo: "agente", Valor: "DJA", Hold: true},
		{Escopo: "card", Valor: "DJA229", Hold: true},
		{Escopo: "projeto", Valor: "acme", Hold: true},
		{Escopo: "agente", Valor: "dja", Hold: true, Prioridade: ptr(1001)},
		{Escopo: "agente", Valor: "dja", Hold: true, Prioridade: ptr(-1001)},
		{Escopo: "agente", Valor: "dja", Hold: true, Motivo: strings.Repeat("m", 281)},
	} {
		if err := s.SetPolicy(in); !errors.Is(err, ErrInvalid) {
			t.Errorf("SetPolicy(%+v) = %v, want ErrInvalid", in, err)
		}
	}

	// A hold is stored, upper-casing the card scope.
	if err := s.SetPolicy(PolicyInput{Escopo: "card", Valor: "dja-229", Hold: true, Motivo: "on hold"}); err != nil {
		t.Fatalf("SetPolicy(card hold): %v", err)
	}
	hold, prio, motivo, atualizado := policyRow(t, s, "card", "DJA-229")
	if hold != 1 || prio.Valid || motivo.String != "on hold" || atualizado != "2026-10-10T12:00:00-03:00" {
		t.Errorf("policy row = hold %d prio %v motivo %q atualizado %q, want the stored hold", hold, prio, motivo.String, atualizado)
	}

	// An upsert replaces the row, keeping one record.
	p := 7
	if err := s.SetPolicy(PolicyInput{Escopo: "card", Valor: "dja-229", Hold: false, Prioridade: &p}); err != nil {
		t.Fatalf("SetPolicy(upsert): %v", err)
	}
	hold, prio, motivo, _ = policyRow(t, s, "card", "DJA-229")
	if hold != 0 || !prio.Valid || prio.Int64 != 7 || motivo.Valid {
		t.Errorf("policy row after upsert = hold %d prio %v motivo %q, want the priority", hold, prio, motivo.String)
	}
	var rows int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM policies`).Scan(&rows); err != nil {
		t.Fatalf("policy count: %v", err)
	}
	if rows != 1 {
		t.Errorf("policy rows = %d, want 1", rows)
	}

	// ClearPolicy deletes; a second clear is ErrNotFound.
	if err := s.ClearPolicy("card", "dja-229"); err != nil {
		t.Fatalf("ClearPolicy: %v", err)
	}
	if err := s.ClearPolicy("card", "dja-229"); !errors.Is(err, ErrNotFound) {
		t.Errorf("second ClearPolicy = %v, want ErrNotFound", err)
	}
	if err := s.ClearPolicy("bogus", "x"); !errors.Is(err, ErrInvalid) {
		t.Errorf("ClearPolicy(bad scope) = %v, want ErrInvalid", err)
	}
}

// TestFolder: SetFolder inserts and upserts keeping criado; ClearFolder
// deletes or reports ErrNotFound; nothing on disk is touched.
// Mutation captured: an upsert that resets criado fails the kept-criado
// assertion.
func TestFolder(t *testing.T) {
	s := openStore(t)

	bad := []FolderInput{
		{Maquina: "bad maquina", Caminho: "data", Pedido: "x"},
		{Maquina: "mac-a", Caminho: "data\nreport", Pedido: "x"},
		{Maquina: "mac-a", Caminho: "data", Pedido: ""},
		{Maquina: "mac-a", Caminho: "data", Pedido: "x", TamanhoBytes: ptr(int64(-1))},
	}
	for _, in := range bad {
		if err := s.SetFolder(in); !errors.Is(err, ErrInvalid) {
			t.Errorf("SetFolder(%+v) = %v, want ErrInvalid", in, err)
		}
	}

	size := int64(4096)
	if err := s.SetFolder(FolderInput{Maquina: "mac-a", Caminho: "data/report", TamanhoBytes: &size, Pedido: "delete after review"}); err != nil {
		t.Fatalf("SetFolder(insert): %v", err)
	}
	criado := func(t *testing.T) string {
		t.Helper()
		var criado string
		if err := s.db.QueryRow(`SELECT criado FROM folders WHERE maquina = 'mac-a' AND caminho = 'data/report'`).Scan(&criado); err != nil {
			t.Fatalf("folder row: %v", err)
		}
		return criado
	}
	first := criado(t)
	if first != "2026-10-10T12:00:00-03:00" {
		t.Errorf("folder criado = %q, want the fixed clock", first)
	}

	if err := s.SetFolder(FolderInput{Maquina: "mac-a", Caminho: "data/report", Pedido: "keep it"}); err != nil {
		t.Fatalf("SetFolder(update): %v", err)
	}
	var count int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM folders`).Scan(&count); err != nil {
		t.Fatalf("folder count: %v", err)
	}
	if count != 1 {
		t.Errorf("folder rows = %d, want 1", count)
	}
	if second := criado(t); second != first {
		t.Errorf("folder criado = %q after the update, want the original %q", second, first)
	}

	if err := s.ClearFolder("mac-a", "data/report"); err != nil {
		t.Fatalf("ClearFolder: %v", err)
	}
	if err := s.ClearFolder("mac-a", "data/report"); !errors.Is(err, ErrNotFound) {
		t.Errorf("second ClearFolder = %v, want ErrNotFound", err)
	}
	if err := s.ClearFolder("bad maquina", "data"); !errors.Is(err, ErrInvalid) {
		t.Errorf("ClearFolder(bad maquina) = %v, want ErrInvalid", err)
	}
}

// TestSetNode: nil fields are unchanged, dropping the flag clears the
// motivo, and the fact fields are validated.
// Mutation captured: an update that rewrites the stored runners (or sets
// ultimo_contato) fails the unchanged-fields assertions.
func TestSetNode(t *testing.T) {
	s := openStore(t)

	if err := s.SetNode(NodeUpdate{Maquina: "bad maquina"}); !errors.Is(err, ErrInvalid) {
		t.Errorf("SetNode(bad maquina) = %v, want ErrInvalid", err)
	}
	if err := s.SetNode(NodeUpdate{Maquina: "mac-a", SohoVersion: ptr(strings.Repeat("s", 121))}); !errors.Is(err, ErrInvalid) {
		t.Errorf("SetNode(long soho_version) = %v, want ErrInvalid", err)
	}
	if err := s.SetNode(NodeUpdate{Maquina: "mac-a", ManutencaoMotivo: ptr("a\nb")}); !errors.Is(err, ErrInvalid) {
		t.Errorf("SetNode(control in motivo) = %v, want ErrInvalid", err)
	}
	if err := s.SetNode(NodeUpdate{Maquina: "mac-a", Runners: []string{"a", "a"}}); !errors.Is(err, ErrInvalid) {
		t.Errorf("SetNode(duplicate runners) = %v, want ErrInvalid", err)
	}
	if err := s.SetNode(NodeUpdate{Maquina: "mac-a", Runners: []string{"Runner-1"}}); !errors.Is(err, ErrInvalid) {
		t.Errorf("SetNode(bad runner) = %v, want ErrInvalid", err)
	}

	m := true
	if err := s.SetNode(NodeUpdate{Maquina: "mac-a", Manutencao: &m, ManutencaoMotivo: ptr("disk full"), SohoVersion: ptr("1.2.3"), Runners: []string{"a", "b"}}); err != nil {
		t.Fatalf("SetNode(initial): %v", err)
	}
	node := nodeRow(t, s, "mac-a")
	if !node.manutencao || node.motivo != "disk full" || node.soho != "1.2.3" || node.runners != `["a","b"]` {
		t.Errorf("node row = %+v, want the initial update", node)
	}
	if node.contato != "" || node.poll != "" {
		t.Errorf("node contact/poll = %q/%q, want untouched", node.contato, node.poll)
	}

	// Nil fields stay.
	if err := s.SetNode(NodeUpdate{Maquina: "mac-a", SohoVersion: ptr("9.9")}); err != nil {
		t.Fatalf("SetNode(only version): %v", err)
	}
	node = nodeRow(t, s, "mac-a")
	if !node.manutencao || node.motivo != "disk full" || node.soho != "9.9" || node.runners != `["a","b"]` {
		t.Errorf("node row = %+v, want the nil fields unchanged", node)
	}

	// Dropping the flag clears the stored motivo.
	f := false
	if err := s.SetNode(NodeUpdate{Maquina: "mac-a", Manutencao: &f}); err != nil {
		t.Fatalf("SetNode(maintenance off): %v", err)
	}
	node = nodeRow(t, s, "mac-a")
	if node.manutencao || node.motivo != "" || node.soho != "9.9" {
		t.Errorf("node row = %+v, want the flag off and the motivo cleared", node)
	}

	// An explicit motivo wins over the same update's clearing flag.
	if err := s.SetNode(NodeUpdate{Maquina: "mac-a", Manutencao: &f, ManutencaoMotivo: ptr("leftover")}); err != nil {
		t.Fatalf("SetNode(flag off with motivo): %v", err)
	}
	node = nodeRow(t, s, "mac-a")
	if node.manutencao || node.motivo != "leftover" {
		t.Errorf("node row = %+v, want the explicit motivo", node)
	}

	// An empty runner list is stored as an empty JSON array.
	if err := s.SetNode(NodeUpdate{Maquina: "mac-a", Runners: []string{}}); err != nil {
		t.Fatalf("SetNode(empty runners): %v", err)
	}
	node = nodeRow(t, s, "mac-a")
	if node.runners != "[]" {
		t.Errorf("node runners = %q, want []", node.runners)
	}

	// A second node is independent.
	if err := s.SetNode(NodeUpdate{Maquina: "mac-b"}); err != nil {
		t.Fatalf("SetNode(mac-b): %v", err)
	}
	if b := nodeRow(t, s, "mac-b"); b.manutencao || b.motivo != "" {
		t.Errorf("mac-b row = %+v, want defaults", b)
	}
	var count int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM nodes`).Scan(&count); err != nil {
		t.Fatalf("node count: %v", err)
	}
	if count != 2 {
		t.Errorf("node rows = %d, want 2", count)
	}
}

type nodeRowT struct {
	maquina, contato, poll, soho, runners, motivo string
	manutencao                                    bool
}

func nodeRow(t *testing.T, s *Store, maquina string) nodeRowT {
	t.Helper()
	var n nodeRowT
	var contato, poll, soho, runners, motivo sql.NullString
	if err := s.db.QueryRow(`SELECT ultimo_contato, ultimo_poll, soho_version, runners, manutencao, manutencao_motivo FROM nodes WHERE maquina = ?`, maquina).
		Scan(&contato, &poll, &soho, &runners, &n.manutencao, &motivo); err != nil {
		t.Fatalf("node row: %v", err)
	}
	n.maquina = maquina
	n.contato = contato.String
	n.poll = poll.String
	n.soho = soho.String
	n.runners = runners.String
	n.motivo = motivo.String
	return n
}
