package registry

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"
)

// fixedNow is the injected clock of every test: a fixed instant in a fixed
// zone, so a returned timestamp must read 2026-10-10T12:00:00-03:00.
var fixedNow = time.Date(2026, 10, 10, 12, 0, 0, 0, time.FixedZone("BRT", -3*3600))

func fixedClock() func() time.Time {
	return func() time.Time { return fixedNow }
}

// openStore opens a fresh store in a temp dir and closes it on cleanup.
func openStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "registry.db"), Options{Now: fixedClock()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// tableNames lists the user tables of the open store.
func tableNames(t *testing.T, s *Store) []string {
	t.Helper()
	rows, err := s.db.Query(`SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name`)
	if err != nil {
		t.Fatalf("table list: %v", err)
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("table list: %v", err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("table list: %v", err)
	}
	sort.Strings(names)
	return names
}

// TestOpen: the first open creates the seven tables and sets user_version
// to 1 in WAL mode at file mode 0600; a second open reuses the schema; a
// database whose user_version is newer is refused with ErrSchema.
// Mutation captured: a version check that does not reject version 2 (or a
// DSN that drops journal_mode(WAL)) fails the ErrSchema / wal assertions.
func TestOpen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "registry.db")

	s, err := Open(path, Options{Now: fixedClock()})
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	defer s.Close()

	var version int
	if err := s.db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		t.Fatalf("user_version: %v", err)
	}
	if version != SchemaVersion {
		t.Errorf("user_version = %d, want %d", version, SchemaVersion)
	}

	var mode string
	if err := s.db.QueryRow(`PRAGMA journal_mode`).Scan(&mode); err != nil {
		t.Fatalf("journal_mode: %v", err)
	}
	if mode != "wal" {
		t.Errorf("journal_mode = %q, want wal", mode)
	}

	want := []string{"claims", "events", "folders", "jobs", "nodes", "policies", "waits"}
	if got := tableNames(t, s); !equalStrings(got, want) {
		t.Errorf("tables = %v, want %v", got, want)
	}

	if runtime.GOOS != "windows" {
		fi, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat the db file: %v", err)
		}
		if fi.Mode().Perm() != 0o600 {
			t.Errorf("db file mode = %o, want 600", fi.Mode().Perm())
		}
	}

	// A second open reuses the existing schema.
	s2, err := Open(path, Options{Now: fixedClock()})
	if err != nil {
		t.Fatalf("second Open: %v", err)
	}
	s2.Close()

	// A database written by a newer schema is refused.
	if _, err := s.db.Exec(fmt.Sprintf(`PRAGMA user_version = %d`, SchemaVersion+1)); err != nil {
		t.Fatalf("bump user_version: %v", err)
	}
	if _, err := Open(path, Options{}); !errors.Is(err, ErrSchema) {
		t.Fatalf("Open on a newer user_version = %v, want ErrSchema", err)
	}
}

// TestOpenMigratesV1: a database written by schema 1 is upgraded in place
// on open, with or without the sessao_seq column (both layouts were
// written as version 1); its rows survive, every push path works on it
// afterwards, and a second open finds it current.
//
// Mutation captured: without the migration, the job_event ingest fails
// with "no such column: sessao_seq"; an unconditional ALTER fails the
// layout that already has the column with "duplicate column name".
func TestOpenMigratesV1(t *testing.T) {
	v1DDL := strings.Replace(schemaDDL, "sessao_seq INTEGER, ", "", 1)
	if v1DDL == schemaDDL {
		t.Fatal("schemaDDL no longer holds the schema 2 column; update the v1 fixture")
	}
	t.Run("without sessao_seq", func(t *testing.T) { testMigrateV1(t, v1DDL, 1) })
	t.Run("with sessao_seq", func(t *testing.T) { testMigrateV1(t, schemaDDL, 1) })
	// An early schema 2 upgrade added the column without the order
	// repair; the repair runs on every open.
	t.Run("schema 2 without the order repair", func(t *testing.T) { testMigrateV1(t, schemaDDL, 2) })
}

func testMigrateV1(t *testing.T, v1DDL string, oldVersion int) {
	path := pathFor(t)
	db, err := sql.Open("sqlite", dsnFor(path))
	if err != nil {
		t.Fatalf("open v1: %v", err)
	}
	if _, err := db.Exec(v1DDL); err != nil {
		t.Fatalf("v1 DDL: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO jobs(maquina, chave, origem, projeto, branch, fonte_push, sessao_ts_unix, encerrado, ultima_atividade, ultima_atividade_unix)
		VALUES('node-a', 'sessao:example-org/example-repo#feat/x', 'sessao', 'example-org/example-repo', 'feat/x', 1, ?, 0, '2026-10-10T11:00:00-03:00', ?)`, fixedNow.Unix()-3600, fixedNow.Unix()-3600); err != nil {
		t.Fatalf("v1 row: %v", err)
	}
	if _, err := db.Exec(fmt.Sprintf(`PRAGMA user_version = %d`, oldVersion)); err != nil {
		t.Fatalf("v1 version: %v", err)
	}
	db.Close()
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatalf("chmod: %v", err)
	}

	s, err := Open(path, Options{Now: fixedClock()})
	if err != nil {
		t.Fatalf("Open v1: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	var version int
	if err := s.db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil || version != SchemaVersion {
		t.Fatalf("user_version = %d (%v), want %d", version, err, SchemaVersion)
	}
	res, err := s.Check(CheckRequest{Agente: "dja", Projeto: "example-org/example-repo", Branch: "feat/x"})
	if err != nil || res.Status != CheckDuplicate {
		t.Fatalf("check for the v1 session branch = %+v (%v), want duplicate", res, err)
	}
	mustIngest(t, s, recordBase(nil))
	// The stored session's outbox seq is unknown: a record of its very
	// second is refused, whatever its seq.
	if mustIngest(t, s, sessaoRec("2026-10-10T11:00:00-03:00", "update", "card-7", "feat/x", "", "")) {
		t.Error("same-second record after the migration: applied = true, want false")
	}
	if f := jobFact(t, s, "node-a", "sessao:example-org/example-repo#feat/x"); f.card != nil {
		t.Errorf("v1 session after a same-second record: row = %+v, want unchanged", f)
	}
	if !mustIngest(t, s, sessaoRec("2026-10-10T11:30:00-03:00", "end", "", "feat/x", "", "")) {
		t.Fatal("end of the v1 session: applied = false")
	}
	if f := jobFact(t, s, "node-a", "sessao:example-org/example-repo#feat/x"); f.encerrado != 1 {
		t.Errorf("v1 session after its end: row = %+v, want tombstoned", f)
	}
	s.Close()
	s2, err := Open(path, Options{Now: fixedClock()})
	if err != nil {
		t.Fatalf("reopen of the migrated database: %v", err)
	}
	s2.Close()
}

// TestOpenParentDirMode: the created parent directory is 0700.
// Mutation captured: MkdirAll called with 0755 fails this assertion.
func TestOpenParentDirMode(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unix modes")
	}
	nested := filepath.Join(t.TempDir(), "a", "b")
	s, err := Open(filepath.Join(nested, "registry.db"), Options{Now: fixedClock()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()
	fi, err := os.Stat(nested)
	if err != nil {
		t.Fatalf("stat the nested dir: %v", err)
	}
	if fi.Mode().Perm() != 0o700 {
		t.Errorf("nested dir mode = %o, want 700", fi.Mode().Perm())
	}
}

// TestOpenPathWithSpace: a path with spaces opens, writes and reads.
// Mutation captured: a DSN built without percent-escaping the path fails
// to open the file.
func TestOpenPathWithSpace(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "registry data")
	s, err := Open(filepath.Join(dir, "reg istry.db"), Options{Now: fixedClock()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()
	res, err := s.Check(CheckRequest{Agente: "dja", Card: "dja-229"})
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if res.Status != CheckOK || res.Claim == nil {
		t.Fatalf("status = %q, want ok with a claim", res.Status)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestRegistryTimestampsUTCOffset: every timestamp the registry writes or
// returns is TSLayout in the clock's own location; with a UTC clock the
// inventory's generated instant, a claim's created/expiry and a node's
// last poll end with +00:00, never Z (R6).
func TestRegistryTimestampsUTCOffset(t *testing.T) {
	utcNow := time.Date(2026, 10, 10, 12, 0, 0, 0, time.FixedZone("UTC", 0))
	now := func() time.Time { return utcNow }
	s, err := Open(pathFor(t), Options{Now: now})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	inv, err := s.Inventory(Filter{})
	if err != nil {
		t.Fatalf("Inventory: %v", err)
	}
	if strings.HasSuffix(inv.Gerado, "Z") || !strings.HasSuffix(inv.Gerado, "+00:00") {
		t.Errorf("gerado = %q, want a +00:00 numeric offset", inv.Gerado)
	}

	res, err := s.Check(CheckRequest{Agente: "dja", Card: "dja-229"})
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if res.Claim == nil || !strings.HasSuffix(res.Claim.Criado, "+00:00") || !strings.HasSuffix(res.Claim.Expira, "+00:00") {
		t.Errorf("claim criado = %q expira = %q, want the +00:00 numeric offset", res.Claim.Criado, res.Claim.Expira)
	}

	if err := s.ApplyPoll(PollResult{Maquina: "node-a", At: utcNow, OK: false, Motivo: "probe_timeout"}); err != nil {
		t.Fatalf("ApplyPoll: %v", err)
	}
	inv, err = s.Inventory(Filter{})
	if err != nil {
		t.Fatalf("Inventory: %v", err)
	}
	var poll *string
	for _, n := range inv.Nodes {
		if n.Maquina == "node-a" {
			poll = n.UltimoPoll
		}
	}
	if poll == nil || !strings.HasSuffix(*poll, "+00:00") {
		t.Errorf("ultimo_poll = %v, want a +00:00 numeric offset", poll)
	}
}

// TestOpenUnsafeState: a symlinked or non-regular database or sidecar, an
// existing database or sidecar with group or other mode bits, and a
// parent directory writable by group or other are all refused with
// ErrUnsafeState (F2/F3); the message names the file and the reason,
// never its content.
func TestOpenUnsafeState(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unix file modes and symlinks")
	}
	t.Run("symlinked db", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "real.db"), nil, 0o600); err != nil {
			t.Fatalf("seed: %v", err)
		}
		if err := os.Symlink(filepath.Join(dir, "real.db"), filepath.Join(dir, "registry.db")); err != nil {
			t.Fatalf("symlink: %v", err)
		}
		if _, err := Open(filepath.Join(dir, "registry.db"), Options{Now: fixedClock()}); !errors.Is(err, ErrUnsafeState) {
			t.Fatalf("Open on a symlinked db = %v, want ErrUnsafeState", err)
		}
	})
	t.Run("symlinked sidecar", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "registry.db")
		s, err := Open(path, Options{Now: fixedClock()})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		s.Close()
		if err := os.Symlink(path, path+"-wal"); err != nil {
			t.Fatalf("symlink: %v", err)
		}
		if _, err := Open(path, Options{Now: fixedClock()}); !errors.Is(err, ErrUnsafeState) {
			t.Fatalf("Open on a symlinked sidecar = %v, want ErrUnsafeState", err)
		}
	})
	t.Run("group-readable db", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "registry.db")
		s, err := Open(path, Options{Now: fixedClock()})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		s.Close()
		if err := os.Chmod(path, 0o644); err != nil {
			t.Fatalf("chmod: %v", err)
		}
		if _, err := Open(path, Options{Now: fixedClock()}); !errors.Is(err, ErrUnsafeState) {
			t.Fatalf("Open on a 0644 db = %v, want ErrUnsafeState", err)
		}
	})
	t.Run("group-writable parent", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.Chmod(dir, 0o775); err != nil {
			t.Fatalf("chmod: %v", err)
		}
		if _, err := Open(filepath.Join(dir, "registry.db"), Options{Now: fixedClock()}); !errors.Is(err, ErrUnsafeState) {
			t.Fatalf("Open in a 0775 dir = %v, want ErrUnsafeState", err)
		}
	})
	t.Run("shared non-sticky ancestor", func(t *testing.T) {
		shared := filepath.Join(t.TempDir(), "shared")
		state := filepath.Join(shared, "state")
		if err := os.MkdirAll(state, 0o700); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.Chmod(shared, 0o777); err != nil {
			t.Fatalf("chmod: %v", err)
		}
		if _, err := Open(filepath.Join(state, "registry.db"), Options{Now: fixedClock()}); !errors.Is(err, ErrUnsafeState) {
			t.Fatalf("Open under a 0777 non-sticky ancestor = %v, want ErrUnsafeState", err)
		}
	})
	t.Run("missing dir under a shared non-sticky ancestor", func(t *testing.T) {
		shared := filepath.Join(t.TempDir(), "shared")
		if err := os.Mkdir(shared, 0o700); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.Chmod(shared, 0o777); err != nil {
			t.Fatalf("chmod: %v", err)
		}
		state := filepath.Join(shared, "state")
		if _, err := Open(filepath.Join(state, "registry.db"), Options{Now: fixedClock()}); !errors.Is(err, ErrUnsafeState) {
			t.Fatalf("Open of a missing dir under a 0777 non-sticky ancestor = %v, want ErrUnsafeState", err)
		}
		if _, err := os.Lstat(state); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("refused Open created %s (%v)", state, err)
		}
	})
	t.Run("missing dir under a sticky shared dir", func(t *testing.T) {
		shared := filepath.Join(t.TempDir(), "shared")
		if err := os.Mkdir(shared, 0o700); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.Chmod(shared, 0o777|os.ModeSticky); err != nil {
			t.Fatalf("chmod: %v", err)
		}
		s, err := Open(filepath.Join(shared, "state", "registry.db"), Options{Now: fixedClock()})
		if err != nil {
			t.Fatalf("Open of a missing dir under a sticky shared dir: %v", err)
		}
		s.Close()
	})
	t.Run("symlink in a sticky shared dir", func(t *testing.T) {
		shared := filepath.Join(t.TempDir(), "shared")
		real := filepath.Join(t.TempDir(), "real")
		if err := os.Mkdir(shared, 0o700); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.Mkdir(real, 0o700); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.Symlink(real, filepath.Join(shared, "state")); err != nil {
			t.Fatalf("symlink: %v", err)
		}
		if err := os.Chmod(shared, 0o777|os.ModeSticky); err != nil {
			t.Fatalf("chmod: %v", err)
		}
		if _, err := Open(filepath.Join(shared, "state", "registry.db"), Options{Now: fixedClock()}); !errors.Is(err, ErrUnsafeState) {
			t.Fatalf("Open through a symlink in a sticky shared dir = %v, want ErrUnsafeState", err)
		}
	})
	t.Run("own dir in a sticky shared dir", func(t *testing.T) {
		shared := filepath.Join(t.TempDir(), "shared")
		state := filepath.Join(shared, "state")
		if err := os.MkdirAll(state, 0o700); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.Chmod(shared, 0o777|os.ModeSticky); err != nil {
			t.Fatalf("chmod: %v", err)
		}
		s, err := Open(filepath.Join(state, "registry.db"), Options{Now: fixedClock()})
		if err != nil {
			t.Fatalf("Open in an own dir under a sticky shared dir: %v", err)
		}
		s.Close()
	})
	t.Run("happy reopen", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "registry.db")
		s, err := Open(path, Options{Now: fixedClock()})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		s.Close()
		if _, err := Open(path, Options{Now: fixedClock()}); err != nil {
			t.Fatalf("reopen = %v, want ok", err)
		}
	})
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "secret.db"), []byte("payload-content"), 0o600); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := os.Symlink(filepath.Join(dir, "secret.db"), filepath.Join(dir, "registry.db")); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	_, err := Open(filepath.Join(dir, "registry.db"), Options{Now: fixedClock()})
	if err == nil || !strings.Contains(err.Error(), filepath.Join(dir, "registry.db")) {
		t.Fatalf("err = %v, want the file named", err)
	}
	if strings.Contains(err.Error(), "payload-content") {
		t.Fatalf("err = %v, want no file content", err)
	}
}
