package registry

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sort"
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
	if _, err := s.db.Exec(`PRAGMA user_version = 2`); err != nil {
		t.Fatalf("bump user_version: %v", err)
	}
	if _, err := Open(path, Options{}); !errors.Is(err, ErrSchema) {
		t.Fatalf("Open on user_version 2 = %v, want ErrSchema", err)
	}
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
