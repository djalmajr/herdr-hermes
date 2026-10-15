package registry

// DJA-275: the health counts. GET /v1/health keeps status and schema and
// gains the counts object, read in one read transaction: per kind the
// total, the active rows, the ended rows and the protected ended rows
// (the ended rows within the 7-day replay fence, the future-dated ones
// included). The counts are coherent: total = active + ended. A failed
// count answers the 500 error JSON of the server.

import (
	"net/http"
	"strconv"
	"testing"
)

// dja275SeedHealth mixes active and ended rows of both kinds at the
// fixed clock.
func dja275SeedHealth(t *testing.T, s *Store) {
	t.Helper()
	oneDay := fixedNow.Unix() - 24*3600
	eightDays := fixedNow.Unix() - 8*24*3600
	rows := []struct {
		maquina, chave, origem string
		encerrado              int
		encerradoUnix          *int64
	}{
		{"node-a", "j-active-1", "job", 0, nil},
		{"node-a", "j-active-2", "job", 0, nil},
		{"node-a", "j-ended-fence", "job", 1, &oneDay},
		{"node-a", "j-ended-old", "job", 1, &eightDays},
		{"node-b", "sessao:org001/repo#feat/a", "sessao", 0, nil},
		{"node-b", "sessao:org002/repo#feat/b", "sessao", 1, &oneDay},
		{"node-b", "sessao:org003/repo#feat/c", "sessao", 1, &eightDays},
	}
	tx, err := s.db.Begin()
	if err != nil {
		t.Fatalf("begin seed: %v", err)
	}
	for i, r := range rows {
		var eu interface{}
		if r.encerradoUnix != nil {
			eu = *r.encerradoUnix
		}
		if _, err := tx.Exec(`INSERT INTO jobs(maquina, chave, origem, job_id, encerrado, encerrado_unix)
			VALUES(?, ?, ?, ?, ?, ?)`, r.maquina, r.chave, r.origem, r.chave, r.encerrado, eu); err != nil {
			tx.Rollback()
			t.Fatalf("seed row %d: %v", i, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit seed: %v", err)
	}
}

// TestDJA275HealthCounts: the health answer carries the counts coherent
// with the stored rows: total = active + ended, and the protected ended
// rows are the ended rows inside the 7-day fence.
func TestDJA275HealthCounts(t *testing.T) {
	s := openStore(t)
	dja275SeedHealth(t, s)
	w := doReq(t, NewHandler(s), "GET", "/v1/health", "", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("code = %d, want 200 (body %s)", w.Code, w.Body.String())
	}
	want := `{"status":"ok","schema":` + strconv.Itoa(SchemaVersion) +
		`,"counts":{"jobs":{"total":4,"active":2,"ended":2,"protected_ended":1},"sessions":{"total":3,"active":1,"ended":2,"protected_ended":1}}}` + "\n"
	if w.Body.String() != want {
		t.Errorf("body = %q, want %q", w.Body.String(), want)
	}
	// The store method itself: the same counts, in one read
	// transaction.
	c, err := s.HealthCounts()
	if err != nil {
		t.Fatalf("HealthCounts: %v", err)
	}
	if c.Jobs.Total != c.Jobs.Active+c.Jobs.Ended || c.Sessions.Total != c.Sessions.Active+c.Sessions.Ended {
		t.Errorf("counts not coherent: jobs %+v, sessions %+v (total must be active + ended)", c.Jobs, c.Sessions)
	}
	if c.Jobs != (KindCounts{Total: 4, Active: 2, Ended: 2, ProtectedEnded: 1}) ||
		c.Sessions != (KindCounts{Total: 3, Active: 1, Ended: 2, ProtectedEnded: 1}) {
		t.Errorf("counts = %+v, want the seeded mix", c)
	}
}

// TestDJA275HealthCountsEmpty: an empty registry answers the zero
// counts.
func TestDJA275HealthCountsEmpty(t *testing.T) {
	s := openStore(t)
	w := doReq(t, NewHandler(s), "GET", "/v1/health", "", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("code = %d, want 200 (body %s)", w.Code, w.Body.String())
	}
	want := `{"status":"ok","schema":` + strconv.Itoa(SchemaVersion) +
		`,"counts":{"jobs":{"total":0,"active":0,"ended":0,"protected_ended":0},"sessions":{"total":0,"active":0,"ended":0,"protected_ended":0}}}` + "\n"
	if w.Body.String() != want {
		t.Errorf("body = %q, want %q", w.Body.String(), want)
	}
}

// TestDJA275HealthCountsReadTransaction: the health counts use a read
// transaction: with a second store on the same file holding a write
// transaction and a short busy timeout on the first, HealthCounts
// succeeds with the seeded counts, while a write-locked BEGIN of the
// same read would answer SQLITE_BUSY.
func TestDJA275HealthCountsReadTransaction(t *testing.T) {
	s := openStore(t)
	dja275SeedHealth(t, s)
	other, err := Open(dja275StorePath(t, s), Options{Now: fixedClock()})
	if err != nil {
		t.Fatalf("Open the second store on the same file: %v", err)
	}
	defer other.Close()
	// A short busy timeout: a BEGIN that waits for the write lock fails
	// almost at once; a deferred read does not wait for it.
	if _, err := s.db.Exec(`PRAGMA busy_timeout=1`); err != nil {
		t.Fatalf("short busy timeout: %v", err)
	}
	tx, err := other.db.Begin()
	if err != nil {
		t.Fatalf("begin the write transaction: %v", err)
	}
	defer tx.Rollback()
	c, err := s.HealthCounts()
	if err != nil {
		t.Fatalf("HealthCounts while the other store holds a write transaction: %v", err)
	}
	if c.Jobs != (KindCounts{Total: 4, Active: 2, Ended: 2, ProtectedEnded: 1}) ||
		c.Sessions != (KindCounts{Total: 3, Active: 1, Ended: 2, ProtectedEnded: 1}) {
		t.Errorf("counts = %+v, want the seeded mix of the snapshot", c)
	}
}

// dja275StorePath returns the file path the store is open on.
func dja275StorePath(t *testing.T, s *Store) string {
	t.Helper()
	var seq int
	var name, path string
	if err := s.db.QueryRow(`PRAGMA database_list`).Scan(&seq, &name, &path); err != nil {
		t.Fatalf("database_list: %v", err)
	}
	return path
}

// TestDJA275HealthCountsFailure: a failed count answers the 500 error
// JSON of the server, never the Go error text.
func TestDJA275HealthCountsFailure(t *testing.T) {
	s := openStore(t)
	h := NewHandler(s)
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	w := doReq(t, h, "GET", "/v1/health", "", nil)
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("code = %d, want 500 (body %s)", w.Code, w.Body.String())
	}
	wantJSONLine(t, w.Body.String(), "error", "internal error")
}
