package registry

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// TestDJA229AliasTargetAncestry: a private alias to a slot whose real
// ancestor is a 0777 non-sticky shared directory is refused through the
// alias exactly like it is refused directly, including a missing tail
// behind the alias, and a refused open leaves nothing in the target;
// private aliases to private targets and plain paths keep opening.
func TestDJA229AliasTargetAncestry(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unix file modes and symlinks")
	}
	root := t.TempDir()
	private := filepath.Join(root, "private")
	shared := filepath.Join(root, "shared")
	slot := filepath.Join(shared, "slot")
	for _, d := range []string{private, shared, slot} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
	}
	if err := os.Chmod(shared, 0o777); err != nil {
		t.Fatalf("chmod shared: %v", err)
	}
	alias := filepath.Join(private, "alias")
	if err := os.Symlink(slot, alias); err != nil {
		t.Fatalf("symlink alias: %v", err)
	}
	direct := filepath.Join(slot, "registry.db")
	if err := unsafeStateCheck(direct); !errors.Is(err, ErrUnsafeState) {
		t.Fatalf("direct unsafeStateCheck = %v, want ErrUnsafeState", err)
	}
	s, err := Open(filepath.Join(alias, "registry.db"), Options{Now: fixedClock()})
	if s != nil {
		s.Close()
	}
	if err == nil || !errors.Is(err, ErrUnsafeState) {
		t.Fatalf("Open through the alias = %v, want ErrUnsafeState", err)
	}
	if _, err := os.Lstat(filepath.Join(slot, "registry.db")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("refused open created registry.db in the target (%v)", err)
	}
	tail := filepath.Join(alias, "newdir", "registry.db")
	if _, err := Open(tail, Options{Now: fixedClock()}); err == nil || !errors.Is(err, ErrUnsafeState) {
		t.Fatalf("Open through the alias with a missing tail = %v, want ErrUnsafeState", err)
	}
	if _, err := os.Lstat(filepath.Join(slot, "newdir")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("refused open created newdir in the target (%v)", err)
	}
	if _, err := os.Lstat(filepath.Join(slot, "newdir", "registry.db")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("refused open created registry.db in the target (%v)", err)
	}
	// Control: a private alias to a private (0700, self-owned) target
	// keeps opening.
	priv2 := filepath.Join(root, "priv2")
	if err := os.MkdirAll(priv2, 0o700); err != nil {
		t.Fatalf("mkdir priv2: %v", err)
	}
	alias2 := filepath.Join(private, "alias2")
	if err := os.Symlink(priv2, alias2); err != nil {
		t.Fatalf("symlink alias2: %v", err)
	}
	s2, err := Open(filepath.Join(alias2, "registry.db"), Options{Now: fixedClock()})
	if err != nil {
		t.Fatalf("Open through a private alias to a private target: %v", err)
	}
	s2.Close()
	// Control: a plain temp path keeps opening.
	s3, err := Open(filepath.Join(t.TempDir(), "registry.db"), Options{Now: fixedClock()})
	if err != nil {
		t.Fatalf("Open on a plain path: %v", err)
	}
	s3.Close()
}

// TestDJA229FolderJobBound: the optional folder job is bounded by the
// contract job id grammar, so one stored folder can no longer push the
// worst-case inventory past MaxInventoryBytes.
func TestDJA229FolderJobBound(t *testing.T) {
	s := openStore(t)
	h := NewHandler(s)
	put := func(jobJSON string) *httptest.ResponseRecorder {
		body := `{"maquina":"node-a","caminho":"/retained","pedido":"retain","job":` + jobJSON + `}`
		if len(body) > apiBodyCap {
			t.Fatalf("fixture exceeds the input cap: %d bytes", len(body))
		}
		r := httptest.NewRequest(http.MethodPut, "/v1/folders", strings.NewReader(body))
		r.Host = "127.0.0.1:7429"
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	count := func() int {
		var n int
		if err := s.db.QueryRow(`SELECT COUNT(*) FROM folders`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	w := put(`"` + strings.Repeat("<", 48000) + `"`)
	if w.Code != http.StatusBadRequest {
		t.Errorf("oversized job: status = %d, want 400 (body %s)", w.Code, w.Body.String())
	}
	if n := count(); n != 0 {
		t.Errorf("oversized job: folders stored = %d, want 0", n)
	}
	// A job whose decoded value needs JSON escaping (a quote) is 400.
	w = put(`"job\"1"`)
	if w.Code != http.StatusBadRequest {
		t.Errorf("json-escaping job: status = %d, want 400 (body %s)", w.Code, w.Body.String())
	}
	w = put(`"job<1"`)
	if w.Code != http.StatusBadRequest {
		t.Errorf("hostile job: status = %d, want 400 (body %s)", w.Code, w.Body.String())
	}
	if n := count(); n != 0 {
		t.Errorf("rejected jobs: folders stored = %d, want 0", n)
	}
	valid := strings.Repeat("j", 64)
	for _, job := range []string{"job-1.a_B", valid} {
		w = put(`"` + job + `"`)
		if w.Code != http.StatusOK {
			t.Fatalf("valid job %q: status = %d, want 200 (body %s)", job, w.Code, w.Body.String())
		}
		inv, err := s.Inventory(Filter{})
		if err != nil {
			t.Fatal(err)
		}
		if len(inv.Pastas) != 1 || inv.Pastas[0].Job == nil || *inv.Pastas[0].Job != job {
			t.Fatalf("valid job %q: inventory = %+v, want the stored job round-tripped", job, inv.Pastas)
		}
	}
	w = put(`null`)
	if w.Code != http.StatusOK {
		t.Fatalf("empty job: status = %d, want 200 (body %s)", w.Code, w.Body.String())
	}
	inv, err := s.Inventory(Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(inv.Pastas) != 1 || inv.Pastas[0].Job != nil {
		t.Fatalf("empty job: pastas = %+v, want job null", inv.Pastas)
	}
	enc, err := json.Marshal(inv.Pastas[0])
	if err != nil {
		t.Fatal(err)
	}
	lower := MaxFolders * (len(enc) + 1)
	t.Logf("stored job bytes=%d encoded folder bytes=%d folder section lower bound=%d inventory bound=%d", len(valid), len(enc), lower, MaxInventoryBytes)
	if lower > MaxInventoryBytes {
		t.Errorf("folder section lower bound = %d, want at most MaxInventoryBytes (%d)", lower, MaxInventoryBytes)
	}
}
