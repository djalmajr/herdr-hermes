package outbox

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestReadReplacedFileExact proves the lock-free reader sees exactly the
// content of the replaced file, including after an atomic overwrite swaps
// the file in place.
func TestReadReplacedFileExact(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "state.json")
	if err := os.WriteFile(p, []byte(`{"v":1}`), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := ReadReplacedFile(p)
	if err != nil {
		t.Fatalf("ReadReplacedFile: %v", err)
	}
	if string(got) != `{"v":1}` {
		t.Fatalf("content = %q, want %q", got, `{"v":1}`)
	}
	if err := WriteFileAtomic(p, []byte(`{"v":2}`)); err != nil {
		t.Fatalf("WriteFileAtomic: %v", err)
	}
	got, err = ReadReplacedFile(p)
	if err != nil {
		t.Fatalf("ReadReplacedFile after overwrite: %v", err)
	}
	if string(got) != `{"v":2}` {
		t.Fatalf("content = %q, want %q", got, `{"v":2}`)
	}
}

// TestReadReplacedFileNotExist proves a missing path yields an
// os.IsNotExist error whose message keeps the "open <path>:" prefix the
// callers match on.
func TestReadReplacedFileNotExist(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "missing.json")
	_, err := ReadReplacedFile(p)
	if err == nil {
		t.Fatal("ReadReplacedFile on a missing path: want an error, got nil")
	}
	if !os.IsNotExist(err) {
		t.Fatalf("os.IsNotExist = false, err = %v", err)
	}
	if !strings.HasPrefix(err.Error(), "open ") {
		t.Fatalf("error = %q, want the %q prefix", err, "open ")
	}
}

// TestWriteFileAtomicContentAfterReplace proves WriteFileAtomic leaves the
// new content on every platform (the mode 0600 and no-temp-left assertions
// stay in TestOutboxWriteFileAtomic, which skips on Windows like before).
func TestWriteFileAtomicContentAfterReplace(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "state.json")
	if err := WriteFileAtomic(p, []byte(`{"v":1}`)); err != nil {
		t.Fatalf("WriteFileAtomic: %v", err)
	}
	if err := WriteFileAtomic(p, []byte(`{"v":2}`)); err != nil {
		t.Fatalf("WriteFileAtomic overwrite: %v", err)
	}
	got, err := ReadReplacedFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != `{"v":2}` {
		t.Fatalf("content = %q, want %q", got, `{"v":2}`)
	}
}
