package cred

import (
	"errors"
	"os"
	"runtime"
	"strings"
	"testing"
)

func TestFileStoreRoundTrip(t *testing.T) {
	dir := t.TempDir()
	s := &FileStore{Path: dir + "/credentials"}

	// Get on a missing file is ErrNotConfigured, not a generic error.
	if _, err := s.Get(); !errors.Is(err, ErrNotConfigured) {
		t.Fatalf("Get on missing file = %v, want ErrNotConfigured", err)
	}
	// Delete on a missing file is nil.
	if err := s.Delete(); err != nil {
		t.Fatalf("Delete on missing file = %v, want nil", err)
	}
	if s.Kind() != "file" {
		t.Fatalf("Kind = %q, want \"file\"", s.Kind())
	}

	const key = "sekret-key-123"
	if err := s.Set(key); err != nil {
		t.Fatalf("Set: %v", err)
	}
	got, err := s.Get()
	if err != nil || got != key {
		t.Fatalf("Get = %q, %v; want %q", got, err, key)
	}
	// The file holds exactly the key bytes plus a trailing newline.
	data, err := os.ReadFile(s.Path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(data) != key+"\n" {
		t.Fatalf("file content = %q, want the key plus one trailing newline", data)
	}
	// A second Set replaces the first key.
	if err := s.Set("other"); err != nil {
		t.Fatalf("Set again: %v", err)
	}
	if got, _ := s.Get(); got != "other" {
		t.Fatalf("Get after replace = %q, want \"other\"", got)
	}
	// Delete removes it; Get is ErrNotConfigured again.
	if err := s.Delete(); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := s.Get(); !errors.Is(err, ErrNotConfigured) {
		t.Fatalf("Get after Delete = %v, want ErrNotConfigured", err)
	}
}

func TestFileStoreModeAndEmptyKey(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("file mode assertions are POSIX-only")
	}
	dir := t.TempDir()
	s := &FileStore{Path: dir + "/credentials"}
	if err := s.Set("k"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	fi, err := os.Stat(s.Path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("credentials mode = %v, want 0600", fi.Mode().Perm())
	}
	// A file with only the trailing newline is an error, never an empty
	// key (the push client would otherwise get a "configured" empty key).
	if err := os.WriteFile(s.Path, []byte("\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if key, err := s.Get(); err == nil || key != "" {
		t.Fatalf("Get on newline-only file = %q, %v; want an error", key, err)
	}
}

func TestFileStoreNoPartialFiles(t *testing.T) {
	// Set goes through temp + rename, so a crash cannot leave a
	// credentials file without the newline contract.
	dir := t.TempDir()
	s := &FileStore{Path: dir + "/credentials"}
	if err := s.Set("abc"); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".credentials.tmp") {
			t.Fatalf("temp file left behind: %s", e.Name())
		}
	}
}
