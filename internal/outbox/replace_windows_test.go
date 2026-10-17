//go:build windows

package outbox

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
)

// holdOpen opens path with the given access and share mode and keeps the
// handle open until the test ends (also when an assertion fails).
func holdOpen(t *testing.T, path string, access, share uint32) {
	t.Helper()
	name, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		t.Fatal(err)
	}
	h, err := syscall.CreateFile(name, access, share, nil, syscall.OPEN_EXISTING, syscall.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	t.Cleanup(func() { _ = syscall.CloseHandle(h) })
}

// noTempLeft fails when a WriteFileAtomic temp file for name was left
// behind in dir.
func noTempLeft(t *testing.T, dir, name string) {
	t.Helper()
	left, err := filepath.Glob(filepath.Join(dir, "."+name+".tmp-*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(left) != 0 {
		t.Fatalf("temp files left behind: %v", left)
	}
}

// TestWriteFileAtomicUnderShareDeleteReader proves the last step of the
// atomic replace succeeds while a lock-free reader (the ReadReplacedFile
// share mode, including FILE_SHARE_DELETE) holds the destination open, and
// that the new content is the one read afterwards.
func TestWriteFileAtomicUnderShareDeleteReader(t *testing.T) {
	dir := t.TempDir()
	dest := filepath.Join(dir, "state.json")
	if err := os.WriteFile(dest, []byte(`{"v":1}`), 0o600); err != nil {
		t.Fatal(err)
	}
	holdOpen(t, dest, syscall.GENERIC_READ,
		syscall.FILE_SHARE_READ|syscall.FILE_SHARE_WRITE|syscall.FILE_SHARE_DELETE)
	if err := WriteFileAtomic(dest, []byte(`{"v":2}`)); err != nil {
		t.Fatalf("WriteFileAtomic under a FILE_SHARE_DELETE reader: %v", err)
	}
	got, err := ReadReplacedFile(dest)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != `{"v":2}` {
		t.Fatalf("content = %q, want %q", got, `{"v":2}`)
	}
	noTempLeft(t, dir, "state.json")
}

// TestReadWriteLoopsCoexist proves a reader loop (ReadReplacedFile) and a
// writer loop (WriteFileAtomic) on the same file run to completion in
// goroutines with no error on either side and every read returning one of
// the written versions.
func TestReadWriteLoopsCoexist(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "state.json")
	const iters = 200
	versions := make([]string, iters)
	set := make(map[string]bool, iters)
	for i := range iters {
		versions[i] = fmt.Sprintf(`{"v":%d}`, i)
		set[versions[i]] = true
	}
	// Seed the file before the loops start so a read never misses it.
	if err := os.WriteFile(p, []byte(versions[0]), 0o600); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	werr := make(chan error, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range iters {
			if err := WriteFileAtomic(p, []byte(versions[i])); err != nil {
				werr <- err
				return
			}
		}
		werr <- nil
	}()
	got := make([]string, iters)
	rerr := make(chan error, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range iters {
			b, err := ReadReplacedFile(p)
			if err != nil {
				rerr <- err
				return
			}
			got[i] = string(b)
		}
		rerr <- nil
	}()
	wg.Wait()
	close(werr)
	close(rerr)
	for err := range werr {
		if err != nil {
			t.Fatalf("writer: %v", err)
		}
	}
	for err := range rerr {
		if err != nil {
			t.Fatalf("reader: %v", err)
		}
	}
	for i, g := range got {
		if !set[g] {
			t.Fatalf("read %d = %q, want one of the %d written versions", i, g, len(versions))
		}
	}
	noTempLeft(t, dir, "state.json")
}

// TestReadLoopWithPlainReadIsAnObservation is the discriminating case for
// TestReadWriteLoopsCoexist: the same loops with os.ReadFile in the reader.
// It documents the failure of the old pattern — a reader open without
// FILE_SHARE_DELETE blocks the writer's rename ("Access is denied"), and on
// a native Windows run that is the writer that fails — and it is a race, so
// the test asserts nothing: no error on either side fails it, and it only
// logs how many renames and reads errored and the last error of each side.
// The writer keeps going to the end after an error, so the log carries the
// total count of the raced renames.
func TestReadLoopWithPlainReadIsAnObservation(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "state.json")
	const iters = 200
	versions := make([]string, iters)
	for i := range iters {
		versions[i] = fmt.Sprintf(`{"v":%d}`, i)
	}
	// Seed the file before the loops start so a read never misses it.
	if err := os.WriteFile(p, []byte(versions[0]), 0o600); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	wFailed := 0
	var wLast error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range iters {
			if err := WriteFileAtomic(p, []byte(versions[i])); err != nil {
				mu.Lock()
				wFailed++
				wLast = err
				mu.Unlock()
				continue
			}
		}
	}()
	rFailed := 0
	var rLast error
	for range iters {
		if _, err := os.ReadFile(p); err != nil {
			rFailed++
			rLast = err
		}
	}
	wg.Wait()
	t.Logf("os.ReadFile reader against %d WriteFileAtomic iterations: writer %d/%d renames errored (last: %v); reader %d/%d reads errored (last: %v)", iters, wFailed, iters, wLast, rFailed, iters, rLast)
}
