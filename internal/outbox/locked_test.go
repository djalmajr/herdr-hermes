package outbox

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestOutboxWithLockExclusive: goroutines on one store and on a second
// store on the same dir cannot run the WithLock callback concurrently.
func TestOutboxWithLockExclusive(t *testing.T) {
	dir := t.TempDir()
	a := openStore(t, dir)
	b := openStore(t, dir)
	var mu sync.Mutex
	inside := 0
	var overlapped bool
	fn := func() error {
		mu.Lock()
		inside++
		if inside > 1 {
			overlapped = true
		}
		mu.Unlock()
		time.Sleep(30 * time.Millisecond)
		mu.Lock()
		inside--
		mu.Unlock()
		return nil
	}
	var wg sync.WaitGroup
	for _, s := range []*Store{a, b, a} {
		wg.Add(1)
		go func(s *Store) {
			defer wg.Done()
			if err := s.WithLock(fn); err != nil {
				t.Errorf("WithLock: %v", err)
			}
		}(s)
	}
	wg.Wait()
	if overlapped {
		t.Fatal("two WithLock callbacks ran at the same time")
	}
}

// TestOutboxWithLockReadOnly: a read-only store returns ErrReadOnly
// without calling fn and without creating anything.
func TestOutboxWithLockReadOnly(t *testing.T) {
	cfg := t.TempDir()
	state := filepath.Join(cfg, "state")
	s, err := Open(state, Options{Now: fixedNow, ReadOnly: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	called := false
	err = s.WithLock(func() error {
		called = true
		return nil
	})
	if !errors.Is(err, ErrReadOnly) {
		t.Fatalf("WithLock error = %v, want ErrReadOnly", err)
	}
	if called {
		t.Error("fn was called on a read-only store")
	}
	if _, err := os.Stat(state); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("read-only store created the state dir (stat err %v)", err)
	}
}

// TestOutboxWithLockBlocksAppend: an Append from a second store waits for
// the WithLock callback to finish; it cannot complete while the lock is
// held.
func TestOutboxWithLockBlocksAppend(t *testing.T) {
	dir := t.TempDir()
	a := openStore(t, dir)
	b := openStore(t, dir)
	inside := make(chan struct{})
	done := make(chan struct{})
	var mu sync.Mutex
	appended := false
	var appendedSeq int64
	go func() {
		defer close(done)
		<-inside // start the append only once a holds the lock
		rec, err := b.Append(Record{Tipo: TipoDispatch, Maquina: testMaquina, Projeto: testProjeto, Dados: json.RawMessage(`{}`)})
		if err != nil {
			t.Errorf("Append: %v", err)
			return
		}
		mu.Lock()
		appended = true
		appendedSeq = rec.Seq
		mu.Unlock()
	}()
	err := a.WithLock(func() error {
		close(inside)
		time.Sleep(100 * time.Millisecond)
		mu.Lock()
		during := appended
		mu.Unlock()
		if during {
			t.Error("Append completed while WithLock held the lock")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WithLock: %v", err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Append did not finish within 5s")
	}
	mu.Lock()
	ok, seq := appended, appendedSeq
	mu.Unlock()
	if !ok {
		t.Fatal("Append did not complete after the lock was released")
	}
	if seq != 1 {
		t.Fatalf("Append seq = %d, want 1", seq)
	}
}
