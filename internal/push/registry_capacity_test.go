package push

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/djalmajr/nau/internal/outbox"
	"github.com/djalmajr/nau/internal/registry"
)

const pushT0 = "2026-10-10T12:00:00-03:00"

// pushMovableClock returns an injected clock fixed at start and a setter
// that moves it by whole seconds.
func pushMovableClock(start time.Time) (func() time.Time, func(int64)) {
	cur := start
	return func() time.Time { return cur },
		func(delta int64) { cur = cur.Add(time.Duration(delta) * time.Second) }
}

// pushSessionRec builds one session outbox record.
func pushSessionRec(projeto, branch string) outbox.Record {
	d, _ := json.Marshal(map[string]any{"acao": "start", "projeto": projeto, "branch": branch})
	return outbox.Record{
		Tipo: outbox.TipoSession, Maquina: "node-a", Projeto: projeto,
		Dados: d,
	}
}

// TestPendingAgainstRegistryNoCap: a push batch against a registry that
// no longer caps the jobs and sessions kinds delivers every record in
// the batch, even with a retained ended session row, and advances
// delivered_seq.
func TestPendingAgainstRegistryNoCap(t *testing.T) {
	ctx := context.Background()
	t0, err := time.Parse(time.RFC3339, pushT0)
	if err != nil {
		t.Fatalf("parse the fixed now: %v", err)
	}
	nowFn, _ := pushMovableClock(t0)
	rs, err := registry.Open(filepath.Join(t.TempDir(), "registry.db"), registry.Options{Now: nowFn})
	if err != nil {
		t.Fatalf("registry Open: %v", err)
	}
	t.Cleanup(func() { rs.Close() })
	// One retained ended session, inside the fence at t0. The kind has
	// no count cap: the row neither blocks nor evicts anything.
	if _, err := rs.Ingest(outbox.Record{
		Schema: 1, Seq: 1, TS: pushT0, Tipo: outbox.TipoSession, Maquina: "node-a",
		Projeto: "org/x", IdempotencyKey: "node-a:seed",
		Dados: mustPushJSON(t, map[string]any{"acao": "end", "projeto": "org/x", "branch": "old"}),
	}); err != nil {
		t.Fatalf("seed the ended session: %v", err)
	}
	os, err := outbox.Open(t.TempDir(), outbox.Options{Now: nowFn})
	if err != nil {
		t.Fatalf("outbox Open: %v", err)
	}
	for _, branch := range []string{"new-a", "new-b"} {
		if _, err := os.Append(pushSessionRec("org/x", branch)); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	srv := httptest.NewServer(registry.NewHandler(rs))
	t.Cleanup(srv.Close)
	p, err := NewHTTP(srv.URL+"/v1/ingest", "test-key", "1.0", time.Second)
	if err != nil {
		t.Fatalf("NewHTTP: %v", err)
	}
	noSleep := func(context.Context, time.Duration) error { return nil }

	// The whole batch is admitted at once: nothing of the kind's rows
	// stops the delivery, and the cursor advances past the batch.
	res := Pending(ctx, os, p, noSleep, false)
	if res.Sent != 2 || res.Pending != 0 || res.Code != 0 || res.Status != "" {
		t.Fatalf("batch = %+v, want {Sent:2 Pending:0 Code:0}", res)
	}
	if n, err := os.DeliveredSeq(); err != nil || n != 2 {
		t.Fatalf("delivered_seq = %d (%v), want 2", n, err)
	}
	inv, err := rs.Inventory(registry.Filter{})
	if err != nil {
		t.Fatalf("Inventory: %v", err)
	}
	if len(inv.Jobs) != 2 {
		t.Errorf("active session rows = %d, want 2 (the two starts)", len(inv.Jobs))
	}
}

func mustPushJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}
