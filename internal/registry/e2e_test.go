package registry

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The real Store is the production Backend.
var _ Backend = (*Store)(nil)

// TestStoreOverHTTP drives the real Store through the handler on a
// loopback server: a pushed job event and two polled nodes show up in the
// inventory, and the pre-dispatch check refuses work that overlaps either
// node while it accepts and then refuses a fresh request.
// Mutation captured: a Check that ignores the jobs table answers 200 for
// the workspace already open on node-b.
func TestStoreOverHTTP(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "registry.db"), Options{Now: fixedClock()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	srv := httptest.NewServer(NewHandler(s))
	t.Cleanup(srv.Close)
	client := &http.Client{Timeout: 5 * time.Second}

	do := func(method, path, body string, header map[string]string) (int, string) {
		t.Helper()
		req, err := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		for k, v := range header {
			req.Header.Set(k, v)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("%s %s: %v", method, path, err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}

	event := `{"seq":1,"ts":"2026-10-10T11:59:00-03:00","tipo":"question","resumo":"needs a decision","refs":{"pane":"w5:p1","report":"/local/path"}}`
	record := `{"schema":1,"seq":4,"ts":"2026-10-10T11:59:01-03:00","tipo":"job_event","maquina":"node-a","projeto":"example-org/example-repo","job_id":"job-7","idempotency_key":"job-7:1","dados":` + event + `}`
	hdr := map[string]string{"Idempotency-Key": "job-7:1", "Authorization": "Bearer canary-e2e-0001"}
	if code, body := do("POST", "/v1/ingest", record, hdr); code != 200 || !strings.Contains(body, `"aplicado":true`) {
		t.Fatalf("ingest = %d %s", code, body)
	}
	if code, body := do("POST", "/v1/ingest", record, hdr); code != 200 || !strings.Contains(body, `"aplicado":false`) {
		t.Fatalf("replayed ingest = %d %s, want aplicado false", code, body)
	}

	for _, p := range []PollResult{
		{Maquina: "node-a", At: fixedNow, OK: true, HerdrVersion: "0.9.1", Workspaces: []PolledWorkspace{{ID: "w5", Label: "job-job-7", AgentStatus: "blocked"}}},
		{Maquina: "node-b", At: fixedNow, OK: true, Workspaces: []PolledWorkspace{{ID: "w2", Label: "hm-example-agent-impl-card12", AgentStatus: "working"}, {ID: "w3", Label: "personal", AgentStatus: "idle"}}},
	} {
		if err := s.ApplyPoll(p); err != nil {
			t.Fatalf("ApplyPoll %s: %v", p.Maquina, err)
		}
	}

	code, body := do("GET", "/v1/jobs", "", nil)
	if code != 200 {
		t.Fatalf("jobs = %d %s", code, body)
	}
	if strings.Contains(body, "canary-e2e") || strings.Contains(body, "/local/path") {
		t.Fatalf("inventory leaks the key or a local path: %s", body)
	}
	var inv Inventory
	if err := json.Unmarshal([]byte(body), &inv); err != nil {
		t.Fatalf("inventory: %v", err)
	}
	if len(inv.Nodes) != 2 || !inv.Nodes[0].Online || !inv.Nodes[1].Online {
		t.Fatalf("nodes = %+v, want node-a and node-b online", inv.Nodes)
	}
	var keys []string
	for _, j := range inv.Jobs {
		keys = append(keys, j.Maquina+"/"+j.Chave)
	}
	if strings.Join(keys, ",") != "node-a/job-7,node-b/ws:w2" {
		t.Fatalf("jobs = %v", keys)
	}
	if j := inv.Jobs[0]; !j.EsperandoHumano || j.Workspace == nil || *j.Workspace != "job-job-7" {
		t.Fatalf("job-7 = %+v, want waiting with its workspace", j)
	}
	if j := inv.Jobs[1]; j.Agente == nil || *j.Agente != "example-agent" || j.Ref == nil || *j.Ref != "card12" {
		t.Fatalf("hm job = %+v, want owner from the name", j)
	}
	if len(inv.Esperas) != 1 || inv.Esperas[0].Tipo != EsperaQuestion {
		t.Fatalf("esperas = %+v, want the question of job-7", inv.Esperas)
	}

	// The review workspace of the same ref overlaps the impl one on node-b.
	code, body = do("POST", "/v1/check", `{"agente":"example-agent","workspace":"hm-example-agent-review-card12"}`, nil)
	if code != 409 || !strings.Contains(body, `"status":"duplicate"`) || !strings.Contains(body, `"maquina":"node-b"`) || !strings.Contains(body, `"campo":"ref"`) {
		t.Fatalf("overlapping check = %d %s", code, body)
	}
	free := `{"agente":"example-agent","card":"CARD-99","projeto":"example-org/example-repo","branch":"feat/y"}`
	code, body = do("POST", "/v1/check", free, nil)
	if code != 200 || !strings.Contains(body, `"status":"ok"`) {
		t.Fatalf("free check = %d %s", code, body)
	}
	var ok CheckResult
	if err := json.Unmarshal([]byte(body), &ok); err != nil || ok.Claim == nil {
		t.Fatalf("claim: %v %s", err, body)
	}
	if code, body = do("POST", "/v1/check", free, nil); code != 409 || !strings.Contains(body, `"tipo":"claim"`) {
		t.Fatalf("repeated check = %d %s, want the claim conflict", code, body)
	}
	if code, _ = do("DELETE", "/v1/claims/"+ok.Claim.ID, "", nil); code != 200 {
		t.Fatalf("release = %d", code)
	}
	if code, _ = do("POST", "/v1/check", free, nil); code != 200 {
		t.Fatalf("check after release = %d, want 200", code)
	}

	// A hold on the project refuses even a request that overlaps nothing.
	if code, body = do("PUT", "/v1/policies", `{"escopo":"projeto","valor":"example-org/other","hold":true,"motivo":"on hold"}`, nil); code != 200 {
		t.Fatalf("policy = %d %s", code, body)
	}
	code, body = do("POST", "/v1/check", `{"agente":"example-agent","card":"CARD-100","projeto":"Example-Org/Other"}`, nil)
	if code != 409 || !strings.Contains(body, `"status":"hold"`) {
		t.Fatalf("held check = %d %s", code, body)
	}
	if !bytes.HasSuffix([]byte(body), []byte("\n")) {
		t.Fatalf("answer is not one line: %q", body)
	}
}
