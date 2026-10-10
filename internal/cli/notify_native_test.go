package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/djalmajr/herdr-hermes/internal/jobapi"

	"github.com/djalmajr/herdr-hermes/internal/notify"
	"github.com/djalmajr/herdr-hermes/internal/plugin"
)

// runPluginWorkspace runs `plugin event` with one workspace event under the
// given hook name and payload.
func runPluginWorkspace(t *testing.T, dir string, clk *testClock, hook, payload string) (string, string, int) {
	t.Helper()
	vars := map[string]string{plugin.EnvEvent: hook, plugin.EnvEventJSON: payload}
	return runNotifyFlow(t, dir, clk, vars, "", "plugin", "event")
}

// workspaceCreatedLine is the socket subscription form of workspace.created.
func workspaceCreatedLine(ws, label string) string {
	return fmt.Sprintf(`{"event":"workspace_created","data":{"type":"workspace_created","workspace":{"workspace_id":%q,"label":%q,"number":1,"focused":false,"pane_count":1,"tab_count":1,"active_tab_id":"t1","agent_status":"idle"}}}`, ws, label)
}

// workspaceClosedLine is the socket subscription form of workspace.closed
// without workspace info.
func workspaceClosedLine(ws string) string {
	return fmt.Sprintf(`{"event":"workspace_closed","data":{"type":"workspace_closed","workspace_id":%q,"workspace":null}}`, ws)
}

// TestNotifyFlowAgentDone: a watched pane's agent done is its own
// notification (never coalesced like idle), each true later done is a
// distinct one, and the loop guard coalesces the done of a pane that is
// itself a registered recipient.
func TestNotifyFlowAgentDone(t *testing.T) {
	exe, fakeDir := installFakeSoho(t, capsRule(capsJSON, 0), sendRule(0, 0))
	dir := t.TempDir()
	setSohoConfig(t, dir, exe, "machine-a")
	clk := newTestClock()
	mustRunNotify(t, dir, clk, "notify", "register", "watch", "--pane", notifyTestPane, "--job", notifyTestJob, "--projeto", notifyTestRepo)
	mustRunNotify(t, dir, clk, "notify", "register", "owner", "--projeto", notifyTestRepo, "--to", notifyTestOwner)
	mustRunNotify(t, dir, clk, "notify", "register", "orchestrator", "--job", notifyTestJob, "--to", notifyTestOrch)

	for _, status := range []string{"working", "done", "idle", "working", "done", "done"} {
		if _, _, exit := runPluginAgentStatus(t, dir, clk, notifyTestPane, "w1", status); exit != 0 {
			t.Fatalf("plugin event %s: exit %d", status, exit)
		}
	}
	led := readLedger(t, dir)
	if len(led.Notifications) != 2 {
		t.Fatalf("ledger has %d notifications, want 2 agent_done (the repeated done is a duplicate)", len(led.Notifications))
	}
	for _, n := range led.Notifications {
		if n.Class != notify.ClassAgentDone || len(n.Escalations) != 0 {
			t.Fatalf("notification %+v, want agent_done without escalation", n)
		}
	}
	if led.Notifications[0].ID == led.Notifications[1].ID {
		t.Fatal("the two done transitions share one notification id")
	}
	sends := flowSendCalls(t, fakeDir)
	if len(sends) != 4 {
		t.Fatalf("got %d sends, want 4 (owner and orchestrator, twice)", len(sends))
	}
	for _, argv := range sends {
		if !strings.Contains(argv[2], "class=agent_done") {
			t.Fatalf("send message %q, want class=agent_done", argv[2])
		}
	}

	// Loop guard: the watched pane becomes the owner's own destination; its
	// next done is coalesced, while blocked still notifies.
	mustRunNotify(t, dir, clk, "notify", "register", "owner", "--projeto", notifyTestRepo, "--to", "local/"+notifyTestPane)
	for _, status := range []string{"working", "done"} {
		if _, _, exit := runPluginAgentStatus(t, dir, clk, notifyTestPane, "w1", status); exit != 0 {
			t.Fatalf("plugin event %s: exit %d", status, exit)
		}
	}
	if led := readLedger(t, dir); len(led.Notifications) != 2 {
		t.Fatalf("the guarded done created a notification (%d total)", len(led.Notifications))
	}
	if _, _, exit := runPluginAgentStatus(t, dir, clk, notifyTestPane, "w1", "blocked"); exit != 0 {
		t.Fatal("plugin event blocked: exit != 0")
	}
	led = readLedger(t, dir)
	if len(led.Notifications) != 3 || led.Notifications[2].Class != notify.ClassAgentBlocked {
		t.Fatalf("ledger = %d notifications, want the blocked one notified despite the guard", len(led.Notifications))
	}
}

// TestNotifyFlowWorkspaceRegistered: a registered workspace with a label
// that is not job-<id> is a source routed by its registration; open,
// close and a later reopen of the same id are distinct notifications; a
// closed event without workspace info resolves through the opened record;
// an unregistered non-job workspace writes nothing.
func TestNotifyFlowWorkspaceRegistered(t *testing.T) {
	exe, fakeDir := installFakeSoho(t, capsRule(capsJSON, 0), sendRule(0, 0))
	dir := t.TempDir()
	setSohoConfig(t, dir, exe, "machine-a")
	clk := newTestClock()
	mustRunNotify(t, dir, clk, "notify", "register", "workspace", "--workspace", "w7", "--job", notifyTestJob, "--projeto", notifyTestRepo)
	mustRunNotify(t, dir, clk, "notify", "register", "owner", "--projeto", notifyTestRepo, "--to", notifyTestOwner)
	mustRunNotify(t, dir, clk, "notify", "register", "orchestrator", "--job", notifyTestJob, "--to", notifyTestOrch)

	steps := []struct{ hook, payload string }{
		{"workspace.created", workspaceCreatedLine("w7", "team-room")},
		{"workspace.created", workspaceCreatedLine("w7", "team-room")}, // replay: duplicate
		{"workspace.closed", workspaceClosedLine("w7")},
		{"workspace.created", workspaceCreatedLine("w7", "team-room")},  // later reopen
		{"workspace.created", workspaceCreatedLine("w8", "other-room")}, // unregistered
	}
	for _, s := range steps {
		if stdout, _, exit := runPluginWorkspace(t, dir, clk, s.hook, s.payload); exit != 0 || stdout != "" {
			t.Fatalf("plugin event %s: exit %d stdout %q", s.hook, exit, stdout)
		}
	}
	led := readLedger(t, dir)
	want := []string{notify.ClassWorkspaceOpened, notify.ClassWorkspaceClosed, notify.ClassWorkspaceOpened}
	if len(led.Notifications) != len(want) {
		t.Fatalf("ledger has %d notifications, want %d", len(led.Notifications), len(want))
	}
	ids := map[string]bool{}
	for i, n := range led.Notifications {
		if n.Class != want[i] || n.JobID != notifyTestJob || n.Projeto != notifyTestRepo || n.Workspace != "w7" {
			t.Fatalf("notification %d = %+v, want %s for the registered workspace", i, n, want[i])
		}
		ids[n.ID] = true
	}
	if len(ids) != 3 {
		t.Fatal("the reopen reused an earlier notification id")
	}
	if sends := flowSendCalls(t, fakeDir); len(sends) != 6 {
		t.Fatalf("got %d sends, want 6 (owner and orchestrator for each of 3)", len(sends))
	}
	for _, n := range led.Notifications {
		for _, d := range n.Deliveries {
			if d.State != notify.StateAccepted {
				t.Fatalf("delivery %+v, want accepted", d)
			}
		}
	}
}

// TestNotifyFlowWorkspaceJobLabel: a job-<id> workspace needs no
// registration (the contract label); its closed event without workspace
// info resolves to the job recorded at open; a malformed payload writes
// one friction line and nothing else.
func TestNotifyFlowWorkspaceJobLabel(t *testing.T) {
	exe, fakeDir := installFakeSoho(t, capsRule(capsJSON, 0), sendRule(0, 0))
	dir := t.TempDir()
	setSohoConfig(t, dir, exe, "machine-a")
	clk := newTestClock()
	mustRunNotify(t, dir, clk, "notify", "register", "coordinator", "--to", notifyTestCoord)
	mustRunNotify(t, dir, clk, "notify", "register", "orchestrator", "--job", notifyTestJob, "--to", notifyTestOrch)

	if _, _, exit := runPluginWorkspace(t, dir, clk, "workspace.created", workspaceCreatedLine("w3", "job-"+notifyTestJob)); exit != 0 {
		t.Fatal("created: exit != 0")
	}
	if _, _, exit := runPluginWorkspace(t, dir, clk, "workspace.closed", workspaceClosedLine("w3")); exit != 0 {
		t.Fatal("closed: exit != 0")
	}
	led := readLedger(t, dir)
	if len(led.Notifications) != 2 || led.Notifications[1].Class != notify.ClassWorkspaceClosed || led.Notifications[1].JobID != notifyTestJob {
		t.Fatalf("ledger = %+v, want opened and closed for the job", led.Notifications)
	}
	// The tracked job has no projeto: missing owner escalates to the
	// coordinator.
	for _, n := range led.Notifications {
		if len(n.Escalations) != 1 || n.Escalations[0] != notify.EscMissingOwner {
			t.Fatalf("escalations %v, want [missing_owner]", n.Escalations)
		}
	}
	if sends := flowSendCalls(t, fakeDir); len(sends) != 4 {
		t.Fatalf("got %d sends, want 4 (orchestrator and coordinator, twice)", len(sends))
	}

	before := readLedger(t, dir)
	if _, _, exit := runPluginWorkspace(t, dir, clk, "workspace.closed", `{"workspace_id":"bad id!"}`); exit != 0 {
		t.Fatal("malformed closed: exit != 0")
	}
	if after := readLedger(t, dir); len(after.Notifications) != len(before.Notifications) {
		t.Fatal("a malformed workspace payload created a notification")
	}
	data, err := os.ReadFile(filepath.Join(dir, "state", "friction.log"))
	if err != nil || !strings.Contains(string(data), "unknown event JSON shape") {
		t.Fatalf("friction.log = %q (%v), want the unknown shape line", data, err)
	}
}

// TestNotifyIngestWorkspace: the CLI ingest of a workspace event (socket
// subscription line on stdin) and its ignored paths.
func TestNotifyIngestWorkspace(t *testing.T) {
	exe, _ := installFakeSoho(t, capsRule(capsJSON, 0), sendRule(0, 0))
	dir := t.TempDir()
	setSohoConfig(t, dir, exe, "machine-a")
	clk := newTestClock()

	// Disabled: ignored, nothing written.
	stdout, _, exit := runNotifyFlow(t, dir, clk, nil, workspaceCreatedLine("w5", "room"), "notify", "ingest", "workspace")
	if exit != 0 || stdout != `{"ignored":true,"reason":"disabled"}`+"\n" {
		t.Fatalf("disabled ingest: exit %d stdout %q", exit, stdout)
	}
	if _, err := os.Stat(filepath.Join(dir, "state", "notify")); !os.IsNotExist(err) {
		t.Fatal("a disabled ingest created the notify dir")
	}

	mustRunNotify(t, dir, clk, "notify", "register", "workspace", "--workspace", "w5")
	cases := []struct {
		stdin, want string
		exit        int
	}{
		{`{"event":"pane.agent_status_changed","data":{}}`, `{"ignored":true,"reason":"not_workspace"}`, 0},
		{workspaceCreatedLine("w6", "room"), `{"ignored":true,"reason":"unregistered_workspace"}`, 0},
		{workspaceCreatedLine("w5", "room"), `"created":true`, 0},
		{workspaceCreatedLine("w5", "room"), `"duplicate":true`, 0},
		{`not json`, `"status":"2"`, 2},
	}
	for _, c := range cases {
		stdout, _, exit := runNotifyFlow(t, dir, clk, nil, c.stdin, "notify", "ingest", "workspace")
		if exit != c.exit || !strings.Contains(stdout, c.want) {
			t.Fatalf("ingest %q: exit %d stdout %q, want %d and %s", c.stdin, exit, stdout, c.exit, c.want)
		}
	}
	// Register and unregister round trip.
	out := mustRunNotify(t, dir, clk, "notify", "list")
	if !strings.Contains(out, `"workspace_watches":{"w5":{"workspace":"w5"}}`) {
		t.Fatalf("list = %q, want the workspace registration", out)
	}
	out = mustRunNotify(t, dir, clk, "notify", "unregister", "workspace", "--workspace", "w5")
	if out != `{"unregistered":"workspace","workspace":"w5","removed":true}`+"\n" {
		t.Fatalf("unregister = %q", out)
	}
	if stdout, _, exit := runNotifyFlow(t, dir, clk, nil, "", "notify", "register", "workspace", "--workspace", "bad id!"); exit != 2 || !strings.Contains(stdout, "workspace pattern") {
		t.Fatalf("invalid workspace: exit %d stdout %q", exit, stdout)
	}
}

// TestNotifyRetryCommand: an uncertain delivery (exit 15) is never resent
// by deliver; notify retry re-queues it once and it is accepted; a second
// retry and a disabled store find nothing.
func TestNotifyRetryCommand(t *testing.T) {
	exe, fakeDir := installFakeSoho(t, capsRule(capsJSON, 0), sendRule(15, 1), sendRule(0, 0))
	dir := t.TempDir()
	setSohoConfig(t, dir, exe, "machine-a")
	clk := newTestClock()
	mustRunNotify(t, dir, clk, "notify", "register", "owner", "--projeto", notifyTestRepo, "--to", notifyTestOwner)
	seedJob(t, dir, notifyTestJob, notifyTestRepo, 0, "accepted")
	vars := map[string]string{jobapi.EnvJobID: notifyTestJob}
	if _, _, exit := runNotifyFlow(t, dir, clk, vars, wakeEvent(1), "wake"); exit != 0 {
		t.Fatal("wake exit != 0")
	}
	led := readLedger(t, dir)
	if len(led.Notifications) != 1 || led.Notifications[0].Deliveries[0].State != notify.StateUncertain {
		t.Fatalf("ledger = %+v, want one uncertain owner delivery", led.Notifications)
	}
	id := led.Notifications[0].ID
	clk.add(time.Hour)
	mustRunNotify(t, dir, clk, "notify", "deliver")
	if n := len(flowSendCalls(t, fakeDir)); n != 1 {
		t.Fatalf("deliver resent an uncertain delivery (%d sends)", n)
	}
	out := mustRunNotify(t, dir, clk, "notify", "retry", id, "--role", "owner")
	if out != `{"retried":true,"accepted":1}`+"\n" {
		t.Fatalf("retry = %q", out)
	}
	if d := readLedger(t, dir).Notifications[0].Deliveries[0]; d.State != notify.StateAccepted || d.Attempts != 2 {
		t.Fatalf("after retry delivery = %+v, want accepted on attempt 2", d)
	}
	if stdout, _, exit := runNotifyFlow(t, dir, clk, nil, "", "notify", "retry", id, "--role", "owner"); exit != 3 || stdout != `{"status":"not_found"}`+"\n" {
		t.Fatalf("second retry: exit %d stdout %q, want 3 not_found", exit, stdout)
	}
	empty := t.TempDir()
	if stdout, _, exit := runNotifyFlow(t, empty, clk, nil, "", "notify", "retry", id, "--role", "owner"); exit != 3 || stdout != `{"status":"not_found"}`+"\n" {
		t.Fatalf("disabled retry: exit %d stdout %q", exit, stdout)
	}
	if _, err := os.Stat(filepath.Join(empty, "state")); !os.IsNotExist(err) {
		t.Fatal("a disabled retry created the state dir")
	}
	if stdout, _, exit := runNotifyFlowNowrite(t, dir, clk, nil, "", "notify", "retry", id, "--role", "owner"); exit != 2 || !strings.Contains(stdout, "nowrite") {
		t.Fatalf("NOWRITE retry: exit %d stdout %q", exit, stdout)
	}
}
