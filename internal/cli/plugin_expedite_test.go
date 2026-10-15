package cli

// The CLI-level tests of the recipient-pane expedite of the
// pane.agent_status_changed hook: a pending delivery of the pane that
// just became idle or done (ref "local/<pane>" or the pane's agent name)
// is pulled to now and delivered in the same hook run, without waiting
// for the backoff; the working status does not expedite; a failed name
// resolution writes the fixed friction line and expedites nothing; a
// pane without a pending delivery writes nothing and never resolves a
// name (no herdr call).

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/djalmajr/nau/internal/jobapi"
	"github.com/djalmajr/nau/internal/notify"
	"github.com/djalmajr/nau/internal/outbox"
	"github.com/djalmajr/nau/internal/plugin"
)

// runExpediteEvent runs `plugin event` with one agent status change of
// pane, under the flow clock and the given fake PaneAgentName.
func runExpediteEvent(t *testing.T, dir string, clk *testClock, resolve func(ctx context.Context, pane string) (string, error), pane, ws, status string) (string, string, int) {
	t.Helper()
	json := fmt.Sprintf(`{"event":"pane.agent_status_changed","data":{"pane_id":%q,"workspace_id":%q,"agent_status":%q}}`,
		pane, ws, status)
	env, out, errb := newNotifyFlowEnv(dir, clk, map[string]string{
		plugin.EnvEvent:     "pane.agent_status_changed",
		plugin.EnvEventJSON: json,
	}, "")
	env.PaneAgentName = resolve
	exit := Run([]string{"plugin", "event"}, env)
	return out.String(), errb.String(), exit
}

// sentinelPaneAgentName fails the test if the name resolution runs: the
// ref-matching and the no-pending paths never resolve a pane name.
func sentinelPaneAgentName(t *testing.T) func(ctx context.Context, pane string) (string, error) {
	t.Helper()
	return func(ctx context.Context, pane string) (string, error) {
		t.Errorf("PaneAgentName was called for %q", pane)
		return "", fmt.Errorf("herdr agent get: must not be called")
	}
}

// seedBusyDelivery registers the owner with ref, wakes one terminal
// event with a busy send (exit 17) and returns the dir clock: the owner
// delivery is pending with a future next_attempt_at.
func seedBusyDelivery(t *testing.T, exe, fakeDir string, dir string, clk *testClock, ref string) {
	t.Helper()
	setSohoConfig(t, dir, exe, "machine-a")
	seedJob(t, dir, notifyTestJob, notifyTestRepo, 0, "accepted")
	mustRunNotify(t, dir, clk, "notify", "register", "owner", "--projeto", notifyTestRepo, "--to", ref)
	vars := map[string]string{jobapi.EnvJobID: notifyTestJob}
	_, stderr, exit := runNotifyFlow(t, dir, clk, vars, flowWakeEvent(1, "terminal", ""), "wake")
	if exit != 0 {
		t.Fatalf("wake exit = %d, stderr %q", exit, stderr)
	}
	led := readLedger(t, dir)
	if len(led.Notifications) != 1 {
		t.Fatalf("ledger has %d notifications, want 1", len(led.Notifications))
	}
	d := led.Notifications[0].Deliveries[0]
	if d.State != notify.StatePending || d.Ref != ref {
		t.Fatalf("delivery = %+v, want the pending delivery of %s", d, ref)
	}
	if _, err := time.Parse(notify.TSLayout, d.NextAttemptAt); err != nil {
		t.Fatalf("NextAttemptAt %q: %v", d.NextAttemptAt, err)
	}
	if n := len(flowSendCalls(t, fakeDir)); n != 1 {
		t.Fatalf("the wake made %d sends, want 1 (the busy attempt)", n)
	}
}

// TestPluginAgentStatusExpedite: the idle or done event of a pane that
// is the recipient of a pending delivery pulls the next attempt to now
// and the hook delivers it in the same run.
func TestPluginAgentStatusExpedite(t *testing.T) {
	t.Run("ref match delivers in the same run", func(t *testing.T) {
		exe, fakeDir := installFakeSoho(t,
			capsRule(capsJSON, 0),
			sendRule(17, 1), // the wake's attempt: busy
			sendRule(0, 0),  // the hook's attempt: accepted
		)
		dir := t.TempDir()
		clk := newTestClock()
		seedBusyDelivery(t, exe, fakeDir, dir, clk, "local/"+notifyTestPane)
		next := readLedger(t, dir).Notifications[0].Deliveries[0].NextAttemptAt
		nextTS, err := time.Parse(notify.TSLayout, next)
		if err != nil || !nextTS.After(clk.now()) {
			t.Fatalf("NextAttemptAt %q is not in the future (err %v)", next, err)
		}
		// The pane comes back idle and the clock does not move: only the
		// expedite makes the future delivery due for this run.
		wantNow := clk.now().Format(notify.TSLayout)
		stdout, _, exit := runExpediteEvent(t, dir, clk, sentinelPaneAgentName(t), notifyTestPane, "w1", "idle")
		if exit != 0 || stdout != "" {
			t.Fatalf("plugin event idle: exit %d stdout %q, want 0 and nothing on stdout", exit, stdout)
		}
		if n := len(flowSendCalls(t, fakeDir)); n != 2 {
			t.Fatalf("the hook made %d sends total, want 2 (the pending one delivered now)", n)
		}
		d := readLedger(t, dir).Notifications[0].Deliveries[0]
		if d.State != notify.StateAccepted || d.AcceptedAt == "" {
			t.Fatalf("delivery = %+v, want accepted", d)
		}
		if d.NextAttemptAt != wantNow {
			t.Fatalf("NextAttemptAt = %q, want %q (pulled to now by the expedite)", d.NextAttemptAt, wantNow)
		}
	})

	t.Run("working does not expedite", func(t *testing.T) {
		exe, fakeDir := installFakeSoho(t,
			capsRule(capsJSON, 0),
			sendRule(17, 1),
			sendRule(0, 0),
		)
		dir := t.TempDir()
		clk := newTestClock()
		seedBusyDelivery(t, exe, fakeDir, dir, clk, "local/"+notifyTestPane)
		nextBefore := readLedger(t, dir).Notifications[0].Deliveries[0].NextAttemptAt
		stdout, _, exit := runExpediteEvent(t, dir, clk, sentinelPaneAgentName(t), notifyTestPane, "w1", "working")
		if exit != 0 || stdout != "" {
			t.Fatalf("plugin event working: exit %d stdout %q, want 0 and nothing on stdout", exit, stdout)
		}
		if n := len(flowSendCalls(t, fakeDir)); n != 1 {
			t.Fatalf("the working event made %d sends total, want still 1 (nothing expedited)", n)
		}
		d := readLedger(t, dir).Notifications[0].Deliveries[0]
		if d.State != notify.StatePending || d.NextAttemptAt != nextBefore {
			t.Fatalf("delivery = %+v, want still pending with NextAttemptAt %q", d, nextBefore)
		}
		// Control: the backoff still holds — nothing is claimed before
		// the deadline and the delivery is accepted after it.
		stdout, _, exit = runNotifyFlow(t, dir, clk, map[string]string{}, "", "notify", "deliver")
		if exit != 0 || !strings.Contains(stdout, `"claimed":0`) {
			t.Fatalf("deliver before the deadline: exit %d stdout %q, want nothing claimed", exit, stdout)
		}
		if n := len(flowSendCalls(t, fakeDir)); n != 1 {
			t.Fatalf("deliver before the deadline made %d sends, want still 1", n)
		}
		clk.add(11 * time.Second)
		stdout, _, exit = runNotifyFlow(t, dir, clk, map[string]string{}, "", "notify", "deliver")
		if exit != 0 || !strings.Contains(stdout, `"accepted":1`) {
			t.Fatalf("deliver after the deadline: exit %d stdout %q, want the accepted delivery", exit, stdout)
		}
		if n := len(flowSendCalls(t, fakeDir)); n != 2 {
			t.Fatalf("sends total = %d, want 2", n)
		}
		if d := readLedger(t, dir).Notifications[0].Deliveries[0]; d.State != notify.StateAccepted {
			t.Fatalf("delivery = %+v, want accepted after the backoff", d)
		}
	})

	t.Run("name match resolves the pane agent", func(t *testing.T) {
		exe, fakeDir := installFakeSoho(t,
			capsRule(capsJSON, 0),
			sendRule(17, 1),
			sendRule(0, 0),
		)
		dir := t.TempDir()
		clk := newTestClock()
		seedBusyDelivery(t, exe, fakeDir, dir, clk, "owner-x")
		var gotPane string
		var calls int
		resolve := func(ctx context.Context, pane string) (string, error) {
			calls++
			gotPane = pane
			return "owner-x", nil
		}
		stdout, _, exit := runExpediteEvent(t, dir, clk, resolve, notifyTestPane, "w1", "idle")
		if exit != 0 || stdout != "" {
			t.Fatalf("plugin event idle (name): exit %d stdout %q", exit, stdout)
		}
		if calls != 1 || gotPane != notifyTestPane {
			t.Fatalf("PaneAgentName calls = %d pane %q, want exactly one for %s", calls, gotPane, notifyTestPane)
		}
		if n := len(flowSendCalls(t, fakeDir)); n != 2 {
			t.Fatalf("the hook made %d sends total, want 2 (the name-matched one delivered)", n)
		}
		if d := readLedger(t, dir).Notifications[0].Deliveries[0]; d.State != notify.StateAccepted {
			t.Fatalf("delivery = %+v, want accepted", d)
		}
	})

	t.Run("failed resolution writes friction and expedites nothing", func(t *testing.T) {
		exe, fakeDir := installFakeSoho(t,
			capsRule(capsJSON, 0),
			sendRule(17, 1),
			sendRule(0, 0),
		)
		dir := t.TempDir()
		clk := newTestClock()
		seedBusyDelivery(t, exe, fakeDir, dir, clk, "owner-x")
		nextBefore := readLedger(t, dir).Notifications[0].Deliveries[0].NextAttemptAt
		resolve := func(ctx context.Context, pane string) (string, error) {
			return "", fmt.Errorf("herdr agent get: binary unavailable")
		}
		stdout, _, exit := runExpediteEvent(t, dir, clk, resolve, notifyTestPane, "w1", "idle")
		if exit != 0 || stdout != "" {
			t.Fatalf("plugin event idle (failed resolution): exit %d stdout %q, want 0 and nothing on stdout", exit, stdout)
		}
		friction, err := os.ReadFile(filepath.Join(outbox.StateDir(dir), "friction.log"))
		if err != nil || !strings.Contains(string(friction), "event: recipient name unavailable") {
			t.Fatalf("friction = %q (%v), want the fixed recipient-name line", friction, err)
		}
		if n := len(flowSendCalls(t, fakeDir)); n != 1 {
			t.Fatalf("the failed resolution made %d sends, want still 1 (nothing expedited)", n)
		}
		d := readLedger(t, dir).Notifications[0].Deliveries[0]
		if d.State != notify.StatePending || d.NextAttemptAt != nextBefore {
			t.Fatalf("delivery = %+v, want still pending with NextAttemptAt %q", d, nextBefore)
		}
	})

	t.Run("no pending delivery writes nothing and never calls herdr", func(t *testing.T) {
		exe, fakeDir := installFakeSoho(t, capsRule(capsJSON, 0), sendRule(0, 0))
		dir := t.TempDir()
		setSohoConfig(t, dir, exe, "machine-a")
		clk := newTestClock()
		seedJob(t, dir, notifyTestJob, notifyTestRepo, 0, "accepted")
		mustRunNotify(t, dir, clk, "notify", "register", "owner", "--projeto", notifyTestRepo, "--to", notifyTestOwner)
		vars := map[string]string{jobapi.EnvJobID: notifyTestJob}
		stdout, stderr, exit := runNotifyFlow(t, dir, clk, vars, flowWakeEvent(1, "terminal", ""), "wake")
		if exit != 0 {
			t.Fatalf("wake exit = %d, stderr %q", exit, stderr)
		}
		if n := len(flowSendCalls(t, fakeDir)); n != 1 {
			t.Fatalf("the wake made %d sends, want 1 (accepted, no pending left)", n)
		}
		ledPath := filepath.Join(outbox.StateDir(dir), "notify", "ledger.json")
		before, err := os.ReadFile(ledPath)
		if err != nil {
			t.Fatalf("read ledger: %v", err)
		}
		beforeInfo, err := os.Stat(ledPath)
		if err != nil {
			t.Fatalf("stat ledger: %v", err)
		}
		stdout, _, exit = runExpediteEvent(t, dir, clk, sentinelPaneAgentName(t), notifyTestPane, "w1", "idle")
		if exit != 0 || stdout != "" {
			t.Fatalf("plugin event idle (common case): exit %d stdout %q, want 0 and nothing on stdout", exit, stdout)
		}
		if n := len(flowSendCalls(t, fakeDir)); n != 1 {
			t.Fatalf("the common case made %d sends, want still 1", n)
		}
		after, err := os.ReadFile(ledPath)
		if err != nil {
			t.Fatalf("read ledger: %v", err)
		}
		afterInfo, err := os.Stat(ledPath)
		if err != nil {
			t.Fatalf("stat ledger: %v", err)
		}
		if !bytes.Equal(before, after) {
			t.Fatal("the common case rewrote the ledger file content")
		}
		if !afterInfo.ModTime().Equal(beforeInfo.ModTime()) {
			t.Fatal("the common case rewrote the ledger file (mtime changed)")
		}
		if _, err := os.Stat(filepath.Join(outbox.StateDir(dir), "friction.log")); !os.IsNotExist(err) {
			t.Fatalf("friction.log present (err %v), want absent in the common case", err)
		}
	})
}
