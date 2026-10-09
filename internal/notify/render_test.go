package notify

import (
	"encoding/json"
	"regexp"
	"strings"
	"testing"

	"github.com/djalmajr/herdr-hermes/internal/jobapi"
)

// goldenJobEventLine is the exact rendered line for a blocked job event of
// job-1 (seq 3) with the full registry: owner first, details via the job
// events command (seq-1) and the optional ack.
const goldenJobEventLine = "[herdr-hermes] notification=nf84db8188697ced41e18 class=blocked role=owner job=job-1 project=example-org/example-repo event=blocked seq=3 escalation=blocked | details: herdr-hermes job events --id job-1 --since 2 (or the local outbox: herdr-hermes outbox) | optional ack: herdr-hermes notify ack nf84db8188697ced41e18 --role owner"

// goldenAgentStatusLine is the exact rendered line for the first blocked
// transition of pane w1:p2 in workspace w1, watched for job-1.
const goldenAgentStatusLine = "[herdr-hermes] notification=n61995c5190f08fad90af class=agent_blocked role=owner job=job-1 project=example-org/example-repo pane=w1:p2 workspace=w1 status=blocked escalation=blocked | details: inspect pane w1:p2 | optional ack: herdr-hermes notify ack n61995c5190f08fad90af --role owner"

// goldenAgentDoneLine is the exact rendered line for the first done
// transition of pane w1:p2 in workspace w1, watched for job-1: no
// escalation token, details via the pane.
const goldenAgentDoneLine = "[herdr-hermes] notification=ne4a2d04fd150d99e9710 class=agent_done role=owner job=job-1 project=example-org/example-repo pane=w1:p2 workspace=w1 status=done | details: inspect pane w1:p2 | optional ack: herdr-hermes notify ack ne4a2d04fd150d99e9710 --role owner"

// goldenWorkspaceLine is the exact rendered line for the opened
// transition (N=1) of job workspace w2 (job-1) with the full registry:
// details via the job status command and the outbox hint.
const goldenWorkspaceLine = "[herdr-hermes] notification=n103ac05f9abaf550d1e8 class=workspace_opened role=owner job=job-1 project=example-org/example-repo workspace=w2 | details: herdr-hermes job status --id job-1 (or the local outbox: herdr-hermes outbox) | optional ack: herdr-hermes notify ack n103ac05f9abaf550d1e8 --role owner"

func TestRenderGoldenJobEvent(t *testing.T) {
	in := JobEventInput{
		OutboxSeq: 3,
		Projeto:   "example-org/example-repo",
		JobID:     "job-1",
		Event:     eventJSON(3, jobapi.TypeBlocked, ""),
	}
	n, ok, err := ProjectJobEvent(in, fullRegistry(), fixedNow)
	if err != nil || !ok {
		t.Fatalf("ok = %v, err = %v", ok, err)
	}
	if len(n.Deliveries) == 0 || n.Deliveries[0].Roles[0] != RoleOwner {
		t.Fatalf("no owner delivery: %+v", n.Deliveries)
	}
	if got := Render(n, n.Deliveries[0]); got != goldenJobEventLine {
		t.Errorf("rendered line differs from golden:\n got %q\nwant %q", got, goldenJobEventLine)
	}
}

func TestRenderGoldenAgentStatus(t *testing.T) {
	tr := Transition{
		Pane:      "w1:p2",
		Workspace: "w1",
		From:      "working",
		To:        "blocked",
		N:         1,
		Watch:     Watch{Pane: "w1:p2", Job: "job-1", Projeto: "example-org/example-repo"},
	}
	n, ok := ProjectAgentStatus(tr, fullRegistry(), fixedNow)
	if !ok {
		t.Fatal("ok = false, want projected")
	}
	if len(n.Deliveries) == 0 || n.Deliveries[0].Roles[0] != RoleOwner {
		t.Fatalf("no owner delivery: %+v", n.Deliveries)
	}
	if got := Render(n, n.Deliveries[0]); got != goldenAgentStatusLine {
		t.Errorf("rendered line differs from golden:\n got %q\nwant %q", got, goldenAgentStatusLine)
	}
}

// TestRenderGoldenAgentDone: the exact done line (class agent_done, no
// escalation token).
func TestRenderGoldenAgentDone(t *testing.T) {
	tr := Transition{
		Pane:      "w1:p2",
		Workspace: "w1",
		From:      "working",
		To:        "done",
		N:         1,
		Watch:     Watch{Pane: "w1:p2", Job: "job-1", Projeto: "example-org/example-repo"},
	}
	n, ok := ProjectAgentStatus(tr, fullRegistry(), fixedNow)
	if !ok {
		t.Fatal("ok = false, want projected")
	}
	if len(n.Deliveries) == 0 || n.Deliveries[0].Roles[0] != RoleOwner {
		t.Fatalf("no owner delivery: %+v", n.Deliveries)
	}
	if got := Render(n, n.Deliveries[0]); got != goldenAgentDoneLine {
		t.Errorf("rendered line differs from golden:\n got %q\nwant %q", got, goldenAgentDoneLine)
	}
}

// TestRenderGoldenWorkspace: the exact workspace line (details via the
// job status command, the workspace token).
func TestRenderGoldenWorkspace(t *testing.T) {
	n, err := ProjectWorkspace(WorkspaceInput{Event: WorkspaceCreated, Workspace: "w2", N: 1, JobID: "job-1", Projeto: "example-org/example-repo"}, fullRegistry(), fixedNow)
	if err != nil {
		t.Fatalf("ProjectWorkspace: %v", err)
	}
	if len(n.Deliveries) == 0 || n.Deliveries[0].Roles[0] != RoleOwner {
		t.Fatalf("no owner delivery: %+v", n.Deliveries)
	}
	if got := Render(n, n.Deliveries[0]); got != goldenWorkspaceLine {
		t.Errorf("rendered line differs from golden:\n got %q\nwant %q", got, goldenWorkspaceLine)
	}
}

// TestRenderSingleLine: no newline ever; the message is one line.
func TestRenderSingleLine(t *testing.T) {
	n, ok, err := ProjectJobEvent(JobEventInput{OutboxSeq: 3, Projeto: "example-org/example-repo", JobID: "job-1", Event: eventJSON(3, jobapi.TypeBlocked, "")}, fullRegistry(), fixedNow)
	if err != nil || !ok {
		t.Fatalf("ok = %v, err = %v", ok, err)
	}
	for i, d := range n.Deliveries {
		line := Render(n, d)
		if strings.ContainsAny(line, "\n\r") {
			t.Errorf("delivery %d rendered line contains a newline: %q", i, line)
		}
	}
}

// TestRenderValidation: a value that fails its check renders as "?", and a
// value that is empty is omitted entirely.
func TestRenderValidation(t *testing.T) {
	base := func() *Notification {
		return &Notification{
			ID:          NotificationID("job:job-1:3"),
			SourceKind:  SourceJobEvent,
			Class:       ClassBlocked,
			JobID:       "job-1",
			Projeto:     "example-org/example-repo",
			EventTipo:   jobapi.TypeBlocked,
			EventSeq:    3,
			Escalations: []string{EscBlocked},
			ObservedAt:  fixedNowTS,
			Deliveries:  []Delivery{{Roles: []string{RoleOwner}, Ref: "local/w1:p3", State: StatePending}},
		}
	}
	roleOwner := Delivery{Roles: []string{RoleOwner}, Ref: "local/w1:p3", State: StatePending}
	cases := []struct {
		name    string
		mutate  func(n *Notification, d *Delivery)
		want    string
		notWant string
	}{
		{"invalid id", func(n *Notification, d *Delivery) { n.ID = "zzz" }, "notification=?", ""},
		{"invalid class", func(n *Notification, d *Delivery) { n.Class = "weird" }, "class=?", ""},
		{"class agent_done renders", func(n *Notification, d *Delivery) { n.Class = ClassAgentDone }, "class=agent_done", "class=?"},
		{"class workspace_opened renders", func(n *Notification, d *Delivery) { n.Class = ClassWorkspaceOpened }, "class=workspace_opened", "class=?"},
		{"class workspace_closed renders", func(n *Notification, d *Delivery) { n.Class = ClassWorkspaceClosed }, "class=workspace_closed", "class=?"},
		{"invalid job", func(n *Notification, d *Delivery) { n.JobID = "bad job!" }, "job=?", "details:"},
		{"invalid project", func(n *Notification, d *Delivery) { n.Projeto = "not-a-repo" }, "project=?", ""},
		{"invalid event tipo", func(n *Notification, d *Delivery) { n.EventTipo = "brand_new_tipo" }, "event=?", ""},
		{"invalid pane", func(n *Notification, d *Delivery) { n.Pane = "no-colon" }, "pane=?", ""},
		{"invalid workspace", func(n *Notification, d *Delivery) { n.Workspace = "bad ws" }, "workspace=?", ""},
		{"invalid status", func(n *Notification, d *Delivery) { n.Status = "crashed" }, "status=?", ""},
		{"invalid escalation", func(n *Notification, d *Delivery) { n.Escalations = []string{"weird"} }, "escalation=weird", ""},
		{"invalid role", func(n *Notification, d *Delivery) { d.Roles = []string{"hacker"} }, "role=hacker", ""},
		{"mixed roles", func(n *Notification, d *Delivery) { d.Roles = []string{RoleOwner, "hacker"} }, "role=owner,?", ""},
		{"empty values omitted", func(n *Notification, d *Delivery) {
			n.JobID = ""
			n.Projeto = ""
			n.EventTipo = ""
			n.EventSeq = 0
			n.Escalations = nil
		}, "", "job="},
		{"empty roles omitted", func(n *Notification, d *Delivery) { d.Roles = nil }, "", "role="},
		{"exit zero renders", func(n *Notification, d *Delivery) { n.Exit = intPtr(0) }, "exit=0", ""},
		{"exit renders", func(n *Notification, d *Delivery) { n.Exit = intPtr(22) }, "exit=22", ""},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			n := base()
			d := roleOwner
			c.mutate(n, &d)
			line := Render(n, d)
			switch c.name {
			case "invalid escalation":
				// Each invalid escalation entry renders as ?.
				if !strings.Contains(line, "escalation=?") {
					t.Fatalf("line lacks escalation=?: %s", line)
				}
			case "invalid role":
				// A wholly invalid role list renders role=? and drops the
				// ack (the first role must be a real role).
				if !strings.Contains(line, "role=?") {
					t.Fatalf("line lacks role=?: %s", line)
				}
				if strings.Contains(line, "optional ack") {
					t.Fatalf("line keeps the ack with an invalid first role: %s", line)
				}
			default:
				if c.want != "" && !strings.Contains(line, c.want) {
					t.Fatalf("line lacks %q: %s", c.want, line)
				}
				if c.notWant != "" && strings.Contains(line, c.notWant) {
					t.Fatalf("line keeps %q: %s", c.notWant, line)
				}
			}
		})
	}
}

// TestRenderSegments: the details and ack segments follow the source kind
// and the delivery roles.
func TestRenderSegments(t *testing.T) {
	// Raise: no details segment; project token present, job/event/seq
	// omitted; ack uses the first role.
	raiser, err := ProjectRaise(RaiseInput{ID: "raise-1", Class: ClassStuck, Projeto: "example-org/example-repo"}, fullRegistry(), fixedNow)
	if err != nil {
		t.Fatalf("raise: %v", err)
	}
	line := Render(raiser, raiser.Deliveries[0])
	if strings.Contains(line, "details:") {
		t.Errorf("raise line has a details segment: %s", line)
	}
	if !strings.Contains(line, "project=example-org/example-repo") {
		t.Errorf("raise line lacks the project token: %s", line)
	}
	for _, tok := range []string{" job=", " event=", " seq=", " pane=", " workspace=", " status=", " exit="} {
		if strings.Contains(line, tok) {
			t.Errorf("raise line has token %q: %s", tok, line)
		}
	}
	if !strings.HasSuffix(line, "optional ack: herdr-hermes notify ack "+raiser.ID+" --role owner") {
		t.Errorf("raise line lacks the ack suffix: %s", line)
	}

	// Agent status with an invalid pane: the pane token is ? and the
	// details segment is omitted.
	tr := Transition{Pane: "w1:p2", Workspace: "w1", To: "blocked", N: 1, Watch: Watch{Pane: "w1:p2", Job: "job-1", Projeto: "example-org/example-repo"}}
	n, ok := ProjectAgentStatus(tr, fullRegistry(), fixedNow)
	if !ok {
		t.Fatal("agent status not projected")
	}
	n.Pane = "badpane"
	line = Render(n, n.Deliveries[0])
	if !strings.Contains(line, "pane=?") {
		t.Errorf("line lacks pane=?: %s", line)
	}
	if strings.Contains(line, "details:") {
		t.Errorf("line keeps details with an invalid pane: %s", line)
	}

	// Workspace with a valid job: details via the job status command and
	// the outbox hint.
	nw, err := ProjectWorkspace(WorkspaceInput{Event: WorkspaceClosed, Workspace: "w2", N: 2, JobID: "job-1", Projeto: "example-org/example-repo"}, fullRegistry(), fixedNow)
	if err != nil {
		t.Fatalf("ProjectWorkspace: %v", err)
	}
	line = Render(nw, nw.Deliveries[0])
	if !strings.Contains(line, "details: herdr-hermes job status --id job-1 (or the local outbox: herdr-hermes outbox)") {
		t.Errorf("workspace line lacks the job status details: %s", line)
	}
	if !strings.Contains(line, "workspace=w2") {
		t.Errorf("workspace line lacks the workspace token: %s", line)
	}
	if !strings.Contains(line, "class=workspace_closed") {
		t.Errorf("workspace line lacks class=workspace_closed: %s", line)
	}

	// Workspace with an empty job: no details segment.
	manualWS := &Notification{
		ID:         NotificationID("workspace:w2:1:closed"),
		SourceKind: SourceWorkspace,
		SourceID:   "workspace:w2:1:closed",
		Class:      ClassWorkspaceClosed,
		Workspace:  "w2",
		ObservedAt: fixedNowTS,
	}
	line = Render(manualWS, Delivery{Roles: []string{RoleOwner}, Ref: "local/w1:p3", State: StatePending})
	if strings.Contains(line, "details:") {
		t.Errorf("line keeps details without a valid job: %s", line)
	}
	if strings.Contains(line, " job=") {
		t.Errorf("line keeps an empty job token: %s", line)
	}

	// Job event with a valid job but seq 0: no details segment.
	manual := &Notification{
		ID:          NotificationID("job:job-1:0"),
		SourceKind:  SourceJobEvent,
		SourceID:    "job:job-1:0",
		Class:       ClassQuestion,
		JobID:       "job-1",
		EventTipo:   jobapi.TypeQuestion,
		Escalations: nil,
		ObservedAt:  fixedNowTS,
	}
	line = Render(manual, Delivery{Roles: []string{RoleOwner}, Ref: "local/w1:p3", State: StatePending})
	if strings.Contains(line, "details:") {
		t.Errorf("line keeps details with seq 0: %s", line)
	}
	if strings.Contains(line, " seq=") {
		t.Errorf("line keeps a zero seq token: %s", line)
	}
	// The id is a valid pattern and the first role is real: the ack stays.
	if !strings.HasSuffix(line, "optional ack: herdr-hermes notify ack "+manual.ID+" --role owner") {
		t.Errorf("line lacks the ack suffix: %s", line)
	}
}

// TestRenderFreeTextNeverRendered: a terminal job event with exit 19
// (failed class) whose merged owner+orchestrator delivery is rendered:
// comma-joined roles, the exit string parsed to a number, and a head made
// only of the fixed tokens.
func TestRenderFreeTextNeverRendered(t *testing.T) {
	reg := fullRegistry()
	reg.Owners["example-org/example-repo"] = "owner-agent"
	reg.Orchestrators["job-1"] = "owner-agent"
	n, ok, err := ProjectJobEvent(JobEventInput{OutboxSeq: 9, Projeto: "example-org/example-repo", JobID: "job-1", Event: eventJSON(9, jobapi.TypeTerminal, `,"refs":{"exit":"19"}`)}, reg, fixedNow)
	if err != nil || !ok {
		t.Fatalf("ok = %v, err = %v", ok, err)
	}
	// Merged owner+orchestrator delivery: comma-joined roles, exit 19 as a
	// string parsed to a number, failed class.
	if len(n.Deliveries) != 2 {
		t.Fatalf("deliveries = %d, want 2", len(n.Deliveries))
	}
	line := Render(n, n.Deliveries[0])
	if !strings.Contains(line, "role=owner,orchestrator") {
		t.Errorf("line lacks role=owner,orchestrator: %s", line)
	}
	if !strings.Contains(line, "exit=19") || !strings.Contains(line, "class=failed") {
		t.Errorf("line lacks exit=19/class=failed: %s", line)
	}
	// Every key=value pair of the head must match the fixed token set.
	head := line
	if i := strings.Index(line, " | "); i >= 0 {
		head = line[:i]
	}
	tokens := strings.Fields(strings.TrimPrefix(head, "[herdr-hermes] "))
	wantKeys := []string{"notification", "class", "role", "job", "project", "event", "seq", "exit", "escalation"}
	if len(tokens) != len(wantKeys) {
		t.Fatalf("head tokens = %v, want %v", tokens, wantKeys)
	}
	for i, k := range wantKeys {
		if !strings.HasPrefix(tokens[i], k+"=") {
			t.Errorf("token %d = %q, want prefix %s=", i, tokens[i], k)
		}
	}
	// The whole notification JSON must not leak beyond the allowlisted
	// fields: resumo/refs are not struct fields at all.
	b, err := json.Marshal(n)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(b), "resumo") || strings.Contains(string(b), "refs") {
		t.Errorf("notification json carries event payload: %s", b)
	}
}

// TestRenderDeterministic: the same notification and delivery always
// render the same line.
func TestRenderDeterministic(t *testing.T) {
	idPattern := regexp.MustCompile(`^n[0-9a-f]{20}$`)
	if !idPattern.MatchString(NotificationID("job:job-1:3")) {
		t.Fatal("notification id does not match the render pattern")
	}
	n, ok, err := ProjectJobEvent(JobEventInput{OutboxSeq: 3, Projeto: "example-org/example-repo", JobID: "job-1", Event: eventJSON(3, jobapi.TypeBlocked, "")}, fullRegistry(), fixedNow)
	if err != nil || !ok {
		t.Fatalf("ok = %v, err = %v", ok, err)
	}
	if Render(n, n.Deliveries[0]) != Render(n, n.Deliveries[0]) {
		t.Error("Render is not deterministic")
	}
}
