package a2acore

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
)

// shared fixtures

func sendSubject(projects ...string) Subject {
	return Subject{
		ID:       "agent-1",
		Ref:      "m1/w1:p1",
		Projects: projects,
		Actions:  []Action{ActionSend},
	}
}

func validMeta() map[string]any {
	return map[string]any{
		MetaProject: "org/repo",
		MetaSession: "sess-1",
		MetaEpoch:   float64(1),
		MetaSeq:     float64(1),
	}
}

func baseMessage() MessageView {
	return MessageView{
		ID:       "msg-1",
		Role:     RoleUser,
		Parts:    []PartView{{Kind: PartText, Text: "hello there"}},
		Metadata: validMeta(),
	}
}

func baseTask() *TaskView {
	return &TaskView{
		ID:        "task-1",
		ContextID: "ctx-1",
		State:     StateWorking,
		Owner:     "agent-1",
		Project:   "org/repo",
		Job:       "job-1",
		Messages:  1,
	}
}

func TestSubjectPolicyAuthorize(t *testing.T) {
	cases := []struct {
		name string
		sub  Subject
		act  Action
		proj string
		want error // nil when allowed, the sentinel when refused
	}{
		{"allowed action and project", Subject{Actions: []Action{ActionSend}, Projects: []string{"org/repo"}}, ActionSend, "org/repo", nil},
		{"any project", Subject{Actions: []Action{ActionSend}, Projects: []string{AnyProject}}, ActionSend, "other/repo", nil},
		{"missing action", Subject{Actions: []Action{ActionRead}, Projects: []string{"org/repo"}}, ActionSend, "org/repo", ErrForbidden},
		{"other project", Subject{Actions: []Action{ActionSend}, Projects: []string{"a/b"}}, ActionSend, "c/d", ErrForbidden},
		{"empty project for allowed action", Subject{Actions: []Action{ActionRead}}, ActionRead, "", nil},
		{"empty project no projects", Subject{Actions: []Action{ActionSend}}, ActionSend, "", nil},
		{"no actions no projects", Subject{}, ActionSend, "", ErrForbidden},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := (SubjectPolicy{}).Authorize(c.sub, c.act, c.proj)
			if (err == nil) != (c.want == nil) {
				t.Fatalf("want allowed=%v got err=%v", c.want == nil, err)
			}
			if c.want != nil && !errors.Is(err, c.want) {
				t.Fatalf("error does not wrap %v: %v", c.want, err)
			}
		})
	}
}

func TestParseCorrelation(t *testing.T) {
	base := func() map[string]any {
		return map[string]any{
			MetaProject: "org/repo",
			MetaSession: "sess-1",
			MetaEpoch:   float64(1),
			MetaSeq:     float64(1),
		}
	}
	cases := []struct {
		name     string
		mutate   func(map[string]any)
		p, j, s  string
		e, q     int64
		wantCode string // "" when ok
	}{
		{"basic float64", func(m map[string]any) { m[MetaEpoch] = float64(3); m[MetaSeq] = float64(9) }, "org/repo", "", "sess-1", 3, 9, ""},
		{"integral float64 values", func(m map[string]any) { m[MetaEpoch] = float64(4.0); m[MetaSeq] = float64(2.0) }, "org/repo", "", "sess-1", 4, 2, ""},
		{"int and int64", func(m map[string]any) { m[MetaEpoch] = int(5); m[MetaSeq] = int64(6) }, "org/repo", "", "sess-1", 5, 6, ""},
		{"json numbers", func(m map[string]any) { m[MetaEpoch] = json.Number("7"); m[MetaSeq] = json.Number("8") }, "org/repo", "", "sess-1", 7, 8, ""},
		{"json number integral form", func(m map[string]any) { m[MetaEpoch] = json.Number("1.0") }, "org/repo", "", "sess-1", 1, 1, ""},
		{"with job", func(m map[string]any) { m[MetaJob] = "job-1" }, "org/repo", "job-1", "sess-1", 1, 1, ""},
		{"project and job absent", func(m map[string]any) { delete(m, MetaProject) }, "", "", "sess-1", 1, 1, ""},
		{"session missing", func(m map[string]any) { delete(m, MetaSession) }, "", "", "", 0, 0, RefuseInvalidCorrelation},
		{"session wrong type", func(m map[string]any) { m[MetaSession] = float64(1) }, "", "", "", 0, 0, RefuseInvalidCorrelation},
		{"session bad pattern", func(m map[string]any) { m[MetaSession] = "bad session" }, "", "", "", 0, 0, RefuseInvalidCorrelation},
		{"session empty", func(m map[string]any) { m[MetaSession] = "" }, "", "", "", 0, 0, RefuseInvalidCorrelation},
		{"session too long", func(m map[string]any) { m[MetaSession] = strings.Repeat("a", 65) }, "", "", "", 0, 0, RefuseInvalidCorrelation},
		{"epoch missing", func(m map[string]any) { delete(m, MetaEpoch) }, "", "", "", 0, 0, RefuseInvalidCorrelation},
		{"epoch string", func(m map[string]any) { m[MetaEpoch] = "3" }, "", "", "", 0, 0, RefuseInvalidCorrelation},
		{"epoch bool", func(m map[string]any) { m[MetaEpoch] = true }, "", "", "", 0, 0, RefuseInvalidCorrelation},
		{"epoch nil", func(m map[string]any) { m[MetaEpoch] = nil }, "", "", "", 0, 0, RefuseInvalidCorrelation},
		{"epoch fractional", func(m map[string]any) { m[MetaEpoch] = float64(1.5) }, "", "", "", 0, 0, RefuseInvalidCorrelation},
		{"epoch zero", func(m map[string]any) { m[MetaEpoch] = int(0) }, "", "", "", 0, 0, RefuseInvalidCorrelation},
		{"epoch negative", func(m map[string]any) { m[MetaEpoch] = float64(-3) }, "", "", "", 0, 0, RefuseInvalidCorrelation},
		{"epoch above max", func(m map[string]any) { m[MetaEpoch] = float64(1 << 53) }, "", "", "", 0, 0, RefuseInvalidCorrelation},
		{"seq missing", func(m map[string]any) { delete(m, MetaSeq) }, "", "", "", 0, 0, RefuseInvalidCorrelation},
		{"seq string", func(m map[string]any) { m[MetaSeq] = "2" }, "", "", "", 0, 0, RefuseInvalidCorrelation},
		{"seq fractional", func(m map[string]any) { m[MetaSeq] = float64(2.5) }, "", "", "", 0, 0, RefuseInvalidCorrelation},
		{"seq zero", func(m map[string]any) { m[MetaSeq] = int(0) }, "", "", "", 0, 0, RefuseInvalidCorrelation},
		{"seq negative", func(m map[string]any) { m[MetaSeq] = float64(-1) }, "", "", "", 0, 0, RefuseInvalidCorrelation},
		{"seq above max", func(m map[string]any) { m[MetaSeq] = float64(1 << 53) }, "", "", "", 0, 0, RefuseInvalidCorrelation},
		{"project wrong type", func(m map[string]any) { m[MetaProject] = float64(3) }, "", "", "", 0, 0, RefuseInvalidCorrelation},
		{"project empty", func(m map[string]any) { m[MetaProject] = "" }, "", "", "", 0, 0, RefuseInvalidCorrelation},
		{"project missing slash", func(m map[string]any) { m[MetaProject] = "org" }, "", "", "", 0, 0, RefuseInvalidCorrelation},
		{"project double slash", func(m map[string]any) { m[MetaProject] = "a/b/c" }, "", "", "", 0, 0, RefuseInvalidCorrelation},
		{"job wrong type", func(m map[string]any) { m[MetaJob] = float64(5) }, "", "", "", 0, 0, RefuseInvalidCorrelation},
		{"job empty", func(m map[string]any) { m[MetaJob] = "" }, "", "", "", 0, 0, RefuseInvalidCorrelation},
		{"job with space", func(m map[string]any) { m[MetaJob] = "job 1" }, "", "", "", 0, 0, RefuseInvalidCorrelation},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m := base()
			c.mutate(m)
			p, j, s, e, q, r := ParseCorrelation(m)
			if c.wantCode == "" {
				if r != nil {
					t.Fatalf("want ok, got %v", r)
				}
				if p != c.p || j != c.j || s != c.s || e != c.e || q != c.q {
					t.Fatalf("got (%q %q %q %d %d), want (%q %q %q %d %d)", p, j, s, e, q, c.p, c.j, c.s, c.e, c.q)
				}
				return
			}
			if r == nil {
				t.Fatalf("want refusal %s, got ok (%q %q %q %d %d)", c.wantCode, p, j, s, e, q)
			}
			if r.Code != c.wantCode {
				t.Fatalf("want code %s, got %s", c.wantCode, r.Code)
			}
		})
	}
	t.Run("empty metadata", func(t *testing.T) {
		_, _, _, _, _, r := ParseCorrelation(map[string]any{})
		if r == nil || r.Code != RefuseInvalidCorrelation {
			t.Fatalf("want invalid_correlation, got %v", r)
		}
	})
	t.Run("nil metadata", func(t *testing.T) {
		_, _, _, _, _, r := ParseCorrelation(nil)
		if r == nil || r.Code != RefuseInvalidCorrelation {
			t.Fatalf("want invalid_correlation, got %v", r)
		}
	})
}

type evalCase struct {
	name     string
	projects []string // subject projects; default ["org/repo"]
	mutate   func(*MessageView)
	existing *TaskView
	lim      Limits
	wantCode string // "" when accepted
}

func TestEvaluateRefusals(t *testing.T) {
	cases := []evalCase{
		{name: "agent role", mutate: func(m *MessageView) { m.Role = RoleAgent }, wantCode: RefuseInvalidMessage},
		{name: "empty role", mutate: func(m *MessageView) { m.Role = "" }, wantCode: RefuseInvalidMessage},
		{name: "zero parts", mutate: func(m *MessageView) { m.Parts = nil }, wantCode: RefuseInvalidMessage},
		{name: "too many parts", mutate: func(m *MessageView) {
			m.Parts = []PartView{{Kind: PartText, Text: "a"}, {Kind: PartText, Text: "b"}, {Kind: PartText, Text: "c"}}
		}, lim: Limits{MaxParts: 2}, wantCode: RefuseInvalidMessage},
		{name: "data part", mutate: func(m *MessageView) { m.Parts = []PartView{{Kind: PartData, Text: "x"}} }, wantCode: RefuseInvalidMessage},
		{name: "file part", mutate: func(m *MessageView) { m.Parts = []PartView{{Kind: PartFile, Text: "x"}} }, wantCode: RefuseInvalidMessage},
		{name: "other part", mutate: func(m *MessageView) { m.Parts = []PartView{{Kind: PartOther, Text: "x"}} }, wantCode: RefuseInvalidMessage},
		{name: "blank text", mutate: func(m *MessageView) { m.Parts = []PartView{{Kind: PartText, Text: "   \n\t "}} }, wantCode: RefuseInvalidMessage},
		{name: "oversized text", mutate: func(m *MessageView) { m.Parts = []PartView{{Kind: PartText, Text: "123456789"}} }, lim: Limits{MaxTextBytes: 8}, wantCode: RefuseInvalidMessage},
		{name: "invalid utf-8 text", mutate: func(m *MessageView) { m.Parts = []PartView{{Kind: PartText, Text: string([]byte{0xff, 0xfe, 0xfd})}} }, wantCode: RefuseInvalidMessage},
		{name: "bad message id", mutate: func(m *MessageView) { m.ID = "msg 1" }, wantCode: RefuseInvalidMessage},
		{name: "empty message id", mutate: func(m *MessageView) { m.ID = "" }, wantCode: RefuseInvalidMessage},
		{name: "long message id", mutate: func(m *MessageView) { m.ID = strings.Repeat("a", 129) }, wantCode: RefuseInvalidMessage},
		{name: "new task missing project", mutate: func(m *MessageView) { delete(m.Metadata, MetaProject) }, wantCode: RefuseInvalidCorrelation},
		{name: "new task bad project", mutate: func(m *MessageView) { m.Metadata[MetaProject] = "org" }, wantCode: RefuseInvalidCorrelation},
		{name: "new task bad job", mutate: func(m *MessageView) { m.Metadata[MetaJob] = "job 1" }, wantCode: RefuseInvalidCorrelation},
		{name: "new task with task id set", mutate: func(m *MessageView) { m.TaskID = "task-1" }, wantCode: RefuseInvalidCorrelation},
		{name: "new task bad context id", mutate: func(m *MessageView) { m.ContextID = "ctx 1" }, wantCode: RefuseInvalidCorrelation},
		{name: "new task missing session", mutate: func(m *MessageView) { delete(m.Metadata, MetaSession) }, wantCode: RefuseInvalidCorrelation},
		{name: "new task fractional epoch", mutate: func(m *MessageView) { m.Metadata[MetaEpoch] = float64(1.5) }, wantCode: RefuseInvalidCorrelation},
		{name: "continuation other owner", mutate: func(m *MessageView) { m.TaskID = "task-1" }, existing: func() *TaskView { t := baseTask(); t.Owner = "agent-2"; return t }(), wantCode: RefuseUnknownTask},
		{name: "continuation terminal task", mutate: func(m *MessageView) { m.TaskID = "task-1" }, existing: func() *TaskView { t := baseTask(); t.State = StateCompleted; return t }(), wantCode: RefuseTerminal},
		{name: "continuation project mismatch", mutate: func(m *MessageView) {
			m.TaskID = "task-1"
			m.Metadata[MetaProject] = "other/repo"
		}, existing: baseTask(), wantCode: RefuseCorrelationMismatch},
		{name: "continuation job mismatch", mutate: func(m *MessageView) {
			m.TaskID = "task-1"
			m.Metadata[MetaJob] = "job-2"
		}, existing: baseTask(), wantCode: RefuseCorrelationMismatch},
		{name: "continuation context mismatch", mutate: func(m *MessageView) {
			m.TaskID = "task-1"
			m.ContextID = "ctx-2"
		}, existing: baseTask(), wantCode: RefuseCorrelationMismatch},
		{name: "continuation loop limit default", mutate: func(m *MessageView) { m.TaskID = "task-1" }, existing: func() *TaskView { t := baseTask(); t.Messages = DefaultLimits.MaxMessagesPerTask; return t }(), wantCode: RefuseLoopLimit},
		{name: "continuation loop limit custom", mutate: func(m *MessageView) { m.TaskID = "task-1" }, existing: func() *TaskView { t := baseTask(); t.Messages = 3; return t }(), lim: Limits{MaxMessagesPerTask: 3}, wantCode: RefuseLoopLimit},
		{name: "unauthorized project new task", projects: []string{"a/b"}, wantCode: RefuseUnauthorizedProject},
		{name: "unauthorized inherited project continuation", projects: []string{"a/b"}, mutate: func(m *MessageView) { m.TaskID = "task-1" }, existing: baseTask(), wantCode: RefuseUnauthorizedProject},
	}
	for _, c := range cases {
		projects := c.projects
		if projects == nil {
			projects = []string{"org/repo"}
		}
		t.Run(c.name, func(t *testing.T) {
			m := baseMessage()
			if c.mutate != nil {
				c.mutate(&m)
			}
			_, r := Evaluate(sendSubject(projects...), m, c.existing, c.lim)
			if r == nil {
				t.Fatalf("want refusal %s, got accepted", c.wantCode)
			}
			if r.Code != c.wantCode {
				t.Fatalf("want code %s, got %s (%s)", c.wantCode, r.Code, r.Detail)
			}
		})
	}
}

func TestEvaluateHappyPaths(t *testing.T) {
	t.Run("new task minimal", func(t *testing.T) {
		m := baseMessage()
		delete(m.Metadata, MetaProject)
		m.Metadata[MetaProject] = "org/repo"
		m.Metadata[MetaEpoch] = float64(2)
		m.Metadata[MetaSeq] = int64(3)
		env, r := Evaluate(sendSubject("org/repo"), m, nil, Limits{})
		if r != nil {
			t.Fatalf("want ok, got %v", r)
		}
		want := Envelope{
			Subject:    "agent-1",
			SubjectRef: "m1/w1:p1",
			MessageID:  "msg-1",
			ContextID:  "",
			Project:    "org/repo",
			Session:    "sess-1",
			Epoch:      2,
			Seq:        3,
			TextBytes:  len("hello there"),
		}
		if env != want {
			t.Fatalf("envelope mismatch:\n got %+v\nwant %+v", env, want)
		}
	})
	t.Run("new task with job and chosen context", func(t *testing.T) {
		m := baseMessage()
		m.ContextID = "ctx-new"
		m.Metadata[MetaJob] = "job-1"
		m.Metadata[MetaEpoch] = json.Number("4")
		env, r := Evaluate(sendSubject(AnyProject), m, nil, Limits{})
		if r != nil {
			t.Fatalf("want ok, got %v", r)
		}
		if env.TaskID != "" || env.ContextID != "ctx-new" || env.Project != "org/repo" || env.Job != "job-1" || env.Epoch != 4 {
			t.Fatalf("unexpected envelope %+v", env)
		}
	})
	t.Run("continuation inherits project and job", func(t *testing.T) {
		m := baseMessage()
		m.TaskID = "task-1"
		m.ContextID = "ctx-1"
		m.Metadata[MetaEpoch] = int(1)
		m.Metadata[MetaSeq] = json.Number("2")
		env, r := Evaluate(sendSubject("org/repo"), m, baseTask(), Limits{})
		if r != nil {
			t.Fatalf("want ok, got %v", r)
		}
		want := Envelope{
			Subject:    "agent-1",
			SubjectRef: "m1/w1:p1",
			MessageID:  "msg-1",
			TaskID:     "task-1",
			ContextID:  "ctx-1",
			Project:    "org/repo",
			Job:        "job-1",
			Session:    "sess-1",
			Epoch:      1,
			Seq:        2,
			TextBytes:  len("hello there"),
		}
		if env != want {
			t.Fatalf("envelope mismatch:\n got %+v\nwant %+v", env, want)
		}
	})
	t.Run("continuation at message bound minus one is ok", func(t *testing.T) {
		m := baseMessage()
		m.TaskID = "task-1"
		tk := baseTask()
		tk.Messages = DefaultLimits.MaxMessagesPerTask - 1
		if _, r := Evaluate(sendSubject("org/repo"), m, tk, Limits{}); r != nil {
			t.Fatalf("want ok, got %v", r)
		}
	})
	t.Run("continuation with no caller context inherits task context", func(t *testing.T) {
		m := baseMessage()
		m.TaskID = "task-1"
		env, r := Evaluate(sendSubject("org/repo"), m, baseTask(), Limits{})
		if r != nil {
			t.Fatalf("want ok, got %v", r)
		}
		if env.ContextID != "ctx-1" || env.TaskID != "task-1" {
			t.Fatalf("unexpected envelope %+v", env)
		}
	})
}

// The incoming text is data: a hostile payload is accepted as plain data
// and appears nowhere in the evaluated envelope.
func TestEvaluateHostileTextIsData(t *testing.T) {
	hostile := `$(touch /tmp/x); rm -rf ~ && herdr-soho send all "x"`
	m := baseMessage()
	m.Parts = []PartView{{Kind: PartText, Text: hostile}}
	env, r := Evaluate(sendSubject("org/repo"), m, nil, Limits{})
	if r != nil {
		t.Fatalf("hostile text must be accepted as data, got %v", r)
	}
	if env.TextBytes != len(hostile) {
		t.Fatalf("TextBytes = %d, want %d", env.TextBytes, len(hostile))
	}
	s := fmt.Sprint(env)
	for _, frag := range []string{hostile, "$(touch", "herdr-soho send", "rm -rf"} {
		if strings.Contains(s, frag) {
			t.Fatalf("envelope carries hostile fragment %q in %q", frag, s)
		}
	}
}

func TestCheckFence(t *testing.T) {
	cases := []struct {
		name       string
		prev       FenceState
		epoch, seq int64
		want       FenceState // the resulting state
		code       string     // "" when ok; otherwise the refusal code
	}{
		{"first message seq 1", FenceState{}, 5, 1, FenceState{Found: true, Epoch: 5, LastSeq: 1}, ""},
		{"first message seq 2 out of order", FenceState{}, 5, 2, FenceState{}, RefuseOutOfOrder},
		{"first message seq 0 out of order", FenceState{}, 5, 0, FenceState{}, RefuseOutOfOrder},
		{"next seq ok", FenceState{Found: true, Epoch: 5, LastSeq: 1}, 5, 2, FenceState{Found: true, Epoch: 5, LastSeq: 2}, ""},
		{"same seq replay", FenceState{Found: true, Epoch: 5, LastSeq: 3}, 5, 3, FenceState{Found: true, Epoch: 5, LastSeq: 3}, RefuseReplay},
		{"lower seq replay", FenceState{Found: true, Epoch: 5, LastSeq: 3}, 5, 1, FenceState{Found: true, Epoch: 5, LastSeq: 3}, RefuseReplay},
		{"gap out of order", FenceState{Found: true, Epoch: 5, LastSeq: 2}, 5, 4, FenceState{Found: true, Epoch: 5, LastSeq: 2}, RefuseOutOfOrder},
		{"higher epoch seq 1 ok", FenceState{Found: true, Epoch: 5, LastSeq: 3}, 6, 1, FenceState{Found: true, Epoch: 6, LastSeq: 1}, ""},
		{"higher epoch seq 3 out of order", FenceState{Found: true, Epoch: 5, LastSeq: 3}, 6, 3, FenceState{Found: true, Epoch: 5, LastSeq: 3}, RefuseOutOfOrder},
		{"lower epoch stale", FenceState{Found: true, Epoch: 6, LastSeq: 1}, 5, 1, FenceState{Found: true, Epoch: 6, LastSeq: 1}, RefuseStaleEpoch},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, r := CheckFence(c.prev, Envelope{Epoch: c.epoch, Seq: c.seq})
			if c.code == "" {
				if r != nil {
					t.Fatalf("want ok, got %v", r)
				}
				if got != c.want {
					t.Fatalf("got %v, want %v", got, c.want)
				}
				return
			}
			if r == nil {
				t.Fatalf("want refusal %s, got ok with state %v", c.code, got)
			}
			if r.Code != c.code {
				t.Fatalf("want code %s, got %s", c.code, r.Code)
			}
			if got != c.prev {
				t.Fatalf("refusal must return prev unchanged: got %v, prev %v", got, c.prev)
			}
		})
	}
}

func TestValidateDecision(t *testing.T) {
	base := Decision{
		ID:     "d-1",
		TaskID: "task-1",
		Kind:   DecisionAnswer,
		Text:   "ok, send it",
		Actor:  "local",
	}
	cases := []struct {
		name    string
		mutate  func(*Decision)
		lim     Limits
		wantNil bool
	}{
		{"happy", nil, Limits{}, true},
		{"happy custom limit", nil, Limits{MaxDecisionBytes: 16 << 10}, true},
		{"bad id space", func(d *Decision) { d.ID = "d 1" }, Limits{}, false},
		{"bad id long", func(d *Decision) { d.ID = strings.Repeat("a", 65) }, Limits{}, false},
		{"bad id empty", func(d *Decision) { d.ID = "" }, Limits{}, false},
		{"bad id slash", func(d *Decision) { d.ID = "d/1" }, Limits{}, false},
		{"bad kind", func(d *Decision) { d.Kind = DecisionKind("vote") }, Limits{}, false},
		{"empty kind", func(d *Decision) { d.Kind = "" }, Limits{}, false},
		{"bad task id", func(d *Decision) { d.TaskID = "task 1" }, Limits{}, false},
		{"blank text", func(d *Decision) { d.Text = "   " }, Limits{}, false},
		{"empty text", func(d *Decision) { d.Text = "" }, Limits{}, false},
		{"invalid utf-8 text", func(d *Decision) { d.Text = string([]byte{0xff, 0xfe}) }, Limits{}, false},
		{"text too long custom limit", func(d *Decision) { d.Text = "123456789" }, Limits{MaxDecisionBytes: 8}, false},
		{"text too long default limit", func(d *Decision) { d.Text = strings.Repeat("x", DefaultLimits.MaxDecisionBytes+1) }, Limits{}, false},
		{"empty actor", func(d *Decision) { d.Actor = "" }, Limits{}, false},
		{"actor too long", func(d *Decision) { d.Actor = strings.Repeat("a", 129) }, Limits{}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d := base
			if c.mutate != nil {
				c.mutate(&d)
			}
			r := ValidateDecision(d, c.lim)
			if c.wantNil {
				if r != nil {
					t.Fatalf("want ok, got %v", r)
				}
				return
			}
			if r == nil {
				t.Fatal("want invalid_decision, got ok")
			}
			if r.Code != RefuseInvalidDecision {
				t.Fatalf("want code %s, got %s", RefuseInvalidDecision, r.Code)
			}
		})
	}
}
