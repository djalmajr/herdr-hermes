package cli

import "testing"

// TestJobStdinSourceHelper: jobBodyFromStdin is true for a stdin body ("-")
// and for an absent body, false for an explicit file. It is only consulted
// for the subs in jobStdinCaps.
func TestJobStdinSourceHelper(t *testing.T) {
	cases := []struct {
		name string
		sub  string
		rest []string
		want bool
	}{
		{"start --brief file", "start", []string{"--id", "J1", "--repo", "o/r", "--brief", "/tmp/b.json"}, false},
		{"start --brief=file", "start", []string{"--id", "J1", "--repo", "o/r", "--brief=/tmp/b.json"}, false},
		{"start --brief -", "start", []string{"--id", "J1", "--repo", "o/r", "--brief", "-"}, true},
		{"start --brief=-", "start", []string{"--id", "J1", "--repo", "o/r", "--brief=-"}, true},
		{"start absent", "start", []string{"--id", "J1", "--repo", "o/r"}, true},
		{"start --brief= is a file", "start", []string{"--id", "J1", "--brief="}, false},
		{"start bare trailing --brief is a file", "start", []string{"--id", "J1", "--brief"}, false},
		{"start first --brief wins", "start", []string{"--brief", "-", "--brief", "f"}, true},
		{"start positional is not a source", "start", []string{"--id", "J1", "x"}, true},
		{"amend file", "amend", []string{"--id", "J1", "/tmp/a.md"}, false},
		{"amend --id=J1 file", "amend", []string{"--id=J1", "/tmp/a.md"}, false},
		{"amend -", "amend", []string{"--id", "J1", "-"}, true},
		{"amend --id=J1 -", "amend", []string{"--id=J1", "-"}, true},
		{"amend absent", "amend", []string{"--id", "J1"}, true},
		{"amend nothing", "amend", nil, true},
		{"amend file before --id", "amend", []string{"/tmp/a.md", "--id", "J1"}, false},
		{"amend --id value is not positional", "amend", []string{"--id", "-"}, true},
		{"amend first positional wins over later -", "amend", []string{"--id", "J1", "x", "-"}, false},
		{"amend first positional - wins over later file", "amend", []string{"--id", "J1", "-", "x"}, true},
		{"amend unknown flag skipped", "amend", []string{"--id", "J1", "--force", "-"}, true},
		{"amend trailing --id", "amend", []string{"--id"}, true},
		{"send file", "send", []string{"--id", "J1", "/tmp/s.md"}, false},
		{"send -", "send", []string{"--id=J1", "-"}, true},
		{"send absent", "send", []string{"--id", "J1"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := jobBodyFromStdin(tc.sub, tc.rest); got != tc.want {
				t.Errorf("jobBodyFromStdin(%q, %q) = %v, want %v", tc.sub, tc.rest, got, tc.want)
			}
		})
	}
	// Subs outside jobStdinCaps never reach the helper.
	for _, sub := range []string{"status", "wait", "events", "collect", "ack", "cancel", "close", "list"} {
		if _, ok := jobStdinCaps[sub]; ok {
			t.Errorf("%s is in jobStdinCaps; the helper is only meant for start, amend and send", sub)
		}
	}
}
