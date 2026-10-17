package cli

import (
	"runtime/debug"
	"testing"
)

// TestDevVersion covers the four cases of the dev version derivation:
// a recorded VCS revision (nothing changes, it wins), a module version
// from a "go install" build without VCS information, a (devel) module
// version, and no build info at all.
func TestDevVersion(t *testing.T) {
	revision := "668ff2252f4c9c8b0a1d2e3f4a5b6c7d8e9f0a1b"
	cases := []struct {
		name string
		info *debug.BuildInfo
		ok   bool
		want string
	}{
		{
			name: "with vcs revision",
			info: &debug.BuildInfo{
				Path: "github.com/djalmajr/nau/cmd/nau",
				Main: debug.Module{
					Path:    "github.com/djalmajr/nau",
					Version: "v0.0.0-20261017151413-668ff2252f4c",
				},
				Settings: []debug.BuildSetting{
					{Key: "vcs.revision", Value: revision},
				},
			},
			ok:   true,
			want: "dev+" + revision,
		},
		{
			name: "without revision and with main version",
			info: &debug.BuildInfo{
				Path: "github.com/djalmajr/nau/cmd/nau",
				Main: debug.Module{
					Path:    "github.com/djalmajr/nau",
					Version: "v0.0.0-20261017151413-668ff2252f4c",
				},
			},
			ok:   true,
			want: "dev+v0.0.0-20261017151413-668ff2252f4c",
		},
		{
			name: "devel main version",
			info: &debug.BuildInfo{
				Path: "github.com/djalmajr/nau/cmd/nau",
				Main: debug.Module{
					Path:    "github.com/djalmajr/nau",
					Version: "(devel)",
				},
			},
			ok:   true,
			want: "dev",
		},
		{
			name: "without build info",
			info: nil,
			ok:   false,
			want: "dev",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := devVersion(tc.info, tc.ok); got != tc.want {
				t.Errorf("devVersion = %q, want %q", got, tc.want)
			}
		})
	}
	// An empty module version behaves like (devel): plain dev.
	empty := &debug.BuildInfo{Path: "github.com/djalmajr/nau/cmd/nau"}
	if got := devVersion(empty, true); got != "dev" {
		t.Errorf("devVersion with empty Main.Version = %q, want %q", got, "dev")
	}
}
