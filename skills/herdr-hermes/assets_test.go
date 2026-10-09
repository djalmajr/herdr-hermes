package skill

import (
	"regexp"
	"strings"
	"testing"
)

// denyRegexes is the denylist scanned over every embedded skill file:
// nothing that could identify a machine, a person, a private network or a
// credential may ship inside the binary.
var denyRegexes = []struct {
	name string
	re   *regexp.Regexp
}{
	{"ipv4", regexp.MustCompile(`\b(?:\d{1,3}\.){3}\d{1,3}\b`)},
	{"localhost", regexp.MustCompile(`(?i)\blocalhost\b`)},
	{"machine path /Users/", regexp.MustCompile(`/Users/`)},
	{"machine path /home/", regexp.MustCompile(`/home/`)},
	{`machine path C:\`, regexp.MustCompile(`C:\\`)},
	{"email", regexp.MustCompile(`\b[\w.+-]+@[\w-]+(?:\.[\w-]+)+\b`)},
	{"key sk-", regexp.MustCompile(`\bsk-`)},
	{"key ghp_", regexp.MustCompile(`\bghp_`)},
	{"key xox", regexp.MustCompile(`\bxox`)},
	{"bearer with a real key", regexp.MustCompile(`(?i)bearer\s+[^<\s]`)},
	{"private service", regexp.MustCompile(`(?i)\b(?:tailscale|vpn|corp|intranet)\b`)},
}

var urlRe = regexp.MustCompile(`https?://[^\s"'<>)\]]+`)

// allowedURLHosts are the only URL hosts the embedded docs may use.
var allowedURLHosts = map[string]bool{
	"github.com": true,
	"herdr.dev":  true,
}

// foreignURL returns the first URL in text whose host is not allowed, or
// "". A `Bearer <key>`-style placeholder is the only accepted form after
// the word "Bearer" (checked by the denyRegexes entry above).
func foreignURL(text string) string {
	for _, m := range urlRe.FindAllString(text, -1) {
		rest := m
		switch {
		case strings.HasPrefix(rest, "https://"):
			rest = rest[len("https://"):]
		case strings.HasPrefix(rest, "http://"):
			rest = rest[len("http://"):]
		}
		host := rest
		if i := strings.IndexAny(host, "/?:@"); i >= 0 {
			host = host[:i]
		}
		if !allowedURLHosts[strings.ToLower(strings.TrimRight(host, "."))] {
			return m
		}
	}
	return ""
}

// TestAssets: the embedded skill contains exactly the expected files and no
// embedded file matches any denylist pattern (host, IP, absolute machine
// path, private service name or credential).
func TestAssets(t *testing.T) {
	files, err := Files()
	if err != nil {
		t.Fatalf("Files: %v", err)
	}
	for _, want := range []string{"SKILL.md", "references/protocol.md", "references/setup.md"} {
		if _, ok := files[want]; !ok {
			var names []string
			for k := range files {
				names = append(names, k)
			}
			t.Fatalf("missing embedded file %s (have %v)", want, names)
		}
	}
	for name, data := range files {
		text := string(data)
		for _, d := range denyRegexes {
			if m := d.re.FindString(text); m != "" {
				t.Errorf("%s: denylist %q matched %q", name, d.name, m)
			}
		}
		if u := foreignURL(text); u != "" {
			t.Errorf("%s: URL with a host that is not github.com or herdr.dev: %q", name, u)
		}
	}
}

// TestAssetsDenylistFires proves each denylist pattern fires on a crafted
// sample, so a green TestAssets is not a silent no-op.
func TestAssetsDenylistFires(t *testing.T) {
	for _, tc := range []struct{ name, sample string }{
		{"ipv4", "node 10.20.30.40 reported an error"},
		{"localhost", "bind localhost:8080"},
		{"machine path /Users/", "file at /Users/example/project"},
		{"machine path /home/", "file at /home/example/project"},
		{`machine path C:\`, `file at C:\temp\out`},
		{"email", "mail someone@example.com"},
		{"key sk-", "token sk-abc123"},
		{"key ghp_", "token ghp_abc123"},
		{"key xox", "token xoxb-abc"},
		{"bearer with a real key", "Authorization: Bearer abc123"},
		{"private service", "connect over the vpn"},
	} {
		found := false
		for _, d := range denyRegexes {
			if d.name == tc.name && d.re.MatchString(tc.sample) {
				found = true
			}
		}
		if !found {
			t.Errorf("denylist %q did not fire on sample %q", tc.name, tc.sample)
		}
	}
	if u := foreignURL("see https://example.com/docs"); u == "" {
		t.Error("foreign URL check did not fire on https://example.com/docs")
	}
	if u := foreignURL("https://github.com/djalmajr/herdr-hermes and https://herdr.dev/docs/plugins/"); u != "" {
		t.Errorf("allowed hosts must not fire: %q", u)
	}
}
