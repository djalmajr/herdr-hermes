package cli

import (
	"bytes"
	"io"
	"net/http"
	"strings"
	"testing"
)

// refusalTransport is an injected http.RoundTrip standing in for the
// registry: every request gets the one fixed 409 body.
type refusalTransport struct {
	body    string
	wantURL string
}

func (rt refusalTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.URL.String() != rt.wantURL {
		return nil, &httpError{msg: "unexpected endpoint " + r.URL.String()}
	}
	return &http.Response{
		StatusCode: http.StatusConflict,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(rt.body)),
		Request:    r,
	}, nil
}

// httpError is one RoundTrip failure, never printed verbatim.
type httpError struct{ msg string }

func (e *httpError) Error() string { return e.msg }

// TestDJA229CheckRefusalOutput: a 409 on `jobs check` is a valid refusal
// only for the allowlisted compact check answer, printed byte-identical
// as one stdout line; every other 409 body is the fixed malformed line,
// with the body and its control bytes never echoed.
func TestDJA229CheckRefusalOutput(t *testing.T) {
	args := []string{"jobs", "check", "--agente", "build", "--card", "ABC-1"}
	wantURL := "http://127.0.0.1:7429/v1/check"
	malformedStdout := `{"status":"unavailable","motivo":"registry answered malformed"}` + "\n"
	malformedStderr := "herdr-hermes: jobs: response body is not one JSON value\n"
	run := func(body string) (int, string, string) {
		t.Helper()
		old := http.DefaultTransport
		t.Cleanup(func() { http.DefaultTransport = old })
		http.DefaultTransport = refusalTransport{body: body, wantURL: wantURL}
		var out, errb bytes.Buffer
		code := Run(args, Env{Stdout: &out, Stderr: &errb})
		return code, out.String(), errb.String()
	}
	t.Run("duplicate refusal", func(t *testing.T) {
		body := `{"status":"duplicate","conflitos":[{"tipo":"job","campo":"card","valor":"ABC-1"}]}`
		code, out, err := run(body)
		t.Logf("exit=%d stdout=%q stderr=%q", code, out, err)
		if code != 44 {
			t.Fatalf("exit = %d, want 44", code)
		}
		if out != body+"\n" {
			t.Errorf("stdout = %q, want the body byte-identical plus one newline", out)
		}
		if err != "herdr-hermes: jobs check: refused (duplicate)\n" {
			t.Errorf("stderr = %q, want the fixed duplicate refusal line", err)
		}
	})
	t.Run("hold refusal", func(t *testing.T) {
		body := `{"status":"hold","conflitos":[{"tipo":"hold","campo":"agente","valor":"build"}]}`
		code, out, err := run(body)
		t.Logf("exit=%d stdout=%q stderr=%q", code, out, err)
		if code != 44 {
			t.Fatalf("exit = %d, want 44", code)
		}
		if out != body+"\n" {
			t.Errorf("stdout = %q, want the body byte-identical plus one newline", out)
		}
		if err != "herdr-hermes: jobs check: refused (hold)\n" {
			t.Errorf("stderr = %q, want the fixed hold refusal line", err)
		}
	})
	t.Run("raw DEL and C1 inside a valid refusal are escaped", func(t *testing.T) {
		body := "{\"status\":\"duplicate\",\"conflitos\":[{\"tipo\":\"job\",\"campo\":\"card\",\"valor\":\"A\x7fB\u009b[31mC\"}]}"
		code, out, err := run(body)
		t.Logf("exit=%d stdout=%q stderr=%q", code, out, err)
		if code != 44 {
			t.Fatalf("exit = %d, want 44", code)
		}
		want := `{"status":"duplicate","conflitos":[{"tipo":"job","campo":"card","valor":"A\u007fB\u009b[31mC"}]}` + "\n"
		if out != want {
			t.Errorf("stdout = %q, want %q", out, want)
		}
		if strings.ContainsRune(out, '\x7f') || strings.ContainsRune(out, '\u009b') {
			t.Error("a raw control character reached stdout")
		}
		if err != "herdr-hermes: jobs check: refused (duplicate)\n" {
			t.Errorf("stderr = %q, want the fixed duplicate refusal line", err)
		}
	})
	malformed := []struct{ name, body string }{
		{"invalid UTF-8", "{\"status\":\"duplicate\",\"conflitos\":[{\"valor\":\"A\xffB\"}]}"},
		{"multiline", "{\n\"status\":\"duplicate\"\n}\nadditional-line\n"},
		{"trailing data", `{"status":"duplicate"} extra`},
		{"control in status", `{"status":"duplicate` + "\x1b" + `[31m"}`},
		{"unknown status", `{"status":"weird"}`},
		{"non-object", `[1,2]`},
		{"not json", "plain text"},
	}
	for _, tc := range malformed {
		t.Run(tc.name, func(t *testing.T) {
			code, out, err := run(tc.body)
			t.Logf("exit=%d stdout=%q stderr=%q", code, out, err)
			if code != 4 {
				t.Fatalf("exit = %d, want 4", code)
			}
			if out != malformedStdout {
				t.Errorf("stdout = %q, want the fixed malformed line", out)
			}
			if err != malformedStderr {
				t.Errorf("stderr = %q, want the fixed malformed stderr line", err)
			}
			if strings.ContainsRune(out, '\x1b') || strings.ContainsRune(err, '\x1b') {
				t.Error("control bytes reached the output")
			}
			if strings.Contains(out, "ABC-1") || strings.Contains(err, "ABC-1") {
				t.Error("the untrusted body text was echoed")
			}
		})
	}
	t.Run("retry same malformed answer", func(t *testing.T) {
		body := "{\n\"status\":\"duplicate\"\n}\nadditional-line\n"
		code1, out1, err1 := run(body)
		code2, out2, err2 := run(body)
		if code1 != 4 || out1 != malformedStdout || err1 != malformedStderr {
			t.Fatalf("first run = (%d, %q, %q), want the fixed malformed answer", code1, out1, err1)
		}
		if code2 != code1 || out2 != out1 || err2 != err1 {
			t.Errorf("second run = (%d, %q, %q), want identical to the first (%d, %q, %q)", code2, out2, err2, code1, out1, err1)
		}
	})
}
