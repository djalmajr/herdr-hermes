package cli

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/djalmajr/nau/internal/registry"
)

// controlsCanary stands in for every environment name and entry in the
// JobsOutputControls rows; it must never reach the output.
const controlsCanary = "canary-not-a-key"

// Raw server bytes the rows put inside JSON strings: DEL, U+0080, U+009B
// and U+009F, plus two bytes that do not start a valid UTF-8 sequence,
// and the ordinary multibyte text (U+00E9, U+4E2D, an emoji, U+00A0 and
// U+2028) that must stay byte-identical through the client.
var (
	delRaw   = []byte("\x7f")
	c1Lo     = []byte("\u0080")
	c1Esc    = []byte("\u009b")
	c1Hi     = []byte("\u009f")
	invByteA = []byte{0x9b}
	invByteB = []byte{0xff}
	ordBytes = []byte("é中\U0001F600\u00a0\u2028")
)

// controlsEnv is the Env of every JobsOutputControls row: Getenv answers
// the canary for every name and Environ returns one canary entry, so a
// leak into the output is a test failure.
func controlsEnv(out, errb *bytes.Buffer) Env {
	return Env{
		Stdin:   strings.NewReader(""),
		Stdout:  out,
		Stderr:  errb,
		Getenv:  func(string) string { return controlsCanary },
		Environ: func() []string { return []string{"CONTROLS=" + controlsCanary} },
	}
}

// runAgainst serves one fixed answer on loopback and runs the jobs form
// with the canary Env; it returns the exit code, the raw stdout and
// stderr bytes, the request the server saw and the request counter.
func runAgainst(t *testing.T, code int, resp []byte, redirect bool, args []string) (int, []byte, []byte, wireCapture, *atomic.Int32) {
	t.Helper()
	srv, ch, hits := captureServer(t, code, resp, redirect)
	var out, errb bytes.Buffer
	exit := Run(append([]string{"jobs", "--server", srv.URL}, args...), controlsEnv(&out, &errb))
	return exit, out.Bytes(), errb.Bytes(), requireCapture(t, ch), hits
}

// assertOneLineControlFree checks the row invariants on the raw output:
// exactly one \n on stdout, stdout ending with \n, no raw DEL or C1 rune
// and no lone 0x80..0x9f byte on stdout (bytes of a valid multibyte
// sequence, like an emoji, are ordinary output), and the canary nowhere.
func assertOneLineControlFree(t *testing.T, out, errb []byte) {
	t.Helper()
	s := string(out)
	if n := strings.Count(s, "\n"); n != 1 {
		t.Errorf("stdout %q carries %d newlines, want exactly one", out, n)
	}
	if !bytes.HasSuffix(out, []byte{'\n'}) {
		t.Errorf("stdout %q does not end with a newline", out)
	}
	for rest := out; len(rest) > 0; {
		r, size := utf8.DecodeRune(rest)
		switch {
		case r == utf8.RuneError && size == 1:
			if b := rest[0]; b == 0x7f || (b >= 0x80 && b <= 0x9f) {
				t.Errorf("stdout: lone raw control byte %x", b)
			}
		case r >= 0x7f && r <= 0x9f:
			t.Errorf("stdout: raw control rune U+%04X", r)
		}
		rest = rest[size:]
	}
	if strings.Contains(s, controlsCanary) || strings.Contains(string(errb), controlsCanary) {
		t.Errorf("the environment canary leaked: stdout %q stderr %q", out, errb)
	}
}

// assertDecodedEqual unmarshals the stdout line and the server body into
// any and requires the two values equal.
func assertDecodedEqual(t *testing.T, out, body []byte) {
	t.Helper()
	var a, b any
	if err := json.Unmarshal(out, &a); err != nil {
		t.Fatalf("stdout is not one JSON value: %v", err)
	}
	if err := json.Unmarshal(body, &b); err != nil {
		t.Fatalf("the server body is not one JSON value: %v", err)
	}
	if !reflect.DeepEqual(a, b) {
		t.Errorf("decoded stdout = %v, want the decoded server body %v", a, b)
	}
}

// TestJobsOutputControls: every jobs stdout line escapes raw DEL and C1
// while ordinary bytes stay identical, the fixed lines stay unchanged,
// the canary environment never leaks into the output, and no request
// carries an Authorization header.
func TestJobsOutputControls(t *testing.T) {
	// The 200 rows: the fake answer is one JSON value with raw control
	// bytes inside nested strings; the want line is its escaped form, the
	// ordinary characters byte-identical.
	http200 := []struct {
		name string
		body string
		want string
	}{
		{
			name: "controls and ordinary characters in nested strings",
			body: `{"jobs":[{"id":"j-1","nome":"n` + string(ordBytes) + `","caminho":"/retained` + string(delRaw) + `","pedido":"p` + string(c1Lo) + string(c1Esc) + string(c1Hi) + `"}]}`,
			want: `{"jobs":[{"id":"j-1","nome":"n` + string(ordBytes) + `","caminho":"/retained\u007f","pedido":"p\u0080\u009b\u009f"}]}` + "\n",
		},
		{
			name: "pretty printed body compacts to one line",
			body: "{\n\t\"jobs\": [\n\t\t{\"caminho\": \"/x" + string(delRaw) + "\",\n\t\t\"pedido\": \"y" + string(c1Esc) + "\"\n\t}\n\t]\n}\n",
			want: `{"jobs":[{"caminho":"/x\u007f","pedido":"y\u009b"}]}` + "\n",
		},
		{
			name: "invalid UTF-8 bytes become U+FFFD",
			body: "{\"a\":\"A" + string(invByteA) + "B" + string(invByteB) + "C\"}",
			want: `{"a":"A` + "\uFFFD" + `B` + "\uFFFD" + `C"}` + "\n",
		},
		{
			name: "plain ASCII body stays byte-identical",
			body: `{"jobs":[{"id":"j-1","nome":"plain","caminho":"/x","pedido":"keep"}]}`,
			want: `{"jobs":[{"id":"j-1","nome":"plain","caminho":"/x","pedido":"keep"}]}` + "\n",
		},
	}
	for _, tc := range http200 {
		t.Run(tc.name, func(t *testing.T) {
			code, out, errb, c, _ := runAgainst(t, http.StatusOK, []byte(tc.body), false, nil)
			if code != 0 {
				t.Fatalf("exit = %d, want 0 (stdout %q stderr %q)", code, out, errb)
			}
			if string(out) != tc.want {
				t.Errorf("stdout = %q, want %q", out, tc.want)
			}
			assertOneLineControlFree(t, out, errb)
			if string(errb) != "" {
				t.Errorf("stderr = %q, want empty", errb)
			}
			if c.auth != "" {
				t.Errorf("the request carried Authorization %q, want none", c.auth)
			}
			assertDecodedEqual(t, out, []byte(tc.body))
		})
	}

	t.Run("400 motivo carries the controls escaped", func(t *testing.T) {
		body := `{"motivo":"bad` + string(delRaw) + `value\u009b\u0080\u009f"}`
		code, out, errb, c, _ := runAgainst(t, http.StatusBadRequest, []byte(body), false, nil)
		want := `{"status":"2","motivo":"registry: bad\u007fvalue\u009b\u0080\u009f"}` + "\n"
		if code != 2 {
			t.Fatalf("exit = %d, want 2 (stdout %q stderr %q)", code, out, errb)
		}
		if string(out) != want {
			t.Errorf("stdout = %q, want %q", out, want)
		}
		assertOneLineControlFree(t, out, errb)
		if string(errb) != "" {
			t.Errorf("stderr = %q, want empty", errb)
		}
		if c.auth != "" {
			t.Errorf("the request carried Authorization %q, want none", c.auth)
		}
	})

	t.Run("400 without motivo keeps the fixed line", func(t *testing.T) {
		body := `{"status":"invalid"}`
		code, out, errb, c, _ := runAgainst(t, http.StatusBadRequest, []byte(body), false, nil)
		want := `{"status":"2","motivo":"registry: registry error"}` + "\n"
		if code != 2 {
			t.Fatalf("exit = %d, want 2 (stdout %q stderr %q)", code, out, errb)
		}
		if string(out) != want {
			t.Errorf("stdout = %q, want %q", out, want)
		}
		assertOneLineControlFree(t, out, errb)
		if string(errb) != "" {
			t.Errorf("stderr = %q, want empty", errb)
		}
		if c.auth != "" {
			t.Errorf("the request carried Authorization %q, want none", c.auth)
		}
	})

	t.Run("409 check refusal escapes nested controls", func(t *testing.T) {
		body := `{"status":"duplicate","conflitos":[{"tipo":"job","campo":"card","valor":"A` + string(delRaw) + `B` + string(c1Esc) + `C"}]}`
		code, out, errb, c, _ := runAgainst(t, http.StatusConflict, []byte(body), false, []string{"check", "--agente", "build", "--card", "ABC-1"})
		want := `{"status":"duplicate","conflitos":[{"tipo":"job","campo":"card","valor":"A\u007fB\u009bC"}]}` + "\n"
		if code != 44 {
			t.Fatalf("exit = %d, want 44 (stdout %q stderr %q)", code, out, errb)
		}
		if string(out) != want {
			t.Errorf("stdout = %q, want %q", out, want)
		}
		assertOneLineControlFree(t, out, errb)
		if string(errb) != "nau: jobs check: refused (duplicate)\n" {
			t.Errorf("stderr = %q, want the fixed duplicate refusal line", errb)
		}
		if c.auth != "" {
			t.Errorf("the request carried Authorization %q, want none", c.auth)
		}
		assertDecodedEqual(t, out, []byte(body))
	})

	t.Run("409 on another subcommand keeps the fixed line", func(t *testing.T) {
		body := `{"status":"duplicate","conflitos":[]}`
		code, out, errb, c, _ := runAgainst(t, http.StatusConflict, []byte(body), false, []string{"release", "--claim", "c-1"})
		wantOut := `{"status":"unavailable","motivo":"registry answered 409"}` + "\n"
		wantErr := "nau: jobs: registry answered 409\n"
		if code != 4 {
			t.Fatalf("exit = %d, want 4 (stdout %q stderr %q)", code, out, errb)
		}
		if string(out) != wantOut {
			t.Errorf("stdout = %q, want %q", out, wantOut)
		}
		assertOneLineControlFree(t, out, errb)
		if string(errb) != wantErr {
			t.Errorf("stderr = %q, want %q", errb, wantErr)
		}
		if c.auth != "" {
			t.Errorf("the request carried Authorization %q, want none", c.auth)
		}
	})

	t.Run("negative answers keep their fixed lines", func(t *testing.T) {
		malformedOut := `{"status":"unavailable","motivo":"registry answered malformed"}` + "\n"
		t.Run("two JSON values", func(t *testing.T) {
			code, out, errb, c, _ := runAgainst(t, http.StatusOK, []byte(`{"a":1}{"b":2}`), false, nil)
			if code != 4 {
				t.Fatalf("exit = %d, want 4 (stdout %q stderr %q)", code, out, errb)
			}
			if string(out) != malformedOut {
				t.Errorf("stdout = %q, want %q", out, malformedOut)
			}
			assertOneLineControlFree(t, out, errb)
			if string(errb) != "nau: jobs: response body is not one JSON value\n" {
				t.Errorf("stderr = %q, want the fixed malformed line", errb)
			}
			if c.auth != "" {
				t.Errorf("the request carried Authorization %q, want none", c.auth)
			}
		})
		t.Run("trailing data", func(t *testing.T) {
			code, out, errb, c, _ := runAgainst(t, http.StatusOK, []byte(`{"a":1} x`), false, nil)
			if code != 4 {
				t.Fatalf("exit = %d, want 4 (stdout %q stderr %q)", code, out, errb)
			}
			if string(out) != malformedOut {
				t.Errorf("stdout = %q, want %q", out, malformedOut)
			}
			assertOneLineControlFree(t, out, errb)
			if string(errb) != "nau: jobs: response body is not one JSON value\n" {
				t.Errorf("stderr = %q, want the fixed malformed line", errb)
			}
			if c.auth != "" {
				t.Errorf("the request carried Authorization %q, want none", c.auth)
			}
		})
		t.Run("oversized body", func(t *testing.T) {
			old := jobsRespCap
			jobsRespCap = 8
			t.Cleanup(func() { jobsRespCap = old })
			code, out, errb, c, _ := runAgainst(t, http.StatusOK, []byte(`{"a":12345}`), false, nil)
			if code != 4 {
				t.Fatalf("exit = %d, want 4 (stdout %q stderr %q)", code, out, errb)
			}
			if string(out) != malformedOut {
				t.Errorf("stdout = %q, want %q", out, malformedOut)
			}
			assertOneLineControlFree(t, out, errb)
			if string(errb) != "nau: jobs: response body exceeds 8 bytes\n" {
				t.Errorf("stderr = %q, want the fixed oversized line", errb)
			}
			if c.auth != "" {
				t.Errorf("the request carried Authorization %q, want none", c.auth)
			}
		})
		t.Run("redirect not followed", func(t *testing.T) {
			code, out, errb, c, hits := runAgainst(t, http.StatusFound, nil, true, nil)
			if code != 4 {
				t.Fatalf("exit = %d, want 4 (stdout %q stderr %q)", code, out, errb)
			}
			if string(out) != `{"status":"unavailable","motivo":"registry answered 302"}`+"\n" {
				t.Errorf("stdout = %q, want the fixed 302 line", out)
			}
			assertOneLineControlFree(t, out, errb)
			if string(errb) != "nau: jobs: registry answered 302\n" {
				t.Errorf("stderr = %q, want the fixed 302 line", errb)
			}
			if c.auth != "" {
				t.Errorf("the request carried Authorization %q, want none", c.auth)
			}
			if n := hits.Load(); n != 1 {
				t.Errorf("the server saw %d requests, want exactly one: the redirect is not followed", n)
			}
		})
	})

	t.Run("live loopback round trip through a real registry", func(t *testing.T) {
		fixed := func() time.Time { return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC) }
		store, err := registry.Open(filepath.Join(t.TempDir(), "registry.db"), registry.Options{Now: fixed})
		if err != nil {
			t.Fatalf("registry.Open: %v", err)
		}
		t.Cleanup(func() { _ = store.Close() })
		srv := httptest.NewServer(registry.NewHandler(store))
		t.Cleanup(srv.Close)

		caminho := "/retained\x7f"
		pedido := "please\u009b"

		var out, errb bytes.Buffer
		code := Run([]string{"jobs", "--server", srv.URL, "folder", "add", "--maquina", "node-a", "--caminho", caminho, "--pedido", pedido, "--job", "job-1"}, controlsEnv(&out, &errb))
		if code != 0 {
			t.Fatalf("folder add exit = %d (stdout %q stderr %q)", code, out, errb)
		}
		if errb.String() != "" {
			t.Errorf("folder add stderr = %q, want empty", errb)
		}

		var out2, errb2 bytes.Buffer
		code2 := Run([]string{"jobs", "--server", srv.URL}, controlsEnv(&out2, &errb2))
		if code2 != 0 {
			t.Fatalf("jobs exit = %d (stdout %q stderr %q)", code2, out2, errb2)
		}
		assertOneLineControlFree(t, out2.Bytes(), errb2.Bytes())
		if errb2.String() != "" {
			t.Errorf("jobs stderr = %q, want empty", errb2)
		}
		var inv struct {
			Pastas []struct {
				Caminho string `json:"caminho"`
				Pedido  string `json:"pedido"`
			} `json:"pastas"`
		}
		if err := json.Unmarshal(out2.Bytes(), &inv); err != nil {
			t.Fatalf("the inventory line is not one JSON value: %v", err)
		}
		if len(inv.Pastas) != 1 {
			t.Fatalf("pastas = %v, want exactly one folder", inv.Pastas)
		}
		if inv.Pastas[0].Caminho != caminho {
			t.Errorf("pastas[0].caminho = %q, want %q", inv.Pastas[0].Caminho, caminho)
		}
		if inv.Pastas[0].Pedido != pedido {
			t.Errorf("pastas[0].pedido = %q, want %q", inv.Pastas[0].Pedido, pedido)
		}
	})
}
