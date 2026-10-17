package cli

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
	"unicode"

	"github.com/djalmajr/nau/internal/config"
	"github.com/djalmajr/nau/internal/jobapi"
	"github.com/djalmajr/nau/internal/outbox"
	"github.com/djalmajr/nau/internal/soho"
)

// doctorOutbox is the outbox block of the doctor line.
type doctorOutbox struct {
	UltimoSeq   int64 `json:"ultimo_seq"`
	EntregueSeq int64 `json:"entregue_seq"`
	Pendentes   int64 `json:"pendentes"`
}

// doctorSoho is the herdr_soho block of the doctor line; Capabilities is
// the raw capabilities line or null.
type doctorSoho struct {
	Found        bool    `json:"found"`
	Capabilities *string `json:"capabilities"`
	EphemeralJob bool    `json:"ephemeral_job"`
	JobEvents    bool    `json:"job_events"`
}

// cmdDoctor implements `doctor`: a read-only JSON line with the bridge
// prerequisites. It works under HERDR_HERMES_NOWRITE=1 and creates no
// files. It exits 43 when the backend is missing or lacks either
// capability, else 0. When the selected backend binary still carries
// the legacy herdr-soho name while the renamed soho binary is on the
// machine's PATH, it adds one documented warning to stderr; the JSON
// line and the exit code are unchanged by the warning.
func cmdDoctor(args []string, env Env) int {
	if len(args) != 0 {
		badUsage(env, "usage: doctor")
		return 2
	}
	cfg, err := config.Load(env.ConfigDir)
	if err != nil {
		fail(env, 2, "config: "+err.Error())
		return 2
	}
	runner := soho.Runner{Bin: cfg.HerdrSohoBin, Environ: childEnviron(env)}
	ctx := context.Background()
	so := doctorSoho{}
	if _, lerr := exec.LookPath(cfg.HerdrSohoBin); lerr == nil {
		so.Found = true
	}
	caps, cerr := runner.Capabilities(ctx)
	if cerr == nil {
		so.Found = true
		so.EphemeralJob = caps.Has(jobapi.CapEphemeralJob)
		so.JobEvents = caps.Has(jobapi.CapJobEvents)
		raw := caps.Raw
		so.Capabilities = &raw
	}
	line := struct {
		HerdrSoho     doctorSoho   `json:"herdr_soho"`
		MachineLabel  bool         `json:"machine_label"`
		DispatcherURL bool         `json:"dispatcher_url"`
		KeyConfigured bool         `json:"key_configured"`
		WakeHook      string       `json:"wake_hook"`
		Outbox        doctorOutbox `json:"outbox"`
	}{
		HerdrSoho:     so,
		MachineLabel:  cfg.MachineLabel != "",
		DispatcherURL: cfg.DispatcherURL != "",
		KeyConfigured: keyConfigured(env),
		WakeHook:      wakeHookConfigured(ctx, runner, env),
		Outbox:        doctorOutboxCounts(env),
	}
	_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(line))
	if _, lerr := exec.LookPath("soho"); legacyBackendSelected(cfg.HerdrSohoBin) && lerr == nil {
		_, _ = fmt.Fprintf(env.Stderr, "%s: doctor: herdr_soho_bin points to the legacy herdr-soho binary while soho is on the PATH; '%s config set herdr_soho_bin soho' switches to soho\n",
			env.program(), env.program())
	}
	if !so.EphemeralJob || !so.JobEvents {
		_, _ = fmt.Fprintf(env.Stderr, "%s: soho capabilities missing\n", env.program())
		return 43
	}
	return 0
}

// legacyBackendSelected reports whether the effective backend binary
// carries the legacy herdr-soho name: the base name herdr-soho without
// the windows extension, case-insensitively on Windows and exactly on
// the unix operating systems. For a stored value that is not auto the
// effective value is the stored one (the resolution passes explicit
// selections through), and an auto stored value never resolves to a
// herdr-soho base while soho is on the PATH, so deciding on the
// effective value is the same as deciding on the stored one.
func legacyBackendSelected(bin string) bool {
	return isLegacyBackendName(bin, runtime.GOOS == "windows")
}

// isLegacyBackendName is legacyBackendSelected for a given platform: on
// Windows the .exe extension and the name are both compared without
// case (HERDR-SOHO.EXE is the legacy binary there); on the unix
// operating systems both are exact.
func isLegacyBackendName(bin string, windows bool) bool {
	base := filepath.Base(bin)
	if windows {
		if n := len(base) - len(".exe"); n > 0 && strings.EqualFold(base[n:], ".exe") {
			base = base[:n]
		}
		return strings.EqualFold(base, "herdr-soho")
	}
	return strings.TrimSuffix(base, ".exe") == "herdr-soho"
}

// backendNowriteEnviron is the child environment of the doctor's
// `config` call: the parent environment minus the inherited values of
// the two backend read-only flags (SOHO_NOWRITE and
// HERDR_SOHO_NOWRITE, whatever their values), plus exactly one
// SOHO_NOWRITE=1 and one HERDR_SOHO_NOWRITE=1: the renamed backend
// reads the new flag and the legacy binary still running on the nodes
// reads the old one, so the read-only guarantee holds on both. The key
// comparison is exact on the unix operating systems and
// case-insensitive on Windows, the same pattern as keyIsPaneID in
// internal/soho.
func backendNowriteEnviron(env Env) []string {
	environ := childEnviron(env)
	kept := make([]string, 0, len(environ)+2)
	for _, kv := range environ {
		if keyIsNowrite(kv) {
			continue
		}
		kept = append(kept, kv)
	}
	return append(kept, "SOHO_NOWRITE=1", "HERDR_SOHO_NOWRITE=1")
}

// keyIsNowrite reports whether the environment entry kv has the key
// SOHO_NOWRITE or HERDR_SOHO_NOWRITE: exact on the unix operating
// systems, case-insensitive on Windows.
func keyIsNowrite(kv string) bool {
	key := kv
	if i := strings.IndexByte(kv, '='); i >= 0 {
		key = kv[:i]
	}
	if runtime.GOOS == "windows" {
		return strings.EqualFold(key, "SOHO_NOWRITE") || strings.EqualFold(key, "HERDR_SOHO_NOWRITE")
	}
	return key == "SOHO_NOWRITE" || key == "HERDR_SOHO_NOWRITE"
}

// doctorOutboxCounts reads the outbox counts without creating any file.
func doctorOutboxCounts(env Env) doctorOutbox {
	store, err := outbox.Open(outbox.StateDir(env.ConfigDir), outbox.Options{
		Now:      env.Now,
		ReadOnly: true,
	})
	if err != nil {
		return doctorOutbox{}
	}
	_, last, err := store.Read(0)
	if err != nil {
		return doctorOutbox{}
	}
	delivered, err := store.DeliveredSeq()
	if err != nil {
		delivered = 0
	}
	pending := last - delivered
	if pending < 0 {
		pending = 0
	}
	return doctorOutbox{UltimoSeq: last, EntregueSeq: delivered, Pendentes: pending}
}

// wakeHookConfigured classifies the job_wake_cmd value the way the
// backend's job reads it, in this order, each probe in the read-only
// child environment (see backendNowriteEnviron): first
// `soho config get job_wake_cmd`, whose trimmed first stdout line is
// the effective value and an empty line means unset; then, on any
// other exit or a run that did not finish, `soho config`, whose listed
// job_wake_cmd row wins, else whose `user file:` path is read with the
// backend's single-file config rules. It returns "true" or "false" for
// a decided value and "unknown" when neither probe could be read or
// parsed.
func wakeHookConfigured(ctx context.Context, runner soho.Runner, env Env) string {
	wakeRunner := soho.Runner{Bin: runner.Bin, Environ: backendNowriteEnviron(env), Now: runner.Now}
	if value, ok := wakeConfigGet(ctx, &wakeRunner); ok {
		return wakeValueClass(strings.Fields(value))
	}
	var out bytes.Buffer
	exit, err := wakeRunner.Run(ctx, []string{"config"}, nil, &out, io.Discard, 120*time.Second)
	if err != nil {
		return "unknown"
	}
	// Wait for the delivery to this internal buffer to finish before
	// reading it; the buffer never stalls, so the delivery completes.
	select {
	case <-wakeRunner.DrainDone():
	case <-time.After(10 * time.Second):
		return "unknown"
	}
	if exit != 0 {
		return "unknown"
	}
	return parseWakeHook(out.String())
}

// wakeConfigGet runs `soho config get job_wake_cmd` and reports the
// trimmed first line of its stdout. ok is false when the run failed,
// the output did not drain or the child exited non-zero, so the caller
// falls back to the plain `config` listing: the backend release that
// cannot read a single key rejects the call with exit 2.
func wakeConfigGet(ctx context.Context, wakeRunner *soho.Runner) (string, bool) {
	var out bytes.Buffer
	exit, err := wakeRunner.Run(ctx, []string{"config", "get", "job_wake_cmd"}, nil, &out, io.Discard, 120*time.Second)
	if err != nil {
		return "", false
	}
	select {
	case <-wakeRunner.DrainDone():
	case <-time.After(10 * time.Second):
		return "", false
	}
	if exit != 0 {
		return "", false
	}
	value := out.String()
	if i := strings.IndexByte(value, '\n'); i >= 0 {
		value = value[:i]
	}
	return sohoConfigTrim(value), true
}

// parseWakeHook classifies the `soho config` output. A line whose
// first whitespace-separated field is job_wake_cmd decides the result
// with the value that follows the key (fields after the second, extra
// whitespace and the source column allowed), which keeps the behavior
// with a backend that lists the key. When no such row is present but a
// `user file:` line is, the path after the prefix is read with the
// backend's single-file config rules (the applyConfigText mirror) and
// classified. The rest of the output must be parseable as key-value
// lines for the result to be false; otherwise it is unknown.
func parseWakeHook(output string) string {
	parseable := false
	userFile := ""
	for _, line := range strings.Split(output, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		parseable = true
		if fields[0] == "job_wake_cmd" {
			return wakeValueClass(strings.Fields(line[len(fields[0]):]))
		}
		if rest, ok := strings.CutPrefix(sohoConfigTrim(line), "user file:"); ok {
			userFile = sohoConfigTrim(rest)
		}
	}
	if userFile != "" {
		return wakeHookFromUserFile(userFile)
	}
	if parseable {
		return "false"
	}
	return "unknown"
}

// wakeHookFromUserFile reads the backend user config file at path with
// the backend's own single-file rules (sohoUserConfigValue) and
// classifies the job_wake_cmd value: an absent file or a file without
// the key gives false, a read error that is not a missing file gives
// unknown.
func wakeHookFromUserFile(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return "false"
		}
		return "unknown"
	}
	return wakeValueClass(strings.Fields(sohoUserConfigValue(data, "job_wake_cmd")))
}

// wakeValueClass classifies the whitespace-separated fields of a
// job_wake_cmd value on the operating system the doctor runs on.
func wakeValueClass(fields []string) string {
	return wakeValueClassFor(fields, runtime.GOOS == "windows")
}

// wakeValueClassFor is wakeValueClass for a given platform: on Windows
// the .exe form of the program name in any casing counts and the
// backslash is a separator.
func wakeValueClassFor(fields []string, windows bool) string {
	if wakeValueAccepted(fields, windows) {
		return "true"
	}
	return "false"
}

// wakeValueAccepted reports whether the whitespace-separated fields of
// a job_wake_cmd value name the nau wake hook: the first field is
// accepted by wakeProgramAccepted and the second field is exactly
// wake. Fields after the second are allowed; the retired herdr-hermes
// name, nau-wake, nauwake and wakeup, and any other program are
// rejected.
func wakeValueAccepted(fields []string, windows bool) bool {
	return len(fields) >= 2 && fields[1] == "wake" && wakeProgramAccepted(fields[0], windows)
}

// wakeProgramAccepted reports whether the first field of a
// job_wake_cmd value names the nau binary: exactly nau, or a path
// whose last element is nau (the separators of the platform named by
// windows, where the backslash counts too); on Windows the last
// element also counts in the .exe form in any casing. A first field
// that ends in a separator (a forward slash on the unix operating
// systems, a forward or back slash on Windows) names a directory, not
// an executable, and is rejected before the last element is taken; on
// Windows a leading letter volume prefix (C:) is dropped before the
// last element is taken, the standard library's Windows volume-name
// handling for the letter form (UNC and the long-path prefix are out
// of scope). The file's existence is not checked: the doctor only
// reports.
func wakeProgramAccepted(first string, windows bool) bool {
	if first == ProgramNau {
		return true
	}
	if strings.HasSuffix(first, "/") || (windows && strings.HasSuffix(first, "\\")) {
		return false
	}
	name := first
	if windows {
		if len(name) >= 2 && name[1] == ':' && ((name[0] >= 'a' && name[0] <= 'z') || (name[0] >= 'A' && name[0] <= 'Z')) {
			name = name[2:]
		}
		if i := strings.LastIndexAny(name, "/\\"); i >= 0 {
			name = name[i+1:]
		}
	} else {
		name = filepath.Base(first)
	}
	if name == ProgramNau {
		return true
	}
	return windows && strings.EqualFold(name, ProgramNau+".exe")
}

// sohoUserConfigValue extracts the last value of key from the raw bytes
// of a backend user config file, mirroring the rules of the backend's
// own single-file parser exactly as its job reads the file: the bytes
// are decoded like the backend decodes them (WHATWG UTF-8 with
// U+FFFD for malformed sequences, then CRLF line endings as LF), a line
// is cut at its first # wherever that is, the remainder is trimmed with
// the backend's whitespace set, a blank line or a line without = is
// ignored, the key before the first = is trimmed and normalized (the
// JavaScript whitespace removed, . and - become _, only letters, digits
// and _ remain, and the case is not changed), an empty key is ignored,
// the value after the = is trimmed and a value of two or more
// characters that starts and ends with a double quote loses one pair
// of quotes, and the last occurrence of the normalized key wins.
func sohoUserConfigValue(data []byte, key string) string {
	value := ""
	for _, rawLine := range strings.Split(sohoDecodeText(data), "\n") {
		if hash := strings.IndexByte(rawLine, '#'); hash >= 0 {
			rawLine = rawLine[:hash]
		}
		line := sohoConfigTrim(rawLine)
		if line == "" {
			continue
		}
		eq := strings.IndexByte(line, '=')
		if eq < 0 {
			continue
		}
		normKey := sohoNormalizeKey(sohoConfigTrim(line[:eq]))
		v := sohoConfigTrim(line[eq+1:])
		if len(v) >= 2 && strings.HasPrefix(v, "\"") && strings.HasSuffix(v, "\"") {
			v = v[1 : len(v)-1]
		}
		if normKey == key {
			value = v
		}
	}
	return value
}

// sohoNormalizeKey normalizes a raw config key the way the backend
// normalizes it: the JavaScript whitespace is removed, . and - become
// _, only the letters, digits and _ survive, and the case is kept as
// written.
func sohoNormalizeKey(raw string) string {
	var b strings.Builder
	for _, r := range raw {
		if isSohoConfigWhitespace(r) {
			continue
		}
		if r == '.' || r == '-' {
			b.WriteByte('_')
			continue
		}
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// sohoDecodeText decodes the raw bytes of a backend text file the way
// the backend decodes them: WHATWG UTF-8 with the replacement character
// for a malformed sequence, then CRLF line endings as LF. A leading byte
// order mark survives the decode as a character: the backend's
// whitespace trim removes it from a line.
func sohoDecodeText(data []byte) string {
	return strings.ReplaceAll(sohoDecodeWHATWGUTF8(data), "\r\n", "\n")
}

// sohoDecodeWHATWGUTF8 is the backend's UTF-8 decode: ASCII bytes pass
// through, a multi-byte lead byte (the two-, three- and four-byte forms
// without overlong, surrogate or out-of-range encodings) passes with
// its continuation bytes, and any malformed byte or sequence yields one
// U+FFFD and advances past the bytes it consumed.
func sohoDecodeWHATWGUTF8(data []byte) string {
	var out strings.Builder
	for i := 0; i < len(data); {
		b := data[i]
		if b < 0x80 {
			out.WriteByte(b)
			i++
			continue
		}
		n := 0
		if b >= 0xC2 && b <= 0xDF {
			n = 2
		} else if b >= 0xE0 && b <= 0xEF {
			n = 3
		} else if b >= 0xF0 && b <= 0xF4 {
			n = 4
		}
		if n == 0 {
			out.WriteRune('\uFFFD')
			i++
			continue
		}
		consumed := 1
		valid := true
		for j := 1; j < n; j++ {
			if i+j >= len(data) {
				valid = false
				break
			}
			c := data[i+j]
			if c < 0x80 || c > 0xBF || (j == 1 && ((b == 0xE0 && c < 0xA0) || (b == 0xED && c > 0x9F) || (b == 0xF0 && c < 0x90) || (b == 0xF4 && c > 0x8F))) {
				valid = false
				break
			}
			consumed++
		}
		if !valid {
			out.WriteRune('\uFFFD')
			i += consumed
			continue
		}
		out.Write(data[i : i+n])
		i += n
	}
	return out.String()
}

// sohoConfigTrim trims the whitespace set the backend's config parser
// uses: the JavaScript whitespace class (tab, line terminators, the
// unicode space separators, the line and paragraph separators and the
// byte order mark).
func sohoConfigTrim(value string) string {
	return strings.TrimFunc(value, isSohoConfigWhitespace)
}

// isSohoConfigWhitespace reports whether r is in the backend's config
// whitespace set.
func isSohoConfigWhitespace(r rune) bool {
	return r == '\t' || r == '\n' || r == '\v' || r == '\f' || r == '\r' || r == ' ' ||
		unicode.Is(unicode.Zs, r) || r == '\u2028' || r == '\u2029' || r == '\ufeff'
}
