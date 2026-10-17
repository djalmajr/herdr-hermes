package cli

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/djalmajr/nau/internal/outbox"
	"github.com/djalmajr/nau/internal/testutil/fakesoho"
)

// configTable is the `herdr-soho config` output shape: padded
// KEY / VALUE / SOURCE columns.
func configTable(rows ...string) string {
	lines := []string{"KEY VALUE SOURCE"}
	for _, r := range rows {
		lines = append(lines, r)
	}
	return strings.Join(lines, "\n") + "\n"
}

const wakeRow = "job_wake_cmd        nau wake                              file"

type doctorLine struct {
	HerdrSoho struct {
		Found        bool    `json:"found"`
		Capabilities *string `json:"capabilities"`
		EphemeralJob bool    `json:"ephemeral_job"`
		JobEvents    bool    `json:"job_events"`
	} `json:"herdr_soho"`
	MachineLabel  bool         `json:"machine_label"`
	DispatcherURL bool         `json:"dispatcher_url"`
	KeyConfigured bool         `json:"key_configured"`
	WakeHook      string       `json:"wake_hook"`
	Outbox        doctorOutbox `json:"outbox"`
}

// runDoctor runs doctor and returns the parsed line plus the exit code.
func runDoctor(t *testing.T, cfgDir string, nowrite bool) (doctorLine, string, int) {
	t.Helper()
	stdout, _, exit := runHermes(t, cfgDir, nowrite, "", "doctor")
	var line doctorLine
	if err := json.Unmarshal([]byte(stdout), &line); err != nil {
		t.Fatalf("doctor stdout %q is not a JSON line: %v", stdout, err)
	}
	return line, stdout, exit
}

// TestDoctorFound: a found herdr-soho with both capabilities, a set
// machine_label and dispatcher_url, the wake hook and the outbox counts.
func TestDoctorFound(t *testing.T) {
	exe, _ := installFakeSoho(t,
		capsRule(capsJSON, 0),
		fakesoho.Rule{Argv: []string{"config"}, Stdout: configTable("machine_label   machine-a      file", wakeRow), Code: 0},
	)
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, exe, "machine-a")
	if err := os.WriteFile(filepath.Join(cfgDir, "config"),
		[]byte("machine_label=machine-a\ndispatcher_url=https://bridge.invalid\nherdr_soho_bin="+exe+"\npush_timeout_s=15\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	// Three records, one delivered.
	s, err := outbox.Open(outbox.StateDir(cfgDir), outbox.Options{})
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := s.Append(outbox.Record{
			Tipo: outbox.TipoDispatch, Maquina: "machine-a", Projeto: "org/repo",
			Dados: json.RawMessage(`{"id":"J1"}`),
		}); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := s.SetDeliveredSeq(1); err != nil {
		t.Fatalf("SetDeliveredSeq: %v", err)
	}
	line, _, exit := runDoctor(t, cfgDir, false)
	if exit != 0 {
		t.Fatalf("exit = %d, want 0", exit)
	}
	if !line.HerdrSoho.Found || line.HerdrSoho.Capabilities == nil || *line.HerdrSoho.Capabilities != capsJSON {
		t.Errorf("herdr_soho = %+v", line.HerdrSoho)
	}
	if !line.HerdrSoho.EphemeralJob || !line.HerdrSoho.JobEvents {
		t.Errorf("capabilities booleans = %+v", line.HerdrSoho)
	}
	if !line.MachineLabel || !line.DispatcherURL {
		t.Errorf("machine_label/dispatcher_url = %v/%v", line.MachineLabel, line.DispatcherURL)
	}
	if line.KeyConfigured {
		t.Errorf("key_configured = true, want false (stub)")
	}
	if line.WakeHook != "true" {
		t.Errorf("wake_hook = %q, want true", line.WakeHook)
	}
	if line.Outbox.UltimoSeq != 3 || line.Outbox.EntregueSeq != 1 || line.Outbox.Pendentes != 2 {
		t.Errorf("outbox = %+v, want {3 1 2}", line.Outbox)
	}
}

// TestDoctorMissingBinary: a missing herdr-soho exits 43, reports found
// false and null capabilities, and still reports the rest.
func TestDoctorMissingBinary(t *testing.T) {
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, "no-such-herdr-soho-binary-xyz", "")
	line, _, exit := runDoctor(t, cfgDir, false)
	if exit != 43 {
		t.Fatalf("exit = %d, want 43", exit)
	}
	if line.HerdrSoho.Found {
		t.Errorf("found = true, want false")
	}
	if line.HerdrSoho.Capabilities != nil {
		t.Errorf("capabilities = %v, want null", *line.HerdrSoho.Capabilities)
	}
	if line.HerdrSoho.EphemeralJob || line.HerdrSoho.JobEvents {
		t.Errorf("capabilities booleans = %+v", line.HerdrSoho)
	}
	if line.MachineLabel {
		t.Errorf("machine_label = true, want false")
	}
	if line.WakeHook != "unknown" {
		t.Errorf("wake_hook = %q, want unknown", line.WakeHook)
	}
	if line.Outbox.UltimoSeq != 0 || line.Outbox.EntregueSeq != 0 || line.Outbox.Pendentes != 0 {
		t.Errorf("outbox = %+v, want zeros", line.Outbox)
	}
}

// TestDoctorCapabilitiesMissing: herdr-soho found but one capability
// missing exits 43 and still reports the raw line.
func TestDoctorCapabilitiesMissing(t *testing.T) {
	const line1 = `{"schema":1,"ephemeral_job":1}`
	exe, _ := installFakeSoho(t, capsRule(line1, 0))
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, exe, "machine-a")
	line, _, exit := runDoctor(t, cfgDir, false)
	if exit != 43 {
		t.Fatalf("exit = %d, want 43", exit)
	}
	if !line.HerdrSoho.Found || line.HerdrSoho.Capabilities == nil || *line.HerdrSoho.Capabilities != line1 {
		t.Errorf("herdr_soho = %+v", line.HerdrSoho)
	}
	if !line.HerdrSoho.EphemeralJob || line.HerdrSoho.JobEvents {
		t.Errorf("booleans = %+v, want ephemeral_job only", line.HerdrSoho)
	}
}

// TestDoctorWakeHook: the wake hook classification over the four input
// shapes, and the child environment carries HERDR_SOHO_NOWRITE=1.
func TestDoctorWakeHook(t *testing.T) {
	for _, tc := range []struct {
		name   string
		config fakesoho.Rule
		want   string
	}{
		{"true", fakesoho.Rule{Argv: []string{"config"}, Stdout: configTable(wakeRow), Code: 0}, "true"},
		{"falseOtherValue", fakesoho.Rule{Argv: []string{"config"}, Stdout: configTable("job_wake_cmd        other-cmd wake             file"), Code: 0}, "false"},
		{"falseNoRow", fakesoho.Rule{Argv: []string{"config"}, Stdout: configTable("machine_label   machine-a      file"), Code: 0}, "false"},
		{"unknownExit", fakesoho.Rule{Argv: []string{"config"}, Stdout: "", Code: 2}, "unknown"},
		{"unknownGarbage", fakesoho.Rule{Argv: []string{"config"}, Stdout: "boom\n", Code: 0}, "unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exe, fakeDir := installFakeSoho(t, capsRule(capsJSON, 0), tc.config)
			cfgDir := t.TempDir()
			setSohoConfig(t, cfgDir, exe, "machine-a")
			line, _, exit := runDoctor(t, cfgDir, false)
			if exit != 0 {
				t.Fatalf("exit = %d, want 0", exit)
			}
			if line.WakeHook != tc.want {
				t.Errorf("wake_hook = %q, want %q", line.WakeHook, tc.want)
			}
			// The config call ran with HERDR_SOHO_NOWRITE=1 in the child
			// environment.
			calls, err := fakesoho.ReadCalls(fakeDir)
			if err != nil {
				t.Fatalf("ReadCalls: %v", err)
			}
			var configCall *fakesoho.Call
			for i := range calls {
				if len(calls[i].Argv) == 1 && calls[i].Argv[0] == "config" {
					configCall = &calls[i]
					break
				}
			}
			if configCall == nil {
				t.Fatalf("no config call in the fake log: %v", calls)
			}
			found := false
			for _, kv := range configCall.Env {
				if kv == "HERDR_SOHO_NOWRITE=1" {
					found = true
				}
			}
			if !found {
				t.Errorf("child env lacks HERDR_SOHO_NOWRITE=1: %v", configCall.Env)
			}
		})
	}
}

// TestDoctorWakeHookExactValue: the wake hook is accepted only when the
// trimmed value's first field is exactly nau or a path whose last element
// is nau, and its second field is exactly wake; later fields, extra
// whitespace and the source column are allowed. The retired herdr-hermes
// name, with or without a path, is rejected on every shape.
func TestDoctorWakeHookExactValue(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value string
		want  string
	}{
		// Accepted.
		{"nauWake", "nau wake", "true"},
		{"nauWakeExtraArgs", "nau wake --extra x", "true"},
		{"nauWakeTripleSpace", "nau   wake", "true"},
		{"nauWakeTab", "nau\twake", "true"},
		{"nauWakePadded", "  nau wake  ", "true"},
		// The real nodes configure the absolute path of the nau binary.
		{"absolutePathNau", "/usr/local/bin/nau wake", "true"},
		// Rejected, including the retired herdr-hermes name on every shape.
		{"herdrHermesWake", "herdr-hermes wake", "false"},
		{"nauWakeup", "nau wakeup", "false"},
		{"herdrHermesWakeup", "herdr-hermes wakeup", "false"},
		{"nauWakeboard", "nau wakeboard", "false"},
		{"herdrHermesWakeboard", "herdr-hermes wakeboard", "false"},
		{"herdrHermesWakeExtraArgs", "herdr-hermes wake --extra x", "false"},
		{"herdrHermesWakeTripleSpace", "herdr-hermes   wake", "false"},
		{"herdrHermesWakeTab", "herdr-hermes\twake", "false"},
		{"herdrHermesWakePadded", "  herdr-hermes wake  ", "false"},
		{"nauwakeGlued", "nauwake", "false"},
		{"herdrHermeswakeGlued", "herdr-hermeswake", "false"},
		{"nauOnly", "nau", "false"},
		{"herdrHermesOnly", "herdr-hermes", "false"},
		{"wakeOnly", "wake", "false"},
		{"nauDashNotASpace", "nau-wake", "false"},
		{"otherCmdWake", "other-cmd wake", "false"},
		{"trailingSeparator", "/abs/nau/ wake", "false"},
		{"trailingDoubleSeparator", "/abs/nau// wake", "false"},
		{"absolutePathHerdrHermes", "/usr/local/bin/herdr-hermes wake", "false"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The raw config line carries the exact value between the key
			// and the source column, including any of its own spaces or
			// tabs.
			row := "job_wake_cmd        " + tc.value + "   file"
			exe, _ := installFakeSoho(t,
				capsRule(capsJSON, 0),
				fakesoho.Rule{Argv: []string{"config"}, Stdout: configTable(row), Code: 0},
			)
			cfgDir := t.TempDir()
			setSohoConfig(t, cfgDir, exe, "machine-a")
			line, _, exit := runDoctor(t, cfgDir, false)
			if exit != 0 {
				t.Fatalf("exit = %d, want 0", exit)
			}
			if line.WakeHook != tc.want {
				t.Errorf("wake_hook = %q, want %q", line.WakeHook, tc.want)
			}
		})
	}
}

// TestDoctorNowrite: doctor works under NOWRITE and creates no files in a
// fresh config dir.
func TestDoctorNowrite(t *testing.T) {
	exe, _ := installFakeSoho(t,
		capsRule(capsJSON, 0),
		fakesoho.Rule{Argv: []string{"config"}, Stdout: configTable(wakeRow), Code: 0},
	)
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, exe, "machine-a")
	_, _, exit := runDoctor(t, cfgDir, true)
	if exit != 0 {
		t.Fatalf("exit = %d, want 0", exit)
	}
	// The state dir must not exist at all (doctor is read-only).
	if _, err := os.Stat(filepath.Join(cfgDir, "state")); !os.IsNotExist(err) {
		t.Errorf("state dir exists after doctor under NOWRITE")
	}
}

// TestDoctorBadFlags: doctor with arguments exits 2.
func TestDoctorBadFlags(t *testing.T) {
	exe, _ := installFakeSoho(t, capsRule(capsJSON, 0))
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, exe, "machine-a")
	_, _, exit := runHermes(t, cfgDir, false, "", "doctor", "--bogus")
	if exit != 2 {
		t.Errorf("doctor --bogus: exit = %d, want 2", exit)
	}
}

// sameArgv reports whether the two argv slices are equal element by
// element.
func sameArgv(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestDoctorWakeHookConfigGet: `soho config get job_wake_cmd` is the
// first source of the value: an exit 0 decides from the trimmed first
// line of the stdout (even when the plain `config` listing would say
// otherwise, which is why the plain listing is not consulted), an exit
// 0 with an empty line gives false, and any other exit falls back to
// the plain `config` listing, which then reads the user file. The
// `config get` child carries both NOWRITE variables.
func TestDoctorWakeHookConfigGet(t *testing.T) {
	fileDir := t.TempDir()
	file := filepath.Join(fileDir, "config")
	if err := os.WriteFile(file, []byte("job_wake_cmd=/abs/nau wake\n"), 0o600); err != nil {
		t.Fatalf("write user file: %v", err)
	}
	for _, tc := range []struct {
		name   string
		get    fakesoho.Rule
		config fakesoho.Rule
		want   string
	}{
		{
			"absoluteValue",
			fakesoho.Rule{Argv: []string{"config", "get", "job_wake_cmd"}, Stdout: "/abs/nau wake\n", Code: 0},
			fakesoho.Rule{Argv: []string{"config"}, Stdout: "boom\n", Code: 0},
			"true",
		},
		{
			"emptyLine",
			fakesoho.Rule{Argv: []string{"config", "get", "job_wake_cmd"}, Stdout: "\n", Code: 0},
			fakesoho.Rule{Argv: []string{"config"}, Stdout: configTable(wakeRow), Code: 0},
			"false",
		},
		{
			"exit2FallsToUserFile",
			fakesoho.Rule{Argv: []string{"config", "get", "job_wake_cmd"}, Stderr: "soho: config rejected (2 extra arguments not allowed)\n", Code: 2},
			fakesoho.Rule{Argv: []string{"config"}, Stdout: configTable("user file:    " + file), Code: 0},
			"true",
		},
		{
			"exit1FallsToUserFile",
			fakesoho.Rule{Argv: []string{"config", "get", "job_wake_cmd"}, Stderr: "soho: config failed\n", Code: 1},
			fakesoho.Rule{Argv: []string{"config"}, Stdout: configTable("user file:    " + file), Code: 0},
			"true",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exe, fakeDir := installFakeSoho(t, capsRule(capsJSON, 0), tc.get, tc.config)
			cfgDir := t.TempDir()
			setSohoConfig(t, cfgDir, exe, "machine-a")
			line, _, exit := runDoctor(t, cfgDir, false)
			if exit != 0 {
				t.Fatalf("exit = %d, want 0", exit)
			}
			if line.WakeHook != tc.want {
				t.Errorf("wake_hook = %q, want %q", line.WakeHook, tc.want)
			}
			// The `config get` child ran with both NOWRITE variables in
			// the child environment, the same guarantee the test already
			// checks for the plain `config` call.
			calls, err := fakesoho.ReadCalls(fakeDir)
			if err != nil {
				t.Fatalf("ReadCalls: %v", err)
			}
			var getCall *fakesoho.Call
			for i := range calls {
				if sameArgv(calls[i].Argv, []string{"config", "get", "job_wake_cmd"}) {
					getCall = &calls[i]
					break
				}
			}
			if getCall == nil {
				t.Fatalf("no config get call in the fake log: %v", calls)
			}
			for _, want := range []string{"SOHO_NOWRITE=1", "HERDR_SOHO_NOWRITE=1"} {
				found := false
				for _, kv := range getCall.Env {
					if kv == want {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("config get child env lacks %s: %v", want, getCall.Env)
				}
			}
			if tc.get.Code == 0 && tc.want == "true" {
				// The exit 0 of `config get` decided the result, so the
				// plain `config` listing must not have been consulted.
				for _, c := range calls {
					if len(c.Argv) == 1 && c.Argv[0] == "config" {
						t.Errorf("plain config call was made although config get exited 0: %v", calls)
					}
				}
			}
		})
	}
}

// TestDoctorWakeHookUserFile: when the `config` listing carries no
// job_wake_cmd row, the path on the `user file:` line is read with the
// backend's own single-file config rules (the line is cut at its first
// # wherever it appears, the remainder is trimmed, the key is cut at
// the first = and normalized with . and - becoming _ and case kept,
// the value is trimmed and a quoted value loses one pair of quotes,
// and the last occurrence of the key wins); a missing file or a file
// without the key gives false, and a path with a space in it is read
// whole.
func TestDoctorWakeHookUserFile(t *testing.T) {
	for _, tc := range []struct {
		name  string
		file  string
		want  string
		space bool
	}{
		{"absoluteValue", "job_wake_cmd=/abs/nau wake\n", "true", false},
		{"commentedLineIgnored", "# job_wake_cmd=/abs/nau wake\n", "false", false},
		{"trailingCommentRemoved", "job_wake_cmd=/abs/nau wake    # the hook\n", "true", false},
		{"hashWithoutSpaceCuts", "job_wake_cmd=/abs/nau wake#nota\n", "true", false},
		{"hashCutsInsideValue", "job_wake_cmd=/abs/na#u wake\n", "false", false},
		{"quotedValue", "job_wake_cmd=\"/abs/nau wake\"\n", "true", false},
		{"dashKeyNormalized", "job-wake-cmd=/abs/nau wake\n", "true", false},
		{"paddedKey", " job_wake_cmd = /abs/nau wake\n", "true", false},
		{"upperCaseKey", "JOB_WAKE_CMD=/abs/nau wake\n", "false", false},
		{"bomAndCRLF", "\ufeffjob_wake_cmd=/abs/nau wake\r\n", "true", false},
		{"lastOccurrenceWins", "job_wake_cmd=/abs/other wake\njob_wake_cmd=/abs/nau wake\n", "true", false},
		{"lastOccurrenceWinsRejected", "job_wake_cmd=/abs/nau wake\njob_wake_cmd=/abs/other wake\n", "false", false},
		{"keyAbsent", "machine_label=machine-a\n", "false", false},
		{"fileAbsent", "", "false", false},
		{"pathWithSpace", "job_wake_cmd=/abs/nau wake\n", "true", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if tc.space {
				dir = filepath.Join(dir, "soho dir")
				if err := os.MkdirAll(dir, 0o700); err != nil {
					t.Fatalf("mkdir: %v", err)
				}
			}
			file := filepath.Join(dir, "config")
			if tc.file != "" {
				if err := os.WriteFile(file, []byte(tc.file), 0o600); err != nil {
					t.Fatalf("write user file: %v", err)
				}
			}
			exe, _ := installFakeSoho(t,
				capsRule(capsJSON, 0),
				fakesoho.Rule{Argv: []string{"config"}, Stdout: configTable("user file:    " + file), Code: 0},
			)
			cfgDir := t.TempDir()
			setSohoConfig(t, cfgDir, exe, "machine-a")
			line, _, exit := runDoctor(t, cfgDir, false)
			if exit != 0 {
				t.Fatalf("exit = %d, want 0", exit)
			}
			if line.WakeHook != tc.want {
				t.Errorf("wake_hook = %q, want %q", line.WakeHook, tc.want)
			}
		})
	}
}

// TestWakeValueFields: the value classification on the pure field list,
// including the Windows shapes tested through the flag instead of the
// host operating system: the .exe form of the last element in any
// casing is accepted and the backslash separator counts there; on the
// unix operating systems a backslash is not a separator and the .exe
// suffix is not accepted. The second field must be exactly wake.
func TestWakeValueFields(t *testing.T) {
	for _, tc := range []struct {
		name    string
		fields  []string
		windows bool
		want    bool
	}{
		{"exactNauWake", []string{"nau", "wake"}, false, true},
		{"absoluteNauWake", []string{"/abs/nau", "wake"}, false, true},
		{"absoluteNauWakeExtraArgs", []string{"/abs/nau", "wake", "--extra"}, false, true},
		{"windowsBackslashExe", []string{`C:\x\NAU.EXE`, "wake"}, true, true},
		{"windowsForwardSlashExe", []string{`c:/x/nau.exe`, "wake"}, true, true},
		{"windowsBareExe", []string{`NAU.EXE`, "wake"}, true, true},
		{"windowsBackslashNau", []string{`C:\x\nau`, "wake"}, true, true},
		{"windowsOtherExe", []string{`C:\x\other.exe`, "wake"}, true, false},
		{"windowsNauNoExeMixedCase", []string{`C:\x\NAU`, "wake"}, true, false},
		{"unixExeSuffix", []string{"/abs/nau.exe", "wake"}, false, false},
		{"unixBackslashNotSeparator", []string{`C:\x\NAU.EXE`, "wake"}, false, false},
		{"herdrHermes", []string{"herdr-hermes", "wake"}, false, false},
		{"herdrHermesPath", []string{"/abs/herdr-hermes", "wake"}, false, false},
		{"windowsHerdrHermesExe", []string{`C:\x\herdr-hermes.exe`, "wake"}, true, false},
		{"otherCmd", []string{"/abs/other", "wake"}, false, false},
		{"wakeup", []string{"nau", "wakeup"}, false, false},
		{"nauOnly", []string{"nau"}, false, false},
		// A first field that ends in a separator names a directory, not
		// an executable, and is never accepted.
		{"trailingSlashUnix", []string{"/abs/nau/", "wake"}, false, false},
		{"trailingDoubleSlashUnix", []string{"/abs/nau//", "wake"}, false, false},
		{"trailingBareSlash", []string{"/", "wake"}, false, false},
		{"trailingSlashWindows", []string{`C:\x\nau/`, "wake"}, true, false},
		{"trailingBackslashWindows", []string{`C:\x\nau.exe\`, "wake"}, true, false},
		// On Windows a leading letter volume prefix (C:) is dropped
		// before the last element is taken; on the unix operating
		// systems C:nau is an ordinary name.
		{"volumeRelativeNau", []string{"C:nau", "wake"}, true, true},
		{"volumeRelativeExe", []string{"C:nau.exe", "wake"}, true, true},
		{"volumeRelativeMixedCase", []string{"c:NAU.EXE", "wake"}, true, true},
		{"volumeRelativeOther", []string{"C:other.exe", "wake"}, true, false},
		{"volumeSlashExe", []string{"C:/nau.exe", "wake"}, true, true},
		{"volumeBackslashExe", []string{`C:\nau.exe`, "wake"}, true, true},
		{"posixVolumeIsName", []string{"C:nau", "wake"}, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := wakeValueAccepted(tc.fields, tc.windows); got != tc.want {
				t.Errorf("wakeValueAccepted(%v, %v) = %v, want %v", tc.fields, tc.windows, got, tc.want)
			}
		})
	}
}
