package cli

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/djalmajr/herdr-hermes/internal/outbox"
	"github.com/djalmajr/herdr-hermes/internal/testutil/fakesoho"
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

const wakeRow = "job_wake_cmd        herdr-hermes wake                    file"

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
