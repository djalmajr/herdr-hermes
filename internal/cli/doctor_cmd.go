package cli

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

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
	base := strings.TrimSuffix(filepath.Base(bin), ".exe")
	if runtime.GOOS == "windows" {
		return strings.EqualFold(base, "herdr-soho")
	}
	return base == "herdr-soho"
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

// wakeHookConfigured runs `soho config` with SOHO_NOWRITE=1 and
// HERDR_SOHO_NOWRITE=1 in the child environment (see
// backendNowriteEnviron) and classifies the job_wake_cmd value: "true"
// when the trimmed value's first field is exactly "nau" and its second
// field is exactly "wake", "false" when the output is parseable but
// shows no such hook, "unknown" on a non-zero exit or unparseable
// output.
func wakeHookConfigured(ctx context.Context, runner soho.Runner, env Env) string {
	wakeRunner := soho.Runner{Bin: runner.Bin, Environ: backendNowriteEnviron(env), Now: runner.Now}
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

// parseWakeHook classifies the `soho config` output. A line whose
// first whitespace-separated field is job_wake_cmd decides the result:
// true when the trimmed value's first field is exactly nau and its second
// field is exactly wake; fields after the second, extra whitespace and the
// source column are allowed. The rest of the output must be parseable as
// key-value lines for the result to be false; otherwise it is unknown.
func parseWakeHook(output string) string {
	parseable := false
	for _, line := range strings.Split(output, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		parseable = true
		if fields[0] == "job_wake_cmd" {
			value := strings.Fields(line[len(fields[0]):])
			if len(value) >= 2 && value[0] == ProgramNau && value[1] == "wake" {
				return "true"
			}
			return "false"
		}
	}
	if parseable {
		return "false"
	}
	return "unknown"
}
