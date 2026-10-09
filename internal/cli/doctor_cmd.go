package cli

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"time"

	"github.com/djalmajr/herdr-hermes/internal/config"
	"github.com/djalmajr/herdr-hermes/internal/jobapi"
	"github.com/djalmajr/herdr-hermes/internal/outbox"
	"github.com/djalmajr/herdr-hermes/internal/soho"
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
// files. It exits 43 when herdr-soho is missing or lacks either
// capability, else 0.
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
	if !so.EphemeralJob || !so.JobEvents {
		_, _ = fmt.Fprintf(env.Stderr, "herdr-hermes: herdr-soho capabilities missing\n")
		return 43
	}
	return 0
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

// wakeHookConfigured runs `herdr-soho config` with HERDR_SOHO_NOWRITE=1 in
// the child environment and classifies the job_wake_cmd value: "true" when
// it starts with "herdr-hermes wake", "false" when the output is
// parseable but shows no such hook, "unknown" on a non-zero exit or
// unparseable output.
func wakeHookConfigured(ctx context.Context, runner soho.Runner, env Env) string {
	environ := childEnviron(env)
	// The child gets HERDR_SOHO_NOWRITE=1; any inherited value is replaced
	// so the read-only flag is guaranteed.
	filtered := environ[:0]
	for _, kv := range environ {
		if !strings.HasPrefix(kv, "HERDR_SOHO_NOWRITE=") {
			filtered = append(filtered, kv)
		}
	}
	filtered = append(filtered, "HERDR_SOHO_NOWRITE=1")
	wakeRunner := soho.Runner{Bin: runner.Bin, Environ: filtered, Now: runner.Now}
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

// parseWakeHook classifies the `herdr-soho config` output. A line whose
// first whitespace-separated field is job_wake_cmd decides the result:
// true when its value starts with "herdr-hermes wake", false otherwise.
// Output with no such line is false when it is parseable as key-value
// lines and unknown otherwise.
func parseWakeHook(output string) string {
	const prefix = "herdr-hermes wake"
	parseable := false
	for _, line := range strings.Split(output, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		parseable = true
		if fields[0] == "job_wake_cmd" {
			value := strings.TrimSpace(line[len(fields[0]):])
			if strings.HasPrefix(value, prefix) {
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
