package cli

import (
	"errors"
	"fmt"
	"strings"

	"github.com/djalmajr/herdr-hermes/internal/config"
	"github.com/djalmajr/herdr-hermes/internal/jobapi"
	"github.com/djalmajr/herdr-hermes/internal/outbox"
)

// decisionStdinCap is the cap on a decision resumo read on stdin: 16 KiB,
// the same bound the contract gives `job send` bodies.
const decisionStdinCap = jobapi.StdinCapSend

// decisionDados is the dados payload of a decision record. The field order
// is normative.
type decisionDados struct {
	Resumo string `json:"resumo"`
	Motivo string `json:"motivo"`
	Escopo string `json:"escopo"`
}

// cmdDecision implements
// `decision --projeto <org/repo> --escopo global|projeto --motivo <text>
// [--job <id>] [<resumo>|-]`. It appends one decision record for decisions
// taken outside a job; job decisions already travel as job events.
func cmdDecision(args []string, env Env) int {
	var (
		projeto, escopo, motivo, job string
		positional                   []string
		fromStdin                    bool
	)
	for i := 0; i < len(args); i++ {
		switch flag := args[i]; flag {
		case "--projeto", "--escopo", "--motivo", "--job":
			if i+1 >= len(args) {
				badUsage(env, "decision: "+flag+" requires a value")
				return 2
			}
			i++
			switch flag {
			case "--projeto":
				projeto = args[i]
			case "--escopo":
				escopo = args[i]
			case "--motivo":
				motivo = args[i]
			case "--job":
				job = args[i]
			}
		case "-":
			fromStdin = true
		default:
			if strings.HasPrefix(flag, "-") {
				badUsage(env, "decision: unknown flag "+quote(flag))
				return 2
			}
			positional = append(positional, flag)
		}
	}
	if fromStdin && len(positional) > 0 {
		badUsage(env, "decision: the resumo comes from the positional argument or from stdin with '-', not both")
		return 2
	}
	if len(positional) > 1 {
		badUsage(env, "decision: at most one positional resumo")
		return 2
	}
	resumo := ""
	if fromStdin {
		data, err := readCapped(env.Stdin, decisionStdinCap)
		if errors.Is(err, errInputOverCap) {
			fail(env, 2, "decision: the resumo from stdin is over the 16 KiB cap")
			return 2
		}
		if err != nil {
			fail(env, 2, "decision: reading stdin: "+err.Error())
			return 2
		}
		resumo = trimTrailingNewline(string(data))
	} else if len(positional) == 1 {
		resumo = positional[0]
	}
	if resumo == "" {
		fail(env, 2, "decision: the resumo is required (positional argument or '-' on stdin)")
		return 2
	}
	if escopo != "global" && escopo != "projeto" {
		fail(env, 2, "decision: --escopo must be global or projeto")
		return 2
	}
	if !jobapi.ValidRepo(projeto) {
		fail(env, 2, "decision: --projeto must be <org>/<repo> with each part matching the contract charset")
		return 2
	}
	if motivo == "" {
		fail(env, 2, "decision: --motivo is required")
		return 2
	}
	if job != "" && !jobapi.ValidID(job) {
		fail(env, 2, "decision: --job must match the job id pattern")
		return 2
	}
	cfg, err := config.Load(env.ConfigDir)
	if err != nil {
		fail(env, 2, "config: "+err.Error())
		return 2
	}
	if cfg.MachineLabel == "" {
		fail(env, 2, "machine_label is empty: set it with `herdr-hermes config set machine_label <label>`")
		return 2
	}
	resumo = cutRunes(resumo, stateCut)
	motivo = cutRunes(motivo, stateCut)
	store, err := outbox.Open(outbox.StateDir(env.ConfigDir), outbox.Options{Now: env.Now})
	if err != nil {
		fail(env, 2, "decision: "+err.Error())
		return 2
	}
	rec, err := store.Append(outbox.Record{
		Tipo:    outbox.TipoDecision,
		Maquina: cfg.MachineLabel,
		Projeto: projeto,
		JobID:   jobIDPtr(job),
		Dados:   mustMarshal(decisionDados{Resumo: resumo, Motivo: motivo, Escopo: escopo}),
	})
	if err != nil {
		fail(env, 2, "decision: "+err.Error())
		return 2
	}
	_, _ = fmt.Fprintf(env.Stdout, "{\"seq\":%d}\n", rec.Seq)
	return 0
}

// trimTrailingNewline removes one trailing LF (or CRLF).
func trimTrailingNewline(s string) string {
	if strings.HasSuffix(s, "\n") {
		s = s[:len(s)-1]
		if strings.HasSuffix(s, "\r") {
			s = s[:len(s)-1]
		}
	}
	return s
}
