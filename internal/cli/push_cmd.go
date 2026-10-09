package cli

import (
	"context"
	"fmt"

	"github.com/djalmajr/herdr-hermes/internal/config"
	"github.com/djalmajr/herdr-hermes/internal/outbox"
)

// cmdPush implements `push`: push the pending outbox records to the
// dispatcher, interactively (with the 10/30/90 s retries). The exit code is
// the outcome code: 0 (success or push disabled), 40 (no key), 41 (key
// rejected) or 42 (unreachable or rejected). The status member is printed
// only when the outcome has one.
func cmdPush(args []string, env Env) int {
	if len(args) != 0 {
		badUsage(env, "usage: push")
		return 2
	}
	cfg, err := config.Load(env.ConfigDir)
	if err != nil {
		fail(env, 2, "config: "+err.Error())
		return 2
	}
	store, err := outbox.Open(outbox.StateDir(env.ConfigDir), outbox.Options{Now: env.Now})
	if err != nil {
		fail(env, 2, "push: "+err.Error())
		return 2
	}
	o := pushPending(context.Background(), env, cfg, store, true)
	_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(struct {
		Enviados  int    `json:"enviados"`
		Pendentes int    `json:"pendentes"`
		Status    string `json:"status,omitempty"`
	}{Enviados: o.Sent, Pendentes: o.Pending, Status: o.Status}))
	return o.Code
}
