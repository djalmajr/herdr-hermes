package cli

import (
	"context"
	"fmt"
	"time"

	"github.com/djalmajr/herdr-hermes/internal/jobapi"
	"github.com/djalmajr/herdr-hermes/internal/outbox"
)

// cmdOutbox implements the pull channel: outbox [--since <seq>] [--wait
// <ms>]. It is read-only: it never creates the state directory, the lock
// file or any other file, and it works under HERDR_HERMES_NOWRITE=1.
func cmdOutbox(args []string, env Env) int {
	since, waitMS, ok := parseOutboxArgs(args)
	if !ok {
		badUsage(env, "usage: outbox [--since <seq>] [--wait <ms>]")
		return 2
	}
	store, err := outbox.Open(outbox.StateDir(env.ConfigDir), outbox.Options{
		Now:      env.Now,
		ReadOnly: true,
	})
	if err != nil {
		fail(env, 2, "outbox: "+err.Error())
		return 2
	}
	lines, last, err := store.Read(since)
	if err != nil {
		fail(env, 2, "outbox: "+err.Error())
		return 2
	}
	if len(lines) == 0 && waitMS > 0 {
		found, wlast, err := store.Wait(context.Background(), since, time.Duration(waitMS)*time.Millisecond, envClock{env: env})
		if err != nil {
			fail(env, 2, "outbox: "+err.Error())
			return 2
		}
		if wlast > last {
			last = wlast
		}
		if found {
			lines, _, err = store.Read(since)
			if err != nil {
				fail(env, 2, "outbox: "+err.Error())
				return 2
			}
		}
	}
	for _, l := range lines {
		_, _ = fmt.Fprintf(env.Stdout, "%s\n", l)
	}
	delivered, err := store.DeliveredSeq()
	if err != nil {
		fail(env, 2, "outbox: "+err.Error())
		return 2
	}
	_, _ = fmt.Fprintf(env.Stdout, "{\"outbox\":\"fim\",\"ultimo_seq\":%d,\"entregue_seq\":%d}\n", last, delivered)
	return 0
}

func parseOutboxArgs(args []string) (since int64, waitMS int, ok bool) {
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--since":
			if i+1 >= len(args) {
				return 0, 0, false
			}
			v, err := parseLong(args[i+1])
			if err != nil || v < 0 {
				return 0, 0, false
			}
			since = v
			i++
		case "--wait":
			if i+1 >= len(args) {
				return 0, 0, false
			}
			v, err := parseLong(args[i+1])
			if err != nil || v < 0 || v > jobapi.MaxWaitMS {
				return 0, 0, false
			}
			waitMS = int(v)
			i++
		default:
			return 0, 0, false
		}
	}
	return since, waitMS, true
}

func parseLong(s string) (int64, error) {
	n := int64(0)
	if len(s) == 0 {
		return 0, errBadNumber
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return 0, errBadNumber
		}
		n = n*10 + int64(r-'0')
	}
	return n, nil
}

var errBadNumber = fmt.Errorf("bad number")

// envClock adapts Env.Now/Env.Sleep to the outbox Clock.
type envClock struct {
	env Env
}

func (c envClock) Now() time.Time { return c.env.Now() }
func (c envClock) Sleep(ctx context.Context, d time.Duration) error {
	return c.env.Sleep(ctx, d)
}
