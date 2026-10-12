package main

import (
	"os"

	"github.com/djalmajr/nau/internal/cli"
)

// nau is the primary entry point. It runs the same cli.Run code path as
// the deprecated herdr-hermes entry point, against the same state
// directory, with its own program name for the usage text and the
// stderr diagnostic prefix.
func main() {
	env := cli.EnvFromOS()
	env.Program = cli.ProgramNau
	os.Exit(cli.Run(os.Args[1:], env))
}
