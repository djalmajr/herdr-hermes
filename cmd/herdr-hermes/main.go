package main

import (
	"os"

	"github.com/djalmajr/nau/internal/cli"
)

// herdr-hermes is the deprecated entry point of nau: same commands, same
// state, same exit codes. It runs the same cli.Run code path as nau with
// its own program name for the usage text and the stderr diagnostic
// prefix.
func main() {
	env := cli.EnvFromOS()
	env.Program = cli.ProgramHerdrHermes
	os.Exit(cli.Run(os.Args[1:], env))
}
