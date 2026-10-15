package main

import (
	"os"

	"github.com/djalmajr/nau/internal/cli"
)

// nau is the single entry point of the program. It runs cli.Run against
// the state directory, with its program name for the usage text and the
// stderr diagnostic prefix.
func main() {
	env := cli.EnvFromOS()
	env.Program = cli.ProgramNau
	os.Exit(cli.Run(os.Args[1:], env))
}
