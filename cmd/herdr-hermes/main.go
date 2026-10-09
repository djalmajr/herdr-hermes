package main

import (
	"os"

	"github.com/djalmajr/herdr-hermes/internal/cli"
)

func main() {
	os.Exit(cli.Run(os.Args[1:], cli.EnvFromOS()))
}
