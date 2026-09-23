// Command wallet runs the wager wallet service. See package cli for the
// commands and exit codes.
package main

import (
	"context"
	"os"

	"github.com/Pantani/backend-challenge-go/internal/cli"
)

func main() {
	os.Exit(cli.Run(context.Background(), os.Args[1:], os.LookupEnv, os.Stdout, os.Stderr))
}
