// Package cli implements the wallet binary commands:
//
//	wallet serve                 run the API, consumers and workers (default)
//	wallet migrate up            apply every pending migration
//	wallet migrate down [n]      revert n migrations (default 1)
//	wallet migrate version       print the current schema version
//	wallet provision-queues      create the SQS queues and the DLQ redrive
//
// The command is parsed before any configuration is read, and each command
// loads only the variables it needs: migrate reads the database ones,
// provision-queues the AWS ones and serve all of them.
package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os/signal"
	"strconv"
	"syscall"

	"go.uber.org/fx"

	"github.com/Pantani/backend-challenge-go/internal/adapter/postgres"
	sqsadapter "github.com/Pantani/backend-challenge-go/internal/adapter/sqs"
	"github.com/Pantani/backend-challenge-go/internal/bootstrap"
	"github.com/Pantani/backend-challenge-go/internal/config"
)

// ErrUsage reports an unknown command or bad arguments.
var ErrUsage = errors.New("usage: wallet [serve | migrate up | migrate down [n] | migrate version | provision-queues]")

// Exit codes of Run.
const (
	ExitOK    = 0
	ExitError = 1
	ExitUsage = 2
)

// migrator is the subset of postgres.Migrator the migrate command uses.
type migrator interface {
	Up() error
	Down(steps int) error
	Version() (version uint, dirty bool, err error)
	Close() error
}

// application is the lifecycle surface the CLI owns. Keeping it smaller than
// fx.App makes startup and shutdown behavior deterministic in tests.
type application interface {
	Err() error
	Start(context.Context) error
	Stop(context.Context) error
	Wait() <-chan fx.ShutdownSignal
}

// Constructors of the external dependencies, replaceable in tests.
var (
	newMigrator  = defaultMigrator
	newSQSClient = bootstrap.NewSQSClient
	newApp       = defaultApp
)

func defaultMigrator(databaseURL string) (migrator, error) {
	m, err := postgres.NewMigrator(databaseURL)
	if err != nil {
		return nil, err
	}
	return m, nil
}

func defaultApp(ctx context.Context, cfg config.Config) application { return bootstrap.New(ctx, cfg) }

// Run executes the command in args and returns the process exit code:
// ExitUsage for an unknown command or bad arguments, ExitError for any other
// failure (printed on stderr as "error: ..."), ExitOK otherwise. Command
// output goes to stdout.
func Run(ctx context.Context, args []string, lookup config.Lookup, stdout, stderr io.Writer) int {
	err := run(ctx, args, lookup, stdout)
	if err == nil {
		return ExitOK
	}
	_, _ = fmt.Fprintln(stderr, "error:", err)
	if errors.Is(err, ErrUsage) {
		return ExitUsage
	}
	return ExitError
}

func run(ctx context.Context, args []string, lookup config.Lookup, stdout io.Writer) error {
	cmd, rest := "serve", []string(nil)
	if len(args) > 0 {
		cmd, rest = args[0], args[1:]
	}
	switch cmd {
	case "serve":
		return noArgs(rest, func() error { return serveCmd(ctx, lookup) })
	case "migrate":
		return migrateCmd(lookup, rest, stdout)
	case "provision-queues":
		return noArgs(rest, func() error { return provision(ctx, lookup, stdout) })
	}
	return ErrUsage
}

// noArgs runs fn only when the command received no surplus arguments.
func noArgs(rest []string, fn func() error) error {
	if len(rest) > 0 {
		return ErrUsage
	}
	return fn()
}

func serveCmd(ctx context.Context, lookup config.Lookup) error {
	cfg, err := config.Load(lookup)
	if err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}
	return serve(ctx, cfg)
}

// serve runs until SIGINT/SIGTERM (or ctx cancellation) or until the
// application asks to shut down (the HTTP server failed), then stops it
// within the configured shutdown timeout. Signal handling is released before
// the stop, so a second signal terminates the process.
func serve(ctx context.Context, cfg config.Config) error {
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	startCtx, cancelStart := context.WithTimeout(ctx, cfg.StartupTimeout)
	defer cancelStart()
	application := newApp(startCtx, cfg)
	if err := application.Err(); err != nil {
		return fmt.Errorf("build application: %w", err)
	}
	if err := application.Start(startCtx); err != nil {
		return err
	}
	cancelStart()
	var exitErr error
	select {
	case <-ctx.Done():
	case sig := <-application.Wait():
		if sig.ExitCode != 0 {
			exitErr = fmt.Errorf("application failed (exit code %d)", sig.ExitCode)
		}
	}
	stop()
	stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.ShutdownTimeout)
	defer cancel()
	return errors.Join(exitErr, application.Stop(stopCtx))
}

// migration is a parsed migrate subcommand.
type migration struct {
	op    string
	steps int
}

// parseMigration validates the migrate arguments before any configuration
// is read, so `wallet migrate` alone is a usage error.
func parseMigration(args []string) (migration, error) {
	if len(args) == 0 {
		return migration{}, ErrUsage
	}
	switch args[0] {
	case "up", "version":
		return migration{op: args[0]}, noArgs(args[1:], func() error { return nil })
	case "down":
		steps, err := parseSteps(args[1:])
		return migration{op: "down", steps: steps}, err
	}
	return migration{}, ErrUsage
}

// parseSteps reads the optional step count of `migrate down` (default 1).
func parseSteps(args []string) (int, error) {
	if len(args) == 0 {
		return 1, nil
	}
	n, err := strconv.Atoi(args[0])
	if err != nil || n < 1 || len(args) > 1 {
		return 0, ErrUsage
	}
	return n, nil
}

func migrateCmd(lookup config.Lookup, args []string, stdout io.Writer) error {
	mig, err := parseMigration(args)
	if err != nil {
		return err
	}
	cfg, err := config.LoadDatabase(lookup)
	if err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}
	m, err := newMigrator(cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer func() { _ = m.Close() }()
	return mig.run(m, stdout)
}

func (mig migration) run(m migrator, stdout io.Writer) error {
	switch mig.op {
	case "up":
		return m.Up()
	case "down":
		return m.Down(mig.steps)
	}
	return printVersion(m, stdout)
}

type versioner interface {
	Version() (uint, bool, error)
}

func printVersion(m versioner, stdout io.Writer) error {
	v, dirty, err := m.Version()
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(stdout, "version=%d dirty=%t\n", v, dirty)
	return err
}

func provision(ctx context.Context, lookup config.Lookup, stdout io.Writer) error {
	cfg, err := config.LoadSQS(lookup)
	if err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}
	client, err := newSQSClient(ctx, cfg)
	if err != nil {
		return err
	}
	q, err := sqsadapter.Provision(ctx, client, sqsadapter.ProvisionConfig{
		Names:           bootstrap.QueueNames(cfg),
		MaxReceiveCount: sqsadapter.RedriveMaxReceiveCount(cfg.SQSMaxReceiveCount), VisibilityTimeout: int(cfg.SQSVisibilityTimeout.Seconds()),
	})
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(stdout, "input=%s\ndlq=%s\nevents=%s\n", q.Input, q.DLQ, q.Events)
	return err
}
