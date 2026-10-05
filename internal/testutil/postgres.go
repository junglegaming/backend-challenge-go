package testutil

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// StartPostgres starts a throwaway Postgres 16 container, applies every
// migration in the repository, and returns the DSN. The container is removed
// when the test finishes.
func StartPostgres(t testing.TB) string {
	t.Helper()
	ctx := context.Background()

	container, err := tcpostgres.Run(ctx, "postgres:16",
		tcpostgres.WithDatabase("wagering_test"),
		tcpostgres.WithUsername("wagering"),
		tcpostgres.WithPassword("wagering"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Logf("terminate postgres container: %v", err)
		}
	})

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("build connection string: %v", err)
	}
	if err := applyMigrations(dsn); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	return dsn
}

func applyMigrations(dsn string) error {
	m, err := migrate.New(migrationsURL(), dsn)
	if err != nil {
		return fmt.Errorf("load migrations: %w", err)
	}
	defer func() { _, _ = m.Close() }()

	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("run migrations: %w", err)
	}
	return nil
}

// migrationsURL resolves migrations/ from this file's location, so the path is
// right whichever package's tests call it. A relative path would depend on the
// working directory of the caller.
func migrationsURL() string {
	_, thisFile, _, _ := runtime.Caller(0)
	root := filepath.Join(filepath.Dir(thisFile), "..", "..", "migrations")
	return "file://" + filepath.ToSlash(root)
}
