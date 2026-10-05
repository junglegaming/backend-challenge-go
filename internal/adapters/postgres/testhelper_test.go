package postgres

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/VictorM0nteiro/backend-challenge-go/internal/testutil"
	"github.com/golang-migrate/migrate/v4"
)

const migrationsPath = "file://../../../migrations"

// newTestPool starts a throwaway Postgres through testutil and returns a
// ready-to-use *Pool. The pool is closed when the test finishes.
func newTestPool(t *testing.T) *Pool {
	t.Helper()
	dsn := testutil.StartPostgres(t)

	pool, err := NewPool(context.Background(), PoolConfig{
		DSN:             dsn,
		MaxConns:        5,
		MaxConnLifetime: time.Minute,
		AcquireTimeout:  5 * time.Second,
	})
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func applyMigrations(dsn string) error {
	m, err := migrate.New(migrationsPath, dsn)
	if err != nil {
		return fmt.Errorf("load migrations: %w", err)
	}
	defer func() { _, _ = m.Close() }()

	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("run migrations: %w", err)
	}
	return nil
}
