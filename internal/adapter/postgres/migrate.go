package postgres

import (
	"embed"
	"errors"
	"fmt"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5" // registers the pgx5:// driver
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

// Migrator applies and reverts the versioned, embedded migrations.
type Migrator struct {
	m *migrate.Migrate
}

// NewMigrator builds a migrator for a postgres:// URL.
func NewMigrator(databaseURL string) (*Migrator, error) {
	var m *migrate.Migrate
	src, err := iofs.New(migrationFiles, "migrations")
	if err == nil {
		m, err = migrate.NewWithSourceInstance("iofs", src, toPgx5URL(databaseURL))
	}
	if err != nil {
		return nil, fmt.Errorf("open migrator: %w", err)
	}
	return &Migrator{m: m}, nil
}

// toPgx5URL rewrites a postgres:// or postgresql:// URL to the pgx5:// scheme
// golang-migrate registers for its pgx v5 driver; other URLs pass through.
func toPgx5URL(url string) string {
	for _, prefix := range []string{"postgres://", "postgresql://"} {
		if strings.HasPrefix(url, prefix) {
			return "pgx5://" + strings.TrimPrefix(url, prefix)
		}
	}
	return url
}

// Up applies every pending migration.
func (m *Migrator) Up() error { return ignoreNoChange(m.m.Up()) }

// Down reverts up to the given number of migrations, which must be at least
// one; reverting past the first migration stops at version 0 without error,
// and a database with no migration applied is a no-op.
func (m *Migrator) Down(steps int) error {
	if steps < 1 {
		return fmt.Errorf("migrate down: steps must be >= 1, got %d", steps)
	}
	v, _, err := m.Version()
	if err != nil || v == 0 {
		return err
	}
	return ignoreNoChange(m.m.Steps(-steps))
}

// Version returns the current version and whether it is dirty.
func (m *Migrator) Version() (uint, bool, error) {
	v, dirty, err := m.m.Version()
	if errors.Is(err, migrate.ErrNilVersion) {
		return 0, false, nil
	}
	return v, dirty, err
}

// Close releases the migrator connections.
func (m *Migrator) Close() error {
	srcErr, dbErr := m.m.Close()
	return errors.Join(srcErr, dbErr)
}

// ignoreNoChange treats "nothing to apply" (ErrNoChange) and "fewer steps
// left than requested" (ErrShortLimit) as success: the schema is at the
// version the caller wanted or as close to it as it can get.
func ignoreNoChange(err error) error {
	var short migrate.ErrShortLimit
	if errors.Is(err, migrate.ErrNoChange) || errors.As(err, &short) {
		return nil
	}
	return err
}
