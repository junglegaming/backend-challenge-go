package postgres

import (
	"errors"
	"io/fs"
	"testing"

	"github.com/golang-migrate/migrate/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOnlyInitialMigrationIsEmbedded(t *testing.T) {
	t.Parallel()
	entries, err := fs.ReadDir(migrationFiles, "migrations")
	require.NoError(t, err)

	names := make([]string, len(entries))
	for i, entry := range entries {
		names[i] = entry.Name()
	}
	assert.Equal(t, []string{"000001_init.down.sql", "000001_init.up.sql"}, names)
}

func TestToPgx5URL(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "pgx5://h/db", toPgx5URL("postgres://h/db"))
	assert.Equal(t, "pgx5://h/db", toPgx5URL("postgresql://h/db"))
	assert.Equal(t, "pgx5://h/db", toPgx5URL("pgx5://h/db"))
}

func TestIgnoreNoChange(t *testing.T) {
	t.Parallel()
	assert.NoError(t, ignoreNoChange(nil))
	assert.NoError(t, ignoreNoChange(migrate.ErrNoChange))
	assert.NoError(t, ignoreNoChange(migrate.ErrShortLimit{Short: 2}))
	other := errors.New("dirty")
	assert.Same(t, other, ignoreNoChange(other))
}

func TestDownRejectsNonPositiveSteps(t *testing.T) {
	t.Parallel()
	m := &Migrator{}
	require.Error(t, m.Down(0))
	require.Error(t, m.Down(-1))
}
