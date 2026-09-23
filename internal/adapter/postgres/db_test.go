package postgres

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/puddle/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Pantani/backend-challenge-go/internal/app"
)

func TestMapError(t *testing.T) {
	t.Parallel()
	assert.NoError(t, mapError(nil))
	cases := map[string]struct {
		err  error
		want error
	}{
		"unique":             {&pgconn.PgError{Code: "23505"}, app.ErrConflict},
		"serialization":      {&pgconn.PgError{Code: "40001"}, app.ErrConflict},
		"deadlock":           {&pgconn.PgError{Code: "40P01"}, app.ErrConflict},
		"lock timeout":       {&pgconn.PgError{Code: "55P03"}, app.ErrConflict},
		"statement timeout":  {&pgconn.PgError{Code: "57014"}, app.ErrUnavailable},
		"completion unknown": {&pgconn.PgError{Code: "40003"}, app.ErrUnavailable},
		"shutdown":           {&pgconn.PgError{Code: "57P01"}, app.ErrUnavailable},
		"connection class":   {&pgconn.PgError{Code: "08006"}, app.ErrUnavailable},
		"resources class":    {&pgconn.PgError{Code: "53300"}, app.ErrUnavailable},
		"system class":       {&pgconn.PgError{Code: "58030"}, app.ErrUnavailable},
		"wrapped pg error":   {fmt.Errorf("exec: %w", &pgconn.PgError{Code: "40001"}), app.ErrConflict},
		"network":            {&net.OpError{Op: "dial", Err: errors.New("refused")}, app.ErrUnavailable},
		"closed pool":        {fmt.Errorf("acquire: %w", puddle.ErrClosedPool), app.ErrUnavailable},
		"conn closed":        {pgconn.ErrConnClosed, app.ErrUnavailable},
	}
	for name, tc := range cases {
		assert.ErrorIs(t, mapError(tc.err), tc.want, name)
	}
}

func TestMapErrorPassesThrough(t *testing.T) {
	t.Parallel()
	cases := map[string]error{
		"check violation":   &pgconn.PgError{Code: "23514"},
		"plain":             errors.New("plain"),
		"deadline":          context.DeadlineExceeded,
		"cancelled":         context.Canceled,
		"wrapped deadline":  fmt.Errorf("query: %w", context.DeadlineExceeded),
		"unknown pool text": errors.New("closed pool"),
	}
	for name, err := range cases {
		got := mapError(err)
		assert.Equal(t, err, got, name)
		assert.NotErrorIs(t, got, app.ErrUnavailable, name)
		assert.NotErrorIs(t, got, app.ErrConflict, name)
	}
}

func TestUniqueViolation(t *testing.T) {
	t.Parallel()
	assert.True(t, isUniqueViolation(&pgconn.PgError{Code: "23505", ConstraintName: "c"}, "c"))
	assert.False(t, isUniqueViolation(&pgconn.PgError{Code: "23505", ConstraintName: "other"}, "c"))
	assert.False(t, isUniqueViolation(errors.New("x"), "c"))
}

func TestNewPoolValidation(t *testing.T) {
	t.Parallel()
	_, err := NewPool(context.Background(), Config{URL: "postgres://%%%"})
	require.Error(t, err)
	_, err = NewPool(context.Background(), Config{URL: "postgres://u:p@localhost:1/db", MaxConns: 0})
	require.Error(t, err, "pgxpool refuses MaxConns < 1")
	pool, err := NewPool(context.Background(), Config{URL: "postgres://u:p@localhost:1/db", MaxConns: 1, LockTimeout: time.Second})
	require.NoError(t, err, "the pool is lazy")
	pool.Close()
}

func TestNewPoolDefaultsAndOverrides(t *testing.T) {
	t.Parallel()
	pool, err := NewPool(context.Background(), Config{URL: "postgres://u:p@localhost:1/db", MaxConns: 3, LockTimeout: time.Second, StatementTimeout: 2 * time.Second})
	require.NoError(t, err)
	defer pool.Close()
	cfg := pool.Config()
	assert.Equal(t, int32(3), cfg.MaxConns)
	assert.Equal(t, int32(DefaultMinConns), cfg.MinConns)
	assert.Equal(t, DefaultMaxConnLifetime, cfg.MaxConnLifetime)
	assert.Equal(t, DefaultMaxConnIdleTime, cfg.MaxConnIdleTime)
	assert.Equal(t, DefaultHealthCheckPeriod, cfg.HealthCheckPeriod)
	assert.Equal(t, "1000", cfg.ConnConfig.RuntimeParams["lock_timeout"])
	assert.Equal(t, "2000", cfg.ConnConfig.RuntimeParams["statement_timeout"])
	assert.Equal(t, "wallet-service", cfg.ConnConfig.RuntimeParams["application_name"])

	custom, err := NewPool(context.Background(), Config{URL: "postgres://u:p@localhost:1/db", MaxConns: 3, MinConns: 2,
		MaxConnLifetime: time.Minute, MaxConnIdleTime: 2 * time.Minute, HealthCheckPeriod: 3 * time.Minute})
	require.NoError(t, err)
	defer custom.Close()
	cfg = custom.Config()
	assert.Equal(t, int32(2), cfg.MinConns)
	assert.Equal(t, time.Minute, cfg.MaxConnLifetime)
	assert.Equal(t, 2*time.Minute, cfg.MaxConnIdleTime)
	assert.Equal(t, 3*time.Minute, cfg.HealthCheckPeriod)
}
