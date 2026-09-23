// Package postgres implements the application ports with pgx and explicit SQL.
//
// Money is stored as BIGINT minor units plus a CHAR(3) currency. Every write
// of a use case runs inside the single pgx.Tx opened by UnitOfWork.Do, which
// the repositories of that call share; reads that need no locks use the pool.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/puddle/v2"

	"github.com/Pantani/backend-challenge-go/internal/app"
)

// Pool defaults applied by NewPool when the corresponding Config field is zero.
const (
	DefaultMinConns          = 1
	DefaultMaxConnLifetime   = time.Hour
	DefaultMaxConnIdleTime   = 30 * time.Minute
	DefaultHealthCheckPeriod = time.Minute
)

// Config configures the connection pool. Zero values of the optional fields
// take the Default* constants; MaxConns must be at least 1 (pgxpool refuses
// smaller values).
type Config struct {
	// URL is the postgres:// connection string.
	URL string
	// MaxConns caps the open connections of the pool.
	MaxConns int32
	// MinConns is the number of connections the pool keeps open (and warms
	// on demand) even when idle.
	MinConns int32
	// LockTimeout bounds how long a statement waits for a row lock (the
	// session lock_timeout, sent to the server in milliseconds).
	LockTimeout time.Duration
	// StatementTimeout bounds how long a single statement may run (the
	// session statement_timeout, sent to the server in milliseconds).
	StatementTimeout time.Duration
	// MaxConnLifetime closes a connection after it has existed this long,
	// spreading reconnections behind load balancers and failovers.
	MaxConnLifetime time.Duration
	// MaxConnIdleTime closes a connection unused for this long.
	MaxConnIdleTime time.Duration
	// HealthCheckPeriod is how often the pool prunes expired connections and
	// replenishes MinConns.
	HealthCheckPeriod time.Duration
}

// NewPool builds a lazy pool (connections are opened on demand; callers
// Ping it to validate the dependency). lock_timeout and statement_timeout bound
// how long a request can wait on a wallet lock or a slow statement.
func NewPool(ctx context.Context, cfg Config) (*pgxpool.Pool, error) {
	pc, err := pgxpool.ParseConfig(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	pc.MaxConns = cfg.MaxConns
	pc.MinConns = orDefault(cfg.MinConns, DefaultMinConns)
	pc.MaxConnLifetime = orDefault(cfg.MaxConnLifetime, DefaultMaxConnLifetime)
	pc.MaxConnIdleTime = orDefault(cfg.MaxConnIdleTime, DefaultMaxConnIdleTime)
	pc.HealthCheckPeriod = orDefault(cfg.HealthCheckPeriod, DefaultHealthCheckPeriod)
	pc.ConnConfig.RuntimeParams["lock_timeout"] = fmt.Sprint(cfg.LockTimeout.Milliseconds())
	pc.ConnConfig.RuntimeParams["statement_timeout"] = fmt.Sprint(cfg.StatementTimeout.Milliseconds())
	pc.ConnConfig.RuntimeParams["application_name"] = "wallet-service"
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	return pool, nil
}

// orDefault returns v, or def when v is zero.
func orDefault[T int32 | time.Duration](v, def T) T {
	if v == 0 {
		return def
	}
	return v
}

// Ping validates connectivity, classifying failures as ErrUnavailable.
func Ping(ctx context.Context, pool *pgxpool.Pool) error {
	return mapError(pool.Ping(ctx))
}

// dbtx is satisfied by both *pgxpool.Pool and pgx.Tx.
type dbtx interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// sqlStateErrors maps individual SQLSTATE codes to application errors.
//
// Lost races (unique violation, serialization failure, deadlock, lock_timeout)
// are ErrConflict: the same request is expected to succeed when retried after
// the competing transaction finishes. Whole error classes that mean the
// server, not the request, is in trouble are handled by mapPgError:
// connection exceptions (08), insufficient resources (53, e.g. too many
// connections) and system errors (58, e.g. I/O failures) are ErrUnavailable.
//
// 57014 query_canceled is ErrUnavailable rather than ErrConflict because the
// session statement_timeout fires when the server is too slow, not when
// another transaction won a race; the operation may succeed later, but not by
// an immediate retry. 40003 statement_completion_unknown is ErrUnavailable
// for the same reason: the connection was lost mid-statement and the outcome
// must be re-read, not blindly retried.
var sqlStateErrors = map[string]error{
	pgerrcode.UniqueViolation:            app.ErrConflict,
	pgerrcode.SerializationFailure:       app.ErrConflict,
	pgerrcode.DeadlockDetected:           app.ErrConflict,
	pgerrcode.LockNotAvailable:           app.ErrConflict,    // lock_timeout
	pgerrcode.QueryCanceled:              app.ErrUnavailable, // statement_timeout
	pgerrcode.StatementCompletionUnknown: app.ErrUnavailable,
	pgerrcode.AdminShutdown:              app.ErrUnavailable,
	pgerrcode.CrashShutdown:              app.ErrUnavailable,
	pgerrcode.CannotConnectNow:           app.ErrUnavailable,
}

// mapError classifies driver errors into application errors. Context
// deadlines and cancellations are returned as they are: the caller's own
// context decided, so they are neither a database outage nor a lost race.
func mapError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return err
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return mapPgError(pgErr)
	}
	if isConnectionError(err) {
		return fmt.Errorf("%w: %w", app.ErrUnavailable, err)
	}
	return err
}

// mapPgError maps a server error by its SQLSTATE: individual codes through
// sqlStateErrors, then the classes that mean the server is unavailable. Any
// other server error (e.g. a constraint violation) is returned as is, because
// it reports a bug or a business rule and must never be retried.
func mapPgError(e *pgconn.PgError) error {
	if mapped, ok := sqlStateErrors[e.Code]; ok {
		return fmt.Errorf("%w: %w", mapped, e)
	}
	if pgerrcode.IsConnectionException(e.Code) || pgerrcode.IsInsufficientResources(e.Code) || pgerrcode.IsSystemError(e.Code) {
		return fmt.Errorf("%w: %w", app.ErrUnavailable, e)
	}
	return e
}

// isConnectionError reports client-side failures to reach or keep a
// connection: dial errors, network errors and timeouts, a closed pool and a
// closed connection.
func isConnectionError(err error) bool {
	var connErr *pgconn.ConnectError
	var netErr net.Error
	return errors.As(err, &connErr) || errors.As(err, &netErr) || pgconn.Timeout(err) ||
		errors.Is(err, puddle.ErrClosedPool) || errors.Is(err, pgconn.ErrConnClosed)
}

// isUniqueViolation reports a unique violation on the named constraint.
func isUniqueViolation(err error, constraint string) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation && pgErr.ConstraintName == constraint
}

// notFound translates pgx.ErrNoRows into the sentinel of the repository and
// classifies any other error.
func notFound(err, sentinel error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return sentinel
	}
	return mapError(err)
}

// collect runs the query and maps every row with scan, classifying driver
// errors; pgx.CollectRows closes the rows and reports rows.Err.
func collect[T any](ctx context.Context, db dbtx, scan func(pgx.CollectableRow) (T, error), query string, args ...any) ([]T, error) {
	rows, err := db.Query(ctx, query, args...)
	if err != nil {
		return nil, mapError(err)
	}
	out, err := pgx.CollectRows(rows, scan)
	if err != nil {
		return nil, mapError(err)
	}
	return out, nil
}
