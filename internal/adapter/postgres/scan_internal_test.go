package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Pantani/backend-challenge-go/internal/app"
	"github.com/Pantani/backend-challenge-go/internal/domain/wallet"
)

var errBoom = errors.New("boom")

type failingRow struct{}

func (failingRow) Scan(...any) error { return errBoom }

// failingDB fails every statement, as a dropped connection would.
type failingDB struct{}

func (failingDB) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errBoom
}
func (failingDB) Query(context.Context, string, ...any) (pgx.Rows, error) { return nil, errBoom }
func (failingDB) QueryRow(context.Context, string, ...any) pgx.Row        { return failingRow{} }

// noRowsDB answers every statement with an empty result: Exec touches no
// row, Query returns no rows, QueryRow reports pgx.ErrNoRows.
type noRowsDB struct{}

func (noRowsDB) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.NewCommandTag("UPDATE 0"), nil
}
func (noRowsDB) Query(context.Context, string, ...any) (pgx.Rows, error) { return emptyRows{}, nil }
func (noRowsDB) QueryRow(context.Context, string, ...any) pgx.Row        { return noRow{} }

type noRow struct{}

func (noRow) Scan(...any) error { return pgx.ErrNoRows }

// emptyRows is a pgx.Rows with no rows; only the methods CollectRows uses
// are implemented (the embedded nil interface panics on the others).
type emptyRows struct{ pgx.Rows }

func (emptyRows) Close()                                       {}
func (emptyRows) Err() error                                   { return nil }
func (emptyRows) Next() bool                                   { return false }
func (emptyRows) CommandTag() pgconn.CommandTag                { return pgconn.NewCommandTag("SELECT 0") }
func (emptyRows) Values() ([]any, error)                       { return nil, nil }
func (emptyRows) RawValues() [][]byte                          { return nil }
func (emptyRows) FieldDescriptions() []pgconn.FieldDescription { return nil }

// brokenRows fails while iterating, as a connection dropped mid-result.
type brokenRows struct{ emptyRows }

func (brokenRows) Err() error { return pgconn.ErrConnClosed }

type brokenRowsDB struct{ noRowsDB }

func (brokenRowsDB) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return brokenRows{}, nil
}

func TestScanFailures(t *testing.T) {
	t.Parallel()
	_, err := scanLedgerRow(failingRow{})
	require.ErrorIs(t, err, errBoom)
	_, err = scanTransaction(failingRow{})
	require.ErrorIs(t, err, errBoom)
	_, err = queryTransactions(context.Background(), failingDB{}, "SELECT 1")
	require.ErrorIs(t, err, errBoom)
	_, err = scanTransaction(noRow{})
	require.ErrorIs(t, err, pgx.ErrNoRows, "no rows is passed through for the caller's sentinel")
}

func TestCollect(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	scan := func(row pgx.CollectableRow) (int, error) { var n int; return n, row.Scan(&n) }
	out, err := collect(ctx, noRowsDB{}, scan, "SELECT 1")
	require.NoError(t, err)
	assert.Empty(t, out)
	_, err = collect(ctx, failingDB{}, scan, "SELECT 1")
	require.ErrorIs(t, err, errBoom, "query failure")
	_, err = collect(ctx, brokenRowsDB{}, scan, "SELECT 1")
	require.ErrorIs(t, err, app.ErrUnavailable, "rows.Err is classified")
}

func TestNotFoundSentinels(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, err := getWallet(ctx, noRowsDB{}, "SELECT 1", uuid.New())
	require.ErrorIs(t, err, app.ErrWalletNotFound)
	_, err = getTransactionByExternal(ctx, noRowsDB{}, "p", "e")
	require.ErrorIs(t, err, app.ErrTransactionNotFound)
	_, err = transactionRepo{db: noRowsDB{}}.LockDuePending(ctx, uuid.New(), time.Now())
	require.ErrorIs(t, err, app.ErrNotDue)
	_, err = getWallet(ctx, failingDB{}, "SELECT 1", uuid.New())
	require.ErrorIs(t, err, errBoom, "other failures are not disguised as not found")
}

func TestRowsAffectedGuards(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	w, _, err := wallet.Open(wallet.OpenParams{ID: uuid.New(), PlayerID: uuid.New(), InitialBalance: mustMoney(t, 100, "BRL"),
		OpeningTxID: uuid.New(), OpeningEntryID: uuid.New(), Now: time.Now()})
	require.NoError(t, err)
	require.ErrorIs(t, walletRepo{db: noRowsDB{}}.Save(ctx, w, 1), app.ErrConflict)
	require.ErrorIs(t, transactionRepo{db: noRowsDB{}}.Save(ctx, opening(t)), app.ErrTransactionNotFound)
	err = inboxRepo{db: noRowsDB{}}.Complete(ctx, "consumer", "m-1", uuid.Nil, time.Now())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "consumer/m-1")
	assert.False(t, app.IsTransient(err), "a consumer bug is not retried")

	require.ErrorIs(t, walletRepo{db: failingDB{}}.Save(ctx, w, 1), errBoom)
	require.ErrorIs(t, transactionRepo{db: failingDB{}}.Save(ctx, opening(t)), errBoom)
	require.ErrorIs(t, inboxRepo{db: failingDB{}}.Complete(ctx, "c", "m", uuid.Nil, time.Now()), errBoom)
}

func TestIgnoreTxClosed(t *testing.T) {
	t.Parallel()
	assert.NoError(t, ignoreTxClosed(nil))
	assert.NoError(t, ignoreTxClosed(pgx.ErrTxClosed))
	assert.ErrorIs(t, ignoreTxClosed(&pgconn.PgError{Code: "57P01"}), app.ErrUnavailable)
}
