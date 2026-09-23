package postgres

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Pantani/backend-challenge-go/internal/app"
	"github.com/Pantani/backend-challenge-go/internal/domain/money"
	"github.com/Pantani/backend-challenge-go/internal/domain/wager"
	"github.com/Pantani/backend-challenge-go/internal/domain/wallet"
)

// Queries implements app.Queries on the pool (no locks).
type Queries struct {
	pool *pgxpool.Pool
}

// NewQueries builds the read side.
func NewQueries(pool *pgxpool.Pool) *Queries { return &Queries{pool: pool} }

// GetWallet implements app.Queries.
func (q *Queries) GetWallet(ctx context.Context, id uuid.UUID) (*wallet.Wallet, error) {
	return getWallet(ctx, q.pool, `SELECT `+walletColumns+` FROM wallets WHERE id = $1`, id)
}

// ListLedger implements app.Queries.
func (q *Queries) ListLedger(ctx context.Context, walletID uuid.UUID, afterSeq int64, limit int) ([]app.LedgerRow, error) {
	return collect(ctx, q.pool, func(row pgx.CollectableRow) (app.LedgerRow, error) { return scanLedgerRow(row) },
		`SELECT `+ledgerColumns+` FROM ledger_entries WHERE wallet_id = $1 AND seq > $2 ORDER BY seq LIMIT $3`,
		walletID, afterSeq, limit)
}

// GetTransaction implements app.Queries.
func (q *Queries) GetTransaction(ctx context.Context, id uuid.UUID) (*wager.Transaction, error) {
	t, err := scanTransaction(q.pool.QueryRow(ctx, `SELECT `+transactionColumns+` FROM wager_transactions WHERE id = $1`, id))
	if err != nil {
		return nil, notFound(err, app.ErrTransactionNotFound)
	}
	return t, nil
}

// GetTransactionByExternal implements app.Queries.
func (q *Queries) GetTransactionByExternal(ctx context.Context, providerID, externalID string) (*wager.Transaction, error) {
	return getTransactionByExternal(ctx, q.pool, providerID, externalID)
}

// Reconcile reads the stored balance and the ledger sums in a single
// statement, i.e. in one consistent snapshot.
func (q *Queries) Reconcile(ctx context.Context, walletID uuid.UUID) (app.ReconciliationSnapshot, error) {
	var (
		snap     app.ReconciliationSnapshot
		stored   int64
		currency string
	)
	err := q.pool.QueryRow(ctx, `SELECT w.balance_minor, w.currency,
			COALESCE(SUM(CASE l.direction WHEN 'CREDIT' THEN l.amount_minor
				ELSE -l.amount_minor END), 0)::BIGINT,
			COUNT(l.id)
		FROM wallets w LEFT JOIN ledger_entries l ON l.wallet_id = w.id
		WHERE w.id = $1 GROUP BY w.id`, walletID).Scan(&stored, &currency, &snap.NetMinor, &snap.Entries)
	if err != nil {
		return snap, notFound(err, app.ErrWalletNotFound)
	}
	snap.Stored, err = money.FromMinor(stored, money.Currency(currency))
	return snap, err
}

// ListDuePending implements app.Queries.
func (q *Queries) ListDuePending(ctx context.Context, now time.Time, limit int) ([]app.DueTransaction, error) {
	return collect(ctx, q.pool, func(row pgx.CollectableRow) (app.DueTransaction, error) {
		var d app.DueTransaction
		return d, row.Scan(&d.ID, &d.WalletID)
	}, `SELECT id, wallet_id FROM wager_transactions
		WHERE status = 'PENDING_REFERENCE' AND next_attempt_at <= $1 ORDER BY next_attempt_at LIMIT $2`, now, limit)
}
