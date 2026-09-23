package postgres

import (
	"context"

	"github.com/google/uuid"

	"github.com/Pantani/backend-challenge-go/internal/app"
	"github.com/Pantani/backend-challenge-go/internal/domain/money"
	"github.com/Pantani/backend-challenge-go/internal/domain/wallet"
)

const walletColumns = `id, player_id, currency, balance_minor, version, created_at, updated_at`

type walletRepo struct{ db dbtx }

// Create inserts the wallet; a second wallet of the same player and currency
// is ErrWalletExists.
func (r walletRepo) Create(ctx context.Context, w *wallet.Wallet) error {
	_, err := r.db.Exec(ctx, `INSERT INTO wallets (`+walletColumns+`) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		w.ID(), w.PlayerID(), string(w.Currency()), w.Balance().Minor(), w.Version(), w.CreatedAt(), w.UpdatedAt())
	if isUniqueViolation(err, "wallets_player_currency_key") {
		return app.ErrWalletExists
	}
	return mapError(err)
}

// GetForUpdate reads the wallet under a row lock (SELECT ... FOR UPDATE) held
// until the surrounding transaction ends, serialising the writers of a
// wallet; waiting longer than lock_timeout is ErrConflict.
func (r walletRepo) GetForUpdate(ctx context.Context, id uuid.UUID) (*wallet.Wallet, error) {
	return getWallet(ctx, r.db, `SELECT `+walletColumns+` FROM wallets WHERE id = $1 FOR UPDATE`, id)
}

// Save is a conditional update on the version read under the row lock: even
// if a caller forgot the lock, a stale writer gets ErrConflict instead of
// overwriting a committed balance (no lost updates). A missing wallet is
// also ErrConflict: it cannot be told apart from a stale version by the
// UPDATE alone, and GetForUpdate reported ErrWalletNotFound before.
func (r walletRepo) Save(ctx context.Context, w *wallet.Wallet, expectedVersion int64) error {
	tag, err := r.db.Exec(ctx,
		`UPDATE wallets SET balance_minor = $2, version = $3, updated_at = $4 WHERE id = $1 AND version = $5`,
		w.ID(), w.Balance().Minor(), w.Version(), w.UpdatedAt(), expectedVersion)
	if err != nil {
		return mapError(err)
	}
	if tag.RowsAffected() != 1 {
		return app.ErrConflict
	}
	return nil
}

// getWallet runs a single-row walletColumns query and rehydrates the wallet;
// no row is ErrWalletNotFound.
func getWallet(ctx context.Context, db dbtx, query string, id uuid.UUID) (*wallet.Wallet, error) {
	var (
		s        wallet.Snapshot
		currency string
		minor    int64
	)
	err := db.QueryRow(ctx, query, id).Scan(&s.ID, &s.PlayerID, &currency, &minor, &s.Version, &s.CreatedAt, &s.UpdatedAt)
	if err != nil {
		return nil, notFound(err, app.ErrWalletNotFound)
	}
	if s.Balance, err = money.FromMinor(minor, money.Currency(currency)); err != nil {
		return nil, err
	}
	return wallet.Rehydrate(s)
}
