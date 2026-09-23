package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Pantani/backend-challenge-go/internal/app"
	"github.com/Pantani/backend-challenge-go/internal/domain/money"
	"github.com/Pantani/backend-challenge-go/internal/domain/wallet"
)

const ledgerColumns = `seq, id, wallet_id, transaction_id, direction, amount_minor, currency,
	balance_before_minor, balance_after_minor, created_at`

type ledgerRepo struct{ db dbtx }

// Append inserts one ledger entry. The chain trigger refuses an entry that
// does not start at the wallet's last balance or uses another currency, and
// the deferred trigger refuses the commit unless the wallet row matches.
func (r ledgerRepo) Append(ctx context.Context, e wallet.LedgerEntry) error {
	_, err := r.db.Exec(ctx, `INSERT INTO ledger_entries
		(id, wallet_id, transaction_id, direction, amount_minor, currency, balance_before_minor, balance_after_minor, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		e.ID(), e.WalletID(), e.TransactionID(), string(e.Direction()), e.Amount().Minor(),
		string(e.Amount().Currency()), e.BalanceBefore().Minor(), e.BalanceAfter().Minor(), e.CreatedAt())
	return mapError(err)
}

// scanLedgerRow reads one ledgerColumns row into a LedgerRow. A stored
// currency the domain does not know is reported, never silently zeroed.
func scanLedgerRow(row pgx.Row) (app.LedgerRow, error) {
	var (
		seq                   int64
		p                     wallet.LedgerEntryParams
		direction, currency   string
		amount, before, after int64
		createdAt             time.Time
	)
	if err := row.Scan(&seq, &p.ID, &p.WalletID, &p.TransactionID, &direction, &amount, &currency, &before, &after, &createdAt); err != nil {
		return app.LedgerRow{}, mapError(err)
	}
	p.Direction, p.CreatedAt = wallet.Direction(direction), createdAt
	c := money.Currency(currency)
	var err error
	if p.Amount, err = money.FromMinor(amount, c); err != nil {
		return app.LedgerRow{}, err
	}
	if p.BalanceBefore, err = money.FromMinor(before, c); err != nil {
		return app.LedgerRow{}, err
	}
	if p.BalanceAfter, err = money.FromMinor(after, c); err != nil {
		return app.LedgerRow{}, err
	}
	entry, err := wallet.NewLedgerEntry(p)
	return app.LedgerRow{Seq: seq, Entry: entry}, err
}
