package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/VictorM0nteiro/backend-challenge-go/internal/domain"
)

const wagerColumns = `id, provider_id, external_transaction_id, idempotency_key, request_hash, wallet_id, player_id,
	round_id, game_id, kind, amount_minor, currency, reference_external_transaction_id, reference_transaction_id,
	state, failure_code, balance_after_minor, created_at, updated_at`

// insertWagerTransaction writes an operation in its final state for this
// request. It runs inside the same transaction as the wallet change, so an
// operation that was processed exists if and only if its money moved.
func insertWagerTransaction(ctx context.Context, tx pgx.Tx, wt *domain.WagerTransaction) error {
	rec := wt.Record()

	const query = `INSERT INTO wager_transactions (` + wagerColumns + `)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)`

	_, err := tx.Exec(ctx, query,
		rec.ID, rec.ProviderID, rec.ExternalTransactionID, rec.IdempotencyKey, rec.RequestHash,
		rec.WalletID, rec.PlayerID, rec.RoundID, rec.GameID, string(rec.Kind),
		rec.Amount.AmountMinor(), rec.Amount.Currency(),
		nullableString(rec.ReferenceExternalTransactionID), nullableUUID(rec.ReferenceTransactionID),
		string(rec.State), nullableString(string(rec.FailureCode)), nullableMinor(rec.BalanceAfter),
		rec.CreatedAt, rec.UpdatedAt,
	)
	if isConstraintViolation(err, externalTransactionUnique) {
		// The same (provider, externalTransactionId) arrived under another
		// idempotency key. Nothing is applied: returning the error rolls back
		// the whole transaction, including the wallet change.
		return ErrDuplicateExternalTransaction
	}
	if err != nil {
		return fmt.Errorf("postgres: insert wager transaction: %w", err)
	}
	return nil
}

// externalTransactionUnique is the name Postgres gives the UNIQUE (provider_id,
// external_transaction_id) constraint of wager_transactions. Other unique
// indexes on the table, such as the one-reversal index, must not match.
const externalTransactionUnique = "wager_transactions_provider_id_external_transaction_id_key"

// isConstraintViolation reports whether err is a unique violation (23505) of
// the named constraint.
func isConstraintViolation(err error, constraint string) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation && pgErr.ConstraintName == constraint
}

// findWagerByExternalID loads the operation a provider sent under
// externalID. A missing row is not an error here: it is how a reversal learns
// its reference does not exist, so it returns (nil, nil).
func findWagerByExternalID(ctx context.Context, tx pgx.Tx, providerID, externalID string) (*domain.WagerTransaction, error) {
	const query = `SELECT ` + wagerColumns + ` FROM wager_transactions
		WHERE provider_id = $1 AND external_transaction_id = $2`

	rec, err := scanWagerRecord(tx.QueryRow(ctx, query, providerID, externalID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return domain.RehydrateWagerTransaction(rec), nil
}

// hasSuccessfulReversal reports whether a REFUND or ROLLBACK already moved
// money back for referenceID. It is checked after the wallet lock is held, so
// two reversals of the same bet cannot both pass it.
func hasSuccessfulReversal(ctx context.Context, tx pgx.Tx, referenceID uuid.UUID) (bool, error) {
	const query = `SELECT EXISTS (
		SELECT 1 FROM wager_transactions
		WHERE reference_transaction_id = $1
		  AND kind IN ('REFUND', 'ROLLBACK')
		  AND state = 'PROCESSED')`

	var exists bool
	if err := tx.QueryRow(ctx, query, referenceID).Scan(&exists); err != nil {
		return false, fmt.Errorf("postgres: check existing reversal: %w", err)
	}
	return exists, nil
}

func scanWagerRecord(row pgx.Row) (domain.WagerRecord, error) {
	var (
		rec                    domain.WagerRecord
		kind, state, currency  string
		amountMinor            int64
		referenceExternalID    *string
		referenceTransactionID *uuid.UUID
		failureCode            *string
		balanceAfterMinor      *int64
		createdAt, updatedAt   time.Time
	)
	err := row.Scan(
		&rec.ID, &rec.ProviderID, &rec.ExternalTransactionID, &rec.IdempotencyKey, &rec.RequestHash,
		&rec.WalletID, &rec.PlayerID, &rec.RoundID, &rec.GameID, &kind,
		&amountMinor, &currency, &referenceExternalID, &referenceTransactionID,
		&state, &failureCode, &balanceAfterMinor, &createdAt, &updatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.WagerRecord{}, err
		}
		return domain.WagerRecord{}, fmt.Errorf("postgres: scan wager transaction: %w", err)
	}

	amount, err := domain.NewMoney(amountMinor, currency)
	if err != nil {
		return domain.WagerRecord{}, fmt.Errorf("postgres: rebuild wager amount: %w", err)
	}

	rec.Kind = domain.WagerKind(kind)
	rec.Amount = amount
	rec.State = domain.WagerState(state)
	rec.CreatedAt, rec.UpdatedAt = createdAt, updatedAt
	if referenceExternalID != nil {
		rec.ReferenceExternalTransactionID = *referenceExternalID
	}
	if referenceTransactionID != nil {
		rec.ReferenceTransactionID = *referenceTransactionID
	}
	if failureCode != nil {
		rec.FailureCode = domain.FailureCode(*failureCode)
	}
	if balanceAfterMinor != nil {
		balance, err := domain.NewMoney(*balanceAfterMinor, currency)
		if err != nil {
			return domain.WagerRecord{}, fmt.Errorf("postgres: rebuild balance after: %w", err)
		}
		rec.BalanceAfter = &balance
	}
	return rec, nil
}

func nullableString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func nullableUUID(id uuid.UUID) *uuid.UUID {
	if id == uuid.Nil {
		return nil
	}
	return &id
}

func nullableMinor(m *domain.Money) *int64 {
	if m == nil {
		return nil
	}
	v := m.AmountMinor()
	return &v
}
