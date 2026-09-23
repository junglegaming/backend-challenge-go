package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/Pantani/backend-challenge-go/internal/app"
	"github.com/Pantani/backend-challenge-go/internal/domain/money"
	"github.com/Pantani/backend-challenge-go/internal/domain/wager"
)

const transactionColumns = `id, origin, kind, status, wallet_id, player_id, amount_minor, currency,
	provider_id, external_transaction_id, idempotency_key, payload_hash, round_id, game_id,
	reference_external_transaction_id, reference_transaction_id, failure_code, result_balance_minor,
	result_currency, attempts, next_attempt_at, correlation_id, created_at, updated_at`

type transactionRepo struct{ db dbtx }

// Create inserts a concluded (or deferred) transaction; a reused external id
// or idempotency key is ErrConflict through the partial unique indexes.
func (r transactionRepo) Create(ctx context.Context, t *wager.Transaction) error {
	ext := t.External()
	resultMinor, resultCurrency := resultColumns(t)
	_, err := r.db.Exec(ctx, `INSERT INTO wager_transactions (`+transactionColumns+`)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24)`,
		t.ID(), string(t.Origin()), string(t.Kind()), string(t.Status()), t.WalletID(), t.PlayerID(),
		t.Amount().Minor(), string(t.Amount().Currency()),
		nullString(ext.ProviderID), nullString(ext.ExternalID), nullString(ext.IdempotencyKey), nullString(ext.PayloadHash),
		nullString(ext.RoundID), nullString(ext.GameID), nullString(ext.ReferenceExternalID),
		nullUUID(t.ReferenceTxID()), nullString(string(t.FailureCode())), resultMinor, resultCurrency,
		t.Attempts(), nullTime(t.NextAttemptAt()), t.CorrelationID(), t.CreatedAt(), t.UpdatedAt())
	return mapError(err)
}

// Save persists a transition of the mutable columns (status, reference,
// failure, result, schedule). The guard trigger refuses to touch terminal
// rows or immutable columns, so a bug cannot rewrite history. Saving a
// transaction that was never created is ErrTransactionNotFound.
func (r transactionRepo) Save(ctx context.Context, t *wager.Transaction) error {
	resultMinor, resultCurrency := resultColumns(t)
	tag, err := r.db.Exec(ctx, `UPDATE wager_transactions
		SET status = $2, reference_transaction_id = $3, failure_code = $4, result_balance_minor = $5,
		    result_currency = $6, attempts = $7, next_attempt_at = $8, updated_at = $9
		WHERE id = $1`,
		t.ID(), string(t.Status()), nullUUID(t.ReferenceTxID()), nullString(string(t.FailureCode())),
		resultMinor, resultCurrency, t.Attempts(), nullTime(t.NextAttemptAt()), t.UpdatedAt())
	if err != nil {
		return mapError(err)
	}
	if tag.RowsAffected() != 1 {
		return app.ErrTransactionNotFound
	}
	return nil
}

// FindExisting returns the external transactions of the provider that share
// the idempotency key or the external id, for the idempotency decision.
func (r transactionRepo) FindExisting(ctx context.Context, providerID, key, externalID string) ([]*wager.Transaction, error) {
	return queryTransactions(ctx, r.db, `SELECT `+transactionColumns+` FROM wager_transactions
		WHERE origin = 'EXTERNAL' AND provider_id = $1 AND (idempotency_key = $2 OR external_transaction_id = $3)`,
		providerID, key, externalID)
}

// GetByExternal returns the provider's transaction with the external id, or
// ErrTransactionNotFound.
func (r transactionRepo) GetByExternal(ctx context.Context, providerID, externalID string) (*wager.Transaction, error) {
	return getTransactionByExternal(ctx, r.db, providerID, externalID)
}

// HasProcessedReversal reports whether a PROCESSED REFUND or ROLLBACK already
// references the transaction.
func (r transactionRepo) HasProcessedReversal(ctx context.Context, referenceTxID uuid.UUID) (bool, error) {
	var exists bool
	err := r.db.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM wager_transactions
		WHERE reference_transaction_id = $1 AND status = 'PROCESSED' AND kind IN ('REFUND', 'ROLLBACK'))`,
		referenceTxID).Scan(&exists)
	return exists, mapError(err)
}

// LockDuePending locks the transaction for the retry of a deferred operation.
// A row that is not PENDING_REFERENCE, not due yet, or already locked by
// another instance (SKIP LOCKED) is ErrNotDue.
func (r transactionRepo) LockDuePending(ctx context.Context, id uuid.UUID, now time.Time) (*wager.Transaction, error) {
	t, err := scanTransaction(r.db.QueryRow(ctx, `SELECT `+transactionColumns+` FROM wager_transactions
		WHERE id = $1 AND status = 'PENDING_REFERENCE' AND next_attempt_at <= $2
		FOR UPDATE SKIP LOCKED`, id, now))
	if err != nil {
		return nil, notFound(err, app.ErrNotDue)
	}
	return t, nil
}

// WakeDependents makes the deferred operations waiting for the given external
// transaction due now. It skips rows another instance is resolving right
// now; they are rescheduled by that instance anyway.
func (r transactionRepo) WakeDependents(ctx context.Context, providerID, externalID string, now time.Time) error {
	_, err := r.db.Exec(ctx, `UPDATE wager_transactions SET next_attempt_at = $3
		WHERE id IN (
			SELECT id FROM wager_transactions
			WHERE status = 'PENDING_REFERENCE' AND provider_id = $1
			  AND reference_external_transaction_id = $2 AND next_attempt_at > $3
			FOR UPDATE SKIP LOCKED)`, providerID, externalID, now)
	return mapError(err)
}

// getTransactionByExternal is the single-row lookup by (provider, external
// id) shared by the repository and the read side.
func getTransactionByExternal(ctx context.Context, db dbtx, providerID, externalID string) (*wager.Transaction, error) {
	t, err := scanTransaction(db.QueryRow(ctx, `SELECT `+transactionColumns+` FROM wager_transactions
		WHERE origin = 'EXTERNAL' AND provider_id = $1 AND external_transaction_id = $2`, providerID, externalID))
	if err != nil {
		return nil, notFound(err, app.ErrTransactionNotFound)
	}
	return t, nil
}

// queryTransactions runs a transactionColumns query and rehydrates every row.
func queryTransactions(ctx context.Context, db dbtx, query string, args ...any) ([]*wager.Transaction, error) {
	return collect(ctx, db, func(row pgx.CollectableRow) (*wager.Transaction, error) { return scanTransaction(row) }, query, args...)
}

// transactionRow mirrors the nullable columns of wager_transactions.
type transactionRow struct {
	s                                          wager.Snapshot
	origin, kind, status, currency             string
	amount                                     int64
	provider, external, key, hash, round, game *string
	refExternal, failure, resultCurrency       *string
	refID                                      *uuid.UUID
	result                                     *int64
	next                                       *time.Time
}

// scanTransaction reads one transactionColumns row. pgx.ErrNoRows is passed
// through unmapped so callers can pick their own not-found sentinel.
func scanTransaction(row pgx.Row) (*wager.Transaction, error) {
	var r transactionRow
	err := row.Scan(&r.s.ID, &r.origin, &r.kind, &r.status, &r.s.WalletID, &r.s.PlayerID, &r.amount, &r.currency,
		&r.provider, &r.external, &r.key, &r.hash, &r.round, &r.game, &r.refExternal, &r.refID, &r.failure, &r.result,
		&r.resultCurrency, &r.s.Attempts, &r.next, &r.s.CorrelationID, &r.s.CreatedAt, &r.s.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}
	if err != nil {
		return nil, mapError(err)
	}
	return r.toDomain()
}

// toDomain rehydrates the row. Unknown currencies, of the amount or of the
// observed result balance, are reported instead of being silently dropped.
func (r transactionRow) toDomain() (*wager.Transaction, error) {
	if wager.Status(r.status) == wager.StatusPending {
		return nil, wager.ErrInvalidTransaction
	}
	amount, err := money.FromMinor(r.amount, money.Currency(r.currency))
	if err != nil {
		return nil, err
	}
	s := r.s
	s.Origin, s.Kind, s.Status, s.Amount = wager.Origin(r.origin), wager.Kind(r.kind), wager.Status(r.status), amount
	s.External = wager.External{
		ProviderID: deref(r.provider), ExternalID: deref(r.external), IdempotencyKey: deref(r.key),
		PayloadHash: deref(r.hash), RoundID: deref(r.round), GameID: deref(r.game), ReferenceExternalID: deref(r.refExternal),
	}
	s.FailureCode = wager.FailureCode(deref(r.failure))
	s.ReferenceTxID = derefUUID(r.refID)
	s.NextAttemptAt = derefTime(r.next)
	if r.result != nil {
		if s.ResultBalance, err = money.FromMinor(*r.result, money.Currency(deref(r.resultCurrency))); err != nil {
			return nil, err
		}
	}
	return wager.Rehydrate(s)
}
