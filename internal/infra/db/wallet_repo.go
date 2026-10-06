package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/Lzdevmendes/backend-challenge-go/internal/domain"
)

type WalletRepository struct {
	db *PostgresDB
}

func NewWalletRepository(db *PostgresDB) *WalletRepository {
	return &WalletRepository{db: db}
}

func (r *WalletRepository) FindWalletForUpdate(ctx context.Context, tx pgx.Tx, id uuid.UUID) (*domain.Wallet, error) {
	query := `SELECT id, player_id, currency, balance, version, created_at, updated_at 
              FROM wallets 
              WHERE id = $1 
              FOR UPDATE`
	
	var fetchedID, pID uuid.UUID
	var curStr string
	var balanceUnits int64
	var version int64
	var createdAt, updatedAt time.Time

	err := tx.QueryRow(ctx, query, id).Scan(
		&fetchedID, &pID, &curStr, &balanceUnits, &version, &createdAt, &updatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil // Return nil if not found
		}
		return nil, fmt.Errorf("failed to lock wallet: %w", err)
	}

	currency, _ := domain.ParseCurrency(curStr)
	balanceMoney, _ := domain.MoneyFromUnits(balanceUnits, currency)

	w, err := domain.RehydrateWallet(fetchedID, pID, balanceMoney, version, createdAt, updatedAt)
	if err != nil {
		return nil, fmt.Errorf("failed to rehydrate wallet: %w", err)
	}
	return w, nil
}

func (r *WalletRepository) CreateWallet(ctx context.Context, tx pgx.Tx, w *domain.Wallet) error {
	query := `INSERT INTO wallets (id, player_id, currency, balance, version, created_at, updated_at) 
              VALUES ($1, $2, $3, $4, $5, $6, $7)`
	
	_, err := tx.Exec(ctx, query,
		w.ID(), w.PlayerID(), string(w.Balance().Currency()), w.Balance().Units(), w.Version(), time.Now().UTC(), time.Now().UTC(),
	)
	return err
}

func (r *WalletRepository) UpdateWalletBalance(ctx context.Context, tx pgx.Tx, w *domain.Wallet) error {
	query := `UPDATE wallets SET balance = $1, version = $2, updated_at = $3 
              WHERE id = $4 AND version = $5`
	
	cmd, err := tx.Exec(ctx, query,
		w.Balance().Units(), w.Version(), time.Now().UTC(), w.ID(), w.Version()-1,
	)
	if err != nil {
		return fmt.Errorf("failed to update wallet: %w", err)
	}
	if cmd.RowsAffected() == 0 {
		return fmt.Errorf("optimistic locking failed for wallet update")
	}
	return nil
}

func (r *WalletRepository) InsertWagerTransaction(ctx context.Context, tx pgx.Tx, t *domain.WagerTransaction) error {
	query := `
		INSERT INTO transactions (
			id, origin, provider_id, external_id, payload_hash, wallet_id, player_id,
			kind, amount, currency, reference_id, resolved_reference_id,
			status, failure_code, result_balance, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
	`
	
	req := t.Request()
	var providerID, externalID, refID *string
	if req.ProviderID != "" { providerID = &req.ProviderID }
	if req.ExternalTransactionID != "" { externalID = &req.ExternalTransactionID }
	if req.ReferenceExternalTransactionID != "" { refID = &req.ReferenceExternalTransactionID }
	
	var resolvedRefID *uuid.UUID
	if t.ResolvedReferenceID() != uuid.Nil {
		rrID := t.ResolvedReferenceID()
		resolvedRefID = &rrID
	}

	var failCode *string
	if t.FailureCode() != "" {
		fc := string(t.FailureCode())
		failCode = &fc
	}

	var resBal *int64
	if t.ResultBalance().IsInitialized() {
		units := t.ResultBalance().Units()
		resBal = &units
	}

	_, err := tx.Exec(ctx, query,
		t.ID(), string(t.Origin()), providerID, externalID, t.PayloadHash(), t.WalletID(), t.PlayerID(),
		string(t.Kind()), t.Money().Units(), string(t.Money().Currency()), refID, resolvedRefID,
		string(t.Status()), failCode, resBal, t.CreatedAt(), t.UpdatedAt(),
	)
	return err
}

func (r *WalletRepository) UpdateWagerTransactionStatus(ctx context.Context, tx pgx.Tx, txID uuid.UUID, status domain.Status, failureCode domain.FailureCode) error {
	query := `UPDATE transactions SET status = $1, failure_code = $2, updated_at = $3 WHERE id = $4`
	var fc *string
	if failureCode != "" {
		s := string(failureCode)
		fc = &s
	}
	
	cmd, err := tx.Exec(ctx, query, string(status), fc, time.Now().UTC(), txID)
	if err != nil {
		return err
	}
	if cmd.RowsAffected() == 0 {
		return errors.New("transaction not found")
	}
	return nil
}

func (r *WalletRepository) UpdateWagerTransactionProcessed(ctx context.Context, tx pgx.Tx, t *domain.WagerTransaction) error {
	query := `UPDATE transactions 
              SET status = $1, result_balance = $2, resolved_reference_id = $3, updated_at = $4 
              WHERE id = $5`
	
	var resBal *int64
	if t.ResultBalance().IsInitialized() {
		units := t.ResultBalance().Units()
		resBal = &units
	}

	var resolvedRefID *uuid.UUID
	if t.ResolvedReferenceID() != uuid.Nil {
		rrID := t.ResolvedReferenceID()
		resolvedRefID = &rrID
	}

	cmd, err := tx.Exec(ctx, query, string(t.Status()), resBal, resolvedRefID, time.Now().UTC(), t.ID())
	if err != nil {
		return err
	}
	if cmd.RowsAffected() == 0 {
		return errors.New("transaction not found")
	}
	return nil
}

func (r *WalletRepository) InsertLedgerEntry(ctx context.Context, tx pgx.Tx, entryID, walletID, transactionID uuid.UUID, movement domain.Movement) error {
	query := `
		INSERT INTO ledger_entries (id, wallet_id, transaction_id, direction, amount, balance_before, balance_after, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`
	_, err := tx.Exec(ctx, query,
		entryID, walletID, transactionID, string(movement.Direction), movement.Amount.Units(),
		movement.BalanceBefore.Units(), movement.BalanceAfter.Units(), time.Now().UTC(),
	)
	return err
}

func (r *WalletRepository) InsertOutboxEvent(ctx context.Context, tx pgx.Tx, eventType string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	query := `INSERT INTO outbox_events (id, event_type, payload, status, created_at) VALUES ($1, $2, $3, 'PENDING', $4)`
	eventID, err := uuid.NewV7()
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, query, eventID, eventType, data, time.Now().UTC())
	return err
}

func (r *WalletRepository) scanTransaction(row pgx.Row) (*domain.WagerTransaction, error) {
	var snap domain.TransactionSnapshot
	var provID, extID, refID, resRefID, failCode *string
	var amountUnits int64
	var resBal *int64
	var curStr, originStr, kindStr, statusStr string
	
	err := row.Scan(
		&snap.ID, &originStr, &provID, &extID, &snap.PayloadHash, &snap.WalletID, &snap.PlayerID,
		&kindStr, &amountUnits, &curStr, &refID, &resRefID,
		&statusStr, &failCode, &resBal, &snap.CreatedAt, &snap.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}

	snap.Origin = domain.Origin(originStr)
	snap.Kind = domain.Kind(kindStr)
	snap.Status = domain.Status(statusStr)
	
	if provID != nil { snap.Request.ProviderID = *provID }
	if extID != nil { snap.Request.ExternalTransactionID = *extID }
	if refID != nil { snap.Request.ReferenceExternalTransactionID = *refID }
	if failCode != nil { snap.FailureCode = domain.FailureCode(*failCode) }
	if resRefID != nil { snap.ResolvedReferenceID = uuid.MustParse(*resRefID) }

	snap.Request.PlayerID = snap.PlayerID
	snap.Request.WalletID = snap.WalletID
	snap.Request.Kind = snap.Kind

	currency, _ := domain.ParseCurrency(curStr)
	snap.Money, _ = domain.MoneyFromUnits(amountUnits, currency)
	snap.Request.Money = snap.Money

	if resBal != nil {
		snap.ResultBalance, _ = domain.MoneyFromUnits(*resBal, currency)
	}

	return domain.RehydrateTransaction(snap)
}

func (r *WalletRepository) CheckIdempotency(ctx context.Context, tx pgx.Tx, providerID, externalTxID string) (*domain.WagerTransaction, error) {
	query := `SELECT id, origin, provider_id, external_id, payload_hash, wallet_id, player_id,
              kind, amount, currency, reference_id, resolved_reference_id,
              status, failure_code, result_balance, created_at, updated_at
              FROM transactions
              WHERE provider_id = $1 AND external_id = $2 LIMIT 1`
	return r.scanTransaction(tx.QueryRow(ctx, query, providerID, externalTxID))
}

func (r *WalletRepository) InsertInboxMessage(ctx context.Context, tx pgx.Tx, consumerName, messageID string) (bool, error) {
	// Let's use the inbox_events table which has message_id and hash. consumerName can be merged into message_id or hash for uniqueness if needed, but messageID from SQS is usually globally unique.
	query := `INSERT INTO inbox_events (id, message_id, hash, processed_at) VALUES ($1, $2, $3, $4) ON CONFLICT (message_id) DO NOTHING`
	id, _ := uuid.NewV7()
	tag, err := tx.Exec(ctx, query, id, consumerName+":"+messageID, "", time.Now().UTC())
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

func (r *WalletRepository) FindTransactionByExternalIDInTx(ctx context.Context, tx pgx.Tx, providerID, externalTxID string) (*domain.WagerTransaction, error) {
	return r.CheckIdempotency(ctx, tx, providerID, externalTxID)
}

func (r *WalletRepository) HasProcessedReversalInTx(ctx context.Context, tx pgx.Tx, providerID, referenceExtID string) (bool, error) {
	query := `SELECT 1 FROM transactions 
              WHERE provider_id = $1 AND reference_id = $2 
              AND kind IN ('REFUND', 'ROLLBACK') 
              AND status = 'PROCESSED' LIMIT 1`
	var exists int
	err := tx.QueryRow(ctx, query, providerID, referenceExtID).Scan(&exists)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (r *WalletRepository) GetPendingReferenceTxIDsInTx(ctx context.Context, tx pgx.Tx, limit int) ([]uuid.UUID, error) {
	query := `SELECT id FROM transactions 
              WHERE status = 'PENDING_REFERENCE' 
              ORDER BY created_at ASC 
              FOR UPDATE SKIP LOCKED LIMIT $1`
	rows, err := tx.Query(ctx, query, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (r *WalletRepository) FindTransactionByIDInTx(ctx context.Context, tx pgx.Tx, id uuid.UUID) (*domain.WagerTransaction, error) {
	query := `SELECT id, origin, provider_id, external_id, payload_hash, wallet_id, player_id,
              kind, amount, currency, reference_id, resolved_reference_id,
              status, failure_code, result_balance, created_at, updated_at
              FROM transactions
              WHERE id = $1 LIMIT 1`
	return r.scanTransaction(tx.QueryRow(ctx, query, id))
}
