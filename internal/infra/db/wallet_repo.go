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

type WalletRepository struct{}

func NewWalletRepository() *WalletRepository {
	return &WalletRepository{}
}

// FindWalletForUpdate fetches a wallet and locks the row to prevent concurrent modifications (lost updates).
func (r *WalletRepository) FindWalletForUpdate(ctx context.Context, tx pgx.Tx, playerID uuid.UUID, currencyStr string) (*domain.Wallet, error) {
	query := `
		SELECT id, player_id, currency, balance, version, created_at, updated_at
		FROM wallets
		WHERE player_id = $1 AND currency = $2
		FOR UPDATE
	`

	var idStr, pIDStr, curStr string
	var balanceUnits int64
	var version int64
	var createdAt, updatedAt time.Time

	err := tx.QueryRow(ctx, query, playerID.String(), currencyStr).Scan(&idStr, &pIDStr, &curStr, &balanceUnits, &version, &createdAt, &updatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil // Not found
		}
		return nil, fmt.Errorf("failed to fetch wallet for update: %w", err)
	}

	id := uuid.MustParse(idStr)
	pID := uuid.MustParse(pIDStr)

	currency, err := domain.ParseCurrency(curStr)
	if err != nil {
		return nil, err
	}

	balanceMoney, err := domain.MoneyFromUnits(balanceUnits, currency)
	if err != nil {
		return nil, err
	}

	wallet, err := domain.RehydrateWallet(id, pID, balanceMoney, version, createdAt, updatedAt)
	if err != nil {
		return nil, fmt.Errorf("failed to rehydrate wallet: %w", err)
	}

	return wallet, nil
}

func (r *WalletRepository) UpdateWalletBalance(ctx context.Context, tx pgx.Tx, w *domain.Wallet) error {
	query := `
		UPDATE wallets
		SET balance = $1, version = $2, updated_at = $3
		WHERE id = $4 AND version = $5
	`
	tag, err := tx.Exec(ctx, query, w.Balance().Units(), w.Version(), w.UpdatedAt(), w.ID().String(), w.Version()-1)
	if err != nil {
		return fmt.Errorf("failed to update wallet balance: %w", err)
	}

	if tag.RowsAffected() == 0 {
		return errors.New("optimistic lock failed or wallet not found")
	}

	return nil
}

func (r *WalletRepository) InsertWagerTransaction(ctx context.Context, tx pgx.Tx, t *domain.WagerTransaction) error {
	query := `
		INSERT INTO wager_transactions (
			id, provider_id, external_transaction_id, player_id, wallet_id,
			round_id, game_id, kind, amount, currency, reference_external_transaction_id,
			status, failure_code, idempotency_key, payload_hash, created_at
		) VALUES (
			$1, $2, $3, $4, $5,
			$6, $7, $8, $9, $10, $11,
			$12, $13, $14, $15, $16
		)
	`
	req := t.Request()
	providerID := req.ProviderID
	externalTxID := req.ExternalTransactionID

	if t.Origin() == domain.OriginInternal {
		providerID = "INTERNAL"
		externalTxID = t.ID().String()
	}

	var ref *string
	if req.ReferenceExternalTransactionID != "" {
		ref = &req.ReferenceExternalTransactionID
	}

	var roundID, gameID *string
	if req.RoundID != "" {
		roundID = &req.RoundID
	}
	if req.GameID != "" {
		gameID = &req.GameID
	}

	var failCode *string
	if t.FailureCode() != "" {
		fStr := string(t.FailureCode())
		failCode = &fStr
	}

	_, err := tx.Exec(ctx, query,
		t.ID().String(), providerID, externalTxID, t.PlayerID().String(), t.WalletID().String(),
		roundID, gameID, string(t.Kind()), t.Money().Units(), string(t.Money().Currency()), ref,
		string(t.Status()), failCode, req.IdempotencyKey, t.PayloadHash(), t.CreatedAt(),
	)
	if err != nil {
		return fmt.Errorf("failed to insert wager transaction: %w", err)
	}
	return nil
}

func (r *WalletRepository) UpdateWagerTransactionStatus(ctx context.Context, tx pgx.Tx, txID uuid.UUID, status domain.Status, failureCode domain.FailureCode) error {
	var failCode *string
	if failureCode != "" {
		fStr := string(failureCode)
		failCode = &fStr
	}
	query := `
		UPDATE wager_transactions
		SET status = $1, failure_code = $2
		WHERE id = $3
	`
	_, err := tx.Exec(ctx, query, string(status), failCode, txID.String())
	if err != nil {
		return fmt.Errorf("failed to update wager transaction status: %w", err)
	}
	return nil
}

func (r *WalletRepository) InsertLedgerEntry(ctx context.Context, tx pgx.Tx, entryID uuid.UUID, walletID uuid.UUID, transactionID uuid.UUID, movement domain.Movement) error {
	query := `
		INSERT INTO wallet_ledger (
			id, wallet_id, transaction_id, direction, amount,
			balance_before, balance_after, created_at
		) VALUES (
			$1, $2, $3, $4, $5,
			$6, $7, $8
		)
	`
	_, err := tx.Exec(ctx, query,
		entryID.String(), walletID.String(), transactionID.String(), string(movement.Direction), movement.Amount.Units(),
		movement.BalanceBefore.Units(), movement.BalanceAfter.Units(), time.Now().UTC(),
	)
	if err != nil {
		return fmt.Errorf("failed to insert ledger entry: %w", err)
	}
	return nil
}

func (r *WalletRepository) InsertOutboxEvent(ctx context.Context, tx pgx.Tx, eventType string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal outbox event payload: %w", err)
	}

	query := `
		INSERT INTO outbox_events (id, event_type, payload, status, created_at)
		VALUES ($1, $2, $3, 'PENDING', $4)
	`
	eventID, err := uuid.NewV7()
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, query, eventID.String(), eventType, data, time.Now().UTC())
	if err != nil {
		return fmt.Errorf("failed to insert outbox event: %w", err)
	}

	return nil
}

func (r *WalletRepository) CheckIdempotency(ctx context.Context, tx pgx.Tx, providerID, externalTxID string) (*domain.WagerTransaction, error) {
	query := `
		SELECT id, provider_id, external_transaction_id, player_id, wallet_id,
		       round_id, game_id, kind, amount, currency, reference_external_transaction_id,
		       status, failure_code, idempotency_key, payload_hash, created_at
		FROM wager_transactions
		WHERE provider_id = $1 AND external_transaction_id = $2
	`
	var snap domain.TransactionSnapshot
	var roundID, gameID, ref, failCode *string
	var amountUnits int64
	var curStr, kindStr, statusStr string
	var idStr, pIDStr, wIDStr string

	err := tx.QueryRow(ctx, query, providerID, externalTxID).Scan(
		&idStr, &snap.Request.ProviderID, &snap.Request.ExternalTransactionID, &pIDStr, &wIDStr,
		&roundID, &gameID, &kindStr, &amountUnits, &curStr, &ref,
		&statusStr, &failCode, &snap.Request.IdempotencyKey, &snap.PayloadHash, &snap.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil // Not found
		}
		return nil, fmt.Errorf("failed to check idempotency: %w", err)
	}

	snap.ID = uuid.MustParse(idStr)
	snap.PlayerID = uuid.MustParse(pIDStr)
	snap.WalletID = uuid.MustParse(wIDStr)

	snap.Request.PlayerID = snap.PlayerID
	snap.Request.WalletID = snap.WalletID

	snap.Kind = domain.Kind(kindStr)
	snap.Request.Kind = snap.Kind
	snap.Status = domain.Status(statusStr)
	snap.Origin = domain.OriginExternal

	if roundID != nil {
		snap.Request.RoundID = *roundID
	}
	if gameID != nil {
		snap.Request.GameID = *gameID
	}
	if ref != nil {
		snap.Request.ReferenceExternalTransactionID = *ref
	}
	if failCode != nil {
		snap.FailureCode = domain.FailureCode(*failCode)
	}

	currency, err := domain.ParseCurrency(curStr)
	if err != nil {
		return nil, err
	}

	m, err := domain.MoneyFromUnits(amountUnits, currency)
	if err != nil {
		return nil, err
	}
	snap.Money = m
	snap.Request.Money = m

	return domain.RehydrateTransaction(snap)
}
