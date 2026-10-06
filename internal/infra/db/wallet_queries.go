package db

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/Lzdevmendes/backend-challenge-go/internal/domain"
)

func (r *WalletRepository) FindWalletByID(ctx context.Context, id uuid.UUID) (*domain.Wallet, error) {
	query := `
		SELECT id, player_id, currency, balance, version, created_at, updated_at
		FROM wallets
		WHERE id = $1
	`

	var idStr, pIDStr, curStr string
	var balanceUnits int64
	var version int64
	var createdAt, updatedAt time.Time

	err := r.pool.QueryRow(ctx, query, id.String()).Scan(&idStr, &pIDStr, &curStr, &balanceUnits, &version, &createdAt, &updatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to find wallet: %w", err)
	}

	wID := uuid.MustParse(idStr)
	pID := uuid.MustParse(pIDStr)
	currency, _ := domain.ParseCurrency(curStr)
	balanceMoney, _ := domain.MoneyFromUnits(balanceUnits, currency)

	return domain.RehydrateWallet(wID, pID, balanceMoney, version, createdAt, updatedAt)
}

func (r *WalletRepository) FindTransactionByID(ctx context.Context, id uuid.UUID) (*domain.WagerTransaction, error) {
	query := `
		SELECT id, provider_id, external_transaction_id, player_id, wallet_id,
		       round_id, game_id, kind, amount, currency, reference_external_transaction_id,
		       status, failure_code, idempotency_key, payload_hash, created_at
		FROM wager_transactions
		WHERE id = $1
	`
	return r.scanTransaction(ctx, query, id.String())
}

func (r *WalletRepository) FindTransactionByExternalID(ctx context.Context, providerID, externalTxID string) (*domain.WagerTransaction, error) {
	query := `
		SELECT id, provider_id, external_transaction_id, player_id, wallet_id,
		       round_id, game_id, kind, amount, currency, reference_external_transaction_id,
		       status, failure_code, idempotency_key, payload_hash, created_at
		FROM wager_transactions
		WHERE provider_id = $1 AND external_transaction_id = $2
	`
	return r.scanTransaction(ctx, query, providerID, externalTxID)
}

func (r *WalletRepository) scanTransaction(ctx context.Context, query string, args ...any) (*domain.WagerTransaction, error) {
	var snap domain.TransactionSnapshot
	var roundID, gameID, ref, failCode *string
	var amountUnits int64
	var curStr, kindStr, statusStr string
	var idStr, pIDStr, wIDStr string

	err := r.pool.QueryRow(ctx, query, args...).Scan(
		&idStr, &snap.Request.ProviderID, &snap.Request.ExternalTransactionID, &pIDStr, &wIDStr,
		&roundID, &gameID, &kindStr, &amountUnits, &curStr, &ref,
		&statusStr, &failCode, &snap.Request.IdempotencyKey, &snap.PayloadHash, &snap.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to query transaction: %w", err)
	}

	snap.ID = uuid.MustParse(idStr)
	snap.PlayerID = uuid.MustParse(pIDStr)
	snap.WalletID = uuid.MustParse(wIDStr)

	snap.Request.PlayerID = snap.PlayerID
	snap.Request.WalletID = snap.WalletID

	snap.Kind = domain.Kind(kindStr)
	snap.Request.Kind = snap.Kind
	snap.Status = domain.Status(statusStr)

	if snap.Request.ProviderID == "INTERNAL" {
		snap.Origin = domain.OriginInternal
	} else {
		snap.Origin = domain.OriginExternal
	}

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

	currency, _ := domain.ParseCurrency(curStr)
	m, _ := domain.MoneyFromUnits(amountUnits, currency)
	snap.Money = m
	snap.Request.Money = m

	return domain.RehydrateTransaction(snap)
}

func (r *WalletRepository) GetLedger(ctx context.Context, walletID uuid.UUID, cursor string, limit int) ([]domain.Movement, string, error) {
	query := `
		SELECT id, direction, amount, balance_before, balance_after, created_at
		FROM wallet_ledger
		WHERE wallet_id = $1
	`
	args := []any{walletID.String()}

	if cursor != "" {
		// Use simple decode
		decoded, err := base64.StdEncoding.DecodeString(cursor)
		if err == nil {
			query += " AND created_at > $2"
			args = append(args, string(decoded))
		}
	}

	query += fmt.Sprintf(" ORDER BY created_at ASC LIMIT %d", limit+1)

	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	// We only need the movements. Actually the challenge asks for movements but the interface returns domain.Movement.
	// We didn't fully model LedgerEntry to be returned but we can just return domain.Movement list
	var movements []domain.Movement
	var lastCreatedAt time.Time

	wallet, _ := r.FindWalletByID(ctx, walletID)
	if wallet == nil {
		return nil, "", fmt.Errorf("wallet not found")
	}

	for rows.Next() {
		var idStr, direction string
		var amount, balanceBefore, balanceAfter int64
		var createdAt time.Time

		if err := rows.Scan(&idStr, &direction, &amount, &balanceBefore, &balanceAfter, &createdAt); err != nil {
			return nil, "", err
		}

		mAmount, _ := domain.MoneyFromUnits(amount, wallet.Currency())
		mBefore, _ := domain.MoneyFromUnits(balanceBefore, wallet.Currency())
		mAfter, _ := domain.MoneyFromUnits(balanceAfter, wallet.Currency())

		movements = append(movements, domain.Movement{
			Direction:     domain.Direction(direction),
			Amount:        mAmount,
			BalanceBefore: mBefore,
			BalanceAfter:  mAfter,
			// version is not in ledger table right now
		})
		lastCreatedAt = createdAt
	}

	nextCursor := ""
	if len(movements) > limit {
		movements = movements[:limit]
		nextCursor = base64.StdEncoding.EncodeToString([]byte(lastCreatedAt.Format(time.RFC3339Nano)))
	}

	return movements, nextCursor, nil
}

func (r *WalletRepository) ReconcileWallet(ctx context.Context, walletID uuid.UUID) (domain.Money, domain.Money, int, error) {
	wallet, err := r.FindWalletByID(ctx, walletID)
	if err != nil || wallet == nil {
		return domain.Money{}, domain.Money{}, 0, err
	}

	query := `
		SELECT direction, amount
		FROM wallet_ledger
		WHERE wallet_id = $1
	`
	rows, err := r.pool.Query(ctx, query, walletID.String())
	if err != nil {
		return domain.Money{}, domain.Money{}, 0, err
	}
	defer rows.Close()

	var calculatedUnits int64
	checked := 0

	for rows.Next() {
		var direction string
		var amount int64
		if err := rows.Scan(&direction, &amount); err != nil {
			return domain.Money{}, domain.Money{}, 0, err
		}
		if direction == "CREDIT" {
			calculatedUnits += amount
		} else {
			calculatedUnits -= amount
		}
		checked++
	}

	calcMoney, _ := domain.MoneyFromUnits(calculatedUnits, wallet.Currency())

	return wallet.Balance(), calcMoney, checked, nil
}
