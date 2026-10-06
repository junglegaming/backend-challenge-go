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

	var fetchedID, pID uuid.UUID
	var curStr string
	var balanceUnits int64
	var version int64
	var createdAt, updatedAt time.Time

	err := r.db.Pool.QueryRow(ctx, query, id).Scan(
		&fetchedID, &pID, &curStr, &balanceUnits, &version, &createdAt, &updatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to find wallet: %w", err)
	}

	currency, _ := domain.ParseCurrency(curStr)
	balanceMoney, _ := domain.MoneyFromUnits(balanceUnits, currency)

	return domain.RehydrateWallet(fetchedID, pID, balanceMoney, version, createdAt, updatedAt)
}

func (r *WalletRepository) FindTransactionByID(ctx context.Context, id uuid.UUID) (*domain.WagerTransaction, error) {
	query := `SELECT id, origin, provider_id, external_id, payload_hash, wallet_id, player_id,
              kind, amount, currency, reference_id, resolved_reference_id,
              status, failure_code, result_balance, created_at, updated_at
              FROM transactions
              WHERE id = $1 LIMIT 1`
	return r.scanTransaction(r.db.Pool.QueryRow(ctx, query, id))
}

func (r *WalletRepository) FindTransactionByExternalID(ctx context.Context, providerID, externalTxID string) (*domain.WagerTransaction, error) {
	query := `SELECT id, origin, provider_id, external_id, payload_hash, wallet_id, player_id,
              kind, amount, currency, reference_id, resolved_reference_id,
              status, failure_code, result_balance, created_at, updated_at
              FROM transactions
              WHERE provider_id = $1 AND external_id = $2 LIMIT 1`
	return r.scanTransaction(r.db.Pool.QueryRow(ctx, query, providerID, externalTxID))
}

func (r *WalletRepository) GetLedger(ctx context.Context, walletID uuid.UUID, cursor string, limit int) ([]domain.Movement, string, error) {
	query := `
		SELECT direction, amount, balance_before, balance_after, created_at
		FROM ledger_entries
		WHERE wallet_id = $1
	`
	args := []any{walletID}

	if cursor != "" {
		decoded, err := base64.StdEncoding.DecodeString(cursor)
		if err == nil {
			query += " AND created_at > $2"
			args = append(args, string(decoded))
		}
	}

	query += fmt.Sprintf(" ORDER BY created_at ASC LIMIT %d", limit+1)

	rows, err := r.db.Pool.Query(ctx, query, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	var movements []domain.Movement
	var lastCreatedAt time.Time

	wallet, err := r.FindWalletByID(ctx, walletID)
	if err != nil || wallet == nil {
		return nil, "", fmt.Errorf("wallet not found")
	}

	for rows.Next() {
		var direction string
		var amount, balanceBefore, balanceAfter int64
		var createdAt time.Time

		if err := rows.Scan(&direction, &amount, &balanceBefore, &balanceAfter, &createdAt); err != nil {
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
		FROM ledger_entries
		WHERE wallet_id = $1
	`
	rows, err := r.db.Pool.Query(ctx, query, walletID)
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
