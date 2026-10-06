package usecases

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/Lzdevmendes/backend-challenge-go/internal/domain"
)

type TransactionManager interface {
	RunInTransaction(ctx context.Context, fn func(ctx context.Context, tx pgx.Tx) error) error
}

type WalletRepository interface {
	FindWalletForUpdate(ctx context.Context, tx pgx.Tx, playerID uuid.UUID, currencyStr string) (*domain.Wallet, error)
	UpdateWalletBalance(ctx context.Context, tx pgx.Tx, w *domain.Wallet) error
	InsertWagerTransaction(ctx context.Context, tx pgx.Tx, t *domain.WagerTransaction) error
	UpdateWagerTransactionStatus(ctx context.Context, tx pgx.Tx, txID uuid.UUID, status domain.Status, failureCode domain.FailureCode) error
	InsertLedgerEntry(ctx context.Context, tx pgx.Tx, entryID, walletID, transactionID uuid.UUID, movement domain.Movement) error
	InsertOutboxEvent(ctx context.Context, tx pgx.Tx, eventType string, payload any) error
	CheckIdempotency(ctx context.Context, tx pgx.Tx, providerID, externalTxID string) (*domain.WagerTransaction, error)
}
