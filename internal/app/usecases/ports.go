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
	InsertInboxMessage(ctx context.Context, tx pgx.Tx, consumerName, messageID string) (bool, error)
	FindTransactionByExternalIDInTx(ctx context.Context, tx pgx.Tx, providerID, externalTxID string) (*domain.WagerTransaction, error)
	HasProcessedReversalInTx(ctx context.Context, tx pgx.Tx, providerID, referenceExtID string) (bool, error)
	GetPendingReferenceTxIDsInTx(ctx context.Context, tx pgx.Tx, limit int) ([]uuid.UUID, error)
	FindTransactionByIDInTx(ctx context.Context, tx pgx.Tx, id uuid.UUID) (*domain.WagerTransaction, error)

	// Queries
	FindWalletByID(ctx context.Context, id uuid.UUID) (*domain.Wallet, error)
	FindTransactionByID(ctx context.Context, id uuid.UUID) (*domain.WagerTransaction, error)
	FindTransactionByExternalID(ctx context.Context, providerID, externalTxID string) (*domain.WagerTransaction, error)
	GetLedger(ctx context.Context, walletID uuid.UUID, cursor string, limit int) ([]domain.Movement, string, error)
	ReconcileWallet(ctx context.Context, walletID uuid.UUID) (domain.Money, domain.Money, int, error)
}
