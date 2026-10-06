package usecases

import (
	"context"

	"github.com/google/uuid"

	"github.com/Lzdevmendes/backend-challenge-go/internal/domain"
)

type QueryUseCase struct {
	repo WalletRepository
}

func NewQueryUseCase(repo WalletRepository) *QueryUseCase {
	return &QueryUseCase{repo: repo}
}

func (uc *QueryUseCase) GetWallet(ctx context.Context, id uuid.UUID) (*domain.Wallet, error) {
	return uc.repo.FindWalletByID(ctx, id)
}

func (uc *QueryUseCase) GetTransaction(ctx context.Context, id uuid.UUID) (*domain.WagerTransaction, error) {
	return uc.repo.FindTransactionByID(ctx, id)
}

func (uc *QueryUseCase) GetTransactionByExternalID(ctx context.Context, providerID, externalTxID string) (*domain.WagerTransaction, error) {
	return uc.repo.FindTransactionByExternalID(ctx, providerID, externalTxID)
}

func (uc *QueryUseCase) GetLedger(ctx context.Context, walletID uuid.UUID, cursor string, limit int) ([]domain.Movement, string, error) {
	return uc.repo.GetLedger(ctx, walletID, cursor, limit)
}

func (uc *QueryUseCase) ReconcileWallet(ctx context.Context, walletID uuid.UUID) (stored domain.Money, calc domain.Money, checked int, err error) {
	return uc.repo.ReconcileWallet(ctx, walletID)
}
