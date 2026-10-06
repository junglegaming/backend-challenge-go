package usecases

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/Lzdevmendes/backend-challenge-go/internal/domain"
)

type CreateWalletUseCase struct {
	txManager TransactionManager
	repo      WalletRepository
}

func NewCreateWalletUseCase(txManager TransactionManager, repo WalletRepository) *CreateWalletUseCase {
	return &CreateWalletUseCase{
		txManager: txManager,
		repo:      repo,
	}
}

type CreateWalletCommand struct {
	PlayerID uuid.UUID
	Currency string
}

func (uc *CreateWalletUseCase) Execute(ctx context.Context, cmd CreateWalletCommand) (*domain.Wallet, error) {
	currency, err := domain.ParseCurrency(cmd.Currency)
	if err != nil {
		return nil, err
	}

	zeroMoney, _ := domain.Zero(currency)
	walletID, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}

	wallet, err := domain.NewWallet(walletID, cmd.PlayerID, zeroMoney, time.Now())
	if err != nil {
		return nil, err
	}

	err = uc.txManager.RunInTransaction(ctx, func(ctx context.Context, tx pgx.Tx) error {
		// Insert Wallet directly. Since we don't have an InsertWallet in repo yet, let's assume it exists or use raw SQL here.
		// It's better to add InsertWallet to repo.
		query := `
			INSERT INTO wallets (id, player_id, currency, balance, version, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
		`
		_, err := tx.Exec(ctx, query, wallet.ID().String(), wallet.PlayerID().String(), string(wallet.Currency()), wallet.Balance().Units(), wallet.Version(), wallet.CreatedAt(), wallet.UpdatedAt())
		if err != nil {
			return err
		}

		// Also create the OPENING transaction
		txID, _ := uuid.NewV7()
		openingTx, err := domain.NewOpeningTransaction(txID, wallet, time.Now())
		if err != nil {
			return err
		}

		if err := uc.repo.InsertWagerTransaction(ctx, tx, openingTx); err != nil {
			return err
		}

		// Ledger entry
		entryID, _ := uuid.NewV7()
		movement := domain.Movement{
			Direction:     domain.Credit,
			Amount:        zeroMoney,
			BalanceBefore: zeroMoney,
			BalanceAfter:  zeroMoney,
			WalletVersion: wallet.Version(),
		}
		if err := uc.repo.InsertLedgerEntry(ctx, tx, entryID, wallet.ID(), openingTx.ID(), movement); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return wallet, nil
}
