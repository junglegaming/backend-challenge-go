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
	PlayerID       uuid.UUID
	Currency       string
	InitialBalance string
}

func (uc *CreateWalletUseCase) Execute(ctx context.Context, cmd CreateWalletCommand) (*domain.Wallet, error) {
	if cmd.InitialBalance == "" {
		cmd.InitialBalance = "0.00"
	}
	
	initMoney, err := domain.ParseMoney(cmd.InitialBalance, cmd.Currency)
	if err != nil {
		return nil, err
	}

	walletID, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}

	wallet, err := domain.NewWallet(walletID, cmd.PlayerID, initMoney, time.Now())
	if err != nil {
		return nil, err
	}

	err = uc.txManager.RunInTransaction(ctx, func(ctx context.Context, tx pgx.Tx) error {
		query := `
			INSERT INTO wallets (id, player_id, currency, balance, version, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
		`
		_, err := tx.Exec(ctx, query, wallet.ID(), wallet.PlayerID(), string(wallet.Currency()), wallet.Balance().Units(), wallet.Version(), wallet.CreatedAt(), wallet.UpdatedAt())
		if err != nil {
			return err
		}

		if initMoney.IsPositive() {
			txID, _ := uuid.NewV7()
			now := time.Now().UTC()
			openingTx, err := domain.NewOpeningTransaction(txID, wallet, now)
			if err != nil {
				return err
			}
			
			if err := openingTx.MarkProcessed(wallet.Balance(), uuid.Nil, now); err != nil {
				return err
			}

			if err := uc.repo.InsertWagerTransaction(ctx, tx, openingTx); err != nil {
				return err
			}

			entryID, _ := uuid.NewV7()
			movement := domain.Movement{
				Direction:     domain.Credit,
				Amount:        initMoney,
				BalanceBefore: domain.Money{}, // Not strictly zero domain.Money{} but an empty one? Wait, domain.Zero(currency).
			}
			movement.BalanceBefore, _ = domain.Zero(wallet.Currency())
			movement.BalanceAfter = initMoney
			movement.WalletVersion = wallet.Version()

			if err := uc.repo.InsertLedgerEntry(ctx, tx, entryID, wallet.ID(), openingTx.ID(), movement); err != nil {
				return err
			}

			// Generate events
			evMeta := domain.EventMeta{
				EventID:       uuid.Must(uuid.NewV7()),
				CorrelationID: openingTx.ID().String(),
				CausationID:   openingTx.ID().String(),
				OccurredAt:    now,
			}

			balEvent, _ := domain.NewWalletBalanceChanged(evMeta, wallet.ID(), openingTx.ID(), movement)
			_ = uc.repo.InsertOutboxEvent(ctx, tx, balEvent.EventType, balEvent)

			procEvent, _ := domain.NewWagerTransactionProcessed(evMeta, openingTx)
			_ = uc.repo.InsertOutboxEvent(ctx, tx, procEvent.EventType, procEvent)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return wallet, nil
}
