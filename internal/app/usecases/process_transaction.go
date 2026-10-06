package usecases

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/Lzdevmendes/backend-challenge-go/internal/domain"
)

type ProcessTransactionUseCase struct {
	txManager TransactionManager
	repo      WalletRepository
}

func NewProcessTransactionUseCase(txManager TransactionManager, repo WalletRepository) *ProcessTransactionUseCase {
	return &ProcessTransactionUseCase{
		txManager: txManager,
		repo:      repo,
	}
}

// Execute processes a wager transaction idempotently.
func (uc *ProcessTransactionUseCase) Execute(ctx context.Context, req domain.ExternalRequest) (*domain.WagerTransaction, error) {
	var finalTx *domain.WagerTransaction

	err := uc.txManager.RunInTransaction(ctx, func(ctx context.Context, tx pgx.Tx) error {
		// 1. Idempotency Check
		existingTx, err := uc.repo.CheckIdempotency(ctx, tx, req.ProviderID, req.ExternalTransactionID)
		if err != nil {
			return err
		}
		if existingTx != nil {
			// Validate payload hash to ensure it's not a different request with same ID
			newHash, err := domain.PayloadHash(req)
			if err != nil {
				return err
			}
			if existingTx.PayloadHash() != newHash {
				return fmt.Errorf("%w: payload hash conflict", domain.ErrInvalidArgument)
			}
			finalTx = existingTx
			return nil // Return existing result (Idempotency)
		}

		// Create Domain Transaction
		txID, err := uuid.NewV7()
		if err != nil {
			return err
		}
		now := time.Now().UTC()
		wagerTx, err := domain.NewExternalTransaction(txID, req, now)
		if err != nil {
			return err
		}

		// Insert early so we can refer to it
		if err := uc.repo.InsertWagerTransaction(ctx, tx, wagerTx); err != nil {
			return err
		}

		// Lock Wallet
		wallet, err := uc.repo.FindWalletForUpdate(ctx, tx, req.PlayerID, string(req.Money.Currency()))
		if err != nil {
			return err
		}

		if wallet == nil {
			wagerTx.MarkRejected(domain.FailureInternal, domain.Money{}, now)
			// Return cleanly but fail the transaction
			_ = uc.repo.UpdateWagerTransactionStatus(ctx, tx, wagerTx.ID(), wagerTx.Status(), domain.FailureInternal)
			finalTx = wagerTx
			return nil
		}

		// Apply Movement
		var movement domain.Movement
		var applyErr error

		switch req.Kind {
		case domain.KindBet:
			movement, applyErr = wallet.Debit(req.Money, now)
		case domain.KindWin, domain.KindRefund:
			movement, applyErr = wallet.Credit(req.Money, now)
		case domain.KindLoss:
			// Loss doesn't change balance but completes the round.
			applyErr = nil
		case domain.KindRollback:
			movement, applyErr = wallet.Credit(req.Money, now) // Rollback of a bet is a credit back
		default:
			applyErr = fmt.Errorf("unsupported kind: %s", req.Kind)
		}

		if applyErr != nil {
			if errorsIsInsufficientFunds(applyErr) {
				wagerTx.MarkRejected(domain.FailureInsufficientFunds, wallet.Balance(), now)
				uc.repo.UpdateWagerTransactionStatus(ctx, tx, wagerTx.ID(), wagerTx.Status(), domain.FailureInsufficientFunds)

				uc.repo.InsertOutboxEvent(ctx, tx, "WagerTransactionRejected", wagerTx)
				finalTx = wagerTx
				return nil
			}
			return applyErr
		}

		if req.Kind != domain.KindLoss {
			wagerTx.MarkProcessed(wallet.Balance(), uuid.Nil, now) // Assume reference checking handles uuid later if needed

			// Update Wallet
			if err := uc.repo.UpdateWalletBalance(ctx, tx, wallet); err != nil {
				return err
			}

			// Ledger
			entryID, err := uuid.NewV7()
			if err != nil {
				return err
			}
			if err := uc.repo.InsertLedgerEntry(ctx, tx, entryID, wallet.ID(), wagerTx.ID(), movement); err != nil {
				return err
			}
		} else {
			wagerTx.MarkProcessed(wallet.Balance(), uuid.Nil, now)
		}

		if err := uc.repo.UpdateWagerTransactionStatus(ctx, tx, wagerTx.ID(), wagerTx.Status(), ""); err != nil {
			return err
		}

		// Outbox Event
		if err := uc.repo.InsertOutboxEvent(ctx, tx, "WagerTransactionProcessed", wagerTx); err != nil {
			return err
		}

		finalTx = wagerTx
		return nil
	})

	return finalTx, err
}

func errorsIsInsufficientFunds(err error) bool {
	return errors.Is(err, domain.ErrInsufficientFunds)
}
