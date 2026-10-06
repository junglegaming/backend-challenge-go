package usecases

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/Lzdevmendes/backend-challenge-go/internal/domain"
)

type PendingReferenceUseCase struct {
	txManager TransactionManager
	repo      WalletRepository
}

func NewPendingReferenceUseCase(txManager TransactionManager, repo WalletRepository) *PendingReferenceUseCase {
	return &PendingReferenceUseCase{
		txManager: txManager,
		repo:      repo,
	}
}

// ProcessBatch finds pending transactions and tries to process them or reject them if timed out.
func (uc *PendingReferenceUseCase) ProcessBatch(ctx context.Context, batchSize int, timeout time.Duration) (int, error) {
	var processedCount int

	err := uc.txManager.RunInTransaction(ctx, func(ctx context.Context, tx pgx.Tx) error {
		// We need a method in repo to fetch pending reference tx IDs
		txIDs, err := uc.repo.GetPendingReferenceTxIDsInTx(ctx, tx, batchSize)
		if err != nil {
			return err
		}

		now := time.Now().UTC()

		for _, txID := range txIDs {
			// Find the transaction
			wagerTx, err := uc.repo.FindTransactionByIDInTx(ctx, tx, txID)
			if err != nil || wagerTx == nil {
				continue
			}

			// If it's already older than 10 minutes, reject it
			if now.Sub(wagerTx.CreatedAt()) > timeout {
				wagerTx.MarkRejected(domain.FailureReferenceNotFound, domain.Money{}, now)
				uc.repo.UpdateWagerTransactionStatus(ctx, tx, wagerTx.ID(), wagerTx.Status(), domain.FailureReferenceNotFound)
				uc.repo.InsertOutboxEvent(ctx, tx, "WagerTransactionRejected", wagerTx)
				processedCount++
				continue
			}

			// Try to find the reference
			refTx, err := uc.repo.FindTransactionByExternalIDInTx(ctx, tx, wagerTx.Request().ProviderID, wagerTx.Request().ReferenceExternalTransactionID)
			if err != nil {
				continue
			}

			if refTx == nil {
				// Still not found, skip
				continue
			}

			decision, failureCode := domain.EvaluateReference(wagerTx, refTx)

			switch decision {
			case domain.ReferenceWait:
				continue
			case domain.ReferenceRejected:
				wagerTx.MarkRejected(failureCode, domain.Money{}, now)
				uc.repo.UpdateWagerTransactionStatus(ctx, tx, wagerTx.ID(), wagerTx.Status(), failureCode)
				processedCount++
				continue
			case domain.ReferenceAccepted:
				// Ensure no double reversal
				if domain.IsReversal(wagerTx.Kind()) {
					hasReversal, err := uc.repo.HasProcessedReversalInTx(ctx, tx, wagerTx.Request().ProviderID, wagerTx.Request().ReferenceExternalTransactionID)
					if err != nil {
						continue
					}
					if hasReversal {
						wagerTx.MarkRejected(domain.FailureAlreadyReversed, domain.Money{}, now)
						uc.repo.UpdateWagerTransactionStatus(ctx, tx, wagerTx.ID(), wagerTx.Status(), domain.FailureAlreadyReversed)
						processedCount++
						continue
					}
				}

				// Time to process the transaction!
				wallet, err := uc.repo.FindWalletForUpdate(ctx, tx, wagerTx.PlayerID(), string(wagerTx.Money().Currency()))
				if err != nil {
					continue
				}
				if wallet == nil {
					wagerTx.MarkRejected(domain.FailureInternal, domain.Money{}, now)
					uc.repo.UpdateWagerTransactionStatus(ctx, tx, wagerTx.ID(), wagerTx.Status(), domain.FailureInternal)
					processedCount++
					continue
				}

				req := wagerTx.Request()
				var movement domain.Movement
				var applyErr error

				switch req.Kind {
				case domain.KindBet:
					movement, applyErr = wallet.Debit(req.Money, now)
				case domain.KindWin, domain.KindRefund:
					movement, applyErr = wallet.Credit(req.Money, now)
				case domain.KindLoss:
					applyErr = nil
				case domain.KindRollback:
					movement, applyErr = wallet.Credit(req.Money, now)
				}

				if applyErr != nil {
					if errorsIsInsufficientFunds(applyErr) {
						wagerTx.MarkRejected(domain.FailureInsufficientFunds, wallet.Balance(), now)
						uc.repo.UpdateWagerTransactionStatus(ctx, tx, wagerTx.ID(), wagerTx.Status(), domain.FailureInsufficientFunds)
						uc.repo.InsertOutboxEvent(ctx, tx, "WagerTransactionRejected", wagerTx)
						processedCount++
						continue
					}
					continue
				}

				if req.Kind != domain.KindLoss {
					wagerTx.MarkProcessed(wallet.Balance(), refTx.ID(), now)
					if err := uc.repo.UpdateWalletBalance(ctx, tx, wallet); err != nil {
						continue
					}
					entryID, _ := uuid.NewV7()
					uc.repo.InsertLedgerEntry(ctx, tx, entryID, wallet.ID(), wagerTx.ID(), movement)
				} else {
					wagerTx.MarkProcessed(wallet.Balance(), refTx.ID(), now)
				}

				uc.repo.UpdateWagerTransactionStatus(ctx, tx, wagerTx.ID(), wagerTx.Status(), "")
				uc.repo.InsertOutboxEvent(ctx, tx, "WagerTransactionProcessed", wagerTx)
				processedCount++
			}
		}

		return nil
	})

	return processedCount, err
}
