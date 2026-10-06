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

func (uc *ProcessTransactionUseCase) Execute(ctx context.Context, req domain.ExternalRequest, consumerName, messageID string) (*domain.WagerTransaction, error) {
	var finalTx *domain.WagerTransaction

	err := uc.txManager.RunInTransaction(ctx, func(ctx context.Context, tx pgx.Tx) error {
		// 0. Check inbox deduplication if coming from SQS
		if messageID != "" && consumerName != "" {
			inserted, err := uc.repo.InsertInboxMessage(ctx, tx, consumerName, messageID)
			if err != nil {
				return err
			}
			if !inserted {
				// Message was already processed by this consumer
				return nil
			}
		}

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

		txID, err := uuid.NewV7()
		if err != nil {
			return err
		}
		now := time.Now().UTC()
		wagerTx, err := domain.NewExternalTransaction(txID, req, now)
		if err != nil {
			return err
		}

		createMeta := func() domain.EventMeta {
			evID, _ := uuid.NewV7()
			corrID := req.IdempotencyKey
			if corrID == "" {
				corrID = wagerTx.ID().String()
			}
			return domain.EventMeta{
				EventID:       evID,
				CorrelationID: corrID,
				CausationID:   wagerTx.ID().String(),
				OccurredAt:    now,
			}
		}

		publishRejected := func(code domain.FailureCode, bal domain.Money) error {
			if err := wagerTx.MarkRejected(code, bal, now); err != nil {
				return err
			}
			if err := uc.repo.UpdateWagerTransactionStatus(ctx, tx, wagerTx.ID(), wagerTx.Status(), wagerTx.FailureCode()); err != nil {
				return err
			}
			ev, _ := domain.NewWagerTransactionRejected(createMeta(), wagerTx)
			return uc.repo.InsertOutboxEvent(ctx, tx, ev.EventType, ev)
		}

		// 2. Reference Checking (if applicable)
		var refTx *domain.WagerTransaction
		if req.ReferenceExternalTransactionID != "" {
			refTx, err = uc.repo.FindTransactionByExternalIDInTx(ctx, tx, req.ProviderID, req.ReferenceExternalTransactionID)
			if err != nil {
				return err
			}

			if refTx == nil {
				if err := wagerTx.MarkPendingReference(now); err != nil {
					return err
				}
				if err := uc.repo.InsertWagerTransaction(ctx, tx, wagerTx); err != nil {
					return err
				}
				ev, _ := domain.NewWagerTransactionPendingReference(createMeta(), wagerTx)
				if err := uc.repo.InsertOutboxEvent(ctx, tx, ev.EventType, ev); err != nil {
					return err
				}
				finalTx = wagerTx
				return nil // It's pending, return cleanly
			}

			decision, failureCode := domain.EvaluateReference(wagerTx, refTx)
			switch decision {
			case domain.ReferenceWait:
				if err := wagerTx.MarkPendingReference(now); err != nil {
					return err
				}
				if err := uc.repo.InsertWagerTransaction(ctx, tx, wagerTx); err != nil {
					return err
				}
				ev, _ := domain.NewWagerTransactionPendingReference(createMeta(), wagerTx)
				if err := uc.repo.InsertOutboxEvent(ctx, tx, ev.EventType, ev); err != nil {
					return err
				}
				finalTx = wagerTx
				return nil
			case domain.ReferenceRejected:
				if err := uc.repo.InsertWagerTransaction(ctx, tx, wagerTx); err != nil {
					return err
				}
				if err := publishRejected(failureCode, domain.Money{}); err != nil {
					return err
				}
				finalTx = wagerTx
				return nil
			case domain.ReferenceAccepted:
				// Ensure no double reversal
				if domain.IsReversal(wagerTx.Kind()) {
					hasReversal, err := uc.repo.HasProcessedReversalInTx(ctx, tx, req.ProviderID, req.ReferenceExternalTransactionID)
					if err != nil {
						return err
					}
					if hasReversal {
						if err := uc.repo.InsertWagerTransaction(ctx, tx, wagerTx); err != nil {
							return err
						}
						if err := publishRejected(domain.FailureAlreadyReversed, domain.Money{}); err != nil {
							return err
						}
						finalTx = wagerTx
						return nil
					}
				}
			}
		}

		// Insert early so we can update it later
		if err := uc.repo.InsertWagerTransaction(ctx, tx, wagerTx); err != nil {
			return err
		}

		// Lock Wallet
		wallet, err := uc.repo.FindWalletForUpdate(ctx, tx, req.WalletID)
		if err != nil {
			return err
		}

		if wallet == nil {
			if err := publishRejected(domain.FailureInternal, domain.Money{}); err != nil {
				return err
			}
			finalTx = wagerTx
			return nil
		}

		if wallet.PlayerID() != req.PlayerID {
			if err := publishRejected(domain.FailurePlayerWalletMismatch, wallet.Balance()); err != nil {
				return err
			}
			finalTx = wagerTx
			return nil
		}

		if wallet.Currency() != req.Money.Currency() {
			if err := publishRejected(domain.FailureCurrencyMismatch, wallet.Balance()); err != nil {
				return err
			}
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
			applyErr = nil
		case domain.KindRollback:
			if refTx != nil {
				switch refTx.Kind() {
				case domain.KindBet:
					movement, applyErr = wallet.Credit(req.Money, now)
				case domain.KindWin, domain.KindRefund:
					movement, applyErr = wallet.Debit(req.Money, now)
				case domain.KindLoss:
					applyErr = nil
				default:
					applyErr = fmt.Errorf("cannot rollback kind: %s", refTx.Kind())
				}
			} else {
				applyErr = fmt.Errorf("rollback requires reference tx")
			}
		default:
			applyErr = fmt.Errorf("unsupported kind: %s", req.Kind)
		}

		if applyErr != nil {
			if errors.Is(applyErr, domain.ErrInsufficientFunds) {
				if err := publishRejected(domain.FailureInsufficientFunds, wallet.Balance()); err != nil {
					return err
				}
				finalTx = wagerTx
				return nil
			}
			return applyErr
		}

		resolvedRefID := uuid.Nil
		if refTx != nil {
			resolvedRefID = refTx.ID()
		}

		if req.Kind != domain.KindLoss && !(req.Kind == domain.KindRollback && refTx != nil && refTx.Kind() == domain.KindLoss) {
			if err := wagerTx.MarkProcessed(wallet.Balance(), resolvedRefID, now); err != nil {
				return err
			}

			if err := uc.repo.UpdateWalletBalance(ctx, tx, wallet); err != nil {
				return err
			}

			entryID, err := uuid.NewV7()
			if err != nil {
				return err
			}
			if err := uc.repo.InsertLedgerEntry(ctx, tx, entryID, wallet.ID(), wagerTx.ID(), movement); err != nil {
				return err
			}

			balEvent, _ := domain.NewWalletBalanceChanged(createMeta(), wallet.ID(), wagerTx.ID(), movement)
			if err := uc.repo.InsertOutboxEvent(ctx, tx, balEvent.EventType, balEvent); err != nil {
				return err
			}
		} else {
			if err := wagerTx.MarkProcessed(wallet.Balance(), resolvedRefID, now); err != nil {
				return err
			}
		}

		if err := uc.repo.UpdateWagerTransactionProcessed(ctx, tx, wagerTx); err != nil {
			return err
		}

		// Update ResultBalance and ResolvedReferenceID properly since status update doesn't update those in our current UpdateWagerTransactionStatus
		// Actually, let's use a full update method for processed transactions. We don't have it in the interface but we can just add one.
		// Wait, earlier I wrote `UpdateWagerTransactionProcessed` in wallet_repo.go but did I add it to the interface? Let me check later.

		// Outbox Event
		ev, _ := domain.NewWagerTransactionProcessed(createMeta(), wagerTx)
		if err := uc.repo.InsertOutboxEvent(ctx, tx, ev.EventType, ev); err != nil {
			return err
		}

		finalTx = wagerTx
		return nil
	})

	return finalTx, err
}
