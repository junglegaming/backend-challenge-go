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

func (uc *PendingReferenceUseCase) ProcessBatch(ctx context.Context, batchSize int, timeout time.Duration) (int, error) {
	var processedCount int

	err := uc.txManager.RunInTransaction(ctx, func(ctx context.Context, tx pgx.Tx) error {
		txIDs, err := uc.repo.GetPendingReferenceTxIDsInTx(ctx, tx, batchSize)
		if err != nil {
			return err
		}

		now := time.Now().UTC()

		for _, txID := range txIDs {
			wagerTx, err := uc.repo.FindTransactionByIDInTx(ctx, tx, txID)
			if err != nil || wagerTx == nil {
				continue
			}

			req := wagerTx.Request()

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

			if now.Sub(wagerTx.CreatedAt()) > timeout {
				_ = publishRejected(domain.FailureReferenceNotFound, domain.Money{})
				processedCount++
				continue
			}

			refTx, err := uc.repo.FindTransactionByExternalIDInTx(ctx, tx, req.ProviderID, req.ReferenceExternalTransactionID)
			if err != nil || refTx == nil {
				continue
			}

			decision, failureCode := domain.EvaluateReference(wagerTx, refTx)

			switch decision {
			case domain.ReferenceWait:
				continue
			case domain.ReferenceRejected:
				_ = publishRejected(failureCode, domain.Money{})
				processedCount++
				continue
			case domain.ReferenceAccepted:
				if domain.IsReversal(wagerTx.Kind()) {
					hasReversal, err := uc.repo.HasProcessedReversalInTx(ctx, tx, req.ProviderID, req.ReferenceExternalTransactionID)
					if err != nil {
						continue
					}
					if hasReversal {
						_ = publishRejected(domain.FailureAlreadyReversed, domain.Money{})
						processedCount++
						continue
					}
				}

				wallet, err := uc.repo.FindWalletForUpdate(ctx, tx, req.WalletID)
				if err != nil {
					continue
				}
				if wallet == nil {
					_ = publishRejected(domain.FailureInternal, domain.Money{})
					processedCount++
					continue
				}

				if wallet.PlayerID() != req.PlayerID {
					_ = publishRejected(domain.FailurePlayerWalletMismatch, wallet.Balance())
					processedCount++
					continue
				}

				if wallet.Currency() != req.Money.Currency() {
					_ = publishRejected(domain.FailureCurrencyMismatch, wallet.Balance())
					processedCount++
					continue
				}

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
						_ = publishRejected(domain.FailureInsufficientFunds, wallet.Balance())
						processedCount++
					}
					continue
				}

				resolvedRefID := refTx.ID()

				if req.Kind != domain.KindLoss && !(req.Kind == domain.KindRollback && refTx != nil && refTx.Kind() == domain.KindLoss) {
					if err := wagerTx.MarkProcessed(wallet.Balance(), resolvedRefID, now); err != nil {
						continue
					}
					if err := uc.repo.UpdateWalletBalance(ctx, tx, wallet); err != nil {
						continue
					}
					entryID, _ := uuid.NewV7()
					_ = uc.repo.InsertLedgerEntry(ctx, tx, entryID, wallet.ID(), wagerTx.ID(), movement)
					balEvent, _ := domain.NewWalletBalanceChanged(createMeta(), wallet.ID(), wagerTx.ID(), movement)
					_ = uc.repo.InsertOutboxEvent(ctx, tx, balEvent.EventType, balEvent)
				} else {
					if err := wagerTx.MarkProcessed(wallet.Balance(), resolvedRefID, now); err != nil {
						continue
					}
				}

				_ = uc.repo.UpdateWagerTransactionProcessed(ctx, tx, wagerTx)
				ev, _ := domain.NewWagerTransactionProcessed(createMeta(), wagerTx)
				_ = uc.repo.InsertOutboxEvent(ctx, tx, ev.EventType, ev)
				processedCount++
			}
		}

		return nil
	})

	return processedCount, err
}
