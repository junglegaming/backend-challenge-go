package app

import (
	"context"
	"errors"

	"github.com/Pantani/backend-challenge-go/internal/domain/event"
	"github.com/Pantani/backend-challenge-go/internal/domain/wager"
)

// ResolveDue retries one batch of due PENDING_REFERENCE operations and
// returns how many were concluded or rescheduled. Several instances can run
// it concurrently: each operation is claimed with SKIP LOCKED under its
// wallet lock, and state lives in PostgreSQL, so a restart loses nothing.
func (s *WagerService) ResolveDue(ctx context.Context) (int, error) {
	due, err := s.Queries.ListDuePending(ctx, s.Clock.Now(), s.Policy.BatchSize)
	if err != nil {
		return 0, err
	}
	resolved := 0
	for _, d := range due {
		if ctx.Err() != nil {
			break
		}
		if s.resolveOne(ctx, d) {
			resolved++
		}
	}
	return resolved, ctx.Err()
}

// resolveOne runs one attempt of a due operation in its own unit of work
// and reports whether it was concluded or rescheduled. Worker attempts are
// observed under SourceWorker like any other submission.
func (s *WagerService) resolveOne(ctx context.Context, d DueTransaction) bool {
	start := s.Clock.Now()
	tx, err := inTx(ctx, s, "pending", func(ctx context.Context, r Repositories) (*wager.Transaction, error) {
		return s.resolveInTx(ctx, r, d)
	})
	switch {
	case err == nil:
		s.Metrics.PendingResolution(tx.Status())
		s.observe(SourceWorker, SubmitResult{Transaction: tx}, start)
		return true
	case errors.Is(err, ErrNotDue):
		return false
	case IsTransient(err):
		s.Logger.WarnContext(ctx, "pending reference retry postponed", "transactionId", d.ID, "walletId", d.WalletID, "error", err)
		return false
	}
	s.markFailed(ctx, d, err)
	return false
}

func (s *WagerService) resolveInTx(ctx context.Context, r Repositories, d DueTransaction) (*wager.Transaction, error) {
	now := s.Clock.Now()
	w, err := r.Wallets().GetForUpdate(ctx, d.WalletID)
	if err != nil {
		return nil, err
	}
	tx, err := r.Transactions().LockDuePending(ctx, d.ID, now)
	if err != nil {
		return nil, err
	}
	return tx, s.settle(ctx, r, w, tx, "", now, false)
}

// markFailed records a permanent, non-business failure for auditing so the
// operation stops being retried forever. FAILED is terminal: its event is
// emitted and the operations waiting for it are woken up, exactly like a
// rejection.
func (s *WagerService) markFailed(ctx context.Context, d DueTransaction, cause error) {
	s.Logger.ErrorContext(ctx, "pending reference failed permanently", "transactionId", d.ID, "walletId", d.WalletID, "error", cause)
	err := s.UoW.Do(ctx, func(ctx context.Context, r Repositories) error {
		return s.failInTx(ctx, r, d)
	})
	if err != nil {
		s.Logger.ErrorContext(ctx, "could not mark pending reference as failed", "transactionId", d.ID, "error", err)
		return
	}
	s.Metrics.PendingResolution(wager.StatusFailed)
}

func (s *WagerService) failInTx(ctx context.Context, r Repositories, d DueTransaction) error {
	now := s.Clock.Now()
	tx, err := r.Transactions().LockDuePending(ctx, d.ID, now)
	if err != nil {
		return err
	}
	if err := tx.Fail(wager.CodeInternalFailure, now); err != nil {
		return err
	}
	out := &outcome{tx: tx, now: now}
	out.records = append(out.records, event.NewWagerTransactionFailed(s.meta(out), tx))
	return s.persist(ctx, r, out, false)
}
