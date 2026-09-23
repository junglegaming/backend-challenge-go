package app

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/Pantani/backend-challenge-go/internal/domain/event"
	"github.com/Pantani/backend-challenge-go/internal/domain/wager"
	"github.com/Pantani/backend-challenge-go/internal/domain/wallet"
)

// outcome is everything that must be committed atomically for one decision.
type outcome struct {
	tx *wager.Transaction
	// wallet is nil when the outcome carries no balance change.
	wallet      *wallet.Wallet
	prevVersion int64
	entry       *wallet.LedgerEntry
	records     []event.Record
	causation   string
	// now is the single instant of the operation: transitions, ledger
	// entries and events all carry it.
	now time.Time
}

// settle decides the operation and persists the result inside the caller's
// unit of work. now is the instant the operation was created (or retried),
// so every row and event of the attempt carries the same timestamp.
func (s *WagerService) settle(ctx context.Context, r Repositories, w *wallet.Wallet, tx *wager.Transaction, causation string, now time.Time, isNew bool) error {
	out, err := s.conclude(ctx, r, w, tx, causation, now)
	if err != nil {
		return err
	}
	return s.persist(ctx, r, out, isNew)
}

// conclude loads the reference, applies the pure business rules and performs
// the resulting transition in memory. Nothing is written yet.
func (s *WagerService) conclude(ctx context.Context, r Repositories, w *wallet.Wallet, tx *wager.Transaction, causation string, now time.Time) (*outcome, error) {
	ref, reversed, err := loadReference(ctx, r.Transactions(), tx)
	if err != nil {
		return nil, err
	}
	d := wager.Decide(wager.DecisionInput{Wallet: w, Transaction: tx, Reference: ref, ReferenceReversed: reversed})
	out := &outcome{tx: tx, wallet: w, prevVersion: w.Version(), causation: causation, now: now}
	switch d.Action {
	case wager.ActionProcess:
		err = s.applyProcess(out, ref, d)
	case wager.ActionReject:
		err = s.applyReject(out, d.Code)
	default:
		err = s.applyAwait(out, ref)
	}
	return out, err
}

func loadReference(ctx context.Context, repo TransactionRepository, tx *wager.Transaction) (*wager.Transaction, bool, error) {
	ext := tx.External()
	if ext.ReferenceExternalID == "" {
		return nil, false, nil
	}
	ref, err := repo.GetByExternal(ctx, ext.ProviderID, ext.ReferenceExternalID)
	if errors.Is(err, ErrTransactionNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	reversed, err := repo.HasProcessedReversal(ctx, ref.ID())
	return ref, reversed, err
}

// newMeta builds the tracing metadata of one event with a fresh event id.
func newMeta(ids IDGenerator, correlation, causation string, at time.Time) event.Meta {
	return event.Meta{EventID: ids.New(), CorrelationID: correlation, CausationID: causation, OccurredAt: at}
}

func (s *WagerService) meta(out *outcome) event.Meta {
	return newMeta(s.IDs, out.tx.CorrelationID(), out.causation, out.now)
}

func (s *WagerService) applyProcess(out *outcome, ref *wager.Transaction, d wager.Decision) error {
	return sequence(
		func() error { return s.move(out, d) },
		func() error { return out.tx.Process(out.wallet.Balance(), referenceID(ref), out.now) },
		func() error {
			out.records = append(out.records, event.NewWagerTransactionProcessed(s.meta(out), out.tx))
			return nil
		},
		func() error {
			if out.entry != nil {
				out.records = append(out.records, event.NewWalletBalanceChanged(s.meta(out), *out.entry, out.wallet.Version()))
			}
			return nil
		},
	)
}

// move applies the balance change and keeps its ledger entry.
func (s *WagerService) move(out *outcome, d wager.Decision) error {
	if !d.Moves {
		return nil
	}
	entry, err := out.wallet.Apply(wallet.Movement{
		EntryID: s.IDs.New(), TransactionID: out.tx.ID(), Direction: d.Direction, Amount: out.tx.Amount(), Now: out.now,
	})
	out.entry = &entry
	return err
}

func referenceID(ref *wager.Transaction) uuid.UUID {
	if ref == nil {
		return uuid.Nil
	}
	return ref.ID()
}

func (s *WagerService) applyReject(out *outcome, code wager.FailureCode) error {
	if err := out.tx.Reject(code, out.wallet.Balance(), out.now); err != nil {
		return err
	}
	out.records = append(out.records, event.NewWagerTransactionRejected(s.meta(out), out.tx))
	return nil
}

// applyAwait defers the operation with exponential backoff, or rejects it
// once the attempts are exhausted. Only the first deferral emits an event.
func (s *WagerService) applyAwait(out *outcome, ref *wager.Transaction) error {
	if out.tx.Attempts() >= s.Policy.MaxAttempts {
		return s.applyReject(out, expiredCode(ref))
	}
	first := out.tx.Status() == wager.StatusPending
	if err := out.tx.AwaitReference(s.Policy.Next(out.tx.Attempts(), out.now), out.now); err != nil {
		return err
	}
	if first {
		out.records = append(out.records, event.NewWagerTransactionPendingReference(s.meta(out), out.tx))
	}
	return nil
}

// expiredCode distinguishes a reference that never arrived from one that
// arrived but never concluded.
func expiredCode(ref *wager.Transaction) wager.FailureCode {
	if ref == nil {
		return wager.CodeReferenceNotFound
	}
	return wager.CodeReferenceNotProcessed
}
