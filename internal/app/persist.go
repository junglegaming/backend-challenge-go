package app

import (
	"context"

	"github.com/Pantani/backend-challenge-go/internal/domain/wager"
)

// persist writes the transaction, the balance change, its ledger entry and
// the outbox records. The caller's unit of work commits them atomically.
func (s *WagerService) persist(ctx context.Context, r Repositories, out *outcome, isNew bool) error {
	return sequence(
		func() error { return saveTransaction(ctx, r.Transactions(), out.tx, isNew) },
		func() error { return saveMovement(ctx, r, out) },
		func() error { return r.Outbox().Append(ctx, out.records...) },
		func() error { return wakeDependents(ctx, r.Transactions(), out) },
	)
}

func saveTransaction(ctx context.Context, repo TransactionRepository, tx *wager.Transaction, isNew bool) error {
	if isNew {
		return repo.Create(ctx, tx)
	}
	return repo.Save(ctx, tx)
}

// saveMovement persists the balance change, if any. An outcome without a
// ledger entry (rejection, deferral, permanent failure) may carry no wallet
// at all, so the nil check on the entry must come first.
func saveMovement(ctx context.Context, r Repositories, out *outcome) error {
	if out.entry == nil {
		return nil
	}
	return sequence(
		func() error { return r.Wallets().Save(ctx, out.wallet, out.prevVersion) },
		func() error { return r.Ledger().Append(ctx, *out.entry) },
	)
}

// wakeDependents makes operations waiting for this one due immediately once
// it reached a terminal state.
func wakeDependents(ctx context.Context, repo TransactionRepository, out *outcome) error {
	if !out.tx.Status().Terminal() {
		return nil
	}
	ext := out.tx.External()
	return repo.WakeDependents(ctx, ext.ProviderID, ext.ExternalID, out.now)
}
