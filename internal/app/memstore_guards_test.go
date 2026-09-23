package app_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Pantani/backend-challenge-go/internal/app"
	"github.com/Pantani/backend-challenge-go/internal/domain/wager"
	"github.com/Pantani/backend-challenge-go/internal/domain/wallet"
	"github.com/Pantani/backend-challenge-go/internal/testutil"
)

// The in-memory store mirrors the schema guards; these tests keep the mirror
// honest so the service tests above can rely on it.

func TestMemStoreCheckRow(t *testing.T) {
	t.Parallel()
	base := wager.Snapshot{Kind: wager.KindBet, Status: wager.StatusPendingReference, NextAttemptAt: time.Now()}
	cases := map[string]func(s *wager.Snapshot){
		"rejected without code": func(s *wager.Snapshot) { s.Status = wager.StatusRejected },
		"failed without code":   func(s *wager.Snapshot) { s.Status = wager.StatusFailed },
		"processed no balance":  func(s *wager.Snapshot) { s.Status = wager.StatusProcessed },
		"pending no schedule":   func(s *wager.Snapshot) { s.NextAttemptAt = time.Time{} },
		"stored PENDING":        func(s *wager.Snapshot) { s.Status = wager.StatusPending },
		"refund no reference":   func(s *wager.Snapshot) { s.Kind = wager.KindRefund },
	}
	for name, mutate := range cases {
		s := base
		mutate(&s)
		assert.ErrorIs(t, checkRow(s), errCheck, name)
	}
	require.NoError(t, checkRow(base))
}

func TestMemWalletsSaveGuards(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	w := h.openWallet(t, "10.00")
	ctx := context.Background()
	save := func(mutate func(s *wallet.Snapshot)) error {
		s := walletSnapshot(w)
		mutate(&s)
		changed, err := wallet.Rehydrate(s)
		require.NoError(t, err)
		return h.store.Do(ctx, func(ctx context.Context, r app.Repositories) error {
			return r.Wallets().Save(ctx, changed, w.Version())
		})
	}
	require.ErrorIs(t, save(func(s *wallet.Snapshot) { s.Balance = testutil.BRL(t, "5.00") }), errCheck, "balance changed without a version bump")
	require.ErrorIs(t, save(func(s *wallet.Snapshot) { s.Version++ }), errCheck, "version bumped without a balance change")
	require.ErrorIs(t, save(func(s *wallet.Snapshot) { s.Version = 99 }), errCheck)
	require.NoError(t, save(func(s *wallet.Snapshot) { s.Balance, s.Version = testutil.BRL(t, "5.00"), s.Version+1 }))
	require.ErrorIs(t, save(func(*wallet.Snapshot) {}), app.ErrConflict, "stale version")
}

func TestMemLedgerUniqueTransaction(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	w := h.openWallet(t, "10.00")
	entry, err := wallet.NewLedgerEntry(wallet.LedgerEntryParams{
		ID: uuid.New(), WalletID: w.ID(), TransactionID: uuid.New(), Direction: wallet.Credit,
		Amount: testutil.BRL(t, "1.00"), BalanceBefore: testutil.BRL(t, "10.00"), BalanceAfter: testutil.BRL(t, "11.00"), CreatedAt: t0,
	})
	require.NoError(t, err)
	err = h.store.Do(context.Background(), func(ctx context.Context, r app.Repositories) error {
		if err := r.Ledger().Append(ctx, entry); err != nil {
			return err
		}
		return r.Ledger().Append(ctx, entry)
	})
	require.ErrorIs(t, err, app.ErrConflict)
	assert.Equal(t, 1, h.store.ledgerCount(w.ID()), "rolled back")
}
