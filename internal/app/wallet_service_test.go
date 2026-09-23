package app_test

import (
	"context"
	"encoding/base64"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Pantani/backend-challenge-go/internal/app"
	"github.com/Pantani/backend-challenge-go/internal/domain/event"
	"github.com/Pantani/backend-challenge-go/internal/domain/money"
	"github.com/Pantani/backend-challenge-go/internal/domain/wager"
	"github.com/Pantani/backend-challenge-go/internal/domain/wallet"
	"github.com/Pantani/backend-challenge-go/internal/testutil"
)

func TestOpenWalletWithBalanceCreatesOpening(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	w := h.openWallet(t, "1000.00")
	assert.Equal(t, int64(1), w.Version())
	assert.Equal(t, 1, h.store.ledgerCount(w.ID()))
	assert.Equal(t, []string{event.TypeWagerTransactionProcessed, event.TypeWalletBalanceChanged}, h.store.outboxTypes())

	page, err := h.wallets.Ledger(context.Background(), w.ID(), "", 0)
	require.NoError(t, err)
	require.Len(t, page.Entries, 1)
	opening, err := h.wagers.Get(context.Background(), app.Caller{Internal: true}, page.Entries[0].TransactionID())
	require.NoError(t, err)
	assert.Equal(t, wager.KindOpening, opening.Kind())
	assert.Equal(t, wager.StatusProcessed, opening.Status())
	_, err = h.wagers.Get(context.Background(), app.Caller{ProviderID: "provider-a"}, opening.ID())
	require.ErrorIs(t, err, app.ErrTransactionNotFound, "providers never see internal operations")
}

func TestOpenWalletZeroBalanceAndDuplicates(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	player := uuid.New()
	cmd := app.OpenWalletCommand{PlayerID: player, InitialBalance: testutil.BRL(t, "0.00")}
	w, err := h.wallets.Open(context.Background(), cmd)
	require.NoError(t, err)
	assert.Zero(t, h.store.ledgerCount(w.ID()))
	assert.Empty(t, h.store.outboxTypes())

	_, err = h.wallets.Open(context.Background(), cmd)
	require.ErrorIs(t, err, app.ErrWalletExists)

	_, err = h.wallets.Open(context.Background(), app.OpenWalletCommand{PlayerID: player})
	require.ErrorIs(t, err, app.ErrValidation)
}

func TestOpenWalletRollsBack(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.store.failOn("ledger.append", errBoom)
	_, err := h.wallets.Open(context.Background(), app.OpenWalletCommand{PlayerID: uuid.New(), InitialBalance: testutil.BRL(t, "5.00")})
	require.ErrorIs(t, err, errBoom)
	assert.Empty(t, h.store.st.wallets)
}

func TestLedgerPagination(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	w := h.openWallet(t, "100.00")
	for _, ext := range []string{"a", "b", "c", "d"} {
		h.submit(t, w, op{ext: ext, kind: "BET", amount: "1.00"})
	}
	ctx := context.Background()
	first, err := h.wallets.Ledger(ctx, w.ID(), "", 2)
	require.NoError(t, err)
	require.Len(t, first.Entries, 2)
	require.NotEmpty(t, first.NextCursor)

	second, err := h.wallets.Ledger(ctx, w.ID(), first.NextCursor, 2)
	require.NoError(t, err)
	require.Len(t, second.Entries, 2)
	third, err := h.wallets.Ledger(ctx, w.ID(), second.NextCursor, 2)
	require.NoError(t, err)
	require.Len(t, third.Entries, 1)
	assert.Empty(t, third.NextCursor)
	assert.Equal(t, "96.00", third.Entries[0].BalanceAfter().Amount())
}

func TestLedgerValidation(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	w := h.openWallet(t, "1.00")
	ctx := context.Background()
	bad := []string{"%%%", base64.RawURLEncoding.EncodeToString([]byte("v2:1")), base64.RawURLEncoding.EncodeToString([]byte("v1:x")),
		base64.RawURLEncoding.EncodeToString([]byte("v1:-1"))}
	for _, cursor := range bad {
		_, err := h.wallets.Ledger(ctx, w.ID(), cursor, 10)
		assert.ErrorIs(t, err, app.ErrValidation, cursor)
	}
	for _, limit := range []int{-1, app.MaxLedgerLimit + 1} {
		_, err := h.wallets.Ledger(ctx, w.ID(), "", limit)
		assert.ErrorIs(t, err, app.ErrValidation)
	}
	_, err := h.wallets.Ledger(ctx, uuid.New(), "", 10)
	require.ErrorIs(t, err, app.ErrWalletNotFound)
	h.store.failOn("q.ledger", errBoom)
	_, err = h.wallets.Ledger(ctx, w.ID(), "", 10)
	require.ErrorIs(t, err, errBoom)
}

func TestReconcile(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	w := h.openWallet(t, "1000.00")
	h.submit(t, w, op{ext: "a", kind: "BET", amount: "25.00"})
	ctx := context.Background()

	rec, err := h.wallets.Reconcile(ctx, w.ID())
	require.NoError(t, err)
	assert.True(t, rec.Consistent)
	assert.Equal(t, "975.00", rec.Stored.Amount())
	assert.Equal(t, "975.00", rec.Calculated.Amount())
	assert.Equal(t, "0.00", rec.Difference.Amount())
	assert.Equal(t, int64(2), rec.CheckedEntries)

	h.store.putWalletBalance(w.ID(), func(s *wallet.Snapshot) { s.Balance = testutil.BRL(t, "980.00") })
	rec, err = h.wallets.Reconcile(ctx, w.ID())
	require.NoError(t, err)
	assert.False(t, rec.Consistent)
	assert.Equal(t, "5.00", rec.Difference.Amount())
	assert.Contains(t, h.logs.String(), "wallet reconciliation divergence")
	assert.Equal(t, "980.00", h.balance(t, w), "reconciliation never changes the balance")

	h.store.failOn("q.reconcile", errBoom)
	_, err = h.wallets.Reconcile(ctx, w.ID())
	require.ErrorIs(t, err, errBoom)
}

// reconcileStub returns a crafted snapshot to exercise overflow handling.
type reconcileStub struct {
	*memStore
	snap app.ReconciliationSnapshot
}

func (s reconcileStub) Reconcile(context.Context, uuid.UUID) (app.ReconciliationSnapshot, error) {
	return s.snap, nil
}

func TestReconcileOverflow(t *testing.T) {
	t.Parallel()
	maxM, err := money.FromMinor(math.MaxInt64, "BRL")
	require.NoError(t, err)
	snaps := []app.ReconciliationSnapshot{
		{Stored: testutil.BRL(t, "0.00"), NetMinor: math.MinInt64},
		{Stored: maxM, NetMinor: -math.MaxInt64},
	}
	for i, snap := range snaps {
		svc := app.NewWalletService(app.WalletDeps{Deps: app.Deps{Queries: reconcileStub{newMemStore(), snap}}})
		_, err := svc.Reconcile(context.Background(), uuid.New())
		assert.ErrorIs(t, err, money.ErrOverflow, "case %d", i)
	}
}

func TestMemReconcileExactAggregate(t *testing.T) {
	for _, tc := range []struct {
		name     string
		amounts  []int64
		want     int64
		overflow bool
	}{
		{"credit overflow", []int64{math.MaxInt64, 1}, 0, true},
		{"debit overflow", []int64{-math.MaxInt64, -2}, 0, true},
		{"intermediate overflow cancels", []int64{math.MaxInt64, 1, -math.MaxInt64}, 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			w := h.openWallet(t, "0.00")
			for _, amount := range tc.amounts {
				h.store.st.ledger = append(h.store.st.ledger, app.LedgerRow{Entry: reconciliationEntry(t, w.ID(), amount)})
			}
			got, err := h.store.Reconcile(context.Background(), w.ID())
			if tc.overflow {
				require.ErrorIs(t, err, money.ErrOverflow)
				assert.Zero(t, got.NetMinor)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got.NetMinor)
		})
	}
}

func reconciliationEntry(t *testing.T, id uuid.UUID, minor int64) wallet.LedgerEntry {
	t.Helper()
	direction := wallet.Credit
	before, after := int64(0), minor
	if minor < 0 {
		direction = wallet.Debit
		before, after = -minor, 0
		minor = -minor
	}
	amount, err := money.FromMinor(minor, "BRL")
	require.NoError(t, err)
	b, err := money.FromMinor(before, "BRL")
	require.NoError(t, err)
	a, err := money.FromMinor(after, "BRL")
	require.NoError(t, err)
	entry, err := wallet.NewLedgerEntry(wallet.LedgerEntryParams{ID: uuid.New(), WalletID: id, TransactionID: uuid.New(), Direction: direction, Amount: amount, BalanceBefore: b, BalanceAfter: a, CreatedAt: time.Now()})
	require.NoError(t, err)
	return entry
}
