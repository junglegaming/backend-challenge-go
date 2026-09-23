package app_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Pantani/backend-challenge-go/internal/app"
	"github.com/Pantani/backend-challenge-go/internal/domain/event"
	"github.com/Pantani/backend-challenge-go/internal/domain/wager"
)

var errBoom = errors.New("boom")

func TestSubmitBetDebitsOnceAndEmitsEvents(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	w := h.openWallet(t, "1000.00")

	res := h.submit(t, w, op{ext: "t-1", kind: "BET", amount: "25.00"})
	assert.False(t, res.Replay)
	assert.Equal(t, wager.StatusProcessed, res.Transaction.Status())
	assert.Equal(t, "975.00", res.Transaction.ResultBalance().Amount())
	assert.Equal(t, "975.00", h.balance(t, w))
	assert.Equal(t, 2, h.store.ledgerCount(w.ID()))
	assert.Equal(t, []string{
		event.TypeWagerTransactionProcessed, event.TypeWalletBalanceChanged,
		event.TypeWagerTransactionProcessed, event.TypeWalletBalanceChanged,
	}, h.store.outboxTypes())
}

func TestReplayReturnsOriginalResult(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	w := h.openWallet(t, "1000.00")
	first := h.submit(t, w, op{ext: "t-1", kind: "BET", amount: "25.00"})
	h.submit(t, w, op{ext: "t-2", kind: "BET", amount: "100.00"})

	replay := h.submit(t, w, op{ext: "t-1", kind: "BET", amount: "25.00"})
	assert.True(t, replay.Replay)
	assert.Equal(t, 1.0, h.counter(t, "wager_duplicates_total", map[string]string{"source": app.SourceHTTP}))
	assert.Equal(t, 3.0, h.counter(t, "wager_transactions_total", map[string]string{"source": app.SourceHTTP}))
	assert.Equal(t, first.Transaction.ID(), replay.Transaction.ID())
	assert.Equal(t, "975.00", replay.Transaction.ResultBalance().Amount(), "balance observed originally")
	assert.Equal(t, "875.00", h.balance(t, w))
	assert.Equal(t, 3, h.store.ledgerCount(w.ID()))
}

func TestIdempotencyConflicts(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	w := h.openWallet(t, "1000.00")
	h.submit(t, w, op{ext: "t-1", key: "k-1", kind: "BET", amount: "25.00"})

	_, err := h.wagers.Submit(context.Background(), h.cmd(t, w, op{ext: "t-1", key: "k-1", kind: "BET", amount: "26.00"}))
	require.ErrorIs(t, err, app.ErrIdempotencyConflict)
	_, err = h.wagers.Submit(context.Background(), h.cmd(t, w, op{ext: "t-1", key: "k-2", kind: "BET", amount: "25.00"}))
	require.ErrorIs(t, err, app.ErrDuplicateExternalID)
	assert.Equal(t, "975.00", h.balance(t, w))
}

func TestSubmitUnknownWallet(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	w := h.openWallet(t, "1.00")
	cmd := h.cmd(t, w, op{ext: "t-1", kind: "BET", amount: "1.00"})
	cmd.WalletID = uuid.New()
	_, err := h.wagers.Submit(context.Background(), cmd)
	require.ErrorIs(t, err, app.ErrWalletNotFound)
}

func TestConcurrentBetsOnSameBalance(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	w := h.openWallet(t, "100.00")
	a := h.submit(t, w, op{ext: "a", kind: "BET", amount: "80.00"})
	b := h.submit(t, w, op{ext: "b", kind: "BET", amount: "80.00"})

	assert.Equal(t, wager.StatusProcessed, a.Transaction.Status())
	assert.Equal(t, wager.StatusRejected, b.Transaction.Status())
	assert.Equal(t, wager.CodeInsufficientFunds, b.Transaction.FailureCode())
	assert.Equal(t, "20.00", b.Transaction.ResultBalance().Amount())
	assert.Equal(t, "20.00", h.balance(t, w))
	assert.Equal(t, 2, h.store.ledgerCount(w.ID()), "opening + one debit")
	assert.Contains(t, h.store.outboxTypes(), event.TypeWagerTransactionRejected)
}

func TestLossAndWin(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	w := h.openWallet(t, "10.00")
	loss := h.submit(t, w, op{ext: "l", kind: "LOSS", amount: "0.00"})
	assert.Equal(t, wager.StatusProcessed, loss.Transaction.Status())
	got, err := h.wallets.Get(context.Background(), w.ID())
	require.NoError(t, err)
	assert.Equal(t, int64(1), got.Version(), "LOSS does not change the version")
	assert.Equal(t, 1, h.store.ledgerCount(w.ID()))

	win := h.submit(t, w, op{ext: "w", kind: "WIN", amount: "5.00"})
	assert.Equal(t, "15.00", win.Transaction.ResultBalance().Amount())
}

func TestReversalArrivingBeforeReference(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	w := h.openWallet(t, "100.00")

	refund := h.submit(t, w, op{ext: "r", kind: "REFUND", amount: "30.00", ref: "b"})
	assert.Equal(t, wager.StatusPendingReference, refund.Transaction.Status())
	assert.Contains(t, h.store.outboxTypes(), event.TypeWagerTransactionPendingReference)
	assert.Zero(t, h.resolve(t), "not due yet")

	h.submit(t, w, op{ext: "b", kind: "BET", amount: "30.00"})
	assert.Equal(t, 1, h.resolve(t), "the bet woke the refund up")

	got, err := h.wagers.Get(context.Background(), app.Caller{Internal: true}, refund.Transaction.ID())
	require.NoError(t, err)
	assert.Equal(t, wager.StatusProcessed, got.Status())
	assert.Equal(t, "100.00", h.balance(t, w))

	replay := h.submit(t, w, op{ext: "r", kind: "REFUND", amount: "30.00", ref: "b"})
	assert.True(t, replay.Replay)
	assert.Equal(t, wager.StatusProcessed, replay.Transaction.Status())
}

func TestPendingReferenceExpires(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	w := h.openWallet(t, "100.00")
	refund := h.submit(t, w, op{ext: "r", kind: "REFUND", amount: "30.00", ref: "missing"})

	for range 3 {
		h.clock.Advance(time.Hour)
		assert.Equal(t, 1, h.resolve(t))
	}
	got, err := h.wagers.Get(context.Background(), app.Caller{ProviderID: "provider-a"}, refund.Transaction.ID())
	require.NoError(t, err)
	assert.Equal(t, wager.StatusRejected, got.Status())
	assert.Equal(t, wager.CodeReferenceNotFound, got.FailureCode())
	assert.Equal(t, 3, got.Attempts())
}

func TestPendingReferenceExpiresWhileReferenceIsPending(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	w := h.openWallet(t, "100.00")
	refund := h.submit(t, w, op{ext: "r", kind: "REFUND", amount: "30.00", ref: "missing"})
	rollback := h.submit(t, w, op{ext: "rb", kind: "ROLLBACK", amount: "30.00", ref: "r"})
	assert.Equal(t, wager.StatusPendingReference, rollback.Transaction.Status())
	// Keep the referenced refund pending while the rollback expires.
	h.store.putTx(refund.Transaction.ID(), func(s *wager.Snapshot) { s.NextAttemptAt = t0.Add(1000 * time.Hour) })

	for range 3 {
		h.clock.Advance(time.Hour)
		h.resolve(t)
	}
	got, err := h.wagers.GetByExternal(context.Background(), app.Caller{ProviderID: "provider-a"}, "provider-a", "rb")
	require.NoError(t, err)
	assert.Equal(t, wager.CodeReferenceNotProcessed, got.FailureCode())
}

func TestDoubleReversalIsRejected(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	w := h.openWallet(t, "100.00")
	h.submit(t, w, op{ext: "b", kind: "BET", amount: "30.00"})
	first := h.submit(t, w, op{ext: "r1", kind: "REFUND", amount: "30.00", ref: "b"})
	second := h.submit(t, w, op{ext: "r2", kind: "ROLLBACK", amount: "30.00", ref: "b"})

	assert.Equal(t, wager.StatusProcessed, first.Transaction.Status())
	assert.Equal(t, wager.CodeAlreadyReversed, second.Transaction.FailureCode())
	assert.Equal(t, "100.00", h.balance(t, w))
}

func TestRollbackOfWinWithoutFunds(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	w := h.openWallet(t, "0.00")
	h.submit(t, w, op{ext: "w", kind: "WIN", amount: "50.00"})
	h.submit(t, w, op{ext: "b", kind: "BET", amount: "40.00"})
	res := h.submit(t, w, op{ext: "rb", kind: "ROLLBACK", amount: "50.00", ref: "w"})
	assert.Equal(t, wager.CodeReversalInsufficientFunds, res.Transaction.FailureCode())
	assert.Equal(t, "10.00", h.balance(t, w))
}

func TestSubmitRetriesConflicts(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	w := h.openWallet(t, "100.00")
	h.store.failOn("txs.create", app.ErrConflict)
	res := h.submit(t, w, op{ext: "b", kind: "BET", amount: "1.00"})
	assert.Equal(t, wager.StatusProcessed, res.Transaction.Status())
	assert.Equal(t, 1.0, h.counter(t, "wallet_concurrency_conflicts_total", map[string]string{"operation": "submit"}))

	h.store.failOn("txs.create", app.ErrConflict, app.ErrConflict, app.ErrConflict)
	_, err := h.wagers.Submit(context.Background(), h.cmd(t, w, op{ext: "c", kind: "BET", amount: "1.00"}))
	require.ErrorIs(t, err, app.ErrConflict)
	assert.True(t, app.IsTransient(err))
}

func TestSubmitRollsBackOnPersistenceFailure(t *testing.T) {
	t.Parallel()
	for _, failing := range []string{"wallets.save", "ledger.append", "outbox.append", "txs.wake", "txs.find", "wallets.get", "txs.create"} {
		h := newHarness(t)
		w := h.openWallet(t, "100.00")
		h.store.failOn(failing, errBoom)
		_, err := h.wagers.Submit(context.Background(), h.cmd(t, w, op{ext: "b", kind: "BET", amount: "1.00"}))
		require.ErrorIs(t, err, errBoom, failing)
		assert.Equal(t, "100.00", h.balance(t, w), failing)
		assert.Equal(t, 1, h.store.ledgerCount(w.ID()), failing)
		assert.Equal(t, 1, h.store.txCount(), "only the opening remains: %s", failing)
		assert.Equal(t, 2, h.store.outboxCount(), "only the opening events remain: %s", failing)
	}
}

func TestSubmitReferenceLookupFailures(t *testing.T) {
	t.Parallel()
	for _, failing := range []string{"txs.getByExternal", "txs.reversed"} {
		h := newHarness(t)
		w := h.openWallet(t, "100.00")
		h.submit(t, w, op{ext: "b", kind: "BET", amount: "1.00"})
		h.store.failOn(failing, errBoom)
		_, err := h.wagers.Submit(context.Background(), h.cmd(t, w, op{ext: "r", kind: "REFUND", amount: "1.00", ref: "b"}))
		require.ErrorIs(t, err, errBoom, failing)
	}
}

func TestSubmitInvalidCommandAndMovementFailure(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	w := h.openWallet(t, "100.00")
	cmd := h.cmd(t, w, op{ext: "b", kind: "BET", amount: "1.00"})
	cmd.RoundID = ""
	_, err := h.wagers.Submit(context.Background(), cmd)
	require.ErrorIs(t, err, wager.ErrInvalidTransaction)

	h.ids.failNextID(2) // transaction id, then the ledger entry id
	_, err = h.wagers.Submit(context.Background(), h.cmd(t, w, op{ext: "b", kind: "BET", amount: "1.00"}))
	require.Error(t, err)
	assert.Equal(t, "100.00", h.balance(t, w))
}

func TestTransactionVisibility(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	w := h.openWallet(t, "100.00")
	res := h.submit(t, w, op{ext: "b", kind: "BET", amount: "1.00"})
	ctx := context.Background()
	id := res.Transaction.ID()

	_, err := h.wagers.Get(ctx, app.Caller{ProviderID: "provider-a"}, id)
	require.NoError(t, err)
	_, err = h.wagers.Get(ctx, app.Caller{ProviderID: "provider-b"}, id)
	require.ErrorIs(t, err, app.ErrTransactionNotFound)
	_, err = h.wagers.Get(ctx, app.Caller{}, id)
	require.ErrorIs(t, err, app.ErrTransactionNotFound)
	_, err = h.wagers.Get(ctx, app.Caller{Internal: true}, uuid.New())
	require.ErrorIs(t, err, app.ErrTransactionNotFound)

	_, err = h.wagers.GetByExternal(ctx, app.Caller{ProviderID: "provider-b"}, "provider-a", "b")
	require.ErrorIs(t, err, app.ErrForbidden)
	got, err := h.wagers.GetByExternal(ctx, app.Caller{Internal: true}, "provider-a", "b")
	require.NoError(t, err)
	assert.Equal(t, id, got.ID())
}
