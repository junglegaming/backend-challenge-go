package app_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Pantani/backend-challenge-go/internal/app"
	"github.com/Pantani/backend-challenge-go/internal/domain/wager"
)

func TestRefundAfterRolledBackRefundIsAlreadyReversed(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	w := h.openWallet(t, "100.00")
	h.submit(t, w, op{ext: "b", kind: "BET", amount: "30.00"})
	first := h.submit(t, w, op{ext: "r1", kind: "REFUND", amount: "30.00", ref: "b"})
	rollback := h.submit(t, w, op{ext: "rb", kind: "ROLLBACK", amount: "30.00", ref: "r1"})
	second := h.submit(t, w, op{ext: "r2", kind: "REFUND", amount: "30.00", ref: "b"})

	assert.Equal(t, wager.StatusProcessed, first.Transaction.Status())
	assert.Equal(t, wager.StatusProcessed, rollback.Transaction.Status())
	assert.Equal(t, wager.CodeAlreadyReversed, second.Transaction.FailureCode())
	assert.Equal(t, "70.00", h.balance(t, w))
}

func TestRejectedReferenceWakesDependents(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	w := h.openWallet(t, "10.00")
	refund := h.submit(t, w, op{ext: "r", kind: "REFUND", amount: "30.00", ref: "b"})
	require.Equal(t, wager.StatusPendingReference, refund.Transaction.Status())

	bet := h.submit(t, w, op{ext: "b", kind: "BET", amount: "30.00"})
	require.Equal(t, wager.StatusRejected, bet.Transaction.Status())
	assert.Equal(t, 1, h.resolve(t), "the rejection woke the refund up")
	got, err := h.wagers.Get(context.Background(), app.Caller{Internal: true}, refund.Transaction.ID())
	require.NoError(t, err)
	assert.Equal(t, wager.StatusRejected, got.Status())
	assert.Equal(t, wager.CodeReferenceNotProcessed, got.FailureCode())
	assert.Equal(t, "10.00", h.balance(t, w))
}

func TestReplayOfPendingReference(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	w := h.openWallet(t, "100.00")
	pending := h.submit(t, w, op{ext: "r", kind: "REFUND", amount: "30.00", ref: "missing"})
	replay := h.submit(t, w, op{ext: "r", kind: "REFUND", amount: "30.00", ref: "missing"})
	assert.True(t, replay.Replay)
	assert.Equal(t, pending.Transaction.ID(), replay.Transaction.ID())
	assert.Equal(t, wager.StatusPendingReference, replay.Transaction.Status())
	assert.Equal(t, 1, h.store.txCount()-1, "no second row (opening excluded)")
}

func TestSameKeyOnAnotherWalletIsAConflict(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	a, b := h.openWallet(t, "100.00"), h.openWallet(t, "100.00")
	h.submit(t, a, op{ext: "t", key: "k", kind: "BET", amount: "1.00"})
	_, err := h.wagers.Submit(context.Background(), h.cmd(t, b, op{ext: "t", key: "k", kind: "BET", amount: "1.00"}))
	require.ErrorIs(t, err, app.ErrIdempotencyConflict)
	assert.Equal(t, "100.00", h.balance(t, b))
}

func TestOwnershipRejections(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	w := h.openWallet(t, "100.00")
	usd := h.submit(t, w, op{ext: "usd", kind: "BET", amount: "1.00", currency: "USD"})
	assert.Equal(t, wager.CodeCurrencyMismatch, usd.Transaction.FailureCode())
	assert.Equal(t, "BRL", string(usd.Transaction.ResultBalance().Currency()), "observed balance is in the wallet currency")
	assert.Equal(t, "100.00", usd.Transaction.ResultBalance().Amount())

	cmd := h.cmd(t, w, op{ext: "other", kind: "BET", amount: "1.00"})
	cmd.PlayerID = uuid.New()
	res, err := h.wagers.Submit(context.Background(), cmd)
	require.NoError(t, err)
	assert.Equal(t, wager.CodePlayerWalletMismatch, res.Transaction.FailureCode())
	assert.Equal(t, "100.00", h.balance(t, w))
}

func TestWinReferences(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	w := h.openWallet(t, "100.00")
	h.submit(t, w, op{ext: "b", kind: "BET", amount: "10.00"})
	ok := h.submit(t, w, op{ext: "w1", kind: "WIN", amount: "20.00", ref: "b"})
	assert.Equal(t, wager.StatusProcessed, ok.Transaction.Status())
	assert.Equal(t, "110.00", ok.Transaction.ResultBalance().Amount())

	toWin := h.submit(t, w, op{ext: "w2", kind: "WIN", amount: "5.00", ref: "w1"})
	assert.Equal(t, wager.CodeReferenceKindInvalid, toWin.Transaction.FailureCode())
	missing := h.submit(t, w, op{ext: "w3", kind: "WIN", amount: "5.00", ref: "nope"})
	assert.Equal(t, wager.StatusPendingReference, missing.Transaction.Status())
	assert.Equal(t, "110.00", h.balance(t, w))
}

func TestResolveDueHonoursBatchSize(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	w := h.openWallet(t, "100.00")
	for i := range 11 {
		h.submit(t, w, op{ext: fmt.Sprintf("r-%d", i), kind: "REFUND", amount: "1.00", ref: "missing"})
	}
	h.clock.Advance(time.Hour)
	assert.Equal(t, 10, h.resolve(t), "BatchSize")
	assert.Equal(t, 1, h.resolve(t))
	assert.Zero(t, h.resolve(t))
}

func TestStaleWalletVersionIsRetried(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	w := h.openWallet(t, "100.00")
	h.store.failOn("wallets.save", app.ErrConflict)
	res := h.submit(t, w, op{ext: "b", kind: "BET", amount: "1.00"})
	assert.Equal(t, wager.StatusProcessed, res.Transaction.Status())
	assert.Equal(t, "99.00", h.balance(t, w))
	assert.Equal(t, 1.0, h.counter(t, "wallet_concurrency_conflicts_total", map[string]string{"operation": "submit"}))
}

func TestCommitFailureLeavesNothingBehind(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	w := h.openWallet(t, "100.00")
	before := h.store.outboxCount()
	h.store.failOn("uow.commit", errBoom)
	_, err := h.wagers.Submit(context.Background(), h.cmd(t, w, op{ext: "b", kind: "BET", amount: "1.00"}))
	require.ErrorIs(t, err, errBoom)
	assert.Equal(t, "100.00", h.balance(t, w))
	assert.Equal(t, 1, h.store.ledgerCount(w.ID()))
	assert.Equal(t, before, h.store.outboxCount())
	assert.Equal(t, 1, h.store.txCount(), "only the opening")
}

func TestGetQueriesPropagateErrors(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	w := h.openWallet(t, "100.00")
	res := h.submit(t, w, op{ext: "b", kind: "BET", amount: "1.00"})
	ctx, caller := context.Background(), app.Caller{Internal: true}

	h.store.failOn("q.tx", errBoom)
	_, err := h.wagers.Get(ctx, caller, res.Transaction.ID())
	require.ErrorIs(t, err, errBoom)
	h.store.failOn("q.txByExternal", errBoom)
	_, err = h.wagers.GetByExternal(ctx, caller, "provider-a", "b")
	require.ErrorIs(t, err, errBoom)
	_, err = h.wagers.GetByExternal(ctx, caller, "provider-a", "nope")
	require.ErrorIs(t, err, app.ErrTransactionNotFound)
}
