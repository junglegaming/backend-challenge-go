package app_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Pantani/backend-challenge-go/internal/app"
	"github.com/Pantani/backend-challenge-go/internal/domain/event"
	"github.com/Pantani/backend-challenge-go/internal/domain/wager"
	"github.com/Pantani/backend-challenge-go/internal/testutil"
)

func pendingRefund(t *testing.T, h *harness, ref string) app.SubmitResult {
	t.Helper()
	w := h.openWallet(t, "100.00")
	h.submit(t, w, op{ext: "b", kind: "BET", amount: "30.00"})
	res := h.submit(t, w, op{ext: "r", kind: "REFUND", amount: "30.00", ref: ref})
	require.Equal(t, wager.StatusPendingReference, res.Transaction.Status())
	h.clock.Advance(time.Hour)
	return res
}

func (h *harness) transaction(t *testing.T, res app.SubmitResult) *wager.Transaction {
	t.Helper()
	got, err := h.wagers.Get(context.Background(), app.Caller{Internal: true}, res.Transaction.ID())
	require.NoError(t, err)
	return got
}

func (h *harness) status(t *testing.T, res app.SubmitResult) wager.Status {
	t.Helper()
	return h.transaction(t, res).Status()
}

func TestResolveDueListingFailureAndCancellation(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.store.failOn("q.due", errBoom)
	_, err := h.wagers.ResolveDue(context.Background())
	require.ErrorIs(t, err, errBoom)

	pendingRefund(t, h, "missing")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	n, err := h.wagers.ResolveDue(ctx)
	require.ErrorIs(t, err, context.Canceled)
	assert.Zero(t, n)
}

func TestResolveDueSkipsClaimedAndTransientFailures(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	res := pendingRefund(t, h, "missing")

	h.store.failOn("txs.lock", app.ErrNotDue)
	assert.Zero(t, h.resolve(t))
	h.store.failOn("txs.lock", app.ErrUnavailable)
	assert.Zero(t, h.resolve(t))
	assert.Contains(t, h.logs.String(), "pending reference retry postponed")
	assert.Equal(t, wager.StatusPendingReference, h.status(t, res))
}

func TestResolveDuePermanentFailureMarksFailed(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	res := pendingRefund(t, h, "missing")
	w, err := h.wallets.Get(context.Background(), res.Transaction.WalletID())
	require.NoError(t, err)
	dependent := h.submit(t, w, op{ext: "rb", kind: "ROLLBACK", amount: "30.00", ref: "r"})
	require.Equal(t, wager.StatusPendingReference, dependent.Transaction.Status())
	h.store.failOn("txs.getByExternal", errBoom)

	assert.Zero(t, h.resolve(t))
	assert.Equal(t, wager.StatusFailed, h.status(t, res))
	assert.Equal(t, wager.CodeInternalFailure, h.transaction(t, res).FailureCode())
	assert.Contains(t, h.store.outboxTypes(), event.TypeWagerTransactionFailed)
	assert.Equal(t, 1.0, h.counter(t, "wager_pending_resolutions_total", map[string]string{"status": "FAILED"}))
	assert.Equal(t, 1, h.resolve(t), "the failure woke the dependent rollback up")
	assert.Equal(t, wager.CodeReferenceNotProcessed, h.transaction(t, dependent).FailureCode())
}

func TestResolveDueObservesWorkerSource(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	pendingRefund(t, h, "missing")
	assert.Equal(t, 1, h.resolve(t))
	labels := map[string]string{"source": app.SourceWorker, "status": string(wager.StatusPendingReference)}
	assert.Equal(t, 1.0, h.counter(t, "wager_transactions_total", labels))
	assert.Equal(t, 1.0, h.counter(t, "wager_processing_seconds", map[string]string{"source": app.SourceWorker}))
	assert.Equal(t, 1.0, h.counter(t, "wager_pending_resolutions_total", map[string]string{"status": "PENDING_REFERENCE"}))
}

func TestRetryConflictsStopsOnCancelledContext(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	w := h.openWallet(t, "100.00")
	h.store.failOn("txs.create", app.ErrConflict, app.ErrConflict)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := h.wagers.Submit(ctx, h.cmd(t, w, op{ext: "b", kind: "BET", amount: "1.00"}))
	require.ErrorIs(t, err, app.ErrConflict)
	require.ErrorIs(t, err, context.Canceled)
	assert.Zero(t, h.counter(t, "wallet_concurrency_conflicts_total", nil), "no retry after cancellation")
}

func TestMarkFailedCanFail(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	res := pendingRefund(t, h, "missing")
	h.store.failOn("wallets.get", errBoom)
	h.store.failOn("txs.lock", app.ErrNotDue)

	assert.Zero(t, h.resolve(t))
	assert.Contains(t, h.logs.String(), "could not mark pending reference as failed")
	assert.Equal(t, wager.StatusPendingReference, h.status(t, res))
}

func TestDefensiveTransitionsOnTerminalRows(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"await", "process"} {
		h := newHarness(t)
		res := pendingRefund(t, h, "missing")
		if name == "process" {
			// Make the reference processable after the refund was deferred.
			h.store.putTx(res.Transaction.ID(), func(s *wager.Snapshot) { s.External.ReferenceExternalID = "b" })
		}
		h.store.lockMutate = func(s *wager.Snapshot) {
			s.Status, s.ResultBalance = wager.StatusProcessed, testutil.BRL(t, "1.00")
			s.ReferenceTxID = uuid.New()
		}
		assert.Zero(t, h.resolve(t), name)
		assert.Contains(t, h.logs.String(), "could not mark pending reference as failed", name)
	}
}

func TestDefensiveRejectOnTerminalRow(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	res := pendingRefund(t, h, "missing")
	h.store.putTx(res.Transaction.ID(), func(s *wager.Snapshot) { s.Attempts = 99 })
	h.store.lockMutate = func(s *wager.Snapshot) {
		s.Status, s.FailureCode = wager.StatusRejected, wager.CodeReferenceNotFound
		s.ResultBalance = testutil.BRL(t, "1.00")
	}
	assert.Zero(t, h.resolve(t))
	assert.Contains(t, h.logs.String(), "pending reference failed permanently")
}

func TestPendingPolicyBackoff(t *testing.T) {
	t.Parallel()
	p := app.PendingPolicy{BaseDelay: time.Second, MaxDelay: time.Minute}
	assert.Equal(t, t0.Add(time.Second), p.Next(0, t0))
	assert.Equal(t, t0.Add(8*time.Second), p.Next(3, t0))
	assert.Equal(t, t0.Add(time.Minute), p.Next(10, t0))
	assert.Equal(t, t0.Add(time.Minute), p.Next(1000, t0))
	assert.Equal(t, t0.Add(time.Minute), app.PendingPolicy{MaxDelay: time.Minute}.Next(1, t0))
	overflowing := app.PendingPolicy{BaseDelay: time.Duration(1<<62 + 1), MaxDelay: time.Hour}
	assert.Equal(t, t0.Add(time.Hour), overflowing.Next(2, t0), "an overflowed shift falls back to the cap")
}

func TestBackoff(t *testing.T) {
	t.Parallel()
	assert.Equal(t, time.Second, app.Backoff(time.Second, time.Minute, 0))
	assert.Equal(t, time.Second, app.Backoff(time.Second, time.Minute, -3), "negative exponents are clamped")
	assert.Equal(t, 8*time.Second, app.Backoff(time.Second, time.Minute, 3))
	assert.Equal(t, time.Minute, app.Backoff(time.Second, time.Minute, 6), "capped at the limit")
	assert.Equal(t, time.Minute, app.Backoff(time.Second, time.Minute, 1000), "shift is bounded")
	assert.Equal(t, time.Minute, app.Backoff(0, time.Minute, 1), "no base means the limit")
	assert.Equal(t, time.Hour, app.Backoff(time.Duration(1<<62+1), time.Hour, 2), "overflow falls back to the limit")
	assert.Equal(t, time.Hour, app.Backoff(1<<62, time.Hour, 2), "overflow to zero falls back to the limit")
}

func TestIsTransient(t *testing.T) {
	t.Parallel()
	for _, err := range []error{app.ErrConflict, app.ErrUnavailable, context.Canceled, context.DeadlineExceeded} {
		assert.True(t, app.IsTransient(err), err)
		assert.True(t, app.IsTransient(fmt.Errorf("wrapped: %w", err)), err)
	}
	assert.True(t, app.IsTransient(errors.Join(errBoom, app.ErrUnavailable)))
	assert.False(t, app.IsTransient(app.ErrValidation))
	assert.False(t, app.IsTransient(errBoom))
}
