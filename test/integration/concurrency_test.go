//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Pantani/backend-challenge-go/internal/adapter/postgres"
	"github.com/Pantani/backend-challenge-go/internal/app"
	"github.com/Pantani/backend-challenge-go/internal/domain/wager"
	"github.com/Pantani/backend-challenge-go/internal/domain/wallet"
	"github.com/Pantani/backend-challenge-go/internal/testutil"
	"github.com/Pantani/backend-challenge-go/test/testenv"
)

type outcome struct {
	res app.SubmitResult
	err error
}

func (s services) submitAsync(w *wallet.Wallet, ext, kind, amount, ref string) outcome {
	cmd, err := app.NewSubmitCommand(s.input(w, "provider-a", ext, kind, amount, ref))
	if err != nil {
		return outcome{err: err}
	}
	res, err := s.wagers.Submit(context.Background(), cmd)
	return outcome{res: res, err: err}
}

func TestFiftyIdenticalBetsDebitOnce(t *testing.T) {
	t.Parallel()
	s := newServices(t, defaultPolicy)
	w := s.openWallet(t, "1000.00")
	results, err := testenv.Parallel(50, func(int) (outcome, error) {
		result := s.submitAsync(w, "same-bet", "BET", "25.00", "")
		return result, result.err
	})
	require.NoError(t, err)

	ids, replays := map[string]int{}, 0
	for _, r := range results {
		ids[r.res.Transaction.ID().String()]++
		replays += boolToInt(r.res.Replay)
	}
	assert.Len(t, ids, 1, "every delivery resolves to the same transaction")
	assert.Equal(t, 49, replays, "the application deduplicated 49 deliveries")
	assert.Equal(t, 1, s.debits(t, w))
	assert.Equal(t, "975.00", s.balance(t, w))
	s.requireConsistent(t, w)
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func TestTwoBetsRaceOnSameBalance(t *testing.T) {
	t.Parallel()
	s := newServices(t, defaultPolicy)
	w := s.openWallet(t, "100.00")
	results, err := testenv.Parallel(2, func(i int) (outcome, error) {
		result := s.submitAsync(w, []string{"bet-a", "bet-b"}[i], "BET", "80.00", "")
		return result, result.err
	})
	require.NoError(t, err)

	statuses := map[wager.Status]int{}
	for _, r := range results {
		statuses[r.res.Transaction.Status()]++
	}
	assert.Equal(t, map[wager.Status]int{wager.StatusProcessed: 1, wager.StatusRejected: 1}, statuses)
	assert.Equal(t, "20.00", s.balance(t, w))
	assert.Equal(t, 1, s.debits(t, w))

	for _, ext := range []string{"bet-a", "bet-b"} {
		r := s.submitAsync(w, ext, "BET", "80.00", "")
		require.NoError(t, r.err)
		assert.True(t, r.res.Replay, "resending does not change the result")
	}
	assert.Equal(t, "20.00", s.balance(t, w))
	s.requireConsistent(t, w)
}

func TestIndependentWalletsProgressInParallel(t *testing.T) {
	t.Parallel()
	s := newServices(t, defaultPolicy)
	locked, free := s.openWallet(t, "100.00"), s.openWallet(t, "100.00")

	// Hold the row lock of one wallet in an open transaction.
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, `SELECT 1 FROM wallets WHERE id = $1 FOR UPDATE`, locked.ID())
	require.NoError(t, err)

	start := time.Now()
	r := s.submitAsync(free, "free-bet", "BET", "10.00", "")
	require.NoError(t, r.err)
	assert.Less(t, time.Since(start), 2*time.Second, "another wallet is not blocked by the lock")

	blocked := make(chan outcome, 1)
	go func() { blocked <- s.submitAsync(locked, "locked-bet", "BET", "10.00", "") }()
	select {
	case <-blocked:
		t.Fatal("the locked wallet must wait for its lock")
	case <-time.After(300 * time.Millisecond):
	}
	require.NoError(t, tx.Rollback(ctx))
	got := <-blocked
	require.NoError(t, got.err)
	assert.Equal(t, "90.00", s.balance(t, locked))
}

func TestManyWalletsConcurrently(t *testing.T) {
	t.Parallel()
	s := newServices(t, defaultPolicy)
	initialBalance := testutil.BRL(t, "100.00")
	wallets, err := testenv.Parallel(10, func(int) (*wallet.Wallet, error) {
		return s.wallets.Open(context.Background(), app.OpenWalletCommand{PlayerID: uuid.New(), InitialBalance: initialBalance})
	})
	require.NoError(t, err)
	results, err := testenv.Parallel(100, func(i int) (outcome, error) {
		result := s.submitAsync(wallets[i%10], fmt.Sprintf("multi-%d", i), "BET", "10.00", "")
		return result, result.err
	})
	require.NoError(t, err)
	for _, r := range results {
		require.Equal(t, wager.StatusProcessed, r.res.Transaction.Status())
	}
	for _, w := range wallets {
		assert.Equal(t, "0.00", s.balance(t, w))
		s.requireConsistent(t, w)
	}
}

func TestConcurrentReversalsOnlyOneSucceeds(t *testing.T) {
	t.Parallel()
	s := newServices(t, defaultPolicy)
	w := s.openWallet(t, "100.00")
	s.submit(t, w, "rev-bet", "BET", "40.00", "")
	results, err := testenv.Parallel(2, func(i int) (outcome, error) {
		result := s.submitAsync(w, []string{"rev-refund", "rev-rollback"}[i], []string{"REFUND", "ROLLBACK"}[i], "40.00", "rev-bet")
		return result, result.err
	})
	require.NoError(t, err)
	codes := map[wager.FailureCode]int{}
	for _, r := range results {
		codes[r.res.Transaction.FailureCode()]++
	}
	assert.Equal(t, map[wager.FailureCode]int{"": 1, wager.CodeAlreadyReversed: 1}, codes)
	assert.Equal(t, "100.00", s.balance(t, w), "the debit is returned only once")
	s.requireConsistent(t, w)
}

func TestPendingReferenceResolvedOnceByCompetingWorkers(t *testing.T) {
	t.Parallel()
	s := newServices(t, defaultPolicy)
	w := s.openWallet(t, "100.00")
	refund := s.submit(t, w, "late-refund", "REFUND", "30.00", "late-bet")
	require.Equal(t, wager.StatusPendingReference, refund.Transaction.Status())
	s.submit(t, w, "late-bet", "BET", "30.00", "")

	// Three instances race for the same due operation (they may also pick
	// up pending rows of other tests sharing the database).
	workers := []services{s, newServices(t, defaultPolicy), newServices(t, defaultPolicy)}
	_, err := testenv.Parallel(3, func(i int) (struct{}, error) {
		_, err := workers[i].wagers.ResolveDue(context.Background())
		return struct{}{}, err
	})
	require.NoError(t, err)
	var credits int
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM ledger_entries WHERE transaction_id = $1`, refund.Transaction.ID()).Scan(&credits))
	assert.Equal(t, 1, credits, "the refund was applied exactly once")

	got, err := s.wagers.Get(context.Background(), app.Caller{Internal: true}, refund.Transaction.ID())
	require.NoError(t, err)
	assert.Equal(t, wager.StatusProcessed, got.Status())
	assert.Equal(t, "100.00", s.balance(t, w))
	s.requireConsistent(t, w)
}

func TestPendingReferenceExpiresWithRejection(t *testing.T) {
	t.Parallel()
	s := newServices(t, app.PendingPolicy{BaseDelay: 10 * time.Millisecond, MaxDelay: 20 * time.Millisecond, MaxAttempts: 2, BatchSize: 500})
	w := s.openWallet(t, "100.00")
	refund := s.submit(t, w, "orphan-refund", "REFUND", "30.00", "never-arrives")

	require.Eventually(t, func() bool {
		_, _ = s.wagers.ResolveDue(context.Background())
		got, err := s.wagers.Get(context.Background(), app.Caller{Internal: true}, refund.Transaction.ID())
		return err == nil && got.Status() == wager.StatusRejected
	}, 10*time.Second, 50*time.Millisecond)
	got, err := s.wagers.Get(context.Background(), app.Caller{Internal: true}, refund.Transaction.ID())
	require.NoError(t, err)
	assert.Equal(t, wager.CodeReferenceNotFound, got.FailureCode())

	var events int
	require.NoError(t, pool.QueryRow(context.Background(), `SELECT count(*) FROM outbox_events
		WHERE aggregate_id = $1 AND event_type IN ('WagerTransactionPendingReference', 'WagerTransactionRejected')`,
		refund.Transaction.ID()).Scan(&events))
	assert.Equal(t, 2, events)
}

func TestStaleWriterGetsConflict(t *testing.T) {
	t.Parallel()
	s := newServices(t, defaultPolicy)
	w := s.openWallet(t, "100.00")
	err := postgres.NewUnitOfWork(pool).Do(context.Background(), func(ctx context.Context, r app.Repositories) error {
		stale, err := r.Wallets().GetForUpdate(ctx, w.ID())
		require.NoError(t, err)
		return r.Wallets().Save(ctx, stale, stale.Version()+10)
	})
	require.ErrorIs(t, err, app.ErrConflict)
}

func TestLockTimeoutIsTransient(t *testing.T) {
	t.Parallel()
	s := newServices(t, defaultPolicy)
	w := s.openWallet(t, "100.00")
	short, err := postgres.NewPool(context.Background(), postgres.Config{URL: env.DatabaseURL, MaxConns: 4,
		LockTimeout: 100 * time.Millisecond, StatementTimeout: time.Second})
	require.NoError(t, err)
	defer short.Close()

	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(ctx) }()
	_, err = tx.Exec(ctx, `SELECT 1 FROM wallets WHERE id = $1 FOR UPDATE`, w.ID())
	require.NoError(t, err)

	svc := app.NewWagerService(app.WagerDeps{Deps: app.Deps{UoW: postgres.NewUnitOfWork(short), Queries: postgres.NewQueries(short),
		Clock: app.SystemClock{}, IDs: app.UUIDv7{}, Metrics: s.metrics, Logger: nil}, Policy: defaultPolicy, ConflictRetries: 1})
	cmd, err := app.NewSubmitCommand(s.input(w, "provider-a", "lock-timeout", "BET", "1.00", ""))
	require.NoError(t, err)
	_, err = svc.Submit(ctx, cmd)
	require.ErrorIs(t, err, app.ErrConflict)
	assert.True(t, app.IsTransient(err))
}

func TestReplayKeepsTheCurrencyOfTheObservedBalance(t *testing.T) {
	t.Parallel()
	s := newServices(t, defaultPolicy)
	w := s.openWallet(t, "100.00")
	in := s.input(w, "provider-a", "usd-bet", "BET", "10.00", "")
	in.Currency = "USD"
	cmd, err := app.NewSubmitCommand(in)
	require.NoError(t, err)
	first, err := s.wagers.Submit(context.Background(), cmd)
	require.NoError(t, err)
	require.Equal(t, wager.CodeCurrencyMismatch, first.Transaction.FailureCode())

	replay, err := s.wagers.Submit(context.Background(), cmd)
	require.NoError(t, err)
	assert.True(t, replay.Replay)
	assert.Equal(t, "100.00 BRL", replay.Transaction.ResultBalance().String(), "the wallet balance keeps its own currency")
	assert.Equal(t, "10.00 USD", replay.Transaction.Amount().String())
}
