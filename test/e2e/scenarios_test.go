//go:build e2e

package e2e_test

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Pantani/backend-challenge-go/test/testenv"
)

// Register first so logs include failures from later scenario cleanups too.
func logScenarioFailure(t *testing.T) {
	t.Helper()
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		for _, run := range fixture.runs {
			t.Logf("instance %s pid=%d logs:\n%s", run.name, run.cmd.Process.Pid, run.logs.String())
		}
	})
}

func openWallet(t *testing.T, amount string) testenv.Wallet {
	t.Helper()
	w := instances[0].client().OpenWallet(t, amount)
	t.Cleanup(func() { reconcileWallet(t, w) })
	return w
}

func submit(t *testing.T, inst *instance, w testenv.Wallet, ext, kind, amount, ref string) testenv.Response {
	t.Helper()
	res, err := submitE(inst, w, ext, kind, amount, ref)
	require.NoError(t, err)
	return res
}

// submitE is the goroutine-safe variant of submit.
func submitE(inst *instance, w testenv.Wallet, ext, kind, amount, ref string) (testenv.Response, error) {
	return inst.client().Submit(context.Background(), w, "provider-a", ext, kind, amount, ref)
}

func balance(t *testing.T, w testenv.Wallet) string {
	t.Helper()
	res := call(t, instances[1], http.MethodGet, "/wallets/"+w.ID, "wallet-service", "", nil)
	require.Equal(t, http.StatusOK, res.Status)
	return res.Body["balance"].(map[string]any)["amount"].(string)
}

func debits(t *testing.T, w testenv.Wallet) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	n, err := testenv.CountDebits(ctx, pool, w.ID)
	require.NoError(t, err)
	return n
}

// concurrently submits n requests at once, round-robin across the
// instances, and fails the test on the test goroutine if any request failed
// (FailNow must never run on a worker goroutine).
func concurrently(t *testing.T, n int, fn func(i int, inst *instance) (testenv.Response, error)) []testenv.Response {
	t.Helper()
	results, err := testenv.Parallel(n, func(i int) (testenv.Response, error) {
		return fn(i, instances[i%len(instances)])
	})
	require.NoError(t, err)
	return results
}

func statusCount(results []testenv.Response) map[int]int {
	count := map[int]int{}
	for _, r := range results {
		count[r.Status]++
	}
	return count
}

func TestFiftyIdenticalBetsAcrossThreeInstances(t *testing.T) {
	logScenarioFailure(t)
	w := openWallet(t, "1000.00")
	ext := uuid.NewString()
	results := concurrently(t, 50, func(_ int, inst *instance) (testenv.Response, error) {
		return submitE(inst, w, ext, "BET", "25.00", "")
	})
	assert.Equal(t, map[int]int{http.StatusCreated: 1, http.StatusOK: 49}, statusCount(results), "one processing, 49 idempotent replays")
	assert.Equal(t, 1, debits(t, w))
	assert.Equal(t, "975.00", balance(t, w))
}

func TestTwoBetsRaceAcrossInstances(t *testing.T) {
	logScenarioFailure(t)
	w := openWallet(t, "100.00")
	exts := []string{uuid.NewString(), uuid.NewString()}
	results := concurrently(t, 2, func(i int, inst *instance) (testenv.Response, error) {
		return submitE(inst, w, exts[i], "BET", "80.00", "")
	})

	statuses := map[string]string{}
	for _, r := range results {
		statuses[r.Body["status"].(string)] = fmt.Sprint(r.Body["failureCode"])
	}
	assert.Equal(t, map[string]string{"PROCESSED": "<nil>", "REJECTED": "INSUFFICIENT_FUNDS"}, statuses)
	assert.Equal(t, "20.00", balance(t, w))
	assert.Equal(t, 1, debits(t, w))

	concurrently(t, 4, func(i int, inst *instance) (testenv.Response, error) {
		return submitE(inst, w, exts[i%2], "BET", "80.00", "")
	})
	assert.Equal(t, "20.00", balance(t, w), "resending does not change the outcome")
}

func TestDistinctWalletsInParallelAcrossInstances(t *testing.T) {
	logScenarioFailure(t)
	wallets := []testenv.Wallet{openWallet(t, "100.00"), openWallet(t, "100.00"), openWallet(t, "100.00"), openWallet(t, "100.00")}
	results := concurrently(t, 40, func(i int, inst *instance) (testenv.Response, error) {
		return submitE(inst, wallets[i%4], uuid.NewString(), "BET", "10.00", "")
	})
	assert.Equal(t, map[int]int{http.StatusCreated: 40}, statusCount(results))
	for _, w := range wallets {
		assert.Equal(t, "0.00", balance(t, w))
	}
}

func sendMessage(t *testing.T, messageID string, w testenv.Wallet, ext, kind, amount string) {
	t.Helper()
	env := testenv.Envelope(messageID, testenv.SubmitInput(w, "provider-a", ext, kind, amount, ""))
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	require.NoError(t, testenv.SendMessage(ctx, sqsClient, queues.Input, env))
}

func TestSameOperationThroughHTTPAndSQSAcrossInstances(t *testing.T) {
	logScenarioFailure(t)
	w := openWallet(t, "100.00")
	ext := uuid.NewString()
	sendMessage(t, "msg-"+ext, w, ext, "BET", "10.00")
	sendMessage(t, "msg-dup-"+ext, w, ext, "BET", "10.00") // same operation, other message
	res := submit(t, instances[2], w, ext, "BET", "10.00", "")
	require.Contains(t, []int{http.StatusCreated, http.StatusOK}, res.Status)

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()
		var done int
		err := pool.QueryRow(ctx, `SELECT count(*) FROM inbox_messages
			WHERE message_id IN ($1, $2) AND processed_at IS NOT NULL`, "msg-"+ext, "msg-dup-"+ext).Scan(&done)
		if !assert.NoError(collect, err) {
			return
		}
		assert.Equal(collect, 2, done)
	}, 30*time.Second, 200*time.Millisecond)
	assert.Equal(t, "90.00", balance(t, w))
	assert.Equal(t, 1, debits(t, w))
}

func TestRefundBeforeBetAcrossInstances(t *testing.T) {
	logScenarioFailure(t)
	w := openWallet(t, "100.00")
	bet, refund := uuid.NewString(), uuid.NewString()
	pending := submit(t, instances[0], w, refund, "REFUND", "40.00", bet)
	require.Equal(t, http.StatusAccepted, pending.Status)
	assert.Equal(t, "PENDING_REFERENCE", pending.Body["status"])

	require.Equal(t, http.StatusCreated, submit(t, instances[1], w, bet, "BET", "40.00", "").Status)
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		res, err := instances[2].client().Do(context.Background(), http.MethodGet,
			"/providers/provider-a/wagering/transactions/"+refund, "provider-a", "", nil)
		if !assert.NoError(collect, err) {
			return
		}
		assert.Equal(collect, "PROCESSED", res.Body["status"])
	}, 20*time.Second, 200*time.Millisecond, "resolved by the worker of whichever instance got it")
	assert.Equal(t, "100.00", balance(t, w))

	other := call(t, instances[2], http.MethodGet, "/wagering/transactions/"+pending.Body["transactionId"].(string), "provider-b", "", nil)
	assert.Equal(t, http.StatusNotFound, other.Status, "provider isolation holds on every instance")
}

// TestCrashAndRestart kills every instance (no graceful shutdown) and
// verifies that idempotency, pending references and balances survive.
func TestCrashAndRestart(t *testing.T) {
	logScenarioFailure(t)
	w := openWallet(t, "100.00")
	bet, refund, lateBet := uuid.NewString(), uuid.NewString(), uuid.NewString()
	first := submit(t, instances[0], w, bet, "BET", "30.00", "")
	require.Equal(t, http.StatusCreated, first.Status)
	require.Equal(t, http.StatusAccepted, submit(t, instances[1], w, refund, "REFUND", "20.00", lateBet).Status)

	for _, inst := range instances {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		err := inst.kill(ctx)
		cancel()
		require.NoError(t, err)
	}
	for _, inst := range instances {
		require.NoError(t, inst.startWith(t.Context()))
	}

	replay := submit(t, instances[2], w, bet, "BET", "30.00", "")
	assert.Equal(t, http.StatusOK, replay.Status)
	assert.Equal(t, first.Body["transactionId"], replay.Body["transactionId"])
	require.Equal(t, http.StatusCreated, submit(t, instances[0], w, lateBet, "BET", "20.00", "").Status)
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		res, err := instances[1].client().Do(context.Background(), http.MethodGet,
			"/providers/provider-a/wagering/transactions/"+refund, "provider-a", "", nil)
		if !assert.NoError(collect, err) {
			return
		}
		assert.Equal(collect, "PROCESSED", res.Body["status"])
	}, 30*time.Second, 200*time.Millisecond, "the pending reference survived the crash")
	assert.Equal(t, "70.00", balance(t, w))
}

// Each scenario reconciles its own acquired wallets, even with -run or -shuffle.
func reconcileWallet(t *testing.T, w testenv.Wallet) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	rec, err := instances[0].client().Do(ctx, http.MethodPost, "/wallets/"+w.ID+"/reconciliation", "wallet-service", "", nil)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, rec.Status)
	assert.Equal(t, true, rec.Body["consistent"], w.ID)
	var balance int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT balance_minor FROM wallets WHERE id = $1`, w.ID).Scan(&balance))
	assert.GreaterOrEqual(t, balance, int64(0))
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		var total, pending int
		err := pool.QueryRow(ctx, `SELECT count(*), count(*) FILTER (WHERE published_at IS NULL)
			FROM outbox_events WHERE aggregate_id = $1 OR aggregate_id IN
			(SELECT id FROM wager_transactions WHERE wallet_id = $1)`, w.ID).Scan(&total, &pending)
		if !assert.NoError(collect, err) {
			return
		}
		assert.Positive(collect, total, "the scenario must produce outbox events")
		assert.Zero(collect, pending)
	}, 30*time.Second, 200*time.Millisecond, "every event of this wallet was published")
}
