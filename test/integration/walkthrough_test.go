//go:build integration

package integration_test

import (
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Pantani/backend-challenge-go/test/testenv"
)

// TestWalkthrough catches missing financial effects, duplicate debits, broken
// authorization and stalled asynchronous processing in one ordered journey.
func TestWalkthrough(t *testing.T) {
	r := startApp(t)
	w := testenv.Wallet{}
	prefix := uuid.NewString()
	steps := []struct {
		name string
		run  func(*testing.T)
	}{
		{"01_health_and_authentication", func(t *testing.T) { walkthroughAccess(t, r) }},
		{"02_open_wallet", func(t *testing.T) {
			w = r.http.OpenWallet(t, "1000.00")
			walkthroughBalance(t, r, w, "1000.00")
		}},
		{"03_bet_replay_conflict_and_reads", func(t *testing.T) { walkthroughBet(t, r, w, prefix) }},
		{"04_win_loss_refund_rollback_and_rejection", func(t *testing.T) { walkthroughOperations(t, r, w, prefix) }},
		{"05_refund_before_bet", func(t *testing.T) { walkthroughPending(t, r, w, prefix) }},
		{"06_sqs_and_http_deduplication", func(t *testing.T) { walkthroughQueue(t, r, w, prefix) }},
		{"07_ledger_and_reconciliation", func(t *testing.T) { walkthroughLedger(t, r, w) }},
		{"08_outbox_publication", func(t *testing.T) { walkthroughOutbox(t, w) }},
	}
	for _, step := range steps {
		if !t.Run(step.name, step.run) {
			t.FailNow()
		}
	}
}

// walkthroughAccess checks health endpoints and rejects missing or unauthorized credentials.
func walkthroughAccess(t *testing.T, r runningApp) {
	t.Helper()
	for _, path := range []string{"/health/live", "/health/ready"} {
		require.Equal(t, http.StatusOK, r.http.Call(t, http.MethodGet, path, "", "", nil).Status)
	}
	path := "/wallets/" + uuid.NewString()
	require.Equal(t, http.StatusUnauthorized, r.http.Call(t, http.MethodGet, path, "", "", nil).Status)
	require.Equal(t, http.StatusForbidden, r.http.Call(t, http.MethodGet, path, "provider-a", "", nil).Status)
}

// walkthroughBalance checks the persisted wallet balance through the HTTP API.
func walkthroughBalance(t *testing.T, r runningApp, w testenv.Wallet, amount string) {
	t.Helper()
	require.Equal(t, map[string]any{"amount": amount, "currency": "BRL"}, r.wallet(t, w.ID)["balance"])
}

// walkthroughSubmit submits an authenticated operation and checks its HTTP status.
func walkthroughSubmit(t *testing.T, r runningApp, w testenv.Wallet, ext, kind, amount, ref string, status int) testenv.Response {
	t.Helper()
	res, err := r.http.Submit(t.Context(), w, "provider-a", ext, kind, amount, ref)
	require.NoError(t, err)
	require.Equal(t, status, res.Status, res.Body)
	return res
}

// walkthroughBet checks deduplication, conflicting payloads and provider isolation.
func walkthroughBet(t *testing.T, r runningApp, w testenv.Wallet, prefix string) {
	t.Helper()
	ext := prefix + "-bet"
	first := walkthroughSubmit(t, r, w, ext, "BET", "25.00", "", http.StatusCreated)
	require.Equal(t, "PROCESSED", first.Body["status"])
	require.Equal(t, false, first.Body["idempotentReplay"])
	replay := walkthroughSubmit(t, r, w, ext, "BET", "25.00", "", http.StatusOK)
	require.Equal(t, first.Body["transactionId"], replay.Body["transactionId"])
	require.Equal(t, true, replay.Body["idempotentReplay"])
	conflict := walkthroughSubmit(t, r, w, ext, "BET", "26.00", "", http.StatusConflict)
	require.Equal(t, "IDEMPOTENCY_CONFLICT", conflict.Body["code"])
	id, ok := first.Body["transactionId"].(string)
	require.True(t, ok)
	for _, path := range []string{"/wagering/transactions/" + id, "/providers/provider-a/wagering/transactions/" + ext} {
		res := r.http.Call(t, http.MethodGet, path, "provider-a", "", nil)
		require.Equal(t, http.StatusOK, res.Status)
		require.Equal(t, id, res.Body["transactionId"])
		require.Equal(t, "PROCESSED", res.Body["status"])
	}
	require.Equal(t, http.StatusNotFound, r.http.Call(t, http.MethodGet, "/wagering/transactions/"+id, "provider-b", "", nil).Status)
	walkthroughBalance(t, r, w, "975.00")
}

// walkthroughOperations checks each operation against independently calculated balances.
func walkthroughOperations(t *testing.T, r runningApp, w testenv.Wallet, prefix string) {
	t.Helper()
	cases := []struct{ ext, kind, amount, ref, balance, status, failure string }{
		{"win", "WIN", "50.00", "bet", "1025.00", "PROCESSED", ""},
		{"loss", "LOSS", "0.00", "", "1025.00", "PROCESSED", ""},
		{"refund", "REFUND", "25.00", "bet", "1050.00", "PROCESSED", ""},
		{"rollback", "ROLLBACK", "50.00", "win", "1000.00", "PROCESSED", ""},
		{"rejected", "BET", "1001.00", "", "1000.00", "REJECTED", "INSUFFICIENT_FUNDS"},
	}
	for _, c := range cases {
		ref := ""
		if c.ref != "" {
			ref = prefix + "-" + c.ref
		}
		code := http.StatusCreated
		if c.failure != "" {
			code = http.StatusUnprocessableEntity
		}
		res := walkthroughSubmit(t, r, w, prefix+"-"+c.ext, c.kind, c.amount, ref, code)
		require.Equal(t, c.status, res.Body["status"], c.kind)
		if c.failure != "" {
			require.Equal(t, c.failure, res.Body["failureCode"])
		}
		walkthroughBalance(t, r, w, c.balance)
	}
}

// walkthroughAwait waits for asynchronous processing with a bounded deadline.
func walkthroughAwait(t *testing.T, r runningApp, ext string) {
	t.Helper()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		res, err := r.http.Do(t.Context(), http.MethodGet, "/providers/provider-a/wagering/transactions/"+ext, "provider-a", "", nil)
		if !assert.NoError(c, err) {
			return
		}
		assert.Equal(c, http.StatusOK, res.Status)
		assert.Equal(c, "PROCESSED", res.Body["status"])
	}, 30*time.Second, 100*time.Millisecond)
}

// walkthroughPending checks that a refund waits for its missing bet and then resolves.
func walkthroughPending(t *testing.T, r runningApp, w testenv.Wallet, prefix string) {
	t.Helper()
	bet, refund := prefix+"-late-bet", prefix+"-early-refund"
	pending := walkthroughSubmit(t, r, w, refund, "REFUND", "40.00", bet, http.StatusAccepted)
	require.Equal(t, "PENDING_REFERENCE", pending.Body["status"])
	walkthroughBalance(t, r, w, "1000.00")
	res := walkthroughSubmit(t, r, w, bet, "BET", "40.00", "", http.StatusCreated)
	require.Equal(t, "PROCESSED", res.Body["status"])
	walkthroughAwait(t, r, refund)
	walkthroughBalance(t, r, w, "1000.00")
}

// walkthroughQueue checks SQS processing and a subsequent HTTP replay without a second debit.
func walkthroughQueue(t *testing.T, r runningApp, w testenv.Wallet, prefix string) {
	t.Helper()
	ext, messageID := prefix+"-sqs", prefix+"-message"
	sendMessage(t, r.api, r.queues, messageID, testenv.SubmitInput(w, "provider-a", ext, "BET", "15.00", ""))
	walkthroughAwait(t, r, ext)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		var done int
		err := pool.QueryRow(t.Context(), "SELECT count(*) FROM inbox_messages WHERE message_id=$1 AND processed_at IS NOT NULL", messageID).Scan(&done)
		if !assert.NoError(c, err) {
			return
		}
		assert.Equal(c, 1, done)
	}, 20*time.Second, 100*time.Millisecond)
	replay := walkthroughSubmit(t, r, w, ext, "BET", "15.00", "", http.StatusOK)
	require.Equal(t, true, replay.Body["idempotentReplay"])
	walkthroughBalance(t, r, w, "985.00")
}

// walkthroughLedger checks pagination completeness and reconciles the final balance.
func walkthroughLedger(t *testing.T, r runningApp, w testenv.Wallet) {
	t.Helper()
	cursor := ""
	seen := map[string]bool{}
	for range 10 {
		res := r.http.Call(t, http.MethodGet, "/wallets/"+w.ID+"/ledger?limit=2&cursor="+url.QueryEscape(cursor), "wallet-service", "", nil)
		require.Equal(t, http.StatusOK, res.Status)
		items, ok := res.Body["items"].([]any)
		require.True(t, ok)
		require.LessOrEqual(t, len(items), 2)
		for _, item := range items {
			entry := item.(map[string]any)
			id := entry["id"].(string)
			require.False(t, seen[id], "ledger entry repeated across pages")
			seen[id] = true
		}
		cursor, _ = res.Body["nextCursor"].(string)
		if cursor == "" {
			break
		}
	}
	require.Empty(t, cursor, "pagination must terminate")
	require.Len(t, seen, 8, "opening plus seven financial movements; LOSS/rejection/replays add none")
	rec := r.http.Call(t, http.MethodPost, "/wallets/"+w.ID+"/reconciliation", "wallet-service", "", nil)
	require.Equal(t, http.StatusOK, rec.Status)
	require.Equal(t, true, rec.Body["consistent"])
	require.EqualValues(t, 8, rec.Body["checkedEntries"])
	require.Equal(t, map[string]any{"amount": "985.00", "currency": "BRL"}, rec.Body["calculatedBalance"])
}

// walkthroughOutbox waits for publication acknowledgements for every wallet event.
func walkthroughOutbox(t *testing.T, w testenv.Wallet) {
	t.Helper()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		var total, pending int
		err := pool.QueryRow(t.Context(), "SELECT count(*), count(*) FILTER (WHERE published_at IS NULL) FROM outbox_events WHERE partition_key=$1", w.ID).Scan(&total, &pending)
		if !assert.NoError(c, err) {
			return
		}
		assert.Positive(c, total)
		assert.Zero(c, pending)
	}, 30*time.Second, 100*time.Millisecond)
}
