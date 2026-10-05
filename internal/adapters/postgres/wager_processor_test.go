package postgres

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/VictorM0nteiro/backend-challenge-go/internal/domain"
)

// wagerReq builds a request whose hash is derived from its content, the way
// the real caller does, so two requests with the same body hash the same.
func wagerReq(t *testing.T, walletID, playerID uuid.UUID, kind domain.WagerKind, ext, ref string, minor int64, key string) WagerRequest {
	t.Helper()
	hash := string(kind) + "|" + ext + "|" + ref + "|" + mustMoney(t, minor, "BRL").String()
	return WagerRequest{
		Key:         IdempotencyKey{Scope: "provider-a", Endpoint: "POST /wagering/transactions", Key: key},
		RequestHash: hash,
		Command: domain.WagerCommand{
			ProviderID:                     "provider-a",
			ExternalTransactionID:          ext,
			IdempotencyKey:                 key,
			RequestHash:                    hash,
			WalletID:                       walletID,
			PlayerID:                       playerID,
			RoundID:                        "round-1",
			GameID:                         "fortune-chimp",
			Kind:                           kind,
			Amount:                         mustMoney(t, minor, "BRL"),
			ReferenceExternalTransactionID: ref,
		},
	}
}

func keyRows(t *testing.T, pool *Pool, key string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM idempotency_keys WHERE key = $1`, key).Scan(&n); err != nil {
		t.Fatalf("count keys: %v", err)
	}
	return n
}

func balanceOf(t *testing.T, pool *Pool, walletID uuid.UUID) int64 {
	t.Helper()
	w, err := NewWalletRepository(pool).FindByID(context.Background(), walletID)
	if err != nil {
		t.Fatalf("FindByID: %v", err)
	}
	return w.Balance().AmountMinor()
}

// ownerOf returns the player that owns the wallet. The processor refuses an
// operation naming anyone else, so requests must carry the real owner.
func ownerOf(t *testing.T, pool *Pool, walletID uuid.UUID) uuid.UUID {
	t.Helper()
	w, err := NewWalletRepository(pool).FindByID(context.Background(), walletID)
	if err != nil {
		t.Fatalf("FindByID: %v", err)
	}
	return w.PlayerID()
}

// assertLedgerMatchesBalance checks the stored balance against the ledger: the
// balance before the first entry, plus credits, minus debits, must equal the
// stored balance. (The first entry's balance_before is the starting point
// because test wallets are created with a balance and no OPENING line.)
func assertLedgerMatchesBalance(t *testing.T, pool *Pool, walletID uuid.UUID) {
	t.Helper()
	ctx := context.Background()

	var start, net int64
	err := pool.QueryRow(ctx, `
		SELECT balance_before_minor FROM wallet_ledger_entries
		WHERE wallet_id = $1 ORDER BY created_at ASC LIMIT 1`, walletID).Scan(&start)
	if errors.Is(err, pgx.ErrNoRows) {
		return // no movements: nothing to reconcile
	}
	if err != nil {
		t.Fatalf("first ledger entry: %v", err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(CASE direction WHEN 'CREDIT' THEN amount_minor ELSE -amount_minor END), 0)
		FROM wallet_ledger_entries WHERE wallet_id = $1`, walletID).Scan(&net); err != nil {
		t.Fatalf("ledger net: %v", err)
	}

	if stored := balanceOf(t, pool, walletID); stored != start+net {
		t.Fatalf("stored balance %d, ledger says %d (start %d, net %d)", stored, start+net, start, net)
	}
}

func TestProcess_NewBetIsProcessedAndDebits(t *testing.T) {
	pool := newTestPool(t)
	walletID := createTestWallet(t, NewWalletRepository(pool), 10000)
	player := ownerOf(t, pool, walletID)
	proc := NewWagerProcessor(pool)

	out, err := proc.Process(context.Background(), wagerReq(t, walletID, player, domain.WagerKindBet, "bet-1", "", 2500, "k-1"))
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if out.StatusCode != statusProcessed || out.Replayed {
		t.Fatalf("status = %d replayed = %v, want 201 and false", out.StatusCode, out.Replayed)
	}
	if !strings.Contains(string(out.Body), `"status":"PROCESSED"`) {
		t.Fatalf("body = %s, want PROCESSED", out.Body)
	}
	if got := balanceOf(t, pool, walletID); got != 7500 {
		t.Fatalf("balance = %d, want 7500", got)
	}
	if got := countRows(t, pool, "wallet_ledger_entries", walletID); got != 1 {
		t.Fatalf("ledger rows = %d, want 1", got)
	}
	assertLedgerMatchesBalance(t, pool, walletID)
}

func TestProcess_ReplayReturnsSameOutcomeWithoutSecondDebit(t *testing.T) {
	pool := newTestPool(t)
	walletID := createTestWallet(t, NewWalletRepository(pool), 10000)
	req := wagerReq(t, walletID, ownerOf(t, pool, walletID), domain.WagerKindBet, "bet-1", "", 2500, "k-1")
	proc := NewWagerProcessor(pool)

	first, err := proc.Process(context.Background(), req)
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	second, err := proc.Process(context.Background(), req)
	if err != nil {
		t.Fatalf("second: %v", err)
	}

	if !second.Replayed || first.Replayed {
		t.Fatalf("replayed first=%v second=%v, want false then true", first.Replayed, second.Replayed)
	}
	if second.TransactionID != first.TransactionID {
		t.Fatalf("transaction id changed on replay")
	}
	if second.StatusCode != first.StatusCode || string(second.Body) != string(first.Body) {
		t.Fatalf("replay answer differs:\n first  %d %s\n second %d %s", first.StatusCode, first.Body, second.StatusCode, second.Body)
	}
	if got := balanceOf(t, pool, walletID); got != 7500 {
		t.Fatalf("balance = %d, want 7500 (debited once)", got)
	}
	if got := countRows(t, pool, "wallet_ledger_entries", walletID); got != 1 {
		t.Fatalf("ledger rows = %d, want 1", got)
	}
	assertLedgerMatchesBalance(t, pool, walletID)
}

func TestProcess_SameKeyDifferentBodyIsRejected(t *testing.T) {
	pool := newTestPool(t)
	walletID := createTestWallet(t, NewWalletRepository(pool), 10000)
	player := ownerOf(t, pool, walletID)
	proc := NewWagerProcessor(pool)

	if _, err := proc.Process(context.Background(), wagerReq(t, walletID, player, domain.WagerKindBet, "bet-1", "", 2500, "k-1")); err != nil {
		t.Fatalf("first: %v", err)
	}
	_, err := proc.Process(context.Background(), wagerReq(t, walletID, player, domain.WagerKindBet, "bet-1", "", 4000, "k-1"))
	if !errors.Is(err, ErrIdempotencyKeyReuse) {
		t.Fatalf("err = %v, want ErrIdempotencyKeyReuse", err)
	}
	if got := balanceOf(t, pool, walletID); got != 7500 {
		t.Fatalf("balance = %d, want 7500 (second body must not apply)", got)
	}
}

func TestProcess_InFlightKeyIsRejected(t *testing.T) {
	pool := newTestPool(t)
	walletID := createTestWallet(t, NewWalletRepository(pool), 10000)
	req := wagerReq(t, walletID, ownerOf(t, pool, walletID), domain.WagerKindBet, "bet-1", "", 2500, "k-1")

	if _, err := pool.Exec(context.Background(),
		`INSERT INTO idempotency_keys (scope, endpoint, key, request_hash, state) VALUES ($1, $2, $3, $4, 'in_flight')`,
		req.Key.Scope, req.Key.Endpoint, req.Key.Key, req.RequestHash); err != nil {
		t.Fatalf("seed in_flight key: %v", err)
	}

	_, err := NewWagerProcessor(pool).Process(context.Background(), req)
	if !errors.Is(err, ErrRequestInFlight) {
		t.Fatalf("err = %v, want ErrRequestInFlight", err)
	}
	if got := countRows(t, pool, "wager_transactions"); got != 0 {
		t.Fatalf("wager rows = %d, want 0", got)
	}
}

func TestProcess_InsufficientFundsIsRecordedAndReplayed(t *testing.T) {
	pool := newTestPool(t)
	walletID := createTestWallet(t, NewWalletRepository(pool), 1000)
	req := wagerReq(t, walletID, ownerOf(t, pool, walletID), domain.WagerKindBet, "bet-1", "", 2500, "k-1")
	proc := NewWagerProcessor(pool)

	first, err := proc.Process(context.Background(), req)
	if err != nil {
		t.Fatalf("a business rejection must not be an error: %v", err)
	}
	if first.StatusCode != statusRejected || !strings.Contains(string(first.Body), string(domain.FailureInsufficientFunds)) {
		t.Fatalf("answer = %d %s, want 422 with insufficient_funds", first.StatusCode, first.Body)
	}
	if got := balanceOf(t, pool, walletID); got != 1000 {
		t.Fatalf("balance = %d, want 1000 (rejected bet must not move money)", got)
	}
	if got := countRows(t, pool, "wallet_ledger_entries", walletID); got != 0 {
		t.Fatalf("ledger rows = %d, want 0", got)
	}

	replay, err := proc.Process(context.Background(), req)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if !replay.Replayed || replay.StatusCode != statusRejected || string(replay.Body) != string(first.Body) {
		t.Fatalf("replay = %+v, want the stored rejection", replay)
	}
}

func TestProcess_RefundReversesProcessedBet(t *testing.T) {
	pool := newTestPool(t)
	walletID := createTestWallet(t, NewWalletRepository(pool), 10000)
	player := ownerOf(t, pool, walletID)
	proc := NewWagerProcessor(pool)
	ctx := context.Background()

	if _, err := proc.Process(ctx, wagerReq(t, walletID, player, domain.WagerKindBet, "bet-1", "", 2500, "k-bet")); err != nil {
		t.Fatalf("bet: %v", err)
	}
	out, err := proc.Process(ctx, wagerReq(t, walletID, player, domain.WagerKindRefund, "refund-1", "bet-1", 2500, "k-refund"))
	if err != nil {
		t.Fatalf("refund: %v", err)
	}
	if out.StatusCode != statusProcessed {
		t.Fatalf("refund status = %d, body %s", out.StatusCode, out.Body)
	}
	if got := balanceOf(t, pool, walletID); got != 10000 {
		t.Fatalf("balance = %d, want 10000 after refund", got)
	}
	if got := countRows(t, pool, "wallet_ledger_entries", walletID); got != 2 {
		t.Fatalf("ledger rows = %d, want 2 (debit then credit)", got)
	}
	assertLedgerMatchesBalance(t, pool, walletID)
}

func TestProcess_SecondReversalOfSameBetIsDuplicate(t *testing.T) {
	pool := newTestPool(t)
	walletID := createTestWallet(t, NewWalletRepository(pool), 10000)
	player := ownerOf(t, pool, walletID)
	proc := NewWagerProcessor(pool)
	ctx := context.Background()

	if _, err := proc.Process(ctx, wagerReq(t, walletID, player, domain.WagerKindBet, "bet-1", "", 2500, "k-bet")); err != nil {
		t.Fatalf("bet: %v", err)
	}
	if _, err := proc.Process(ctx, wagerReq(t, walletID, player, domain.WagerKindRefund, "refund-1", "bet-1", 2500, "k-refund-1")); err != nil {
		t.Fatalf("first refund: %v", err)
	}

	out, err := proc.Process(ctx, wagerReq(t, walletID, player, domain.WagerKindRollback, "rollback-1", "bet-1", 2500, "k-rollback"))
	if err != nil {
		t.Fatalf("second reversal must be a rejection, not an error: %v", err)
	}
	if out.StatusCode != statusRejected || !strings.Contains(string(out.Body), string(domain.FailureDuplicateReversal)) {
		t.Fatalf("answer = %d %s, want 422 duplicate_reversal", out.StatusCode, out.Body)
	}
	if got := balanceOf(t, pool, walletID); got != 10000 {
		t.Fatalf("balance = %d, want 10000 (money returned only once)", got)
	}
	assertLedgerMatchesBalance(t, pool, walletID)
}

func TestProcess_InvalidInputLeavesNoKeyBehind(t *testing.T) {
	pool := newTestPool(t)
	walletID := createTestWallet(t, NewWalletRepository(pool), 10000)
	proc := NewWagerProcessor(pool)
	ctx := context.Background()

	bad := wagerReq(t, walletID, ownerOf(t, pool, walletID), domain.WagerKindBet, "bet-1", "", 0, "k-1")
	if _, err := proc.Process(ctx, bad); !errors.Is(err, domain.ErrInvalidAmount) {
		t.Fatalf("err = %v, want ErrInvalidAmount", err)
	}
	if got := keyRows(t, pool, "k-1"); got != 0 {
		t.Fatalf("invalid input left %d key row(s) behind", got)
	}

	good := wagerReq(t, walletID, bad.Command.PlayerID, domain.WagerKindBet, "bet-1", "", 2500, "k-1")
	if _, err := proc.Process(ctx, good); err != nil {
		t.Fatalf("the same key must work once the input is valid: %v", err)
	}
}

// TestProcess_LossInAnotherCurrencyIsRejectedAndLeavesNoKeyBehind: a LOSS moves
// no money, so only the processor's own check catches a wrong currency.
func TestProcess_LossInAnotherCurrencyIsRejectedAndLeavesNoKeyBehind(t *testing.T) {
	pool := newTestPool(t)
	walletID := createTestWallet(t, NewWalletRepository(pool), 10000)

	req := wagerReq(t, walletID, ownerOf(t, pool, walletID), domain.WagerKindLoss, "loss-1", "", 0, "k-loss")
	req.Command.Amount = mustMoney(t, 0, "USD")

	_, err := NewWagerProcessor(pool).Process(context.Background(), req)
	if !errors.Is(err, domain.ErrCurrencyMismatch) {
		t.Fatalf("err = %v, want ErrCurrencyMismatch", err)
	}
	if got := keyRows(t, pool, "k-loss"); got != 0 {
		t.Fatalf("rejected input left %d key row(s) behind", got)
	}
	if got := countRows(t, pool, "wager_transactions"); got != 0 {
		t.Fatalf("wager rows = %d, want 0", got)
	}
}

// TestProcess_ConcurrentSameKeyCreatesOneOperation is the idempotency race:
// many simultaneous requests with one key must produce one operation, one
// debit, and identical answers for everyone.
func TestProcess_ConcurrentSameKeyCreatesOneOperation(t *testing.T) {
	pool := newTestPool(t)
	walletID := createTestWallet(t, NewWalletRepository(pool), 10000)
	req := wagerReq(t, walletID, ownerOf(t, pool, walletID), domain.WagerKindBet, "bet-1", "", 2500, "k-race")
	proc := NewWagerProcessor(pool)

	const n = 50 // README §13.1: the same bet 50 times in parallel
	outcomes := make([]Outcome, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			outcomes[i], errs[i] = proc.Process(context.Background(), req)
		}(i)
	}
	wg.Wait()

	first := true
	for i := range outcomes {
		if errs[i] != nil {
			t.Fatalf("request %d: %v", i, errs[i])
		}
		if !outcomes[i].Replayed {
			if !first {
				t.Fatalf("more than one request was treated as the original")
			}
			first = false
		}
		if outcomes[i].TransactionID != outcomes[0].TransactionID {
			t.Fatalf("request %d saw a different operation", i)
		}
	}
	if got := countRows(t, pool, "wager_transactions"); got != 1 {
		t.Fatalf("operations = %d, want 1", got)
	}
	if got := countRows(t, pool, "wallet_ledger_entries", walletID); got != 1 {
		t.Fatalf("ledger rows = %d, want 1", got)
	}
	if got := balanceOf(t, pool, walletID); got != 7500 {
		t.Fatalf("balance = %d, want 7500", got)
	}
	assertLedgerMatchesBalance(t, pool, walletID)
}

// TestProcess_PlayerThatDoesNotOwnTheWalletIsRefused: an operation that names a
// player other than the wallet's owner must not move money, must leave nothing
// behind, and must not consume its idempotency key.
func TestProcess_PlayerThatDoesNotOwnTheWalletIsRefused(t *testing.T) {
	pool := newTestPool(t)
	walletID := createTestWallet(t, NewWalletRepository(pool), 10000)

	req := wagerReq(t, walletID, uuid.New(), domain.WagerKindBet, "bet-1", "", 2500, "k-stranger")
	_, err := NewWagerProcessor(pool).Process(context.Background(), req)
	if !errors.Is(err, ErrWalletOwnerMismatch) {
		t.Fatalf("err = %v, want ErrWalletOwnerMismatch", err)
	}
	if got := balanceOf(t, pool, walletID); got != 10000 {
		t.Fatalf("balance = %d, want 10000: nothing may move", got)
	}
	if got := keyRows(t, pool, "k-stranger"); got != 0 {
		t.Fatalf("refused operation left %d key row(s) behind", got)
	}
	if got := countRows(t, pool, "wager_transactions"); got != 0 {
		t.Fatalf("wager rows = %d, want 0", got)
	}
}

// TestProcess_SameExternalIDUnderAnotherKeyIsAConflictAndMovesNothing: the same
// (provider, externalTransactionId) cannot be applied again by changing the
// idempotency key. The second attempt must say so, leave the balance alone and
// leave its own key unused.
func TestProcess_SameExternalIDUnderAnotherKeyIsAConflictAndMovesNothing(t *testing.T) {
	pool := newTestPool(t)
	walletID := createTestWallet(t, NewWalletRepository(pool), 10000)
	player := ownerOf(t, pool, walletID)
	proc := NewWagerProcessor(pool)
	ctx := context.Background()

	if _, err := proc.Process(ctx, wagerReq(t, walletID, player, domain.WagerKindBet, "bet-1", "", 2500, "k-first")); err != nil {
		t.Fatalf("first: %v", err)
	}

	// Same external id, another key. The body is identical, so only the key differs.
	_, err := proc.Process(ctx, wagerReq(t, walletID, player, domain.WagerKindBet, "bet-1", "", 2500, "k-second"))
	if !errors.Is(err, ErrDuplicateExternalTransaction) {
		t.Fatalf("err = %v, want ErrDuplicateExternalTransaction", err)
	}
	if got := balanceOf(t, pool, walletID); got != 7500 {
		t.Fatalf("balance = %d, want 7500: the operation must not apply twice", got)
	}
	if got := keyRows(t, pool, "k-second"); got != 0 {
		t.Fatalf("refused operation left %d key row(s) behind", got)
	}
	if got := countRows(t, pool, "wager_transactions"); got != 1 {
		t.Fatalf("wager rows = %d, want 1", got)
	}
	assertLedgerMatchesBalance(t, pool, walletID)
}

// TestProcess_DistinctWalletsAdvanceInParallel runs one bet per wallet at the
// same time. Every one must be processed, and each wallet must reconcile.
func TestProcess_DistinctWalletsAdvanceInParallel(t *testing.T) {
	pool := newTestPool(t)
	repo := NewWalletRepository(pool)
	proc := NewWagerProcessor(pool)

	const wallets = 20
	ids := make([]uuid.UUID, wallets)
	for i := range ids {
		ids[i] = createTestWallet(t, repo, 10000)
	}

	outcomes := make([]Outcome, wallets)
	errs := make([]error, wallets)
	var wg sync.WaitGroup
	for i := range ids {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := wagerReq(t, ids[i], ownerOf(t, pool, ids[i]), domain.WagerKindBet,
				"bet-"+ids[i].String(), "", 2500, "k-"+ids[i].String())
			outcomes[i], errs[i] = proc.Process(context.Background(), req)
		}()
	}
	wg.Wait()

	for i, id := range ids {
		if errs[i] != nil {
			t.Fatalf("wallet %d: %v", i, errs[i])
		}
		if outcomes[i].StatusCode != statusProcessed {
			t.Fatalf("wallet %d: status %d, want 201", i, outcomes[i].StatusCode)
		}
		if got := balanceOf(t, pool, id); got != 7500 {
			t.Fatalf("wallet %d: balance %d, want 7500", i, got)
		}
		assertLedgerMatchesBalance(t, pool, id)
	}
}

// TestProcess_ABusyWalletDoesNotBlockAnotherOne proves there is no global lock:
// while a transaction holds wallet A's row lock, an operation on wallet B must
// still finish. Wallet A stays locked for the whole call.
func TestProcess_ABusyWalletDoesNotBlockAnotherOne(t *testing.T) {
	pool := newTestPool(t)
	repo := NewWalletRepository(pool)
	proc := NewWagerProcessor(pool)
	ctx := context.Background()

	walletA := createTestWallet(t, repo, 10000)
	walletB := createTestWallet(t, repo, 10000)

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := repo.LockForUpdate(ctx, tx, walletA); err != nil {
		t.Fatalf("lock A: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		req := wagerReq(t, walletB, ownerOf(t, pool, walletB), domain.WagerKindBet, "bet-b", "", 2500, "k-b")
		_, err := proc.Process(ctx, req)
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("wallet B: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("wallet B was blocked by the lock on wallet A")
	}
	if got := balanceOf(t, pool, walletB); got != 7500 {
		t.Fatalf("wallet B balance = %d, want 7500", got)
	}
}
