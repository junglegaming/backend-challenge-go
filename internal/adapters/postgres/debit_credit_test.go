package postgres

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/VictorM0nteiro/backend-challenge-go/internal/domain"
)

func mustMoney(t *testing.T, minor int64, currency string) domain.Money {
	t.Helper()
	m, err := domain.NewMoney(minor, currency)
	if err != nil {
		t.Fatalf("NewMoney: %v", err)
	}
	return m
}

func createTestWallet(t *testing.T, repo *WalletRepository, balanceMinor int64) uuid.UUID {
	t.Helper()
	w, err := domain.NewWallet(uuid.New(), "BRL", mustMoney(t, balanceMinor, "BRL"))
	if err != nil {
		t.Fatalf("NewWallet: %v", err)
	}
	if err := repo.Create(context.Background(), w); err != nil {
		t.Fatalf("Create: %v", err)
	}
	return w.ID()
}

func countRows(t *testing.T, pool *Pool, table string, args ...any) int {
	t.Helper()
	query := "SELECT count(*) FROM " + table
	if len(args) > 0 {
		query += " WHERE wallet_id = $1"
	}
	var n int
	if err := pool.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

// TestConcurrentBets_OneWinsOneLoses is the mandatory scenario from README
// §8: a wallet holding 100.00 BRL receives two simultaneous 80.00 BRL bets.
// Exactly one must be processed, the other rejected for insufficient funds,
// the final balance must be 20.00 BRL, and the ledger must hold exactly one
// debit — not zero (both rejected), not two (both processed).
func TestConcurrentBets_OneWinsOneLoses(t *testing.T) {
	pool := newTestPool(t)
	repo := NewWalletRepository(pool)
	ctx := context.Background()

	walletID := createTestWallet(t, repo, 10000) // 100.00 BRL
	bet := mustMoney(t, 8000, "BRL")             // 80.00 BRL

	var (
		wg      sync.WaitGroup
		results [2]error
	)
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, results[i] = DebitWallet(ctx, pool, walletID, uuid.New(), bet)
		}(i)
	}
	wg.Wait()

	successes, rejections := 0, 0
	for _, err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, domain.ErrInsufficientFunds):
			rejections++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if successes != 1 || rejections != 1 {
		t.Fatalf("successes=%d rejections=%d, want exactly 1 and 1", successes, rejections)
	}

	wallet, err := repo.FindByID(ctx, walletID)
	if err != nil {
		t.Fatalf("FindByID: %v", err)
	}
	if got := wallet.Balance().AmountMinor(); got != 2000 {
		t.Fatalf("final balance = %d, want 2000 (20.00 BRL)", got)
	}
	if got := countRows(t, pool, "wallet_ledger_entries", walletID); got != 1 {
		t.Fatalf("ledger rows for this wallet = %d, want exactly 1", got)
	}
}

// TestDebitWallet_ExactBalance_Succeeds documents the boundary explicitly:
// debiting exactly the available balance is allowed (balance must stay
// >= 0, not > 0).
func TestDebitWallet_ExactBalance_Succeeds(t *testing.T) {
	pool := newTestPool(t)
	repo := NewWalletRepository(pool)
	ctx := context.Background()

	walletID := createTestWallet(t, repo, 5000)

	if _, err := DebitWallet(ctx, pool, walletID, uuid.New(), mustMoney(t, 5000, "BRL")); err != nil {
		t.Fatalf("DebitWallet: %v", err)
	}

	wallet, err := repo.FindByID(ctx, walletID)
	if err != nil {
		t.Fatalf("FindByID: %v", err)
	}
	if got := wallet.Balance().AmountMinor(); got != 0 {
		t.Fatalf("balance = %d, want 0", got)
	}
}

// TestCreditWallet_IncreasesBalanceAndVersion is the credit-side happy path.
func TestCreditWallet_IncreasesBalanceAndVersion(t *testing.T) {
	pool := newTestPool(t)
	repo := NewWalletRepository(pool)
	ctx := context.Background()

	walletID := createTestWallet(t, repo, 1000)

	entry, err := CreditWallet(ctx, pool, walletID, uuid.New(), mustMoney(t, 500, "BRL"))
	if err != nil {
		t.Fatalf("CreditWallet: %v", err)
	}
	if entry.Direction() != domain.DirectionCredit {
		t.Errorf("direction = %q, want CREDIT", entry.Direction())
	}

	wallet, err := repo.FindByID(ctx, walletID)
	if err != nil {
		t.Fatalf("FindByID: %v", err)
	}
	if got := wallet.Balance().AmountMinor(); got != 1500 {
		t.Fatalf("balance = %d, want 1500", got)
	}
	if wallet.Version() != 2 {
		t.Errorf("version = %d, want 2", wallet.Version())
	}
}

// TestInsertLedgerEntry_RejectsDuplicateTransactionID proves that the
// database's own UNIQUE(wallet_id, transaction_id) constraint — not
// application code — is what stops the same transaction from being applied
// to the same wallet twice.
func TestInsertLedgerEntry_RejectsDuplicateTransactionID(t *testing.T) {
	pool := newTestPool(t)
	repo := NewWalletRepository(pool)
	ctx := context.Background()

	walletID := createTestWallet(t, repo, 1000)
	transactionID := uuid.New()

	if _, err := DebitWallet(ctx, pool, walletID, transactionID, mustMoney(t, 100, "BRL")); err != nil {
		t.Fatalf("first debit: %v", err)
	}

	// Re-run the exact same movement with the same transactionID, bypassing
	// any application-level idempotency check that might exist later, to
	// prove the schema itself is the backstop.
	_, err := DebitWallet(ctx, pool, walletID, transactionID, mustMoney(t, 100, "BRL"))
	if !errors.Is(err, ErrDuplicateLedgerEntry) {
		t.Fatalf("err = %v, want ErrDuplicateLedgerEntry", err)
	}

	wallet, err := repo.FindByID(ctx, walletID)
	if err != nil {
		t.Fatalf("FindByID: %v", err)
	}
	// The second attempt's debit must have been rolled back along with the
	// failed insert — the wallet should reflect only the first debit.
	if got := wallet.Balance().AmountMinor(); got != 900 {
		t.Fatalf("balance = %d, want 900 (only the first debit applied)", got)
	}
}
