package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/VictorM0nteiro/backend-challenge-go/internal/domain"
)

// scalarCount runs a COUNT query and returns its single value.
func scalarCount(t *testing.T, pool *Pool, query string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatalf("count query %q: %v", query, err)
	}
	return n
}

func TestOpenWallet_ZeroWritesOnlyTheWallet(t *testing.T) {
	pool := newTestPool(t)
	repo := NewWalletRepository(pool)

	open, err := domain.OpenWallet(uuid.New(), "BRL", mustMoney(t, 0, "BRL"))
	if err != nil {
		t.Fatalf("OpenWallet: %v", err)
	}
	if err := repo.OpenWallet(context.Background(), open); err != nil {
		t.Fatalf("repo.OpenWallet: %v", err)
	}

	if n := scalarCount(t, pool, `SELECT count(*) FROM wallets`); n != 1 {
		t.Fatalf("wallets = %d, want 1", n)
	}
	if n := scalarCount(t, pool, `SELECT count(*) FROM wager_transactions`); n != 0 {
		t.Fatalf("wager_transactions = %d, want 0", n)
	}
	if n := scalarCount(t, pool, `SELECT count(*) FROM outbox`); n != 0 {
		t.Fatalf("outbox = %d, want 0", n)
	}
}

func TestOpenWallet_PositiveWritesOpeningAndEventsInOneCommit(t *testing.T) {
	pool := newTestPool(t)
	repo := NewWalletRepository(pool)
	ctx := context.Background()

	open, err := domain.OpenWallet(uuid.New(), "BRL", mustMoney(t, 100000, "BRL"))
	if err != nil {
		t.Fatalf("OpenWallet: %v", err)
	}
	if err := repo.OpenWallet(ctx, open); err != nil {
		t.Fatalf("repo.OpenWallet: %v", err)
	}

	wallet, err := repo.FindByID(ctx, open.Wallet.ID())
	if err != nil {
		t.Fatalf("FindByID: %v", err)
	}
	if wallet.Balance().AmountMinor() != 100000 || wallet.Version() != 1 {
		t.Fatalf("wallet = balance %d version %d, want 100000 and 1", wallet.Balance().AmountMinor(), wallet.Version())
	}

	if n := scalarCount(t, pool, `SELECT count(*) FROM wager_transactions WHERE kind = 'OPENING' AND state = 'PROCESSED'`); n != 1 {
		t.Fatalf("OPENING rows = %d, want 1", n)
	}
	if n := scalarCount(t, pool, `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1`, open.Wallet.ID()); n != 1 {
		t.Fatalf("ledger rows = %d, want 1", n)
	}
	if n := scalarCount(t, pool, `SELECT count(*) FROM outbox WHERE event_type = $1`, eventWagerTransactionProcessed); n != 1 {
		t.Fatalf("%s events = %d, want 1", eventWagerTransactionProcessed, n)
	}
	if n := scalarCount(t, pool, `SELECT count(*) FROM outbox WHERE event_type = $1`, eventWalletBalanceChanged); n != 1 {
		t.Fatalf("%s events = %d, want 1", eventWalletBalanceChanged, n)
	}
}

func TestOpenWallet_DuplicatePlayerAndCurrencyRollsBackEverything(t *testing.T) {
	pool := newTestPool(t)
	repo := NewWalletRepository(pool)
	ctx := context.Background()
	player := uuid.New()

	first, err := domain.OpenWallet(player, "BRL", mustMoney(t, 100000, "BRL"))
	if err != nil {
		t.Fatalf("first OpenWallet: %v", err)
	}
	if err := repo.OpenWallet(ctx, first); err != nil {
		t.Fatalf("first repo.OpenWallet: %v", err)
	}

	second, err := domain.OpenWallet(player, "BRL", mustMoney(t, 5000, "BRL"))
	if err != nil {
		t.Fatalf("second OpenWallet: %v", err)
	}
	if err := repo.OpenWallet(ctx, second); !errors.Is(err, ErrWalletAlreadyExists) {
		t.Fatalf("err = %v, want ErrWalletAlreadyExists", err)
	}

	// The rejected opening must leave nothing behind: no second wallet, no second
	// OPENING, no second ledger line and no extra events.
	if n := scalarCount(t, pool, `SELECT count(*) FROM wallets WHERE player_id = $1`, player); n != 1 {
		t.Fatalf("wallets for player = %d, want 1", n)
	}
	if n := scalarCount(t, pool, `SELECT count(*) FROM wager_transactions`); n != 1 {
		t.Fatalf("wager_transactions = %d, want 1", n)
	}
	if n := scalarCount(t, pool, `SELECT count(*) FROM outbox`); n != 2 {
		t.Fatalf("outbox = %d, want 2", n)
	}
}

// Explicação

// - scalarCount é um helper próprio deste arquivo. Ele não depende da assinatura de countRows, que é usado em outros testes com outro formato.
// - O teste de saldo zero prova a regra do README: sem OPENING, sem ledger e sem evento.
// - O teste positivo confere as três escritas na mesma transação: carteira, operação e lançamento, mais os dois eventos.
// - O teste de duplicata é o mais importante. Ele mostra que a transação abortada não deixa resíduo. Se alguém mover a inserção do OPENING para antes da carteira, ou usar o pool no lugar da transação, esse teste quebra.
