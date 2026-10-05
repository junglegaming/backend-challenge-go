package domain

import (
	"errors"
	"testing"

	"github.com/google/uuid"
)

func TestOpenWallet_ZeroCreatesOnlyTheWallet(t *testing.T) {
	open, err := OpenWallet(uuid.New(), "BRL", mustMoney(t, 0, "BRL"))
	if err != nil {
		t.Fatalf("OpenWallet: %v", err)
	}
	if open.Wallet.Version() != 1 || !open.Wallet.Balance().IsZero() {
		t.Fatalf("wallet version = %d, balance = %s; want version 1 and zero", open.Wallet.Version(), open.Wallet.Balance())
	}
	if open.Operation != nil || open.Entry != nil {
		t.Fatal("a zero opening must not produce an OPENING, a ledger line or events")
	}
}

func TestOpenWallet_PositiveProducesOpeningAndCredit(t *testing.T) {
	open, err := OpenWallet(uuid.New(), "BRL", mustMoney(t, 100000, "BRL"))
	if err != nil {
		t.Fatalf("OpenWallet: %v", err)
	}

	if open.Wallet.Version() != 1 {
		t.Fatalf("version = %d, want 1 (an opening must not bump the version)", open.Wallet.Version())
	}
	if open.Wallet.Balance().AmountMinor() != 100000 {
		t.Fatalf("balance = %d, want 100000", open.Wallet.Balance().AmountMinor())
	}

	op := open.Operation
	if op == nil || op.Kind() != WagerKindOpening || op.State() != WagerStateProcessed {
		t.Fatalf("operation = %+v, want OPENING in PROCESSED", op)
	}
	if op.BalanceAfter() == nil || op.BalanceAfter().AmountMinor() != 100000 {
		t.Fatalf("operation balanceAfter = %v, want 100000", op.BalanceAfter())
	}

	e := open.Entry
	if e == nil || e.Direction() != DirectionCredit {
		t.Fatalf("entry = %+v, want a CREDIT", e)
	}
	if e.TransactionID() != op.ID() || e.WalletID() != open.Wallet.ID() {
		t.Fatal("the credit must point at the OPENING and at this wallet")
	}
	if !e.BalanceBefore().IsZero() || e.BalanceAfter().AmountMinor() != 100000 {
		t.Fatalf("entry balances = %s -> %s, want 0 -> 100000", e.BalanceBefore(), e.BalanceAfter())
	}
}

func TestOpenWallet_RejectsCurrencyMismatchAndNegative(t *testing.T) {
	player := uuid.New()

	if _, err := OpenWallet(player, "USD", mustMoney(t, 100, "BRL")); !errors.Is(err, ErrCurrencyMismatch) {
		t.Fatalf("err = %v, want ErrCurrencyMismatch", err)
	}
	if _, err := OpenWallet(player, "BRL", mustMoney(t, -100, "BRL")); !errors.Is(err, ErrInvalidAmount) {
		t.Fatalf("err = %v, want ErrInvalidAmount", err)
	}
}

// Explicação

// - WalletOpening devolve as três peças juntas. Operation e Entry são nil no saldo zero, e isso fica explícito no tipo. Quem persiste só precisa checar nil.
// - O OPENING é montado por newOpeningOperation, sem passar por NewWagerTransaction. Assim a regra "OPENING nunca vem de fora" continua valendo na porta de entrada, e a abertura interna ainda consegue existir.
// - idempotencyKey e requestHash ficam vazios. A coluna é NOT NULL, mas "" é um valor válido. A unicidade da abertura vem de UNIQUE(player_id, currency) na tabela de carteiras, não da tabela de idempotência.
// - O lançamento usa newWalletLedgerEntry, que recalcula balanceAfter e confere a aritmética. Não há caminho em que o saldo da carteira e o ledger divergem.
