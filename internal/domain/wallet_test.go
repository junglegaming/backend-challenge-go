package domain

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func mustMoney(t *testing.T, minor int64, currency string) Money {
	t.Helper()
	m, err := NewMoney(minor, currency)
	if err != nil {
		t.Fatalf("NewMoney: %v", err)
	}
	return m
}

func TestWallet_Debit(t *testing.T) {
	tests := []struct {
		name          string
		balance       int64
		debit         int64
		wantErr       error
		wantRemaining int64
	}{
		{"saldo_exatamente_igual_ao_valor_permite_debito", 5000, 5000, nil, 0},
		{"saldo_um_centavo_menor_que_o_valor_rejeita", 4999, 5000, ErrInsufficientFunds, 4999},
		{"debito_zero_e_rejeitado", 5000, 0, ErrInvalidAmount, 5000},
		{"debito_negativo_e_rejeitado", 5000, -100, ErrInvalidAmount, 5000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w, err := NewWallet(uuid.New(), "BRL", mustMoney(t, tt.balance, "BRL"))
			if err != nil {
				t.Fatalf("NewWallet: %v", err)
			}

			entry, err := w.Debit(uuid.New(), mustMoney(t, tt.debit, "BRL"))

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if w.Balance().AmountMinor() != tt.wantRemaining {
				t.Fatalf("balance = %d, want %d", w.Balance().AmountMinor(), tt.wantRemaining)
			}

			if tt.wantErr == nil {
				if entry == nil {
					t.Fatal("expected a ledger entry, got nil")
				}
				if entry.Direction() != DirectionDebit {
					t.Errorf("direction = %q, want DEBIT", entry.Direction())
				}
				if entry.BalanceAfter().AmountMinor() != tt.wantRemaining {
					t.Errorf("entry.BalanceAfter = %d, want %d", entry.BalanceAfter().AmountMinor(), tt.wantRemaining)
				}
				if w.Version() != 2 {
					t.Errorf("version = %d, want 2", w.Version())
				}
			} else if entry != nil {
				t.Error("expected no ledger entry on a rejected debit")
			}
		})
	}
}

func TestWallet_Credit(t *testing.T) {
	w, err := NewWallet(uuid.New(), "BRL", mustMoney(t, 1000, "BRL"))
	if err != nil {
		t.Fatalf("NewWallet: %v", err)
	}

	entry, err := w.Credit(uuid.New(), mustMoney(t, 500, "BRL"))
	if err != nil {
		t.Fatalf("Credit: %v", err)
	}
	if w.Balance().AmountMinor() != 1500 {
		t.Fatalf("balance = %d, want 1500", w.Balance().AmountMinor())
	}
	if entry.Direction() != DirectionCredit {
		t.Errorf("direction = %q, want CREDIT", entry.Direction())
	}
	if w.Version() != 2 {
		t.Errorf("version = %d, want 2", w.Version())
	}
}

func TestWallet_Debit_RejectsCurrencyMismatch(t *testing.T) {
	w, _ := NewWallet(uuid.New(), "BRL", mustMoney(t, 1000, "BRL"))
	usd := mustMoney(t, 100, "USD")
	if _, err := w.Debit(uuid.New(), usd); !errors.Is(err, ErrCurrencyMismatch) {
		t.Fatalf("err = %v, want ErrCurrencyMismatch", err)
	}
}

func TestNewWallet_RejectsNegativeInitialBalance(t *testing.T) {
	neg := mustMoney(t, -100, "BRL")
	if _, err := NewWallet(uuid.New(), "BRL", neg); !errors.Is(err, ErrInvalidAmount) {
		t.Fatalf("err = %v, want ErrInvalidAmount", err)
	}
}

func TestNewWallet_RejectsCurrencyMismatchWithInitialBalance(t *testing.T) {
	usd := mustMoney(t, 100, "USD")
	if _, err := NewWallet(uuid.New(), "BRL", usd); !errors.Is(err, ErrCurrencyMismatch) {
		t.Fatalf("err = %v, want ErrCurrencyMismatch", err)
	}
}

func TestWalletLedgerEntry_InvariantHoldsAfterDebit(t *testing.T) {
	w, _ := NewWallet(uuid.New(), "BRL", mustMoney(t, 1000, "BRL"))

	entry, err := w.Debit(uuid.New(), mustMoney(t, 400, "BRL"))
	if err != nil {
		t.Fatalf("Debit: %v", err)
	}

	before := entry.BalanceBefore().AmountMinor()
	after := entry.BalanceAfter().AmountMinor()
	amount := entry.Amount().AmountMinor()
	if before-amount != after {
		t.Fatalf("balanceAfter (%d) != balanceBefore (%d) - amount (%d)", after, before, amount)
	}
}

func TestRehydrateWallet_DoesNotReapplyAnything(t *testing.T) {
	balance := mustMoney(t, 7000, "BRL")
	id, playerID := uuid.New(), uuid.New()

	now := time.Now().UTC()
	w := RehydrateWallet(id, playerID, "BRL", balance, 5, now, now)

	if w.Version() != 5 {
		t.Fatalf("version = %d, want 5 (rehydration must not bump it)", w.Version())
	}
	if w.Balance().AmountMinor() != 7000 {
		t.Fatalf("balance = %d, want 7000 (rehydration must not move it)", w.Balance().AmountMinor())
	}
}
