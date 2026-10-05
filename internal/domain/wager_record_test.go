package domain

import (
	"errors"
	"testing"

	"github.com/google/uuid"
)

func TestWagerRecord_RoundTripPreservesEveryField(t *testing.T) {
	bet, err := NewWagerTransaction(validCommand(t, WagerKindBet, 2500))
	if err != nil {
		t.Fatalf("NewWagerTransaction: %v", err)
	}
	if err := bet.MarkProcessed(mustMoney(t, 7500, "BRL")); err != nil {
		t.Fatalf("MarkProcessed: %v", err)
	}

	back := RehydrateWagerTransaction(bet.Record())

	if back.Record() != bet.Record() {
		t.Fatalf("round trip changed the record:\n got  %+v\n want %+v", back.Record(), bet.Record())
	}
	if back.State() != WagerStateProcessed || back.BalanceAfter() == nil {
		t.Fatalf("rehydrated state = %s, balanceAfter = %v", back.State(), back.BalanceAfter())
	}
}

func TestBindReference_OnlyWhilePending(t *testing.T) {
	t.Run("pending_aceita_a_referencia", func(t *testing.T) {
		cmd := validCommand(t, WagerKindRefund, 2500)
		cmd.ReferenceExternalTransactionID = "bet-1"
		refund, err := NewWagerTransaction(cmd)
		if err != nil {
			t.Fatalf("NewWagerTransaction: %v", err)
		}
		target := uuid.New()
		if err := refund.BindReference(target); err != nil {
			t.Fatalf("BindReference: %v", err)
		}
		if refund.ReferenceTransactionID() != target {
			t.Fatalf("reference = %s, want %s", refund.ReferenceTransactionID(), target)
		}
	})

	t.Run("processada_nao_aceita_nova_referencia", func(t *testing.T) {
		wt := processedBet(t, 2500)
		if err := wt.BindReference(uuid.New()); !errors.Is(err, ErrInvalidWagerTransition) {
			t.Fatalf("err = %v, want ErrInvalidWagerTransition", err)
		}
	})
}
