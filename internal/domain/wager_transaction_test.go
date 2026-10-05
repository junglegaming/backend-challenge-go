package domain

import (
	"errors"
	"testing"

	"github.com/google/uuid"
)

func validCommand(t *testing.T, kind WagerKind, minor int64) WagerCommand {
	t.Helper()
	return WagerCommand{
		ProviderID:            "provider-a",
		ExternalTransactionID: "transaction-123",
		IdempotencyKey:        "provider-a:transaction-123",
		RequestHash:           "hash-abc",
		WalletID:              uuid.New(),
		PlayerID:              uuid.New(),
		RoundID:               "round-987",
		GameID:                "fortune-chimp",
		Kind:                  kind,
		Amount:                mustMoney(t, minor, "BRL"),
	}
}

func processedBet(t *testing.T, minor int64) *WagerTransaction {
	t.Helper()
	bet, err := NewWagerTransaction(validCommand(t, WagerKindBet, minor))
	if err != nil {
		t.Fatalf("NewWagerTransaction: %v", err)
	}
	if err := bet.MarkProcessed(mustMoney(t, 0, "BRL")); err != nil {
		t.Fatalf("MarkProcessed: %v", err)
	}
	return bet
}

func TestNewWagerTransaction_Validation(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*WagerCommand)
		minor   int64
		kind    WagerKind
		wantErr error
	}{
		{"bet_valido", func(*WagerCommand) {}, 2500, WagerKindBet, nil},
		{"win_valido_sem_referencia", func(*WagerCommand) {}, 2500, WagerKindWin, nil},
		{"loss_zero_e_valido", func(*WagerCommand) {}, 0, WagerKindLoss, nil},
		{"loss_diferente_de_zero_e_rejeitado", func(*WagerCommand) {}, 100, WagerKindLoss, ErrInvalidWagerTransaction},
		{"bet_zero_e_rejeitado", func(*WagerCommand) {}, 0, WagerKindBet, ErrInvalidAmount},
		{"bet_negativo_e_rejeitado", func(*WagerCommand) {}, -100, WagerKindBet, ErrInvalidAmount},
		{"opening_vindo_de_fora_e_rejeitado", func(*WagerCommand) {}, 100, WagerKindOpening, ErrInvalidWagerTransaction},
		{"tipo_desconhecido_e_rejeitado", func(*WagerCommand) {}, 100, WagerKind("CHARGEBACK"), ErrInvalidWagerTransaction},
		{"refund_sem_referencia_e_rejeitado", func(*WagerCommand) {}, 100, WagerKindRefund, ErrInvalidWagerTransaction},
		{"rollback_sem_referencia_e_rejeitado", func(*WagerCommand) {}, 100, WagerKindRollback, ErrInvalidWagerTransaction},
		{"refund_com_referencia_e_valido", func(c *WagerCommand) { c.ReferenceExternalTransactionID = "bet-1" }, 100, WagerKindRefund, nil},
		{"bet_com_referencia_e_rejeitado", func(c *WagerCommand) { c.ReferenceExternalTransactionID = "bet-1" }, 100, WagerKindBet, ErrInvalidWagerTransaction},
		{"provider_vazio_e_rejeitado", func(c *WagerCommand) { c.ProviderID = "" }, 100, WagerKindBet, ErrInvalidWagerTransaction},
		{"wallet_nula_e_rejeitada", func(c *WagerCommand) { c.WalletID = uuid.Nil }, 100, WagerKindBet, ErrInvalidWagerTransaction},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := validCommand(t, tt.kind, tt.minor)
			tt.mutate(&cmd)

			wt, err := NewWagerTransaction(cmd)

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr == nil {
				if wt.State() != WagerStatePending {
					t.Errorf("initial state = %s, want PENDING", wt.State())
				}
				if wt.ID() == uuid.Nil {
					t.Error("expected a generated id")
				}
			}
		})
	}
}

func TestWagerTransaction_LegalTransitions(t *testing.T) {
	t.Run("pending_para_processed", func(t *testing.T) {
		wt, _ := NewWagerTransaction(validCommand(t, WagerKindBet, 2500))
		if err := wt.MarkProcessed(mustMoney(t, 7500, "BRL")); err != nil {
			t.Fatalf("MarkProcessed: %v", err)
		}
		if wt.State() != WagerStateProcessed || !wt.IsTerminal() {
			t.Fatalf("state = %s terminal=%v, want PROCESSED and terminal", wt.State(), wt.IsTerminal())
		}
		if wt.BalanceAfter() == nil || wt.BalanceAfter().AmountMinor() != 7500 {
			t.Fatalf("balanceAfter = %v, want 7500 stored", wt.BalanceAfter())
		}
	})

	t.Run("pending_reference_resolve_depois_processed", func(t *testing.T) {
		wt, _ := NewWagerTransaction(validCommand(t, WagerKindWin, 100))
		if err := wt.MarkPendingReference(FailureReferenceNotFound); err != nil {
			t.Fatalf("MarkPendingReference: %v", err)
		}
		if err := wt.ResolveReference(uuid.New()); err != nil {
			t.Fatalf("ResolveReference: %v", err)
		}
		if wt.State() != WagerStatePending {
			t.Fatalf("state = %s, want PENDING after resolving", wt.State())
		}
		if err := wt.MarkProcessed(mustMoney(t, 100, "BRL")); err != nil {
			t.Fatalf("MarkProcessed: %v", err)
		}
	})

	t.Run("pending_reference_para_rejected_por_expiracao", func(t *testing.T) {
		cmd := validCommand(t, WagerKindRollback, 100)
		cmd.ReferenceExternalTransactionID = "bet-1"
		wt, err := NewWagerTransaction(cmd)
		if err != nil {
			t.Fatalf("NewWagerTransaction: %v", err)
		}
		if err := wt.MarkPendingReference(FailureReferenceNotFound); err != nil {
			t.Fatalf("MarkPendingReference: %v", err)
		}
		if err := wt.MarkRejected(FailureReferenceExpired); err != nil {
			t.Fatalf("MarkRejected: %v", err)
		}
		if wt.FailureCode() != FailureReferenceExpired {
			t.Fatalf("failure code = %q, want reference_expired", wt.FailureCode())
		}
	})
}

func TestWagerTransaction_RejectsIllegalTransitions(t *testing.T) {
	t.Run("terminal_nao_muda_de_estado", func(t *testing.T) {
		wt := processedBet(t, 2500)
		if err := wt.MarkRejected(FailureInsufficientFunds); !errors.Is(err, ErrInvalidWagerTransition) {
			t.Fatalf("MarkRejected after PROCESSED: err = %v, want ErrInvalidWagerTransition", err)
		}
		if err := wt.MarkProcessed(mustMoney(t, 0, "BRL")); !errors.Is(err, ErrInvalidWagerTransition) {
			t.Fatalf("MarkProcessed twice: err = %v, want ErrInvalidWagerTransition", err)
		}
		if wt.State() != WagerStateProcessed {
			t.Fatalf("state changed to %s despite rejected transition", wt.State())
		}
	})

	t.Run("rejeicao_sem_codigo_e_rejeitada", func(t *testing.T) {
		wt, _ := NewWagerTransaction(validCommand(t, WagerKindBet, 2500))
		if err := wt.MarkRejected(""); !errors.Is(err, ErrInvalidWagerTransition) {
			t.Fatalf("err = %v, want ErrInvalidWagerTransition", err)
		}
	})

	t.Run("resolver_sem_estar_pendente_de_referencia", func(t *testing.T) {
		wt, _ := NewWagerTransaction(validCommand(t, WagerKindBet, 2500))
		if err := wt.ResolveReference(uuid.New()); !errors.Is(err, ErrInvalidWagerTransition) {
			t.Fatalf("err = %v, want ErrInvalidWagerTransition", err)
		}
	})
}

func TestValidateReversal(t *testing.T) {
	bet := processedBet(t, 2500)
	reversalOf := func(kind WagerKind, minor int64) *WagerTransaction {
		cmd := validCommand(t, kind, minor)
		cmd.PlayerID = bet.PlayerID()
		cmd.WalletID = bet.WalletID()
		cmd.ProviderID = bet.ProviderID()
		cmd.RoundID = bet.RoundID()
		cmd.ReferenceExternalTransactionID = bet.ExternalTransactionID()
		wt, err := NewWagerTransaction(cmd)
		if err != nil {
			t.Fatalf("NewWagerTransaction reversal: %v", err)
		}
		return wt
	}

	pendingBet, _ := NewWagerTransaction(validCommand(t, WagerKindBet, 2500))

	tests := []struct {
		name       string
		reversal   *WagerTransaction
		referenced *WagerTransaction
		already    bool
		want       FailureCode
	}{
		{"refund_de_bet_processada_e_valido", reversalOf(WagerKindRefund, 2500), bet, false, ""},
		{"referencia_inexistente", reversalOf(WagerKindRefund, 2500), nil, false, FailureReferenceNotFound},
		{"referencia_pendente_nao_serve", reversalOf(WagerKindRefund, 2500), pendingBet, false, FailureReferenceNotProcessed},
		{"valor_diferente_e_rejeitado", reversalOf(WagerKindRefund, 1000), bet, false, FailureReferenceAmountMismatch},
		{"refund_de_win_nao_e_permitido", reversalOf(WagerKindRefund, 2500), winOf(t, bet), false, FailureReferenceMismatch},
		{"rollback_de_win_e_valido", reversalOf(WagerKindRollback, 2500), winOf(t, bet), false, ""},
		{"segunda_reversao_e_duplicada", reversalOf(WagerKindRefund, 2500), bet, true, FailureDuplicateReversal},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ValidateReversal(tt.reversal, tt.referenced, tt.already); got != tt.want {
				t.Fatalf("ValidateReversal = %q, want %q", got, tt.want)
			}
		})
	}
}

func winOf(t *testing.T, bet *WagerTransaction) *WagerTransaction {
	t.Helper()
	cmd := validCommand(t, WagerKindWin, 2500)
	cmd.PlayerID = bet.PlayerID()
	cmd.WalletID = bet.WalletID()
	cmd.ProviderID = bet.ProviderID()
	cmd.RoundID = bet.RoundID()
	win, err := NewWagerTransaction(cmd)
	if err != nil {
		t.Fatalf("NewWagerTransaction win: %v", err)
	}
	if err := win.MarkProcessed(mustMoney(t, 0, "BRL")); err != nil {
		t.Fatalf("MarkProcessed win: %v", err)
	}
	return win
}
