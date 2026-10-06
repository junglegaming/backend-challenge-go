package domain

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

var now = time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)

func brl(t *testing.T, amount string) Money { return mustMoney(t, amount, "BRL") }

func newWallet(t *testing.T, initial string) *Wallet {
	t.Helper()
	w, err := NewWallet(uuid.New(), uuid.New(), brl(t, initial), now)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func TestWalletCreation(t *testing.T) {
	w := newWallet(t, "100")
	if w.Version() != 1 || w.Balance().Amount() != "100.00" {
		t.Fatalf("version=%d balance=%s", w.Version(), w.Balance())
	}
	if _, err := NewWallet(uuid.Nil, uuid.New(), brl(t, "1"), now); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("id nulo: %v", err)
	}
	if _, err := NewWallet(uuid.New(), uuid.New(), Money{}, now); !errors.Is(err, ErrUninitialized) {
		t.Errorf("money zero-value: %v", err)
	}
}

func TestWalletDebitCredit(t *testing.T) {
	w := newWallet(t, "100")
	m, err := w.Debit(brl(t, "80"), now)
	if err != nil || m.BalanceBefore.Amount() != "100.00" || m.BalanceAfter.Amount() != "20.00" || m.WalletVersion != 2 {
		t.Fatalf("debit: %+v %v", m, err)
	}
	if _, err := w.Debit(brl(t, "80"), now); !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("segundo débito: %v", err)
	}
	if w.Balance().Amount() != "20.00" || w.Version() != 2 {
		t.Fatal("débito rejeitado não pode alterar saldo nem versão")
	}
	if _, err := w.Credit(brl(t, "5"), now); err != nil || w.Version() != 3 {
		t.Fatalf("credit: %v", err)
	}
	if _, err := w.Debit(mustMoney(t, "1", "USD"), now); !errors.Is(err, ErrCurrencyMismatch) {
		t.Errorf("moeda: %v", err)
	}
	if _, err := w.Credit(brl(t, "0"), now); !errors.Is(err, ErrInvalidAmount) {
		t.Errorf("zero: %v", err)
	}
}

func TestWalletRehydrationDoesNotChangeState(t *testing.T) {
	w, err := RehydrateWallet(uuid.New(), uuid.New(), brl(t, "7.5"), 9, now, now)
	if err != nil || w.Version() != 9 || w.Balance().Amount() != "7.50" {
		t.Fatalf("%v", err)
	}
	neg, _ := MoneyFromUnits(-1, "BRL")
	if _, err := RehydrateWallet(uuid.New(), uuid.New(), neg, 1, now, now); err == nil {
		t.Error("reidratação com saldo negativo")
	}
	if _, err := RehydrateWallet(uuid.New(), uuid.New(), brl(t, "1"), 0, now, now); err == nil {
		t.Error("reidratação com versão 0")
	}
}

func TestLedgerEntryValidatesArithmetic(t *testing.T) {
	id, wid, tid := uuid.New(), uuid.New(), uuid.New()
	if _, err := NewLedgerEntry(id, wid, tid, Debit, brl(t, "25"), brl(t, "100"), brl(t, "75"), now); err != nil {
		t.Fatal(err)
	}
	if _, err := NewLedgerEntry(id, wid, tid, Credit, brl(t, "25"), brl(t, "100"), brl(t, "125"), now); err != nil {
		t.Fatal(err)
	}
	if _, err := NewLedgerEntry(id, wid, tid, Debit, brl(t, "25"), brl(t, "100"), brl(t, "80"), now); err == nil {
		t.Error("aritmética inválida aceita")
	}
	if _, err := NewLedgerEntry(id, wid, tid, Debit, brl(t, "25"), brl(t, "10"), mustMoneyUnits(t, -1500), now); err == nil {
		t.Error("saldo posterior negativo aceito")
	}
}

func mustMoneyUnits(t *testing.T, u int64) Money {
	m, err := MoneyFromUnits(u, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func request(t *testing.T, kind Kind, amount, ref string) ExternalRequest {
	return ExternalRequest{
		ProviderID: "provider-a", ExternalTransactionID: "tx-" + string(kind), IdempotencyKey: "provider-a:tx",
		PlayerID: uuid.MustParse("0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1"), WalletID: uuid.MustParse("0192f291-27dd-7d3f-8071-5f8685deef37"),
		RoundID: "round-987", GameID: "fortune-chimp", Kind: kind, Money: brl(t, amount), ReferenceExternalTransactionID: ref,
	}
}

func TestZeroAmountPolicyPerKind(t *testing.T) {
	cases := []struct {
		kind   Kind
		amount string
		ref    string
		ok     bool
	}{
		{KindBet, "0", "", false}, {KindBet, "1", "", true},
		{KindWin, "0", "", false}, {KindWin, "1", "", true}, {KindWin, "1", "bet-1", true},
		{KindLoss, "0", "", true}, {KindLoss, "0.01", "", false},
		{KindRefund, "0", "bet-1", false}, {KindRefund, "1", "bet-1", true}, {KindRefund, "1", "", false},
		{KindRollback, "0", "bet-1", false}, {KindRollback, "1", "bet-1", true}, {KindRollback, "1", "", false},
		{KindBet, "1", "bet-1", false},
	}
	for _, c := range cases {
		_, err := NewExternalTransaction(uuid.New(), request(t, c.kind, c.amount, c.ref), now)
		if (err == nil) != c.ok {
			t.Errorf("%s amount=%s ref=%q: err=%v", c.kind, c.amount, c.ref, err)
		}
	}
}

func TestOpeningKindIsRejectedExternally(t *testing.T) {
	if _, err := NewExternalTransaction(uuid.New(), request(t, KindOpening, "1", ""), now); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("OPENING externo: %v", err)
	}
	if _, err := ParseExternalKind("OPENING"); err == nil {
		t.Fatal("ParseExternalKind aceitou OPENING")
	}
}

func TestOpeningTransaction(t *testing.T) {
	w := newWallet(t, "1000")
	op, err := NewOpeningTransaction(uuid.New(), w, now)
	if err != nil {
		t.Fatal(err)
	}
	if op.Origin() != OriginInternal || op.Kind() != KindOpening || op.Request().ProviderID != "" || op.PayloadHash() != "" {
		t.Fatalf("abertura carregou metadados externos: %+v", op.Request())
	}
	if err := op.MarkProcessed(w.Balance(), uuid.Nil, now); err != nil {
		t.Fatal(err)
	}
	evt, err := NewWagerTransactionProcessed(EventMeta{EventID: uuid.New(), CorrelationID: "c", OccurredAt: now}, op)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(evt)
	if strings.Contains(string(raw), "providerId") || !strings.Contains(string(raw), `"kind":"OPENING"`) {
		t.Fatalf("evento de abertura: %s", raw)
	}
	if _, err := NewOpeningTransaction(uuid.New(), newWallet(t, "0"), now); err == nil {
		t.Fatal("abertura com saldo zero não deve gerar OPENING")
	}
}

func TestTransactionStateMachine(t *testing.T) {
	tx, _ := NewExternalTransaction(uuid.New(), request(t, KindRefund, "1", "bet-1"), now)
	if tx.Status() != StatusPending {
		t.Fatal("estado inicial")
	}
	if err := tx.MarkPendingReference(now); err != nil {
		t.Fatal(err)
	}
	if err := tx.MarkPendingReference(now); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("PENDING_REFERENCE -> PENDING_REFERENCE: %v", err)
	}
	if err := tx.MarkRejected(FailureReferenceNotFound, brl(t, "0"), now); err != nil {
		t.Fatal(err)
	}
	for name, f := range map[string]func() error{
		"processed": func() error { return tx.MarkProcessed(brl(t, "1"), uuid.Nil, now) },
		"rejected":  func() error { return tx.MarkRejected(FailureInsufficientFunds, brl(t, "1"), now) },
		"failed":    func() error { return tx.MarkFailed(now) },
		"pending":   func() error { return tx.MarkPendingReference(now) },
	} {
		if err := f(); !errors.Is(err, ErrInvalidTransition) {
			t.Errorf("terminal -> %s: %v", name, err)
		}
	}
	if tx.FailureCode() != FailureReferenceNotFound {
		t.Fatal("estado terminal alterado")
	}
}

func TestRejectRequiresBusinessCode(t *testing.T) {
	tx, _ := NewExternalTransaction(uuid.New(), request(t, KindBet, "1", ""), now)
	if err := tx.MarkRejected("", brl(t, "0"), now); err == nil {
		t.Error("rejeição sem código")
	}
	if err := tx.MarkRejected(FailureInternal, brl(t, "0"), now); err == nil {
		t.Error("INTERNAL_FAILURE é falha, não rejeição")
	}
	if err := tx.MarkFailed(now); err != nil || tx.FailureCode() != FailureInternal {
		t.Fatalf("failed: %v", err)
	}
}

func TestPayloadHashEquivalenceAndConflict(t *testing.T) {
	a := request(t, KindBet, "25", "")
	b := request(t, KindBet, "25.00", "")
	b.IdempotencyKey = "outra-chave"
	ha, _ := PayloadHash(a)
	hb, _ := PayloadHash(b)
	if ha != hb {
		t.Fatal("formas equivalentes e chave diferente devem gerar o mesmo hash")
	}
	c := request(t, KindBet, "25.01", "")
	hc, _ := PayloadHash(c)
	if ha == hc {
		t.Fatal("conteúdo diferente gerou o mesmo hash")
	}
	if len(ha) != 64 {
		t.Fatalf("hash = %s", ha)
	}
}

func processed(t *testing.T, req ExternalRequest) *WagerTransaction {
	tx, err := NewExternalTransaction(uuid.New(), req, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.MarkProcessed(brl(t, "0"), uuid.Nil, now); err != nil {
		t.Fatal(err)
	}
	return tx
}

func TestEvaluateReference(t *testing.T) {
	bet := processed(t, request(t, KindBet, "25", ""))
	win := processed(t, request(t, KindWin, "40", ""))
	refundReq := request(t, KindRefund, "25", "tx-BET")

	op, _ := NewExternalTransaction(uuid.New(), refundReq, now)
	if d, _ := EvaluateReference(op, bet); d != ReferenceAccepted {
		t.Fatal("REFUND de BET válida")
	}
	if _, code := EvaluateReference(op, win); code != FailureReferenceKindInvalid {
		t.Fatalf("REFUND de WIN: %s", code)
	}

	rbWin, _ := NewExternalTransaction(uuid.New(), request(t, KindRollback, "40", "tx-WIN"), now)
	if d, _ := EvaluateReference(rbWin, win); d != ReferenceAccepted {
		t.Fatal("ROLLBACK de WIN")
	}

	partial, _ := NewExternalTransaction(uuid.New(), request(t, KindRefund, "10", "tx-BET"), now)
	if _, code := EvaluateReference(partial, bet); code != FailureReversalAmountMismatch {
		t.Fatalf("parcial: %s", code)
	}

	otherRound := request(t, KindRefund, "25", "tx-BET")
	otherRound.RoundID = "round-x"
	op2, _ := NewExternalTransaction(uuid.New(), otherRound, now)
	if _, code := EvaluateReference(op2, bet); code != FailureReferenceMismatch {
		t.Fatalf("rodada diferente: %s", code)
	}

	pendingBet, _ := NewExternalTransaction(uuid.New(), request(t, KindBet, "25", ""), now)
	if d, _ := EvaluateReference(op, pendingBet); d != ReferenceWait {
		t.Fatal("referência pendente deve aguardar")
	}
	rejectedBet, _ := NewExternalTransaction(uuid.New(), request(t, KindBet, "25", ""), now)
	_ = rejectedBet.MarkRejected(FailureInsufficientFunds, brl(t, "0"), now)
	if _, code := EvaluateReference(op, rejectedBet); code != FailureReferenceNotProcessed {
		t.Fatalf("referência rejeitada: %s", code)
	}

	winWithRef, _ := NewExternalTransaction(uuid.New(), request(t, KindWin, "100", "tx-BET"), now)
	if d, _ := EvaluateReference(winWithRef, bet); d != ReferenceAccepted {
		t.Fatal("WIN com valor diferente da BET é permitido")
	}
}

func TestMovementDirection(t *testing.T) {
	cases := []struct {
		op, ref Kind
		dir     Direction
		ok      bool
	}{
		{KindBet, "", Debit, true}, {KindWin, "", Credit, true}, {KindRefund, KindBet, Credit, true},
		{KindRollback, KindBet, Credit, true}, {KindRollback, KindWin, Debit, true}, {KindRollback, KindRefund, Debit, true},
		{KindLoss, "", "", false}, {KindOpening, "", Credit, true},
	}
	for _, c := range cases {
		d, ok := MovementDirection(c.op, c.ref)
		if d != c.dir || ok != c.ok {
			t.Errorf("%s/%s => %s %v", c.op, c.ref, d, ok)
		}
	}
}

func TestWalletBalanceChangedEvent(t *testing.T) {
	w := newWallet(t, "100")
	mv, _ := w.Debit(brl(t, "25"), now)
	evt, err := NewWalletBalanceChanged(EventMeta{EventID: uuid.New(), CorrelationID: "c", OccurredAt: now}, w.ID(), uuid.New(), mv)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(evt)
	for _, want := range []string{`"eventType":"WalletBalanceChanged"`, `"version":1`, `"balanceBefore":{"amount":"100.00"`, `"balanceAfter":{"amount":"75.00"`, `"walletVersion":2`, `"direction":"DEBIT"`, `"occurredAt":"2026-09-08T12:00:00Z"`} {
		if !strings.Contains(string(raw), want) {
			t.Errorf("faltou %s em %s", want, raw)
		}
	}
	if _, err := NewWalletBalanceChanged(EventMeta{}, w.ID(), uuid.New(), mv); err == nil {
		t.Error("evento sem metadados")
	}
}
