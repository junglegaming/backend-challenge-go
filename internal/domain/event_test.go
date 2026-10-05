package domain

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
)

// debitedBet builds a BET that was processed against a wallet it really debited,
// so the ledger entry and the wallet version are real.
func debitedBet(t *testing.T) (*WagerTransaction, *WalletLedgerEntry, *Wallet) {
	t.Helper()
	wallet, err := NewWallet(uuid.New(), "BRL", mustMoney(t, 10000, "BRL"))
	if err != nil {
		t.Fatalf("NewWallet: %v", err)
	}
	cmd := validCommand(t, WagerKindBet, 2500)
	cmd.WalletID, cmd.PlayerID = wallet.ID(), wallet.PlayerID()
	bet, err := NewWagerTransaction(cmd)
	if err != nil {
		t.Fatalf("NewWagerTransaction: %v", err)
	}
	entry, err := wallet.Debit(bet.ID(), bet.Amount())
	if err != nil {
		t.Fatalf("Debit: %v", err)
	}
	if err := bet.MarkProcessed(wallet.Balance()); err != nil {
		t.Fatalf("MarkProcessed: %v", err)
	}
	return bet, entry, wallet
}

func TestEventsFor_ABetProducesProcessedThenBalanceChanged(t *testing.T) {
	bet, entry, wallet := debitedBet(t)

	events := EventsFor(bet, entry, wallet.Version(), "corr-1")
	if len(events) != 2 {
		t.Fatalf("got %d events, want 2", len(events))
	}
	processed, changed := events[0], events[1]

	if processed.Type != EventWagerTransactionProcessed || processed.AggregateID != bet.ID() {
		t.Fatalf("first event = %s on %s, want %s on the transaction", processed.Type, processed.AggregateID, EventWagerTransactionProcessed)
	}
	if changed.Type != EventWalletBalanceChanged || changed.AggregateID != wallet.ID() {
		t.Fatalf("second event = %s on %s, want %s on the wallet", changed.Type, changed.AggregateID, EventWalletBalanceChanged)
	}
	for _, e := range events {
		if e.Version != EventVersion || e.CorrelationID != "corr-1" || e.ID == uuid.Nil || e.OccurredAt.IsZero() {
			t.Fatalf("incomplete envelope: %+v", e)
		}
	}
	if processed.ID == changed.ID {
		t.Fatal("two events share an id")
	}
	if changed.CausationID != processed.ID.String() {
		t.Fatalf("causationId = %q, want the processed event %q", changed.CausationID, processed.ID)
	}

	data := changed.Data.(WalletBalanceChangedData)
	if data.Direction != DirectionDebit || data.Money.AmountMinor() != 2500 ||
		data.BalanceBefore.AmountMinor() != 10000 || data.BalanceAfter.AmountMinor() != 7500 ||
		data.WalletVersion != 2 || data.TransactionID != bet.ID() || data.WalletID != wallet.ID() {
		t.Fatalf("WalletBalanceChanged data = %+v", data)
	}
}

func TestEventsFor_ALossProducesOnlyProcessed(t *testing.T) {
	cmd := validCommand(t, WagerKindLoss, 0)
	loss, err := NewWagerTransaction(cmd)
	if err != nil {
		t.Fatalf("NewWagerTransaction: %v", err)
	}
	if err := loss.MarkProcessed(mustMoney(t, 10000, "BRL")); err != nil {
		t.Fatalf("MarkProcessed: %v", err)
	}

	events := EventsFor(loss, nil, 1, "corr")
	if len(events) != 1 || events[0].Type != EventWagerTransactionProcessed {
		t.Fatalf("events = %+v, want one WagerTransactionProcessed and no balance change", events)
	}
}

func TestEventsFor_ARejectionCarriesTheFailureCode(t *testing.T) {
	bet, err := NewWagerTransaction(validCommand(t, WagerKindBet, 2500))
	if err != nil {
		t.Fatalf("NewWagerTransaction: %v", err)
	}
	if err := bet.MarkRejected(FailureInsufficientFunds); err != nil {
		t.Fatalf("MarkRejected: %v", err)
	}

	events := EventsFor(bet, nil, 1, "corr")
	if len(events) != 1 || events[0].Type != EventWagerTransactionRejected {
		t.Fatalf("events = %+v, want one WagerTransactionRejected", events)
	}
	if got := events[0].Data.(WagerTransactionRejectedData).FailureCode; got != FailureInsufficientFunds {
		t.Fatalf("failureCode = %q, want %q", got, FailureInsufficientFunds)
	}
}

func TestEventsFor_AnUndecidedOperationProducesNothing(t *testing.T) {
	pending, err := NewWagerTransaction(validCommand(t, WagerKindBet, 2500))
	if err != nil {
		t.Fatalf("NewWagerTransaction: %v", err)
	}
	if events := EventsFor(pending, nil, 1, "corr"); len(events) != 0 {
		t.Fatalf("a PENDING operation produced %d events", len(events))
	}
}

func TestEventsFor_AMissingCorrelationIdFallsBackToTheTransactionId(t *testing.T) {
	bet, entry, wallet := debitedBet(t)
	for _, e := range EventsFor(bet, entry, wallet.Version(), "") {
		if e.CorrelationID != bet.ID().String() {
			t.Fatalf("correlationId = %q, want the transaction id %q", e.CorrelationID, bet.ID())
		}
	}
}

// The envelope is a contract: field names, an RFC 3339 UTC timestamp, and money
// as strings, never numbers.
func TestEvent_JSONShapeIsTheContract(t *testing.T) {
	bet, entry, wallet := debitedBet(t)
	events := EventsFor(bet, entry, wallet.Version(), "corr-json")

	raw, err := json.Marshal(events[1])
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	for _, key := range []string{"eventId", "eventType", "aggregateId", "correlationId", "causationId", "occurredAt", "version", "data"} {
		if _, ok := got[key]; !ok {
			t.Fatalf("envelope has no %q: %s", key, raw)
		}
	}
	if got["eventType"] != "WalletBalanceChanged" || got["version"] != float64(1) {
		t.Fatalf("eventType/version = %v/%v", got["eventType"], got["version"])
	}

	occurred, err := time.Parse(time.RFC3339Nano, got["occurredAt"].(string))
	if err != nil || occurred.Location() != time.UTC {
		t.Fatalf("occurredAt %q is not RFC 3339 in UTC (%v)", got["occurredAt"], err)
	}

	data := got["data"].(map[string]any)
	for _, key := range []string{"walletId", "transactionId", "direction", "money", "balanceBefore", "balanceAfter", "walletVersion"} {
		if _, ok := data[key]; !ok {
			t.Fatalf("data has no %q: %s", key, raw)
		}
	}
	money := data["money"].(map[string]any)
	if money["amount"] != "25.00" || money["currency"] != "BRL" {
		t.Fatalf("money = %v, want the decimal string 25.00 in BRL", money)
	}
}

func TestEventsFor_AnOpeningHasNoProviderFields(t *testing.T) {
	open, err := OpenWallet(uuid.New(), "BRL", mustMoney(t, 100000, "BRL"))
	if err != nil {
		t.Fatalf("OpenWallet: %v", err)
	}

	events := EventsFor(open.Operation, open.Entry, open.Wallet.Version(), "corr-open")
	if len(events) != 2 {
		t.Fatalf("got %d events, want 2", len(events))
	}

	raw, err := json.Marshal(events[0])
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got struct {
		Data map[string]any `json:"data"`
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, key := range []string{"providerId", "externalTransactionId"} {
		if _, ok := got.Data[key]; ok {
			t.Fatalf("an OPENING event carries %q, which does not apply to an internal origin: %s", key, raw)
		}
	}
	if got.Data["kind"] != "OPENING" {
		t.Fatalf("kind = %v, want OPENING", got.Data["kind"])
	}
}
