package event_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Pantani/backend-challenge-go/internal/domain/event"
	"github.com/Pantani/backend-challenge-go/internal/domain/wager"
	"github.com/Pantani/backend-challenge-go/internal/domain/wallet"
	"github.com/Pantani/backend-challenge-go/internal/testutil"
)

var (
	now      = time.Date(2026, 9, 8, 12, 0, 0, 123456789, time.FixedZone("BRT", -3*3600))
	eventID  = uuid.MustParse("00000000-0000-0000-0000-00000000000e")
	txID     = uuid.MustParse("00000000-0000-0000-0000-000000000001")
	walletID = uuid.MustParse("00000000-0000-0000-0000-000000000002")
	playerID = uuid.MustParse("00000000-0000-0000-0000-000000000003")
	refID    = uuid.MustParse("00000000-0000-0000-0000-000000000004")
	entryID  = uuid.MustParse("00000000-0000-0000-0000-000000000005")
)

func externalTx(t *testing.T, kind wager.Kind, amount, ref string) *wager.Transaction {
	t.Helper()
	tx, err := wager.NewExternal(wager.ExternalParams{
		ID: txID, WalletID: walletID, PlayerID: playerID, Kind: kind, Amount: testutil.BRL(t, amount),
		CorrelationID: "corr", Now: now,
		External: wager.External{ProviderID: "provider-a", ExternalID: "t-1", IdempotencyKey: "k", PayloadHash: "h",
			RoundID: "r-1", GameID: "g-1", ReferenceExternalID: ref},
	})
	require.NoError(t, err)
	return tx
}

func decode(t *testing.T, r event.Record) map[string]any {
	t.Helper()
	var out map[string]any
	require.NoError(t, json.Unmarshal(r.Payload, &out))
	return out
}

// data returns the "data" object of a decoded envelope.
func data(t *testing.T, doc map[string]any) map[string]any {
	t.Helper()
	d, ok := doc["data"].(map[string]any)
	require.True(t, ok, "data must be an object")
	return d
}

func meta() event.Meta {
	return event.Meta{EventID: eventID, CorrelationID: "corr", CausationID: "msg-1", OccurredAt: now}
}

const envelopeHead = `"eventId":"00000000-0000-0000-0000-00000000000e","correlationId":"corr","causationId":"msg-1",` +
	`"occurredAt":"2026-09-08T15:00:00.123Z","version":1`

const txData = `"transactionId":"00000000-0000-0000-0000-000000000001","origin":"EXTERNAL","walletId":"00000000-0000-0000-0000-000000000002",` +
	`"playerId":"00000000-0000-0000-0000-000000000003","providerId":"provider-a","externalTransactionId":"t-1","roundId":"r-1","gameId":"g-1"`

func TestProcessedEnvelope(t *testing.T) {
	t.Parallel()
	tx := externalTx(t, wager.KindRefund, "25.00", "t-0")
	require.NoError(t, tx.Process(testutil.BRL(t, "975.00"), refID, now))
	m := meta()

	r := event.NewWagerTransactionProcessed(m, tx)
	assert.Equal(t, m.EventID, r.EventID)
	assert.Equal(t, event.TypeWagerTransactionProcessed, r.EventType)
	assert.Equal(t, event.AggregateTransaction, r.AggregateType)
	assert.Equal(t, tx.ID(), r.AggregateID)
	assert.Equal(t, tx.WalletID().String(), r.PartitionKey)
	assert.Equal(t, now.UTC(), r.OccurredAt)

	want := `{` + envelopeHead + `,"eventType":"WagerTransactionProcessed","aggregateType":"WagerTransaction",` +
		`"aggregateId":"00000000-0000-0000-0000-000000000001","data":{` + txData + `,"kind":"REFUND","status":"PROCESSED",` +
		`"money":{"amount":"25.00","currency":"BRL"},"referenceExternalTransactionId":"t-0",` +
		`"referenceTransactionId":"00000000-0000-0000-0000-000000000004","balance":{"amount":"975.00","currency":"BRL"}}}`
	assert.JSONEq(t, want, string(r.Payload))
}

func TestOpeningProcessedOmitsExternalFields(t *testing.T) {
	t.Parallel()
	tx, err := wager.NewOpening(wager.OpeningParams{ID: txID, WalletID: walletID, PlayerID: playerID, Amount: testutil.BRL(t, "10.00"), Now: now})
	require.NoError(t, err)
	require.NoError(t, tx.Process(testutil.BRL(t, "10.00"), uuid.Nil, now))
	m := meta()
	m.CausationID = ""

	doc := decode(t, event.NewWagerTransactionProcessed(m, tx))
	assert.NotContains(t, doc, "causationId")
	d := data(t, doc)
	for _, k := range []string{"providerId", "externalTransactionId", "roundId", "gameId", "referenceTransactionId", "referenceExternalTransactionId"} {
		assert.NotContains(t, d, k)
	}
	assert.Equal(t, "OPENING", d["kind"])
	assert.Equal(t, "INTERNAL", d["origin"])
}

func TestRejectedEnvelope(t *testing.T) {
	t.Parallel()
	rejected := externalTx(t, wager.KindBet, "80.00", "")
	require.NoError(t, rejected.Reject(wager.CodeInsufficientFunds, testutil.BRL(t, "20.00"), now))
	r := event.NewWagerTransactionRejected(meta(), rejected)

	want := `{` + envelopeHead + `,"eventType":"WagerTransactionRejected","aggregateType":"WagerTransaction",` +
		`"aggregateId":"00000000-0000-0000-0000-000000000001","data":{` + txData + `,"kind":"BET","status":"REJECTED",` +
		`"money":{"amount":"80.00","currency":"BRL"},"failureCode":"INSUFFICIENT_FUNDS"}}`
	assert.JSONEq(t, want, string(r.Payload))

	d := data(t, decode(t, r))
	assert.Equal(t, "REJECTED", d["status"])
	assert.Equal(t, map[string]any{"amount": "80.00", "currency": "BRL"}, d["money"])
	assert.NotContains(t, d, "balance", "the observed balance is not published")
}

func TestFailedEnvelope(t *testing.T) {
	t.Parallel()
	failed := externalTx(t, wager.KindRefund, "80.00", "t-0")
	require.NoError(t, failed.Fail(wager.CodeInternalFailure, now))
	r := event.NewWagerTransactionFailed(meta(), failed)
	assert.Equal(t, event.TypeWagerTransactionFailed, r.EventType)
	assert.Equal(t, event.AggregateTransaction, r.AggregateType)
	assert.Equal(t, failed.WalletID().String(), r.PartitionKey)

	want := `{` + envelopeHead + `,"eventType":"WagerTransactionFailed","aggregateType":"WagerTransaction",` +
		`"aggregateId":"00000000-0000-0000-0000-000000000001","data":{` + txData + `,"kind":"REFUND","status":"FAILED",` +
		`"money":{"amount":"80.00","currency":"BRL"},"referenceExternalTransactionId":"t-0","failureCode":"INTERNAL_FAILURE"}}`
	assert.JSONEq(t, want, string(r.Payload))
	assert.NotContains(t, data(t, decode(t, r)), "balance", "a failed operation observed no balance")
}

func TestPendingReferenceEnvelope(t *testing.T) {
	t.Parallel()
	pending := externalTx(t, wager.KindRollback, "80.00", "t-0")
	require.NoError(t, pending.AwaitReference(now.Add(time.Second), now))
	r := event.NewWagerTransactionPendingReference(meta(), pending)

	want := `{` + envelopeHead + `,"eventType":"WagerTransactionPendingReference","aggregateType":"WagerTransaction",` +
		`"aggregateId":"00000000-0000-0000-0000-000000000001","data":{` + txData + `,"kind":"ROLLBACK","status":"PENDING_REFERENCE",` +
		`"money":{"amount":"80.00","currency":"BRL"},"referenceExternalTransactionId":"t-0","nextAttemptAt":"2026-09-08T15:00:01.123Z"}}`
	assert.JSONEq(t, want, string(r.Payload))
}

func TestWalletBalanceChanged(t *testing.T) {
	t.Parallel()
	entry, err := wallet.NewLedgerEntry(wallet.LedgerEntryParams{
		ID: entryID, WalletID: walletID, TransactionID: txID, Direction: wallet.Debit,
		Amount: testutil.BRL(t, "25.00"), BalanceBefore: testutil.BRL(t, "1000.00"), BalanceAfter: testutil.BRL(t, "975.00"), CreatedAt: now,
	})
	require.NoError(t, err)
	r := event.NewWalletBalanceChanged(meta(), entry, 2)
	assert.Equal(t, eventID, r.EventID)
	assert.Equal(t, event.TypeWalletBalanceChanged, r.EventType)
	assert.Equal(t, event.AggregateWallet, r.AggregateType)
	assert.Equal(t, walletID, r.AggregateID)
	assert.Equal(t, walletID.String(), r.PartitionKey)

	want := `{` + envelopeHead + `,"eventType":"WalletBalanceChanged","aggregateType":"Wallet",` +
		`"aggregateId":"00000000-0000-0000-0000-000000000002","data":{"walletId":"00000000-0000-0000-0000-000000000002",` +
		`"transactionId":"00000000-0000-0000-0000-000000000001","direction":"DEBIT","money":{"amount":"25.00","currency":"BRL"},` +
		`"balanceBefore":{"amount":"1000.00","currency":"BRL"},"balanceAfter":{"amount":"975.00","currency":"BRL"},"walletVersion":2}}`
	assert.JSONEq(t, want, string(r.Payload))
}
