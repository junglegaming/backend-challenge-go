package postgres

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"

	"github.com/VictorM0nteiro/backend-challenge-go/internal/domain"
	"github.com/VictorM0nteiro/backend-challenge-go/internal/logctx"
)

type outboxRow struct {
	ID          uuid.UUID
	AggregateID uuid.UUID
	Type        string
	Envelope    map[string]any
}

// outboxRows returns every outbox row in the order the events happened.
func outboxRows(t *testing.T, pool *Pool) []outboxRow {
	t.Helper()
	rows, err := pool.Query(context.Background(),
		`SELECT id, aggregate_id, event_type, payload FROM outbox ORDER BY seq`)
	if err != nil {
		t.Fatalf("query outbox: %v", err)
	}
	defer rows.Close()

	var out []outboxRow
	for rows.Next() {
		var r outboxRow
		var payload []byte
		if err := rows.Scan(&r.ID, &r.AggregateID, &r.Type, &payload); err != nil {
			t.Fatalf("scan outbox: %v", err)
		}
		if err := json.Unmarshal(payload, &r.Envelope); err != nil {
			t.Fatalf("outbox payload is not JSON: %v", err)
		}
		out = append(out, r)
	}
	return out
}

// withCorrelation returns a context that carries a correlation id, as an HTTP
// request or an SQS message does.
func withCorrelation(id string) context.Context {
	ctx := logctx.NewContext(context.Background())
	logctx.SetCorrelationID(ctx, id)
	return ctx
}

func TestProcess_ABetWritesItsTwoEventsInTheSameCommit(t *testing.T) {
	pool := newTestPool(t)
	walletID := createTestWallet(t, NewWalletRepository(pool), 10000)
	req := wagerReq(t, walletID, ownerOf(t, pool, walletID), domain.WagerKindBet, "bet-1", "", 2500, "k-1")

	out, err := NewWagerProcessor(pool).Process(withCorrelation("corr-evt"), req)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}

	rows := outboxRows(t, pool)
	if len(rows) != 2 {
		t.Fatalf("outbox rows = %d, want 2", len(rows))
	}
	processed, changed := rows[0], rows[1]

	if processed.Type != "WagerTransactionProcessed" || processed.AggregateID != out.TransactionID {
		t.Fatalf("first row = %s on %s, want WagerTransactionProcessed on the operation", processed.Type, processed.AggregateID)
	}
	if changed.Type != "WalletBalanceChanged" || changed.AggregateID != walletID {
		t.Fatalf("second row = %s on %s, want WalletBalanceChanged on the wallet", changed.Type, changed.AggregateID)
	}

	for _, r := range rows {
		// The row id is the eventId, so every republication keeps it.
		if r.Envelope["eventId"] != r.ID.String() {
			t.Fatalf("eventId = %v, row id = %s: they must be the same", r.Envelope["eventId"], r.ID)
		}
		if r.Envelope["correlationId"] != "corr-evt" || r.Envelope["version"] != float64(1) {
			t.Fatalf("envelope = %v", r.Envelope)
		}
	}
	if changed.Envelope["causationId"] != processed.ID.String() {
		t.Fatalf("causationId = %v, want the processed event %s", changed.Envelope["causationId"], processed.ID)
	}

	data := changed.Envelope["data"].(map[string]any)
	if data["direction"] != "DEBIT" || data["walletVersion"] != float64(2) || data["transactionId"] != out.TransactionID.String() {
		t.Fatalf("WalletBalanceChanged data = %v", data)
	}
	if after := data["balanceAfter"].(map[string]any); after["amount"] != "75.00" {
		t.Fatalf("balanceAfter = %v, want 75.00", after)
	}
}

func TestProcess_ARejectionWritesOneRejectedEventAndNoBalanceChange(t *testing.T) {
	pool := newTestPool(t)
	walletID := createTestWallet(t, NewWalletRepository(pool), 1000)
	req := wagerReq(t, walletID, ownerOf(t, pool, walletID), domain.WagerKindBet, "bet-1", "", 2500, "k-1")

	if _, err := NewWagerProcessor(pool).Process(withCorrelation("corr-rej"), req); err != nil {
		t.Fatalf("Process: %v", err)
	}

	rows := outboxRows(t, pool)
	if len(rows) != 1 || rows[0].Type != "WagerTransactionRejected" {
		t.Fatalf("rows = %+v, want exactly one WagerTransactionRejected", rows)
	}
	if code := rows[0].Envelope["data"].(map[string]any)["failureCode"]; code != "insufficient_funds" {
		t.Fatalf("failureCode = %v, want insufficient_funds", code)
	}
}

func TestProcess_ALossWritesOnlyTheProcessedEvent(t *testing.T) {
	pool := newTestPool(t)
	walletID := createTestWallet(t, NewWalletRepository(pool), 10000)
	req := wagerReq(t, walletID, ownerOf(t, pool, walletID), domain.WagerKindLoss, "loss-1", "", 0, "k-loss")

	if _, err := NewWagerProcessor(pool).Process(context.Background(), req); err != nil {
		t.Fatalf("Process: %v", err)
	}

	rows := outboxRows(t, pool)
	if len(rows) != 1 || rows[0].Type != "WagerTransactionProcessed" {
		t.Fatalf("rows = %+v, want one WagerTransactionProcessed and no WalletBalanceChanged", rows)
	}
}

func TestProcess_AReplayAddsNoEvents(t *testing.T) {
	pool := newTestPool(t)
	walletID := createTestWallet(t, NewWalletRepository(pool), 10000)
	req := wagerReq(t, walletID, ownerOf(t, pool, walletID), domain.WagerKindBet, "bet-1", "", 2500, "k-1")
	proc := NewWagerProcessor(pool)

	for i := 0; i < 3; i++ {
		if _, err := proc.Process(context.Background(), req); err != nil {
			t.Fatalf("Process #%d: %v", i, err)
		}
	}

	if got := len(outboxRows(t, pool)); got != 2 {
		t.Fatalf("outbox rows = %d, want 2: replays must not publish the event again", got)
	}
}

// An operation that fails after the wallet and the ledger were written rolls
// back everything, events included: no event may describe a change that did not
// commit.
func TestProcess_ARefusedOperationLeavesNoEvents(t *testing.T) {
	pool := newTestPool(t)
	walletID := createTestWallet(t, NewWalletRepository(pool), 10000)
	player := ownerOf(t, pool, walletID)
	proc := NewWagerProcessor(pool)

	if _, err := proc.Process(context.Background(), wagerReq(t, walletID, player, domain.WagerKindBet, "bet-1", "", 2500, "k-1")); err != nil {
		t.Fatalf("first: %v", err)
	}
	before := len(outboxRows(t, pool))

	// Same operation under another key: refused by the unique index, after the
	// wallet was already debited inside the transaction.
	_, err := proc.Process(context.Background(), wagerReq(t, walletID, player, domain.WagerKindBet, "bet-1", "", 2500, "k-2"))
	if err == nil {
		t.Fatal("the duplicate was not refused")
	}
	// A player that does not own the wallet is refused too.
	if _, err := proc.Process(context.Background(), wagerReq(t, walletID, uuid.New(), domain.WagerKindBet, "bet-2", "", 2500, "k-3")); err == nil {
		t.Fatal("the stranger was not refused")
	}

	if after := len(outboxRows(t, pool)); after != before {
		t.Fatalf("outbox rows went from %d to %d: a refused operation must leave no events", before, after)
	}
}

func TestOpenWallet_EventsUseTheSameEnvelope(t *testing.T) {
	pool := newTestPool(t)
	repo := NewWalletRepository(pool)

	open, err := domain.OpenWallet(uuid.New(), "BRL", mustMoney(t, 100000, "BRL"))
	if err != nil {
		t.Fatalf("OpenWallet: %v", err)
	}
	if err := repo.OpenWallet(withCorrelation("corr-open"), open); err != nil {
		t.Fatalf("repo.OpenWallet: %v", err)
	}

	rows := outboxRows(t, pool)
	if len(rows) != 2 {
		t.Fatalf("outbox rows = %d, want 2", len(rows))
	}
	for _, r := range rows {
		if r.Envelope["eventId"] != r.ID.String() || r.Envelope["correlationId"] != "corr-open" || r.Envelope["version"] != float64(1) {
			t.Fatalf("envelope = %v", r.Envelope)
		}
	}
	data := rows[0].Envelope["data"].(map[string]any)
	if _, has := data["providerId"]; has {
		t.Fatalf("an OPENING event carries providerId: %v", data)
	}
}
