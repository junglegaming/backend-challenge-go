package wager_test

import (
	"math"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Pantani/backend-challenge-go/internal/domain/money"
	"github.com/Pantani/backend-challenge-go/internal/domain/wager"
	"github.com/Pantani/backend-challenge-go/internal/domain/wallet"
	"github.com/Pantani/backend-challenge-go/internal/testutil"
)

func decide(f fixture, tx, ref *wager.Transaction, reversed bool) wager.Decision {
	return wager.Decide(wager.DecisionInput{Wallet: f.wallet, Transaction: tx, Reference: ref, ReferenceReversed: reversed})
}

func TestDecideSimpleKinds(t *testing.T) {
	t.Parallel()
	f := newFixture(t, "100.00")
	cases := []struct {
		kind   wager.Kind
		amount string
		want   wager.Decision
	}{
		{wager.KindBet, "80.00", wager.Decision{Action: wager.ActionProcess, Moves: true, Direction: wallet.Debit}},
		{wager.KindBet, "100.00", wager.Decision{Action: wager.ActionProcess, Moves: true, Direction: wallet.Debit}},
		{wager.KindBet, "100.01", wager.Decision{Action: wager.ActionReject, Code: wager.CodeInsufficientFunds}},
		{wager.KindWin, "50.00", wager.Decision{Action: wager.ActionProcess, Moves: true, Direction: wallet.Credit}},
		{wager.KindLoss, "0.00", wager.Decision{Action: wager.ActionProcess}},
	}
	for _, tc := range cases {
		got := decide(f, f.tx(t, tc.kind, tc.amount, ""), nil, false)
		assert.Equal(t, tc.want, got, "%s %s", tc.kind, tc.amount)
	}
}

func TestDecideOwnership(t *testing.T) {
	t.Parallel()
	f := newFixture(t, "100.00")

	p := f.params(t, wager.KindBet, "1.00", "")
	p.PlayerID = uuid.New()
	other, err := wager.NewExternal(p)
	require.NoError(t, err)
	assert.Equal(t, wager.CodePlayerWalletMismatch, decide(f, other, nil, false).Code)

	p = f.params(t, wager.KindBet, "1.00", "")
	p.Amount = testutil.Money(t, "1.00", "USD")
	usd, err := wager.NewExternal(p)
	require.NoError(t, err)
	assert.Equal(t, wager.CodeCurrencyMismatch, decide(f, usd, nil, false).Code)
}

func TestDecideAwaitsMissingOrPendingReference(t *testing.T) {
	t.Parallel()
	f := newFixture(t, "100.00")
	refund := f.tx(t, wager.KindRefund, "10.00", "bet-1")
	assert.Equal(t, wager.ActionAwait, decide(f, refund, nil, false).Action)

	pendingRef := f.tx(t, wager.KindRefund, "10.00", "bet-0")
	require.NoError(t, pendingRef.AwaitReference(now, now))
	assert.Equal(t, wager.ActionAwait, decide(f, refund, pendingRef, false).Action)

	stillPending := f.tx(t, wager.KindBet, "10.00", "")
	assert.Equal(t, wager.ActionAwait, decide(f, refund, stillPending, false).Action)
}

func TestDecideIsPure(t *testing.T) {
	t.Parallel()
	f := newFixture(t, "100.00")
	bet := f.processed(t, wager.KindBet, "10.00", "")
	balance, version := f.wallet.Balance(), f.wallet.Version()
	for _, tx := range []*wager.Transaction{f.tx(t, wager.KindBet, "50.00", ""), f.tx(t, wager.KindRefund, "10.00", "b"), f.tx(t, wager.KindBet, "500.00", "")} {
		decide(f, tx, bet, false)
		assert.Equal(t, wager.StatusPending, tx.Status())
	}
	assert.Equal(t, balance, f.wallet.Balance())
	assert.Equal(t, version, f.wallet.Version())
	assert.Equal(t, wager.StatusProcessed, bet.Status())
}

func TestDecideReversals(t *testing.T) {
	t.Parallel()
	f := newFixture(t, "100.00")
	bet := f.processed(t, wager.KindBet, "10.00", "")
	win := f.processed(t, wager.KindWin, "30.00", "")
	refund := f.processed(t, wager.KindRefund, "10.00", "x")
	rollback := f.processed(t, wager.KindRollback, "10.00", "x")
	loss := f.processed(t, wager.KindLoss, "0.00", "")

	cases := []struct {
		name string
		tx   *wager.Transaction
		ref  *wager.Transaction
		want wager.Decision
	}{
		{"refund bet", f.tx(t, wager.KindRefund, "10.00", "b"), bet, wager.Decision{Action: wager.ActionProcess, Moves: true, Direction: wallet.Credit}},
		{"rollback bet", f.tx(t, wager.KindRollback, "10.00", "b"), bet, wager.Decision{Action: wager.ActionProcess, Moves: true, Direction: wallet.Credit}},
		{"rollback win", f.tx(t, wager.KindRollback, "30.00", "w"), win, wager.Decision{Action: wager.ActionProcess, Moves: true, Direction: wallet.Debit}},
		{"rollback refund", f.tx(t, wager.KindRollback, "10.00", "r"), refund, wager.Decision{Action: wager.ActionProcess, Moves: true, Direction: wallet.Debit}},
		{"win refs bet", f.tx(t, wager.KindWin, "99.00", "b"), bet, wager.Decision{Action: wager.ActionProcess, Moves: true, Direction: wallet.Credit}},
		{"refund win", f.tx(t, wager.KindRefund, "30.00", "w"), win, wager.Decision{Action: wager.ActionReject, Code: wager.CodeReferenceKindInvalid}},
		{"win refs win", f.tx(t, wager.KindWin, "30.00", "w"), win, wager.Decision{Action: wager.ActionReject, Code: wager.CodeReferenceKindInvalid}},
		{"partial refund", f.tx(t, wager.KindRefund, "5.00", "b"), bet, wager.Decision{Action: wager.ActionReject, Code: wager.CodeReversalAmountMismatch}},
		{"rollback amount mismatch", f.tx(t, wager.KindRollback, "29.00", "w"), win, wager.Decision{Action: wager.ActionReject, Code: wager.CodeReversalAmountMismatch}},
		{"rollback rollback", f.tx(t, wager.KindRollback, "10.00", "x"), rollback, wager.Decision{Action: wager.ActionReject, Code: wager.CodeReferenceKindInvalid}},
		{"refund refund", f.tx(t, wager.KindRefund, "10.00", "r"), refund, wager.Decision{Action: wager.ActionReject, Code: wager.CodeReferenceKindInvalid}},
		{"rollback loss", f.tx(t, wager.KindRollback, "1.00", "l"), loss, wager.Decision{Action: wager.ActionReject, Code: wager.CodeReferenceKindInvalid}},
	}
	for _, tc := range cases {
		assert.Equal(t, tc.want, decide(f, tc.tx, tc.ref, false), tc.name)
	}
}

func TestDecideRollbackOfOpeningIsInvalid(t *testing.T) {
	t.Parallel()
	f := newFixture(t, "100.00")
	opening, err := wager.NewOpening(wager.OpeningParams{ID: uuid.New(), WalletID: f.walletID, PlayerID: f.playerID, Amount: testutil.Money(t, "10.00", "BRL"), Now: now})
	require.NoError(t, err)
	require.NoError(t, opening.Process(f.wallet.Balance(), uuid.Nil, now))
	got := decide(f, f.tx(t, wager.KindRollback, "10.00", "o"), opening, false)
	// OPENING has no provider or round, so the context never matches.
	assert.Equal(t, wager.CodeReferenceMismatch, got.Code)
}

func TestDecideAlreadyReversed(t *testing.T) {
	t.Parallel()
	f := newFixture(t, "100.00")
	bet := f.processed(t, wager.KindBet, "10.00", "")
	for _, kind := range []wager.Kind{wager.KindRefund, wager.KindRollback} {
		got := decide(f, f.tx(t, kind, "10.00", "b"), bet, true)
		assert.Equal(t, wager.CodeAlreadyReversed, got.Code, kind)
	}
	// A WIN referencing a reversed bet is not a reversal.
	assert.Equal(t, wager.ActionProcess, decide(f, f.tx(t, wager.KindWin, "1.00", "b"), bet, true).Action)
}

func TestDecideReferenceNotProcessed(t *testing.T) {
	t.Parallel()
	f := newFixture(t, "100.00")
	rejectedBet := f.tx(t, wager.KindBet, "10.00", "")
	require.NoError(t, rejectedBet.Reject(wager.CodeInsufficientFunds, f.wallet.Balance(), now))
	failedBet := f.tx(t, wager.KindBet, "10.00", "")
	require.NoError(t, failedBet.Fail(wager.CodeInternalFailure, now))
	for _, ref := range []*wager.Transaction{rejectedBet, failedBet} {
		got := decide(f, f.tx(t, wager.KindRefund, "10.00", "b"), ref, false)
		assert.Equal(t, wager.CodeReferenceNotProcessed, got.Code)
	}
}

func TestDecideReferenceMismatch(t *testing.T) {
	t.Parallel()
	f := newFixture(t, "100.00")
	mutations := map[string]func(p *wager.ExternalParams){
		"provider": func(p *wager.ExternalParams) { p.External.ProviderID = "provider-b" },
		"round":    func(p *wager.ExternalParams) { p.External.RoundID = "round-2" },
		"player":   func(p *wager.ExternalParams) { p.PlayerID = uuid.New() },
		"wallet":   func(p *wager.ExternalParams) { p.WalletID = uuid.New() },
		"currency": func(p *wager.ExternalParams) { p.Amount = testutil.Money(t, "10.00", "USD") },
	}
	for name, mutate := range mutations {
		p := f.params(t, wager.KindBet, "10.00", "")
		mutate(&p)
		ref, err := wager.NewExternal(p)
		require.NoError(t, err)
		balance := testutil.Money(t, "90.00", string(p.Amount.Currency()))
		require.NoError(t, ref.Process(balance, uuid.Nil, now))
		got := decide(f, f.tx(t, wager.KindRefund, "10.00", "b"), ref, false)
		assert.Equal(t, wager.CodeReferenceMismatch, got.Code, name)
	}
}

func TestDecideReversalInsufficientFunds(t *testing.T) {
	t.Parallel()
	f := newFixture(t, "10.00")
	win := f.processed(t, wager.KindWin, "30.00", "")
	got := decide(f, f.tx(t, wager.KindRollback, "30.00", "w"), win, false)
	assert.Equal(t, wager.CodeReversalInsufficientFunds, got.Code)
	assert.NotEqual(t, wager.CodeInsufficientFunds, got.Code)
}

func TestDecideBalanceLimit(t *testing.T) {
	t.Parallel()
	maxM, err := money.FromMinor(math.MaxInt64, "BRL")
	require.NoError(t, err)
	w, err := wallet.Rehydrate(wallet.Snapshot{ID: uuid.New(), PlayerID: uuid.New(), Balance: maxM, Version: 1, CreatedAt: now, UpdatedAt: now})
	require.NoError(t, err)
	f := fixture{wallet: w, walletID: w.ID(), playerID: w.PlayerID()}
	got := decide(f, f.tx(t, wager.KindWin, "0.01", ""), nil, false)
	assert.Equal(t, wager.CodeBalanceLimitExceeded, got.Code)

	bet := f.processed(t, wager.KindBet, "0.01", "")
	got = decide(f, f.tx(t, wager.KindRefund, "0.01", "b"), bet, false)
	assert.Equal(t, wager.CodeBalanceLimitExceeded, got.Code, "reversal credit overflow")
}
