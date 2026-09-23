package wallet_test

import (
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Pantani/backend-challenge-go/internal/domain/money"
	"github.com/Pantani/backend-challenge-go/internal/domain/wallet"
	"github.com/Pantani/backend-challenge-go/internal/testutil"
)

var now = time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)

func openParams(t *testing.T, initial string) wallet.OpenParams {
	t.Helper()
	return wallet.OpenParams{
		ID: uuid.New(), PlayerID: uuid.New(), InitialBalance: testutil.BRL(t, initial),
		OpeningTxID: uuid.New(), OpeningEntryID: uuid.New(), Now: now,
	}
}

func openWallet(t *testing.T, initial string) *wallet.Wallet {
	t.Helper()
	w, _, err := wallet.Open(openParams(t, initial))
	require.NoError(t, err)
	return w
}

func TestOpenWithPositiveBalanceCreatesOpeningCredit(t *testing.T) {
	t.Parallel()
	p := openParams(t, "100.00")
	w, entry, err := wallet.Open(p)
	require.NoError(t, err)

	assert.Equal(t, p.ID, w.ID())
	assert.Equal(t, p.PlayerID, w.PlayerID())
	assert.Equal(t, money.Currency("BRL"), w.Currency())
	assert.Equal(t, "100.00", w.Balance().Amount())
	assert.Equal(t, wallet.InitialVersion, w.Version())
	assert.Equal(t, now, w.CreatedAt())
	assert.Equal(t, now, w.UpdatedAt())

	require.NotNil(t, entry)
	assert.Equal(t, p.OpeningEntryID, entry.ID())
	assert.Equal(t, p.OpeningTxID, entry.TransactionID())
	assert.Equal(t, wallet.Credit, entry.Direction())
	assert.Equal(t, "0.00", entry.BalanceBefore().Amount())
	assert.Equal(t, "100.00", entry.BalanceAfter().Amount())
}

func TestOpenWithZeroBalanceHasNoEntry(t *testing.T) {
	t.Parallel()
	w, entry, err := wallet.Open(openParams(t, "0.00"))
	require.NoError(t, err)
	assert.Nil(t, entry)
	assert.True(t, w.Balance().IsZero())
	assert.Equal(t, wallet.InitialVersion, w.Version())
}

func TestOpenValidation(t *testing.T) {
	t.Parallel()
	neg, err := money.FromMinor(-1, "BRL")
	require.NoError(t, err)
	mutations := map[string]func(p *wallet.OpenParams){
		"nil id":        func(p *wallet.OpenParams) { p.ID = uuid.Nil },
		"nil player":    func(p *wallet.OpenParams) { p.PlayerID = uuid.Nil },
		"zero time":     func(p *wallet.OpenParams) { p.Now = time.Time{} },
		"uninitialized": func(p *wallet.OpenParams) { p.InitialBalance = money.Money{} },
		"negative":      func(p *wallet.OpenParams) { p.InitialBalance = neg },
	}
	for name, mutate := range mutations {
		p := openParams(t, "1.00")
		mutate(&p)
		_, _, err := wallet.Open(p)
		assert.ErrorIs(t, err, wallet.ErrInvalidWallet, name)
	}

	p := openParams(t, "1.00")
	p.OpeningTxID = uuid.Nil
	_, _, err = wallet.Open(p)
	assert.ErrorIs(t, err, wallet.ErrInvalidLedgerEntry)
}

func TestRehydrate(t *testing.T) {
	t.Parallel()
	s := wallet.Snapshot{ID: uuid.New(), PlayerID: uuid.New(), Balance: testutil.BRL(t, "5.00"), Version: 7, CreatedAt: now, UpdatedAt: now}
	w, err := wallet.Rehydrate(s)
	require.NoError(t, err)
	assert.Equal(t, int64(7), w.Version())
	assert.Equal(t, "5.00", w.Balance().Amount())

	neg, err := money.FromMinor(-1, "BRL")
	require.NoError(t, err)
	bad := map[string]func(s *wallet.Snapshot){
		"nil id":                 func(s *wallet.Snapshot) { s.ID = uuid.Nil },
		"nil player":             func(s *wallet.Snapshot) { s.PlayerID = uuid.Nil },
		"version below initial":  func(s *wallet.Snapshot) { s.Version = wallet.InitialVersion - 1 },
		"zero created":           func(s *wallet.Snapshot) { s.CreatedAt = time.Time{} },
		"zero updated":           func(s *wallet.Snapshot) { s.UpdatedAt = time.Time{} },
		"updated before created": func(s *wallet.Snapshot) { s.UpdatedAt = now.Add(-time.Second) },
		"uninitialized balance":  func(s *wallet.Snapshot) { s.Balance = money.Money{} },
		"negative balance":       func(s *wallet.Snapshot) { s.Balance = neg },
	}
	for name, mutate := range bad {
		b := s
		mutate(&b)
		_, err := wallet.Rehydrate(b)
		assert.ErrorIs(t, err, wallet.ErrInvalidWallet, name)
	}
}

func TestRehydrateNormalizesToUTC(t *testing.T) {
	t.Parallel()
	local := now.In(time.FixedZone("BRT", -3*60*60))
	w, err := wallet.Rehydrate(wallet.Snapshot{ID: uuid.New(), PlayerID: uuid.New(), Balance: testutil.BRL(t, "1.00"),
		Version: wallet.InitialVersion, CreatedAt: local, UpdatedAt: local.Add(time.Minute)})
	require.NoError(t, err)
	assert.Equal(t, time.UTC, w.CreatedAt().Location())
	assert.Equal(t, time.UTC, w.UpdatedAt().Location())
	assert.True(t, w.CreatedAt().Equal(now))
}

func TestDebitToExactlyZero(t *testing.T) {
	t.Parallel()
	w := openWallet(t, "10.00")
	entry, err := w.Apply(wallet.Movement{EntryID: uuid.New(), TransactionID: uuid.New(), Direction: wallet.Debit, Amount: testutil.BRL(t, "10.00"), Now: now})
	require.NoError(t, err)
	assert.True(t, entry.BalanceAfter().IsZero())
	assert.True(t, w.Balance().IsZero())
	assert.Equal(t, wallet.InitialVersion+1, w.Version())
}

func TestDebitAndCredit(t *testing.T) {
	t.Parallel()
	w := openWallet(t, "100.00")
	later := now.Add(time.Minute)

	debit, err := w.Apply(wallet.Movement{EntryID: uuid.New(), TransactionID: uuid.New(), Direction: wallet.Debit, Amount: testutil.BRL(t, "80.00"), Now: later})
	require.NoError(t, err)
	assert.Equal(t, "100.00", debit.BalanceBefore().Amount())
	assert.Equal(t, "20.00", debit.BalanceAfter().Amount())
	assert.Equal(t, int64(2), w.Version())
	assert.Equal(t, later, w.UpdatedAt())
	assert.Equal(t, w.ID(), debit.WalletID())
	assert.Equal(t, later, debit.CreatedAt())
	assert.Equal(t, "80.00", debit.Amount().Amount())

	credit, err := w.Apply(wallet.Movement{EntryID: uuid.New(), TransactionID: uuid.New(), Direction: wallet.Credit, Amount: testutil.BRL(t, "5.50"), Now: later})
	require.NoError(t, err)
	assert.Equal(t, "25.50", credit.BalanceAfter().Amount())
	assert.Equal(t, int64(3), w.Version())
}

func TestDebitInsufficientFundsKeepsState(t *testing.T) {
	t.Parallel()
	w := openWallet(t, "100.00")
	_, err := w.Apply(wallet.Movement{EntryID: uuid.New(), TransactionID: uuid.New(), Direction: wallet.Debit, Amount: testutil.BRL(t, "100.01"), Now: now})
	require.ErrorIs(t, err, wallet.ErrInsufficientFunds)
	assert.Equal(t, "100.00", w.Balance().Amount())
	assert.Equal(t, wallet.InitialVersion, w.Version())
	assert.True(t, w.CanDebit(testutil.BRL(t, "100.00")))
	assert.False(t, w.CanDebit(testutil.BRL(t, "100.01")))
}

func TestApplyValidation(t *testing.T) {
	t.Parallel()
	w := openWallet(t, "10.00")
	usd, err := money.Parse("1.00", "USD")
	require.NoError(t, err)
	cases := []struct {
		m    wallet.Movement
		want error
	}{
		{wallet.Movement{EntryID: uuid.New(), TransactionID: uuid.New(), Direction: wallet.Credit, Amount: testutil.BRL(t, "0.00"), Now: now}, wallet.ErrInvalidMovement},
		{wallet.Movement{EntryID: uuid.New(), TransactionID: uuid.New(), Direction: wallet.Credit, Now: now}, wallet.ErrInvalidMovement},
		{wallet.Movement{EntryID: uuid.New(), TransactionID: uuid.New(), Direction: wallet.Credit, Amount: usd, Now: now}, wallet.ErrCurrencyMismatch},
		{wallet.Movement{EntryID: uuid.Nil, TransactionID: uuid.New(), Direction: wallet.Credit, Amount: testutil.BRL(t, "1.00"), Now: now}, wallet.ErrInvalidLedgerEntry},
		{wallet.Movement{EntryID: uuid.New(), TransactionID: uuid.New(), Direction: wallet.Credit, Amount: testutil.BRL(t, "1.00")}, wallet.ErrInvalidLedgerEntry},
		{wallet.Movement{EntryID: uuid.New(), TransactionID: uuid.New(), Direction: "SIDEWAYS", Amount: testutil.BRL(t, "1.00"), Now: now}, wallet.ErrInvalidLedgerEntry},
	}
	for i, tc := range cases {
		_, err := w.Apply(tc.m)
		assert.ErrorIs(t, err, tc.want, "case %d", i)
	}
	assert.Equal(t, "10.00", w.Balance().Amount())
	assert.Equal(t, wallet.InitialVersion, w.Version())
}

func TestCreditOverflow(t *testing.T) {
	t.Parallel()
	maxM, err := money.FromMinor(math.MaxInt64, "BRL")
	require.NoError(t, err)
	w, err := wallet.Rehydrate(wallet.Snapshot{ID: uuid.New(), PlayerID: uuid.New(), Balance: maxM, Version: wallet.InitialVersion, CreatedAt: now, UpdatedAt: now})
	require.NoError(t, err)
	_, err = w.Apply(wallet.Movement{EntryID: uuid.New(), TransactionID: uuid.New(), Direction: wallet.Credit, Amount: testutil.BRL(t, "0.01"), Now: now})
	require.ErrorIs(t, err, money.ErrOverflow)
	assert.Equal(t, maxM, w.Balance())
	assert.Equal(t, wallet.InitialVersion, w.Version())
}
