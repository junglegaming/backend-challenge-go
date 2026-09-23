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

func entryParams(t *testing.T, dir wallet.Direction, amount, before, after string) wallet.LedgerEntryParams {
	t.Helper()
	return wallet.LedgerEntryParams{
		ID: uuid.New(), WalletID: uuid.New(), TransactionID: uuid.New(), Direction: dir,
		Amount: testutil.BRL(t, amount), BalanceBefore: testutil.BRL(t, before), BalanceAfter: testutil.BRL(t, after), CreatedAt: now,
	}
}

func TestLedgerEntryValid(t *testing.T) {
	t.Parallel()
	for _, p := range []wallet.LedgerEntryParams{
		entryParams(t, wallet.Credit, "10.00", "5.00", "15.00"),
		entryParams(t, wallet.Debit, "10.00", "15.00", "5.00"),
		entryParams(t, wallet.Debit, "10.00", "10.00", "0.00"),
	} {
		e, err := wallet.NewLedgerEntry(p)
		require.NoError(t, err)
		assert.Equal(t, p.ID, e.ID())
		assert.Equal(t, p.Direction, e.Direction())
	}
}

func TestLedgerEntryInvalid(t *testing.T) {
	t.Parallel()
	neg, err := money.FromMinor(-100, "BRL")
	require.NoError(t, err)
	maxM, err := money.FromMinor(math.MaxInt64, "BRL")
	require.NoError(t, err)

	mutations := map[string]func(p *wallet.LedgerEntryParams){
		"nil id":          func(p *wallet.LedgerEntryParams) { p.ID = uuid.Nil },
		"nil wallet":      func(p *wallet.LedgerEntryParams) { p.WalletID = uuid.Nil },
		"nil tx":          func(p *wallet.LedgerEntryParams) { p.TransactionID = uuid.Nil },
		"bad direction":   func(p *wallet.LedgerEntryParams) { p.Direction = "SIDEWAYS" },
		"zero time":       func(p *wallet.LedgerEntryParams) { p.CreatedAt = time.Time{} },
		"zero amount":     func(p *wallet.LedgerEntryParams) { p.Amount = testutil.BRL(t, "0.00") },
		"negative before": func(p *wallet.LedgerEntryParams) { p.BalanceBefore = neg },
		"negative after":  func(p *wallet.LedgerEntryParams) { p.BalanceAfter = neg },
		"wrong after":     func(p *wallet.LedgerEntryParams) { p.BalanceAfter = testutil.BRL(t, "16.00") },
		"wrong direction": func(p *wallet.LedgerEntryParams) { p.Direction = wallet.Debit },
		"overflowing sum": func(p *wallet.LedgerEntryParams) { p.BalanceBefore = maxM },
		"uninit after":    func(p *wallet.LedgerEntryParams) { p.BalanceAfter = money.Money{} },
	}
	for name, mutate := range mutations {
		p := entryParams(t, wallet.Credit, "10.00", "5.00", "15.00")
		mutate(&p)
		_, err := wallet.NewLedgerEntry(p)
		assert.ErrorIs(t, err, wallet.ErrInvalidLedgerEntry, name)
	}
}

func TestDirection(t *testing.T) {
	t.Parallel()
	assert.True(t, wallet.Debit.Valid())
	assert.True(t, wallet.Credit.Valid())
	assert.False(t, wallet.Direction("X").Valid())
}
