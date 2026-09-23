package wager_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Pantani/backend-challenge-go/internal/domain/money"
	"github.com/Pantani/backend-challenge-go/internal/domain/wager"
	"github.com/Pantani/backend-challenge-go/internal/testutil"
)

func TestUninitializedTransitions(t *testing.T) {
	balance := testutil.Money(t, "0.00", "BRL")
	transitions := map[string]func(*wager.Transaction) error{
		"process": func(tx *wager.Transaction) error { return tx.Process(balance, uuid.Nil, now) },
		"reject":  func(tx *wager.Transaction) error { return tx.Reject(wager.CodeInsufficientFunds, balance, now) },
		"fail":    func(tx *wager.Transaction) error { return tx.Fail(wager.CodeInternalFailure, now) },
		"await":   func(tx *wager.Transaction) error { return tx.AwaitReference(now.Add(time.Second), now) },
	}
	for name, transition := range transitions {
		t.Run(name, func(t *testing.T) {
			tx := &wager.Transaction{}
			require.Error(t, transition(tx))
			assert.Equal(t, wager.Status(""), tx.Status())
		})
	}
}

func validSnapshot(t *testing.T) wager.Snapshot {
	t.Helper()
	p := newFixture(t, "100.00").params(t, wager.KindBet, "1.00", "")
	return wager.Snapshot{ID: p.ID, WalletID: p.WalletID, PlayerID: p.PlayerID,
		Origin: wager.OriginExternal, Kind: p.Kind, Status: wager.StatusProcessed,
		Amount: p.Amount, External: p.External, ResultBalance: testutil.Money(t, "99.00", "BRL"), CreatedAt: now, UpdatedAt: now}
}

func TestSnapshotRejectsInvalidState(t *testing.T) {
	cases := map[string]func(*wager.Snapshot){
		"unknown origin":         func(s *wager.Snapshot) { s.Origin = "UNKNOWN" },
		"missing metadata":       func(s *wager.Snapshot) { s.External = wager.External{} },
		"zero bet":               func(s *wager.Snapshot) { s.Amount = testutil.Money(t, "0.00", "BRL") },
		"negative bet":           func(s *wager.Snapshot) { s.Amount = s.Amount.Neg() },
		"nonzero loss":           func(s *wager.Snapshot) { s.Kind = wager.KindLoss },
		"missing updated":        func(s *wager.Snapshot) { s.UpdatedAt = time.Time{} },
		"updated before created": func(s *wager.Snapshot) { s.UpdatedAt = now.Add(-time.Second) },
		"negative attempts":      func(s *wager.Snapshot) { s.Attempts = -1 },
		"missing balance":        func(s *wager.Snapshot) { s.ResultBalance = money.Money{} },
		"negative balance":       func(s *wager.Snapshot) { s.ResultBalance = s.ResultBalance.Neg() },
		"processed failure":      func(s *wager.Snapshot) { s.FailureCode = wager.CodeInternalFailure },
		"bet reference":          func(s *wager.Snapshot) { s.ReferenceTxID = uuid.New() },
		"rejection no code":      func(s *wager.Snapshot) { s.Status = wager.StatusRejected },
		"failure no code":        func(s *wager.Snapshot) { s.Status = wager.StatusFailed; s.ResultBalance = money.Money{} },
		"pending result":         func(s *wager.Snapshot) { s.Status = wager.StatusPending },
		"waiting without schedule": func(s *wager.Snapshot) {
			s.Status = wager.StatusPendingReference
			s.Kind = wager.KindRefund
			s.External.ReferenceExternalID = "bet"
			s.ResultBalance = money.Money{}
		},
		"opening metadata": func(s *wager.Snapshot) { s.Origin = wager.OriginInternal; s.Kind = wager.KindOpening },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			s := validSnapshot(t)
			mutate(&s)
			_, err := wager.Rehydrate(s)
			require.ErrorIs(t, err, wager.ErrInvalidTransaction)
		})
	}
}

func TestTransitionsRejectInvalidTime(t *testing.T) {
	for _, stamp := range []time.Time{{}, now.Add(-time.Second)} {
		tx := newFixture(t, "100.00").tx(t, wager.KindRefund, "1.00", "bet")
		balance := testutil.Money(t, "100.00", "BRL")
		assert.ErrorIs(t, tx.Process(balance, uuid.New(), stamp), wager.ErrInvalidTransaction)
		assert.ErrorIs(t, tx.Reject(wager.CodeReferenceNotFound, balance, stamp), wager.ErrInvalidTransaction)
		assert.ErrorIs(t, tx.Fail(wager.CodeInternalFailure, stamp), wager.ErrInvalidTransaction)
		assert.ErrorIs(t, tx.AwaitReference(now.Add(time.Second), stamp), wager.ErrInvalidTransaction)
		assert.Equal(t, wager.StatusPending, tx.Status())
	}
	tx := newFixture(t, "100.00").tx(t, wager.KindRefund, "1.00", "bet")
	assert.ErrorIs(t, tx.AwaitReference(time.Time{}, now), wager.ErrInvalidTransaction)
	assert.ErrorIs(t, tx.AwaitReference(now.Add(-time.Second), now), wager.ErrInvalidTransaction)
	assert.Equal(t, 0, tx.Attempts())
}

func TestSnapshotRoundTripStates(t *testing.T) {
	cases := map[string]func(*wager.Snapshot){
		"processed": func(*wager.Snapshot) {},
		"pending":   func(s *wager.Snapshot) { s.Status = wager.StatusPending; s.ResultBalance = money.Money{} },
		"failed": func(s *wager.Snapshot) {
			s.Status = wager.StatusFailed
			s.FailureCode = wager.CodeInternalFailure
			s.ResultBalance = money.Money{}
		},
		"rejected different currency": func(s *wager.Snapshot) {
			s.Status = wager.StatusRejected
			s.FailureCode = wager.CodeCurrencyMismatch
			s.ResultBalance = testutil.Money(t, "100.00", "USD")
		},
		"opening": func(s *wager.Snapshot) {
			s.Origin = wager.OriginInternal
			s.Kind = wager.KindOpening
			s.External = wager.External{}
		},
		"waiting": func(s *wager.Snapshot) {
			s.Kind = wager.KindRefund
			s.External.ReferenceExternalID = "bet"
			s.Status = wager.StatusPendingReference
			s.ResultBalance = money.Money{}
			s.Attempts = 1
			s.NextAttemptAt = now.Add(time.Second)
		},
		"processed after retries": func(s *wager.Snapshot) {
			s.Kind = wager.KindRefund
			s.External.ReferenceExternalID = "bet"
			s.ReferenceTxID = uuid.New()
			s.Attempts = 3
			s.NextAttemptAt = now.Add(-time.Second)
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			s := validSnapshot(t)
			mutate(&s)
			tx, err := wager.Rehydrate(s)
			require.NoError(t, err)
			assert.Equal(t, s.Status, tx.Status())
			assert.Equal(t, s.ResultBalance, tx.ResultBalance())
			assert.Equal(t, s.Attempts, tx.Attempts())
			assert.Equal(t, s.NextAttemptAt, tx.NextAttemptAt())
		})
	}
}

func TestProcessedCurrencyMustMatchAmount(t *testing.T) {
	balance := testutil.Money(t, "99.00", "USD")
	tx := newFixture(t, "100.00").tx(t, wager.KindBet, "1.00", "")
	assert.ErrorIs(t, tx.Process(balance, uuid.Nil, now), wager.ErrInvalidTransaction)
	assert.Equal(t, wager.StatusPending, tx.Status())
	assert.Equal(t, money.Money{}, tx.ResultBalance())
	snapshot := validSnapshot(t)
	snapshot.ResultBalance = balance
	_, err := wager.Rehydrate(snapshot)
	require.ErrorIs(t, err, wager.ErrInvalidTransaction)
	require.NoError(t, tx.Reject(wager.CodeCurrencyMismatch, balance, now))
	assert.Equal(t, balance, tx.ResultBalance())
}

func TestWinResolvedReferenceMatchesExternalReference(t *testing.T) {
	cases := []struct {
		name, external string
		resolved       uuid.UUID
	}{
		{name: "missing resolved reference", external: "bet"},
		{name: "unexpected resolved reference", resolved: uuid.New()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tx := newFixture(t, "100.00").tx(t, wager.KindWin, "1.00", tc.external)
			balance := testutil.Money(t, "101.00", "BRL")
			assert.ErrorIs(t, tx.Process(balance, tc.resolved, now), wager.ErrInvalidTransaction)
			assert.Equal(t, wager.StatusPending, tx.Status())
			s := validSnapshot(t)
			s.Kind = wager.KindWin
			s.External.ReferenceExternalID = tc.external
			s.ReferenceTxID = tc.resolved
			_, err := wager.Rehydrate(s)
			require.ErrorIs(t, err, wager.ErrInvalidTransaction)
		})
	}
}

func TestWaitingSnapshotRejectsRegressingSchedule(t *testing.T) {
	s := validSnapshot(t)
	s.Status = wager.StatusPendingReference
	s.Kind = wager.KindRefund
	s.External.ReferenceExternalID = "bet"
	s.ResultBalance = money.Money{}
	s.Attempts = 1
	s.NextAttemptAt = s.UpdatedAt.Add(-time.Second)
	_, err := wager.Rehydrate(s)
	require.ErrorIs(t, err, wager.ErrInvalidTransaction)
	s.NextAttemptAt = s.UpdatedAt
	_, err = wager.Rehydrate(s)
	require.NoError(t, err)
}
