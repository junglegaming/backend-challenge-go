package wager_test

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Pantani/backend-challenge-go/internal/domain/money"
	"github.com/Pantani/backend-challenge-go/internal/domain/wager"
	"github.com/Pantani/backend-challenge-go/internal/testutil"
)

func TestParseExternalKind(t *testing.T) {
	t.Parallel()
	for _, k := range []string{"BET", "WIN", "LOSS", "REFUND", "ROLLBACK"} {
		got, err := wager.ParseExternalKind(k)
		require.NoError(t, err)
		assert.Equal(t, wager.Kind(k), got)
	}
	for _, k := range []string{"OPENING", "bet", "", "DEPOSIT"} {
		_, err := wager.ParseExternalKind(k)
		assert.ErrorIs(t, err, wager.ErrInvalidKind, k)
	}
}

func TestKindPredicates(t *testing.T) {
	t.Parallel()
	type preds struct{ external, requiresRef, acceptsRef, reversal, zero bool }
	matrix := map[wager.Kind]preds{
		wager.KindOpening:  {},
		wager.KindBet:      {external: true},
		wager.KindWin:      {external: true, acceptsRef: true},
		wager.KindLoss:     {external: true, zero: true},
		wager.KindRefund:   {external: true, requiresRef: true, acceptsRef: true, reversal: true},
		wager.KindRollback: {external: true, requiresRef: true, acceptsRef: true, reversal: true},
	}
	for k, want := range matrix {
		got := preds{k.External(), k.RequiresReference(), k.AcceptsReference(), k.IsReversal(), k.RequiresZeroAmount()}
		assert.True(t, k.Valid(), k)
		assert.Equal(t, want, got, k)
	}
	unknown := wager.Kind("X")
	assert.False(t, unknown.Valid())
	assert.False(t, unknown.External())
	assert.False(t, unknown.AcceptsReference())
}

func TestStatusPredicates(t *testing.T) {
	t.Parallel()
	terminal := map[wager.Status]bool{
		wager.StatusPending: false, wager.StatusPendingReference: false,
		wager.StatusProcessed: true, wager.StatusRejected: true, wager.StatusFailed: true,
	}
	for s, want := range terminal {
		assert.True(t, s.Valid())
		assert.Equal(t, want, s.Terminal(), s)
	}
	assert.False(t, wager.Status("X").Valid())
}

func TestZeroValuePolicy(t *testing.T) {
	t.Parallel()
	f := newFixture(t, "100.00")
	cases := []struct {
		kind   wager.Kind
		amount string
		ref    string
		want   error
	}{
		{wager.KindBet, "1.00", "", nil},
		{wager.KindBet, "0.00", "", wager.ErrInvalidTransaction},
		{wager.KindWin, "1.00", "", nil},
		{wager.KindWin, "1.00", "bet-1", nil},
		{wager.KindWin, "0.00", "", wager.ErrInvalidTransaction},
		{wager.KindLoss, "0.00", "", nil},
		{wager.KindLoss, "0.00", "bet-1", wager.ErrInvalidTransaction},
		{wager.KindLoss, "1.00", "", wager.ErrInvalidTransaction},
		{wager.KindRefund, "1.00", "bet-1", nil},
		{wager.KindRefund, "0.00", "bet-1", wager.ErrInvalidTransaction},
		{wager.KindRollback, "1.00", "bet-1", nil},
		{wager.KindRollback, "0.00", "bet-1", wager.ErrInvalidTransaction},
	}
	for _, tc := range cases {
		_, err := wager.NewExternal(f.params(t, tc.kind, tc.amount, tc.ref))
		if tc.want == nil {
			assert.NoError(t, err, "%s %s %q", tc.kind, tc.amount, tc.ref)
			continue
		}
		assert.ErrorIs(t, err, tc.want, "%s %s %q", tc.kind, tc.amount, tc.ref)
	}
}

func TestNewExternal(t *testing.T) {
	t.Parallel()
	f := newFixture(t, "100.00")
	p := f.params(t, wager.KindRefund, "25.00", "bet-1")
	tx, err := wager.NewExternal(p)
	require.NoError(t, err)

	assert.Equal(t, p.ID, tx.ID())
	assert.Equal(t, wager.OriginExternal, tx.Origin())
	assert.Equal(t, wager.KindRefund, tx.Kind())
	assert.Equal(t, wager.StatusPending, tx.Status())
	assert.Equal(t, f.walletID, tx.WalletID())
	assert.Equal(t, f.playerID, tx.PlayerID())
	assert.Equal(t, "25.00", tx.Amount().Amount())
	assert.Equal(t, p.External, tx.External())
	assert.Equal(t, "corr-1", tx.CorrelationID())
	assert.Equal(t, now, tx.CreatedAt())
	assert.Equal(t, now, tx.UpdatedAt())
	assert.Equal(t, uuid.Nil, tx.ReferenceTxID())
	assert.Empty(t, tx.FailureCode())
	assert.Zero(t, tx.Attempts())
	assert.True(t, tx.NextAttemptAt().IsZero())
	assert.Error(t, tx.ResultBalance().Validate())
}

func TestNewExternalValidation(t *testing.T) {
	t.Parallel()
	f := newFixture(t, "100.00")
	long := strings.Repeat("x", 129)
	invalid := wager.ErrInvalidTransaction
	mutations := map[string]struct {
		mutate func(p *wager.ExternalParams)
		want   error
	}{
		"opening kind":        {func(p *wager.ExternalParams) { p.Kind = wager.KindOpening }, wager.ErrInvalidKind},
		"unknown kind":        {func(p *wager.ExternalParams) { p.Kind = "DEPOSIT" }, wager.ErrInvalidKind},
		"nil id":              {func(p *wager.ExternalParams) { p.ID = uuid.Nil }, invalid},
		"nil wallet":          {func(p *wager.ExternalParams) { p.WalletID = uuid.Nil }, invalid},
		"nil player":          {func(p *wager.ExternalParams) { p.PlayerID = uuid.Nil }, invalid},
		"zero time":           {func(p *wager.ExternalParams) { p.Now = time.Time{} }, invalid},
		"uninitialized money": {func(p *wager.ExternalParams) { p.Amount = money.Money{} }, invalid},
		"empty provider":      {func(p *wager.ExternalParams) { p.External.ProviderID = "" }, invalid},
		"empty external id":   {func(p *wager.ExternalParams) { p.External.ExternalID = "" }, invalid},
		"empty key":           {func(p *wager.ExternalParams) { p.External.IdempotencyKey = "" }, invalid},
		"empty hash":          {func(p *wager.ExternalParams) { p.External.PayloadHash = "" }, invalid},
		"empty round":         {func(p *wager.ExternalParams) { p.External.RoundID = "" }, invalid},
		"empty game":          {func(p *wager.ExternalParams) { p.External.GameID = "" }, invalid},
		"long game":           {func(p *wager.ExternalParams) { p.External.GameID = long }, invalid},
		"missing reference":   {func(p *wager.ExternalParams) { p.Kind = wager.KindRefund }, invalid},
		"long reference":      {func(p *wager.ExternalParams) { p.Kind, p.External.ReferenceExternalID = wager.KindWin, long }, invalid},
		"bet with reference":  {func(p *wager.ExternalParams) { p.External.ReferenceExternalID = "x" }, invalid},
		"self reference": {func(p *wager.ExternalParams) {
			p.Kind, p.External.ReferenceExternalID = wager.KindRefund, p.External.ExternalID
		}, invalid},
		"negative amount (neg)": {func(p *wager.ExternalParams) { p.Amount = p.Amount.Neg() }, invalid},
	}
	for name, tc := range mutations {
		p := f.params(t, wager.KindBet, "10.00", "")
		tc.mutate(&p)
		_, err := wager.NewExternal(p)
		assert.ErrorIs(t, err, tc.want, name)
	}

	p := f.params(t, wager.KindBet, "10.00", "")
	p.External.GameID = ""
	_, err := wager.NewExternal(p)
	assert.ErrorContains(t, err, "gameId", "the error names the offending field")
}

func TestNewOpening(t *testing.T) {
	t.Parallel()
	f := newFixture(t, "0.00")
	p := wager.OpeningParams{ID: uuid.New(), WalletID: f.walletID, PlayerID: f.playerID, Amount: testutil.Money(t, "10.00", "BRL"), Now: now}
	tx, err := wager.NewOpening(p)
	require.NoError(t, err)
	assert.Equal(t, wager.KindOpening, tx.Kind())
	assert.Equal(t, wager.OriginInternal, tx.Origin())
	assert.Equal(t, wager.External{}, tx.External())
	assert.Equal(t, wager.StatusPending, tx.Status())

	p.Amount = testutil.Money(t, "0.00", "BRL")
	_, err = wager.NewOpening(p)
	require.ErrorIs(t, err, wager.ErrInvalidTransaction)
	p.ID = uuid.Nil
	_, err = wager.NewOpening(p)
	require.ErrorIs(t, err, wager.ErrInvalidTransaction)
}

func TestTransitions(t *testing.T) {
	t.Parallel()
	f := newFixture(t, "100.00")
	later := now.Add(time.Second)
	bal := testutil.Money(t, "75.00", "BRL")
	ref := uuid.New()

	processed := f.tx(t, wager.KindWin, "25.00", "bet-1")
	require.NoError(t, processed.Process(bal, ref, later))
	assert.Equal(t, wager.StatusProcessed, processed.Status())
	assert.Equal(t, bal, processed.ResultBalance())
	assert.Equal(t, ref, processed.ReferenceTxID())
	assert.Equal(t, later, processed.UpdatedAt())

	rejected := f.tx(t, wager.KindBet, "25.00", "")
	require.NoError(t, rejected.Reject(wager.CodeInsufficientFunds, bal, later))
	assert.Equal(t, wager.StatusRejected, rejected.Status())
	assert.Equal(t, wager.CodeInsufficientFunds, rejected.FailureCode())

	failed := f.tx(t, wager.KindBet, "25.00", "")
	require.NoError(t, failed.Fail(wager.CodeInternalFailure, later))
	assert.Equal(t, wager.StatusFailed, failed.Status())

	assertTerminal(t, bal, processed, rejected, failed)
}

func assertTerminal(t *testing.T, bal money.Money, txs ...*wager.Transaction) {
	t.Helper()
	later := now.Add(time.Second)
	for _, terminal := range txs {
		assert.ErrorIs(t, terminal.Process(bal, uuid.Nil, later), wager.ErrInvalidTransition)
		assert.ErrorIs(t, terminal.Reject(wager.CodeInsufficientFunds, bal, later), wager.ErrInvalidTransition)
		assert.ErrorIs(t, terminal.Fail(wager.CodeInternalFailure, later), wager.ErrInvalidTransition)
		assert.ErrorIs(t, terminal.AwaitReference(later, later), wager.ErrInvalidTransition)
	}
}

func TestPendingReferenceToProcessed(t *testing.T) {
	t.Parallel()
	f := newFixture(t, "100.00")
	tx := f.tx(t, wager.KindRefund, "25.00", "bet-1")
	require.NoError(t, tx.AwaitReference(now.Add(time.Second), now))
	require.Equal(t, wager.StatusPendingReference, tx.Status())
	ref := uuid.New()
	require.NoError(t, tx.Process(testutil.Money(t, "125.00", "BRL"), ref, now.Add(time.Second)))
	assert.Equal(t, wager.StatusProcessed, tx.Status())
	assert.Equal(t, ref, tx.ReferenceTxID())
	assert.Equal(t, 1, tx.Attempts(), "attempt count is preserved")
}

func TestRehydratedTerminalRefusesTransitions(t *testing.T) {
	t.Parallel()
	bal := testutil.Money(t, "75.00", "BRL")
	var txs []*wager.Transaction
	for _, status := range []wager.Status{wager.StatusProcessed, wager.StatusRejected, wager.StatusFailed} {
		snapshot := validSnapshot(t)
		snapshot.Status = status
		if status != wager.StatusProcessed {
			snapshot.FailureCode = wager.CodeInternalFailure
		}
		if status == wager.StatusFailed {
			snapshot.ResultBalance = money.Money{}
		}
		tx, err := wager.Rehydrate(snapshot)
		require.NoError(t, err)
		txs = append(txs, tx)
	}
	assertTerminal(t, bal, txs...)
}

func TestTransitionValidation(t *testing.T) {
	t.Parallel()
	f := newFixture(t, "100.00")
	bal := testutil.Money(t, "1.00", "BRL")
	tx := f.tx(t, wager.KindBet, "25.00", "")
	assert.ErrorIs(t, tx.Process(money.Money{}, uuid.Nil, now), wager.ErrInvalidTransaction)
	assert.ErrorIs(t, tx.Process(bal, uuid.New(), now), wager.ErrInvalidTransaction, "BET cannot resolve a reference")
	assert.ErrorIs(t, tx.Reject("", bal, now), wager.ErrInvalidTransaction)
	assert.ErrorIs(t, tx.Reject(wager.CodeInsufficientFunds, money.Money{}, now), wager.ErrInvalidTransaction)
	assert.ErrorIs(t, tx.Fail("", now), wager.ErrInvalidTransaction)
	assert.ErrorIs(t, tx.AwaitReference(now, now), wager.ErrInvalidTransition)
	assert.Equal(t, wager.StatusPending, tx.Status())

	refund := f.tx(t, wager.KindRefund, "25.00", "bet-1")
	assert.ErrorIs(t, refund.Process(bal, uuid.Nil, now), wager.ErrInvalidTransaction, "REFUND needs a resolved reference")
	assert.Equal(t, wager.StatusPending, refund.Status())

	failed := f.tx(t, wager.KindBet, "25.00", "")
	require.NoError(t, failed.Fail(wager.CodeInternalFailure, now))
	assert.Error(t, failed.ResultBalance().Validate(), "Fail records no balance")
}

func TestAwaitReference(t *testing.T) {
	t.Parallel()
	f := newFixture(t, "100.00")
	tx := f.tx(t, wager.KindRefund, "25.00", "bet-1")
	next := now.Add(time.Second)

	require.NoError(t, tx.AwaitReference(next, now))
	assert.Equal(t, wager.StatusPendingReference, tx.Status())
	assert.Equal(t, 1, tx.Attempts())
	assert.Equal(t, next, tx.NextAttemptAt())

	require.NoError(t, tx.AwaitReference(next.Add(time.Second), next))
	assert.Equal(t, 2, tx.Attempts())

	require.NoError(t, tx.Reject(wager.CodeReferenceNotFound, testutil.Money(t, "100.00", "BRL"), next))
	assert.Equal(t, wager.StatusRejected, tx.Status())
}

func TestRehydrate(t *testing.T) {
	t.Parallel()
	f := newFixture(t, "100.00")
	s := wager.Snapshot{
		ID: uuid.New(), Origin: wager.OriginExternal, Kind: wager.KindBet, Status: wager.StatusProcessed,
		WalletID: f.walletID, PlayerID: f.playerID, Amount: testutil.Money(t, "1.00", "BRL"),
		External: f.params(t, wager.KindBet, "1.00", "").External, ResultBalance: testutil.Money(t, "99.00", "BRL"),
		Attempts: 3, CorrelationID: "c", CreatedAt: now, UpdatedAt: now,
	}
	tx, err := wager.Rehydrate(s)
	require.NoError(t, err)
	assert.Equal(t, 3, tx.Attempts())
	assert.Equal(t, wager.StatusProcessed, tx.Status())

	local := now.In(time.FixedZone("BRT", -3*60*60))
	s.NextAttemptAt, s.CreatedAt, s.UpdatedAt = local.Add(time.Minute), local, local
	tx, err = wager.Rehydrate(s)
	require.NoError(t, err)
	assert.Equal(t, time.UTC, tx.CreatedAt().Location())
	assert.Equal(t, time.UTC, tx.UpdatedAt().Location())
	assert.Equal(t, time.UTC, tx.NextAttemptAt().Location())
	assert.True(t, tx.NextAttemptAt().Equal(now.Add(time.Minute)), "NextAttemptAt is preserved")

	mutations := map[string]func(s *wager.Snapshot){
		"nil id":                     func(s *wager.Snapshot) { s.ID = uuid.Nil },
		"bad kind":                   func(s *wager.Snapshot) { s.Kind = "X" },
		"bad status":                 func(s *wager.Snapshot) { s.Status = "X" },
		"bad amount":                 func(s *wager.Snapshot) { s.Amount = money.Money{} },
		"internal non-op":            func(s *wager.Snapshot) { s.Origin = wager.OriginInternal },
		"external opening":           func(s *wager.Snapshot) { s.Kind = wager.KindOpening },
		"reversal without reference": func(s *wager.Snapshot) { s.Kind = wager.KindRollback },
	}
	for name, mutate := range mutations {
		c := s
		mutate(&c)
		_, err := wager.Rehydrate(c)
		assert.ErrorIs(t, err, wager.ErrInvalidTransaction, name)
	}
}

func TestFingerprint(t *testing.T) {
	t.Parallel()
	base := wager.Fingerprint{
		ProviderID: "provider-a", ExternalTransactionID: "t-1", PlayerID: "p", WalletID: "w",
		RoundID: "r", GameID: "g", Kind: wager.KindBet, Amount: "25.00", Currency: "BRL",
	}
	h := base.Hash()
	assert.Len(t, h, 64)
	assert.Equal(t, h, base.Hash(), "deterministic")
	// Canonical JSON: sorted keys, no whitespace, reference omitted when empty.
	canonical := `{"externalTransactionId":"t-1","gameId":"g","kind":"BET",` +
		`"money":{"amount":"25.00","currency":"BRL"},"playerId":"p","providerId":"provider-a","roundId":"r","walletId":"w"}`
	sum := sha256.Sum256([]byte(canonical))
	assert.Equal(t, hex.EncodeToString(sum[:]), h)

	variants := map[string]func(f *wager.Fingerprint){
		"amount":         func(f *wager.Fingerprint) { f.Amount = "25.01" },
		"reference":      func(f *wager.Fingerprint) { f.ReferenceExternalTransactionID = "t-0" },
		"kind":           func(f *wager.Fingerprint) { f.Kind = wager.KindWin },
		"provider":       func(f *wager.Fingerprint) { f.ProviderID = "provider-b" },
		"case sensitive": func(f *wager.Fingerprint) { f.ProviderID = "Provider-A" },
		"currency":       func(f *wager.Fingerprint) { f.Currency = "USD" },
	}
	for name, mutate := range variants {
		changed := base
		mutate(&changed)
		assert.NotEqual(t, h, changed.Hash(), name)
	}
}

func TestFingerprintDoesNotEscapeHTML(t *testing.T) {
	t.Parallel()
	f := wager.Fingerprint{
		ProviderID: "a&b<c>", ExternalTransactionID: "t-1", PlayerID: "p", WalletID: "w",
		RoundID: "r", GameID: "g", Kind: wager.KindBet, Amount: "25.00", Currency: "BRL",
	}
	canonical := `{"externalTransactionId":"t-1","gameId":"g","kind":"BET",` +
		`"money":{"amount":"25.00","currency":"BRL"},"playerId":"p","providerId":"a&b<c>","roundId":"r","walletId":"w"}`
	sum := sha256.Sum256([]byte(canonical))
	assert.Equal(t, hex.EncodeToString(sum[:]), f.Hash())
}
