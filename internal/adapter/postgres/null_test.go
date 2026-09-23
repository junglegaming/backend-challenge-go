package postgres

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Pantani/backend-challenge-go/internal/domain/money"
	"github.com/Pantani/backend-challenge-go/internal/domain/wager"
)

func TestNullHelpersRoundTrip(t *testing.T) {
	t.Parallel()
	assert.Nil(t, nullString(""))
	assert.Equal(t, "x", *nullString("x"))
	assert.Equal(t, "", deref(nil))
	assert.Equal(t, "x", deref(nullString("x")))

	id := uuid.New()
	assert.Nil(t, nullUUID(uuid.Nil))
	assert.Equal(t, id, *nullUUID(id))
	assert.Equal(t, uuid.Nil, derefUUID(nil))
	assert.Equal(t, id, derefUUID(nullUUID(id)))

	now := time.Now()
	assert.Nil(t, nullTime(time.Time{}))
	assert.Equal(t, now, *nullTime(now))
	assert.True(t, derefTime(nil).IsZero())
	assert.Equal(t, now, derefTime(nullTime(now)))
}

func mustMoney(t *testing.T, minor int64, currency string) money.Money {
	t.Helper()
	m, err := money.FromMinor(minor, money.Currency(currency))
	require.NoError(t, err)
	return m
}

func opening(t *testing.T) *wager.Transaction {
	t.Helper()
	tx, err := wager.NewOpening(wager.OpeningParams{ID: uuid.New(), WalletID: uuid.New(), PlayerID: uuid.New(),
		Amount: mustMoney(t, 100, "BRL"), Now: time.Now()})
	require.NoError(t, err)
	return tx
}

func TestResultColumns(t *testing.T) {
	t.Parallel()
	tx := opening(t)
	minor, currency := resultColumns(tx)
	assert.Nil(t, minor, "no balance observed yet")
	assert.Nil(t, currency)

	require.NoError(t, tx.Process(mustMoney(t, 100, "BRL"), uuid.Nil, time.Now()))
	minor, currency = resultColumns(tx)
	require.NotNil(t, minor)
	require.NotNil(t, currency)
	assert.Equal(t, int64(100), *minor)
	assert.Equal(t, "BRL", *currency)
}

func validRow() transactionRow {
	now := time.Now()
	return transactionRow{
		s:      wager.Snapshot{ID: uuid.New(), WalletID: uuid.New(), PlayerID: uuid.New(), CreatedAt: now, UpdatedAt: now},
		origin: "INTERNAL", kind: "OPENING", status: "PROCESSED", currency: "BRL", amount: 100,
	}
}

func TestToDomain(t *testing.T) {
	t.Parallel()
	row := validRow()
	row.status = "PENDING"
	_, err := row.toDomain()
	require.ErrorIs(t, err, wager.ErrInvalidTransaction)

	result, next, currency := int64(250), time.Now(), "BRL"
	row.status = "PROCESSED"
	row.result, row.resultCurrency, row.next = &result, &currency, &next
	tx, err := row.toDomain()
	require.NoError(t, err)
	assert.Equal(t, int64(250), tx.ResultBalance().Minor())
	assert.Equal(t, uuid.Nil, tx.ReferenceTxID())
	assert.Equal(t, next.UTC(), tx.NextAttemptAt())
}

func TestToDomainReportsUnknownCurrencies(t *testing.T) {
	t.Parallel()
	row := validRow()
	row.currency = "XYZ"
	_, err := row.toDomain()
	require.Error(t, err, "unknown amount currency")

	row = validRow()
	result, currency := int64(1), "XYZ"
	row.result, row.resultCurrency = &result, &currency
	_, err = row.toDomain()
	require.Error(t, err, "unknown result currency is not silently dropped")

	row = validRow()
	row.result = &result
	_, err = row.toDomain()
	require.Error(t, err, "result without currency")
}
