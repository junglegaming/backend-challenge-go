package contract_test

import (
	"errors"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Pantani/backend-challenge-go/internal/contract"
	"github.com/Pantani/backend-challenge-go/internal/domain/money"
)

type doc struct {
	Name  string         `json:"name"`
	Count int            `json:"count"`
	Money contract.Money `json:"money"`
	Tags  []string       `json:"tags"`
	Flag  bool           `json:"flag"`
}

func TestDecodeStrict(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name, body, msg string
		sentinel        error
	}{
		{name: "valid", body: `{"name":"a","count":1,"money":{"amount":"1.00","currency":"BRL"}}`},
		{name: "empty", body: ``, sentinel: contract.ErrEmptyBody, msg: "body is required"},
		{name: "whitespace", body: "  \n", sentinel: contract.ErrEmptyBody, msg: "body is required"},
		{name: "truncated", body: `{"name":`, msg: "malformed JSON: unexpected end of body"},
		{name: "syntax", body: `{"name":}`, msg: "malformed JSON at offset 9"},
		{name: "not object", body: `[]`, msg: "field body must be a object"},
		{name: "wrong type", body: `{"count":"x"}`, msg: "field count must be a number"},
		{name: "nested wrong type", body: `{"money":{"amount":1.5}}`, msg: "field money.amount must be a string"},
		{name: "array wanted", body: `{"tags":"x"}`, msg: "field tags must be a array"},
		{name: "bool wanted", body: `{"flag":"x"}`, msg: "field flag must be a boolean"},
		{name: "object wanted", body: `{"money":"x"}`, msg: "field money must be a object"},
		{name: "unknown field", body: `{"nope":1}`, msg: `unknown field "nope"`},
		{name: "trailing object", body: `{"name":"a"}{}`, sentinel: contract.ErrTrailingData, msg: "body must contain a single JSON object"},
		{name: "trailing garbage", body: `{"name":"a"} x`, sentinel: contract.ErrTrailingData, msg: "body must contain a single JSON object"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var d doc
			err := contract.DecodeStrict(strings.NewReader(tc.body), &d)
			if tc.msg == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, contract.ErrDecode)
			assert.Equal(t, "invalid JSON body: "+tc.msg, err.Error())
			if tc.sentinel != nil {
				assert.ErrorIs(t, err, tc.sentinel)
			}
			assert.NotContains(t, err.Error(), "contract_test.", "Go type names never leak")
		})
	}
}

func TestDecodeStrictReaderFailure(t *testing.T) {
	t.Parallel()
	var d doc
	err := contract.DecodeStrict(iotest.ErrReader(errors.New("connection reset")), &d)
	require.ErrorIs(t, err, contract.ErrDecode)
	assert.Equal(t, "invalid JSON body: malformed JSON", err.Error(), "transport errors are not echoed to clients")
	var de *contract.DecodeError
	require.True(t, errors.As(err, &de))
	assert.EqualError(t, de.Cause, "connection reset")
}

func TestDecodeErrorUnwrapsCause(t *testing.T) {
	t.Parallel()
	var d doc
	err := contract.DecodeStrict(strings.NewReader(`{"count":"x"}`), &d)
	var de *contract.DecodeError
	require.True(t, errors.As(err, &de))
	assert.Error(t, de.Cause)
	assert.Equal(t, "field count must be a number", de.Message)
}

func TestOperationToInput(t *testing.T) {
	t.Parallel()
	op := contract.Operation{
		ProviderID: "p", ExternalTransactionID: "e", PlayerID: "pl", WalletID: "w", RoundID: "r", GameID: "g", Kind: "BET",
		Money: contract.Money{Amount: "1.00", Currency: "BRL"}, ReferenceExternalTransactionID: "ref",
	}
	in := op.ToInput("key", "corr", "cause")
	assert.Equal(t, "p", in.ProviderID)
	assert.Equal(t, "e", in.ExternalTransactionID)
	assert.Equal(t, "key", in.IdempotencyKey)
	assert.Equal(t, "pl", in.PlayerID)
	assert.Equal(t, "w", in.WalletID)
	assert.Equal(t, "r", in.RoundID)
	assert.Equal(t, "g", in.GameID)
	assert.Equal(t, "BET", in.Kind)
	assert.Equal(t, "1.00", in.Amount)
	assert.Equal(t, "BRL", in.Currency)
	assert.Equal(t, "ref", in.ReferenceExternalTransactionID)
	assert.Equal(t, "corr", in.CorrelationID)
	assert.Equal(t, "cause", in.CausationID)
}

func TestNewMoneyAndFormatTime(t *testing.T) {
	t.Parallel()
	m, err := money.Parse("1000.50", "BRL")
	require.NoError(t, err)
	assert.Equal(t, contract.Money{Amount: "1000.50", Currency: "BRL"}, contract.NewMoney(m))
	at := time.Date(2026, 9, 8, 12, 0, 0, 123_000_000, time.FixedZone("x", 3600))
	assert.Equal(t, "2026-09-08T11:00:00.123Z", contract.FormatTime(at))
}
