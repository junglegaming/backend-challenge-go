package httpapi_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Pantani/backend-challenge-go/internal/adapter/httpapi"
	"github.com/Pantani/backend-challenge-go/internal/app"
	"github.com/Pantani/backend-challenge-go/internal/domain/wager"
	"github.com/Pantani/backend-challenge-go/internal/domain/wallet"
	"github.com/Pantani/backend-challenge-go/internal/testutil"
)

func TestOpenWallet(t *testing.T) {
	t.Parallel()
	var got app.OpenWalletCommand
	w := func(t *testing.T) *wallet.Wallet { return sampleWallet(t) }
	f := &fixture{wallets: fakeWallets{open: func(c app.OpenWalletCommand) (*wallet.Wallet, error) {
		got = c
		return w(t), nil
	}}}
	body := `{"playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","initialBalance":{"amount":"1000.00","currency":"BRL"}}`
	rec, resp := f.do(t, call{method: http.MethodPost, path: "/wallets", token: "admin", body: body})
	require.Equal(t, http.StatusCreated, rec.Code)
	assert.Equal(t, map[string]any{"amount": "1000.00", "currency": "BRL"}, resp["balance"])
	assert.InDelta(t, 1, resp["version"], 0)
	assert.Equal(t, "1000.00", got.InitialBalance.Amount())
	assert.NotEmpty(t, got.CorrelationID)
}

func TestOpenWalletErrors(t *testing.T) {
	t.Parallel()
	f := &fixture{wallets: fakeWallets{open: func(app.OpenWalletCommand) (*wallet.Wallet, error) { return nil, app.ErrWalletExists }}}
	cases := []struct {
		body   string
		status int
		code   string
	}{
		{`{"playerId":`, http.StatusBadRequest, httpapi.CodeInvalidRequest},
		{`{"playerId":"x","unknown":1}`, http.StatusBadRequest, httpapi.CodeInvalidRequest},
		{`{"playerId":"x"}{}`, http.StatusBadRequest, httpapi.CodeInvalidRequest},
		{`{"playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","initialBalance":{"amount":1000.00,"currency":"BRL"}}`, http.StatusBadRequest, httpapi.CodeInvalidRequest},
		{`{"playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","initialBalance":{"amount":"NaN","currency":"BRL"}}`, http.StatusBadRequest, httpapi.CodeInvalidRequest},
		{`{"playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","initialBalance":{"amount":"1.00","currency":"BRL"}}`, http.StatusConflict, httpapi.CodeWalletExists},
	}
	for _, tc := range cases {
		rec, resp := f.do(t, call{method: http.MethodPost, path: "/wallets", token: "admin", body: tc.body})
		assert.Equal(t, tc.status, rec.Code, tc.body)
		assert.Equal(t, tc.code, resp["code"], tc.body)
	}
}

func TestGetWalletLedgerAndReconciliation(t *testing.T) {
	t.Parallel()
	w := sampleWallet(t)
	entry, err := wallet.NewLedgerEntry(wallet.LedgerEntryParams{ID: uuid.New(), WalletID: w.ID(), TransactionID: uuid.New(),
		Direction: wallet.Debit, Amount: testutil.BRL(t, "25.00"), BalanceBefore: testutil.BRL(t, "1000.00"), BalanceAfter: testutil.BRL(t, "975.00"), CreatedAt: now})
	require.NoError(t, err)
	var gotCursor string
	var gotLimit int
	f := &fixture{wallets: fakeWallets{
		get: func(uuid.UUID) (*wallet.Wallet, error) { return w, nil },
		ledger: func(_ uuid.UUID, c string, l int) (app.LedgerPage, error) {
			gotCursor, gotLimit = c, l
			return app.LedgerPage{Entries: []wallet.LedgerEntry{entry}, NextCursor: "next"}, nil
		},
		reconcile: func(id uuid.UUID) (app.Reconciliation, error) {
			return app.Reconciliation{WalletID: id, Stored: testutil.BRL(t, "975.00"), Calculated: testutil.BRL(t, "975.00"), Difference: testutil.BRL(t, "0.00"), Consistent: true, CheckedEntries: 2}, nil
		},
	}}
	base := "/wallets/" + w.ID().String()
	rec, resp := f.do(t, call{method: http.MethodGet, path: base, token: "admin"})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, w.ID().String(), resp["id"])

	rec, resp = f.do(t, call{method: http.MethodGet, path: base + "/ledger?cursor=abc&limit=10", token: "admin"})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "next", resp["nextCursor"])
	assert.Len(t, resp["items"], 1)
	assert.Equal(t, "abc", gotCursor)
	assert.Equal(t, 10, gotLimit)

	rec, _ = f.do(t, call{method: http.MethodGet, path: base + "/ledger", token: "admin"})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, gotLimit, "an absent limit lets the use case apply its default")

	rec, resp = f.do(t, call{method: http.MethodPost, path: base + "/reconciliation", token: "admin"})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, true, resp["consistent"])
	assert.Equal(t, map[string]any{"amount": "0.00", "currency": "BRL"}, resp["difference"])
}

func TestWalletRouteErrors(t *testing.T) {
	t.Parallel()
	f := &fixture{wallets: fakeWallets{
		get:       func(uuid.UUID) (*wallet.Wallet, error) { return nil, app.ErrWalletNotFound },
		ledger:    func(uuid.UUID, string, int) (app.LedgerPage, error) { return app.LedgerPage{}, app.ErrUnavailable },
		reconcile: func(uuid.UUID) (app.Reconciliation, error) { return app.Reconciliation{}, fmt.Errorf("db exploded") },
		open: func(app.OpenWalletCommand) (*wallet.Wallet, error) {
			return nil, fmt.Errorf("insert into wallets (secret internals): %w", app.ErrWalletExists)
		},
	}}
	id := uuid.NewString()
	cases := []struct {
		method, path string
		status       int
	}{
		{http.MethodGet, "/wallets/nope", http.StatusBadRequest},
		{http.MethodGet, "/wallets/" + uuid.Nil.String(), http.StatusBadRequest},
		{http.MethodGet, "/wallets/" + id + "/ledger?limit=0", http.StatusBadRequest},
		{http.MethodGet, "/wallets/" + id + "/ledger?limit=201", http.StatusBadRequest},
		{http.MethodGet, "/wallets/" + id, http.StatusNotFound},
		{http.MethodGet, "/wallets/nope/ledger", http.StatusBadRequest},
		{http.MethodGet, "/wallets/" + id + "/ledger?limit=x", http.StatusBadRequest},
		{http.MethodGet, "/wallets/" + id + "/ledger", http.StatusServiceUnavailable},
		{http.MethodPost, "/wallets/nope/reconciliation", http.StatusBadRequest},
		{http.MethodPost, "/wallets/" + id + "/reconciliation", http.StatusInternalServerError},
	}
	for _, tc := range cases {
		rec, resp := f.do(t, call{method: tc.method, path: tc.path, token: "admin"})
		assert.Equal(t, tc.status, rec.Code, tc.path)
		assert.NotContains(t, resp["message"], "exploded", "internal errors are not exposed")
	}
	_, resp := f.do(t, call{method: http.MethodPost, path: "/wallets", token: "admin",
		body: `{"playerId":"` + uuid.NewString() + `","initialBalance":{"amount":"1.00","currency":"BRL"}}`})
	assert.Equal(t, "a wallet already exists for this player and currency", resp["message"], "wrapped context never reaches clients")
}

const submitBody = `{"providerId":"provider-a","externalTransactionId":"transaction-123",
"playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","walletId":"0192f291-27dd-7d3f-8071-5f8685deef37",
"roundId":"round-987","gameId":"fortune-chimp","kind":"BET","money":{"amount":"25.00","currency":"BRL"}}`

func TestSubmitStatuses(t *testing.T) {
	t.Parallel()
	cases := []struct {
		status wager.Status
		replay bool
		code   int
	}{
		{wager.StatusProcessed, false, http.StatusCreated},
		{wager.StatusProcessed, true, http.StatusOK},
		{wager.StatusPendingReference, false, http.StatusAccepted},
		{wager.StatusRejected, false, http.StatusUnprocessableEntity},
		{wager.StatusFailed, true, http.StatusUnprocessableEntity},
	}
	for _, tc := range cases {
		tx := sampleTx(t, "provider-a", tc.status)
		var got app.SubmitCommand
		f := &fixture{wagers: fakeWagers{submit: func(c app.SubmitCommand) (app.SubmitResult, error) {
			got = c
			return app.SubmitResult{Transaction: tx, Replay: tc.replay}, nil
		}}}
		rec, resp := f.do(t, call{method: http.MethodPost, path: "/wagering/transactions", token: "provider-a", body: submitBody,
			headers: map[string]string{"Idempotency-Key": "provider-a:transaction-123"}})
		assert.Equal(t, tc.code, rec.Code, tc.status)
		assert.Equal(t, string(tc.status), resp["status"])
		assert.Equal(t, tc.replay, resp["idempotentReplay"])
		assert.Equal(t, "provider-a:transaction-123", got.IdempotencyKey)
	}
}

func TestAccessLogCarriesCallerWalletAndTransaction(t *testing.T) {
	t.Parallel()
	tx := sampleTx(t, "provider-a", wager.StatusProcessed)
	var got app.SubmitCommand
	f := &fixture{wagers: fakeWagers{submit: func(c app.SubmitCommand) (app.SubmitResult, error) {
		got = c
		return app.SubmitResult{Transaction: tx}, nil
	}}}
	rec, _ := f.do(t, call{method: http.MethodPost, path: "/wagering/transactions", token: "provider-a", body: submitBody,
		headers: map[string]string{"Idempotency-Key": "provider-a:transaction-123"}})
	require.Equal(t, http.StatusCreated, rec.Code)
	var access map[string]any
	for _, line := range strings.Split(strings.TrimSpace(f.logs.String()), "\n") {
		var record map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &record))
		if record["msg"] == "http request" {
			access = record
		}
	}
	require.NotNil(t, access, "access log written")
	assert.Equal(t, "provider-a", access["providerId"])
	assert.Equal(t, got.WalletID.String(), access["walletId"])
	assert.Equal(t, tx.ID().String(), access["transactionId"])
	assert.NotEmpty(t, access["correlationId"])
}

func TestSubmitErrors(t *testing.T) {
	t.Parallel()
	f := &fixture{wagers: fakeWagers{submit: func(app.SubmitCommand) (app.SubmitResult, error) {
		return app.SubmitResult{}, app.ErrIdempotencyConflict
	}}}
	key := map[string]string{"Idempotency-Key": "k"}
	cases := []struct {
		c      call
		status int
		code   string
	}{
		{call{token: "provider-a", body: submitBody}, http.StatusBadRequest, httpapi.CodeInvalidRequest},
		{call{token: "provider-b", body: submitBody, headers: key}, http.StatusForbidden, httpapi.CodeForbidden},
		{call{token: "provider-a", body: "[]", headers: key}, http.StatusBadRequest, httpapi.CodeInvalidRequest},
		{call{token: "provider-a", body: submitBody[:40] + `"OPENING"}`, headers: key}, http.StatusBadRequest, httpapi.CodeInvalidRequest},
		{call{token: "provider-a", body: submitBody, headers: key}, http.StatusConflict, httpapi.CodeIdempotencyConflict},
	}
	for _, tc := range cases {
		tc.c.method, tc.c.path = http.MethodPost, "/wagering/transactions"
		rec, resp := f.do(t, tc.c)
		assert.Equal(t, tc.status, rec.Code, tc.code)
		assert.Equal(t, tc.code, resp["code"])
	}
}

func TestSubmitUnavailableSetsRetryAfter(t *testing.T) {
	t.Parallel()
	f := &fixture{wagers: fakeWagers{submit: func(app.SubmitCommand) (app.SubmitResult, error) { return app.SubmitResult{}, app.ErrConflict }}}
	rec, resp := f.do(t, call{method: http.MethodPost, path: "/wagering/transactions", token: "provider-a", body: submitBody,
		headers: map[string]string{"Idempotency-Key": "k"}})
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Equal(t, httpapi.CodeServiceUnavailable, resp["code"])
	assert.Equal(t, "1", rec.Header().Get("Retry-After"))
}

func TestGetTransactionRoutes(t *testing.T) {
	t.Parallel()
	tx := sampleTx(t, "provider-a", wager.StatusPendingReference)
	var callers []app.Caller
	f := &fixture{wagers: fakeWagers{
		get: func(c app.Caller, id uuid.UUID) (*wager.Transaction, error) {
			callers = append(callers, c)
			if id != tx.ID() {
				return nil, app.ErrTransactionNotFound
			}
			return tx, nil
		},
		byExt: func(c app.Caller, p, _ string) (*wager.Transaction, error) {
			callers = append(callers, c)
			if p != c.ProviderID && !c.Internal {
				return nil, app.ErrForbidden
			}
			return tx, nil
		},
	}}
	rec, resp := f.do(t, call{method: http.MethodGet, path: "/wagering/transactions/" + tx.ID().String(), token: "provider-a"})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "PENDING_REFERENCE", resp["status"])
	assert.Equal(t, "2026-09-08T12:00:00.000Z", resp["nextAttemptAt"])
	assert.NotContains(t, resp, "referenceTransactionId", "pending reference is not resolved yet")
	assert.NotContains(t, resp, "balance", "pending operations have no result balance yet")

	rec, _ = f.do(t, call{method: http.MethodGet, path: "/wagering/transactions/" + uuid.NewString(), token: "admin"})
	assert.Equal(t, http.StatusNotFound, rec.Code)
	rec, _ = f.do(t, call{method: http.MethodGet, path: "/wagering/transactions/nope", token: "admin"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	rec, _ = f.do(t, call{method: http.MethodGet, path: "/providers/provider-a/wagering/transactions/t-1", token: "admin"})
	assert.Equal(t, http.StatusOK, rec.Code)
	rec, _ = f.do(t, call{method: http.MethodGet, path: "/providers/provider-a/wagering/transactions/t-1", token: "provider-b"})
	assert.Equal(t, http.StatusForbidden, rec.Code)

	assert.Equal(t, app.Caller{ProviderID: "provider-a"}, callers[0])
	assert.Equal(t, app.Caller{Internal: true}, callers[1])
}

func TestEscapedExternalIDPreservesPathValues(t *testing.T) {
	t.Parallel()
	tx := sampleTx(t, "provider-a", wager.StatusProcessed)
	var gotProviderID, gotExternalID string
	f := &fixture{wagers: fakeWagers{byExt: func(_ app.Caller, providerID, externalID string) (*wager.Transaction, error) {
		gotProviderID, gotExternalID = providerID, externalID
		return tx, nil
	}}}
	cases := []struct {
		name, path, providerID, externalID string
	}{
		{"escaped slash", "/providers/provider%2Fregion/wagering/transactions/external%2Fpart", "provider/region", "external/part"},
		{"escaped current segment", "/providers/provider-a/wagering/transactions/%2E", "provider-a", "."},
		{"escaped parent segment", "/providers/provider-a/wagering/transactions/%2E%2E", "provider-a", ".."},
	}
	for _, tc := range cases {
		rec, _ := f.do(t, call{method: http.MethodGet, path: tc.path, token: "admin"})
		require.Equal(t, http.StatusOK, rec.Code, tc.name)
		assert.Equal(t, tc.providerID, gotProviderID, tc.name)
		assert.Equal(t, tc.externalID, gotExternalID, tc.name)
	}
}

func TestTransactionResponseWithoutOptionalFields(t *testing.T) {
	t.Parallel()
	s := wager.Snapshot{ID: uuid.New(), Origin: wager.OriginInternal, Kind: wager.KindOpening, Status: wager.StatusProcessed,
		WalletID: uuid.New(), PlayerID: uuid.New(), Amount: testutil.BRL(t, "1.00"), ResultBalance: testutil.BRL(t, "1.00"), CreatedAt: now, UpdatedAt: now}
	tx, err := wager.Rehydrate(s)
	require.NoError(t, err)
	f := &fixture{wagers: fakeWagers{get: func(app.Caller, uuid.UUID) (*wager.Transaction, error) { return tx, nil }}}
	_, resp := f.do(t, call{method: http.MethodGet, path: "/wagering/transactions/" + tx.ID().String(), token: "admin"})
	for _, k := range []string{"providerId", "referenceTransactionId", "nextAttemptAt", "failureCode"} {
		assert.NotContains(t, resp, k)
	}
}

func TestDecodeErrorMessages(t *testing.T) {
	t.Parallel()
	f := &fixture{wallets: fakeWallets{open: func(app.OpenWalletCommand) (*wallet.Wallet, error) { return sampleWallet(t), nil }}}
	cases := []struct{ body, msg string }{
		{``, "body is required"},
		{`{"playerId":`, "malformed JSON: unexpected end of body"},
		{`{"playerId":}`, "malformed JSON at offset 13"},
		{`[]`, "field body must be a object"},
		{`{"playerId":1}`, "field playerId must be a string"},
		{`{"initialBalance":{"amount":1000.00}}`, "field initialBalance.amount must be a string"},
		{`{"initialBalance":"x"}`, "field initialBalance must be a object"},
		{`{"playerId":"x","unknown":1}`, `unknown field "unknown"`},
		{`{"playerId":"x"}{}`, "body must contain a single JSON object"},
	}
	for _, tc := range cases {
		rec, resp := f.do(t, call{method: http.MethodPost, path: "/wallets", token: "admin", body: tc.body})
		assert.Equal(t, http.StatusBadRequest, rec.Code, tc.body)
		assert.Equal(t, httpapi.CodeInvalidRequest, resp["code"], tc.body)
		assert.Equal(t, "validation failed: "+tc.msg, resp["message"], tc.body)
		assert.NotContains(t, resp["message"], "httpapi.", "Go identifiers never leak")
	}
}

func TestOversizedBodyIs413(t *testing.T) {
	t.Parallel()
	f := &fixture{}
	body := `{"playerId":"` + strings.Repeat("x", 65<<10) + `"}`
	rec, resp := f.do(t, call{method: http.MethodPost, path: "/wallets", token: "admin", body: body})
	assert.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
	assert.Equal(t, httpapi.CodePayloadTooLarge, resp["code"])
	assert.Equal(t, "request body must not exceed 65536 bytes", resp["message"])
}

func TestContentTypeNegotiation(t *testing.T) {
	t.Parallel()
	f := &fixture{wallets: fakeWallets{open: func(app.OpenWalletCommand) (*wallet.Wallet, error) { return sampleWallet(t), nil }}}
	body := `{"playerId":"` + uuid.NewString() + `","initialBalance":{"amount":"1.00","currency":"BRL"}}`
	for _, ok := range []string{"", "application/json", "application/json; charset=utf-8", "Application/JSON"} {
		rec, _ := f.do(t, call{method: http.MethodPost, path: "/wallets", token: "admin", body: body, headers: map[string]string{"Content-Type": ok}})
		assert.Equal(t, http.StatusCreated, rec.Code, ok)
	}
	for _, bad := range []string{"text/plain", "application/xml", "application/x-www-form-urlencoded", "garbage/;;"} {
		rec, resp := f.do(t, call{method: http.MethodPost, path: "/wallets", token: "admin", body: body, headers: map[string]string{"Content-Type": bad}})
		assert.Equal(t, http.StatusUnsupportedMediaType, rec.Code, bad)
		assert.Equal(t, httpapi.CodeUnsupportedMediaType, resp["code"], bad)
		assert.Equal(t, "Content-Type must be application/json", resp["message"])
	}
}

func TestIdempotencyKeyRules(t *testing.T) {
	t.Parallel()
	var got app.SubmitCommand
	f := &fixture{wagers: fakeWagers{submit: func(c app.SubmitCommand) (app.SubmitResult, error) {
		got = c
		return app.SubmitResult{Transaction: sampleTx(t, "provider-a", wager.StatusProcessed)}, nil
	}}}
	submit := func(key string) (*httptest.ResponseRecorder, map[string]any) {
		return f.do(t, call{method: http.MethodPost, path: "/wagering/transactions", token: "provider-a", body: submitBody,
			headers: map[string]string{"Idempotency-Key": key}})
	}
	rec, _ := submit("  provider-a:transaction-123 \t")
	require.Equal(t, http.StatusCreated, rec.Code)
	assert.Equal(t, "provider-a:transaction-123", got.IdempotencyKey, "surrounding whitespace is trimmed")
	rec, _ = submit(strings.Repeat("k", 128))
	assert.Equal(t, http.StatusCreated, rec.Code, "128 characters are allowed")

	rejected := []struct{ key, msg string }{
		{"   ", "Idempotency-Key header is required"},
		{strings.Repeat("k", 129), "Idempotency-Key must be at most 128 characters"},
		{"key\x7f", "Idempotency-Key must contain only printable ASCII characters"},
		{"clé", "Idempotency-Key must contain only printable ASCII characters"},
	}
	for _, tc := range rejected {
		rec, resp := submit(tc.key)
		assert.Equal(t, http.StatusBadRequest, rec.Code, tc.msg)
		assert.Equal(t, httpapi.CodeInvalidRequest, resp["code"])
		assert.Equal(t, "validation failed: "+tc.msg, resp["message"])
	}
}
