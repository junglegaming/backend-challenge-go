package httpapi_test

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/Pantani/backend-challenge-go/internal/adapter/auth"
	"github.com/Pantani/backend-challenge-go/internal/adapter/httpapi"
	"github.com/Pantani/backend-challenge-go/internal/app"
	"github.com/Pantani/backend-challenge-go/internal/domain/money"
	"github.com/Pantani/backend-challenge-go/internal/domain/wager"
	"github.com/Pantani/backend-challenge-go/internal/domain/wallet"
	"github.com/Pantani/backend-challenge-go/internal/observability"
	"github.com/Pantani/backend-challenge-go/internal/testutil"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

var now = time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)

type fakeWallets struct {
	open      func(app.OpenWalletCommand) (*wallet.Wallet, error)
	get       func(uuid.UUID) (*wallet.Wallet, error)
	ledger    func(uuid.UUID, string, int) (app.LedgerPage, error)
	reconcile func(uuid.UUID) (app.Reconciliation, error)
}

func (f fakeWallets) Open(_ context.Context, c app.OpenWalletCommand) (*wallet.Wallet, error) {
	return f.open(c)
}
func (f fakeWallets) Get(_ context.Context, id uuid.UUID) (*wallet.Wallet, error) { return f.get(id) }
func (f fakeWallets) Ledger(_ context.Context, id uuid.UUID, c string, l int) (app.LedgerPage, error) {
	return f.ledger(id, c, l)
}
func (f fakeWallets) Reconcile(_ context.Context, id uuid.UUID) (app.Reconciliation, error) {
	return f.reconcile(id)
}

type fakeWagers struct {
	submit func(app.SubmitCommand) (app.SubmitResult, error)
	get    func(app.Caller, uuid.UUID) (*wager.Transaction, error)
	byExt  func(app.Caller, string, string) (*wager.Transaction, error)
}

func (f fakeWagers) Submit(_ context.Context, c app.SubmitCommand) (app.SubmitResult, error) {
	return f.submit(c)
}
func (f fakeWagers) Get(_ context.Context, c app.Caller, id uuid.UUID) (*wager.Transaction, error) {
	return f.get(c, id)
}
func (f fakeWagers) GetByExternal(_ context.Context, c app.Caller, p, e string) (*wager.Transaction, error) {
	return f.byExt(c, p, e)
}

// tokens maps bearer tokens to principals; unknown tokens are invalid.
type tokens map[string]auth.Principal

func (t tokens) Verify(_ context.Context, raw string) (auth.Principal, error) {
	p, ok := t[raw]
	if !ok {
		return auth.Principal{}, auth.ErrUnauthenticated
	}
	return p, nil
}

var principals = tokens{
	"admin":      {ClientID: "wallet-service", Roles: []string{auth.RoleWalletAdmin}},
	"provider-a": {ClientID: "provider-a", ProviderID: "provider-a", Roles: []string{auth.RoleProvider}},
	"provider-b": {ClientID: "provider-b", ProviderID: "provider-b", Roles: []string{auth.RoleProvider}},
	"no-roles":   {ClientID: "nobody"},
}

// fixture builds one server per test so its logs accumulate across calls.
type fixture struct {
	wallets fakeWallets
	wagers  fakeWagers
	checks  []httpapi.HealthCheck
	logs    *bytes.Buffer
	handler http.Handler
}

func (f *fixture) server() http.Handler {
	if f.handler == nil {
		f.logs = &bytes.Buffer{}
		f.handler = httpapi.NewHandler(httpapi.Deps{
			Wallets: f.wallets, Wagers: f.wagers, Verifier: principals, Checks: f.checks,
			Metrics: testutil.NewMetrics(), MetricsHandler: http.NotFoundHandler(),
			Logger: observability.NewLogger(f.logs, "debug", "test"), ReadyTimeout: time.Second,
		})
	}
	return f.handler
}

type call struct {
	method, path, token, body string
	scheme                    string
	headers                   map[string]string
}

func (f *fixture) do(t *testing.T, c call) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	req := httptest.NewRequestWithContext(context.Background(), c.method, c.path, strings.NewReader(c.body))
	if c.token != "" {
		req.Header.Set("Authorization", cmp.Or(c.scheme, "Bearer")+" "+c.token)
	}
	for k, v := range c.headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	f.server().ServeHTTP(rec, req)
	if c.path != "/metrics" {
		assert.Equal(t, "application/json", rec.Header().Get("Content-Type"), "every response is JSON")
		assert.NotEmpty(t, rec.Header().Get("X-Correlation-Id"), "every response carries a correlation id")
	}
	var body map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	return rec, body
}

func sampleWallet(t *testing.T) *wallet.Wallet {
	t.Helper()
	w, err := wallet.Rehydrate(wallet.Snapshot{ID: uuid.New(), PlayerID: uuid.New(), Balance: testutil.BRL(t, "1000.00"), Version: 1, CreatedAt: now, UpdatedAt: now})
	require.NoError(t, err)
	return w
}

func sampleTx(t *testing.T, provider string, status wager.Status) *wager.Transaction {
	t.Helper()
	s := wager.Snapshot{
		ID: uuid.New(), Origin: wager.OriginExternal, Kind: wager.KindRefund, Status: status, WalletID: uuid.New(),
		PlayerID: uuid.New(), Amount: testutil.BRL(t, "25.00"), ResultBalance: testutil.BRL(t, "975.00"), ReferenceTxID: uuid.New(),
		External: wager.External{ProviderID: provider, ExternalID: "t-1", IdempotencyKey: "k", PayloadHash: "h",
			RoundID: "r", GameID: "g", ReferenceExternalID: "t-0"},
		NextAttemptAt: now, CreatedAt: now, UpdatedAt: now,
	}
	if status != wager.StatusProcessed {
		s.ReferenceTxID = uuid.Nil
	}
	if status == wager.StatusRejected || status == wager.StatusFailed {
		s.FailureCode = wager.CodeInsufficientFunds
	}
	if status == wager.StatusPendingReference || status == wager.StatusFailed {
		s.ResultBalance = money.Money{}
	}
	s.Attempts = 1
	tx, err := wager.Rehydrate(s)
	require.NoError(t, err)
	return tx
}

func TestHealth(t *testing.T) {
	t.Parallel()
	f := &fixture{checks: []httpapi.HealthCheck{
		{Name: "postgres", Check: func(context.Context) error { return nil }},
	}}
	rec, body := f.do(t, call{method: http.MethodGet, path: "/health/live"})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "alive", body["status"])

	rec, body = f.do(t, call{method: http.MethodGet, path: "/health/ready"})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "ready", body["status"])

	down := &fixture{checks: append(f.checks, httpapi.HealthCheck{Name: "sqs", Check: func(context.Context) error { return errors.New("down") }})}
	rec, body = down.do(t, call{method: http.MethodGet, path: "/health/ready"})
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Equal(t, map[string]any{"postgres": "ok", "sqs": "unavailable"}, body["checks"])
}

func TestReadinessChecksRunConcurrently(t *testing.T) {
	t.Parallel()
	slow := func(ctx context.Context) error {
		select {
		case <-time.After(400 * time.Millisecond):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	f := &fixture{checks: []httpapi.HealthCheck{{Name: "a", Check: slow}, {Name: "b", Check: slow}, {Name: "c", Check: slow}}}
	start := time.Now()
	rec, body := f.do(t, call{method: http.MethodGet, path: "/health/ready"})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "ready", body["status"])
	assert.Less(t, time.Since(start), 900*time.Millisecond, "checks overlap instead of running in sequence")
}

func TestAuthentication(t *testing.T) {
	t.Parallel()
	f := &fixture{}
	rec, body := f.do(t, call{method: http.MethodGet, path: "/wallets/" + uuid.NewString()})
	assert.Equal(t, http.StatusUnauthorized, rec.Code)
	assert.Equal(t, httpapi.CodeUnauthorized, body["code"])
	assert.Contains(t, rec.Header().Get("WWW-Authenticate"), "Bearer")

	rec, _ = f.do(t, call{method: http.MethodGet, path: "/wallets/" + uuid.NewString(), token: "forged"})
	assert.Equal(t, http.StatusUnauthorized, rec.Code)
	assert.Contains(t, rec.Header().Get("WWW-Authenticate"), "invalid_token")

	for _, scheme := range []string{"Basic", "Bearer\t"} {
		rec, _ = f.do(t, call{method: http.MethodGet, path: "/health/ready", token: "admin", scheme: scheme})
		assert.Equal(t, http.StatusOK, rec.Code, "public route")
		rec, _ = f.do(t, call{method: http.MethodPost, path: "/wagering/transactions", token: "provider-a", scheme: scheme})
		assert.Equal(t, http.StatusUnauthorized, rec.Code, scheme)
	}
}

func TestBearerSchemeIsCaseInsensitive(t *testing.T) {
	t.Parallel()
	f := &fixture{wallets: fakeWallets{get: func(uuid.UUID) (*wallet.Wallet, error) { return sampleWallet(t), nil }}}
	for _, scheme := range []string{"bearer", "BEARER", "Bearer "} {
		rec, _ := f.do(t, call{method: http.MethodGet, path: "/wallets/" + uuid.NewString(), token: "admin", scheme: scheme})
		assert.Equal(t, http.StatusOK, rec.Code, scheme)
	}
}

func TestAuthorizationPolicies(t *testing.T) {
	t.Parallel()
	f := &fixture{}
	id := uuid.NewString()
	forbidden := []call{
		{method: http.MethodPost, path: "/wallets", token: "provider-a", body: "{}"},
		{method: http.MethodGet, path: "/wallets/" + id, token: "provider-a"},
		{method: http.MethodGet, path: "/wallets/" + id + "/ledger", token: "provider-a"},
		{method: http.MethodPost, path: "/wallets/" + id + "/reconciliation", token: "provider-a"},
		{method: http.MethodPost, path: "/wagering/transactions", token: "admin", body: "{}"},
		{method: http.MethodGet, path: "/wagering/transactions/" + id, token: "no-roles"},
	}
	for _, c := range forbidden {
		rec, body := f.do(t, c)
		assert.Equal(t, http.StatusForbidden, rec.Code, c.path)
		assert.Equal(t, httpapi.CodeForbidden, body["code"])
	}
}

func TestPanicRecovery(t *testing.T) {
	t.Parallel()
	f := &fixture{wallets: fakeWallets{get: func(uuid.UUID) (*wallet.Wallet, error) { panic("bug") }}}
	rec, body := f.do(t, call{method: http.MethodGet, path: "/wallets/" + uuid.NewString(), token: "admin"})
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Equal(t, httpapi.CodeInternalError, body["code"])
	assert.Contains(t, f.logs.String(), "panic serving request")
}

func TestCorrelationID(t *testing.T) {
	t.Parallel()
	f := &fixture{}
	for _, ok := range []string{"abc", "req_1:2.3-x", strings.Repeat("x", 128)} {
		rec, _ := f.do(t, call{method: http.MethodGet, path: "/health/live", headers: map[string]string{"X-Correlation-Id": ok}})
		assert.Equal(t, ok, rec.Header().Get("X-Correlation-Id"))
	}
	for _, bad := range []string{strings.Repeat("x", 129), "with space", "new\nline", "ünïcode", "a/b"} {
		rec, _ := f.do(t, call{method: http.MethodGet, path: "/health/live", headers: map[string]string{"X-Correlation-Id": bad}})
		got := rec.Header().Get("X-Correlation-Id")
		assert.NotEqual(t, bad, got)
		assert.Len(t, got, 36, "replaced by a generated UUID")
	}
	assert.Contains(t, f.logs.String(), `"correlationId":"abc"`, "the access log carries the client id")
}

func TestUnmatchedRoutesAreJSON(t *testing.T) {
	t.Parallel()
	f := &fixture{}
	rec, body := f.do(t, call{method: http.MethodGet, path: "/nope", headers: map[string]string{"X-Correlation-Id": "c-404"}})
	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Equal(t, httpapi.CodeNotFound, body["code"])
	assert.Equal(t, "c-404", rec.Header().Get("X-Correlation-Id"))

	rec, body = f.do(t, call{method: http.MethodDelete, path: "/wallets", token: "admin"})
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
	assert.Equal(t, httpapi.CodeMethodNotAllowed, body["code"])
	assert.Equal(t, "POST", rec.Header().Get("Allow"))

	rec, body = f.do(t, call{method: http.MethodPut, path: "/wallets/" + uuid.NewString()})
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
	assert.Equal(t, httpapi.CodeMethodNotAllowed, body["code"])
	assert.Equal(t, "GET, HEAD", rec.Header().Get("Allow"))

	assert.Contains(t, f.logs.String(), `"route":"unmatched"`, "unmatched requests are logged under one label")
	assert.Contains(t, f.logs.String(), `"correlationId":"c-404"`)
}

func TestPathCleaningRedirectsAreObserved(t *testing.T) {
	t.Parallel()
	f := &fixture{}
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/health/../health/live", nil)
	req.Header.Set("X-Correlation-Id", "c-redirect")
	rec := httptest.NewRecorder()
	f.server().ServeHTTP(rec, req)
	assert.Equal(t, http.StatusTemporaryRedirect, rec.Code)
	assert.Equal(t, "/health/live", rec.Header().Get("Location"))
	assert.Equal(t, "c-redirect", rec.Header().Get("X-Correlation-Id"), "the mux redirect goes through the middleware")
	assert.Contains(t, f.logs.String(), `"route":"redirect"`)
}

func TestEmptyPatternPathCleaningRedirectsFollowServeMux(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, method, path string
	}{
		{"method mismatch after cleaning", http.MethodPut, "/health/../health/live"},
		{"unknown path after cleaning", http.MethodGet, "/unknown/../still-missing"},
	}
	for _, tc := range tests {
		reference := http.NewServeMux()
		reference.HandleFunc("GET /health/live", func(http.ResponseWriter, *http.Request) {})
		want := httptest.NewRecorder()
		reference.ServeHTTP(want, httptest.NewRequestWithContext(context.Background(), tc.method, tc.path, nil))
		require.Contains(t, []int{http.StatusMovedPermanently, http.StatusTemporaryRedirect}, want.Code, tc.name)
		require.NotEmpty(t, want.Header().Get("Location"), tc.name)

		f := &fixture{}
		got := httptest.NewRecorder()
		f.server().ServeHTTP(got, httptest.NewRequestWithContext(context.Background(), tc.method, tc.path, nil))
		assert.Equal(t, want.Code, got.Code, tc.name)
		assert.Equal(t, want.Header().Get("Location"), got.Header().Get("Location"), tc.name)
		assert.Contains(t, f.logs.String(), `"route":"redirect"`, tc.name)
		assert.NotContains(t, f.logs.String(), `"route":"unmatched"`, tc.name)
	}
}

func TestPanicAbortHandlerIsRethrown(t *testing.T) {
	t.Parallel()
	f := &fixture{wallets: fakeWallets{get: func(uuid.UUID) (*wallet.Wallet, error) { panic(http.ErrAbortHandler) }}}
	assert.PanicsWithValue(t, http.ErrAbortHandler, func() {
		f.do(t, call{method: http.MethodGet, path: "/wallets/" + uuid.NewString(), token: "admin"})
	})
	assert.NotContains(t, f.logs.String(), "panic serving request")
}

func TestMetricsRoute(t *testing.T) {
	t.Parallel()
	f := &fixture{}
	rec, _ := f.do(t, call{method: http.MethodGet, path: "/metrics"})
	assert.Equal(t, http.StatusNotFound, rec.Code, "served by the injected handler")
}
