package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"

	"github.com/VictorM0nteiro/backend-challenge-go/internal/adapters/postgres"
	"github.com/VictorM0nteiro/backend-challenge-go/internal/testutil"
)

const (
	testPlayerID = "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1"
	wagersURL    = "/wagering/transactions"
)

// fakeQueue stands in for SQS in the HTTP tests, which do not start LocalStack.
type fakeQueue struct{ err error }

func (f fakeQueue) Ping(context.Context) error { return f.err }

// newTestAPI starts the API with a healthy queue.
func newTestAPI(t *testing.T) string {
	t.Helper()
	return newTestAPIWithQueue(t, fakeQueue{})
}

// newTestAPIWithQueue starts a real Postgres, a real pool, the real routes, and
// an authenticator bound to the Keycloak from TestMain. The queue decides how
// readiness answers for it. It returns the base URL.
func newTestAPIWithQueue(t *testing.T, queue QueueChecker) string {
	t.Helper()
	ctx := context.Background()

	pool, err := postgres.NewPool(ctx, postgres.PoolConfig{
		DSN:            testutil.StartPostgres(t),
		MaxConns:       5,
		AcquireTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	t.Cleanup(pool.Close)

	auth := NewAuthenticator(testKeycloak.Issuer, oidc.NewRemoteKeySet(ctx, testKeycloak.JWKSURL()))
	api := NewServer(pool,
		postgres.NewWalletRepository(pool),
		postgres.NewWagerReader(pool),
		postgres.NewWagerProcessor(pool),
		auth,
		queue,
	)
	ts := httptest.NewServer(api.Handler())
	t.Cleanup(ts.Close)
	return ts.URL
}

// call sends one request and decodes the JSON answer. The body may be empty.
func call(t *testing.T, method, url string, headers map[string]string, body any) (int, map[string]any) {
	t.Helper()
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		reader = bytes.NewReader(raw)
	}

	req, err := http.NewRequest(method, url, reader)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()

	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

// errorCode returns error.code, or "" when the answer carries no error.
func errorCode(out map[string]any) string {
	e, _ := out["error"].(map[string]any)
	code, _ := e["code"].(string)
	return code
}

// bearer returns the Authorization header for a client's real token.
func bearer(client string) map[string]string {
	return map[string]string{"Authorization": "Bearer " + testTokens[client]}
}

// wagerHeaders is what a provider sends for one operation.
func wagerHeaders(provider, key string) map[string]string {
	h := bearer(provider)
	h["Idempotency-Key"] = key
	return h
}

// openWallet opens a BRL wallet for testPlayerID as the internal service.
func openWallet(t *testing.T, base, amount string) string {
	t.Helper()
	status, out := call(t, http.MethodPost, base+"/wallets", bearer("wallet-service"), map[string]any{
		"playerId":       testPlayerID,
		"initialBalance": map[string]string{"amount": amount, "currency": "BRL"},
	})
	if status != http.StatusCreated {
		t.Fatalf("open wallet: status %d, body %v", status, out)
	}
	return out["id"].(string)
}

// wagerBody builds a request body for provider-a.
func wagerBody(walletID, externalID, kind, amount string) map[string]any {
	return map[string]any{
		"providerId":            "provider-a",
		"externalTransactionId": externalID,
		"playerId":              testPlayerID,
		"walletId":              walletID,
		"roundId":               "round-987",
		"gameId":                "fortune-chimp",
		"kind":                  kind,
		"money":                 map[string]string{"amount": amount, "currency": "BRL"},
	}
}

func TestOpenWallet_ZeroThenDuplicateIsConflict(t *testing.T) {
	base := newTestAPI(t)
	body := map[string]any{
		"playerId":       testPlayerID,
		"initialBalance": map[string]string{"amount": "0", "currency": "BRL"},
	}

	if status, out := call(t, http.MethodPost, base+"/wallets", bearer("wallet-service"), body); status != http.StatusCreated {
		t.Fatalf("first open: status %d, body %v", status, out)
	}
	status, out := call(t, http.MethodPost, base+"/wallets", bearer("wallet-service"), body)
	if status != http.StatusConflict || errorCode(out) != "conflict" {
		t.Fatalf("duplicate open: status %d code %q, want 409 conflict", status, errorCode(out))
	}
}

func TestWallets_NoTokenIs401(t *testing.T) {
	base := newTestAPI(t)
	walletID := openWallet(t, base, "10.00")

	status, out := call(t, http.MethodGet, base+"/wallets/"+walletID, nil, nil)
	if status != http.StatusUnauthorized || errorCode(out) != "unauthenticated" {
		t.Fatalf("status %d code %q, want 401 unauthenticated", status, errorCode(out))
	}
}

func TestSubmitWager_BetIsProcessedThenReplayedWithTheSameBalance(t *testing.T) {
	base := newTestAPI(t)
	walletID := openWallet(t, base, "1000.00")
	headers := wagerHeaders("provider-a", "provider-a:transaction-123")
	bet := wagerBody(walletID, "transaction-123", "BET", "25.00")

	status, first := call(t, http.MethodPost, base+wagersURL, headers, bet)
	if status != http.StatusCreated || first["status"] != "PROCESSED" || first["idempotentReplay"] != false {
		t.Fatalf("first: status %d body %v", status, first)
	}
	if first["balance"].(map[string]any)["amount"] != "975.00" {
		t.Fatalf("first balance = %v, want 975.00", first["balance"])
	}

	status, replay := call(t, http.MethodPost, base+wagersURL, headers, bet)
	if status != http.StatusCreated || replay["idempotentReplay"] != true {
		t.Fatalf("replay: status %d body %v", status, replay)
	}
	if replay["transactionId"] != first["transactionId"] {
		t.Fatal("replay returned a different transaction")
	}
	if replay["balance"].(map[string]any)["amount"] != "975.00" {
		t.Fatalf("replay balance = %v, want the balance observed by the original", replay["balance"])
	}
}

func TestSubmitWager_SameKeyWithDifferentBodyIs422(t *testing.T) {
	base := newTestAPI(t)
	walletID := openWallet(t, base, "1000.00")
	headers := wagerHeaders("provider-a", "k-reuse")

	if status, out := call(t, http.MethodPost, base+wagersURL, headers, wagerBody(walletID, "t-1", "BET", "25.00")); status != http.StatusCreated {
		t.Fatalf("first: status %d body %v", status, out)
	}
	status, out := call(t, http.MethodPost, base+wagersURL, headers, wagerBody(walletID, "t-1", "BET", "30.00"))
	if status != http.StatusUnprocessableEntity || errorCode(out) != "idempotency_key_reused" {
		t.Fatalf("status %d code %q, want 422 idempotency_key_reused", status, errorCode(out))
	}
}

func TestSubmitWager_SameOperationUnderAnotherKeyIs409(t *testing.T) {
	base := newTestAPI(t)
	walletID := openWallet(t, base, "1000.00")
	bet := wagerBody(walletID, "t-dup", "BET", "25.00")

	if status, out := call(t, http.MethodPost, base+wagersURL, wagerHeaders("provider-a", "k-one"), bet); status != http.StatusCreated {
		t.Fatalf("first: status %d body %v", status, out)
	}
	status, out := call(t, http.MethodPost, base+wagersURL, wagerHeaders("provider-a", "k-two"), bet)
	if status != http.StatusConflict || errorCode(out) != "duplicate_operation" {
		t.Fatalf("status %d code %q, want 409 duplicate_operation", status, errorCode(out))
	}

	// The wallet was debited once.
	status, out = call(t, http.MethodGet, base+"/wallets/"+walletID, bearer("wallet-service"), nil)
	if status != http.StatusOK || out["balance"].(map[string]any)["amount"] != "975.00" {
		t.Fatalf("wallet: status %d body %v, want balance 975.00", status, out)
	}
}

func TestSubmitWager_InsufficientFundsIsARecordedRejectionNotAnError(t *testing.T) {
	base := newTestAPI(t)
	walletID := openWallet(t, base, "10.00")

	status, out := call(t, http.MethodPost, base+wagersURL,
		wagerHeaders("provider-a", "k-poor"), wagerBody(walletID, "t-poor", "BET", "25.00"))
	if status != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422", status)
	}
	if out["status"] != "REJECTED" || out["failureCode"] != "insufficient_funds" {
		t.Fatalf("body = %v, want REJECTED with insufficient_funds", out)
	}
	if errorCode(out) != "" {
		t.Fatal("a business rejection must not carry the error envelope")
	}
}

func TestSubmitWager_OtherProviderIsNotFound(t *testing.T) {
	base := newTestAPI(t)
	walletID := openWallet(t, base, "1000.00")

	// provider-b is authenticated, but the body names provider-a.
	status, out := call(t, http.MethodPost, base+wagersURL,
		wagerHeaders("provider-b", "k-other"), wagerBody(walletID, "t-1", "BET", "25.00"))
	if status != http.StatusNotFound || errorCode(out) != "not_found" {
		t.Fatalf("status %d code %q, want 404 not_found", status, errorCode(out))
	}
}

func TestSubmitWager_BadRequestsLeaveNoKeyBehind(t *testing.T) {
	base := newTestAPI(t)
	walletID := openWallet(t, base, "1000.00")

	cases := []struct {
		name    string
		headers map[string]string
		body    map[string]any
		status  int
		code    string
	}{
		{"sem Idempotency-Key", bearer("provider-a"), wagerBody(walletID, "t-1", "BET", "25.00"), 400, "invalid_request"},
		{"sem token", map[string]string{"Idempotency-Key": "k-x"}, wagerBody(walletID, "t-1", "BET", "25.00"), 401, "unauthenticated"},
		{"valor com tres casas", wagerHeaders("provider-a", "k-bad"), wagerBody(walletID, "t-2", "BET", "25.001"), 400, "invalid_request"},
		{"OPENING vindo de fora", wagerHeaders("provider-a", "k-bad"), wagerBody(walletID, "t-3", "OPENING", "25.00"), 400, "invalid_request"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, out := call(t, http.MethodPost, base+wagersURL, tc.headers, tc.body)
			if status != tc.status || errorCode(out) != tc.code {
				t.Fatalf("status %d code %q, want %d %s", status, errorCode(out), tc.status, tc.code)
			}
		})
	}

	// The rejected attempts used "k-bad". It must still be free, because a
	// failed input rolls back its own key claim.
	status, out := call(t, http.MethodPost, base+wagersURL,
		wagerHeaders("provider-a", "k-bad"), wagerBody(walletID, "t-ok", "BET", "25.00"))
	if status != http.StatusCreated {
		t.Fatalf("key reused after invalid inputs: status %d body %v", status, out)
	}
}

func TestGetWager_OnlyTheOwnerProviderSeesIt(t *testing.T) {
	base := newTestAPI(t)
	walletID := openWallet(t, base, "1000.00")

	_, created := call(t, http.MethodPost, base+wagersURL,
		wagerHeaders("provider-a", "k-get"), wagerBody(walletID, "t-get", "BET", "25.00"))
	url := base + wagersURL + "/" + created["transactionId"].(string)

	status, out := call(t, http.MethodGet, url, bearer("provider-a"), nil)
	if status != http.StatusOK || out["status"] != "PROCESSED" {
		t.Fatalf("owner: status %d body %v", status, out)
	}

	status, out = call(t, http.MethodGet, url, bearer("provider-b"), nil)
	if status != http.StatusNotFound || errorCode(out) != "not_found" {
		t.Fatalf("other provider: status %d code %q, want 404", status, errorCode(out))
	}
}

func TestHealth_LiveAndReadyAnswer200WhileDatabaseIsUp(t *testing.T) {
	base := newTestAPI(t)
	for _, path := range []string{"/health/live", "/health/ready"} {
		if status, out := call(t, http.MethodGet, base+path, nil, nil); status != http.StatusOK {
			t.Fatalf("%s: status %d body %v", path, status, out)
		}
	}
}

func TestHealth_ReadyIs503WhenTheQueueIsDown(t *testing.T) {
	base := newTestAPIWithQueue(t, fakeQueue{err: errors.New("queue unreachable")})

	status, out := call(t, http.MethodGet, base+"/health/ready", nil, nil)
	if status != http.StatusServiceUnavailable || out["status"] != "unavailable" {
		t.Fatalf("status %d body %v, want 503 unavailable", status, out)
	}
	checks := out["checks"].(map[string]any)
	if checks["queue"] != "down" || checks["database"] != "up" {
		t.Fatalf("checks = %v, want queue down and database up", checks)
	}
}
