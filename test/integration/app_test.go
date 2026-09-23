//go:build integration

package integration_test

import (
	"context"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"

	sqsadapter "github.com/Pantani/backend-challenge-go/internal/adapter/sqs"
	"github.com/Pantani/backend-challenge-go/internal/bootstrap"
	"github.com/Pantani/backend-challenge-go/internal/worker"
	"github.com/Pantani/backend-challenge-go/test/testenv"
)

type runningApp struct {
	http   testenv.Client
	group  *worker.Group
	app    *fx.App
	api    sqsadapter.API
	queues sqsadapter.Queues
}

func queueVars(names sqsadapter.QueueNames) map[string]string {
	return map[string]string{"SQS_INPUT_QUEUE": names.Input, "SQS_DLQ": names.DLQ, "SQS_EVENTS_QUEUE": names.Events}
}

func newApp(t *testing.T, overrides map[string]string) (*fx.App, *bootstrap.Addr, *worker.Group, context.Context, context.CancelFunc) {
	t.Helper()
	cfg, err := env.Config(overrides)
	require.NoError(t, err)
	var addr *bootstrap.Addr
	var group *worker.Group
	startCtx, cancelStart := context.WithTimeout(context.Background(), cfg.StartupTimeout)
	a := bootstrap.New(startCtx, cfg, fx.Replace(bootstrap.LogOutput{Writer: io.Discard}), fx.Populate(&addr, &group))
	return a, addr, group, startCtx, cancelStart
}

func startApp(t *testing.T) runningApp {
	t.Helper()
	api, q, names := provisionQueues(t, 3)
	a, addr, group, startCtx, cancelStart := newApp(t, queueVars(names))
	defer cancelStart()
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer stopCancel()
		require.NoError(t, a.Stop(stopCtx))
	})
	require.NoError(t, a.Start(startCtx))
	cancelStart()
	return runningApp{http: client("http://" + addr.String()), group: group, app: a, api: api, queues: q}
}

// client drives the API at base, authenticating with Keycloak tokens.
func client(base string) testenv.Client {
	return testenv.Client{Base: base, Token: env.Token}
}

func (r runningApp) wallet(t *testing.T, id string) map[string]any {
	t.Helper()
	res, err := r.walletE(id)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, res.Status)
	return res.Body
}

func (r runningApp) walletE(id string) (testenv.Response, error) {
	return r.http.Do(context.Background(), http.MethodGet, "/wallets/"+id, "wallet-service", "", nil)
}

func betBody(w testenv.Wallet, provider, ext, amount string) string {
	return testenv.OperationBody(w, provider, ext, "BET", amount, "")
}

func TestApplicationServesAuthenticatedFlows(t *testing.T) {
	t.Parallel()
	r := startApp(t)
	assert.Equal(t, http.StatusOK, r.http.Call(t, http.MethodGet, "/health/live", "", "", nil).Status)
	assert.Equal(t, http.StatusOK, r.http.Call(t, http.MethodGet, "/health/ready", "", "", nil).Status)

	w := r.http.OpenWallet(t, "1000.00")
	walletID := w.ID
	ext := uuid.NewString()
	key := map[string]string{"Idempotency-Key": "provider-a:" + ext}
	bet := r.http.Call(t, http.MethodPost, "/wagering/transactions", "provider-a", betBody(w, "provider-a", ext, "25.00"), key)
	require.Equal(t, http.StatusCreated, bet.Status, bet.Body)
	assert.Equal(t, map[string]any{"amount": "975.00", "currency": "BRL"}, bet.Body["balance"])

	replay := r.http.Call(t, http.MethodPost, "/wagering/transactions", "provider-a", betBody(w, "provider-a", ext, "25.00"), key)
	assert.Equal(t, http.StatusOK, replay.Status)
	assert.Equal(t, true, replay.Body["idempotentReplay"])
	conflict := r.http.Call(t, http.MethodPost, "/wagering/transactions", "provider-a", betBody(w, "provider-a", ext, "26.00"), key)
	assert.Equal(t, http.StatusConflict, conflict.Status)

	txID := bet.Body["transactionId"].(string)
	assert.Equal(t, http.StatusOK, r.http.Call(t, http.MethodGet, "/wagering/transactions/"+txID, "provider-a", "", nil).Status)
	assert.Equal(t, http.StatusOK, r.http.Call(t, http.MethodGet, "/providers/provider-a/wagering/transactions/"+ext, "provider-a", "", nil).Status)
	ledger := r.http.Call(t, http.MethodGet, "/wallets/"+walletID+"/ledger?limit=1", "wallet-service", "", nil)
	assert.Len(t, ledger.Body["items"], 1)
	assert.NotEmpty(t, ledger.Body["nextCursor"])
	rec := r.http.Call(t, http.MethodPost, "/wallets/"+walletID+"/reconciliation", "wallet-service", "", nil)
	assert.Equal(t, true, rec.Body["consistent"])
	assert.InDelta(t, 2, rec.Body["checkedEntries"], 0)
}

func TestProviderIsolationAndNoEffectsWhenUnauthorized(t *testing.T) {
	t.Parallel()
	r := startApp(t)
	w := r.http.OpenWallet(t, "100.00")
	walletID := w.ID
	ext := uuid.NewString()
	key := map[string]string{"Idempotency-Key": "provider-a:" + ext}
	bet := r.http.Call(t, http.MethodPost, "/wagering/transactions", "provider-a", betBody(w, "provider-a", ext, "10.00"), key)
	require.Equal(t, http.StatusCreated, bet.Status)
	txID := bet.Body["transactionId"].(string)

	cases := []struct {
		method, path, client, body string
		want                       int
	}{
		{http.MethodGet, "/wagering/transactions/" + txID, "provider-b", "", http.StatusNotFound},
		{http.MethodGet, "/providers/provider-a/wagering/transactions/" + ext, "provider-b", "", http.StatusForbidden},
		{http.MethodPost, "/wagering/transactions", "provider-b", betBody(w, "provider-a", ext, "10.00"), http.StatusForbidden},
		{http.MethodPost, "/wagering/transactions", "provider-a", betBody(w, "provider-a", uuid.NewString(), "50.00"), http.StatusBadRequest},
		{http.MethodPost, "/wagering/transactions", "", betBody(w, "provider-a", uuid.NewString(), "50.00"), http.StatusUnauthorized},
		{http.MethodPost, "/wagering/transactions", "no-role-client", betBody(w, "provider-a", uuid.NewString(), "50.00"), http.StatusForbidden},
		{http.MethodPost, "/wagering/transactions", "wallet-service", betBody(w, "provider-a", uuid.NewString(), "50.00"), http.StatusForbidden},
		{http.MethodPost, "/wallets", "provider-a", `{"playerId":"` + uuid.NewString() + `","initialBalance":{"amount":"1.00","currency":"BRL"}}`, http.StatusForbidden},
		{http.MethodPost, "/wallets/" + walletID + "/reconciliation", "provider-a", "", http.StatusForbidden},
		{http.MethodGet, "/wallets/" + walletID + "/ledger", "provider-b", "", http.StatusForbidden},
	}
	for _, c := range cases {
		headers := map[string]string{"Idempotency-Key": "provider-a:" + ext}
		if c.want == http.StatusBadRequest {
			headers = nil // missing Idempotency-Key
		}
		res := r.http.Call(t, c.method, c.path, c.client, c.body, headers)
		assert.Equal(t, c.want, res.Status, "%s %s as %s", c.method, c.path, c.client)
		assert.NotContains(t, res.Body, "transactionId", "no data is exposed")
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, r.http.Base+"/wallets/"+walletID, nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer forged.token.value")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)

	got := r.wallet(t, walletID)
	assert.Equal(t, map[string]any{"amount": "90.00", "currency": "BRL"}, got["balance"], "no financial effect")
	assert.InDelta(t, 2, got["version"], 0)
}

func TestApplicationConsumesSQSAndPublishesEvents(t *testing.T) {
	t.Parallel()
	r := startApp(t)
	w := r.http.OpenWallet(t, "100.00")
	walletID := w.ID
	ext := uuid.NewString()
	sendMessage(t, r.api, r.queues, "msg-"+ext, testenv.SubmitInput(w, "provider-a", ext, "BET", "15.00", ""))

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		res, err := r.walletE(walletID)
		if !assert.NoError(collect, err) || !assert.Equal(collect, http.StatusOK, res.Status) {
			return
		}
		assert.Equal(collect, "85.00", res.Body["balance"].(map[string]any)["amount"])
	}, 20*time.Second, 200*time.Millisecond)
	// Any running instance may publish them (the outbox is shared), so the
	// publication is checked on the outbox itself.
	wid, err := uuid.Parse(walletID)
	require.NoError(t, err)
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		pending, err := unpublished(wid)
		if !assert.NoError(collect, err) {
			return
		}
		assert.Zero(collect, pending)
	}, 20*time.Second, 200*time.Millisecond,
		"opening and bet events published after commit")
}

func TestApplicationStopsWorkersOnShutdown(t *testing.T) {
	t.Parallel()
	_, _, names := provisionQueues(t, 3)
	a, addr, group, startCtx, cancelStart := newApp(t, queueVars(names))
	defer cancelStart()
	require.NoError(t, a.Start(startCtx))
	cancelStart()
	require.Positive(t, group.Running())
	base := "http://" + addr.String()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	require.NoError(t, a.Stop(ctx))
	assert.Zero(t, group.Running(), "every worker returned")
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, base+"/health/live", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	if resp != nil {
		_ = resp.Body.Close()
	}
	require.Error(t, err, "no new inputs after shutdown")
}

func TestApplicationRefusesToStartWithBrokenDependencies(t *testing.T) {
	t.Parallel()
	_, _, names := provisionQueues(t, 3)
	cases := map[string]map[string]string{
		"missing queue": {"SQS_INPUT_QUEUE": "does-not-exist.fifo"},
		"database down": {"DATABASE_URL": "postgres://wallet:wallet@127.0.0.1:1/wallet?sslmode=disable"},
		"bad database":  {"DATABASE_URL": "postgres://%%%"},
		"bad listen":    {"HTTP_ADDR": "256.0.0.1:1"},
	}
	for name, overrides := range cases {
		vars := queueVars(names)
		for k, v := range overrides {
			vars[k] = v
		}
		a, _, _, startCtx, cancelStart := newApp(t, vars)
		err := a.Start(startCtx)
		cancelStart()
		assert.Error(t, err, name)
	}

	cfg, err := env.Config(queueVars(names))
	require.NoError(t, err)
	cfg.SQSSenderProviders = "missing-equals"
	startCtx, cancelStart := context.WithTimeout(context.Background(), cfg.StartupTimeout)
	defer cancelStart()
	app := bootstrap.New(startCtx, cfg, fx.Replace(bootstrap.LogOutput{Writer: io.Discard}))
	assert.Error(t, app.Err(), "programmatic sender policy bypasses environment validation but not bootstrap validation")
}

// hookRecorder keeps the stop hooks in execution order (by the function
// that registered them).
type hookRecorder struct {
	mu    sync.Mutex
	stops []string
}

func (r *hookRecorder) LogEvent(e fxevent.Event) {
	if h, ok := e.(*fxevent.OnStopExecuting); ok {
		r.mu.Lock()
		r.stops = append(r.stops, h.CallerName)
		r.mu.Unlock()
	}
}

func (r *hookRecorder) index(caller string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.IndexFunc(r.stops, func(s string) bool { return strings.HasSuffix(s, caller) })
}

// TestFxStopsServerThenWorkersThenPool is the regression test for the stop
// order: the consumers stop fetching first, the HTTP server drains next, the
// worker group (in-flight consumers, relay, resolver) stops after it and the
// database pool closes last, so no worker runs against a closed pool.
func TestFxStopsServerThenWorkersThenPool(t *testing.T) {
	t.Parallel()
	_, _, names := provisionQueues(t, 3)
	cfg, err := env.Config(queueVars(names))
	require.NoError(t, err)
	rec := &hookRecorder{}
	startCtx, cancelStart := context.WithTimeout(context.Background(), cfg.StartupTimeout)
	defer cancelStart()
	a := bootstrap.New(startCtx, cfg, fx.Replace(bootstrap.LogOutput{Writer: io.Discard}),
		fx.WithLogger(func() fxevent.Logger { return rec }))
	require.NoError(t, a.Start(startCtx))
	cancelStart()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	require.NoError(t, a.Stop(ctx))

	consumers := rec.index("bootstrap.stopConsumersFirst")
	server, workers, pool := rec.index("bootstrap.startServer"), rec.index("bootstrap.startWorkers"), rec.index("bootstrap.newPool")
	require.GreaterOrEqual(t, consumers, 0, rec.stops)
	require.GreaterOrEqual(t, server, 0, rec.stops)
	require.GreaterOrEqual(t, workers, 0, rec.stops)
	require.GreaterOrEqual(t, pool, 0, rec.stops)
	assert.Less(t, consumers, server, "no new queue input while HTTP drains: %v", rec.stops)
	assert.Less(t, server, workers, "no new inputs before the workers stop: %v", rec.stops)
	assert.Less(t, workers, pool, "workers stop before the pool closes: %v", rec.stops)
}

func TestFxGraphIsValid(t *testing.T) {
	t.Parallel()
	cfg, err := env.Config(nil)
	require.NoError(t, err)
	require.NoError(t, fx.ValidateApp(bootstrap.Options(cfg)))
}

// Not parallel: it changes the process environment read by the AWS SDK.
func TestApplicationRefusesToStartWithBrokenAWSConfig(t *testing.T) {
	t.Setenv("AWS_PROFILE", "profile-that-does-not-exist")
	t.Setenv("AWS_CONFIG_FILE", t.TempDir()+"/missing")
	a, _, _, _, cancelStart := newApp(t, nil)
	defer cancelStart()
	require.Error(t, a.Err())
}
