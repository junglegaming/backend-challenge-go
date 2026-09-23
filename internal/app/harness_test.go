package app_test

import (
	"bytes"
	"context"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/Pantani/backend-challenge-go/internal/app"
	"github.com/Pantani/backend-challenge-go/internal/domain/wallet"
	"github.com/Pantani/backend-challenge-go/internal/observability"
	"github.com/Pantani/backend-challenge-go/internal/testutil"
)

var t0 = time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)

// fakeIDs returns random UUIDs, or uuid.Nil on the configured call numbers.
type fakeIDs struct {
	calls atomic.Int64
	nilAt atomic.Int64
}

func (f *fakeIDs) New() uuid.UUID {
	if f.calls.Add(1) == f.nilAt.Load() {
		return uuid.Nil
	}
	return uuid.New()
}

// failNextID makes the n-th next call return uuid.Nil.
func (f *fakeIDs) failNextID(n int64) { f.nilAt.Store(f.calls.Load() + n) }

type harness struct {
	store    *memStore
	clock    *testutil.FakeClock
	ids      *fakeIDs
	registry *prometheus.Registry
	metrics  *observability.Metrics
	logs     *bytes.Buffer
	wagers   *app.WagerService
	wallets  *app.WalletService
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	h := &harness{store: newMemStore(), clock: testutil.NewFakeClock(t0), ids: &fakeIDs{}, logs: &bytes.Buffer{}, registry: prometheus.NewRegistry()}
	h.metrics = observability.NewMetrics(h.registry)
	logger := slog.New(slog.NewJSONHandler(h.logs, nil))
	deps := app.Deps{UoW: h.store, Queries: h.store, Clock: h.clock, IDs: h.ids, Metrics: h.metrics, Logger: logger}
	h.wagers = app.NewWagerService(app.WagerDeps{
		Deps:   deps,
		Policy: app.PendingPolicy{BaseDelay: time.Second, MaxDelay: time.Minute, MaxAttempts: 3, BatchSize: 10}, ConflictRetries: 2,
	})
	h.wallets = app.NewWalletService(app.WalletDeps{Deps: deps})
	return h
}

// counter returns the value of a counter series, summing the samples whose
// labels contain every pair of labels (the family may have more labels).
// The child collectors of observability.Metrics are unexported, so the
// registry is gathered instead of calling testutil.ToFloat64 on them.
func (h *harness) counter(t *testing.T, name string, labels map[string]string) float64 {
	t.Helper()
	families, err := h.registry.Gather()
	require.NoError(t, err)
	total := 0.0
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			if hasLabels(m, labels) {
				total += m.GetCounter().GetValue() + float64(m.GetHistogram().GetSampleCount())
			}
		}
	}
	return total
}

func hasLabels(m *dto.Metric, want map[string]string) bool {
	got := map[string]string{}
	for _, p := range m.GetLabel() {
		got[p.GetName()] = p.GetValue()
	}
	for k, v := range want {
		if got[k] != v {
			return false
		}
	}
	return true
}

func (h *harness) openWallet(t *testing.T, amount string) *wallet.Wallet {
	t.Helper()
	w, err := h.wallets.Open(context.Background(), app.OpenWalletCommand{PlayerID: uuid.New(), InitialBalance: testutil.BRL(t, amount), CorrelationID: "open"})
	require.NoError(t, err)
	return w
}

// op describes a provider operation in tests.
type op struct {
	provider string
	ext      string
	key      string
	kind     string
	amount   string
	currency string
	ref      string
	round    string
}

func (h *harness) cmd(t *testing.T, w *wallet.Wallet, o op) app.SubmitCommand {
	t.Helper()
	cmd, err := app.NewSubmitCommand(input(w, o))
	require.NoError(t, err)
	return cmd
}

func input(w *wallet.Wallet, o op) app.SubmitInput {
	in := app.SubmitInput{
		ProviderID: "provider-a", ExternalTransactionID: o.ext, IdempotencyKey: o.key, PlayerID: w.PlayerID().String(),
		WalletID: w.ID().String(), RoundID: "round-1", GameID: "game-1", Kind: o.kind, Amount: o.amount, Currency: "BRL",
		ReferenceExternalTransactionID: o.ref, CorrelationID: "corr",
	}
	if o.provider != "" {
		in.ProviderID = o.provider
	}
	if o.key == "" {
		in.IdempotencyKey = in.ProviderID + ":" + o.ext
	}
	if o.currency != "" {
		in.Currency = o.currency
	}
	if o.round != "" {
		in.RoundID = o.round
	}
	return in
}

func (h *harness) submit(t *testing.T, w *wallet.Wallet, o op) app.SubmitResult {
	t.Helper()
	res, err := h.wagers.Submit(context.Background(), h.cmd(t, w, o))
	require.NoError(t, err)
	return res
}

func (h *harness) balance(t *testing.T, w *wallet.Wallet) string {
	t.Helper()
	got, err := h.wallets.Get(context.Background(), w.ID())
	require.NoError(t, err)
	return got.Balance().Amount()
}

func (h *harness) resolve(t *testing.T) int {
	t.Helper()
	n, err := h.wagers.ResolveDue(context.Background())
	require.NoError(t, err)
	return n
}
