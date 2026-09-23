package observability_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Pantani/backend-challenge-go/internal/domain/wager"
	"github.com/Pantani/backend-challenge-go/internal/observability"
)

func TestLoggerAddsContextAttributes(t *testing.T) {
	t.Parallel()
	buf := &bytes.Buffer{}
	logger := observability.NewLogger(buf, "debug", "instance-1")
	ctx := observability.WithAttrs(context.Background(), slog.String("correlationId", "c-1"))
	ctx = observability.WithAttrs(ctx, slog.String("walletId", "w-1"))
	logger.With("static", 1).WithGroup("g").DebugContext(ctx, "hello", "k", "v")

	var rec map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &rec))
	assert.Equal(t, "hello", rec["msg"])
	assert.Equal(t, "wallet-service", rec["service"])
	assert.Equal(t, "instance-1", rec["instance"])
	group := rec["g"].(map[string]any)
	assert.Equal(t, "c-1", group["correlationId"])
	assert.Equal(t, "w-1", group["walletId"])
}

func TestLoggerLevels(t *testing.T) {
	t.Parallel()
	buf := &bytes.Buffer{}
	observability.NewLogger(buf, "warn", "i").Info("hidden")
	assert.Empty(t, buf.String())
	observability.NewLogger(buf, "bogus", "i").Info("shown with the info fallback")
	assert.True(t, strings.Contains(buf.String(), "info fallback"))
}

func TestMetrics(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := observability.NewMetrics(reg)
	m.TransactionResult("http", wager.StatusProcessed, false)
	m.TransactionResult("sqs", wager.StatusProcessed, true)
	m.ConcurrencyConflict("submit")
	m.ProcessingDuration("http", time.Millisecond)
	m.ReconciliationDivergence()
	m.PendingResolution(wager.StatusRejected)
	m.SQSMessage("dlq")
	m.OutboxPublished()
	m.OutboxFailure()
	m.OutboxDeadLettered()
	m.OutboxLag(3 * time.Second)
	m.HTTPRequest("GET /x", 200, time.Millisecond)

	count, err := testutil.GatherAndCount(reg)
	require.NoError(t, err)
	assert.Equal(t, 14, count)
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
# HELP wager_duplicates_total Idempotent replays (duplicate deliveries) by source.
# TYPE wager_duplicates_total counter
wager_duplicates_total{source="sqs"} 1
`), "wager_duplicates_total"))
}

func TestParseLevel(t *testing.T) {
	t.Parallel()
	for in, want := range map[string]slog.Level{"debug": slog.LevelDebug, "INFO": slog.LevelInfo, "Warn": slog.LevelWarn, "error": slog.LevelError} {
		got, err := observability.ParseLevel(in)
		require.NoError(t, err, in)
		assert.Equal(t, want, got, in)
	}
	_, err := observability.ParseLevel("loud")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "loud")
}

func TestWithAttrsOnEmptyContextAndMergeOrder(t *testing.T) {
	t.Parallel()
	buf := &bytes.Buffer{}
	logger := observability.NewLogger(buf, "info", "i")
	logger.InfoContext(context.Background(), "plain")
	var rec map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &rec))
	assert.NotContains(t, rec, "walletId", "an empty context adds nothing")

	ctx := observability.WithAttrs(context.Background(), slog.String("k", "first"), slog.String("only", "1"))
	ctx = observability.WithAttrs(ctx, slog.String("k", "second"))
	buf.Reset()
	logger.InfoContext(ctx, "merged")
	line := buf.String()
	assert.Less(t, strings.Index(line, `"k":"first"`), strings.Index(line, `"k":"second"`), "earlier attributes come first, later ones are appended")
	assert.Contains(t, line, `"only":"1"`)
}

func TestMetricNamesAndLabels(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := observability.NewMetrics(reg)
	m.TransactionResult("http", wager.StatusProcessed, false)
	m.HTTPRequest("GET /wallets/{id}", 404, time.Millisecond)
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
# HELP wager_transactions_total Concluded wager submissions by source and status.
# TYPE wager_transactions_total counter
wager_transactions_total{source="http",status="PROCESSED"} 1
# HELP http_requests_total HTTP requests by route and status code.
# TYPE http_requests_total counter
http_requests_total{code="404",route="GET /wallets/{id}"} 1
`), "wager_transactions_total", "http_requests_total"))
}
