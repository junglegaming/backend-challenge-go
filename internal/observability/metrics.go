package observability

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/Pantani/backend-challenge-go/internal/domain/wager"
)

// Metrics holds every collector of the service. No metric ever carries a
// monetary amount; only counts, durations and ages are exported.
type Metrics struct {
	transactions       *prometheus.CounterVec
	duplicates         *prometheus.CounterVec
	conflicts          *prometheus.CounterVec
	processing         *prometheus.HistogramVec
	reconciliationDiff prometheus.Counter
	pending            *prometheus.CounterVec
	sqsMessages        *prometheus.CounterVec
	outboxPublished    prometheus.Counter
	outboxFailures     prometheus.Counter
	outboxDead         prometheus.Counter
	outboxLag          prometheus.Gauge
	httpRequests       *prometheus.CounterVec
	httpDuration       *prometheus.HistogramVec
}

// NewMetrics registers the collectors in reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		transactions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wager_transactions_total", Help: "Concluded wager submissions by source and status.",
		}, []string{"source", "status"}),
		duplicates: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wager_duplicates_total", Help: "Idempotent replays (duplicate deliveries) by source.",
		}, []string{"source"}),
		conflicts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wallet_concurrency_conflicts_total", Help: "Lost races retried by operation.",
		}, []string{"operation"}),
		processing: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "wager_processing_seconds", Help: "Submission latency by source.", Buckets: prometheus.DefBuckets,
		}, []string{"source"}),
		reconciliationDiff: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "wallet_reconciliation_divergences_total", Help: "Reconciliations whose balance differs from the ledger.",
		}),
		pending: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wager_pending_resolutions_total", Help: "Pending reference attempts by resulting status.",
		}, []string{"status"}),
		sqsMessages: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "sqs_messages_total", Help: "Consumed messages by outcome (processed, duplicate, retry, dlq, released).",
		}, []string{"outcome"}),
		outboxPublished: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "outbox_published_total", Help: "Outbox events published.",
		}),
		outboxFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "outbox_publish_failures_total", Help: "Failed outbox publication attempts (retried with backoff).",
		}),
		outboxDead: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "outbox_dead_lettered_total", Help: "Outbox events dead-lettered after exhausting their attempts.",
		}),
		outboxLag: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "outbox_lag_seconds", Help: "Age of the oldest unpublished outbox event.",
		}),
		httpRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_requests_total", Help: "HTTP requests by route and status code.",
		}, []string{"route", "code"}),
		httpDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "http_request_duration_seconds", Help: "HTTP latency by route.", Buckets: prometheus.DefBuckets,
		}, []string{"route"}),
	}
	reg.MustRegister(m.transactions, m.duplicates, m.conflicts, m.processing, m.reconciliationDiff, m.pending,
		m.sqsMessages, m.outboxPublished, m.outboxFailures, m.outboxDead, m.outboxLag, m.httpRequests, m.httpDuration)
	return m
}

// TransactionResult implements app.Metrics.
func (m *Metrics) TransactionResult(source string, status wager.Status, replay bool) {
	m.transactions.WithLabelValues(source, string(status)).Inc()
	if replay {
		m.duplicates.WithLabelValues(source).Inc()
	}
}

// ConcurrencyConflict implements app.Metrics.
func (m *Metrics) ConcurrencyConflict(operation string) { m.conflicts.WithLabelValues(operation).Inc() }

// ProcessingDuration implements app.Metrics.
func (m *Metrics) ProcessingDuration(source string, d time.Duration) {
	m.processing.WithLabelValues(source).Observe(d.Seconds())
}

// ReconciliationDivergence implements app.Metrics.
func (m *Metrics) ReconciliationDivergence() { m.reconciliationDiff.Inc() }

// PendingResolution implements app.Metrics.
func (m *Metrics) PendingResolution(status wager.Status) {
	m.pending.WithLabelValues(string(status)).Inc()
}

// SQSMessage counts a consumed message by outcome.
func (m *Metrics) SQSMessage(outcome string) { m.sqsMessages.WithLabelValues(outcome).Inc() }

// OutboxPublished counts a publication.
func (m *Metrics) OutboxPublished() { m.outboxPublished.Inc() }

// OutboxFailure counts a failed publication attempt.
func (m *Metrics) OutboxFailure() { m.outboxFailures.Inc() }

// OutboxDeadLettered counts a dead-lettered event.
func (m *Metrics) OutboxDeadLettered() { m.outboxDead.Inc() }

// OutboxLag sets the age of the oldest unpublished event.
func (m *Metrics) OutboxLag(d time.Duration) { m.outboxLag.Set(d.Seconds()) }

// HTTPRequest records a served request.
func (m *Metrics) HTTPRequest(route string, code int, d time.Duration) {
	m.httpRequests.WithLabelValues(route, strconv.Itoa(code)).Inc()
	m.httpDuration.WithLabelValues(route).Observe(d.Seconds())
}
