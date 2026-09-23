package worker

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/Pantani/backend-challenge-go/internal/app"
)

// ErrPermanentPublish marks an invalid event payload that retrying cannot repair.
// All unclassified publisher errors remain retryable, including configuration errors.
var ErrPermanentPublish = errors.New("permanently invalid event payload")

// Publisher delivers one outbox record to the broker.
type Publisher interface {
	// Publish sends m. It must be idempotent per EventID: a record whose
	// confirmation was lost is published again with the same EventID, and
	// the broker (or the downstream consumer) deduplicates it.
	Publish(ctx context.Context, m app.OutboxMessage) error
}

// RelayMetrics records relay activity.
type RelayMetrics interface {
	// OutboxPublished counts a record confirmed as published.
	OutboxPublished()
	// OutboxFailure counts a failed publication attempt.
	OutboxFailure()
	// OutboxDeadLettered counts a permanently invalid payload quarantined after MaxAttempts.
	OutboxDeadLettered()
	// OutboxLag reports the age of the oldest pending record (0 when none).
	OutboxLag(d time.Duration)
}

// RelayConfig configures the relay.
type RelayConfig struct {
	// Owner identifies this instance in leases.
	Owner string
	// BatchSize bounds how many singular claims one round processes.
	BatchSize int
	// Lease is how long a claim is exclusive; a crashed publisher's records
	// become claimable again after it expires.
	Lease time.Duration
	// RetryBase is the delay after the first failed publication; it doubles
	// on every failure (app.Backoff) up to RetryMax.
	RetryBase time.Duration
	// RetryMax caps the retry delay.
	RetryMax time.Duration
	// PublishTime bounds one broker publication.
	PublishTime time.Duration
	// FinalizeTime is the budget of the durable writes around it (attempt
	// accounting and the confirmation, retry or dead-letter mutation). A
	// whole publication fits PublishTime + FinalizeTime, below Lease.
	FinalizeTime time.Duration
	// MaxAttempts quarantines explicitly permanent payload failures at this count.
	// Transient and unclassified errors keep retrying with capped backoff.
	MaxAttempts int
}

// Relay publishes committed outbox records. Several relays (instances) can
// run at once: claims use FOR UPDATE SKIP LOCKED plus a lease, retries use
// exponential backoff, and republication keeps the eventId.
type Relay struct {
	store     app.OutboxStore
	publisher Publisher
	clock     app.Clock
	cfg       RelayConfig
	logger    *slog.Logger
	metrics   RelayMetrics
}

// NewRelay builds a relay.
func NewRelay(store app.OutboxStore, publisher Publisher, clock app.Clock, cfg RelayConfig, logger *slog.Logger, metrics RelayMetrics) *Relay {
	return &Relay{store: store, publisher: publisher, clock: clock, cfg: cfg, logger: logger, metrics: metrics}
}

// maxRounds bounds how many acquisition rounds one tick runs. Each round
// processes up to BatchSize singular claims, so repeated rounds drain wallets.
const maxRounds = 20

// Tick publishes singular-acquisition rounds until nothing is due, then
// refreshes the lag.
func (r *Relay) Tick(ctx context.Context) {
	for range maxRounds {
		if !r.round(ctx) || ctx.Err() != nil {
			break
		}
	}
	r.refreshLag(ctx)
}

// round claims and publishes up to BatchSize records, acquiring each claim
// immediately before its work. It reports whether another round may have due
// work and stops before acquiring a claim after cancellation.
func (r *Relay) round(ctx context.Context) bool {
	for range r.cfg.BatchSize {
		if ctx.Err() != nil {
			return false
		}
		m, ok, err := r.store.Claim(ctx, r.cfg.Owner, uuid.New(), r.clock.Now(), r.cfg.Lease)
		if err != nil {
			r.logger.WarnContext(ctx, "outbox claim failed", "error", err)
			return false
		}
		if !ok {
			return false
		}
		r.publish(ctx, m)
	}
	return true
}

// publish handles one claimed record. It is detached from the shutdown
// signal so a claimed record is finished rather than abandoned, and the
// whole publication (attempt accounting, broker call, durable outcome) is
// bounded by PublishTime + FinalizeTime, which is shorter than the lease.
func (r *Relay) publish(parent context.Context, m app.OutboxMessage) {
	ctx, cancel := Detach(parent, r.cfg.PublishTime+r.cfg.FinalizeTime)
	defer cancel()
	log := r.logger.With("eventId", m.EventID, "eventType", m.EventType, "aggregateId", m.AggregateID)
	attempts, ok, err := r.store.StartAttempt(ctx, m.EventID, m.ClaimID, r.clock.Now(), r.cfg.Lease)
	if err != nil || !ok {
		log.WarnContext(ctx, "outbox publication attempt not started; claim was lost", "error", err)
		return
	}
	m.Attempts = attempts
	publishCtx, cancelPublish := context.WithTimeout(ctx, r.cfg.PublishTime)
	err = r.publisher.Publish(publishCtx, m)
	cancelPublish()
	if err != nil {
		r.failed(ctx, log, m, err)
		return
	}
	r.confirm(ctx, log, m)
}

// failed preserves recovery after any number of transient failures. Only an
// explicitly permanent payload can exhaust its attempts and be quarantined.
func (r *Relay) failed(ctx context.Context, log *slog.Logger, m app.OutboxMessage, cause error) {
	if errors.Is(cause, ErrPermanentPublish) && m.Attempts >= r.cfg.MaxAttempts {
		r.quarantine(ctx, log, m, cause)
		return
	}
	ok, err := r.store.MarkFailed(ctx, m.EventID, m.ClaimID, r.clock.Now().Add(r.backoff(m.Attempts)), cause.Error())
	if err != nil || !ok {
		log.WarnContext(ctx, "outbox failure not recorded; lease expiry will release it", "error", err)
		return
	}
	r.metrics.OutboxFailure()
	log.WarnContext(ctx, "outbox publish failed", "attempts", m.Attempts, "error", cause)
}

func (r *Relay) quarantine(ctx context.Context, log *slog.Logger, m app.OutboxMessage, cause error) {
	ok, err := r.store.MarkDead(ctx, m.EventID, m.ClaimID, r.clock.Now(), cause.Error())
	if err != nil || !ok {
		log.WarnContext(ctx, "outbox failure not recorded; lease expiry will release it", "error", err)
		return
	}
	r.metrics.OutboxFailure()
	r.metrics.OutboxDeadLettered()
	log.ErrorContext(ctx, "outbox event dead-lettered after exhausting its attempts", "attempts", m.Attempts, "error", cause)
}

// confirm records the publication. If this fails (or the lease was lost) the
// record is published again later with the same eventId.
func (r *Relay) confirm(ctx context.Context, log *slog.Logger, m app.OutboxMessage) {
	ok, err := r.store.MarkPublished(ctx, m.EventID, m.ClaimID, r.clock.Now())
	if err != nil || !ok {
		log.WarnContext(ctx, "outbox publication not confirmed; it will be republished with the same eventId", "error", err)
		return
	}
	r.metrics.OutboxPublished()
}

// backoff is the delay before the next attempt after attempts failures.
func (r *Relay) backoff(attempts int) time.Duration {
	return app.Backoff(r.cfg.RetryBase, r.cfg.RetryMax, attempts-1)
}

func (r *Relay) refreshLag(ctx context.Context) {
	oldest, ok, err := r.store.OldestPending(ctx)
	if err != nil {
		return
	}
	lag := time.Duration(0)
	if ok {
		lag = r.clock.Now().Sub(oldest)
	}
	r.metrics.OutboxLag(lag)
}
