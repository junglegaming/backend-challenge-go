package app

import (
	"time"
)

// Metric and log sources.
const (
	// SourceHTTP labels operations submitted through the HTTP API.
	SourceHTTP = "http"
	// SourceSQS labels operations consumed from the broker.
	SourceSQS = "sqs"
	// SourceWorker labels pending references resolved by the worker.
	SourceWorker = "worker"
)

// PendingPolicy controls how PENDING_REFERENCE operations are retried.
type PendingPolicy struct {
	// BaseDelay is the delay after the first deferral; it doubles each time.
	BaseDelay time.Duration
	// MaxDelay caps the exponential backoff.
	MaxDelay time.Duration
	// MaxAttempts is how many deferrals happen before the operation is
	// rejected with REFERENCE_NOT_FOUND (or REFERENCE_NOT_PROCESSED). It must
	// be at least 1 (config validation enforces it); with 0 an operation
	// whose reference is missing would be rejected without ever waiting.
	MaxAttempts int
	// BatchSize bounds how many due operations one worker tick resolves.
	BatchSize int
}

// maxShift bounds the exponent of Backoff so base << shift stays far from
// int64 overflow for realistic bases.
const maxShift = 20

// Backoff is the single exponential backoff formula of the service: it
// returns base doubled exponent times (exponent is clamped to [0, 20]),
// capped at limit. A non-positive base, a shift that overflows or a delay
// above limit all yield limit.
func Backoff(base, limit time.Duration, exponent int) time.Duration {
	shift := min(max(exponent, 0), maxShift)
	delay := base << shift
	if delay <= 0 || delay > limit || delay>>shift != base {
		return limit
	}
	return delay
}

// Next returns when the attempt-th retry should run (exponential backoff).
func (p PendingPolicy) Next(attempts int, now time.Time) time.Time {
	return now.Add(Backoff(p.BaseDelay, p.MaxDelay, attempts))
}
