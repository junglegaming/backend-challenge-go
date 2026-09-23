// Package testutil holds the small helpers shared by the unit, integration
// and e2e tests: money literals, a concurrency-safe log sink, a fake clock
// and a private metrics registry. It has no build tag so every test package
// can import it.
package testutil

import (
	"bytes"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/Pantani/backend-challenge-go/internal/domain/money"
	"github.com/Pantani/backend-challenge-go/internal/observability"
)

// Money parses amount in the given currency, failing the test on error.
func Money(t testing.TB, amount, currency string) money.Money {
	t.Helper()
	m, err := money.Parse(amount, currency)
	require.NoError(t, err)
	return m
}

// BRL parses a BRL amount, failing the test on error.
func BRL(t testing.TB, amount string) money.Money {
	t.Helper()
	return Money(t, amount, "BRL")
}

// NewMetrics returns metrics registered on a private registry, so tests
// never collide on the global one.
func NewMetrics() *observability.Metrics {
	return observability.NewMetrics(prometheus.NewRegistry())
}

// SyncBuffer is an in-memory log sink safe for concurrent writers and
// readers (loggers write from worker goroutines while the test reads).
type SyncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

// Write appends p under the lock.
func (s *SyncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

// String returns everything written so far.
func (s *SyncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

// FakeClock is a manually driven app.Clock, safe for concurrent use.
type FakeClock struct {
	mu  sync.Mutex
	now time.Time
}

// NewFakeClock returns a clock stopped at now.
func NewFakeClock(now time.Time) *FakeClock { return &FakeClock{now: now} }

// Now returns the current fake instant.
func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance moves the clock by d; a negative duration moves it backwards.
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// Set moves the clock to now.
func (c *FakeClock) Set(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = now
}
