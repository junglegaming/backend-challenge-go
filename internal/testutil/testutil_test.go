package testutil

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMoneyParsesRequestedCurrency(t *testing.T) {
	assert.Equal(t, "12.34 BRL", Money(t, "12.34", "BRL").String())
}

func TestBRLParsesBrazilianReal(t *testing.T) {
	assert.Equal(t, "12.34 BRL", BRL(t, "12.34").String())
}

func TestNewMetricsReturnsIndependentMetrics(t *testing.T) {
	assert.NotNil(t, NewMetrics())
	assert.NotNil(t, NewMetrics())
}

func TestSyncBufferIsSafeForConcurrentReadersAndWriters(t *testing.T) {
	var buf SyncBuffer
	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for i := range 32 {
		wg.Go(func() {
			_, err := fmt.Fprintf(&buf, "line-%d\n", i)
			errs <- err
			_ = buf.String()
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	for i := range 32 {
		assert.Contains(t, buf.String(), fmt.Sprintf("line-%d\n", i))
	}
}

func TestFakeClockAdvanceAndSet(t *testing.T) {
	start := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)
	clock := NewFakeClock(start)
	clock.Advance(2 * time.Second)
	assert.Equal(t, start.Add(2*time.Second), clock.Now())
	clock.Set(start.Add(-time.Hour))
	assert.Equal(t, start.Add(-time.Hour), clock.Now())
}
