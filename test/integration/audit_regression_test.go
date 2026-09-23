//go:build integration

package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Pantani/backend-challenge-go/internal/adapter/postgres"
	"github.com/Pantani/backend-challenge-go/internal/app"
)

func TestReconcileTurnoverAboveInt64(t *testing.T) {
	s := newServices(t, defaultPolicy)
	w := s.openWallet(t, "92233720368547758.07")
	s.submit(t, w, "turnover-bet", "BET", "92233720368547758.07", "")
	s.submit(t, w, "turnover-win", "WIN", "1.00", "")
	rec, err := s.wallets.Reconcile(context.Background(), w.ID())
	require.NoError(t, err)
	assert.True(t, rec.Consistent)
	assert.Equal(t, "1.00", rec.Calculated.Amount())
	assert.Equal(t, "0.00", rec.Difference.Amount())
	assert.EqualValues(t, 3, rec.CheckedEntries)
}

type recoveringPublisher struct {
	recordingPublisher
	eventID   uuid.UUID
	remaining int
}

func (p *recoveringPublisher) Publish(ctx context.Context, m app.OutboxMessage) error {
	if m.EventID == p.eventID && p.remaining > 0 {
		p.remaining--
		return errors.New("temporary broker outage")
	}
	return p.recordingPublisher.Publish(ctx, m)
}

func TestOutboxRecoversAfterTransientFailures(t *testing.T) {
	s := newServices(t, defaultPolicy)
	w := s.openWallet(t, "10.00")
	ctx := context.Background()
	var eventID uuid.UUID
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT event_id FROM outbox_events WHERE partition_key = $1 ORDER BY seq LIMIT 1`, w.ID().String()).Scan(&eventID))
	pub := &recoveringPublisher{eventID: eventID, remaining: 3}
	relay := newRelayWithAttempts(t, "recovery", postgres.NewOutboxStore(pool), pub, 1)
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		relay.Tick(ctx)
		assert.Equal(collect, 1, pub.count(eventID))
	}, 2*time.Second, 25*time.Millisecond)
	var published bool
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT published_at IS NOT NULL FROM outbox_events WHERE event_id = $1`, eventID).Scan(&published))
	assert.True(t, published)
}
