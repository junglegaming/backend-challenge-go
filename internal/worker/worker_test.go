package worker_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/Pantani/backend-challenge-go/internal/app"
	"github.com/Pantani/backend-challenge-go/internal/observability"
	"github.com/Pantani/backend-challenge-go/internal/testutil"
	"github.com/Pantani/backend-challenge-go/internal/worker"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

var (
	errBoom = errors.New("boom")
	t0      = time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
)

func TestGroupStopsWorkers(t *testing.T) {
	g := worker.NewGroup(context.Background(), observability.NewLogger(&testutil.SyncBuffer{}, "info", "t"))
	var ticks atomic.Int64
	g.Go("ticker", func(ctx context.Context) { worker.Loop(ctx, time.Millisecond, func(context.Context) { ticks.Add(1) }) })
	require.Eventually(t, func() bool { return ticks.Load() > 2 }, time.Second, time.Millisecond)
	assert.Equal(t, 1, g.Running())

	require.NoError(t, g.Stop(context.Background()))
	assert.Zero(t, g.Running())
}

func TestGroupGoAfterStopIsDropped(t *testing.T) {
	logs := &testutil.SyncBuffer{}
	g := worker.NewGroup(context.Background(), observability.NewLogger(logs, "info", "t"))
	require.NoError(t, g.Stop(context.Background()))
	g.Go("late", func(context.Context) { t.Error("a worker must not start after Stop") })
	assert.Zero(t, g.Running())
	assert.Contains(t, logs.String(), "group already stopped")
	require.NoError(t, g.Stop(context.Background()), "Stop is idempotent")
}

func TestGroupCountsDuplicateNames(t *testing.T) {
	g := worker.NewGroup(context.Background(), observability.NewLogger(&testutil.SyncBuffer{}, "info", "t"))
	block := func(ctx context.Context) { <-ctx.Done() }
	g.Go("same", block)
	g.Go("same", block)
	assert.Equal(t, 2, g.Running(), "names are labels, not identities")
	require.NoError(t, g.Stop(context.Background()))
	assert.Zero(t, g.Running())
}

func TestDetachAndSleep(t *testing.T) {
	parent, cancel := context.WithCancel(context.WithValue(context.Background(), ctxKey{}, "v"))
	cancel()
	ctx, stop := worker.Detach(parent, time.Minute)
	defer stop()
	require.NoError(t, ctx.Err(), "the parent's cancellation is not inherited")
	assert.Equal(t, "v", ctx.Value(ctxKey{}), "values are")
	_, ok := ctx.Deadline()
	assert.True(t, ok)

	start := time.Now()
	worker.Sleep(parent, time.Minute)
	assert.Less(t, time.Since(start), time.Second, "a done context cuts the sleep short")
	worker.Sleep(context.Background(), time.Millisecond)
	assert.GreaterOrEqual(t, time.Since(start), time.Millisecond)
}

type ctxKey struct{}

func TestGroupStopDeadline(t *testing.T) {
	g := worker.NewGroup(context.Background(), observability.NewLogger(&testutil.SyncBuffer{}, "info", "t"))
	release := make(chan struct{})
	g.Go("stuck", func(context.Context) { <-release })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err := g.Stop(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Contains(t, err.Error(), "1 workers still running")
	close(release)
	require.NoError(t, g.Stop(context.Background()))
}

type fakeStore struct {
	claimErr     error
	msgs         []app.OutboxMessage
	published    []uuid.UUID
	failed       map[uuid.UUID]time.Time
	markOK       bool
	markErr      error
	markFailErr  error
	oldest       time.Time
	hasOldest    bool
	oldestErr    error
	claimedOwner string
	rounds       int
	dead         []uuid.UUID
	attempts     map[uuid.UUID]int
	claimed      []uuid.UUID
	startLost    bool
	startErr     error
	markFailLost bool
	markDeadLost bool
	failedCtxErr error
	claimHook    func()
	claimIDs     map[uuid.UUID]uuid.UUID
	claimUntil   map[uuid.UUID]time.Time
	startAt      time.Time
	renewedUntil time.Time
}

// Claim hands out the queued messages one at a time, like a drained outbox.
func (s *fakeStore) Claim(_ context.Context, owner string, claimID uuid.UUID, now time.Time, lease time.Duration) (app.OutboxMessage, bool, error) {
	s.claimedOwner = owner
	s.rounds++
	if s.claimErr != nil || len(s.msgs) == 0 {
		return app.OutboxMessage{}, false, s.claimErr
	}
	m := s.msgs[0]
	s.msgs = s.msgs[1:]
	m.ClaimID = claimID
	s.claimed = append(s.claimed, m.EventID)
	if s.attempts == nil {
		s.attempts = make(map[uuid.UUID]int)
	}
	s.attempts[m.EventID] = m.Attempts
	if s.claimIDs == nil {
		s.claimIDs = make(map[uuid.UUID]uuid.UUID)
		s.claimUntil = make(map[uuid.UUID]time.Time)
	}
	s.claimIDs[m.EventID] = claimID
	s.claimUntil[m.EventID] = now.Add(lease)
	if s.claimHook != nil {
		s.claimHook()
	}
	return m, true, nil
}

func (s *fakeStore) StartAttempt(
	_ context.Context, id, claimID uuid.UUID, now time.Time, lease time.Duration,
) (int, bool, error) {
	if s.startErr != nil || s.startLost {
		return 0, false, s.startErr
	}
	s.startAt = now
	if s.claimIDs[id] != claimID || !now.Before(s.claimUntil[id]) {
		return 0, false, nil
	}
	s.claimUntil[id] = now.Add(lease)
	s.renewedUntil = s.claimUntil[id]
	s.attempts[id]++
	return s.attempts[id], true, nil
}

func (s *fakeStore) MarkPublished(_ context.Context, id, _ uuid.UUID, _ time.Time) (bool, error) {
	if s.markOK {
		s.published = append(s.published, id)
	}
	return s.markOK, s.markErr
}

func (s *fakeStore) MarkFailed(ctx context.Context, id, _ uuid.UUID, next time.Time, _ string) (bool, error) {
	s.failedCtxErr = ctx.Err()
	if s.markFailLost {
		return false, s.markFailErr
	}
	s.failed[id] = next
	return true, s.markFailErr
}

func (s *fakeStore) MarkDead(_ context.Context, id, _ uuid.UUID, _ time.Time, _ string) (bool, error) {
	s.dead = append(s.dead, id)
	return !s.markDeadLost, s.markFailErr
}

func (s *fakeStore) OldestPending(context.Context) (time.Time, bool, error) {
	return s.oldest, s.hasOldest, s.oldestErr
}

type fakePublisher struct {
	fail      map[uuid.UUID]bool
	permanent bool
	hook      func(ctx context.Context, m app.OutboxMessage) // observes each call
}

func (p fakePublisher) Publish(ctx context.Context, m app.OutboxMessage) error {
	if p.hook != nil {
		p.hook(ctx, m)
	}
	if p.fail[m.EventID] {
		if p.permanent {
			return errors.Join(worker.ErrPermanentPublish, errBoom)
		}
		return errBoom
	}
	return nil
}

func newRelay(store *fakeStore, pub fakePublisher, logs *testutil.SyncBuffer) *worker.Relay {
	return worker.NewRelay(store, pub, testutil.NewFakeClock(t0), worker.RelayConfig{
		Owner: "instance-1", BatchSize: 10, Lease: time.Minute, RetryBase: time.Second, RetryMax: 30 * time.Second,
		PublishTime: time.Second, FinalizeTime: time.Second,
		MaxAttempts: 50,
	}, observability.NewLogger(logs, "debug", "t"), testutil.NewMetrics())
}

type relayMetrics struct {
	published int
	failures  int
	dead      int
}

func (m *relayMetrics) OutboxPublished()      { m.published++ }
func (m *relayMetrics) OutboxFailure()        { m.failures++ }
func (m *relayMetrics) OutboxDeadLettered()   { m.dead++ }
func (*relayMetrics) OutboxLag(time.Duration) {}

func deadlineRelay(store *fakeStore, pub worker.Publisher, metrics worker.RelayMetrics) *worker.Relay {
	return worker.NewRelay(store, pub, testutil.NewFakeClock(t0), worker.RelayConfig{
		Owner: "deadline", BatchSize: 2, Lease: time.Second, RetryBase: time.Second, RetryMax: time.Second,
		PublishTime: 10 * time.Millisecond, FinalizeTime: time.Second, MaxAttempts: 2,
	}, observability.NewLogger(&testutil.SyncBuffer{}, "debug", "deadline"), metrics)
}

type deadlinePublisher struct{ calls int }

func (p *deadlinePublisher) Publish(ctx context.Context, _ app.OutboxMessage) error {
	p.calls++
	<-ctx.Done()
	return ctx.Err()
}

func TestRelayPublishesAndRetries(t *testing.T) {
	ok, bad := uuid.New(), uuid.New()
	store := &fakeStore{markOK: true, failed: map[uuid.UUID]time.Time{}, msgs: []app.OutboxMessage{
		{EventID: ok}, {EventID: bad, Attempts: 2},
	}, hasOldest: true, oldest: t0.Add(-time.Second)}
	logs := &testutil.SyncBuffer{}
	newRelay(store, fakePublisher{fail: map[uuid.UUID]bool{bad: true}}, logs).Tick(context.Background())

	assert.Equal(t, "instance-1", store.claimedOwner)
	assert.Equal(t, []uuid.UUID{ok}, store.published)
	assert.Equal(t, t0.Add(4*time.Second), store.failed[bad], "exponential backoff")
	assert.Contains(t, logs.String(), "outbox publish failed")
}

func TestRelayBackoffIsCapped(t *testing.T) {
	id := uuid.New()
	store := &fakeStore{failed: map[uuid.UUID]time.Time{}, msgs: []app.OutboxMessage{{EventID: id, Attempts: 39}}}
	newRelay(store, fakePublisher{fail: map[uuid.UUID]bool{id: true}}, &testutil.SyncBuffer{}).Tick(context.Background())
	assert.Equal(t, t0.Add(30*time.Second), store.failed[id])
}

func TestRelayFailurePaths(t *testing.T) {
	logs := &testutil.SyncBuffer{}
	newRelay(&fakeStore{claimErr: errBoom}, fakePublisher{}, logs).Tick(context.Background())
	assert.Contains(t, logs.String(), "outbox claim failed")

	id := uuid.New()
	store := &fakeStore{failed: map[uuid.UUID]time.Time{}, msgs: []app.OutboxMessage{{EventID: id}}, markFailErr: errBoom, oldestErr: errBoom}
	newRelay(store, fakePublisher{fail: map[uuid.UUID]bool{id: true}}, logs).Tick(context.Background())
	assert.Contains(t, logs.String(), "failure not recorded; lease expiry will release it")

	lost := &fakeStore{msgs: []app.OutboxMessage{{EventID: uuid.New()}}, markOK: false}
	newRelay(lost, fakePublisher{}, logs).Tick(context.Background())
	assert.Contains(t, logs.String(), "republished with the same eventId")
}

type fakePending struct {
	n   int
	err error
}

func (f fakePending) ResolveDue(context.Context) (int, error) { return f.n, f.err }

func TestPendingResolver(t *testing.T) {
	logs := &testutil.SyncBuffer{}
	logger := observability.NewLogger(logs, "info", "t")
	worker.NewPendingResolver(fakePending{n: 2}, logger).Tick(context.Background())
	assert.Contains(t, logs.String(), "pending references resolved")

	worker.NewPendingResolver(fakePending{err: errBoom}, logger).Tick(context.Background())
	assert.Contains(t, logs.String(), "pending reference batch failed")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	before := logs.String()
	worker.NewPendingResolver(fakePending{err: context.Canceled}, logger).Tick(ctx)
	assert.Equal(t, before, logs.String(), "cancellation during shutdown is not an error")
}

func TestRelayRunsRoundsUntilDrainedOrCancelled(t *testing.T) {
	store := &fakeStore{markOK: true, msgs: []app.OutboxMessage{{EventID: uuid.New()}}}
	newRelay(store, fakePublisher{}, &testutil.SyncBuffer{}).Tick(context.Background())
	assert.Equal(t, 2, store.rounds, "a second round finds nothing left")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cancelled := &fakeStore{markOK: true, msgs: []app.OutboxMessage{{EventID: uuid.New()}}}
	newRelay(cancelled, fakePublisher{}, &testutil.SyncBuffer{}).Tick(ctx)
	assert.Zero(t, cancelled.rounds, "shutdown stops before acquiring another claim")
}

func TestRelayPreservesBatchThroughputWithSingularClaims(t *testing.T) {
	msgs := make([]app.OutboxMessage, 25)
	for i := range msgs {
		msgs[i].EventID = uuid.New()
	}
	store := &fakeStore{markOK: true, msgs: msgs}
	newRelay(store, fakePublisher{}, &testutil.SyncBuffer{}).Tick(context.Background())
	assert.Len(t, store.published, len(msgs), "singular claims still honor the configured batch across rounds")
}

func TestRelayDeadLettersPoisonEvents(t *testing.T) {
	poison, last := uuid.New(), uuid.New()
	store := &fakeStore{failed: map[uuid.UUID]time.Time{}, msgs: []app.OutboxMessage{
		{EventID: poison, Attempts: 49}, {EventID: last, Attempts: 48},
	}}
	logs := &testutil.SyncBuffer{}
	newRelay(store, fakePublisher{fail: map[uuid.UUID]bool{poison: true, last: true}, permanent: true}, logs).Tick(context.Background())
	assert.Equal(t, []uuid.UUID{poison}, store.dead)
	assert.NotContains(t, store.failed, poison, "a dead-lettered event is not rescheduled")
	assert.Contains(t, store.failed, last, "one attempt short of the limit is still retried")
	assert.Contains(t, logs.String(), "dead-lettered after exhausting its attempts")
}

func TestRelayMarkDeadFailureIsLogged(t *testing.T) {
	poison := uuid.New()
	store := &fakeStore{failed: map[uuid.UUID]time.Time{}, msgs: []app.OutboxMessage{{EventID: poison, Attempts: 49}}, markFailErr: errBoom}
	logs := &testutil.SyncBuffer{}
	newRelay(store, fakePublisher{fail: map[uuid.UUID]bool{poison: true}, permanent: true}, logs).Tick(context.Background())
	assert.Equal(t, []uuid.UUID{poison}, store.dead, "MarkDead was attempted")
	assert.Contains(t, logs.String(), "failure not recorded; lease expiry will release it")
}

func TestRelayPublishesWithinPublishTimeDetachedFromShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	first, second := uuid.New(), uuid.New()
	store := &fakeStore{markOK: true, msgs: []app.OutboxMessage{{EventID: first}, {EventID: second}}}
	var seen []uuid.UUID
	pub := fakePublisher{hook: func(pctx context.Context, m app.OutboxMessage) {
		seen = append(seen, m.EventID)
		deadline, ok := pctx.Deadline()
		assert.True(t, ok, "PublishTime bounds the publication")
		assert.WithinDuration(t, time.Now().Add(time.Second), deadline, 500*time.Millisecond)
		cancel() // shutdown while publishing
		assert.NoError(t, pctx.Err(), "the publication in flight is not aborted")
	}}
	newRelay(store, pub, &testutil.SyncBuffer{}).Tick(ctx)
	assert.Equal(t, []uuid.UUID{first}, seen, "the round stops before another singular acquisition")
	assert.Equal(t, []uuid.UUID{first}, store.published)
}

func TestRelayDeadlineFinalizesFailureWithLiveContext(t *testing.T) {
	id := uuid.New()
	store := &fakeStore{failed: map[uuid.UUID]time.Time{}, msgs: []app.OutboxMessage{{EventID: id}}}
	pub := &deadlinePublisher{}
	metrics := &relayMetrics{}

	deadlineRelay(store, pub, metrics).Tick(context.Background())

	require.Equal(t, 1, pub.calls)
	assert.NoError(t, store.failedCtxErr, "finalization has a fresh context after publication times out")
	assert.Equal(t, t0.Add(time.Second), store.failed[id], "the timeout is durably backed off")
	assert.Equal(t, 1, metrics.failures)
}

func TestRelayDeadMetricsRequireDurableMutation(t *testing.T) {
	id := uuid.New()
	store := &fakeStore{
		failed: map[uuid.UUID]time.Time{}, msgs: []app.OutboxMessage{{EventID: id, Attempts: 1}}, markDeadLost: true,
	}
	metrics := &relayMetrics{}

	deadlineRelay(store, fakePublisher{fail: map[uuid.UUID]bool{id: true}, permanent: true}, metrics).Tick(context.Background())

	assert.Equal(t, 0, metrics.failures, "a stale claim does not count a durable failure")
	assert.Equal(t, 0, metrics.dead, "dead-lettering is counted only after MarkDead succeeds")
}

func TestRelayFailureMetricsRequireDurableMutation(t *testing.T) {
	id := uuid.New()
	store := &fakeStore{
		failed: map[uuid.UUID]time.Time{}, msgs: []app.OutboxMessage{{EventID: id}}, markFailLost: true,
	}
	metrics := &relayMetrics{}

	deadlineRelay(store, &deadlinePublisher{}, metrics).Tick(context.Background())

	assert.Zero(t, metrics.failures)
	assert.NotContains(t, store.failed, id)
}

func TestRelayCountsDurableDeadLetter(t *testing.T) {
	id := uuid.New()
	store := &fakeStore{failed: map[uuid.UUID]time.Time{}, msgs: []app.OutboxMessage{{EventID: id, Attempts: 1}}}
	metrics := &relayMetrics{}

	deadlineRelay(store, fakePublisher{fail: map[uuid.UUID]bool{id: true}, permanent: true}, metrics).Tick(context.Background())

	assert.Equal(t, 1, metrics.failures)
	assert.Equal(t, 1, metrics.dead)
}

func TestRelayCancellationDoesNotClaimOrAttemptLaterRecords(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	first, second := uuid.New(), uuid.New()
	store := &fakeStore{markOK: true, msgs: []app.OutboxMessage{{EventID: first}, {EventID: second}}}
	pub := fakePublisher{hook: func(context.Context, app.OutboxMessage) { cancel() }}

	newRelay(store, pub, &testutil.SyncBuffer{}).Tick(ctx)

	assert.Equal(t, []uuid.UUID{first}, store.claimed, "cancellation is checked before the next claim")
	assert.Equal(t, 1, store.attempts[first])
	assert.Zero(t, store.attempts[second])
}

func TestRelayLostAttemptNeverPublishes(t *testing.T) {
	tests := []struct {
		name  string
		store *fakeStore
	}{
		{name: "lost claim", store: &fakeStore{startLost: true}},
		{name: "store error", store: &fakeStore{startErr: errBoom}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id := uuid.New()
			tt.store.msgs = []app.OutboxMessage{{EventID: id}}
			pub := &deadlinePublisher{}

			deadlineRelay(tt.store, pub, &relayMetrics{}).Tick(context.Background())

			assert.Zero(t, pub.calls, "publication cannot begin after StartAttempt loses ownership")
			assert.Zero(t, tt.store.attempts[id])
		})
	}
}

type countingPublisher struct{ calls int }

func (p *countingPublisher) Publish(context.Context, app.OutboxMessage) error {
	p.calls++
	return nil
}

func TestRelayRenewsOnlyALiveClaimImmediatelyBeforePublish(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		delay       time.Duration
		wantPublish int
	}{
		{name: "delayed within original lease", delay: 750 * time.Millisecond, wantPublish: 1},
		{name: "delay reaches original lease", delay: time.Second, wantPublish: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			clock := testutil.NewFakeClock(t0)
			id := uuid.New()
			store := &fakeStore{markOK: true, msgs: []app.OutboxMessage{{EventID: id}}, claimHook: func() {
				clock.Advance(tt.delay)
			}}
			publisher := &countingPublisher{}
			relay := worker.NewRelay(store, publisher, clock, worker.RelayConfig{
				Owner: "lease-test", BatchSize: 1, Lease: time.Second, RetryBase: time.Second,
				RetryMax: time.Second, PublishTime: 100 * time.Millisecond, FinalizeTime: 100 * time.Millisecond,
				MaxAttempts: 2,
			}, observability.NewLogger(&testutil.SyncBuffer{}, "debug", "lease-test"), testutil.NewMetrics())

			relay.Tick(context.Background())

			assert.Equal(t, tt.wantPublish, publisher.calls)
			assert.Equal(t, t0.Add(tt.delay), store.startAt)
			if tt.wantPublish == 0 {
				assert.Zero(t, store.attempts[id], "an expired claim consumes no attempt")
				assert.Empty(t, store.published, "an expired claim cannot reach durable confirmation")
				assert.Equal(t, t0.Add(time.Second), store.claimUntil[id], "an expired claim is not revived")
				return
			}
			assert.Equal(t, t0.Add(tt.delay+time.Second), store.renewedUntil)
			assert.Equal(t, 1, store.attempts[id])
			assert.Equal(t, []uuid.UUID{id}, store.published, "the renewed claim publishes and confirms safely")
		})
	}
}
