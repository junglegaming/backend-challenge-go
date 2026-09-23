//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Pantani/backend-challenge-go/internal/adapter/postgres"
	sqsadapter "github.com/Pantani/backend-challenge-go/internal/adapter/sqs"
	"github.com/Pantani/backend-challenge-go/internal/app"
	"github.com/Pantani/backend-challenge-go/internal/domain/wager"
	"github.com/Pantani/backend-challenge-go/internal/observability"
	"github.com/Pantani/backend-challenge-go/internal/testutil"
	"github.com/Pantani/backend-challenge-go/internal/worker"
	"github.com/Pantani/backend-challenge-go/test/testenv"
)

func newRelay(t *testing.T, owner string, store app.OutboxStore, pub worker.Publisher) *worker.Relay {
	t.Helper()
	return newRelayWithAttempts(t, owner, store, pub, 1000)
}

func newRelayWithAttempts(t *testing.T, owner string, store app.OutboxStore, pub worker.Publisher, maxAttempts int) *worker.Relay {
	t.Helper()
	return worker.NewRelay(store, pub, app.SystemClock{}, worker.RelayConfig{
		Owner: owner, BatchSize: 500, Lease: time.Second, RetryBase: 10 * time.Millisecond, RetryMax: 20 * time.Millisecond,
		PublishTime: 5 * time.Second, FinalizeTime: time.Second, MaxAttempts: maxAttempts,
	}, observability.NewLogger(io.Discard, "error", owner), testutil.NewMetrics())
}

// unpublished counts outbox rows of a wallet not yet published.
func unpublished(walletID uuid.UUID) (int, error) {
	var n int
	err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM outbox_events WHERE partition_key = $1 AND published_at IS NULL`, walletID.String()).Scan(&n)
	return n, err
}

// drain reads every message currently in a queue.
func drain(t *testing.T, api sqsadapter.API, url string) []string {
	t.Helper()
	var bodies []string
	for range 20 {
		out, err := api.ReceiveMessage(context.Background(), &awssqs.ReceiveMessageInput{QueueUrl: aws.String(url), MaxNumberOfMessages: 10, WaitTimeSeconds: 1})
		require.NoError(t, err)
		if len(out.Messages) == 0 && len(bodies) > 0 {
			return bodies
		}
		for _, m := range out.Messages {
			bodies = append(bodies, aws.ToString(m.Body))
			_, _ = api.DeleteMessage(context.Background(), &awssqs.DeleteMessageInput{QueueUrl: aws.String(url), ReceiptHandle: m.ReceiptHandle})
		}
	}
	return bodies
}

func eventIDs(t *testing.T, bodies []string) map[string]int {
	t.Helper()
	ids := map[string]int{}
	for _, b := range bodies {
		var env struct {
			EventID string `json:"eventId"`
		}
		require.NoError(t, json.Unmarshal([]byte(b), &env))
		ids[env.EventID]++
	}
	return ids
}

// Not parallel: relays claim the whole (shared) outbox.
func TestTwoPublishersShareTheOutbox(t *testing.T) {
	s := newServices(t, defaultPolicy)
	api, q, _ := provisionQueues(t, 3)
	w := s.openWallet(t, "100.00")
	for i := range 5 {
		s.submit(t, w, fmt.Sprintf("pub-%d", i), "BET", "1.00", "")
	}
	pending, err := unpublished(w.ID())
	require.NoError(t, err)
	require.Equal(t, 12, pending, "committed events wait for a publisher (commit happened, publication did not)")

	pub := sqsadapter.NewPublisher(api, q.Events)
	store := postgres.NewOutboxStore(pool)
	relays := []*worker.Relay{newRelay(t, "relay-a", store, pub), newRelay(t, "relay-b", store, pub)}
	_, err = testenv.Parallel(2, func(i int) (struct{}, error) {
		relays[i].Tick(context.Background())
		return struct{}{}, nil
	})
	require.NoError(t, err)
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		relays[0].Tick(context.Background())
		pending, err := unpublished(w.ID())
		if !assert.NoError(collect, err) {
			return
		}
		assert.Zero(collect, pending)
	}, 10*time.Second, 100*time.Millisecond)

	ids := eventIDs(t, drain(t, api, q.Events))
	var mine int
	require.NoError(t, pool.QueryRow(context.Background(), `SELECT count(*) FROM outbox_events WHERE partition_key = $1`, w.ID().String()).Scan(&mine))
	assert.GreaterOrEqual(t, len(ids), mine, "every event of the wallet reached the queue")
	for id, n := range ids {
		assert.Equal(t, 1, n, "event %s delivered once (FIFO deduplication by eventId)", id)
	}
}

// forgetfulStore publishes but crashes before confirming.
type forgetfulStore struct{ app.OutboxStore }

func (forgetfulStore) MarkPublished(context.Context, uuid.UUID, uuid.UUID, time.Time) (bool, error) {
	return false, fmt.Errorf("crash before confirming")
}

// recordingPublisher remembers every published event id.
type recordingPublisher struct {
	mu  sync.Mutex
	ids []uuid.UUID
}

type observedClaim struct {
	eventID uuid.UUID
	claimID uuid.UUID
}

type observedOutboxStore struct {
	app.OutboxStore
	mu     sync.Mutex
	claims []observedClaim
}

func (s *observedOutboxStore) Claim(
	ctx context.Context, owner string, claimID uuid.UUID, now time.Time, lease time.Duration,
) (app.OutboxMessage, bool, error) {
	m, ok, err := s.OutboxStore.Claim(ctx, owner, claimID, now, lease)
	if ok {
		s.mu.Lock()
		s.claims = append(s.claims, observedClaim{eventID: m.EventID, claimID: claimID})
		s.mu.Unlock()
	}
	return m, ok, err
}

func (s *observedOutboxStore) snapshotClaims() []observedClaim {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]observedClaim(nil), s.claims...)
}

type gatedFailureStore struct {
	*observedOutboxStore
	entered   chan struct{}
	release   *releaseGate
	delegated atomic.Bool
	result    chan markFailedResult
}

type markFailedResult struct {
	ok          bool
	err         error
	contextLive bool
}

func (s *gatedFailureStore) MarkFailed(
	ctx context.Context, eventID, claimID uuid.UUID, next time.Time, cause string,
) (bool, error) {
	if err := s.waitForRelease(ctx); err != nil {
		return false, err
	}
	s.delegated.Store(true)
	contextLive := ctx.Err() == nil
	ok, err := s.OutboxStore.MarkFailed(ctx, eventID, claimID, next, cause)
	s.recordResult(ctx, markFailedResult{ok: ok, err: err, contextLive: contextLive})
	return ok, err
}

func (s *gatedFailureStore) waitForRelease(ctx context.Context) error {
	select {
	case s.entered <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-s.release.ch:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func (s *gatedFailureStore) recordResult(ctx context.Context, result markFailedResult) {
	select {
	case s.result <- result:
	case <-ctx.Done():
	}
}

type blockingPublisher struct{}

func (blockingPublisher) Publish(ctx context.Context, _ app.OutboxMessage) error {
	<-ctx.Done()
	return ctx.Err()
}

type partitionPublisher struct {
	failID    uuid.UUID
	published []uuid.UUID
}

func (p *partitionPublisher) Publish(_ context.Context, m app.OutboxMessage) error {
	if m.EventID == p.failID {
		return errors.New("second relay failure")
	}
	p.published = append(p.published, m.EventID)
	return nil
}

type relayCounters struct {
	published atomic.Int64
	failures  atomic.Int64
	dead      atomic.Int64
}

func (m *relayCounters) OutboxPublished()      { m.published.Add(1) }
func (m *relayCounters) OutboxFailure()        { m.failures.Add(1) }
func (m *relayCounters) OutboxDeadLettered()   { m.dead.Add(1) }
func (*relayCounters) OutboxLag(time.Duration) {}

func relayForDeadlineTest(
	store app.OutboxStore, pub worker.Publisher, metrics worker.RelayMetrics, batch int,
) *worker.Relay {
	// The deliberately short lease forces reacquisition while the first relay
	// is gated. Production config rejects PublishTime + FinalizeTime > Lease.
	return worker.NewRelay(store, pub, app.SystemClock{}, worker.RelayConfig{
		Owner: "shared-owner", BatchSize: batch, Lease: 80 * time.Millisecond,
		RetryBase: time.Second, RetryMax: time.Second, PublishTime: 20 * time.Millisecond,
		FinalizeTime: 500 * time.Millisecond, MaxAttempts: 5,
	}, observability.NewLogger(io.Discard, "error", "deadline-it"), metrics)
}

func waitForRelaySignal(t *testing.T, signal <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatal(message)
	}
}

func registerRelayCleanup(
	t *testing.T, cancel context.CancelFunc, store *gatedFailureStore, done <-chan struct{},
) {
	t.Helper()
	t.Cleanup(func() {
		assert.NoError(t, stopGatedRelay(cancel, store, done))
	})
}

func stopGatedRelay(cancel context.CancelFunc, store *gatedFailureStore, done <-chan struct{}) error {
	cancel()
	store.release.release()
	select {
	case <-done:
		return nil
	case <-time.After(time.Second):
		return errors.New("first relay did not stop within cleanup timeout")
	}
}

func TestStopGatedRelayCancelsBeforeGateAndJoinsWithoutResultDrain(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := &gatedFailureStore{release: newReleaseGate(), result: make(chan markFailedResult, 1)}
	done := make(chan struct{})
	go func() {
		defer close(done)
		<-store.release.ch
		store.result <- markFailedResult{err: ctx.Err()}
	}()

	require.NoError(t, stopGatedRelay(cancel, store, done))
	result := <-store.result
	assert.ErrorIs(t, result.err, context.Canceled, "the parent is canceled before the gate opens")
}

func waitForClaimExpiration(t *testing.T, eventID uuid.UUID) {
	t.Helper()
	var lockedUntil time.Time
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT locked_until FROM outbox_events WHERE event_id = $1`, eventID).Scan(&lockedUntil))
	if wait := time.Until(lockedUntil.Add(20 * time.Millisecond)); wait > 0 {
		time.Sleep(wait)
	}
}

func assertStaleFailureResult(t *testing.T, store *gatedFailureStore) {
	t.Helper()
	assert.True(t, store.delegated.Load(), "the stale mutation reached the real fenced store")
	select {
	case result := <-store.result:
		assert.True(t, result.contextLive, "terminal mutation gets a live context within the shared finalization budget")
		assert.False(t, result.ok, "the stale fencing token is rejected")
		assert.NoError(t, result.err, "losing a claim is not a store error")
	default:
		t.Error("stale MarkFailed result was not captured")
	}
}

func (p *recordingPublisher) Publish(_ context.Context, m app.OutboxMessage) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ids = append(p.ids, m.EventID)
	return nil
}

func (p *recordingPublisher) count(id uuid.UUID) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, x := range p.ids {
		n += boolToInt(x == id)
	}
	return n
}

// Not parallel: relays claim the whole (shared) outbox.
func TestCrashBetweenPublishAndConfirmIsRecovered(t *testing.T) {
	s := newServices(t, defaultPolicy)
	w := s.openWallet(t, "10.00")
	var eventID uuid.UUID
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT event_id FROM outbox_events WHERE partition_key = $1 ORDER BY seq LIMIT 1`, w.ID().String()).Scan(&eventID))

	pub := &recordingPublisher{}
	store := postgres.NewOutboxStore(pool)
	newRelay(t, "crashing", forgetfulStore{store}, pub).Tick(context.Background())
	require.GreaterOrEqual(t, pub.count(eventID), 1)
	pending, err := unpublished(w.ID())
	require.NoError(t, err)
	require.Equal(t, 2, pending, "not confirmed")

	// Another instance takes over once the lease expires, keeping the eventId.
	survivor := newRelay(t, "survivor", store, pub)
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		survivor.Tick(context.Background())
		pending, err := unpublished(w.ID())
		if !assert.NoError(collect, err) {
			return
		}
		assert.Zero(collect, pending)
	}, 10*time.Second, 200*time.Millisecond)
	assert.GreaterOrEqual(t, pub.count(eventID), 2, "republished with the same eventId")
}

// failingPublisher fails the first attempts to exercise the backoff.
type failingPublisher struct {
	mu    sync.Mutex
	fails int
}

func (p *failingPublisher) Publish(context.Context, app.OutboxMessage) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.fails > 0 {
		p.fails--
		return fmt.Errorf("broker down")
	}
	return nil
}

// Not parallel: relays claim the whole (shared) outbox.
func TestPublicationRetriesWithBackoff(t *testing.T) {
	s := newServices(t, defaultPolicy)
	w := s.openWallet(t, "10.00")
	relay := newRelay(t, "retry", postgres.NewOutboxStore(pool), &failingPublisher{fails: 1000})
	relay.Tick(context.Background())
	var attempts int
	var lastError string
	require.NoError(t, pool.QueryRow(context.Background(), `SELECT max(attempts), max(last_error) FROM outbox_events
		WHERE partition_key = $1`, w.ID().String()).Scan(&attempts, &lastError))
	assert.GreaterOrEqual(t, attempts, 1)
	assert.Equal(t, "broker down", lastError)
	pending, err := unpublished(w.ID())
	require.NoError(t, err)
	assert.Equal(t, 2, pending)

	ok := newRelay(t, "retry", postgres.NewOutboxStore(pool), &failingPublisher{})
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		ok.Tick(context.Background())
		pending, err := unpublished(w.ID())
		if !assert.NoError(collect, err) {
			return
		}
		assert.Zero(collect, pending)
	}, 10*time.Second, 100*time.Millisecond)
}

// localSenders trusts the LocalStack account id, the SenderId of every
// message sent to LocalStack.
var localSenders = sqsadapter.SenderPolicy{"000000000000": {"*"}}

// consumerFor builds a consumer on the test queues.
// itMaxReceiveCount is the consumer limit of consumerFor.
const itMaxReceiveCount = 2

func consumerFor(api sqsadapter.API, q sqsadapter.Queues, svc sqsadapter.Processor) *sqsadapter.Consumer {
	return consumerWithLimit(api, q, svc, itMaxReceiveCount)
}

func consumerWithLimit(api sqsadapter.API, q sqsadapter.Queues, svc sqsadapter.Processor, maxReceiveCount int) *sqsadapter.Consumer {
	return sqsadapter.NewConsumer(api, sqsadapter.ConsumerConfig{
		Name: "it-consumer", QueueURL: q.Input, DLQURL: q.DLQ, MaxMessages: 10, WaitTime: time.Second,
		VisibilityTimeout: 2 * time.Second, ProcessTimeout: time.Second, RetryBase: time.Second, RetryMax: time.Second,
		MaxReceiveCount: maxReceiveCount, Senders: localSenders,
	}, svc, observability.NewLogger(io.Discard, "error", "c"), testutil.NewMetrics())
}

type releaseGate struct {
	once sync.Once
	ch   chan struct{}
}

func newReleaseGate() *releaseGate { return &releaseGate{ch: make(chan struct{})} }

func (g *releaseGate) release() { g.once.Do(func() { close(g.ch) }) }

type processObservation struct {
	messageID string
	duration  time.Duration
	err       error
}

type gatedBatchProcessor struct {
	mu       sync.Mutex
	calls    int
	gates    []*releaseGate
	started  chan string
	finished chan processObservation
}

func newGatedBatchProcessor(gates ...*releaseGate) *gatedBatchProcessor {
	return &gatedBatchProcessor{
		gates: gates, started: make(chan string, len(gates)), finished: make(chan processObservation, len(gates)),
	}
}

func (p *gatedBatchProcessor) ConsumeMessage(ctx context.Context, msg app.InboundMessage) (app.ConsumeResult, error) {
	p.mu.Lock()
	index := p.calls
	p.calls++
	p.mu.Unlock()
	if index >= len(p.gates) {
		return app.ConsumeResult{}, fmt.Errorf("unexpected batch message %q", msg.MessageID)
	}
	started := time.Now()
	p.started <- msg.MessageID
	var err error
	select {
	case <-ctx.Done():
		err = ctx.Err()
	case <-p.gates[index].ch:
	}
	p.finished <- processObservation{messageID: msg.MessageID, duration: time.Since(started), err: err}
	return app.ConsumeResult{Duplicate: true}, err
}

type callProbe struct {
	mu    sync.Mutex
	calls []string
}

func (p *callProbe) ConsumeMessage(_ context.Context, msg app.InboundMessage) (app.ConsumeResult, error) {
	p.mu.Lock()
	p.calls = append(p.calls, msg.MessageID)
	p.mu.Unlock()
	return app.ConsumeResult{Duplicate: true}, nil
}

func (p *callProbe) messageIDs() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.calls)
}

type prefetchedAPI struct {
	sqsadapter.API
	mu     sync.Mutex
	batch  *awssqs.ReceiveMessageOutput
	served bool
}

func (a *prefetchedAPI) ReceiveMessage(ctx context.Context, _ *awssqs.ReceiveMessageInput, _ ...func(*awssqs.Options)) (*awssqs.ReceiveMessageOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.served {
		return nil, fmt.Errorf("prefetched batch already served")
	}
	a.served = true
	return a.batch, nil
}

type receiveObservation struct {
	messages       int
	completedAfter time.Duration
	err            error
}

type observedReceiveAPI struct {
	sqsadapter.API
	startedOnce sync.Once
	started     chan struct{}
	results     chan receiveObservation
	startedAt   time.Time
}

func (a *observedReceiveAPI) ReceiveMessage(ctx context.Context, in *awssqs.ReceiveMessageInput, opts ...func(*awssqs.Options)) (*awssqs.ReceiveMessageOutput, error) {
	a.startedOnce.Do(func() { close(a.started) })
	out, err := a.API.ReceiveMessage(ctx, in, opts...)
	observation := receiveObservation{completedAfter: time.Since(a.startedAt), err: err}
	if out != nil {
		observation.messages = len(out.Messages)
	}
	a.results <- observation
	return out, err
}

func receiveFullBatch(ctx context.Context, api sqsadapter.API, queue string, visibility int32, size int32) (*awssqs.ReceiveMessageOutput, time.Time, error) {
	for {
		out, err := api.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
			QueueUrl: aws.String(queue), MaxNumberOfMessages: size, WaitTimeSeconds: 1, VisibilityTimeout: visibility,
			MessageSystemAttributeNames: []types.MessageSystemAttributeName{
				types.MessageSystemAttributeNameApproximateReceiveCount, types.MessageSystemAttributeNameSenderId,
				types.MessageSystemAttributeNameMessageGroupId, types.MessageSystemAttributeNameMessageDeduplicationId,
			},
		})
		if err != nil {
			return nil, time.Time{}, fmt.Errorf("receive exact batch: %w", err)
		}
		if int32(len(out.Messages)) == size {
			return out, time.Now(), nil
		}
		if err := releaseBatch(ctx, api, queue, out.Messages); err != nil {
			return nil, time.Time{}, err
		}
	}
}

func releaseBatch(ctx context.Context, api sqsadapter.API, queue string, messages []types.Message) error {
	for _, message := range messages {
		_, err := api.ChangeMessageVisibility(ctx, &awssqs.ChangeMessageVisibilityInput{
			QueueUrl: aws.String(queue), ReceiptHandle: message.ReceiptHandle, VisibilityTimeout: 0,
		})
		if err != nil {
			return fmt.Errorf("release partial batch: %w", err)
		}
	}
	return nil
}

func await[T any](t *testing.T, ch <-chan T, timeout time.Duration, message string) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(timeout):
		t.Fatal(message)
		var zero T
		return zero
	}
}

func runAsync(run func()) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		run()
		close(done)
	}()
	return done
}

type receiveSummary struct {
	successful           int
	successfulDuringHead int
	successfulAfterOld   int
	messages             int
	canceled             int
}

func (s *receiveSummary) add(t *testing.T, observation receiveObservation, headRelease, oldVisibility, canceledAfter time.Duration) {
	t.Helper()
	if observation.err == nil {
		s.successful++
		s.messages += observation.messages
		if observation.completedAfter < headRelease {
			s.successfulDuringHead++
		}
		if observation.completedAfter > oldVisibility && observation.completedAfter < canceledAfter {
			s.successfulAfterOld++
		}
		return
	}
	if errors.Is(observation.err, context.Canceled) {
		s.canceled++
		return
	}
	assert.NoError(t, observation.err)
}

func summarizeReceives(t *testing.T, observations <-chan receiveObservation, headRelease, oldVisibility, canceledAfter time.Duration) receiveSummary {
	t.Helper()
	var summary receiveSummary
	for {
		select {
		case observation := <-observations:
			summary.add(t, observation, headRelease, oldVisibility, canceledAfter)
		default:
			return summary
		}
	}
}

func TestReceiveSummaryIgnoresCancellation(t *testing.T) {
	t.Parallel()
	observations := make(chan receiveObservation, 1)
	observations <- receiveObservation{err: context.Canceled}
	summary := summarizeReceives(t, observations, time.Second, 2*time.Second, 3*time.Second)
	assert.Zero(t, summary.successful)
	assert.Zero(t, summary.messages)
	assert.Equal(t, 1, summary.canceled)
}

func waitUntil(deadline time.Time) {
	if delay := time.Until(deadline); delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		<-timer.C
	}
}

const (
	slowBatchOldVisibility    = 3 * time.Second
	slowBatchVisibility       = 6 * time.Second
	slowBatchProcessBudget    = 2900 * time.Millisecond
	slowBatchAckBudget        = 50 * time.Millisecond
	slowBatchHeadReleaseAfter = 2200 * time.Millisecond
	slowBatchProbeUntilAfter  = 4600 * time.Millisecond
	slowBatchMinHeadroom      = 400 * time.Millisecond
)

func TestSlowBatchTimingBudget(t *testing.T) {
	t.Parallel()
	perMessage := slowBatchProcessBudget + slowBatchAckBudget
	tailHeld := slowBatchProbeUntilAfter - slowBatchHeadReleaseAfter
	assert.Less(t, perMessage, slowBatchOldVisibility)
	assert.Greater(t, slowBatchVisibility, 2*perMessage)
	assert.Greater(t, slowBatchProbeUntilAfter, slowBatchOldVisibility)
	assert.GreaterOrEqual(t, slowBatchProcessBudget-slowBatchHeadReleaseAfter, slowBatchMinHeadroom)
	assert.GreaterOrEqual(t, slowBatchProcessBudget-tailHeld, slowBatchMinHeadroom)
}

func TestVisibilityProtectsSlowBatchFromSecondConsumer(t *testing.T) {
	t.Parallel()
	// The former per-message rule accepts 2.95s < 3s, while the complete
	// two-message batch requires a visibility window strictly above 5.9s.
	// The gates retain at least 400ms of processing headroom per item.
	s := newServices(t, defaultPolicy)
	api, q, _ := provisionQueues(t, 20)
	_, err := api.SetQueueAttributes(context.Background(), &awssqs.SetQueueAttributesInput{
		QueueUrl: aws.String(q.Input),
		Attributes: map[string]string{
			string(types.QueueAttributeNameVisibilityTimeout): strconv.Itoa(int(slowBatchVisibility.Seconds())),
		},
	})
	require.NoError(t, err)
	w := s.openWallet(t, "100.00")
	expectedIDs := []string{s.prefix + "slow-batch-0", s.prefix + "slow-batch-1"}
	for i, messageID := range expectedIDs {
		sendMessage(t, api, q, messageID, s.input(w, "provider-a", fmt.Sprintf("slow-batch-%d", i), "BET", "1.00", ""))
	}

	receiveCtx, stopReceive := context.WithTimeout(context.Background(), 10*time.Second)
	defer stopReceive()
	batch, receivedAt, err := receiveFullBatch(receiveCtx, api, q.Input, int32(slowBatchVisibility.Seconds()), 2)
	require.NoError(t, err)
	require.Len(t, batch.Messages, 2)

	headGate, tailGate := newReleaseGate(), newReleaseGate()
	t.Cleanup(headGate.release)
	t.Cleanup(tailGate.release)
	processorA := newGatedBatchProcessor(headGate, tailGate)
	consumerConfig := sqsadapter.ConsumerConfig{
		Name: "slow-batch", QueueURL: q.Input, DLQURL: q.DLQ, MaxMessages: 2, WaitTime: 3 * time.Second,
		VisibilityTimeout: slowBatchVisibility, ProcessTimeout: slowBatchProcessBudget, AckTimeout: slowBatchAckBudget,
		RetryBase: time.Second, RetryMax: time.Second, MaxReceiveCount: 20, Senders: localSenders,
	}
	logger := observability.NewLogger(io.Discard, "error", "slow-batch")
	consumerA := sqsadapter.NewConsumer(&prefetchedAPI{API: api, batch: batch}, consumerConfig, processorA, logger, testutil.NewMetrics())
	ctxA, cancelA := context.WithCancel(context.Background())
	t.Cleanup(cancelA)
	doneA := runAsync(func() { consumerA.PollOnce(ctxA) })
	assert.Equal(t, expectedIDs[0], await(t, processorA.started, time.Second, "consumer A did not start the batch head"))

	processorB := &callProbe{}
	observedB := &observedReceiveAPI{
		API: api, started: make(chan struct{}), results: make(chan receiveObservation, 128), startedAt: receivedAt,
	}
	consumerBConfig := consumerConfig
	consumerBConfig.WaitTime = 0
	consumerBConfig.RetryBase = 50 * time.Millisecond
	consumerB := sqsadapter.NewConsumer(observedB, consumerBConfig, processorB, logger, testutil.NewMetrics())
	ctxB, cancelB := context.WithCancel(context.Background())
	t.Cleanup(cancelB)
	doneB := runAsync(func() { consumerB.Run(ctxB) })
	await(t, observedB.started, time.Second, "consumer B did not start receiving while A was blocked")

	waitUntil(receivedAt.Add(slowBatchHeadReleaseAfter))
	headGate.release()
	head := await(t, processorA.finished, time.Second, "consumer A did not finish the batch head")
	assert.NoError(t, head.err)
	assert.Equal(t, expectedIDs[0], head.messageID)
	assert.GreaterOrEqual(t, slowBatchProcessBudget-head.duration, slowBatchMinHeadroom)
	assert.Equal(t, expectedIDs[1], await(t, processorA.started, time.Second, "consumer A did not start the batch tail"))

	waitUntil(receivedAt.Add(slowBatchProbeUntilAfter))
	canceledAfter := time.Since(receivedAt)
	cancelB()
	tailGate.release()
	await(t, doneB, time.Second, "consumer B did not stop after cancellation")
	receives := summarizeReceives(t, observedB.results, slowBatchHeadReleaseAfter, slowBatchOldVisibility, canceledAfter)

	tail := await(t, processorA.finished, time.Second, "consumer A did not finish the batch tail")
	await(t, doneA, time.Second, "consumer A did not finish the exact batch")
	assert.NoError(t, tail.err)
	assert.Equal(t, expectedIDs[1], tail.messageID)
	assert.GreaterOrEqual(t, slowBatchProcessBudget-tail.duration, slowBatchMinHeadroom)
	assert.Greater(t, time.Since(receivedAt), slowBatchOldVisibility)
	assert.GreaterOrEqual(t, receives.successful, 2, "consumer B must complete successful polls while A owns the batch")
	assert.Positive(t, receives.successfulDuringHead, "consumer B must poll successfully while A holds the head")
	assert.Positive(t, receives.successfulAfterOld, "consumer B must poll successfully after the old visibility boundary")
	assert.Zero(t, receives.messages, "consumer B must receive no message during A's protected batch")
	assert.Empty(t, processorB.messageIDs(), "consumer B must receive no message during A's protected batch")
}

func sendMessage(t *testing.T, api sqsadapter.API, q sqsadapter.Queues, messageID string, in app.SubmitInput) {
	t.Helper()
	require.NoError(t, testenv.SendMessage(context.Background(), api, q.Input, testenv.Envelope(messageID, in)))
}

func sendRaw(t *testing.T, api sqsadapter.API, q sqsadapter.Queues, dedup, body, group string) {
	t.Helper()
	_, err := api.SendMessage(context.Background(), &awssqs.SendMessageInput{
		QueueUrl: aws.String(q.Input), MessageBody: aws.String(body),
		MessageGroupId: aws.String(group), MessageDeduplicationId: aws.String(dedup),
	})
	require.NoError(t, err)
}

// noDeleteAPI simulates a consumer crash after the commit and before the
// message is removed from the queue.
type noDeleteAPI struct{ sqsadapter.API }

func (noDeleteAPI) DeleteMessage(context.Context, *awssqs.DeleteMessageInput, ...func(*awssqs.Options)) (*awssqs.DeleteMessageOutput, error) {
	return nil, fmt.Errorf("process killed before DeleteMessage")
}

func TestConsumerCrashAfterCommitIsRedeliveredAndDeduplicated(t *testing.T) {
	t.Parallel()
	s := newServices(t, defaultPolicy)
	api, q, _ := provisionQueues(t, 5)
	w := s.openWallet(t, "100.00")
	in := s.input(w, "provider-a", "sqs-bet", "BET", "10.00", "")
	sendMessage(t, api, q, s.prefix+"msg-1", in)

	consumerFor(noDeleteAPI{api}, q, s.wagers).PollOnce(context.Background())
	assert.Equal(t, "90.00", s.balance(t, w), "committed before the crash")

	// After the visibility timeout the message comes back to another instance.
	other := newServices(t, defaultPolicy)
	c := consumerFor(api, q, other.wagers)
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		c.PollOnce(context.Background())
		depth, err := queueDepth(api, q.Input)
		if !assert.NoError(collect, err) {
			return
		}
		assert.Zero(collect, depth)
	}, 15*time.Second, 100*time.Millisecond)
	assert.Equal(t, "90.00", s.balance(t, w), "the redelivery did not debit again")
	assert.Equal(t, 1, s.debits(t, w))

	var processed bool
	require.NoError(t, pool.QueryRow(context.Background(), `SELECT processed_at IS NOT NULL FROM inbox_messages
		WHERE consumer_name = 'it-consumer' AND message_id = $1`, s.prefix+"msg-1").Scan(&processed))
	assert.True(t, processed)
}

func queueDepth(api sqsadapter.API, url string) (int, error) {
	return queueDepthContext(context.Background(), api, url)
}

func queueDepthContext(ctx context.Context, api sqsadapter.API, url string) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	out, err := api.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{QueueUrl: aws.String(url),
		AttributeNames: []types.QueueAttributeName{"ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"}})
	if err != nil {
		return 0, err
	}
	visible, err := strconv.Atoi(out.Attributes["ApproximateNumberOfMessages"])
	if err != nil {
		return 0, fmt.Errorf("queue visible count: %w", err)
	}
	hidden, err := strconv.Atoi(out.Attributes["ApproximateNumberOfMessagesNotVisible"])
	return visible + hidden, err
}

func TestInvalidMessagesGoToTheDLQ(t *testing.T) {
	t.Parallel()
	s := newServices(t, defaultPolicy)
	api, q, _ := provisionQueues(t, 5)
	w := s.openWallet(t, "100.00")
	in := s.input(w, "provider-a", "invalid-sqs", "BET", "10.00", "")
	tests := []struct {
		name  string
		env   sqsadapter.Envelope
		group string
		dedup string
	}{
		{
			name: "invalid envelope metadata", env: testenv.Envelope("metadata-"+uuid.NewString(), in),
			group: w.ID().String(),
		},
		{
			name: "message group differs from wallet", env: testenv.Envelope("group-"+uuid.NewString(), in),
			group: uuid.NewString(),
		},
		{
			name: "deduplication id differs from message id", env: testenv.Envelope("dedup-"+uuid.NewString(), in),
			group: w.ID().String(), dedup: uuid.NewString(),
		},
	}
	tests[0].env.OccurredAt = "yesterday"
	tests[0].dedup = tests[0].env.MessageID
	tests[1].dedup = tests[1].env.MessageID
	for _, tt := range tests {
		body, err := sqsadapter.EncodeMessage(tt.env)
		require.NoError(t, err)
		sendRaw(t, api, q, tt.dedup, body, tt.group)
	}
	c := consumerFor(api, q, s.wagers)
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		c.PollOnce(context.Background())
		depth, err := queueDepth(api, q.DLQ)
		if !assert.NoError(collect, err) {
			return
		}
		assert.Equal(collect, len(tests), depth)
	}, 15*time.Second, 100*time.Millisecond)
	dead := drain(t, api, q.DLQ)
	require.Len(t, dead, len(tests))
	depth, err := queueDepth(api, q.Input)
	require.NoError(t, err)
	assert.Zero(t, depth)
	assert.Equal(t, "100.00", s.balance(t, w), "invalid messages have no financial effect")
}

// flakyProcessor always fails transiently (e.g. PostgreSQL unavailable).
type flakyProcessor struct{}

func (flakyProcessor) ConsumeMessage(context.Context, app.InboundMessage) (app.ConsumeResult, error) {
	return app.ConsumeResult{}, app.ErrUnavailable
}

// awaitDLQDepth polls c until the DLQ holds want messages.
func awaitDLQDepth(t *testing.T, api sqsadapter.API, q sqsadapter.Queues, c *sqsadapter.Consumer, want int, msg string) {
	t.Helper()
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		c.PollOnce(context.Background())
		depth, err := queueDepth(api, q.DLQ)
		if !assert.NoError(collect, err) {
			return
		}
		assert.Equal(collect, want, depth)
	}, 30*time.Second, 200*time.Millisecond, msg)
}

// receiveDead removes one DLQ message and returns its body and failureReason.
func receiveDead(t *testing.T, api sqsadapter.API, q sqsadapter.Queues) (body, reason string) {
	t.Helper()
	out, err := api.ReceiveMessage(context.Background(), &awssqs.ReceiveMessageInput{QueueUrl: aws.String(q.DLQ),
		MaxNumberOfMessages: 10, WaitTimeSeconds: 1, MessageAttributeNames: []string{"All"}})
	require.NoError(t, err)
	require.Len(t, out.Messages, 1)
	m := out.Messages[0]
	_, err = api.DeleteMessage(context.Background(), &awssqs.DeleteMessageInput{QueueUrl: aws.String(q.DLQ), ReceiptHandle: m.ReceiptHandle})
	require.NoError(t, err)
	return aws.ToString(m.Body), aws.ToString(m.MessageAttributes["failureReason"].StringValue)
}

func TestTransientFailuresAreRetriedThenDeadLetteredWithTheirReason(t *testing.T) {
	t.Parallel()
	s := newServices(t, defaultPolicy)
	api, q, _ := provisionQueues(t, sqsadapter.RedriveMaxReceiveCount(itMaxReceiveCount))
	w := s.openWallet(t, "100.00")
	sendMessage(t, api, q, s.prefix+"flaky", s.input(w, "provider-a", "flaky", "BET", "1.00", ""))

	awaitDLQDepth(t, api, q, consumerFor(api, q, flakyProcessor{}), 1,
		"after SQS_MAX_RECEIVE_COUNT receives the consumer dead-letters it")
	assert.Equal(t, "100.00", s.balance(t, w))
	dead, reason := receiveDead(t, api, q)
	assert.Contains(t, reason, "gave up after 2 receives")

	// Once PostgreSQL is back, replaying the DLQ message processes it once.
	var replay sqsadapter.Envelope
	require.NoError(t, json.Unmarshal([]byte(dead), &replay))
	replay.MessageID = uuid.NewString()
	replayBody, err := sqsadapter.EncodeMessage(replay)
	require.NoError(t, err)
	sendRaw(t, api, q, replay.MessageID, replayBody, w.ID().String())
	consumerFor(api, q, s.wagers).PollOnce(context.Background())
	assert.Equal(t, "99.00", s.balance(t, w))
}

func TestQueueRedriveRemainsASafetyNet(t *testing.T) {
	t.Parallel()
	s := newServices(t, defaultPolicy)
	api, q, _ := provisionQueues(t, 2)
	w := s.openWallet(t, "100.00")
	sendMessage(t, api, q, s.prefix+"flaky", s.input(w, "provider-a", "flaky", "BET", "1.00", ""))

	// A consumer whose own limit is never reached leaves it to the broker.
	awaitDLQDepth(t, api, q, consumerWithLimit(api, q, flakyProcessor{}, 100), 1,
		"after maxReceiveCount the redrive policy moves it")
	assert.Equal(t, "100.00", s.balance(t, w))
}

// failingHead fails one message transiently and processes the others.
type failingHead struct {
	messageID string
	next      sqsadapter.Processor
}

func (p failingHead) ConsumeMessage(ctx context.Context, msg app.InboundMessage) (app.ConsumeResult, error) {
	if msg.MessageID == p.messageID {
		return app.ConsumeResult{}, app.ErrUnavailable
	}
	return p.next.ConsumeMessage(ctx, msg)
}

// A FIFO receive returns the retried head together with the messages behind
// it, so their receive counts climb with the head's. Before the consumer had
// its own limit, the redrive moved those never-attempted followers to the
// DLQ, without a failureReason, together with the head.
func TestFollowerOfAFailingHeadIsNotDeadLettered(t *testing.T) {
	t.Parallel()
	s := newServices(t, defaultPolicy)
	api, q, _ := provisionQueues(t, sqsadapter.RedriveMaxReceiveCount(itMaxReceiveCount))
	w := s.openWallet(t, "100.00")
	head := s.prefix + "head"
	sendMessage(t, api, q, head, s.input(w, "provider-a", "head", "BET", "1.00", ""))
	sendMessage(t, api, q, s.prefix+"follower", s.input(w, "provider-a", "follower", "BET", "2.00", ""))

	c := consumerFor(api, q, failingHead{messageID: head, next: s.wagers})
	awaitDLQDepth(t, api, q, c, 1, "the head is dead-lettered by the consumer")
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		c.PollOnce(context.Background())
		depth, err := queueDepth(api, q.Input)
		if assert.NoError(collect, err) {
			assert.Zero(collect, depth)
		}
	}, 30*time.Second, 200*time.Millisecond)

	dead, reason := receiveDead(t, api, q)
	assert.Contains(t, dead, head, "only the failing head is in the DLQ")
	assert.Contains(t, reason, "unavailable")
	assert.Equal(t, "98.00", s.balance(t, w), "the follower was processed")
	depth, err := queueDepth(api, q.DLQ)
	require.NoError(t, err)
	assert.Zero(t, depth)
}

func TestSameOperationThroughHTTPAndSQS(t *testing.T) {
	t.Parallel()
	s := newServices(t, defaultPolicy)
	r := startApp(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	w, err := s.wallets.Open(ctx, app.OpenWalletCommand{PlayerID: uuid.New(), InitialBalance: testutil.BRL(t, "100.00")})
	require.NoError(t, err)
	in := s.input(w, "provider-a", "cross", "BET", "10.00", "")
	messageID := uuid.NewString()
	responses, err := crossTransportSubmit(ctx, flowClient(r.http.Base, env), r.api, r.queues.Input, in, messageID)
	require.NoError(t, err)
	require.Contains(t, []int{http.StatusOK, http.StatusCreated}, responses[0].Status, responses[0].Body)
	require.Equal(t, "PROCESSED", responses[0].Body["status"])
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		depth, err := queueDepthContext(ctx, r.api, r.queues.Input)
		if !assert.NoError(collect, err) {
			return
		}
		assert.Zero(collect, depth)
	}, 10*time.Second, 100*time.Millisecond)
	updated, err := s.wallets.Get(ctx, w.ID())
	require.NoError(t, err)
	require.Equal(t, "90.00", updated.Balance().Amount())
	debits, err := testenv.CountDebits(ctx, pool, w.ID().String())
	require.NoError(t, err)
	require.Equal(t, 1, debits)
	var transactions, completed int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM wager_transactions
		WHERE provider_id = $1 AND external_transaction_id = $2`, in.ProviderID, in.ExternalTransactionID).Scan(&transactions))
	require.Equal(t, 1, transactions)
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM inbox_messages
		WHERE message_id = $1 AND processed_at IS NOT NULL`, messageID).Scan(&completed))
	require.Equal(t, 1, completed)
	depth, err := queueDepthContext(ctx, r.api, r.queues.DLQ)
	require.NoError(t, err)
	require.Zero(t, depth)
	transaction, err := s.wagers.GetByExternal(ctx, app.Caller{Internal: true}, in.ProviderID, in.ExternalTransactionID)
	require.NoError(t, err)
	require.Equal(t, wager.StatusProcessed, transaction.Status())
}

// A bounded transport must return the caller's deadline, not wait for the
// server to respond or let SQS retries outlive the HTTP submission.
func TestCrossTransportSubmissionHonorsDeadline(t *testing.T) {
	cancelled := make(chan bool, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		select {
		case <-r.Context().Done():
			cancelled <- true
		case <-time.After(time.Second):
			cancelled <- false
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	client := testenv.Client{Base: server.URL, Token: func(context.Context, string) (string, error) { return "test", nil }}
	in := testenv.SubmitInput(testenv.Wallet{ID: uuid.NewString(), PlayerID: uuid.NewString()}, "provider-a", uuid.NewString(), "BET", "1.00", "")
	deadline, _ := ctx.Deadline()
	_, err := crossTransportSubmit(ctx, client, deadlineSendAPI{deadline: deadline}, "input", in, uuid.NewString())
	require.ErrorIs(t, err, context.DeadlineExceeded)
	select {
	case stopped := <-cancelled:
		require.True(t, stopped, "HTTP was cancelled before the server responded")
	case <-time.After(2 * time.Second):
		t.Fatal("HTTP cancellation was not observed")
	}
}

func TestCrossTransportTokenHonorsDeadline(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(100 * time.Millisecond)
		_, _ = io.WriteString(w, `{"access_token":"late-token"}`)
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	client := flowClient(server.URL, &testenv.Env{KeycloakURL: server.URL})
	_, err := client.Token(ctx, "provider-a")
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func flowClient(base string, environment *testenv.Env) testenv.Client {
	return testenv.Client{Base: base, Token: func(ctx context.Context, provider string) (string, error) {
		tokenCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		return environment.Token(tokenCtx, provider)
	}}
}

func TestQueueDepthHonorsDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	deadline, _ := ctx.Deadline()
	_, err := queueDepthContext(ctx, deadlineDepthAPI{deadline: deadline}, "input")
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

type deadlineDepthAPI struct {
	sqsadapter.API
	deadline time.Time
}

func (a deadlineDepthAPI) GetQueueAttributes(ctx context.Context, _ *awssqs.GetQueueAttributesInput, _ ...func(*awssqs.Options)) (*awssqs.GetQueueAttributesOutput, error) {
	if deadline, bounded := ctx.Deadline(); !bounded || deadline.After(a.deadline) {
		return nil, fmt.Errorf("queue depth call lost the caller deadline")
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

type deadlineSendAPI struct {
	sqsadapter.API
	deadline time.Time
}

func (a deadlineSendAPI) SendMessage(ctx context.Context, _ *awssqs.SendMessageInput, _ ...func(*awssqs.Options)) (*awssqs.SendMessageOutput, error) {
	if deadline, bounded := ctx.Deadline(); !bounded || deadline.After(a.deadline) {
		return nil, fmt.Errorf("SQS call lost the caller deadline")
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

func crossTransportSubmit(ctx context.Context, client testenv.Client, api sqsadapter.API, queue string, in app.SubmitInput, messageID string) ([]testenv.Response, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return testenv.Parallel(2, func(i int) (testenv.Response, error) {
		if i == 0 {
			return client.Submit(ctx, testenv.Wallet{ID: in.WalletID, PlayerID: in.PlayerID}, in.ProviderID, in.ExternalTransactionID, in.Kind, in.Amount, "")
		}
		return testenv.Response{}, testenv.SendMessage(ctx, api, queue, testenv.Envelope(messageID, in))
	})
}

func TestSQSSenderMustBeBoundToTheProvider(t *testing.T) {
	t.Parallel()
	s := newServices(t, defaultPolicy)
	api, q, _ := provisionQueues(t, 5)
	w := s.openWallet(t, "100.00")
	sendMessage(t, api, q, s.prefix+"spoofed", s.input(w, "provider-a", "spoofed", "BET", "10.00", ""))

	onlyB := sqsadapter.NewConsumer(api, sqsadapter.ConsumerConfig{
		Name: "it-consumer", QueueURL: q.Input, DLQURL: q.DLQ, MaxMessages: 10, WaitTime: time.Second,
		VisibilityTimeout: 2 * time.Second, ProcessTimeout: time.Second, RetryBase: time.Second, RetryMax: time.Second,
		MaxReceiveCount: 5, Senders: sqsadapter.SenderPolicy{"000000000000": {"provider-b"}},
	}, s.wagers, observability.NewLogger(io.Discard, "error", "c"), testutil.NewMetrics())
	onlyB.PollOnce(context.Background())

	dead := drain(t, api, q.DLQ)
	require.Len(t, dead, 1, "a sender acting for another provider goes to the DLQ")
	assert.Equal(t, "100.00", s.balance(t, w), "no financial effect")
}

// orderedPublisher fails the first attempt of one event and records order.
type orderedPublisher struct {
	recordingPublisher
	failOnce uuid.UUID
	failed   bool
}

func (p *orderedPublisher) Publish(ctx context.Context, m app.OutboxMessage) error {
	p.mu.Lock()
	fail := m.EventID == p.failOnce && !p.failed
	p.failed = p.failed || fail
	p.mu.Unlock()
	if fail {
		return fmt.Errorf("broker hiccup")
	}
	return p.recordingPublisher.Publish(ctx, m)
}

// Not parallel: relays claim the whole (shared) outbox.
func TestOutboxKeepsPerWalletOrderWhenAnEventFails(t *testing.T) {
	s := newServices(t, defaultPolicy)
	w := s.openWallet(t, "100.00")
	s.submit(t, w, "ordered-1", "BET", "1.00", "")
	s.submit(t, w, "ordered-2", "BET", "1.00", "")
	var order []uuid.UUID
	rows, err := pool.Query(context.Background(), `SELECT event_id FROM outbox_events WHERE partition_key = $1 ORDER BY seq`, w.ID().String())
	require.NoError(t, err)
	for rows.Next() {
		var id uuid.UUID
		require.NoError(t, rows.Scan(&id))
		order = append(order, id)
	}
	require.NoError(t, rows.Err())
	require.Len(t, order, 6)

	pub := &orderedPublisher{failOnce: order[0]}
	store := postgres.NewOutboxStore(pool)
	relays := []*worker.Relay{newRelay(t, "ordered-a", store, pub), newRelay(t, "ordered-b", store, pub)}
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		_, err := testenv.Parallel(2, func(i int) (struct{}, error) {
			relays[i].Tick(context.Background())
			return struct{}{}, nil
		})
		if !assert.NoError(collect, err) {
			return
		}
		pending, err := unpublished(w.ID())
		if !assert.NoError(collect, err) {
			return
		}
		assert.Zero(collect, pending)
	}, 20*time.Second, 100*time.Millisecond)

	var published []uuid.UUID
	pub.mu.Lock()
	for _, id := range pub.ids {
		if slices.Contains(order, id) {
			published = append(published, id)
		}
	}
	pub.mu.Unlock()
	assert.Equal(t, order, published, "the failing head held back the rest of the wallet's events")
}

// poisonPublisher always fails one event (e.g. a payload the broker refuses).
type poisonPublisher struct {
	recordingPublisher
	poison uuid.UUID
}

func (p *poisonPublisher) Publish(ctx context.Context, m app.OutboxMessage) error {
	if m.EventID == p.poison {
		return fmt.Errorf("%w: broker refuses this event", worker.ErrPermanentPublish)
	}
	return p.recordingPublisher.Publish(ctx, m)
}

// Not parallel: relays claim the whole (shared) outbox.
func TestPoisonOutboxEventIsDeadLetteredAndUnblocksItsWallet(t *testing.T) {
	s := newServices(t, defaultPolicy)
	w := s.openWallet(t, "10.00")
	var poison uuid.UUID
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT event_id FROM outbox_events WHERE partition_key = $1 ORDER BY seq LIMIT 1`, w.ID().String()).Scan(&poison))

	pub := &poisonPublisher{poison: poison}
	relay := newRelayWithAttempts(t, "poison", postgres.NewOutboxStore(pool), pub, 3)
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		relay.Tick(context.Background())
		pending, err := unpublished(w.ID())
		if !assert.NoError(collect, err) {
			return
		}
		assert.Equal(collect, 1, pending)
	}, 20*time.Second, 50*time.Millisecond,
		"the event behind the poison one is published once the poison is dead-lettered")

	var dead bool
	var lastError string
	require.NoError(t, pool.QueryRow(context.Background(), `SELECT dead_lettered_at IS NOT NULL, last_error FROM outbox_events
		WHERE event_id = $1`, poison).Scan(&dead, &lastError))
	assert.True(t, dead)
	assert.Contains(t, lastError, "broker refuses this event")
	assert.Zero(t, pub.count(poison))
}

func TestProvisioningReconcilesChangedAttributes(t *testing.T) {
	t.Parallel()
	api, q, names := provisionQueues(t, 3)
	_, err := sqsadapter.Provision(context.Background(), api, sqsadapter.ProvisionConfig{Names: names, MaxReceiveCount: 7, VisibilityTimeout: 45})
	require.NoError(t, err, "rerunning with different attributes does not fail with QueueNameExists")

	out, err := api.GetQueueAttributes(context.Background(), &awssqs.GetQueueAttributesInput{QueueUrl: aws.String(q.Input),
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameVisibilityTimeout, types.QueueAttributeNameRedrivePolicy}})
	require.NoError(t, err)
	assert.Equal(t, "45", out.Attributes["VisibilityTimeout"])
	assert.Contains(t, out.Attributes["RedrivePolicy"], "7")
}

// insertOutboxEvent stores one due, unpublished event on its own partition.
func insertOutboxEvent(t *testing.T) uuid.UUID {
	t.Helper()
	id := uuid.New()
	_, err := pool.Exec(context.Background(), `INSERT INTO outbox_events
		(event_id, aggregate_type, aggregate_id, partition_key, event_type, payload, occurred_at, next_attempt_at)
		VALUES ($1, 'Wallet', gen_random_uuid(), $2, 'T', '{"a":1}', $3, $3)`,
		id, id.String(), time.Now().Add(-time.Minute))
	require.NoError(t, err)
	return id
}

// Not parallel: Claim leases the whole (shared) outbox.
func TestOutboxClaimTokenFencesReusedOwner(t *testing.T) {
	ctx := context.Background()
	store := postgres.NewOutboxStore(pool)
	id := insertOutboxEvent(t)
	owner := "shared-instance"
	firstClaimID, secondClaimID := uuid.New(), uuid.New()
	firstNow := time.Now()

	first, ok, err := store.Claim(ctx, owner, firstClaimID, firstNow, time.Minute)
	require.NoError(t, err)
	require.True(t, ok, "the fresh event is the head of its partition")
	require.Equal(t, id, first.EventID)
	require.Equal(t, firstClaimID, first.ClaimID)
	require.Zero(t, first.Attempts, "claiming alone is not a publication attempt")

	attempts, started, err := store.StartAttempt(ctx, id, firstClaimID, firstNow.Add(30*time.Second), time.Minute)
	require.NoError(t, err)
	require.True(t, started, "a live delayed claim starts its real publication attempt")
	require.Equal(t, 1, attempts)

	_, held, err := store.Claim(ctx, owner, secondClaimID, firstNow.Add(70*time.Second), time.Minute)
	require.NoError(t, err)
	assert.False(t, held, "StartAttempt renews the lease beyond its original expiry")

	second, ok, err := store.Claim(ctx, owner, secondClaimID, firstNow.Add(2*time.Minute), time.Minute)
	require.NoError(t, err)
	require.True(t, ok, "an expired lease is taken over")
	require.Equal(t, id, second.EventID)
	require.Equal(t, secondClaimID, second.ClaimID)

	ok, err = store.MarkPublished(ctx, id, firstClaimID, firstNow.Add(2*time.Minute))
	require.NoError(t, err)
	assert.False(t, ok, "the previous claim cannot publish under the reused owner")
	ok, err = store.MarkFailed(ctx, id, firstClaimID, firstNow.Add(3*time.Minute), "late failure")
	require.NoError(t, err)
	assert.False(t, ok, "the previous claim cannot reschedule under the reused owner")
	ok, err = store.MarkDead(ctx, id, firstClaimID, firstNow.Add(3*time.Minute), "late dead letter")
	require.NoError(t, err)
	assert.False(t, ok, "the previous claim cannot dead-letter under the reused owner")
	attempts, started, err = store.StartAttempt(ctx, id, firstClaimID, firstNow.Add(130*time.Second), time.Minute)
	require.NoError(t, err)
	assert.False(t, started, "the previous claim cannot start a publication under the reused owner")
	assert.Zero(t, attempts)

	attempts, started, err = store.StartAttempt(ctx, id, secondClaimID, firstNow.Add(150*time.Second), time.Minute)
	require.NoError(t, err)
	require.True(t, started)
	require.Equal(t, 2, attempts)
	ok, err = store.MarkPublished(ctx, id, secondClaimID, firstNow.Add(3*time.Minute))
	require.NoError(t, err)
	assert.True(t, ok, "the current claim confirms the publication")
	ok, err = store.MarkPublished(ctx, id, secondClaimID, firstNow.Add(3*time.Minute))
	require.NoError(t, err)
	assert.False(t, ok, "a publication is confirmed once")

	expiredID := insertOutboxEvent(t)
	expiredClaimID := uuid.New()
	expiredAt := firstNow.Add(4 * time.Minute).Truncate(time.Microsecond).Add(123 * time.Nanosecond)
	expired, ok, err := store.Claim(ctx, owner, expiredClaimID, expiredAt, time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, expiredID, expired.EventID)
	var originalLockedUntil time.Time
	require.NoError(t, pool.QueryRow(ctx, `SELECT locked_until FROM outbox_events WHERE event_id = $1`, expiredID).
		Scan(&originalLockedUntil))
	attempts, started, err = store.StartAttempt(ctx, expiredID, expiredClaimID, expiredAt.Add(time.Minute), time.Minute)
	require.NoError(t, err)
	assert.False(t, started, "an expired claim cannot be revived by starting an attempt")
	assert.Zero(t, attempts)
	var storedAttempts int
	var lockedUntil time.Time
	require.NoError(t, pool.QueryRow(ctx, `SELECT attempts, locked_until FROM outbox_events WHERE event_id = $1`, expiredID).
		Scan(&storedAttempts, &lockedUntil))
	assert.Zero(t, storedAttempts)
	assert.True(t, lockedUntil.Equal(originalLockedUntil), "an expired StartAttempt leaves locked_until unchanged")
}

// Not parallel: relays claim the whole (shared) outbox.
func TestRelayDeadlineFencesStaleFailureAndProgressesAnotherPartition(t *testing.T) {
	slowID, fastID := insertOutboxEvent(t), insertOutboxEvent(t)
	base := postgres.NewOutboxStore(pool)
	firstObserved := &observedOutboxStore{OutboxStore: base}
	firstStore := &gatedFailureStore{
		observedOutboxStore: firstObserved, entered: make(chan struct{}, 1), release: newReleaseGate(),
		result: make(chan markFailedResult, 1),
	}
	firstMetrics := &relayCounters{}
	firstCtx, cancelFirst := context.WithCancel(context.Background())
	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		relayForDeadlineTest(firstStore, blockingPublisher{}, firstMetrics, 1).Tick(firstCtx)
	}()
	registerRelayCleanup(t, cancelFirst, firstStore, firstDone)
	waitForRelaySignal(t, firstStore.entered, "first relay did not reach durable finalization")
	waitForClaimExpiration(t, slowID)

	secondStore := &observedOutboxStore{OutboxStore: base}
	secondPublisher := &partitionPublisher{failID: slowID}
	secondMetrics := &relayCounters{}
	relayForDeadlineTest(secondStore, secondPublisher, secondMetrics, 2).Tick(context.Background())
	require.NoError(t, stopGatedRelay(cancelFirst, firstStore, firstDone))

	firstClaims, secondClaims := firstObserved.snapshotClaims(), secondStore.snapshotClaims()
	require.Len(t, firstClaims, 1, "the canceled stale relay cannot claim another event")
	require.Len(t, secondClaims, 2)
	assert.Equal(t, slowID, firstClaims[0].eventID)
	assert.Equal(t, slowID, secondClaims[0].eventID)
	assert.NotEqual(t, firstClaims[0].claimID, secondClaims[0].claimID, "each acquisition has a fresh fencing token")
	assert.Equal(t, fastID, secondClaims[1].eventID)
	assert.Equal(t, []uuid.UUID{fastID}, secondPublisher.published, "an independent partition progresses")

	var attempts int
	var lastError string
	var backedOff, released, pending bool
	require.NoError(t, pool.QueryRow(context.Background(), `SELECT attempts, last_error, next_attempt_at > now(),
		claim_id IS NULL, published_at IS NULL FROM outbox_events WHERE event_id = $1`, slowID).
		Scan(&attempts, &lastError, &backedOff, &released, &pending))
	assert.Equal(t, 2, attempts)
	assert.Equal(t, "second relay failure", lastError, "the stale first claim cannot overwrite the current outcome")
	assert.True(t, backedOff)
	assert.True(t, released)
	assert.True(t, pending)
	assert.Zero(t, firstMetrics.failures.Load(), "a stale mutation is not counted")
	assert.Equal(t, int64(1), secondMetrics.failures.Load())
	assert.Equal(t, int64(1), secondMetrics.published.Load())
	assert.Zero(t, secondMetrics.dead.Load())
	assertStaleFailureResult(t, firstStore)
}

func TestInboxRegisterSerialisesConcurrentDeliveries(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	consumer, message := "it-consumer", uuid.NewString()
	registered, release := make(chan struct{}), make(chan struct{})
	first := make(chan error, 1)
	go func() {
		first <- postgres.NewUnitOfWork(pool).Do(ctx, func(ctx context.Context, r app.Repositories) error {
			_, created, err := r.Inbox().Register(ctx, consumer, message, "h1", time.Now())
			if err == nil && !created {
				err = fmt.Errorf("first delivery must create the entry")
			}
			close(registered)
			<-release
			return err
		})
	}()
	<-registered

	type outcome struct {
		entry   app.InboxEntry
		created bool
		err     error
	}
	second := make(chan outcome, 1)
	go func() {
		var o outcome
		o.err = postgres.NewUnitOfWork(pool).Do(ctx, func(ctx context.Context, r app.Repositories) error {
			var err error
			o.entry, o.created, err = r.Inbox().Register(ctx, consumer, message, "h1", time.Now())
			return err
		})
		second <- o
	}()
	select {
	case o := <-second:
		t.Fatalf("second delivery returned %+v before the first committed", o)
	case <-time.After(300 * time.Millisecond):
	}
	close(release)
	require.NoError(t, <-first)
	o := <-second
	require.NoError(t, o.err)
	assert.False(t, o.created, "the redelivery sees the committed entry")
	assert.Equal(t, app.InboxEntry{PayloadHash: "h1"}, o.entry, "not completed yet")
}
