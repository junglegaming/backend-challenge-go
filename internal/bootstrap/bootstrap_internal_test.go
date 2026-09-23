package bootstrap

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"

	sqsadapter "github.com/Pantani/backend-challenge-go/internal/adapter/sqs"
	"github.com/Pantani/backend-challenge-go/internal/app"
	"github.com/Pantani/backend-challenge-go/internal/config"
	"github.com/Pantani/backend-challenge-go/internal/testutil"
	"github.com/Pantani/backend-challenge-go/internal/worker"
)

// blockingAPI parks every receive until its context is done.
type blockingAPI struct{ sqsadapter.API }

func (blockingAPI) ReceiveMessage(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

type blockingQueueAPI struct {
	sqsadapter.API
	once sync.Once
	seen chan struct{}
}

func (a *blockingQueueAPI) GetQueueUrl( //nolint:revive // name imposed by the AWS SDK interface
	ctx context.Context, _ *sqs.GetQueueUrlInput, _ ...func(*sqs.Options),
) (*sqs.GetQueueUrlOutput, error) {
	a.once.Do(func() { close(a.seen) })
	<-ctx.Done()
	return nil, ctx.Err()
}

// emptyStore has nothing to relay.
type emptyStore struct{ app.OutboxStore }

func (emptyStore) Claim(context.Context, string, uuid.UUID, time.Time, time.Duration) (app.OutboxMessage, bool, error) {
	return app.OutboxMessage{}, false, nil
}
func (emptyStore) OldestPending(context.Context) (time.Time, bool, error) {
	return time.Time{}, false, nil
}

type nothingPending struct{}

func (nothingPending) ResolveDue(context.Context) (int, error) { return 0, nil }

type noPublisher struct{}

func (noPublisher) Publish(context.Context, app.OutboxMessage) error { return nil }

func testConfig(t *testing.T) config.Config {
	t.Helper()
	cfg, err := config.Load(config.MapLookup(map[string]string{"HTTP_ADDR": "127.0.0.1:0", "SQS_CONSUMERS": "3",
		"OUTBOX_INTERVAL": "10ms", "PENDING_INTERVAL": "10ms"}))
	require.NoError(t, err)
	return cfg
}

func newWorkerDeps(t *testing.T, lc fx.Lifecycle) workerDeps {
	t.Helper()
	cfg := testConfig(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	metrics := testutil.NewMetrics()
	return workerDeps{
		Lifecycle: lc, Group: newGroup(logger), Consumers: newConsumerGroup(logger), Config: cfg,
		Relay:    newRelay(emptyStore{}, noPublisher{}, app.SystemClock{}, cfg, logger, metrics),
		Pending:  worker.NewPendingResolver(nothingPending{}, logger),
		Consumer: sqsadapter.NewConsumer(blockingAPI{}, sqsadapter.ConsumerConfig{QueueURL: "q", WaitTime: time.Second}, nil, logger, metrics),
	}
}

func TestStartWorkersRunsAndStopsTheGroup(t *testing.T) {
	t.Parallel()
	lc := fxtest.NewLifecycle(t)
	d := newWorkerDeps(t, lc)
	startWorkers(d)
	lc.RequireStart()
	assert.Equal(t, 2, d.Group.Running(), "relay and pending resolver")
	assert.Equal(t, d.Config.SQSConsumers, d.Consumers.Running(), "one goroutine per consumer")
	lc.RequireStop()
	assert.Zero(t, d.Group.Running())
	assert.Zero(t, d.Consumers.Running())
}

// TestShutdownStopsConsumersBeforeHTTPDrains registers the hooks as the
// application does and checks that, while an HTTP request is still being
// drained, the consumers have already stopped fetching.
func TestShutdownStopsConsumersBeforeHTTPDrains(t *testing.T) {
	t.Parallel()
	lc := fxtest.NewLifecycle(t)
	d := newWorkerDeps(t, lc)
	entered, release := make(chan struct{}), make(chan struct{})
	srv := newServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(entered)
		<-release
		w.WriteHeader(http.StatusNoContent)
	}), d.Config)
	addr := &Addr{}
	startWorkers(d)
	startServer(lc, srv, addr, slog.New(slog.NewTextHandler(io.Discard, nil)), noShutdowner{})
	stopConsumersFirst(lc, d.Consumers)
	lc.RequireStart()

	status := make(chan int, 1)
	go func() {
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://"+addr.String()+"/", nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			status <- 0
			return
		}
		_ = resp.Body.Close()
		status <- resp.StatusCode
	}()
	<-entered
	stopped := make(chan error, 1)
	go func() { stopped <- lc.Stop(context.Background()) }()

	require.Eventually(t, func() bool { return d.Consumers.Running() == 0 }, 5*time.Second, 5*time.Millisecond,
		"the consumers stop while the request is in flight")
	assert.Equal(t, 2, d.Group.Running(), "the other workers wait for the HTTP drain")
	select {
	case <-stopped:
		require.Fail(t, "stop returned before the HTTP request was drained")
	default:
	}
	close(release)
	assert.Equal(t, http.StatusNoContent, <-status, "the in-flight request completes")
	require.NoError(t, <-stopped)
	assert.Zero(t, d.Group.Running())
}

type noShutdowner struct{}

func (noShutdowner) Shutdown(...fx.ShutdownOption) error { return nil }

type shutdownSpy struct{ calls chan struct{} }

func (s shutdownSpy) Shutdown(...fx.ShutdownOption) error {
	close(s.calls)
	return nil
}

func TestServeFailureShutsTheApplicationDown(t *testing.T) {
	t.Parallel()
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	require.NoError(t, ln.Close()) // Serve fails immediately on a closed listener
	spy := shutdownSpy{calls: make(chan struct{})}
	serve(&http.Server{ReadHeaderTimeout: time.Second}, ln, slog.New(slog.NewTextHandler(io.Discard, nil)), spy)
	assertClosed(t, spy.calls)
}

func TestStartServerBindsAndClosesTheListener(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	srv := newServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }), cfg)
	assert.Equal(t, 5*time.Second, srv.ReadHeaderTimeout)
	assert.Equal(t, 15*time.Second, srv.ReadTimeout)
	assert.Equal(t, 30*time.Second, srv.WriteTimeout)
	addr := &Addr{}
	lc := fxtest.NewLifecycle(t)
	startServer(lc, srv, addr, slog.New(slog.NewTextHandler(io.Discard, nil)), noShutdowner{})
	assert.Empty(t, addr.String(), "not bound before start")
	lc.RequireStart()
	require.NotEmpty(t, addr.String())
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://"+addr.String()+"/", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
	lc.RequireStop()
	_, err = (&net.Dialer{Timeout: time.Second}).Dial("tcp", addr.String())
	require.Error(t, err, "the listener is closed on stop")

	bad := newServer(srv.Handler, config.Config{HTTPAddr: "256.0.0.1:1"})
	lc = fxtest.NewLifecycle(t)
	startServer(lc, bad, &Addr{}, slog.New(slog.NewTextHandler(io.Discard, nil)), noShutdowner{})
	require.Error(t, lc.Start(context.Background()))
}

func TestRegistryHasRuntimeCollectors(t *testing.T) {
	t.Parallel()
	reg := newRegistry()
	families, err := reg.Gather()
	require.NoError(t, err)
	names := make([]string, 0, len(families))
	for _, f := range families {
		names = append(names, f.GetName())
	}
	assert.Contains(t, names, "go_goroutines")
	assert.Contains(t, names, "process_start_time_seconds")
}

func TestConstructionUsesStartupContext(t *testing.T) {
	t.Run("queue resolution", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
		defer cancel()
		api := &blockingQueueAPI{seen: make(chan struct{})}
		app := New(ctx, testConfig(t), fx.Replace(fx.Annotate(api, fx.As(new(sqsadapter.API)))))

		assert.ErrorIs(t, app.Err(), context.DeadlineExceeded)
		assertClosed(t, api.seen)
	})

	t.Run("database ping", func(t *testing.T) {
		listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, listener.Close()) })
		accepted := make(chan struct{})
		serveStalledPostgres(t, listener, accepted)

		cfg := testConfig(t)
		cfg.DatabaseURL = "postgres://u:p@" + listener.Addr().String() + "/db?sslmode=disable&connect_timeout=10"
		ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
		defer cancel()
		app := New(ctx, cfg, fx.Replace(fx.Annotate(fakeQueueAPI{}, fx.As(new(sqsadapter.API)))))
		require.NoError(t, app.Err())

		err = app.Start(ctx)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
		assertClosed(t, accepted)
	})
}

type fakeQueueAPI struct{ sqsadapter.API }

func (fakeQueueAPI) GetQueueUrl( //nolint:revive // name imposed by the AWS SDK interface
	_ context.Context, _ *sqs.GetQueueUrlInput, _ ...func(*sqs.Options),
) (*sqs.GetQueueUrlOutput, error) {
	return &sqs.GetQueueUrlOutput{QueueUrl: aws.String("http://sqs.local/queue")}, nil
}

func assertClosed(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	default:
		require.Fail(t, "operation did not reach the blocking dependency")
	}
}

func serveStalledPostgres(t *testing.T, listener net.Listener, accepted chan<- struct{}) {
	t.Helper()
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		close(accepted)
		_, _ = io.Copy(io.Discard, conn)
	}()
}
