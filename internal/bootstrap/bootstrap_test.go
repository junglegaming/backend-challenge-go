package bootstrap_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"

	"github.com/Pantani/backend-challenge-go/internal/adapter/httpapi"
	sqsadapter "github.com/Pantani/backend-challenge-go/internal/adapter/sqs"
	"github.com/Pantani/backend-challenge-go/internal/app"
	"github.com/Pantani/backend-challenge-go/internal/bootstrap"
	"github.com/Pantani/backend-challenge-go/internal/config"
	"github.com/Pantani/backend-challenge-go/internal/observability"
	"github.com/Pantani/backend-challenge-go/internal/worker"
)

// fakeAPI resolves any queue name; nothing else is reachable without Docker.
type fakeAPI struct{ sqsadapter.API }

func (fakeAPI) GetQueueUrl( //nolint:revive // name imposed by the AWS SDK interface
	_ context.Context, in *sqs.GetQueueUrlInput, _ ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
	return &sqs.GetQueueUrlOutput{QueueUrl: aws.String("http://sqs.local/" + aws.ToString(in.QueueName))}, nil
}

// recorder keeps the lifecycle events in order.
type recorder struct {
	mu     sync.Mutex
	events []fxevent.Event
}

func (r *recorder) LogEvent(e fxevent.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, e)
}

func (r *recorder) startHooks() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var names []string
	for _, e := range r.events {
		if h, ok := e.(*fxevent.OnStartExecuting); ok {
			names = append(names, h.CallerName)
		}
	}
	return names
}

// graph is populated by the constructors of every module.
type graph struct {
	fx.In
	Pool     *pgxpool.Pool
	API      sqsadapter.API
	Queues   sqsadapter.Queues
	Consumer *sqsadapter.Consumer
	Wagers   *app.WagerService
	Wallets  *app.WalletService
	Verifier httpapi.TokenVerifier
	Group    *worker.Group
	Relay    *worker.Relay
	Pending  *worker.PendingResolver
	Handler  http.Handler
	Server   *http.Server
	Checks   []httpapi.HealthCheck
	Metrics  *observability.Metrics
	Registry *prometheus.Registry
	Logger   *slog.Logger
	Addr     *bootstrap.Addr
}

func testConfig(t *testing.T, overrides map[string]string) config.Config {
	t.Helper()
	vars := map[string]string{"HTTP_ADDR": "127.0.0.1:0",
		"DATABASE_URL": "postgres://u:p@127.0.0.1:1/db?sslmode=disable&connect_timeout=1"}
	for k, v := range overrides {
		vars[k] = v
	}
	cfg, err := config.Load(config.MapLookup(vars))
	require.NoError(t, err)
	return cfg
}

func fakeSQS() fx.Option {
	return fx.Replace(fx.Annotate(fakeAPI{}, fx.As(new(sqsadapter.API))), bootstrap.LogOutput{Writer: io.Discard})
}

// TestGraphIsComplete checks the dependency graph without starting anything.
func TestGraphIsComplete(t *testing.T) {
	t.Parallel()
	cfg, err := config.Load(config.MapLookup(nil))
	require.NoError(t, err)
	require.NoError(t, fx.ValidateApp(bootstrap.Options(cfg)))
}

func TestEveryConstructorRunsAndStartFailsAtThePing(t *testing.T) {
	t.Parallel()
	var g graph
	rec := &recorder{}
	a := bootstrap.New(context.Background(), testConfig(t, nil), fakeSQS(), fx.Populate(&g),
		fx.WithLogger(func() fxevent.Logger { return rec }))
	require.NoError(t, a.Err(), "the whole graph is constructed without Docker")
	assert.Equal(t, "http://sqs.local/wager-transactions.fifo", g.Queues.Input)
	assert.Len(t, g.Checks, 2)
	for _, v := range []any{g.Pool, g.API, g.Consumer, g.Wagers, g.Wallets, g.Verifier, g.Group, g.Relay, g.Pending,
		g.Handler, g.Server, g.Metrics, g.Registry, g.Logger, g.Addr} {
		assert.NotNil(t, v)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := a.Start(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "connect", "the first hook, the database ping, fails")
	assert.Equal(t, []string{"github.com/Pantani/backend-challenge-go/internal/bootstrap.newPool"}, rec.startHooks(),
		"nothing runs before the database is validated")
	assert.Zero(t, g.Group.Running())
	assert.Empty(t, g.Addr.String())
}

func TestConstructionErrorsSurfaceThroughErr(t *testing.T) {
	t.Parallel()
	senderCfg := testConfig(t, nil)
	senderCfg.SQSSenderProviders = "missing-equals"
	require.Error(t, bootstrap.New(context.Background(), senderCfg, fakeSQS()).Err(),
		"programmatic configuration is still checked by the consumer constructor")

	badDatabase := testConfig(t, map[string]string{"DATABASE_URL": "postgres://%%%"})
	require.Error(t, bootstrap.New(context.Background(), badDatabase, fakeSQS()).Err())
}
