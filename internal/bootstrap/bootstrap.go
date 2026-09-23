// Package bootstrap composes the application with Uber Fx. It is the only
// package that knows about Fx; domain, use cases and adapters stay free of it.
//
// Lifecycle order follows hook registration: the pool registers its hook
// while it is constructed, then the startWorkers, startServer and
// stopConsumersFirst invokes register theirs. Fx stops in reverse order:
//
//  1. the SQS consumers stop fetching new batches (no new queue input);
//  2. the HTTP server stops accepting connections and drains the open ones;
//  3. the workers return: a consumer finishes its in-flight message and
//     releases the rest of its batch, the relay and resolver stop;
//  4. the database pool closes.
//
// If a start hook fails, Fx runs the stop hooks of the ones that started.
package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"

	"github.com/Pantani/backend-challenge-go/internal/adapter/auth"
	"github.com/Pantani/backend-challenge-go/internal/adapter/httpapi"
	"github.com/Pantani/backend-challenge-go/internal/adapter/postgres"
	sqsadapter "github.com/Pantani/backend-challenge-go/internal/adapter/sqs"
	"github.com/Pantani/backend-challenge-go/internal/app"
	"github.com/Pantani/backend-challenge-go/internal/config"
	"github.com/Pantani/backend-challenge-go/internal/observability"
	"github.com/Pantani/backend-challenge-go/internal/worker"
)

// LogOutput is where JSON logs are written.
type LogOutput struct{ io.Writer }

// startupContext keeps the construction-only context distinct from request
// contexts in the Fx graph. Constructors may use it while building the graph,
// but runtime operations must receive their own contexts.
type startupContext struct{ context.Context }

// Options returns every module of the service for the given configuration.
// It is intended for graph validation; New supplies the real startup context.
func Options(cfg config.Config) fx.Option {
	return options(startupContext{Context: context.Background()}, cfg)
}

func options(startCtx startupContext, cfg config.Config) fx.Option {
	return fx.Options(
		fx.Supply(cfg, startCtx, LogOutput{os.Stdout}),
		fx.WithLogger(newFxLogger),
		ObservabilityModule, PostgresModule, SQSModule, AppModule, AuthModule, WorkerModule, HTTPModule,
		// Root invokes run after the modules', so this hook is registered
		// last and its OnStop runs first.
		fx.Invoke(stopConsumersFirst),
	)
}

// newFxLogger routes Fx events to the service logger at debug level, so
// the graph construction is visible with LOG_LEVEL=debug only.
func newFxLogger(l *slog.Logger) fxevent.Logger {
	fl := &fxevent.SlogLogger{Logger: l}
	fl.UseLogLevel(slog.LevelDebug)
	return fl
}

// New builds the application. ctx bounds graph construction (dependency
// lookups); Start takes its own context. extra options (fx.Replace,
// fx.Decorate, fx.Populate) let tests adjust the graph.
func New(ctx context.Context, cfg config.Config, extra ...fx.Option) *fx.App {
	return fx.New(options(startupContext{Context: ctx}, cfg), fx.Options(extra...),
		fx.StartTimeout(cfg.StartupTimeout), fx.StopTimeout(cfg.ShutdownTimeout))
}

// ObservabilityModule provides logging and metrics.
var ObservabilityModule = fx.Module("observability",
	fx.Provide(
		func(cfg config.Config, out LogOutput) *slog.Logger {
			return observability.NewLogger(out, cfg.LogLevel, cfg.InstanceID)
		},
		newRegistry,
		fx.Annotate(func(reg *prometheus.Registry) *observability.Metrics { return observability.NewMetrics(reg) },
			fx.As(fx.Self()), fx.As(new(app.Metrics))),
	),
)

// newRegistry is a private registry with the Go runtime and process collectors.
func newRegistry() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return reg
}

// PostgresModule provides the pool and the persistence ports.
var PostgresModule = fx.Module("postgres",
	fx.Provide(
		newPool,
		fx.Annotate(postgres.NewUnitOfWork, fx.As(new(app.UnitOfWork))),
		fx.Annotate(postgres.NewQueries, fx.As(new(app.Queries))),
		fx.Annotate(postgres.NewOutboxStore, fx.As(new(app.OutboxStore))),
	),
)

// newPool validates the database on start and closes the pool on stop.
// Its hook is registered first, so the pool closes last.
func newPool(lc fx.Lifecycle, startCtx startupContext, cfg config.Config) (*pgxpool.Pool, error) {
	pool, err := postgres.NewPool(startCtx, postgres.Config{
		URL: cfg.DatabaseURL, MaxConns: int32(cfg.DBMaxConns),
		LockTimeout: cfg.DBLockTimeout, StatementTimeout: cfg.DBStatementTimeout,
	})
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{
		// Fx only stops hooks whose start succeeded, so a failed ping closes
		// the pool itself.
		OnStart: func(ctx context.Context) error {
			if err := postgres.Ping(ctx, pool); err != nil {
				pool.Close()
				return err
			}
			return nil
		},
		OnStop: func(context.Context) error { pool.Close(); return nil },
	})
	return pool, nil
}

// SQSModule provides the SQS client, queues, publisher and consumer.
var SQSModule = fx.Module("sqs",
	fx.Provide(
		newSQSClient,
		newQueues,
		fx.Annotate(newPublisher, fx.As(new(worker.Publisher))),
		newConsumer,
	),
)

// NewSQSClient builds the SQS client for the configured region and endpoint.
// The CLI uses it too, so provisioning and serving share one setup.
func NewSQSClient(ctx context.Context, cfg config.SQS) (sqsadapter.API, error) {
	client, err := sqsadapter.NewClient(ctx, sqsadapter.ClientConfig{Region: cfg.AWSRegion, Endpoint: cfg.AWSEndpoint})
	if err != nil {
		return nil, err
	}
	return client, nil
}

// QueueNames returns the configured queue names.
func QueueNames(cfg config.SQS) sqsadapter.QueueNames {
	return sqsadapter.QueueNames{Input: cfg.SQSInputQueue, DLQ: cfg.SQSDLQ, Events: cfg.SQSEventsQueue}
}

func newSQSClient(startCtx startupContext, cfg config.Config) (sqsadapter.API, error) {
	return NewSQSClient(startCtx, cfg.SQS)
}

// newQueues resolves the queue URLs; missing queues fail the start.
func newQueues(startCtx startupContext, api sqsadapter.API, cfg config.Config) (sqsadapter.Queues, error) {
	return sqsadapter.ResolveQueues(startCtx, api, QueueNames(cfg.SQS))
}

func newPublisher(api sqsadapter.API, q sqsadapter.Queues) *sqsadapter.Publisher {
	return sqsadapter.NewPublisher(api, q.Events)
}

func newConsumer(api sqsadapter.API, q sqsadapter.Queues, cfg config.Config, svc *app.WagerService,
	logger *slog.Logger, metrics *observability.Metrics) (*sqsadapter.Consumer, error) {
	senders, err := sqsadapter.ParseSenderPolicy(cfg.SQSSenderProviders)
	if err != nil {
		return nil, err
	}
	return sqsadapter.NewConsumer(api, sqsadapter.ConsumerConfig{
		Name: cfg.SQSConsumerName, QueueURL: q.Input, DLQURL: q.DLQ, MaxMessages: int32(cfg.SQSMaxMessages),
		WaitTime: cfg.SQSWaitTime, VisibilityTimeout: cfg.SQSVisibilityTimeout, ProcessTimeout: cfg.SQSProcessTimeout,
		AckTimeout: cfg.SQSAckTimeout,
		RetryBase:  cfg.SQSRetryBase, RetryMax: cfg.SQSRetryMax, MaxReceiveCount: cfg.SQSMaxReceiveCount,
		Senders: senders,
	}, svc, logger, metrics), nil
}

// AppModule provides the use cases.
var AppModule = fx.Module("app",
	fx.Provide(
		func() app.Clock { return app.SystemClock{} },
		func() app.IDGenerator { return app.UUIDv7{} },
		newWagerService,
		newWalletService,
	),
)

type serviceDeps struct {
	fx.In
	UoW     app.UnitOfWork
	Queries app.Queries
	Clock   app.Clock
	IDs     app.IDGenerator
	Metrics app.Metrics
	Logger  *slog.Logger
	Config  config.Config
}

func newWagerService(d serviceDeps) *app.WagerService {
	return app.NewWagerService(app.WagerDeps{
		Deps:            d.deps(),
		ConflictRetries: d.Config.ConflictRetries,
		Policy: app.PendingPolicy{BaseDelay: d.Config.PendingBaseDelay, MaxDelay: d.Config.PendingMaxDelay,
			MaxAttempts: d.Config.PendingMaxAttempts, BatchSize: d.Config.PendingBatch},
	})
}

func newWalletService(d serviceDeps) *app.WalletService {
	return app.NewWalletService(app.WalletDeps{Deps: d.deps()})
}

func (d serviceDeps) deps() app.Deps {
	return app.Deps{UoW: d.UoW, Queries: d.Queries, Clock: d.Clock, IDs: d.IDs, Metrics: d.Metrics, Logger: d.Logger}
}

// AuthModule provides the OIDC token verifier.
var AuthModule = fx.Module("auth",
	fx.Provide(fx.Annotate(func(startCtx startupContext, cfg config.Config) *auth.Verifier {
		return auth.NewVerifier(startCtx, auth.Config{Issuer: cfg.OIDCIssuer, JWKSURL: cfg.OIDCJWKSURL, Audience: cfg.OIDCAudience})
	}, fx.As(new(httpapi.TokenVerifier)))),
)

// WorkerModule starts the outbox relay, the pending resolver and the SQS
// consumers under one supervised group.
var WorkerModule = fx.Module("worker",
	fx.Provide(newGroup, newConsumerGroup, newRelay, newPendingResolver),
	fx.Invoke(startWorkers),
)

// newGroup registers no hook: startWorkers does, so the group stops before
// the resources constructed earlier (the pool) are closed.
func newGroup(logger *slog.Logger) *worker.Group {
	return worker.NewGroup(context.Background(), logger)
}

// consumerGroup supervises the SQS consumers apart from the other workers,
// so shutdown can stop them fetching before the HTTP server drains.
type consumerGroup struct{ *worker.Group }

func newConsumerGroup(logger *slog.Logger) consumerGroup {
	return consumerGroup{worker.NewGroup(context.Background(), logger)}
}

func newRelay(store app.OutboxStore, pub worker.Publisher, clock app.Clock, cfg config.Config,
	logger *slog.Logger, metrics *observability.Metrics) *worker.Relay {
	return worker.NewRelay(store, pub, clock, worker.RelayConfig{
		Owner: cfg.InstanceID, BatchSize: cfg.OutboxBatch, Lease: cfg.OutboxLease,
		RetryBase: cfg.OutboxRetryBase, RetryMax: cfg.OutboxRetryMax, PublishTime: cfg.OutboxPublishTimeout,
		FinalizeTime: cfg.OutboxFinalizeTimeout,
		MaxAttempts:  cfg.OutboxMaxAttempts,
	}, logger, metrics)
}

func newPendingResolver(svc *app.WagerService, logger *slog.Logger) *worker.PendingResolver {
	return worker.NewPendingResolver(svc, logger)
}

type workerDeps struct {
	fx.In
	Lifecycle fx.Lifecycle
	Group     *worker.Group
	Consumers consumerGroup
	Relay     *worker.Relay
	Pending   *worker.PendingResolver
	Consumer  *sqsadapter.Consumer
	Config    config.Config
}

// startWorkers launches the loops on start and waits for them on stop. Its
// hook is appended after every constructor's, so the workers stop before the
// pool closes.
func startWorkers(d workerDeps) {
	d.Lifecycle.Append(fx.Hook{
		OnStart: func(context.Context) error {
			d.Group.Go("outbox-relay", func(ctx context.Context) { worker.Loop(ctx, d.Config.OutboxInterval, d.Relay.Tick) })
			d.Group.Go("pending-resolver", func(ctx context.Context) { worker.Loop(ctx, d.Config.PendingInterval, d.Pending.Tick) })
			for i := range d.Config.SQSConsumers {
				d.Consumers.Go(fmt.Sprintf("sqs-consumer-%d", i), d.Consumer.Run)
			}
			return nil
		},
		OnStop: func(ctx context.Context) error {
			return errors.Join(d.Consumers.Stop(ctx), d.Group.Stop(ctx))
		},
	})
}

// stopConsumersFirst makes the consumers stop fetching as soon as shutdown
// begins, without waiting for them: the HTTP server drains meanwhile, and
// startWorkers' stop hook waits for the in-flight messages afterwards.
func stopConsumersFirst(lc fx.Lifecycle, consumers consumerGroup) {
	lc.Append(fx.StopHook(consumers.Cancel))
}

// HTTPModule serves the API.
var HTTPModule = fx.Module("http",
	fx.Provide(newHealthChecks, newHandler, newServer, func() *Addr { return &Addr{} }),
	fx.Invoke(startServer),
)

// newHealthChecks are the readiness probes: the database and the input queue.
func newHealthChecks(pool *pgxpool.Pool, api sqsadapter.API, q sqsadapter.Queues) []httpapi.HealthCheck {
	return []httpapi.HealthCheck{
		{Name: "postgres", Check: func(ctx context.Context) error { return postgres.Ping(ctx, pool) }},
		{Name: "sqs", Check: sqsadapter.QueueCheck(api, q.Input)},
	}
}

type handlerDeps struct {
	fx.In
	Wallets  *app.WalletService
	Wagers   *app.WagerService
	Verifier httpapi.TokenVerifier
	Metrics  *observability.Metrics
	Registry *prometheus.Registry
	Checks   []httpapi.HealthCheck
	Logger   *slog.Logger
	Config   config.Config
}

func newHandler(d handlerDeps) http.Handler {
	return httpapi.NewHandler(httpapi.Deps{
		Wallets: d.Wallets, Wagers: d.Wagers, Verifier: d.Verifier, Metrics: d.Metrics, Logger: d.Logger,
		MetricsHandler: promhttp.HandlerFor(d.Registry, promhttp.HandlerOpts{}),
		ReadyTimeout:   d.Config.ReadyTimeout,
		Checks:         d.Checks,
	})
}

// HTTP server deadlines: slow clients cannot hold a handler while sending the
// body, and a blocked response write is bounded. IdleTimeout falls back to
// ReadTimeout.
const (
	readHeaderTimeout = 5 * time.Second
	readTimeout       = 15 * time.Second
	writeTimeout      = 30 * time.Second
)

func newServer(h http.Handler, cfg config.Config) *http.Server {
	return &http.Server{Addr: cfg.HTTPAddr, Handler: h,
		ReadHeaderTimeout: readHeaderTimeout, ReadTimeout: readTimeout, WriteTimeout: writeTimeout}
}

// Addr exposes the bound address (useful with ":0" in tests).
type Addr struct{ value string }

// String returns the listening address.
func (a *Addr) String() string { return a.value }

// startServer listens on start (the bound address is published through
// Addr) and drains the connections on stop.
func startServer(lc fx.Lifecycle, srv *http.Server, addr *Addr, logger *slog.Logger, shutdowner fx.Shutdowner) {
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", srv.Addr)
			if err != nil {
				return err
			}
			addr.value = ln.Addr().String()
			logger.Info("http server listening", "addr", addr.value)
			go serve(srv, ln, logger, shutdowner)
			return nil
		},
		OnStop: func(ctx context.Context) error {
			logger.Info("http server shutting down")
			return srv.Shutdown(ctx)
		},
	})
}

// serve runs the server until Shutdown. Any other end is a failure of the
// process, so it asks Fx to shut the application down with exit code 1.
func serve(srv *http.Server, ln net.Listener, logger *slog.Logger, shutdowner fx.Shutdowner) {
	err := srv.Serve(ln)
	if errors.Is(err, http.ErrServerClosed) {
		return
	}
	logger.Error("http server stopped unexpectedly", "error", err)
	if err := shutdowner.Shutdown(fx.ExitCode(1)); err != nil {
		logger.Error("shutdown request failed", "error", err)
	}
}
