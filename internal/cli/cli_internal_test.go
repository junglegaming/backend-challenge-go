package cli

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"

	sqsadapter "github.com/Pantani/backend-challenge-go/internal/adapter/sqs"
	"github.com/Pantani/backend-challenge-go/internal/bootstrap"
	"github.com/Pantani/backend-challenge-go/internal/config"
)

// fakeMigrator records the operation it received.
type fakeMigrator struct {
	op     string
	steps  int
	closed bool
	err    error
}

func (f *fakeMigrator) Up() error { f.op = "up"; return f.err }
func (f *fakeMigrator) Down(steps int) error {
	f.op, f.steps = "down", steps
	return f.err
}
func (f *fakeMigrator) Version() (uint, bool, error) { f.op = "version"; return 3, true, f.err }
func (f *fakeMigrator) Close() error                 { f.closed = true; return nil }

// fakeAPI answers queue lookups and creations in memory; the queue URL is
// derived from the name so every other operation stays unimplemented.
type fakeAPI struct{ sqsadapter.API }

func (fakeAPI) GetQueueUrl( //nolint:revive // name imposed by the AWS SDK interface
	_ context.Context, in *sqs.GetQueueUrlInput, _ ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
	return &sqs.GetQueueUrlOutput{QueueUrl: aws.String("http://sqs.local/" + aws.ToString(in.QueueName))}, nil
}

func (fakeAPI) CreateQueue(_ context.Context, in *sqs.CreateQueueInput, _ ...func(*sqs.Options)) (*sqs.CreateQueueOutput, error) {
	return &sqs.CreateQueueOutput{QueueUrl: aws.String("http://sqs.local/" + aws.ToString(in.QueueName))}, nil
}

func (fakeAPI) GetQueueAttributes(_ context.Context, in *sqs.GetQueueAttributesInput, _ ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error) {
	return &sqs.GetQueueAttributesOutput{Attributes: map[string]string{"QueueArn": "arn:" + aws.ToString(in.QueueUrl)}}, nil
}

func (fakeAPI) SetQueueAttributes(context.Context, *sqs.SetQueueAttributesInput, ...func(*sqs.Options)) (*sqs.SetQueueAttributesOutput, error) {
	return &sqs.SetQueueAttributesOutput{}, nil
}

func TestParseSteps(t *testing.T) {
	t.Parallel()
	cases := []struct {
		args []string
		want int
		err  error
	}{
		{nil, 1, nil},
		{[]string{"3"}, 3, nil},
		{[]string{"0"}, 0, ErrUsage},
		{[]string{"-2"}, 0, ErrUsage},
		{[]string{"two"}, 0, ErrUsage},
		{[]string{"2", "typo"}, 0, ErrUsage},
	}
	for _, tc := range cases {
		got, err := parseSteps(tc.args)
		assert.Equal(t, tc.want, got, tc.args)
		assert.ErrorIs(t, err, tc.err, tc.args)
	}
}

func TestParseMigration(t *testing.T) {
	t.Parallel()
	cases := []struct {
		args []string
		want migration
		err  error
	}{
		{[]string{"up"}, migration{op: "up"}, nil},
		{[]string{"version"}, migration{op: "version"}, nil},
		{[]string{"down"}, migration{op: "down", steps: 1}, nil},
		{[]string{"down", "4"}, migration{op: "down", steps: 4}, nil},
		{[]string{"down", "x"}, migration{op: "down"}, ErrUsage},
		{[]string{"up", "extra"}, migration{op: "up"}, ErrUsage},
		{[]string{"sideways"}, migration{}, ErrUsage},
		{nil, migration{}, ErrUsage},
	}
	for _, tc := range cases {
		got, err := parseMigration(tc.args)
		assert.Equal(t, tc.want, got, tc.args)
		assert.ErrorIs(t, err, tc.err, tc.args)
	}
}

// Not parallel: it swaps the package-level constructors.
func TestMigrateCommandWithFakeMigrator(t *testing.T) {
	fake := &fakeMigrator{}
	var gotURL string
	newMigrator = func(url string) (migrator, error) { gotURL = url; return fake, nil }
	t.Cleanup(func() { newMigrator = defaultMigrator })
	lookup := config.MapLookup(map[string]string{"DATABASE_URL": "postgres://fake", "SQS_CONSUMERS": "0"})

	var out bytes.Buffer
	require.NoError(t, migrateCmd(lookup, []string{"up"}, &out))
	assert.Equal(t, "postgres://fake", gotURL, "the database subset is loaded despite invalid SQS variables")
	assert.Equal(t, "up", fake.op)
	assert.True(t, fake.closed)
	require.NoError(t, migrateCmd(lookup, []string{"down", "2"}, &out))
	assert.Equal(t, migration{op: "down", steps: 2}, migration{op: fake.op, steps: fake.steps})
	require.NoError(t, migrateCmd(lookup, []string{"version"}, &out))
	assert.Equal(t, "version=3 dirty=true\n", out.String())

	fake.err = errors.New("boom")
	require.ErrorIs(t, migrateCmd(lookup, []string{"up"}, &out), fake.err)
	require.ErrorIs(t, migrateCmd(lookup, nil, &out), ErrUsage)
	err := migrateCmd(config.MapLookup(map[string]string{"DB_MAX_CONNS": "0"}), []string{"up"}, &out)
	require.ErrorContains(t, err, "invalid configuration")
	newMigrator = func(string) (migrator, error) { return nil, errors.New("no driver") }
	require.ErrorContains(t, migrateCmd(lookup, []string{"up"}, &out), "no driver")
}

// Not parallel: it swaps the package-level constructors.
func TestProvisionWithFakeClient(t *testing.T) {
	newSQSClient = func(context.Context, config.SQS) (sqsadapter.API, error) { return fakeAPI{}, nil }
	t.Cleanup(func() { newSQSClient = bootstrap.NewSQSClient })
	var out bytes.Buffer
	lookup := config.MapLookup(map[string]string{"SQS_INPUT_QUEUE": "in.fifo", "DB_MAX_CONNS": "0"})
	require.NoError(t, provision(context.Background(), lookup, &out))
	assert.Contains(t, out.String(), "input=http://sqs.local/in.fifo\n")
	assert.Contains(t, out.String(), "dlq=http://sqs.local/wager-transactions-dlq.fifo\n")

	err := provision(context.Background(), config.MapLookup(map[string]string{"SQS_MAX_MESSAGES": "0"}), &out)
	require.ErrorContains(t, err, "invalid configuration")
	newSQSClient = func(context.Context, config.SQS) (sqsadapter.API, error) { return nil, errors.New("no credentials") }
	require.ErrorContains(t, provision(context.Background(), lookup, &out), "no credentials")
}

// Not parallel: it swaps the package-level constructors.
func TestServeFailsFastWhenCancelled(t *testing.T) {
	newApp = func(ctx context.Context, cfg config.Config) application {
		return bootstrap.New(ctx, cfg, fx.Replace(fx.Annotate(fakeAPI{}, fx.As(new(sqsadapter.API)))),
			fx.Replace(bootstrap.LogOutput{Writer: &bytes.Buffer{}}))
	}
	t.Cleanup(func() { newApp = defaultApp })
	cfg, err := config.Load(config.MapLookup(map[string]string{
		"DATABASE_URL": "postgres://u:p@127.0.0.1:1/db?sslmode=disable&connect_timeout=1", "HTTP_ADDR": "127.0.0.1:0"}))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.Error(t, serve(ctx, cfg), "the start fails at the database ping")

	cfg.SQSSenderProviders = "missing-equals"
	require.ErrorContains(t, serve(context.Background(), cfg), "build application", "a construction error is reported before Start")
}

type blockingStartApp struct {
	mu       sync.Mutex
	deadline time.Time
}

func (*blockingStartApp) Err() error { return nil }

func (a *blockingStartApp) Start(ctx context.Context) error {
	a.mu.Lock()
	a.deadline, _ = ctx.Deadline()
	a.mu.Unlock()
	<-ctx.Done()
	return ctx.Err()
}

func (*blockingStartApp) Stop(context.Context) error { return nil }

func (*blockingStartApp) Wait() <-chan fx.ShutdownSignal {
	return make(chan fx.ShutdownSignal)
}

func (a *blockingStartApp) startDeadline() time.Time {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.deadline
}

// Not parallel: it swaps the package-level application constructor.
func TestServeAppliesStartupDeadline(t *testing.T) {
	fake := &blockingStartApp{}
	newApp = func(context.Context, config.Config) application { return fake }
	t.Cleanup(func() { newApp = defaultApp })
	cfg, err := config.Load(config.MapLookup(nil))
	require.NoError(t, err)
	cfg.StartupTimeout = 40 * time.Millisecond
	started := time.Now()

	err = serve(context.Background(), cfg)

	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.WithinDuration(t, started.Add(cfg.StartupTimeout), fake.startDeadline(), 20*time.Millisecond)
	assert.Less(t, time.Since(started), time.Second)
}

// failingApp starts and then reports a shutdown request with exitCode, as
// bootstrap does when the HTTP server dies.
type failingApp struct {
	exitCode int
	stopped  bool
}

func (*failingApp) Err() error                   { return nil }
func (*failingApp) Start(context.Context) error  { return nil }
func (a *failingApp) Stop(context.Context) error { a.stopped = true; return nil }
func (a *failingApp) Wait() <-chan fx.ShutdownSignal {
	ch := make(chan fx.ShutdownSignal, 1)
	ch <- fx.ShutdownSignal{ExitCode: a.exitCode}
	return ch
}

// Not parallel: it swaps the package-level application constructor.
func TestServeStopsAndFailsWhenTheApplicationShutsDown(t *testing.T) {
	fake := &failingApp{exitCode: 1}
	newApp = func(context.Context, config.Config) application { return fake }
	t.Cleanup(func() { newApp = defaultApp })
	cfg, err := config.Load(config.MapLookup(nil))
	require.NoError(t, err)

	require.ErrorContains(t, serve(context.Background(), cfg), "exit code 1")
	assert.True(t, fake.stopped)
}

// Not parallel: it swaps every external constructor.
func TestMalformedConfigurationAllocatesNoResources(t *testing.T) {
	var migrators, sqsClients, applications int
	newMigrator = func(string) (migrator, error) { migrators++; return &fakeMigrator{}, nil }
	newSQSClient = func(context.Context, config.SQS) (sqsadapter.API, error) { sqsClients++; return fakeAPI{}, nil }
	newApp = func(context.Context, config.Config) application { applications++; return &blockingStartApp{} }
	t.Cleanup(func() {
		newMigrator = defaultMigrator
		newSQSClient = bootstrap.NewSQSClient
		newApp = defaultApp
	})

	require.Error(t, serveCmd(context.Background(), config.MapLookup(map[string]string{"DB_MAX_CONNS": "0"})))
	require.Error(t, migrateCmd(config.MapLookup(map[string]string{"DB_MAX_CONNS": "0"}), []string{"up"}, &bytes.Buffer{}))
	require.Error(t, provision(context.Background(), config.MapLookup(map[string]string{"SQS_MAX_MESSAGES": "0"}), &bytes.Buffer{}))
	require.ErrorContains(t,
		serveCmd(context.Background(), config.MapLookup(map[string]string{"SQS_SENDER_PROVIDERS": "missing-equals"})),
		"SQS_SENDER_PROVIDERS",
	)
	assert.Zero(t, migrators)
	assert.Zero(t, sqsClients)
	assert.Zero(t, applications)
}

func TestDefaultMigratorRejectsBadURL(t *testing.T) {
	t.Parallel()
	_, err := defaultMigrator("postgres://%%%")
	require.Error(t, err)
}

func TestPrintVersionError(t *testing.T) {
	t.Parallel()
	boom := errors.New("boom")
	require.ErrorIs(t, printVersion(&fakeMigrator{err: boom}, &bytes.Buffer{}), boom)
}
