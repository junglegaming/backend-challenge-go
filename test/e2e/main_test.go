//go:build e2e

// Package e2e_test runs the compiled wallet binary as three independent
// processes (own memory and connections) against real PostgreSQL,
// LocalStack and Keycloak containers, and drives them over HTTP and SQS.
package e2e_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	sqsadapter "github.com/Pantani/backend-challenge-go/internal/adapter/sqs"
	"github.com/Pantani/backend-challenge-go/internal/testutil"
	"github.com/Pantani/backend-challenge-go/test/testenv"
)

var (
	env       *testenv.Env
	pool      *pgxpool.Pool
	binary    string
	sqsClient sqsadapter.API
	queues    sqsadapter.Queues
	instances []*instance
	tokens    = &tokenCache{values: make(map[string]cachedToken)}
	fixture   = &e2eFixture{}
)

func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	var err error
	if env, err = testenv.Start(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "start containers:", err)
		cancel()
		os.Exit(1)
	}
	code := setupAndRun(ctx, m)
	cancel()
	cleanup, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
	if err := errors.Join(fixture.Close(cleanup), env.Stop(cleanup)); err != nil {
		fmt.Fprintln(os.Stderr, "cleanup:", err)
		code = 1
	}
	cleanupCancel()
	os.Exit(code)
}

func setupAndRun(ctx context.Context, m *testing.M) int {
	if err := prepare(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "prepare:", err)
		return 1
	}
	return m.Run()
}

// e2eFixture owns everything acquired during setup, including partial setup
// and every process run (a restarted instance leaves its old run here).
type e2eFixture struct {
	tempDir string
	pool    *pgxpool.Pool
	runs    []*processRun
}

// Close stops every process run, then releases the pool and the build dir.
func (f *e2eFixture) Close(ctx context.Context) error {
	_, err := testenv.Parallel(len(f.runs), func(index int) (struct{}, error) {
		return struct{}{}, f.runs[index].stop(ctx)
	})
	if f.pool != nil {
		f.pool.Close()
	}
	if f.tempDir != "" {
		err = errors.Join(err, os.RemoveAll(f.tempDir))
	}
	return err
}

// prepare builds the binary, migrates, provisions the queues and starts
// three instances.
func prepare(ctx context.Context) error {
	dir, err := os.MkdirTemp("", "wallet-e2e")
	if err != nil {
		return err
	}
	fixture.tempDir = dir
	binary = filepath.Join(dir, "wallet")
	root, err := testenv.RepoRoot(".")
	if err != nil {
		return err
	}
	build := exec.CommandContext(ctx, "go", "build", "-o", binary, "./cmd/wallet")
	build.Dir = root
	build.WaitDelay = time.Second
	if out, err := build.CombinedOutput(); err != nil {
		return fmt.Errorf("build: %w: %s", err, out)
	}
	// Migrations run as the schema owner; the instances use the runtime login.
	migrate := command(ctx, "migrate", "up")
	migrate.Env = append(migrate.Env, "DATABASE_URL="+env.OwnerDatabaseURL)
	for _, cmd := range []*exec.Cmd{migrate, command(ctx, "provision-queues")} {
		if out, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("%v: %w: %s", cmd.Args[1:], err, out)
		}
	}
	return connect(ctx)
}

func connect(ctx context.Context) error {
	var err error
	if pool, err = pgxpool.New(ctx, env.DatabaseURL); err != nil {
		return err
	}
	fixture.pool = pool
	if sqsClient, err = sqsadapter.NewClient(ctx, sqsadapter.ClientConfig{Region: "us-east-1", Endpoint: env.SQSEndpoint}); err != nil {
		return err
	}
	names := sqsadapter.QueueNames{Input: "wager-transactions.fifo", DLQ: "wager-transactions-dlq.fifo", Events: "wallet-events.fifo"}
	if queues, err = sqsadapter.ResolveQueues(ctx, sqsClient, names); err != nil {
		return err
	}
	for i := range 3 {
		inst := &instance{name: fmt.Sprintf("app-%d", i+1)}
		if err := inst.startWith(ctx); err != nil {
			return err
		}
		instances = append(instances, inst)
	}
	return nil
}

func command(ctx context.Context, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.WaitDelay = time.Second
	cmd.Env = os.Environ()
	for k, v := range env.Vars(map[string]string{"LOG_LEVEL": "info", "SQS_VISIBILITY_TIMEOUT": "2m", "SQS_PROCESS_TIMEOUT": "8s", "SQS_ACK_TIMEOUT": "1s"}) {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	return cmd
}

// instance is one independent wallet process; a restart replaces its run.
type instance struct {
	*processRun
	name    string
	address string
}

// processRun is one started execution of the binary.
type processRun struct {
	name    string
	cmd     *exec.Cmd
	done    chan struct{} // closed once Wait returned
	waitErr error
	killed  bool  // crashed on purpose, so its exit status is expected
	killErr error // result of the kill and reap, reported again on cleanup
	// logs is written by the exec copier goroutine while the process runs.
	logs testutil.SyncBuffer
}

func (i *instance) base() string { return "http://" + i.address }

// client drives this instance with cached Keycloak tokens.
func (i *instance) client() testenv.Client { return testenv.Client{Base: i.base(), Token: tokens.get} }

// startWith launches the process; it is not bound to ctx, which only bounds
// the startup (a test must be able to kill it explicitly).
func (i *instance) startWith(ctx context.Context) error {
	cmd := command(context.WithoutCancel(ctx), "serve")
	cmd.Env = append(cmd.Env, "HTTP_ADDR=127.0.0.1:0", "INSTANCE_ID="+i.name)
	run := &processRun{name: i.name, cmd: cmd, done: make(chan struct{})}
	cmd.Stdout, cmd.Stderr = &run.logs, &run.logs
	if err := cmd.Start(); err != nil {
		return err
	}
	i.processRun, i.address = run, ""
	fixture.runs = append(fixture.runs, run)
	go func() {
		run.waitErr = cmd.Wait()
		close(run.done)
	}()
	return i.waitReady(ctx)
}

func (i *instance) waitReady(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-i.done:
			return errors.Join(fmt.Errorf("%s exited before readiness: %s", i.name, i.logs.String()), i.waitErr)
		case <-ctx.Done():
			return fmt.Errorf("%s readiness: %w: %s", i.name, ctx.Err(), i.logs.String())
		case <-ticker.C:
			if i.ready(ctx) {
				return nil
			}
		}
	}
}

func (i *instance) ready(ctx context.Context) bool {
	if i.address == "" {
		i.address = listeningAddress(i.logs.String())
	}
	if i.address == "" {
		return false
	}
	status, err := get(ctx, i.base()+"/health/ready")
	return err == nil && status == http.StatusOK
}

func listeningAddress(logs string) string {
	for _, line := range strings.Split(logs, "\n") {
		var record struct {
			Message string `json:"msg"`
			Address string `json:"addr"`
		}
		if json.Unmarshal([]byte(line), &record) != nil {
			continue
		}
		if record.Message == "http server listening" {
			return record.Address
		}
	}
	return ""
}

func get(ctx context.Context, url string) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, err
	}
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return 0, err
	}
	return resp.StatusCode, resp.Body.Close()
}

// kill simulates an abrupt crash (SIGKILL) and reaps the child.
func (r *processRun) kill(ctx context.Context) error {
	r.killed = true
	r.killErr = errors.Join(processSignalError(r.cmd.Process.Kill()), waitDone(ctx, r.done))
	return r.killErr
}

// stop sends SIGTERM and falls back to kill after 15 seconds, which covers
// the service's own graceful shutdown budget. A process that already exited
// on its own (not killed by a test) fails the cleanup with its exit status.
func (r *processRun) stop(ctx context.Context) error {
	if r.killed {
		return r.killErr
	}
	select {
	case <-r.done:
		if r.waitErr == nil {
			return fmt.Errorf("%s exited unexpectedly before cleanup", r.name)
		}
		return r.waitErr
	default:
	}
	graceful, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	signalErr := processSignalError(r.cmd.Process.Signal(syscall.SIGTERM))
	if err := waitDone(graceful, r.done); err != nil {
		return errors.Join(signalErr, fmt.Errorf("%s graceful shutdown: %w", r.name, err), r.kill(ctx))
	}
	return errors.Join(signalErr, r.waitErr)
}

func waitDone(ctx context.Context, done <-chan struct{}) error {
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func processSignalError(err error) error {
	if errors.Is(err, os.ErrProcessDone) {
		return nil
	}
	return err
}

// tokenRefresh renews cached tokens well before their 5-minute lifetime.
const tokenRefresh = 4 * time.Minute

type cachedToken struct {
	value    string
	issuedAt time.Time
}

// tokenCache shares Keycloak tokens across requests and instances.
type tokenCache struct {
	mu     sync.Mutex
	values map[string]cachedToken
}

// get returns a cached token, fetching a new one when it is about to expire.
func (c *tokenCache) get(ctx context.Context, client string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if tok, ok := c.values[client]; ok && time.Since(tok.issuedAt) < tokenRefresh {
		return tok.value, nil
	}
	value, err := env.Token(ctx, client)
	if err != nil {
		return "", err
	}
	c.values[client] = cachedToken{value: value, issuedAt: time.Now()}
	return value, nil
}

// call sends a request to one instance, failing the test on transport errors.
// It must run on the test goroutine; concurrent code uses inst.client().Do.
func call(t *testing.T, inst *instance, method, path, client, body string, headers map[string]string) testenv.Response {
	t.Helper()
	return inst.client().Call(t, method, path, client, body, headers)
}
