//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sqsadapter "github.com/Pantani/backend-challenge-go/internal/adapter/sqs"
	"github.com/Pantani/backend-challenge-go/internal/cli"
	"github.com/Pantani/backend-challenge-go/internal/config"
	"github.com/Pantani/backend-challenge-go/test/testenv"
)

// latestMigration is the highest migration number in the embedded set.
func latestMigration(t *testing.T) int {
	t.Helper()
	files, err := filepath.Glob(filepath.Join("..", "..", "internal", "adapter", "postgres", "migrations", "*.up.sql"))
	require.NoError(t, err)
	require.NotEmpty(t, files)
	latest := 0
	for _, f := range files {
		n, err := strconv.Atoi(strings.SplitN(filepath.Base(f), "_", 2)[0])
		require.NoError(t, err, f)
		latest = max(latest, n)
	}
	return latest
}

// runCLI runs the binary's entry point against the containers and returns
// the exit code, stdout and stderr.
func runCLI(ctx context.Context, vars map[string]string, args ...string) (code int, stdout, stderr string) {
	var out, errOut bytes.Buffer
	code = cli.Run(ctx, args, config.MapLookup(env.Vars(vars)), &out, &errOut)
	return code, out.String(), errOut.String()
}

func TestCLIMigrations(t *testing.T) {
	t.Parallel()
	db := map[string]string{"DATABASE_URL": databaseForTest(t, "cli_check")}
	ctx := context.Background()

	code, out, stderr := runCLI(ctx, db, "migrate", "up")
	require.Zero(t, code, stderr)
	assert.Empty(t, out)
	latest := latestMigration(t)
	assert.Equal(t, 1, latest)
	code, out, _ = runCLI(ctx, db, "migrate", "version")
	require.Zero(t, code)
	assert.Equal(t, fmt.Sprintf("version=%d dirty=false\n", latest), out)
	code, _, _ = runCLI(ctx, db, "migrate", "down", "1")
	require.Zero(t, code)
	code, out, _ = runCLI(ctx, db, "migrate", "version")
	require.Zero(t, code)
	assert.Equal(t, fmt.Sprintf("version=%d dirty=false\n", latest-1), out)
	code, _, _ = runCLI(ctx, db, "migrate", "down", "1")
	require.Zero(t, code)
	code, out, _ = runCLI(ctx, db, "migrate", "version")
	require.Zero(t, code)
	assert.Equal(t, "version=0 dirty=false\n", out)
	code, _, _ = runCLI(ctx, db, "migrate", "down")
	assert.Zero(t, code, "nothing left to revert is not an error")

	for _, args := range [][]string{{"migrate"}, {"migrate", "sideways"}, {"migrate", "down", "zero"}, {"unknown"}} {
		code, out, stderr := runCLI(ctx, db, args...)
		assert.Equal(t, cli.ExitUsage, code, args)
		assert.Empty(t, out, args)
		assert.Contains(t, stderr, "usage", args)
	}
	code, _, stderr = runCLI(ctx, map[string]string{"DATABASE_URL": "postgres://%%%"}, "migrate", "up")
	assert.Equal(t, cli.ExitError, code)
	assert.Contains(t, stderr, "error:")
	db["SQS_CONSUMERS"] = "0"
	code, _, stderr = runCLI(ctx, db, "migrate", "up")
	assert.Zero(t, code, "migrate reads only the database variables: %s", stderr)
}

func TestCLIProvisionQueues(t *testing.T) {
	t.Parallel()
	prefix := uuid.NewString()
	names := sqsadapter.QueueNames{Input: prefix + "-in.fifo", DLQ: prefix + "-dlq.fifo", Events: prefix + "-events.fifo"}
	cleanupCLIQueues(t, names)
	vars := queueVars(names)
	vars["DB_MAX_CONNS"] = "0"
	code, out, stderr := runCLI(context.Background(), vars, "provision-queues")
	require.Zero(t, code, "provision reads only the AWS variables: %s", stderr)
	assert.Contains(t, out, names.DLQ)

	vars["SQS_INPUT_QUEUE"] = prefix + "-not-fifo"
	code, _, stderr = runCLI(context.Background(), vars, "provision-queues")
	assert.Equal(t, cli.ExitError, code, "a FIFO queue name must end in .fifo")
	assert.Contains(t, stderr, "error:")
}

// freeAddr reserves a loopback port for a server started by someone else.
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()
	return l.Addr().String()
}

func TestCLIServeUntilCancelled(t *testing.T) {
	t.Parallel()
	_, _, names := provisionQueues(t, 3)
	vars := queueVars(names)
	vars["HTTP_ADDR"] = freeAddr(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)
	go func() {
		code, _, _ := runCLI(ctx, vars)
		done <- code
	}()
	ready := testenv.Client{Base: "http://" + vars["HTTP_ADDR"]}
	require.Eventually(t, func() bool {
		res, err := ready.Do(context.Background(), http.MethodGet, "/health/ready", "", "", nil)
		return err == nil && res.Status == http.StatusOK
	}, 30*time.Second, 200*time.Millisecond, "serve becomes ready")
	cancel()
	select {
	case code := <-done:
		assert.Zero(t, code, "graceful shutdown")
	case <-time.After(30 * time.Second):
		t.Fatal("serve did not stop")
	}

	code, _, stderr := runCLI(context.Background(), map[string]string{"SQS_INPUT_QUEUE": "missing.fifo"}, "serve")
	assert.Equal(t, cli.ExitError, code, stderr)
	assert.Contains(t, stderr, "missing.fifo")
}

// Not parallel: it changes the environment read by the AWS SDK.
func TestCLIProvisionWithBrokenAWSConfig(t *testing.T) {
	t.Setenv("AWS_PROFILE", "profile-that-does-not-exist")
	t.Setenv("AWS_CONFIG_FILE", t.TempDir()+"/missing")
	code, _, _ := runCLI(context.Background(), nil, "provision-queues")
	assert.Equal(t, cli.ExitError, code)
}
