package cli_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/Pantani/backend-challenge-go/internal/cli"
	"github.com/Pantani/backend-challenge-go/internal/config"
)

func run(vars map[string]string, args ...string) (code int, stdout, stderr string) {
	var out, errOut bytes.Buffer
	code = cli.Run(context.Background(), args, config.MapLookup(vars), &out, &errOut)
	return code, out.String(), errOut.String()
}

func TestUsageErrorsExitWithTwo(t *testing.T) {
	t.Parallel()
	for _, args := range [][]string{{"bogus"}, {"migrate"}, {"migrate", "sideways"}, {"migrate", "down", "zero"},
		{"serve", "extra"}, {"provision-queues", "extra"}, {"migrate", "up", "extra"}, {"migrate", "version", "extra"}, {"migrate", "down", "1", "extra"}} {
		code, stdout, stderr := run(map[string]string{"SQS_CONSUMERS": "0"}, args...)
		assert.Equal(t, cli.ExitUsage, code, args)
		assert.Empty(t, stdout, args)
		assert.Contains(t, stderr, "error: "+cli.ErrUsage.Error(), "usage errors never mention the configuration")
	}
}

func TestInvalidConfigurationExitsWithOne(t *testing.T) {
	t.Parallel()
	code, stdout, stderr := run(map[string]string{"SQS_MAX_MESSAGES": "50"}, "serve")
	assert.Equal(t, cli.ExitError, code)
	assert.Empty(t, stdout)
	assert.Contains(t, stderr, "invalid configuration")
	assert.Contains(t, stderr, "SQS_MAX_MESSAGES")

	code, _, stderr = run(map[string]string{"DB_MAX_CONNS": "0"}, "migrate", "up")
	assert.Equal(t, cli.ExitError, code)
	assert.Contains(t, stderr, "DB_MAX_CONNS")
	code, _, stderr = run(map[string]string{"SQS_WAIT_TIME": "1m"}, "provision-queues")
	assert.Equal(t, cli.ExitError, code)
	assert.Contains(t, stderr, "SQS_WAIT_TIME")
}

func TestMigrateWithUnreachableDatabase(t *testing.T) {
	t.Parallel()
	code, _, stderr := run(map[string]string{"DATABASE_URL": "postgres://u:p@127.0.0.1:1/db?sslmode=disable&connect_timeout=1"}, "migrate", "up")
	assert.Equal(t, cli.ExitError, code)
	assert.Contains(t, stderr, "error:")
}
