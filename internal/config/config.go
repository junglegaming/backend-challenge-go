// Package config loads and validates the service configuration from the
// environment. Every variable has a local default (Docker Compose ports), so
// the binary runs against the local stack with no configuration at all.
package config

import (
	"errors"
	"fmt"
	"math"
	"os"
	"slices"
	"strconv"
	"time"

	sqsadapter "github.com/Pantani/backend-challenge-go/internal/adapter/sqs"
	"github.com/Pantani/backend-challenge-go/internal/observability"
)

// SQS API limits.
const (
	// maxSQSDuration is the longest visibility timeout SQS accepts.
	maxSQSDuration = 12 * time.Hour
	maxSQSWaitTime = 20 * time.Second
)

// Database is the PostgreSQL subset of the configuration, enough for
// `wallet migrate`.
type Database struct {
	// DatabaseURL is the postgres:// connection string.
	DatabaseURL string
	// DBMaxConns caps the open connections of the pool.
	DBMaxConns int
	// DBLockTimeout bounds how long a statement waits for a row lock.
	DBLockTimeout time.Duration
	// DBStatementTimeout bounds how long a single statement may run.
	DBStatementTimeout time.Duration
}

// SQS is the AWS subset of the configuration, enough for
// `wallet provision-queues`.
type SQS struct {
	// AWSRegion is the SQS region; AWSEndpoint overrides the endpoint
	// (LocalStack locally; empty, the default, means real AWS).
	AWSRegion   string
	AWSEndpoint string
	// SQSInputQueue, SQSDLQ and SQSEventsQueue are FIFO queue names.
	SQSInputQueue  string
	SQSDLQ         string
	SQSEventsQueue string
	// SQSConsumerName is the source recorded on consumed transactions.
	SQSConsumerName string
	// SQSConsumers is how many consumer goroutines poll the input queue.
	SQSConsumers int
	// SQSMaxMessages is the batch size of one receive (1..10).
	SQSMaxMessages int
	// SQSWaitTime is the long-polling wait (0..20s). SQS durations are sent in
	// whole seconds.
	SQSWaitTime time.Duration
	// SQSVisibilityTimeout hides a received batch from other consumers;
	// SQSProcessTimeout bounds one message and SQSAckTimeout its broker
	// follow-up. Visibility must exceed their sum multiplied by the maximum
	// batch size because messages are handled serially.
	SQSVisibilityTimeout time.Duration
	SQSProcessTimeout    time.Duration
	SQSAckTimeout        time.Duration
	// SQSRetryBase and SQSRetryMax bound the visibility backoff of a retried
	// message.
	SQSRetryBase time.Duration
	SQSRetryMax  time.Duration
	// SQSMaxReceiveCount is how many receives a transiently failing message
	// gets before the consumer dead-letters it with its failure reason. The
	// queue redrive policy is provisioned with headroom above it (see
	// sqsadapter.RedriveMaxReceiveCount) and only acts as a safety net.
	SQSMaxReceiveCount int
	// SQSSenderProviders binds SQS SenderIds to providers
	// ("senderId=provider-a|provider-b;otherId=*").
	SQSSenderProviders string
}

// Config is the complete service configuration.
type Config struct {
	// InstanceID identifies this process: it owns the outbox leases and is
	// attached to every log record.
	InstanceID string
	// LogLevel is debug, info, warn or error (case-insensitive).
	LogLevel string
	// HTTPAddr is the listen address (":0" picks a free port).
	HTTPAddr string
	// StartupTimeout bounds dependency construction and application start.
	StartupTimeout time.Duration
	// ShutdownTimeout bounds the whole graceful stop: the consumers stop
	// fetching, HTTP drains, the workers finish and the pool closes.
	ShutdownTimeout time.Duration
	// ReadyTimeout bounds the /health/ready dependency checks and is also
	// added to the start budget of the application.
	ReadyTimeout time.Duration
	// ConflictRetries is how many times a lost optimistic-lock race is
	// retried before failing the request (0 disables retries).
	ConflictRetries int

	Database
	SQS

	// OIDCIssuer is the expected "iss"; OIDCJWKSURL is where the signing
	// keys are fetched; OIDCAudience is the expected "aud" of this API.
	OIDCIssuer   string
	OIDCJWKSURL  string
	OIDCAudience string

	// PendingInterval is how often due PENDING_REFERENCE operations are
	// resolved; the delay between attempts grows from PendingBaseDelay to
	// PendingMaxDelay, up to PendingMaxAttempts, PendingBatch at a time.
	PendingInterval    time.Duration
	PendingBaseDelay   time.Duration
	PendingMaxDelay    time.Duration
	PendingMaxAttempts int
	PendingBatch       int

	// OutboxInterval is how often the relay claims OutboxBatch records.
	OutboxInterval time.Duration
	OutboxBatch    int
	// OutboxLease is how long a claim is exclusive to this instance; a
	// crashed publisher's records become claimable again after it expires.
	OutboxLease time.Duration
	// OutboxRetryBase and OutboxRetryMax bound the publication backoff.
	OutboxRetryBase time.Duration
	OutboxRetryMax  time.Duration
	// OutboxPublishTimeout bounds one broker publication.
	OutboxPublishTimeout time.Duration
	// OutboxFinalizeTimeout is shared by attempt accounting and its durable outcome.
	OutboxFinalizeTimeout time.Duration
	// OutboxMaxAttempts quarantines explicitly permanent payload failures.
	// Transient publication errors have no attempt limit.
	OutboxMaxAttempts int
}

// Lookup reads a variable; os.LookupEnv in production, a map in tests.
type Lookup func(key string) (string, bool)

// MapLookup returns a Lookup backed by a map.
func MapLookup(values map[string]string) Lookup {
	return func(key string) (string, bool) {
		v, ok := values[key]
		return v, ok
	}
}

// FromEnv loads the configuration from the process environment.
func FromEnv() (Config, error) { return Load(os.LookupEnv) }

// Load reads every variable (with local defaults) and validates the result.
func Load(lookup Lookup) (Config, error) {
	r := &reader{lookup: lookup}
	host, _ := os.Hostname()
	c := Config{
		InstanceID: r.str("INSTANCE_ID", host), LogLevel: r.str("LOG_LEVEL", "info"),
		HTTPAddr: r.str("HTTP_ADDR", ":8080"), StartupTimeout: r.dur("STARTUP_TIMEOUT", 25*time.Second),
		ShutdownTimeout: r.dur("SHUTDOWN_TIMEOUT", 61*time.Second),
		ReadyTimeout:    r.dur("READY_TIMEOUT", 2*time.Second), ConflictRetries: r.int("CONFLICT_RETRIES", 5),
		Database: r.database(), SQS: r.sqs(),

		OIDCIssuer:   r.str("OIDC_ISSUER", "http://localhost:8180/realms/wallet"),
		OIDCJWKSURL:  r.str("OIDC_JWKS_URL", "http://localhost:8180/realms/wallet/protocol/openid-connect/certs"),
		OIDCAudience: r.str("OIDC_AUDIENCE", "wallet-api"),
	}
	r.loadWorkers(&c)
	return finish(r, c, c.validate)
}

// LoadDatabase reads and validates only the database variables.
func LoadDatabase(lookup Lookup) (Database, error) {
	r := &reader{lookup: lookup}
	d := r.database()
	return finish(r, d, d.validate)
}

// LoadSQS reads and validates only the AWS/SQS variables.
func LoadSQS(lookup Lookup) (SQS, error) {
	r := &reader{lookup: lookup}
	s := r.sqs()
	return finish(r, s, s.validate)
}

// finish reports the parse errors first (all of them) and validates otherwise.
func finish[T any](r *reader, v T, validate func() error) (T, error) {
	var zero T
	if err := errors.Join(r.errs...); err != nil {
		return zero, err
	}
	if err := validate(); err != nil {
		return zero, err
	}
	return v, nil
}

func (r *reader) database() Database {
	return Database{
		DatabaseURL: r.str("DATABASE_URL", "postgres://wallet:wallet@localhost:5432/wallet?sslmode=disable"),
		DBMaxConns:  r.int("DB_MAX_CONNS", 20), DBLockTimeout: r.dur("DB_LOCK_TIMEOUT", 5*time.Second),
		DBStatementTimeout: r.dur("DB_STATEMENT_TIMEOUT", 10*time.Second),
	}
}

func (r *reader) sqs() SQS {
	return SQS{
		AWSRegion: r.str("AWS_REGION", "us-east-1"), AWSEndpoint: r.str("AWS_ENDPOINT_URL", ""),
		SQSInputQueue:   r.str("SQS_INPUT_QUEUE", "wager-transactions.fifo"),
		SQSDLQ:          r.str("SQS_DLQ", "wager-transactions-dlq.fifo"),
		SQSEventsQueue:  r.str("SQS_EVENTS_QUEUE", "wallet-events.fifo"),
		SQSConsumerName: r.str("SQS_CONSUMER_NAME", "wager-transactions-consumer"),
		SQSConsumers:    r.int("SQS_CONSUMERS", 2), SQSMaxMessages: r.int("SQS_MAX_MESSAGES", 10),
		SQSWaitTime: r.dur("SQS_WAIT_TIME", 10*time.Second), SQSVisibilityTimeout: r.dur("SQS_VISIBILITY_TIMEOUT", 5*time.Minute),
		SQSProcessTimeout: r.dur("SQS_PROCESS_TIMEOUT", 20*time.Second), SQSAckTimeout: r.dur("SQS_ACK_TIMEOUT", 5*time.Second),
		SQSRetryBase: r.dur("SQS_RETRY_BASE", 2*time.Second), SQSRetryMax: r.dur("SQS_RETRY_MAX", 60*time.Second),
		SQSMaxReceiveCount: r.int("SQS_MAX_RECEIVE_COUNT", 5),
		// LocalStack reports every sender as the account id 000000000000.
		SQSSenderProviders: r.str("SQS_SENDER_PROVIDERS", "000000000000=*"),
	}
}

func (r *reader) loadWorkers(c *Config) {
	c.PendingInterval, c.PendingBaseDelay = r.dur("PENDING_INTERVAL", time.Second), r.dur("PENDING_BASE_DELAY", time.Second)
	c.PendingMaxDelay, c.PendingMaxAttempts = r.dur("PENDING_MAX_DELAY", time.Minute), r.int("PENDING_MAX_ATTEMPTS", 10)
	c.PendingBatch = r.int("PENDING_BATCH", 50)
	c.OutboxInterval, c.OutboxBatch = r.dur("OUTBOX_INTERVAL", 500*time.Millisecond), r.int("OUTBOX_BATCH", 50)
	c.OutboxLease = r.dur("OUTBOX_LEASE", 30*time.Second)
	c.OutboxRetryBase, c.OutboxRetryMax = r.dur("OUTBOX_RETRY_BASE", time.Second), r.dur("OUTBOX_RETRY_MAX", time.Minute)
	c.OutboxPublishTimeout = r.dur("OUTBOX_PUBLISH_TIMEOUT", 10*time.Second)
	c.OutboxFinalizeTimeout = r.dur("OUTBOX_FINALIZE_TIMEOUT", 5*time.Second)
	c.OutboxMaxAttempts = r.int("OUTBOX_MAX_ATTEMPTS", 20)
}

// rule is one validation: ok is false when msg applies.
type rule struct {
	ok  bool
	msg string
}

// check joins the messages of every failed rule.
func check(rules []rule) error {
	var errs []error
	for _, rule := range rules {
		if !rule.ok {
			errs = append(errs, errors.New(rule.msg))
		}
	}
	return errors.Join(errs...)
}

func (d Database) validate() error {
	return check([]rule{
		{d.DatabaseURL != "", "DATABASE_URL is required"},
		{between(d.DBMaxConns, 1, math.MaxInt32), "DB_MAX_CONNS must be between 1 and 2147483647"},
		// PostgreSQL receives these in whole milliseconds, and 0 disables them.
		{pgTimeout(d.DBLockTimeout), "DB_LOCK_TIMEOUT must be between 1 and 2147483647 whole milliseconds"},
		{pgTimeout(d.DBStatementTimeout), "DB_STATEMENT_TIMEOUT must be between 1 and 2147483647 whole milliseconds"},
	})
}

func (s SQS) validate() error {
	_, senderPolicyErr := sqsadapter.ParseSenderPolicy(s.SQSSenderProviders)
	batch := time.Duration(s.SQSMaxMessages) * (s.SQSProcessTimeout + s.SQSAckTimeout)
	return errors.Join(check([]rule{
		{s.SQSConsumers > 0, "SQS_CONSUMERS must be positive"},
		{s.SQSMaxReceiveCount > 0, "SQS_MAX_RECEIVE_COUNT must be positive"},
		{between(s.SQSMaxMessages, 1, 10), "SQS_MAX_MESSAGES must be between 1 and 10"},
		// SQS takes wait, visibility and retry delays in whole seconds.
		{wholeSecondsBetween(s.SQSWaitTime, 0, maxSQSWaitTime), "SQS_WAIT_TIME must be whole seconds between 0s and 20s"},
		{wholeSecondsBetween(s.SQSVisibilityTimeout, time.Second, maxSQSDuration), "SQS_VISIBILITY_TIMEOUT must be positive whole seconds, at most 12h"},
		{positiveSQSDuration(s.SQSProcessTimeout), "SQS_PROCESS_TIMEOUT must be positive, at most 12h"},
		{positiveSQSDuration(s.SQSAckTimeout), "SQS_ACK_TIMEOUT must be positive, at most 12h"},
		{wholeSecondsBetween(s.SQSRetryBase, time.Second, time.Duration(math.MaxInt64)), "SQS_RETRY_BASE must be positive whole seconds"},
		{wholeSecondsBetween(s.SQSRetryMax, time.Second, maxSQSDuration), "SQS_RETRY_MAX must be positive whole seconds, at most 12h"},
		{s.SQSRetryBase <= s.SQSRetryMax, "SQS_RETRY_BASE must not exceed SQS_RETRY_MAX"},
		// The consumer handles a received batch serially, so the batch stays
		// invisible for its whole worst case.
		{s.SQSVisibilityTimeout > batch,
			"SQS_VISIBILITY_TIMEOUT must exceed SQS_MAX_MESSAGES * (SQS_PROCESS_TIMEOUT + SQS_ACK_TIMEOUT)"},
	}), namedSenderPolicyError(senderPolicyErr))
}

func wholeSecondsBetween(d, minimum, maximum time.Duration) bool {
	return whole(d, time.Second) && d >= minimum && d <= maximum
}

func positiveSQSDuration(d time.Duration) bool {
	return d > 0 && d <= maxSQSDuration
}

// pgTimeout reports whether d fits PostgreSQL's timeout settings: whole
// milliseconds in a positive int32 (0 would disable the timeout).
func pgTimeout(d time.Duration) bool {
	return d > 0 && d <= math.MaxInt32*time.Millisecond && whole(d, time.Millisecond)
}

// whole reports whether d is an exact multiple of unit.
func whole(d, unit time.Duration) bool {
	return d%unit == 0
}

func namedSenderPolicyError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("SQS_SENDER_PROVIDERS: %w", err)
}

// validate enforces the ranges and relationships between settings.
func (c Config) validate() error {
	_, levelErr := observability.ParseLevel(c.LogLevel)
	return errors.Join(c.Database.validate(), c.SQS.validate(), check([]rule{
		{nonEmpty(c.OIDCIssuer, c.OIDCJWKSURL, c.OIDCAudience), "OIDC_ISSUER, OIDC_JWKS_URL and OIDC_AUDIENCE are required"},
		{levelErr == nil, "LOG_LEVEL must be debug, info, warn or error"},
		{c.ConflictRetries >= 0, "CONFLICT_RETRIES must not be negative"},
		{positive(c.OutboxBatch, c.PendingBatch, c.PendingMaxAttempts, c.OutboxMaxAttempts),
			"batch sizes and attempts must be positive"},
		{c.PendingBaseDelay > 0 && c.PendingBaseDelay <= c.PendingMaxDelay, "PENDING_BASE_DELAY must be in (0, PENDING_MAX_DELAY]"},
		{positiveDurations(c.PendingInterval, c.OutboxInterval, c.OutboxLease, c.OutboxRetryBase, c.OutboxRetryMax,
			c.OutboxPublishTimeout, c.OutboxFinalizeTimeout, c.ReadyTimeout),
			"worker intervals, leases, retries and timeouts must be positive"},
		{c.StartupTimeout > 0, "STARTUP_TIMEOUT must be positive"},
		{c.ShutdownTimeout > 0, "SHUTDOWN_TIMEOUT must be positive"},
		{c.OutboxRetryBase <= c.OutboxRetryMax, "OUTBOX_RETRY_BASE must not exceed OUTBOX_RETRY_MAX"},
		// A claimed record must be published and finalized before its lease
		// lets another instance claim it.
		{c.OutboxPublishTimeout < c.OutboxLease-c.OutboxFinalizeTimeout,
			"OUTBOX_PUBLISH_TIMEOUT plus OUTBOX_FINALIZE_TIMEOUT must be lower than OUTBOX_LEASE"},
	}))
}

func nonEmpty(values ...string) bool {
	return !slices.Contains(values, "")
}

func positive(values ...int) bool {
	return !slices.ContainsFunc(values, func(v int) bool { return v <= 0 })
}

func positiveDurations(values ...time.Duration) bool {
	return !slices.ContainsFunc(values, func(d time.Duration) bool { return d <= 0 })
}

func between(v, lo, hi int) bool { return v >= lo && v <= hi }

// reader accumulates parse errors so every problem is reported at once.
type reader struct {
	lookup Lookup
	errs   []error
}

// str returns the variable, or def when it is unset or empty.
func (r *reader) str(key, def string) string {
	if v, ok := r.lookup(key); ok && v != "" {
		return v
	}
	return def
}

// int parses the variable as an integer, recording a parse error.
func (r *reader) int(key string, def int) int {
	v, err := strconv.Atoi(r.str(key, strconv.Itoa(def)))
	if err != nil {
		r.errs = append(r.errs, fmt.Errorf("%s: %w", key, err))
	}
	return v
}

// dur parses the variable as a Go duration ("500ms", "30s"), recording a parse error.
func (r *reader) dur(key string, def time.Duration) time.Duration {
	v, err := time.ParseDuration(r.str(key, def.String()))
	if err != nil {
		r.errs = append(r.errs, fmt.Errorf("%s: %w", key, err))
	}
	return v
}
