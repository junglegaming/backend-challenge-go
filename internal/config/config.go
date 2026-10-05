package config

import (
	"errors"
	"fmt"
	"strconv"
	"time"
)

var (
	// ErrMissingDatabaseURL is returned when DATABASE_URL is absent or empty.
	ErrMissingDatabaseURL = errors.New("config: DATABASE_URL is required")
	// ErrMissingAuthIssuer is returned when AUTH_ISSUER is absent or empty.
	ErrMissingAuthIssuer = errors.New("config: AUTH_ISSUER is required")
	// ErrMissingAuthJWKSURL is returned when AUTH_JWKS_URL is absent or empty.
	ErrMissingAuthJWKSURL = errors.New("config: AUTH_JWKS_URL is required")
	// ErrMissingSQSQueueURL is returned when SQS_QUEUE_URL is absent or empty.
	ErrMissingSQSQueueURL = errors.New("config: SQS_QUEUE_URL is required")
)

// Config holds everything the process needs from its environment.
type Config struct {
	DatabaseURL     string
	HTTPAddr        string
	PoolMaxConns    int32
	AcquireTimeout  time.Duration
	ShutdownTimeout time.Duration
	// AuthIssuer is the iss claim every accepted token must carry.
	AuthIssuer string
	// AuthJWKSURL is where the public keys are fetched. It can differ from the
	// issuer, for example an internal network address inside Docker.
	AuthJWKSURL string
	// SQSQueueURL is the FIFO queue the consumer receives from.
	SQSQueueURL string
	// SQSEndpoint overrides the SQS endpoint, for LocalStack. Empty means AWS.
	SQSEndpoint string
	AWSRegion   string
}

// Lookup has the same shape as os.LookupEnv. Taking it as a parameter keeps
// Load free of global state: tests pass a map instead of the real environment.
type Lookup func(key string) (string, bool)

// Load reads and validates the configuration. Invalid values fail here, at
// startup, instead of surfacing later as a confusing runtime error.
func Load(lookup Lookup) (Config, error) {
	dsn, ok := lookup("DATABASE_URL")
	if !ok || dsn == "" {
		return Config{}, ErrMissingDatabaseURL
	}

	issuer, ok := lookup("AUTH_ISSUER")
	if !ok || issuer == "" {
		return Config{}, ErrMissingAuthIssuer
	}

	jwksURL, ok := lookup("AUTH_JWKS_URL")
	if !ok || jwksURL == "" {
		return Config{}, ErrMissingAuthJWKSURL
	}

	queueURL, ok := lookup("SQS_QUEUE_URL")
	if !ok || queueURL == "" {
		return Config{}, ErrMissingSQSQueueURL
	}

	cfg := Config{
		DatabaseURL:     dsn,
		HTTPAddr:        ":8080",
		PoolMaxConns:    10,
		AcquireTimeout:  5 * time.Second,
		ShutdownTimeout: 10 * time.Second,
		AuthIssuer:      issuer,
		AuthJWKSURL:     jwksURL,
		SQSQueueURL:     queueURL,
		AWSRegion:       "us-east-1",
	}

	if raw, ok := lookup("HTTP_ADDR"); ok && raw != "" {
		cfg.HTTPAddr = raw
	}

	if raw, ok := lookup("SQS_ENDPOINT"); ok {
		cfg.SQSEndpoint = raw
	}

	if raw, ok := lookup("AWS_REGION"); ok && raw != "" {
		cfg.AWSRegion = raw
	}

	if raw, ok := lookup("DB_MAX_CONNS"); ok {
		n, err := strconv.ParseInt(raw, 10, 32)
		if err != nil || n <= 0 {
			return Config{}, fmt.Errorf("config: DB_MAX_CONNS must be a positive integer, got %q", raw)
		}
		cfg.PoolMaxConns = int32(n)
	}

	if raw, ok := lookup("DB_ACQUIRE_TIMEOUT"); ok {
		d, err := time.ParseDuration(raw)
		if err != nil || d <= 0 {
			return Config{}, fmt.Errorf("config: DB_ACQUIRE_TIMEOUT must be a positive duration, got %q", raw)
		}
		cfg.AcquireTimeout = d
	}

	if raw, ok := lookup("SHUTDOWN_TIMEOUT"); ok {
		d, err := time.ParseDuration(raw)
		if err != nil || d <= 0 {
			return Config{}, fmt.Errorf("config: SHUTDOWN_TIMEOUT must be a positive duration, got %q", raw)
		}
		cfg.ShutdownTimeout = d
	}

	return cfg, nil
}
