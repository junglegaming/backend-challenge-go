package httpapi

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/google/uuid"

	"github.com/VictorM0nteiro/backend-challenge-go/internal/adapters/postgres"
)

const (
	headerIdempotencyKey = "Idempotency-Key"
	maxIdempotencyKeyLen = 255
	maxBodyBytes         = 64 << 10
)

// Server exposes the README §9 contracts. It holds no business rules: each
// handler decodes the request, checks its shape, and hands the work to the
// persistence layer, which owns the transactions and the locks.
type Server struct {
	pool      *postgres.Pool
	wallets   *postgres.WalletRepository
	wagers    *postgres.WagerReader
	processor *postgres.WagerProcessor
	auth      *Authenticator
	queue     QueueChecker
}

// NewServer wires the handlers to the adapters they need.
func NewServer(
	pool *postgres.Pool,
	wallets *postgres.WalletRepository,
	wagers *postgres.WagerReader,
	processor *postgres.WagerProcessor,
	auth *Authenticator,
	queue QueueChecker,
) *Server {
	return &Server{
		pool:      pool,
		wallets:   wallets,
		wagers:    wagers,
		processor: processor,
		auth:      auth,
		queue:     queue,
	}
}

// Handler returns the routes. Each business route is wrapped by the guard that
// matches who may call it. Health checks stay public.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("POST /wallets", s.auth.Internal(handle(s.openWallet)))
	mux.Handle("GET /wallets/{walletId}", s.auth.Internal(handle(s.getWallet)))
	mux.Handle("POST /wagering/transactions", s.auth.Provider(handle(s.submitWager)))
	mux.Handle("GET /wagering/transactions/{transactionId}", s.auth.Provider(handle(s.getWager)))
	mux.HandleFunc("GET /health/live", handle(s.live))
	mux.HandleFunc("GET /health/ready", handle(s.ready))
	return correlation(accessLog(mux))
}

// handler returns an error instead of writing it, so the mapping from errors
// to status codes lives in one place (statusFor).
type handler func(w http.ResponseWriter, r *http.Request) error

func handle(h handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := h(w, r); err != nil {
			writeError(w, r, err)
		}
	}
}

// providerFrom returns the provider the request was authenticated as. The guard
// stores it in the context before the handler runs, so the value cannot come
// from a header the caller controls.
func providerFrom(r *http.Request) (string, error) {
	p, ok := principalFrom(r.Context())
	if !ok || p.ClientID == "" {
		return "", errUnauthenticated
	}
	return p.ClientID, nil
}

// decodeJSON reads one JSON object into dst. Unknown fields and oversized
// bodies are refused, so a typo in a field name fails loudly instead of being
// ignored.
func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("%w: malformed JSON: %v", errInvalidRequest, err)
	}
	return nil
}

// pathUUID parses a path parameter as a UUID.
func pathUUID(r *http.Request, name string) (uuid.UUID, error) {
	id, err := uuid.Parse(r.PathValue(name))
	if err != nil {
		return uuid.Nil, fmt.Errorf("%w: %s must be a UUID", errInvalidRequest, name)
	}
	return id, nil
}
