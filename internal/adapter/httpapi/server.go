// Package httpapi exposes the use cases over HTTP.
package httpapi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"

	"github.com/Pantani/backend-challenge-go/internal/adapter/auth"
	"github.com/Pantani/backend-challenge-go/internal/app"
	"github.com/Pantani/backend-challenge-go/internal/domain/wager"
	"github.com/Pantani/backend-challenge-go/internal/domain/wallet"
	"github.com/Pantani/backend-challenge-go/internal/observability"
)

// WalletUseCases are the internal wallet operations.
type WalletUseCases interface {
	// Open creates a wallet with its opening balance.
	Open(ctx context.Context, cmd app.OpenWalletCommand) (*wallet.Wallet, error)
	// Get loads a wallet by id.
	Get(ctx context.Context, id uuid.UUID) (*wallet.Wallet, error)
	// Ledger pages the wallet's ledger; limit 0 selects the default page size.
	Ledger(ctx context.Context, id uuid.UUID, cursor string, limit int) (app.LedgerPage, error)
	// Reconcile compares the stored balance with the ledger.
	Reconcile(ctx context.Context, id uuid.UUID) (app.Reconciliation, error)
}

// WagerUseCases are the provider operations.
type WagerUseCases interface {
	// Submit processes a provider operation idempotently.
	Submit(ctx context.Context, cmd app.SubmitCommand) (app.SubmitResult, error)
	// Get loads a transaction visible to the caller.
	Get(ctx context.Context, caller app.Caller, id uuid.UUID) (*wager.Transaction, error)
	// GetByExternal loads a transaction by its provider identifier.
	GetByExternal(ctx context.Context, caller app.Caller, providerID, externalID string) (*wager.Transaction, error)
}

// TokenVerifier validates bearer tokens.
type TokenVerifier interface {
	// Verify checks a raw bearer token and returns its principal.
	Verify(ctx context.Context, raw string) (auth.Principal, error)
}

// HealthCheck probes a dependency for readiness.
type HealthCheck struct {
	// Name identifies the dependency in the readiness response.
	Name string
	// Check returns an error when the dependency is unavailable.
	Check func(ctx context.Context) error
}

// RequestMetrics records served requests.
type RequestMetrics interface {
	// HTTPRequest records one request by route pattern, status and duration.
	HTTPRequest(route string, code int, d time.Duration)
}

// Deps are the collaborators of the HTTP handler.
type Deps struct {
	// Wallets serves the internal wallet routes.
	Wallets WalletUseCases
	// Wagers serves the provider routes.
	Wagers WagerUseCases
	// Verifier authenticates bearer tokens.
	Verifier TokenVerifier
	// Checks are the readiness probes, run concurrently on every readiness call.
	Checks []HealthCheck
	// Metrics receives request metrics.
	Metrics RequestMetrics
	// MetricsHandler serves GET /metrics.
	MetricsHandler http.Handler
	// Logger receives access and error logs.
	Logger *slog.Logger
	// ReadyTimeout bounds each readiness check.
	ReadyTimeout time.Duration
}

// handler holds the collaborators behind every route.
type handler struct {
	Deps
}

// registeredRoute marks handlers owned by this mux. ServeMux.Handler returns
// an unmarked generated handler for canonicalization redirects, allowing the
// outer adapter to classify them without invoking a wildcard route directly.
type registeredRoute struct {
	handler http.Handler
}

func (r *registeredRoute) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.handler.ServeHTTP(w, req)
}

// access decides whether an authenticated principal may call a route.
type access func(p auth.Principal) bool

func internalOnly(p auth.Principal) bool       { return p.IsInternal() }
func providerOnly(p auth.Principal) bool       { return p.IsProvider() }
func providerOrInternal(p auth.Principal) bool { return p.IsInternal() || p.IsProvider() }

// routeUnmatched is the metrics label of requests that match no route, so
// arbitrary paths never create new label values.
const routeUnmatched = "unmatched"

// routeRedirect is the metrics label of the mux's path-cleaning redirects.
const routeRedirect = "redirect"

// NewHandler builds the router. Business routes require a valid bearer token;
// health checks and metrics are public. Every response, including 404 and
// 405, is JSON, carries the correlation id and is logged and measured.
func NewHandler(d Deps) http.Handler {
	h := &handler{Deps: d}
	mux := http.NewServeMux()
	h.route(mux, "GET /health/live", h.live)
	h.route(mux, "GET /health/ready", h.ready)
	mux.Handle("GET /metrics", &registeredRoute{handler: d.MetricsHandler})
	h.route(mux, "POST /wallets", h.secured(internalOnly, h.openWallet))
	h.route(mux, "GET /wallets/{walletId}", h.secured(internalOnly, h.getWallet))
	h.route(mux, "GET /wallets/{walletId}/ledger", h.secured(internalOnly, h.getLedger))
	h.route(mux, "POST /wallets/{walletId}/reconciliation", h.secured(internalOnly, h.reconcile))
	h.route(mux, "POST /wagering/transactions", h.secured(providerOnly, h.submit))
	h.route(mux, "GET /wagering/transactions/{transactionId}", h.secured(providerOrInternal, h.getTransaction))
	h.route(mux, "GET /providers/{providerId}/wagering/transactions/{externalTransactionId}",
		h.secured(providerOrInternal, h.getTransactionByExternal))
	unmatched := h.observe(routeUnmatched, h.unmatched(mux))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next, pattern := mux.Handler(r)
		switch {
		case isMuxRedirect(next, r):
			// ServeMux owns canonicalization and path binding. Handler is
			// consulted only to identify its generated redirect.
			h.observe(routeRedirect, mux.ServeHTTP).ServeHTTP(w, r)
		case pattern == "":
			unmatched.ServeHTTP(w, r)
		default:
			mux.ServeHTTP(w, r)
		}
	})
}

func isRegisteredRoute(next http.Handler) bool {
	_, ok := next.(*registeredRoute)
	return ok
}

// isMuxRedirect probes only handlers generated by ServeMux. Registered route
// handlers are never invoked here because wildcard values are bound only by
// ServeMux.ServeHTTP.
func isMuxRedirect(next http.Handler, r *http.Request) bool {
	if isRegisteredRoute(next) {
		return false
	}
	probe := &headerProbe{header: http.Header{}}
	next.ServeHTTP(probe, r)
	return probe.status >= http.StatusMultipleChoices &&
		probe.status < http.StatusBadRequest && probe.header.Get("Location") != ""
}

// route registers an observed handler under a mux pattern.
func (h *handler) route(mux *http.ServeMux, pattern string, next http.HandlerFunc) {
	mux.Handle(pattern, &registeredRoute{handler: h.observe(pattern, next)})
}

// headerProbe captures what the mux's fallback handler would answer (404 or
// 405 with Allow) without writing anything to the client.
type headerProbe struct {
	header http.Header
	status int
}

func (p *headerProbe) Header() http.Header         { return p.header }
func (p *headerProbe) Write(b []byte) (int, error) { return len(b), nil }
func (p *headerProbe) WriteHeader(code int)        { p.status = code }

// unmatched answers requests that match no route with the contract's JSON
// error, keeping the Allow header that the mux computes for 405.
func (h *handler) unmatched(mux *http.ServeMux) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		fallback, _ := mux.Handler(r)
		probe := &headerProbe{header: http.Header{}}
		fallback.ServeHTTP(probe, r)
		if allow := probe.header.Get(headerAllow); allow != "" {
			w.Header().Set(headerAllow, allow)
		}
		if probe.status == http.StatusMethodNotAllowed {
			writeError(w, http.StatusMethodNotAllowed, CodeMethodNotAllowed, "method not allowed")
			return
		}
		writeError(w, http.StatusNotFound, CodeNotFound, "route not found")
	}
}

// statusRecorder captures the response status for logs and metrics and
// whether anything reached the client, so panic recovery never appends a
// second body to a partial response.
type statusRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

// WriteHeader records the status before forwarding it.
func (r *statusRecorder) WriteHeader(code int) {
	if !r.wroteHeader {
		r.status, r.wroteHeader = code, true
	}
	r.ResponseWriter.WriteHeader(code)
}

// Write marks the response as started; the implicit status is 200.
func (r *statusRecorder) Write(b []byte) (int, error) {
	r.wroteHeader = true
	return r.ResponseWriter.Write(b)
}

// Unwrap exposes the underlying writer to http.ResponseController.
func (r *statusRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

// observe assigns the correlation id, recovers panics and emits the access
// log and metrics of every request.
func (h *handler) observe(route string, next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		correlation := correlationID(r)
		w.Header().Set(headerCorrelationID, correlation)
		ctx := observability.WithAttrs(r.Context(), slog.String("correlationId", correlation))
		fields := &logFields{}
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		defer func() {
			if p := recover(); p != nil {
				h.recovered(ctx, rec, p)
			}
			h.Metrics.HTTPRequest(route, rec.status, time.Since(start))
			args := append([]any{"route", route, "status", rec.status, "durationMs", time.Since(start).Milliseconds()}, fields.attrs...)
			h.Logger.InfoContext(ctx, "http request", args...)
		}()
		ctx = context.WithValue(context.WithValue(ctx, correlationKey{}, correlation), logFieldsKey{}, fields)
		next(rec, r.WithContext(ctx))
	})
}

// logFields collects what the inner handlers learn about a request (caller,
// wallet, transaction) so the access log written by observe carries it too.
// A request is served by a single goroutine, so it needs no lock.
type logFields struct{ attrs []any }

// logFieldsKey carries the request's *logFields in its context.
type logFieldsKey struct{}

// annotate returns a request context whose log records carry attrs, and adds
// them to the request's access log.
func annotate(ctx context.Context, attrs ...slog.Attr) context.Context {
	if f, ok := ctx.Value(logFieldsKey{}).(*logFields); ok {
		for _, a := range attrs {
			f.attrs = append(f.attrs, a)
		}
	}
	return observability.WithAttrs(ctx, attrs...)
}

// recovered turns a handler panic into a 500 unless the response already
// started; http.ErrAbortHandler is re-raised as net/http expects.
func (h *handler) recovered(ctx context.Context, rec *statusRecorder, p any) {
	if err, ok := p.(error); ok && errors.Is(err, http.ErrAbortHandler) {
		panic(p)
	}
	h.Logger.ErrorContext(ctx, "panic serving request", "panic", p)
	if rec.wroteHeader {
		return
	}
	writeError(rec, http.StatusInternalServerError, CodeInternalError, "internal error")
}

// correlationKey carries the correlation id in the request context.
type correlationKey struct{}

// correlationPattern is the accepted shape of a client-supplied correlation
// id: short and free of characters that could break logs.
var correlationPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)

// correlationID keeps a well-formed client correlation id or issues one.
func correlationID(r *http.Request) string {
	if id := r.Header.Get(headerCorrelationID); correlationPattern.MatchString(id) {
		return id
	}
	return uuid.NewString()
}

// correlationFrom reads the correlation id assigned by observe.
func correlationFrom(ctx context.Context) string {
	id, _ := ctx.Value(correlationKey{}).(string)
	return id
}

// principalKey carries the authenticated principal in the request context.
type principalKey struct{}

// principalFrom reads the principal stored by secured.
func principalFrom(ctx context.Context) auth.Principal {
	p, _ := ctx.Value(principalKey{}).(auth.Principal)
	return p
}

// secured authenticates the bearer token and enforces the route policy.
// Unauthorized calls never reach the use cases, so they have no effect.
func (h *handler) secured(allowed access, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		raw, ok := bearerToken(r)
		if !ok {
			unauthorized(w, bearerChallenge, "missing bearer token")
			return
		}
		p, err := h.Verifier.Verify(r.Context(), raw)
		if err != nil {
			h.Logger.DebugContext(r.Context(), "bearer token rejected", "error", err)
			unauthorized(w, bearerChallengeInvalid, "invalid or expired token")
			return
		}
		if !allowed(p) {
			writeError(w, http.StatusForbidden, CodeForbidden, "operation not allowed for this client")
			return
		}
		ctx := annotate(r.Context(), slog.String("clientId", p.ClientID), slog.String("providerId", p.ProviderID))
		next(w, r.WithContext(context.WithValue(ctx, principalKey{}, p)))
	}
}

// unauthorized writes a 401 with the RFC 6750 challenge.
func unauthorized(w http.ResponseWriter, challenge, msg string) {
	w.Header().Set(headerWWWAuthenticate, challenge)
	writeError(w, http.StatusUnauthorized, CodeUnauthorized, msg)
}

// bearerToken extracts the credentials of an Authorization header whose
// scheme is "Bearer", compared case-insensitively as RFC 6750 requires.
func bearerToken(r *http.Request) (string, bool) {
	scheme, raw, found := strings.Cut(r.Header.Get("Authorization"), " ")
	raw = strings.TrimSpace(raw)
	return raw, found && strings.EqualFold(scheme, "Bearer") && raw != ""
}

// fail logs an error and writes its contract mapping.
func (h *handler) fail(w http.ResponseWriter, r *http.Request, err error) {
	status, code, msg := classify(err)
	level := slog.LevelInfo
	if status >= http.StatusInternalServerError {
		level = slog.LevelError
	}
	h.Logger.Log(r.Context(), level, "request failed", "code", code, "error", err)
	writeError(w, status, code, msg)
}

// live answers the liveness probe.
func (h *handler) live(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "alive"})
}

// ready runs every configured HealthCheck concurrently, each with the full
// ReadyTimeout; any failure makes the instance unready.
func (h *handler) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), h.ReadyTimeout)
	defer cancel()
	results := make([]string, len(h.Checks))
	var g errgroup.Group
	for i, c := range h.Checks {
		g.Go(func() error {
			results[i] = h.probe(ctx, c)
			return nil
		})
	}
	_ = g.Wait()
	status, checks := http.StatusOK, make(map[string]string, len(h.Checks))
	for i, c := range h.Checks {
		checks[c.Name] = results[i]
		if results[i] != "ok" {
			status = http.StatusServiceUnavailable
		}
	}
	writeJSON(w, status, map[string]any{"status": readiness(status), "checks": checks})
}

// probe runs one readiness check and renders its outcome.
func (h *handler) probe(ctx context.Context, c HealthCheck) string {
	if err := runCheck(ctx, c); err != nil {
		h.Logger.WarnContext(ctx, "readiness check failed", "check", c.Name, "error", err)
		return "unavailable"
	}
	return "ok"
}

// runCheck contains dependency panics inside the probe goroutine so one faulty
// readiness implementation makes the instance unavailable instead of exiting.
func runCheck(ctx context.Context, c HealthCheck) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("readiness %s panicked: %v", c.Name, recovered)
		}
	}()
	return c.Check(ctx)
}

// readiness renders the aggregate readiness state.
func readiness(status int) string {
	if status == http.StatusOK {
		return "ready"
	}
	return "unready"
}
