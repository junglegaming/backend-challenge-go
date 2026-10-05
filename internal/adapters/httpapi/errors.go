package httpapi

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/VictorM0nteiro/backend-challenge-go/internal/adapters/postgres"
	"github.com/VictorM0nteiro/backend-challenge-go/internal/domain"
	"github.com/VictorM0nteiro/backend-challenge-go/internal/logctx"
)

// Sentinels for failures the HTTP layer detects itself. Adapter and domain
// errors keep their own sentinels.
var (
	errInvalidRequest  = errors.New("invalid request")
	errUnauthenticated = errors.New("missing or invalid bearer token")
	errNotFound        = errors.New("not found")
	errForbidden       = errors.New("forbidden")
)

type errorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type errorBody struct {
	Error errorDetail `json:"error"`
}

// statusFor maps an error to the status and code the contract documents.
// Each sentinel belongs to exactly one case, so the order of the cases does
// not change the answer.
func statusFor(err error) (int, string) {
	switch {
	case errors.Is(err, errInvalidRequest),
		errors.Is(err, domain.ErrInvalidAmount),
		errors.Is(err, domain.ErrInvalidCurrency),
		errors.Is(err, domain.ErrCurrencyMismatch),
		errors.Is(err, domain.ErrInvalidWagerTransaction):
		return http.StatusBadRequest, "invalid_request"

	case errors.Is(err, errUnauthenticated):
		return http.StatusUnauthorized, "unauthenticated"

	case errors.Is(err, errNotFound),
		errors.Is(err, postgres.ErrWalletNotFound),
		errors.Is(err, postgres.ErrWalletOwnerMismatch),
		errors.Is(err, postgres.ErrWagerNotFound):
		return http.StatusNotFound, "not_found"

	case errors.Is(err, postgres.ErrIdempotencyKeyReuse):
		return http.StatusUnprocessableEntity, "idempotency_key_reused"

	case errors.Is(err, postgres.ErrRequestInFlight):
		return http.StatusConflict, "request_in_flight"

	case errors.Is(err, postgres.ErrWalletAlreadyExists),
		errors.Is(err, postgres.ErrDuplicateLedgerEntry):
		return http.StatusConflict, "conflict"

	case errors.Is(err, postgres.ErrDuplicateExternalTransaction):
		return http.StatusConflict, "duplicate_operation"

	case errors.Is(err, postgres.ErrWalletVersionConflict):
		return http.StatusConflict, "concurrent_update"

	case postgres.IsUnavailable(err):
		// The database could not be reached or timed out. Nothing was applied,
		// so the client may retry the same request with the same key.
		return http.StatusServiceUnavailable, "temporarily_unavailable"

	case errors.Is(err, errForbidden):
		return http.StatusForbidden, "forbidden"

	default:
		return http.StatusInternalServerError, "internal"
	}
}

// writeError answers err. A 500 or a 503 is logged with its cause and answered
// with a fixed message, so database details never reach the client. The error
// code is added to the request's log context, so the access log line carries it.
func writeError(w http.ResponseWriter, r *http.Request, err error) {
	status, code := statusFor(err)
	ctx := r.Context()
	logctx.Add(ctx, slog.String("errorCode", code))

	message := err.Error()
	switch status {
	case http.StatusInternalServerError:
		slog.ErrorContext(ctx, "request failed", "error", err)
		message = "internal error"
	case http.StatusServiceUnavailable:
		slog.WarnContext(ctx, "dependency unavailable", "error", err)
		message = "service temporarily unavailable, retry with the same Idempotency-Key"
	}
	writeJSON(w, status, errorBody{Error: errorDetail{Code: code, Message: message}})
}

// writeJSON sets the status and encodes body. A failed encode happens after
// the status is sent, so it can only be logged.
func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		slog.Error("write response", "error", err)
	}
}
