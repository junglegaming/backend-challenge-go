package httpapi

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"net/http"

	"github.com/Pantani/backend-challenge-go/internal/app"
	"github.com/Pantani/backend-challenge-go/internal/contract"
	"github.com/Pantani/backend-challenge-go/internal/domain/wager"
)

// maxBodyBytes bounds request bodies.
const maxBodyBytes = 64 << 10

// Stable error codes of the HTTP contract.
const (
	CodeInvalidRequest       = "INVALID_REQUEST"
	CodeUnauthorized         = "UNAUTHORIZED"
	CodeForbidden            = "FORBIDDEN"
	CodeNotFound             = "NOT_FOUND"
	CodeMethodNotAllowed     = "METHOD_NOT_ALLOWED"
	CodePayloadTooLarge      = "PAYLOAD_TOO_LARGE"
	CodeUnsupportedMediaType = "UNSUPPORTED_MEDIA_TYPE"
	CodeWalletNotFound       = "WALLET_NOT_FOUND"
	CodeTransactionNotFound  = "TRANSACTION_NOT_FOUND"
	CodeWalletExists         = "WALLET_ALREADY_EXISTS"
	CodeIdempotencyConflict  = "IDEMPOTENCY_CONFLICT"
	CodeDuplicateExternal    = "DUPLICATE_EXTERNAL_TRANSACTION"
	CodeServiceUnavailable   = "SERVICE_UNAVAILABLE"
	CodeInternalError        = "INTERNAL_ERROR"
)

// Header names and fixed header values of the contract.
const (
	retryAfterSeconds      = "1"
	contentTypeJSON        = "application/json"
	headerIdempotencyKey   = "Idempotency-Key"
	headerCorrelationID    = "X-Correlation-Id"
	headerRetryAfter       = "Retry-After"
	headerWWWAuthenticate  = "WWW-Authenticate"
	headerContentType      = "Content-Type"
	headerAllow            = "Allow"
	bearerChallenge        = `Bearer realm="wallet-api"`                        //nolint:gosec // RFC 6750 challenge, not a credential
	bearerChallengeInvalid = `Bearer realm="wallet-api", error="invalid_token"` //nolint:gosec // RFC 6750 challenge, not a credential
)

// Transport-level request errors detected before the use cases run.
var (
	errPayloadTooLarge      = errors.New("request body too large")
	errUnsupportedMediaType = errors.New("unsupported media type")
)

// errorTable maps sentinels to the contract. Messages are fixed, so wrapped
// internal context never reaches clients; the only exception is validation,
// whose detail describes the client's own input so it can be corrected.
var errorTable = []struct {
	err    error
	status int
	code   string
	msg    string
}{
	{app.ErrValidation, http.StatusBadRequest, CodeInvalidRequest, ""},
	{errPayloadTooLarge, http.StatusRequestEntityTooLarge, CodePayloadTooLarge, fmt.Sprintf("request body must not exceed %d bytes", maxBodyBytes)},
	{errUnsupportedMediaType, http.StatusUnsupportedMediaType, CodeUnsupportedMediaType, "Content-Type must be application/json"},
	{app.ErrForbidden, http.StatusForbidden, CodeForbidden, "operation not allowed for this client"},
	{app.ErrWalletNotFound, http.StatusNotFound, CodeWalletNotFound, "wallet not found"},
	{app.ErrTransactionNotFound, http.StatusNotFound, CodeTransactionNotFound, "transaction not found"},
	{app.ErrWalletExists, http.StatusConflict, CodeWalletExists, "a wallet already exists for this player and currency"},
	{app.ErrIdempotencyConflict, http.StatusConflict, CodeIdempotencyConflict, "idempotency key reused with a different payload"},
	{app.ErrDuplicateExternalID, http.StatusConflict, CodeDuplicateExternal, "external transaction already registered with another idempotency key"},
}

// classify maps an error to status, code and a client-safe message.
func classify(err error) (int, string, string) {
	for _, e := range errorTable {
		if errors.Is(err, e.err) {
			return e.status, e.code, cmp.Or(e.msg, err.Error())
		}
	}
	if app.IsTransient(err) {
		return http.StatusServiceUnavailable, CodeServiceUnavailable, "temporarily unavailable, retry with the same idempotency key"
	}
	return http.StatusInternalServerError, CodeInternalError, "internal error"
}

// writeJSON encodes body with the given status.
func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set(headerContentType, contentTypeJSON)
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// writeError writes the contract's error body, with Retry-After on 503.
func writeError(w http.ResponseWriter, status int, code, message string) {
	if status == http.StatusServiceUnavailable {
		w.Header().Set(headerRetryAfter, retryAfterSeconds)
	}
	writeJSON(w, status, errorResponse{Code: code, Message: message})
}

// reply writes the DTO of v with status, or the contract mapping of err.
func reply[T, R any](h *handler, w http.ResponseWriter, r *http.Request, status int, v T, err error, dto func(T) R) {
	if err != nil {
		h.fail(w, r, err)
		return
	}
	writeJSON(w, status, dto(v))
}

// decodeJSON reads exactly one JSON object with no unknown fields, bounded
// by maxBodyBytes, from a request whose Content-Type (if any) is JSON.
func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	if err := checkContentType(r); err != nil {
		return err
	}
	err := contract.DecodeStrict(http.MaxBytesReader(w, r.Body, maxBodyBytes), dst)
	var tooLarge *http.MaxBytesError
	var decode *contract.DecodeError
	switch {
	case err == nil:
		return nil
	case errors.As(err, &tooLarge):
		return errPayloadTooLarge
	case errors.As(err, &decode):
		return fmt.Errorf("%w: %s", app.ErrValidation, decode.Message)
	default:
		return fmt.Errorf("%w: invalid JSON body", app.ErrValidation)
	}
}

// checkContentType rejects an explicit non-JSON media type; an absent
// header is accepted for compatibility with minimal clients.
func checkContentType(r *http.Request) error {
	ct := r.Header.Get(headerContentType)
	if ct == "" {
		return nil
	}
	media, _, err := mime.ParseMediaType(ct)
	if err != nil || media != contentTypeJSON {
		return errUnsupportedMediaType
	}
	return nil
}

// submitStatus maps the transaction state to the HTTP contract:
// 201 processed now, 200 replayed, 202 pending reference, 422 rejected/failed.
func submitStatus(res app.SubmitResult) int {
	switch res.Transaction.Status() {
	case wager.StatusProcessed:
		if res.Replay {
			return http.StatusOK
		}
		return http.StatusCreated
	case wager.StatusPendingReference:
		return http.StatusAccepted
	default:
		return http.StatusUnprocessableEntity
	}
}
