package httpapi

import (
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/google/uuid"

	"github.com/Pantani/backend-challenge-go/internal/app"
)

// maxIdempotencyKeyLen bounds the Idempotency-Key header.
const maxIdempotencyKeyLen = 128

// pathUUID parses a non-nil UUID path parameter.
func pathUUID(r *http.Request, name string) (uuid.UUID, error) {
	id, err := uuid.Parse(r.PathValue(name))
	if err != nil || id == uuid.Nil {
		return uuid.Nil, fmt.Errorf("%w: %s must be a non-nil UUID", app.ErrValidation, name)
	}
	return id, nil
}

// openWallet handles POST /wallets.
func (h *handler) openWallet(w http.ResponseWriter, r *http.Request) {
	cmd, err := h.openWalletCommand(w, r)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	wal, err := h.Wallets.Open(r.Context(), cmd)
	reply(h, w, r, http.StatusCreated, wal, err, newWalletResponse)
}

// openWalletCommand decodes and validates the open-wallet body.
func (h *handler) openWalletCommand(w http.ResponseWriter, r *http.Request) (app.OpenWalletCommand, error) {
	var body openWalletRequest
	if err := decodeJSON(w, r, &body); err != nil {
		return app.OpenWalletCommand{}, err
	}
	return app.NewOpenWalletCommand(app.OpenWalletInput{
		PlayerID: body.PlayerID, Amount: body.InitialBalance.Amount, Currency: body.InitialBalance.Currency,
		CorrelationID: correlationFrom(r.Context()),
	})
}

// getWallet handles GET /wallets/{walletId}.
func (h *handler) getWallet(w http.ResponseWriter, r *http.Request) {
	id, err := pathUUID(r, "walletId")
	if err != nil {
		h.fail(w, r, err)
		return
	}
	ctx := annotate(r.Context(), slog.String("walletId", id.String()))
	wal, err := h.Wallets.Get(ctx, id)
	reply(h, w, r.WithContext(ctx), http.StatusOK, wal, err, newWalletResponse)
}

// getLedger handles GET /wallets/{walletId}/ledger.
func (h *handler) getLedger(w http.ResponseWriter, r *http.Request) {
	id, limit, err := ledgerParams(r)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	ctx := annotate(r.Context(), slog.String("walletId", id.String()))
	page, err := h.Wallets.Ledger(ctx, id, r.URL.Query().Get("cursor"), limit)
	reply(h, w, r.WithContext(ctx), http.StatusOK, page, err, func(p app.LedgerPage) ledgerResponse { return newLedgerResponse(id, p) })
}

// ledgerParams reads the wallet id and page size of a ledger request.
func ledgerParams(r *http.Request) (uuid.UUID, int, error) {
	id, err := pathUUID(r, "walletId")
	if err != nil {
		return uuid.Nil, 0, err
	}
	limit, err := queryLimit(r)
	return id, limit, err
}

// queryLimit parses the optional limit: absent means the default page size,
// anything present must be between 1 and app.MaxLedgerLimit.
func queryLimit(r *http.Request) (int, error) {
	raw := r.URL.Query().Get("limit")
	if raw == "" {
		return 0, nil
	}
	limit, err := strconv.Atoi(raw)
	if err != nil || limit < 1 || limit > app.MaxLedgerLimit {
		return 0, fmt.Errorf("%w: limit must be an integer between 1 and %d", app.ErrValidation, app.MaxLedgerLimit)
	}
	return limit, nil
}

// reconcile handles POST /wallets/{walletId}/reconciliation.
func (h *handler) reconcile(w http.ResponseWriter, r *http.Request) {
	id, err := pathUUID(r, "walletId")
	if err != nil {
		h.fail(w, r, err)
		return
	}
	ctx := annotate(r.Context(), slog.String("walletId", id.String()))
	rec, err := h.Wallets.Reconcile(ctx, id)
	reply(h, w, r.WithContext(ctx), http.StatusOK, rec, err, newReconciliationResponse)
}

// submit handles POST /wagering/transactions. The providerId of the body
// must be the one bound to the token: a provider cannot act for another.
func (h *handler) submit(w http.ResponseWriter, r *http.Request) {
	cmd, err := h.submitCommand(w, r)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	ctx := annotate(r.Context(), slog.String("walletId", cmd.WalletID.String()))
	res, err := h.Wagers.Submit(ctx, cmd)
	if err != nil {
		h.fail(w, r.WithContext(ctx), err)
		return
	}
	ctx = annotate(ctx, slog.String("transactionId", res.Transaction.ID().String()))
	h.Logger.InfoContext(ctx, "wager transaction submitted",
		"status", res.Transaction.Status(), "idempotentReplay", res.Replay)
	writeJSON(w, submitStatus(res), newSubmitResponse(res))
}

// submitCommand decodes the body, validates the idempotency key and binds
// the operation to the authenticated provider.
func (h *handler) submitCommand(w http.ResponseWriter, r *http.Request) (app.SubmitCommand, error) {
	var body submitRequest
	if err := decodeJSON(w, r, &body); err != nil {
		return app.SubmitCommand{}, err
	}
	key, err := idempotencyKey(r)
	if err != nil {
		return app.SubmitCommand{}, err
	}
	if body.ProviderID != principalFrom(r.Context()).ProviderID {
		return app.SubmitCommand{}, fmt.Errorf("%w: providerId does not match the authenticated provider", app.ErrForbidden)
	}
	return app.NewSubmitCommand(body.ToInput(key, correlationFrom(r.Context()), ""))
}

// idempotencyKey reads the Idempotency-Key header: trimmed, non-empty, at
// most maxIdempotencyKeyLen printable ASCII characters.
func idempotencyKey(r *http.Request) (string, error) {
	key := strings.TrimSpace(r.Header.Get(headerIdempotencyKey))
	switch {
	case key == "":
		return "", fmt.Errorf("%w: %s header is required", app.ErrValidation, headerIdempotencyKey)
	case len(key) > maxIdempotencyKeyLen:
		return "", fmt.Errorf("%w: %s must be at most %d characters", app.ErrValidation, headerIdempotencyKey, maxIdempotencyKeyLen)
	case !printableASCII(key):
		return "", fmt.Errorf("%w: %s must contain only printable ASCII characters", app.ErrValidation, headerIdempotencyKey)
	}
	return key, nil
}

// printableASCII reports whether s holds only bytes in the 0x20-0x7E range.
func printableASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < 0x20 || s[i] > 0x7E {
			return false
		}
	}
	return true
}

// callerFrom derives the use-case caller from the authenticated principal.
func callerFrom(r *http.Request) app.Caller {
	p := principalFrom(r.Context())
	return app.Caller{ProviderID: p.ProviderID, Internal: p.IsInternal()}
}

// getTransaction handles GET /wagering/transactions/{transactionId}.
func (h *handler) getTransaction(w http.ResponseWriter, r *http.Request) {
	id, err := pathUUID(r, "transactionId")
	if err != nil {
		h.fail(w, r, err)
		return
	}
	ctx := annotate(r.Context(), slog.String("transactionId", id.String()))
	t, err := h.Wagers.Get(ctx, callerFrom(r), id)
	reply(h, w, r.WithContext(ctx), http.StatusOK, t, err, newTransactionResponse)
}

// getTransactionByExternal handles
// GET /providers/{providerId}/wagering/transactions/{externalTransactionId}.
func (h *handler) getTransactionByExternal(w http.ResponseWriter, r *http.Request) {
	t, err := h.Wagers.GetByExternal(r.Context(), callerFrom(r), r.PathValue("providerId"), r.PathValue("externalTransactionId"))
	reply(h, w, r, http.StatusOK, t, err, newTransactionResponse)
}
