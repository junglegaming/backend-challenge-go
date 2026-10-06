package http

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/google/uuid"

	"github.com/Lzdevmendes/backend-challenge-go/internal/app/usecases"
	"github.com/Lzdevmendes/backend-challenge-go/internal/domain"
)

type Handler struct {
	processTxUC  *usecases.ProcessTransactionUseCase
	createWallUC *usecases.CreateWalletUseCase
	queriesUC    *usecases.QueryUseCase
	authMW       *AuthMiddleware
}

func NewHandler(processTxUC *usecases.ProcessTransactionUseCase, createWallUC *usecases.CreateWalletUseCase, queriesUC *usecases.QueryUseCase, authMW *AuthMiddleware) *Handler {
	return &Handler{
		processTxUC:  processTxUC,
		createWallUC: createWallUC,
		queriesUC:    queriesUC,
		authMW:       authMW,
	}
}

func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /wallets", h.authMW.RequireRole("wallet-admin", h.CreateWallet))
	mux.HandleFunc("GET /wallets/{walletId}", h.authMW.RequireRole("wallet-admin", h.GetWallet))
	mux.HandleFunc("GET /wallets/{walletId}/ledger", h.authMW.RequireRole("wallet-admin", h.GetLedger))
	mux.HandleFunc("POST /wallets/{walletId}/reconciliation", h.authMW.RequireRole("wallet-admin", h.Reconcile))

	mux.HandleFunc("POST /wagering/transactions", h.authMW.RequireRole("wager-provider", h.ProcessTransaction))
	mux.HandleFunc("GET /wagering/transactions/{transactionId}", h.authMW.RequireRole("wallet-admin", h.GetTransaction))
	mux.HandleFunc("GET /providers/{providerId}/wagering/transactions/{externalTransactionId}", h.authMW.RequireRole("wager-provider", h.GetTransactionByExternalID))

	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /health/ready", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
}

type CreateWalletRequest struct {
	PlayerID string `json:"playerId"`
	Currency string `json:"currency"`
}

func (h *Handler) CreateWallet(w http.ResponseWriter, r *http.Request) {
	var req CreateWalletRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	playerID, err := uuid.Parse(req.PlayerID)
	if err != nil {
		http.Error(w, "invalid player id", http.StatusBadRequest)
		return
	}

	cmd := usecases.CreateWalletCommand{
		PlayerID: playerID,
		Currency: req.Currency,
	}

	wallet, err := h.createWallUC.Execute(r.Context(), cmd)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{
		"id":       wallet.ID().String(),
		"playerId": wallet.PlayerID().String(),
		"currency": string(wallet.Currency()),
	})
}

type ExternalRequestJSON struct {
	ProviderID                     string `json:"providerId"`
	ExternalTransactionID          string `json:"externalTransactionId"`
	IdempotencyKey                 string `json:"idempotencyKey"`
	PlayerID                       string `json:"playerId"`
	WalletID                       string `json:"walletId"`
	RoundID                        string `json:"roundId,omitempty"`
	GameID                         string `json:"gameId,omitempty"`
	Kind                           string `json:"kind"`
	Amount                         string `json:"amount"`
	Currency                       string `json:"currency"`
	ReferenceExternalTransactionID string `json:"referenceExternalTransactionId,omitempty"`
}

func (h *Handler) ProcessTransaction(w http.ResponseWriter, r *http.Request) {
	var payload ExternalRequestJSON
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	authProviderID, ok := ProviderIDFromContext(r.Context())
	if !ok || (authProviderID != payload.ProviderID && authProviderID != "wallet-admin") {
		http.Error(w, "forbidden: provider_id mismatch", http.StatusForbidden)
		return
	}

	playerID, err := uuid.Parse(payload.PlayerID)
	if err != nil {
		http.Error(w, "invalid playerId", http.StatusBadRequest)
		return
	}

	walletID, err := uuid.Parse(payload.WalletID)
	if err != nil {
		http.Error(w, "invalid walletId", http.StatusBadRequest)
		return
	}

	money, err := domain.ParseMoney(payload.Amount, payload.Currency)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	req := domain.ExternalRequest{
		ProviderID:                     payload.ProviderID,
		ExternalTransactionID:          payload.ExternalTransactionID,
		IdempotencyKey:                 payload.IdempotencyKey,
		PlayerID:                       playerID,
		WalletID:                       walletID,
		RoundID:                        payload.RoundID,
		GameID:                         payload.GameID,
		Kind:                           domain.Kind(payload.Kind),
		Money:                          money,
		ReferenceExternalTransactionID: payload.ReferenceExternalTransactionID,
	}

	// For idempotency check we must pass the same req to the usecase
	tx, err := h.processTxUC.Execute(r.Context(), req, "", "")
	if err != nil {
		if errors.Is(err, domain.ErrInvalidArgument) || errors.Is(err, domain.ErrInvalidAmount) || errors.Is(err, domain.ErrCurrencyMismatch) {
			http.Error(w, err.Error(), http.StatusConflict) // Using conflict for validation / idempotency conflicts to match idempotent design logic
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK) // Or 201 Created depending on first run vs idempotent run
	json.NewEncoder(w).Encode(map[string]any{
		"transactionId":         tx.ID().String(),
		"externalTransactionId": tx.Request().ExternalTransactionID,
		"status":                tx.Status(),
		"failureCode":           tx.FailureCode(),
	})
}

func (h *Handler) GetWallet(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("walletId")
	id, err := uuid.Parse(idStr)
	if err != nil {
		http.Error(w, "invalid wallet id", http.StatusBadRequest)
		return
	}

	wallet, err := h.queriesUC.GetWallet(r.Context(), id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if wallet == nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"id":       wallet.ID().String(),
		"playerId": wallet.PlayerID().String(),
		"balance": map[string]string{
			"amount":   wallet.Balance().Amount(),
			"currency": string(wallet.Balance().Currency()),
		},
		"version": wallet.Version(),
	})
}

func (h *Handler) GetLedger(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("walletId")
	cursor := r.URL.Query().Get("cursor")

	id, err := uuid.Parse(idStr)
	if err != nil {
		http.Error(w, "invalid wallet id", http.StatusBadRequest)
		return
	}

	movements, nextCursor, err := h.queriesUC.GetLedger(r.Context(), id, cursor, 50)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var items []map[string]any
	for _, m := range movements {
		items = append(items, map[string]any{
			"direction": string(m.Direction),
			"amount": map[string]string{
				"amount":   m.Amount.Amount(),
				"currency": string(m.Amount.Currency()),
			},
			"balanceBefore": map[string]string{
				"amount":   m.BalanceBefore.Amount(),
				"currency": string(m.BalanceBefore.Currency()),
			},
			"balanceAfter": map[string]string{
				"amount":   m.BalanceAfter.Amount(),
				"currency": string(m.BalanceAfter.Currency()),
			},
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"items":      items,
		"nextCursor": nextCursor,
	})
}

func (h *Handler) GetTransaction(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("transactionId")
	id, err := uuid.Parse(idStr)
	if err != nil {
		http.Error(w, "invalid tx id", http.StatusBadRequest)
		return
	}

	tx, err := h.queriesUC.GetTransaction(r.Context(), id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if tx == nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"transactionId": tx.ID().String(),
		"status":        tx.Status(),
		"failureCode":   tx.FailureCode(),
	})
}

func (h *Handler) GetTransactionByExternalID(w http.ResponseWriter, r *http.Request) {
	providerID := r.PathValue("providerId")
	extID := r.PathValue("externalTransactionId")

	authProviderID, ok := ProviderIDFromContext(r.Context())
	if !ok || (authProviderID != providerID && authProviderID != "wallet-admin") {
		http.Error(w, "forbidden: provider_id mismatch", http.StatusForbidden)
		return
	}

	tx, err := h.queriesUC.GetTransactionByExternalID(r.Context(), providerID, extID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if tx == nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"transactionId": tx.ID().String(),
		"status":        tx.Status(),
		"failureCode":   tx.FailureCode(),
	})
}

func (h *Handler) Reconcile(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("walletId")
	id, err := uuid.Parse(idStr)
	if err != nil {
		http.Error(w, "invalid wallet id", http.StatusBadRequest)
		return
	}

	stored, calc, checked, err := h.queriesUC.ReconcileWallet(r.Context(), id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	diff, _ := stored.Sub(calc)
	consistent := diff.IsZero()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"walletId": id.String(),
		"storedBalance": map[string]string{
			"amount":   stored.Amount(),
			"currency": string(stored.Currency()),
		},
		"calculatedBalance": map[string]string{
			"amount":   calc.Amount(),
			"currency": string(calc.Currency()),
		},
		"difference": map[string]string{
			"amount":   diff.Amount(),
			"currency": string(diff.Currency()),
		},
		"consistent":     consistent,
		"checkedEntries": checked,
	})
}
