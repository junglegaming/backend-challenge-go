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
}

func NewHandler(processTxUC *usecases.ProcessTransactionUseCase, createWallUC *usecases.CreateWalletUseCase) *Handler {
	return &Handler{
		processTxUC:  processTxUC,
		createWallUC: createWallUC,
	}
}

func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /wallets", h.CreateWallet)
	mux.HandleFunc("POST /wagering/transactions", h.ProcessTransaction)
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
	tx, err := h.processTxUC.Execute(r.Context(), req)
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
