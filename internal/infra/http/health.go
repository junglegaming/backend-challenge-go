package http

import (
	"context"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/Lzdevmendes/backend-challenge-go/internal/infra/db"
)

type HealthHandler struct {
	db        *db.PostgresDB
	sqsClient *sqs.Client
}

func NewHealthHandler(db *db.PostgresDB, sqsClient *sqs.Client) *HealthHandler {
	return &HealthHandler{db: db, sqsClient: sqsClient}
}

func (h *HealthHandler) Live(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"up"}`))
}

func (h *HealthHandler) Ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	if err := h.db.Pool.Ping(ctx); err != nil {
		http.Error(w, `{"status":"down","reason":"db"}`, http.StatusServiceUnavailable)
		return
	}

	if _, err := h.sqsClient.ListQueues(ctx, &sqs.ListQueuesInput{}); err != nil {
		http.Error(w, `{"status":"down","reason":"sqs"}`, http.StatusServiceUnavailable)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"ready"}`))
}
