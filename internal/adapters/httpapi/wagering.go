package httpapi

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/VictorM0nteiro/backend-challenge-go/internal/adapters/postgres"
	"github.com/VictorM0nteiro/backend-challenge-go/internal/app"
	"github.com/VictorM0nteiro/backend-challenge-go/internal/domain"
	"github.com/VictorM0nteiro/backend-challenge-go/internal/logctx"
)

// wagerEndpoint is part of the idempotency key. A key is unique per
// (provider, endpoint, key), so two endpoints can reuse the same string.
const wagerEndpoint = "POST /wagering/transactions"

func (s *Server) submitWager(w http.ResponseWriter, r *http.Request) error {
	provider, err := providerFrom(r)
	if err != nil {
		return err
	}
	key := r.Header.Get(headerIdempotencyKey)
	if key == "" || len(key) > maxIdempotencyKeyLen {
		return fmt.Errorf("%w: Idempotency-Key is required and must be at most %d bytes", errInvalidRequest, maxIdempotencyKeyLen)
	}

	var req wagerRequest
	if err := decodeJSON(w, r, &req); err != nil {
		return err
	}
	// A body that names another provider answers 404, the same as an unknown
	// operation. That way one provider cannot tell whether another exists.
	if req.ProviderID != provider {
		return errNotFound
	}

	money, err := req.Money.toDomain()
	if err != nil {
		return err
	}

	logctx.Add(r.Context(), slog.String("walletId", req.WalletID.String()), slog.String("kind", string(req.Kind)))

	hash, err := app.Fingerprint(app.WagerBody{
		ProviderID:                     req.ProviderID,
		ExternalTransactionID:          req.ExternalTransactionID,
		PlayerID:                       req.PlayerID,
		WalletID:                       req.WalletID,
		RoundID:                        req.RoundID,
		GameID:                         req.GameID,
		Kind:                           req.Kind,
		Money:                          money,
		ReferenceExternalTransactionID: req.ReferenceExternalTransactionID,
	})
	if err != nil {
		return err
	}

	out, err := s.processor.Process(r.Context(), postgres.WagerRequest{
		Key:         postgres.IdempotencyKey{Scope: provider, Endpoint: wagerEndpoint, Key: key},
		RequestHash: hash,
		Command: domain.WagerCommand{
			ProviderID:                     req.ProviderID,
			ExternalTransactionID:          req.ExternalTransactionID,
			IdempotencyKey:                 key,
			RequestHash:                    hash,
			WalletID:                       req.WalletID,
			PlayerID:                       req.PlayerID,
			RoundID:                        req.RoundID,
			GameID:                         req.GameID,
			Kind:                           req.Kind,
			Amount:                         money,
			ReferenceExternalTransactionID: req.ReferenceExternalTransactionID,
		},
	})
	if err != nil {
		return err
	}

	logctx.Add(r.Context(), slog.String("transactionId", out.TransactionID.String()))

	view, err := writeWagerOutcome(w, out)
	if err != nil {
		return err
	}
	slog.InfoContext(r.Context(), "wager operation",
		slog.String("status", string(view.Status)),
		slog.String("failureCode", string(view.FailureCode)),
		slog.Bool("replayed", out.Replayed),
	)
	return nil
}

// writeWagerOutcome answers with the outcome the processor stored. The body
// was fixed when the operation ran; this adds only the flag that tells a
// replay apart from the first answer.
func writeWagerOutcome(w http.ResponseWriter, out postgres.Outcome) (wagerView, error) {
	var stored wagerView
	if err := json.Unmarshal(out.Body, &stored); err != nil {
		return wagerView{}, fmt.Errorf("httpapi: decode stored outcome: %w", err)
	}
	writeJSON(w, out.StatusCode, wagerResponse{wagerView: stored, IdempotentReplay: out.Replayed})
	return stored, nil
}

func (s *Server) getWager(w http.ResponseWriter, r *http.Request) error {
	provider, err := providerFrom(r)
	if err != nil {
		return err
	}
	id, err := pathUUID(r, "transactionId")
	if err != nil {
		return err
	}

	wt, err := s.wagers.FindByID(r.Context(), id)
	if err != nil {
		return err
	}
	// Another provider's operation looks exactly like a missing one.
	if wt.ProviderID() != provider {
		return errNotFound
	}

	writeJSON(w, http.StatusOK, wagerView{
		TransactionID: wt.ID(),
		Status:        wt.State(),
		FailureCode:   wt.FailureCode(),
		Balance:       wt.BalanceAfter(),
	})
	return nil
}

// Explicação

// - O hash é calculado com app.Fingerprint, o mesmo pacote que o SQS vai usar. A chave de idempotência e o header não entram no cálculo.
// - A chave de idempotência é escopada por provedor e endpoint, como o §5.2 pede. Dois provedores podem usar a mesma string sem colisão.
// - Uma rejeição de negócio (saldo insuficiente) chega aqui como resultado normal do Process, com status 422 e corpo REJECTED. Ela não passa pelo statusFor. Por isso insufficient_funds e idempotency_key_reused têm status 422 mas corpos diferentes.
// - writeWagerOutcome decodifica o corpo armazenado e recodifica com o flag. O saldo continua o do processamento original, porque ele veio do corpo armazenado.
