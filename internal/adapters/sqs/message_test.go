package sqs

import (
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/VictorM0nteiro/backend-challenge-go/internal/adapters/postgres"
	"github.com/VictorM0nteiro/backend-challenge-go/internal/app"
	"github.com/VictorM0nteiro/backend-challenge-go/internal/domain"
)

const (
	testPlayerID = "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1"
	testWalletID = "0192f291-27dd-7d3f-8071-5f8685deef37"
)

const validBody = `{
  "messageId": "msg-123",
  "type": "WagerTransactionRequested",
  "occurredAt": "2026-09-08T12:00:00.000Z",
  "data": {
    "providerId": "provider-a",
    "externalTransactionId": "transaction-123",
    "idempotencyKey": "provider-a:transaction-123",
    "playerId": "` + testPlayerID + `",
    "walletId": "` + testWalletID + `",
    "roundId": "round-987",
    "gameId": "fortune-chimp",
    "kind": "BET",
    "money": { "amount": "25.00", "currency": "BRL" }
  }
}`

func TestToRequest_SameOperationHashesLikeHTTP(t *testing.T) {
	env, err := parse(validBody)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	req, err := toRequest(env)
	if err != nil {
		t.Fatalf("toRequest: %v", err)
	}

	// The HTTP handler builds this same body from the same fields.
	money, _ := domain.ParseExternalAmount("25.00", "BRL")
	httpHash, err := app.Fingerprint(app.WagerBody{
		ProviderID:            "provider-a",
		ExternalTransactionID: "transaction-123",
		PlayerID:              uuid.MustParse(testPlayerID),
		WalletID:              uuid.MustParse(testWalletID),
		RoundID:               "round-987",
		GameID:                "fortune-chimp",
		Kind:                  domain.WagerKindBet,
		Money:                 money,
	})
	if err != nil {
		t.Fatalf("Fingerprint: %v", err)
	}

	if req.RequestHash != httpHash {
		t.Fatalf("SQS hash %s, HTTP hash %s: the same operation must hash the same", req.RequestHash, httpHash)
	}
	if req.Key != (postgres.IdempotencyKey{Scope: "provider-a", Endpoint: wagerEndpoint, Key: "provider-a:transaction-123"}) {
		t.Fatalf("key = %+v, want the shared HTTP key space", req.Key)
	}
	if req.Inbox == nil || req.Inbox.MessageID != "msg-123" || req.Inbox.Hash != httpHash {
		t.Fatalf("inbox = %+v, want msg-123 with the operation's hash", req.Inbox)
	}
}

func TestParse_UnknownFieldIsRejected(t *testing.T) {
	body := strings.Replace(validBody, `"gameId": "fortune-chimp",`, `"gameId": "fortune-chimp", "gmaeId": "typo",`, 1)
	if _, err := parse(body); !errors.Is(err, errInvalidMessage) {
		t.Fatalf("err = %v, want errInvalidMessage", err)
	}
}

func TestToRequest_OnlyTheWagerTypeIsAccepted(t *testing.T) {
	body := strings.Replace(validBody, "WagerTransactionRequested", "WalletOpened", 1)
	env, err := parse(body)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if _, err := toRequest(env); !errors.Is(err, errInvalidMessage) {
		t.Fatalf("err = %v, want errInvalidMessage", err)
	}
}

func TestIsPermanent_SeparatesBadInputFromTransientFailures(t *testing.T) {
	env, _ := parse(strings.Replace(validBody, `"25.00"`, `"25.001"`, 1))
	_, err := toRequest(env)
	if !isPermanent(err) {
		t.Fatalf("amount with three decimals is permanent, got err = %v", err)
	}

	cases := []struct {
		name      string
		err       error
		permanent bool
	}{
		{"chave_reutilizada_com_corpo_diferente", postgres.ErrIdempotencyKeyReuse, true},
		{"mensagem_reutilizada_com_corpo_diferente", postgres.ErrMessageReused, true},
		{"carteira_inexistente", postgres.ErrWalletNotFound, true},
		{"jogador_nao_e_dono_da_carteira", postgres.ErrWalletOwnerMismatch, true},
		{"mesma_operacao_com_outra_chave", postgres.ErrDuplicateExternalTransaction, true},
		{"chave_em_processamento", postgres.ErrRequestInFlight, false},
		{"conflito_de_versao", postgres.ErrWalletVersionConflict, false},
		{"banco_fora_do_ar", errors.New("connection refused"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isPermanent(tc.err); got != tc.permanent {
				t.Fatalf("isPermanent = %v, want %v", got, tc.permanent)
			}
		})
	}
}
