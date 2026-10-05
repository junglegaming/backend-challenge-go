package sqs

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/VictorM0nteiro/backend-challenge-go/internal/adapters/postgres"
	"github.com/VictorM0nteiro/backend-challenge-go/internal/app"
	"github.com/VictorM0nteiro/backend-challenge-go/internal/domain"
	"github.com/google/uuid"
)

const (
	messageType          = "WagerTransactionRequested"
	consumerName         = "wager-consumer"
	wagerEndpoint        = "POST /wagering/transactions"
	maxIdempotencyKeyLen = 255
)

// errInvalidMessage is a message the consumer cannot understand. It can never
// succeed, so it is permanent.
var errInvalidMessage = errors.New("sqs: invalid message")

// envelope is the message shape from README §10.
type envelope struct {
	MessageID  string    `json:"messageId"`
	Type       string    `json:"type"`
	OccurredAt time.Time `json:"occurredAt"`
	Data       wagerData `json:"data"`
}

type wagerData struct {
	ProviderID                     string           `json:"providerId"`
	ExternalTransactionID          string           `json:"externalTransactionId"`
	IdempotencyKey                 string           `json:"idempotencyKey"`
	PlayerID                       uuid.UUID        `json:"playerId"`
	WalletID                       uuid.UUID        `json:"walletId"`
	RoundID                        string           `json:"roundId"`
	GameID                         string           `json:"gameId"`
	Kind                           domain.WagerKind `json:"kind"`
	Money                          money            `json:"money"`
	ReferenceExternalTransactionID string           `json:"referenceExternalTransactionId"`
}

// money is the wire form of an amount, the same as the HTTP layer uses.
type money struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

// parse decodes a message body strictly, like the HTTP layer. An unknown field
// is a contract violation, not something to skip silently.
func parse(body string) (envelope, error) {
	var env envelope
	dec := json.NewDecoder(strings.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&env); err != nil {
		return envelope{}, fmt.Errorf("%w: %v", errInvalidMessage, err)
	}
	return env, nil
}

// toRequest maps a verified envelope to the processor's input. Field by field,
// it mirrors the HTTP handler, so both entries compute the same fingerprint for
// the same operation.
func toRequest(env envelope) (postgres.WagerRequest, error) {
	if env.MessageID == "" {
		return postgres.WagerRequest{}, fmt.Errorf("%w: messageId is required", errInvalidMessage)
	}
	if env.Type != messageType {
		return postgres.WagerRequest{}, fmt.Errorf("%w: unsupported type %q", errInvalidMessage, env.Type)
	}

	d := env.Data
	if d.ProviderID == "" {
		return postgres.WagerRequest{}, fmt.Errorf("%w: providerId is required", errInvalidMessage)
	}
	if d.IdempotencyKey == "" || len(d.IdempotencyKey) > maxIdempotencyKeyLen {
		return postgres.WagerRequest{}, fmt.Errorf("%w: idempotencyKey is required and must be at most %d bytes", errInvalidMessage, maxIdempotencyKeyLen)
	}

	amount, err := domain.ParseExternalAmount(d.Money.Amount, d.Money.Currency)
	if err != nil {
		return postgres.WagerRequest{}, err
	}

	hash, err := app.Fingerprint(app.WagerBody{
		ProviderID:                     d.ProviderID,
		ExternalTransactionID:          d.ExternalTransactionID,
		PlayerID:                       d.PlayerID,
		WalletID:                       d.WalletID,
		RoundID:                        d.RoundID,
		GameID:                         d.GameID,
		Kind:                           d.Kind,
		Money:                          amount,
		ReferenceExternalTransactionID: d.ReferenceExternalTransactionID,
	})
	if err != nil {
		return postgres.WagerRequest{}, err
	}

	return postgres.WagerRequest{
		Key:         postgres.IdempotencyKey{Scope: d.ProviderID, Endpoint: wagerEndpoint, Key: d.IdempotencyKey},
		RequestHash: hash,
		Command: domain.WagerCommand{
			ProviderID:                     d.ProviderID,
			ExternalTransactionID:          d.ExternalTransactionID,
			IdempotencyKey:                 d.IdempotencyKey,
			RequestHash:                    hash,
			WalletID:                       d.WalletID,
			PlayerID:                       d.PlayerID,
			RoundID:                        d.RoundID,
			GameID:                         d.GameID,
			Kind:                           d.Kind,
			Amount:                         amount,
			ReferenceExternalTransactionID: d.ReferenceExternalTransactionID,
		},
		Inbox: &postgres.InboxMessage{Consumer: consumerName, MessageID: env.MessageID, Hash: hash},
	}, nil
}

// isPermanent reports whether retrying cannot change the outcome. Those
// messages stay in the queue, and the redrive policy moves them to the DLQ.
// Everything else is transient: a database outage, a version conflict, a key
// still in flight. Those are retried after a backoff.
func isPermanent(err error) bool {
	for _, permanent := range []error{
		errInvalidMessage,
		postgres.ErrMessageReused,
		postgres.ErrIdempotencyKeyReuse,
		postgres.ErrWalletNotFound,
		postgres.ErrWalletOwnerMismatch,
		postgres.ErrDuplicateExternalTransaction,
		domain.ErrInvalidAmount,
		domain.ErrInvalidCurrency,
		domain.ErrCurrencyMismatch,
		domain.ErrInvalidWagerTransaction,
	} {
		if errors.Is(err, permanent) {
			return true
		}
	}
	return false
}

// Explicação

// - parse é estrito de propósito. Um campo novo ou com erro de digitação na mensagem vira erro explícito, em vez de ser ignorado e processar uma operação incompleta. É a mesma regra do HTTP.
// - toRequest é o espelho do handler HTTP, campo por campo. Como o hash vem de app.Fingerprint com a mesma struct, uma operação que chega pelos dois canais tem o mesmo hash. O teste da parte 2 prova isso.
// - isPermanent separa "não adianta tentar de novo" de "tente depois". A lista é explícita: se alguém adicionar um erro novo e esquecer dessa lista, ele vira transitório por padrão, o que é o lado seguro. A mensagem fica na fila, e o redrive a encaminha depois de algumas tentativas.
// - ErrWalletNotFound é permanente porque uma carteira que não existe não passa a existir sozinha. Um ErrWalletVersionConflict, por outro lado, é transitório: outra transação estava na frente.
