// Package contract holds the wire shapes shared by every inbound adapter
// (HTTP and SQS): the money representation, the timestamp format, the wager
// operation payload and the strict JSON decoder. Keeping them in one place
// guarantees that the same operation is accepted and rejected identically
// whichever transport carries it.
package contract

import (
	"time"

	"github.com/Pantani/backend-challenge-go/internal/app"
	"github.com/Pantani/backend-challenge-go/internal/domain/event"
	"github.com/Pantani/backend-challenge-go/internal/domain/money"
)

// Money is the external money contract: the amount is always a JSON string,
// so a JSON number (a float on most clients) is rejected by the decoder.
type Money struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

// NewMoney renders a domain amount in the wire form.
func NewMoney(m money.Money) Money {
	return Money{Amount: m.Amount(), Currency: string(m.Currency())}
}

// FormatTime renders an instant as UTC RFC 3339 with millisecond precision,
// the timestamp format of every API response and event.
func FormatTime(t time.Time) string { return event.FormatTime(t) }

// Operation is the wager operation as submitted by a provider, over HTTP or
// as the data of an SQS message.
type Operation struct {
	ProviderID                     string `json:"providerId"`
	ExternalTransactionID          string `json:"externalTransactionId"`
	PlayerID                       string `json:"playerId"`
	WalletID                       string `json:"walletId"`
	RoundID                        string `json:"roundId"`
	GameID                         string `json:"gameId"`
	Kind                           string `json:"kind"`
	Money                          Money  `json:"money"`
	ReferenceExternalTransactionID string `json:"referenceExternalTransactionId"`
}

// ToInput binds the operation to its transport metadata, producing the
// unvalidated input of the submit use case.
func (o Operation) ToInput(idempotencyKey, correlationID, causationID string) app.SubmitInput {
	return app.SubmitInput{
		ProviderID: o.ProviderID, ExternalTransactionID: o.ExternalTransactionID, IdempotencyKey: idempotencyKey,
		PlayerID: o.PlayerID, WalletID: o.WalletID, RoundID: o.RoundID, GameID: o.GameID, Kind: o.Kind,
		Amount: o.Money.Amount, Currency: o.Money.Currency,
		ReferenceExternalTransactionID: o.ReferenceExternalTransactionID,
		CorrelationID:                  correlationID, CausationID: causationID,
	}
}
