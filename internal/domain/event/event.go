// Package event defines the integration events emitted through the outbox.
//
// Every event has a concrete payload type and a constructor that fixes its
// type and schema version. Payloads only contain strings, integers and
// booleans: money travels as fixed-scale decimal strings and instants as UTC
// RFC 3339 strings, so the serialized snapshot is immutable and float-free.
package event

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"

	"github.com/Pantani/backend-challenge-go/internal/domain/money"
	"github.com/Pantani/backend-challenge-go/internal/domain/wager"
	"github.com/Pantani/backend-challenge-go/internal/domain/wallet"
)

// Event types.
const (
	TypeWagerTransactionProcessed        = "WagerTransactionProcessed"
	TypeWagerTransactionRejected         = "WagerTransactionRejected"
	TypeWagerTransactionFailed           = "WagerTransactionFailed"
	TypeWalletBalanceChanged             = "WalletBalanceChanged"
	TypeWagerTransactionPendingReference = "WagerTransactionPendingReference"
)

// SchemaVersion is the version of every payload defined in this package.
const SchemaVersion = 1

// Aggregate types used for routing.
const (
	AggregateWallet      = "Wallet"
	AggregateTransaction = "WagerTransaction"
)

// Money is the wire representation of money.Money.
type Money struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

// newMoney converts a domain amount to its wire representation.
func newMoney(m money.Money) Money {
	return Money{Amount: m.Amount(), Currency: string(m.Currency())}
}

// Envelope wraps a typed payload with routing and tracing metadata.
type Envelope[T any] struct {
	EventID       string `json:"eventId"`
	EventType     string `json:"eventType"`
	AggregateType string `json:"aggregateType"`
	AggregateID   string `json:"aggregateId"`
	CorrelationID string `json:"correlationId"`
	CausationID   string `json:"causationId,omitempty"`
	OccurredAt    string `json:"occurredAt"`
	Version       int    `json:"version"`
	Data          T      `json:"data"`
}

// Meta carries the tracing metadata of one event. EventID must be fresh for
// every event; CorrelationID, CausationID and OccurredAt are shared by all the
// events emitted for the same operation.
type Meta struct {
	EventID       uuid.UUID
	CorrelationID string
	CausationID   string
	OccurredAt    time.Time
}

// FormatTime renders an instant as UTC RFC 3339 with millisecond precision.
func FormatTime(t time.Time) string { return t.UTC().Format("2006-01-02T15:04:05.000Z07:00") }

// Record is the serialized, immutable snapshot stored in the outbox.
type Record struct {
	EventID       uuid.UUID
	EventType     string
	AggregateType string
	AggregateID   uuid.UUID
	PartitionKey  string
	OccurredAt    time.Time
	Payload       []byte
}

// newRecord wraps data in its envelope and serializes it. partitionKey groups
// events that must keep their relative order (the wallet id).
func newRecord[T any](m Meta, eventType, aggregateType string, aggregateID uuid.UUID, partitionKey string, data T) Record {
	env := Envelope[T]{
		EventID: m.EventID.String(), EventType: eventType, AggregateType: aggregateType,
		AggregateID: aggregateID.String(), CorrelationID: m.CorrelationID, CausationID: m.CausationID,
		OccurredAt: FormatTime(m.OccurredAt), Version: SchemaVersion, Data: data,
	}
	// Payloads only hold strings, ints and bools: Marshal cannot fail.
	payload, _ := json.Marshal(env)
	return Record{
		EventID: m.EventID, EventType: eventType, AggregateType: aggregateType,
		AggregateID: aggregateID, PartitionKey: partitionKey, OccurredAt: m.OccurredAt.UTC(), Payload: payload,
	}
}

// TransactionData is the common payload describing a wager transaction.
type TransactionData struct {
	TransactionID                  string `json:"transactionId"`
	Origin                         string `json:"origin"`
	Kind                           string `json:"kind"`
	Status                         string `json:"status"`
	WalletID                       string `json:"walletId"`
	PlayerID                       string `json:"playerId"`
	Money                          Money  `json:"money"`
	ProviderID                     string `json:"providerId,omitempty"`
	ExternalTransactionID          string `json:"externalTransactionId,omitempty"`
	RoundID                        string `json:"roundId,omitempty"`
	GameID                         string `json:"gameId,omitempty"`
	ReferenceExternalTransactionID string `json:"referenceExternalTransactionId,omitempty"`
}

func newTransactionData(t *wager.Transaction) TransactionData {
	ext := t.External()
	return TransactionData{
		TransactionID: t.ID().String(), Origin: string(t.Origin()), Kind: string(t.Kind()),
		Status: string(t.Status()), WalletID: t.WalletID().String(), PlayerID: t.PlayerID().String(),
		Money: newMoney(t.Amount()), ProviderID: ext.ProviderID, ExternalTransactionID: ext.ExternalID,
		RoundID: ext.RoundID, GameID: ext.GameID, ReferenceExternalTransactionID: ext.ReferenceExternalID,
	}
}

// WagerTransactionProcessed is emitted when an operation succeeds (LOSS included).
type WagerTransactionProcessed struct {
	TransactionData
	ReferenceTransactionID string `json:"referenceTransactionId,omitempty"`
	Balance                Money  `json:"balance"`
}

// NewWagerTransactionProcessed builds the event for a processed transaction.
// t must be in StatusProcessed: its result balance and reference are copied
// as they are.
func NewWagerTransactionProcessed(m Meta, t *wager.Transaction) Record {
	data := WagerTransactionProcessed{TransactionData: newTransactionData(t), Balance: newMoney(t.ResultBalance())}
	if t.ReferenceTxID() != uuid.Nil {
		data.ReferenceTransactionID = t.ReferenceTxID().String()
	}
	return newRecord(m, TypeWagerTransactionProcessed, AggregateTransaction, t.ID(), t.WalletID().String(), data)
}

// WagerTransactionRejected is emitted for definitive business rejections.
type WagerTransactionRejected struct {
	TransactionData
	FailureCode string `json:"failureCode"`
}

// NewWagerTransactionRejected builds the event for a rejected transaction.
// t must be in StatusRejected so that its failure code is set.
func NewWagerTransactionRejected(m Meta, t *wager.Transaction) Record {
	data := WagerTransactionRejected{TransactionData: newTransactionData(t), FailureCode: string(t.FailureCode())}
	return newRecord(m, TypeWagerTransactionRejected, AggregateTransaction, t.ID(), t.WalletID().String(), data)
}

// WagerTransactionFailed is emitted when an operation is abandoned after a
// permanent, non-business failure (for example a resolution attempt that
// kept hitting an internal error). The balance was never touched.
type WagerTransactionFailed struct {
	TransactionData
	FailureCode string `json:"failureCode"`
}

// NewWagerTransactionFailed builds the event for a failed transaction. t
// must be in StatusFailed so that its failure code is set.
func NewWagerTransactionFailed(m Meta, t *wager.Transaction) Record {
	data := WagerTransactionFailed{TransactionData: newTransactionData(t), FailureCode: string(t.FailureCode())}
	return newRecord(m, TypeWagerTransactionFailed, AggregateTransaction, t.ID(), t.WalletID().String(), data)
}

// WagerTransactionPendingReference is emitted when an operation starts waiting.
type WagerTransactionPendingReference struct {
	TransactionData
	NextAttemptAt string `json:"nextAttemptAt"`
}

// NewWagerTransactionPendingReference builds the event for a deferred
// operation. t must be in StatusPendingReference so that NextAttemptAt is set.
func NewWagerTransactionPendingReference(m Meta, t *wager.Transaction) Record {
	data := WagerTransactionPendingReference{TransactionData: newTransactionData(t), NextAttemptAt: FormatTime(t.NextAttemptAt())}
	return newRecord(m, TypeWagerTransactionPendingReference, AggregateTransaction, t.ID(), t.WalletID().String(), data)
}

// WalletBalanceChanged is emitted for every effective balance change.
type WalletBalanceChanged struct {
	WalletID      string `json:"walletId"`
	TransactionID string `json:"transactionId"`
	Direction     string `json:"direction"`
	Money         Money  `json:"money"`
	BalanceBefore Money  `json:"balanceBefore"`
	BalanceAfter  Money  `json:"balanceAfter"`
	WalletVersion int64  `json:"walletVersion"`
}

// NewWalletBalanceChanged builds the event from the ledger entry of a change.
func NewWalletBalanceChanged(m Meta, e wallet.LedgerEntry, walletVersion int64) Record {
	data := WalletBalanceChanged{
		WalletID: e.WalletID().String(), TransactionID: e.TransactionID().String(),
		Direction: string(e.Direction()), Money: newMoney(e.Amount()),
		BalanceBefore: newMoney(e.BalanceBefore()), BalanceAfter: newMoney(e.BalanceAfter()),
		WalletVersion: walletVersion,
	}
	return newRecord(m, TypeWalletBalanceChanged, AggregateWallet, e.WalletID(), e.WalletID().String(), data)
}
