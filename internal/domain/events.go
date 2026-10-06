package domain

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

const (
	EventWagerTransactionProcessed        = "WagerTransactionProcessed"
	EventWagerTransactionRejected         = "WagerTransactionRejected"
	EventWalletBalanceChanged             = "WalletBalanceChanged"
	EventWagerTransactionPendingReference = "WagerTransactionPendingReference"
	eventVersion                          = 1
)

// Event é o envelope de integração; Data é um dos payloads tipados abaixo.
type Event struct {
	EventID       string    `json:"eventId"`
	EventType     string    `json:"eventType"`
	AggregateID   string    `json:"aggregateId"`
	CorrelationID string    `json:"correlationId"`
	CausationID   string    `json:"causationId,omitempty"`
	OccurredAt    time.Time `json:"occurredAt"`
	Version       int       `json:"version"`
	Data          any       `json:"data"`
}

type EventMeta struct {
	EventID       uuid.UUID
	CorrelationID string
	CausationID   string
	OccurredAt    time.Time
}

type TransactionRef struct {
	TransactionID         string `json:"transactionId"`
	WalletID              string `json:"walletId"`
	PlayerID              string `json:"playerId"`
	Kind                  Kind   `json:"kind"`
	Money                 Money  `json:"money"`
	ProviderID            string `json:"providerId,omitempty"`
	ExternalTransactionID string `json:"externalTransactionId,omitempty"`
	RoundID               string `json:"roundId,omitempty"`
	GameID                string `json:"gameId,omitempty"`
	ReferenceExternalID   string `json:"referenceExternalTransactionId,omitempty"`
}

type WagerTransactionProcessedData struct {
	TransactionRef
	Balance Money `json:"balance"`
}

type WagerTransactionRejectedData struct {
	TransactionRef
	FailureCode FailureCode `json:"failureCode"`
}

type WagerTransactionPendingReferenceData struct {
	TransactionRef
}

type WalletBalanceChangedData struct {
	WalletID      string    `json:"walletId"`
	TransactionID string    `json:"transactionId"`
	Direction     Direction `json:"direction"`
	Money         Money     `json:"money"`
	BalanceBefore Money     `json:"balanceBefore"`
	BalanceAfter  Money     `json:"balanceAfter"`
	WalletVersion int64     `json:"walletVersion"`
}

func NewWagerTransactionProcessed(meta EventMeta, t *WagerTransaction) (Event, error) {
	if t.Status() != StatusProcessed {
		return Event{}, fmt.Errorf("%w: transação não processada", ErrInvalidArgument)
	}
	return newEvent(meta, EventWagerTransactionProcessed, t.ID(), WagerTransactionProcessedData{TransactionRef: refOf(t), Balance: t.ResultBalance()})
}

func NewWagerTransactionRejected(meta EventMeta, t *WagerTransaction) (Event, error) {
	if t.Status() != StatusRejected {
		return Event{}, fmt.Errorf("%w: transação não rejeitada", ErrInvalidArgument)
	}
	return newEvent(meta, EventWagerTransactionRejected, t.ID(), WagerTransactionRejectedData{TransactionRef: refOf(t), FailureCode: t.FailureCode()})
}

func NewWagerTransactionPendingReference(meta EventMeta, t *WagerTransaction) (Event, error) {
	if t.Status() != StatusPendingReference {
		return Event{}, fmt.Errorf("%w: transação não aguarda referência", ErrInvalidArgument)
	}
	return newEvent(meta, EventWagerTransactionPendingReference, t.ID(), WagerTransactionPendingReferenceData{TransactionRef: refOf(t)})
}

func NewWalletBalanceChanged(meta EventMeta, walletID, transactionID uuid.UUID, m Movement) (Event, error) {
	return newEvent(meta, EventWalletBalanceChanged, walletID, WalletBalanceChangedData{
		WalletID: walletID.String(), TransactionID: transactionID.String(), Direction: m.Direction,
		Money: m.Amount, BalanceBefore: m.BalanceBefore, BalanceAfter: m.BalanceAfter, WalletVersion: m.WalletVersion,
	})
}

func newEvent(meta EventMeta, eventType string, aggregate uuid.UUID, data any) (Event, error) {
	if meta.EventID == uuid.Nil || meta.CorrelationID == "" || meta.OccurredAt.IsZero() {
		return Event{}, fmt.Errorf("%w: metadados do evento incompletos", ErrInvalidArgument)
	}
	return Event{
		EventID: meta.EventID.String(), EventType: eventType, AggregateID: aggregate.String(),
		CorrelationID: meta.CorrelationID, CausationID: meta.CausationID,
		OccurredAt: meta.OccurredAt.UTC().Truncate(time.Millisecond), Version: eventVersion, Data: data,
	}, nil
}

func refOf(t *WagerTransaction) TransactionRef {
	r := t.Request()
	return TransactionRef{
		TransactionID: t.ID().String(), WalletID: t.WalletID().String(), PlayerID: t.PlayerID().String(),
		Kind: t.Kind(), Money: t.Money(), ProviderID: r.ProviderID, ExternalTransactionID: r.ExternalTransactionID,
		RoundID: r.RoundID, GameID: r.GameID, ReferenceExternalID: r.ReferenceExternalTransactionID,
	}
}
