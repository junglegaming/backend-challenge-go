package domain

import (
	"time"

	"github.com/google/uuid"
)

// EventType names an integration event. The names are a contract with every
// consumer of the events, so they must never change once published.
type EventType string

const (
	EventWagerTransactionProcessed EventType = "WagerTransactionProcessed"
	EventWagerTransactionRejected  EventType = "WagerTransactionRejected"
	EventWalletBalanceChanged      EventType = "WalletBalanceChanged"
)

// EventVersion is the schema version of every event today. A change that breaks
// consumers adds a new version instead of editing this one.
const EventVersion = 1

// Event is the envelope of an integration event (README §11). It is a snapshot:
// built once, from values, at the moment the state it describes was decided,
// and never edited afterwards. The type and the version are set by the
// constructor, not by the caller.
type Event struct {
	// ID is stable. A republication of the same event carries the same ID, which
	// is how a consumer recognises a duplicate.
	ID            uuid.UUID `json:"eventId"`
	Type          EventType `json:"eventType"`
	AggregateID   uuid.UUID `json:"aggregateId"`
	CorrelationID string    `json:"correlationId"`
	// CausationID is the event, or request, that led to this one. Optional.
	CausationID string    `json:"causationId,omitempty"`
	OccurredAt  time.Time `json:"occurredAt"`
	Version     int       `json:"version"`
	Data        any       `json:"data"`
}

func newEvent(eventType EventType, aggregateID uuid.UUID, correlationID, causationID string, data any) Event {
	return Event{
		ID:            uuid.New(),
		Type:          eventType,
		AggregateID:   aggregateID,
		CorrelationID: correlationID,
		CausationID:   causationID,
		OccurredAt:    time.Now().UTC(),
		Version:       EventVersion,
		Data:          data,
	}
}

// WagerTransactionProcessedData is the payload of WagerTransactionProcessed. The
// provider fields are empty for an OPENING, which has no external origin.
type WagerTransactionProcessedData struct {
	TransactionID         uuid.UUID `json:"transactionId"`
	WalletID              uuid.UUID `json:"walletId"`
	PlayerID              uuid.UUID `json:"playerId"`
	ProviderID            string    `json:"providerId,omitempty"`
	ExternalTransactionID string    `json:"externalTransactionId,omitempty"`
	Kind                  WagerKind `json:"kind"`
	Money                 Money     `json:"money"`
	BalanceAfter          *Money    `json:"balanceAfter,omitempty"`
}

// WagerTransactionRejectedData is the payload of WagerTransactionRejected.
type WagerTransactionRejectedData struct {
	TransactionID         uuid.UUID   `json:"transactionId"`
	WalletID              uuid.UUID   `json:"walletId"`
	PlayerID              uuid.UUID   `json:"playerId"`
	ProviderID            string      `json:"providerId"`
	ExternalTransactionID string      `json:"externalTransactionId"`
	Kind                  WagerKind   `json:"kind"`
	Money                 Money       `json:"money"`
	FailureCode           FailureCode `json:"failureCode"`
}

// WalletBalanceChangedData is the payload of WalletBalanceChanged (README §11).
type WalletBalanceChangedData struct {
	WalletID      uuid.UUID `json:"walletId"`
	TransactionID uuid.UUID `json:"transactionId"`
	Direction     Direction `json:"direction"`
	Money         Money     `json:"money"`
	BalanceBefore Money     `json:"balanceBefore"`
	BalanceAfter  Money     `json:"balanceAfter"`
	WalletVersion int64     `json:"walletVersion"`
}

// EventsFor builds the events a decided operation produces.
//
//   - PROCESSED gives WagerTransactionProcessed, including for LOSS.
//   - A ledger entry, meaning the balance really changed, adds
//     WalletBalanceChanged, caused by the first event.
//   - REJECTED gives WagerTransactionRejected.
//   - Any other state gives nothing: the operation is not decided yet.
//
// walletVersion is the wallet's version after the change. When correlationID is
// empty the transaction id stands in, so an event always has one.
func EventsFor(wt *WagerTransaction, entry *WalletLedgerEntry, walletVersion int64, correlationID string) []Event {
	if correlationID == "" {
		correlationID = wt.ID().String()
	}

	switch wt.State() {
	case WagerStateProcessed:
		processed := newEvent(EventWagerTransactionProcessed, wt.ID(), correlationID, "", processedData(wt))
		events := []Event{processed}
		if entry != nil {
			events = append(events, newEvent(EventWalletBalanceChanged, wt.WalletID(), correlationID, processed.ID.String(),
				WalletBalanceChangedData{
					WalletID:      wt.WalletID(),
					TransactionID: wt.ID(),
					Direction:     entry.Direction(),
					Money:         entry.Amount(),
					BalanceBefore: entry.BalanceBefore(),
					BalanceAfter:  entry.BalanceAfter(),
					WalletVersion: walletVersion,
				}))
		}
		return events

	case WagerStateRejected:
		return []Event{newEvent(EventWagerTransactionRejected, wt.ID(), correlationID, "", WagerTransactionRejectedData{
			TransactionID:         wt.ID(),
			WalletID:              wt.WalletID(),
			PlayerID:              wt.PlayerID(),
			ProviderID:            wt.ProviderID(),
			ExternalTransactionID: wt.ExternalTransactionID(),
			Kind:                  wt.Kind(),
			Money:                 wt.Amount(),
			FailureCode:           wt.FailureCode(),
		})}
	}
	return nil
}

func processedData(wt *WagerTransaction) WagerTransactionProcessedData {
	data := WagerTransactionProcessedData{
		TransactionID: wt.ID(),
		WalletID:      wt.WalletID(),
		PlayerID:      wt.PlayerID(),
		Kind:          wt.Kind(),
		Money:         wt.Amount(),
		BalanceAfter:  wt.BalanceAfter(),
	}
	// An OPENING is internal: it has no provider and no external id.
	if wt.Kind() != WagerKindOpening {
		data.ProviderID = wt.ProviderID()
		data.ExternalTransactionID = wt.ExternalTransactionID()
	}
	return data
}
