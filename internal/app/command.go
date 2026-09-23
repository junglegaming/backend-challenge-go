package app

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/Pantani/backend-challenge-go/internal/domain/money"
	"github.com/Pantani/backend-challenge-go/internal/domain/wager"
)

// SubmitInput is the raw operation shared by HTTP and SQS. Every field is a
// string so that both transports go through exactly the same validation and
// normalization before the payload hash is computed.
type SubmitInput struct {
	// ProviderID is the authenticated provider submitting the operation.
	ProviderID string
	// ExternalTransactionID is the provider's identifier of the operation.
	ExternalTransactionID string
	// IdempotencyKey deduplicates retries of the same operation.
	IdempotencyKey string
	// PlayerID is the player's UUID, in any case.
	PlayerID string
	// WalletID is the wallet's UUID, in any case.
	WalletID string
	// RoundID is the game round the operation belongs to.
	RoundID string
	// GameID is the game producing the operation.
	GameID string
	// Kind is the external operation kind (BET, WIN, LOSS, REFUND, ROLLBACK).
	Kind string
	// Amount is the decimal amount in the canonical two-decimal form.
	Amount string
	// Currency is the ISO 4217 code.
	Currency string
	// ReferenceExternalTransactionID is the reversed operation (REFUND,
	// ROLLBACK, required) or the settled bet (WIN, optional). LOSS must not
	// carry one.
	ReferenceExternalTransactionID string
	// CorrelationID traces the operation across services.
	CorrelationID string
	// CausationID is the identifier of the message that caused the operation.
	CausationID string
}

// SubmitCommand is a validated operation.
type SubmitCommand struct {
	// ProviderID is the authenticated provider.
	ProviderID string
	// ExternalID is the provider's identifier of the operation.
	ExternalID string
	// IdempotencyKey deduplicates retries, scoped by ProviderID.
	IdempotencyKey string
	// PlayerID is the normalized player identifier.
	PlayerID uuid.UUID
	// WalletID is the normalized wallet identifier.
	WalletID uuid.UUID
	// RoundID is the game round.
	RoundID string
	// GameID is the game.
	GameID string
	// Kind is the parsed operation kind.
	Kind wager.Kind
	// Amount is the parsed amount and currency.
	Amount money.Money
	// ReferenceExternalID is the referenced operation, if any.
	ReferenceExternalID string
	// PayloadHash is the wager.Fingerprint hash of the business fields; it is
	// compared on idempotent retries to detect a reused key.
	PayloadHash string
	// CorrelationID traces the operation across services.
	CorrelationID string
	// CausationID is the identifier of the message that caused the operation.
	CausationID string
}

// NewSubmitCommand validates and normalizes a raw operation. UUIDs are
// normalized to their canonical lowercase form before hashing; amounts are
// only accepted in the canonical two-decimal form, so no other normalization
// is needed.
func NewSubmitCommand(in SubmitInput) (SubmitCommand, error) {
	ids, err := parseUUIDs(in.PlayerID, in.WalletID)
	if err != nil {
		return SubmitCommand{}, err
	}
	kind, err := wager.ParseExternalKind(in.Kind)
	if err != nil {
		return SubmitCommand{}, invalid(err)
	}
	amount, err := money.Parse(in.Amount, in.Currency)
	if err != nil {
		return SubmitCommand{}, invalid(err)
	}
	cmd := SubmitCommand{
		ProviderID: in.ProviderID, ExternalID: in.ExternalTransactionID, IdempotencyKey: in.IdempotencyKey,
		PlayerID: ids[0], WalletID: ids[1], RoundID: in.RoundID, GameID: in.GameID, Kind: kind,
		Amount: amount, ReferenceExternalID: in.ReferenceExternalTransactionID,
		CorrelationID: in.CorrelationID, CausationID: in.CausationID,
	}
	cmd.PayloadHash = cmd.fingerprint().Hash()
	if err := cmd.validate(); err != nil {
		return SubmitCommand{}, err
	}
	return cmd, nil
}

func parseUUIDs(values ...string) ([]uuid.UUID, error) {
	out := make([]uuid.UUID, len(values))
	for i, v := range values {
		id, err := uuid.Parse(v)
		if err != nil || id == uuid.Nil {
			return nil, fmt.Errorf("%w: invalid uuid %q", ErrValidation, v)
		}
		out[i] = id
	}
	return out, nil
}

func (c SubmitCommand) fingerprint() wager.Fingerprint {
	return wager.Fingerprint{
		ProviderID: c.ProviderID, ExternalTransactionID: c.ExternalID,
		PlayerID: c.PlayerID.String(), WalletID: c.WalletID.String(), RoundID: c.RoundID, GameID: c.GameID,
		Kind: c.Kind, Amount: c.Amount.Amount(), Currency: string(c.Amount.Currency()),
		ReferenceExternalTransactionID: c.ReferenceExternalID,
	}
}

// validationTime is the fixed, non-zero instant used to validate commands:
// the throwaway transaction is never stored, so no clock is needed.
var validationTime = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

// validate runs the domain constructor on throwaway identifiers so every
// transport rejects the same invalid inputs before touching the database.
func (c SubmitCommand) validate() error {
	_, err := wager.NewExternal(c.params(uuid.Max, validationTime))
	if err != nil {
		return invalid(err)
	}
	return nil
}

func (c SubmitCommand) params(id uuid.UUID, now time.Time) wager.ExternalParams {
	return wager.ExternalParams{
		ID: id, WalletID: c.WalletID, PlayerID: c.PlayerID, Kind: c.Kind, Amount: c.Amount,
		CorrelationID: c.CorrelationID, Now: now,
		External: wager.External{
			ProviderID: c.ProviderID, ExternalID: c.ExternalID, IdempotencyKey: c.IdempotencyKey,
			PayloadHash: c.PayloadHash, RoundID: c.RoundID, GameID: c.GameID,
			ReferenceExternalID: c.ReferenceExternalID,
		},
	}
}

// OpenWalletInput is the raw wallet opening request.
type OpenWalletInput struct {
	// PlayerID is the owner's UUID, in any case.
	PlayerID string
	// Amount is the initial balance in the canonical two-decimal form.
	Amount string
	// Currency is the ISO 4217 code of the wallet.
	Currency string
	// CorrelationID traces the request across services.
	CorrelationID string
}

// OpenWalletCommand is a validated wallet opening.
type OpenWalletCommand struct {
	// PlayerID is the normalized owner identifier.
	PlayerID uuid.UUID
	// InitialBalance is credited by the OPENING transaction when positive.
	InitialBalance money.Money
	// CorrelationID traces the request across services.
	CorrelationID string
}

// NewOpenWalletCommand validates a wallet opening request.
func NewOpenWalletCommand(in OpenWalletInput) (OpenWalletCommand, error) {
	ids, err := parseUUIDs(in.PlayerID)
	if err != nil {
		return OpenWalletCommand{}, err
	}
	balance, err := money.Parse(in.Amount, in.Currency)
	if err != nil {
		return OpenWalletCommand{}, invalid(err)
	}
	return OpenWalletCommand{PlayerID: ids[0], InitialBalance: balance, CorrelationID: in.CorrelationID}, nil
}
