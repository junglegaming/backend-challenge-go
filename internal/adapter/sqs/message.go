package sqs

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Pantani/backend-challenge-go/internal/app"
	"github.com/Pantani/backend-challenge-go/internal/contract"
)

// MessageType is the only accepted envelope type.
const MessageType = "WagerTransactionRequested"

const maxMessageIDBytes = 128

// ErrInvalidMessage reports an undecodable or invalid message; it is sent to
// the DLQ without retries.
var ErrInvalidMessage = errors.New("invalid message")

// MessageData is the payload of a wager-transactions message: the shared
// operation shape plus the idempotency key, which HTTP carries in a header.
type MessageData struct {
	contract.Operation
	IdempotencyKey string `json:"idempotencyKey"`
}

// Envelope is the wager-transactions message contract.
type Envelope struct {
	MessageID  string      `json:"messageId"`
	Type       string      `json:"type"`
	OccurredAt string      `json:"occurredAt"`
	Data       MessageData `json:"data"`
}

// EncodeMessage serializes an envelope as a message body.
func EncodeMessage(env Envelope) (string, error) {
	b, err := json.Marshal(env)
	if err != nil {
		return "", fmt.Errorf("encoding message: %w", err)
	}
	return string(b), nil
}

// decodeEnvelope reads exactly one JSON object with no unknown fields and
// nothing after it.
func decodeEnvelope(body string) (Envelope, error) {
	var env Envelope
	if err := contract.DecodeStrict(strings.NewReader(body), &env); err != nil {
		return Envelope{}, fmt.Errorf("%w: %w", ErrInvalidMessage, err)
	}
	return env, nil
}

// DecodeMessage parses and validates a message body with the same rules as
// HTTP. The envelope messageId is the durable message identity; the inbox
// hash covers the type, the idempotency key and the business payload hash.
func DecodeMessage(consumer, body string) (app.InboundMessage, error) {
	env, err := decodeEnvelope(body)
	if err != nil {
		return app.InboundMessage{}, err
	}
	if err := validateEnvelope(env); err != nil {
		return app.InboundMessage{}, err
	}
	cmd, err := app.NewSubmitCommand(env.Data.ToInput(env.Data.IdempotencyKey, env.MessageID, env.MessageID))
	if err != nil {
		return app.InboundMessage{}, fmt.Errorf("%w: %w", ErrInvalidMessage, err)
	}
	sum := sha256.Sum256([]byte(env.Type + "\n" + cmd.IdempotencyKey + "\n" + cmd.PayloadHash))
	return app.InboundMessage{Consumer: consumer, MessageID: env.MessageID, Hash: hex.EncodeToString(sum[:]), Command: cmd}, nil
}

func validateEnvelope(env Envelope) error {
	if id := strings.TrimSpace(env.MessageID); id == "" || len(env.MessageID) > maxMessageIDBytes || !printableASCII(env.MessageID) {
		return fmt.Errorf("%w: invalid messageId", ErrInvalidMessage)
	}
	if _, err := time.Parse(time.RFC3339, env.OccurredAt); err != nil {
		return fmt.Errorf("%w: invalid occurredAt: %w", ErrInvalidMessage, err)
	}
	if env.Type != MessageType {
		return fmt.Errorf("%w: unsupported type %q", ErrInvalidMessage, env.Type)
	}
	return nil
}

func printableASCII(value string) bool {
	for i := range len(value) {
		if value[i] < 0x20 || value[i] > 0x7e {
			return false
		}
	}
	return true
}
