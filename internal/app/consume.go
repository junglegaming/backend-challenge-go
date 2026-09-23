package app

import (
	"context"
)

// InboundMessage is a decoded broker message carrying an operation.
type InboundMessage struct {
	// Consumer names the inbox partition (one per queue consumer).
	Consumer string
	// MessageID is the broker message id, unique within Consumer.
	MessageID string
	// Hash fingerprints the message content to detect a reused message id.
	Hash string
	// Command is the validated operation carried by the message.
	Command SubmitCommand
}

// ConsumeResult is the outcome of a consumed message.
type ConsumeResult struct {
	// Result is the submission outcome; it is empty for a Duplicate.
	Result SubmitResult
	// Duplicate reports a redelivery already handled by the inbox.
	Duplicate bool
}

// ConsumeMessage records the message in the inbox and processes its
// operation in the same SQL transaction, so the inbox completion, the
// operation state, the balance, the ledger and the outbox records commit (or
// roll back) together. A redelivery of a committed message is a duplicate
// and is not observed again.
func (s *WagerService) ConsumeMessage(ctx context.Context, msg InboundMessage) (ConsumeResult, error) {
	start := s.Clock.Now()
	out, err := inTx(ctx, s, "consume", func(ctx context.Context, r Repositories) (ConsumeResult, error) {
		return s.consumeInTx(ctx, r, msg)
	})
	if err == nil && !out.Duplicate {
		s.observe(SourceSQS, out.Result, start)
	}
	return out, err
}

func (s *WagerService) consumeInTx(ctx context.Context, r Repositories, msg InboundMessage) (ConsumeResult, error) {
	now := s.Clock.Now()
	entry, created, err := r.Inbox().Register(ctx, msg.Consumer, msg.MessageID, msg.Hash, now)
	if err != nil {
		return ConsumeResult{}, err
	}
	if !created {
		return duplicate(entry, msg)
	}
	res, err := s.submitInTx(ctx, r, msg.Command, now)
	if err != nil {
		return ConsumeResult{}, err
	}
	return ConsumeResult{Result: res}, r.Inbox().Complete(ctx, msg.Consumer, msg.MessageID, res.Transaction.ID(), now)
}

// duplicate verifies that a redelivered message kept its content. Inbox rows
// are only visible once committed together with their completion.
func duplicate(entry InboxEntry, msg InboundMessage) (ConsumeResult, error) {
	if entry.PayloadHash != msg.Hash {
		return ConsumeResult{}, ErrInboxConflict
	}
	return ConsumeResult{Duplicate: true}, nil
}
