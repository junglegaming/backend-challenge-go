package sqs

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/Pantani/backend-challenge-go/internal/app"
	"github.com/Pantani/backend-challenge-go/internal/observability"
	"github.com/Pantani/backend-challenge-go/internal/worker"
)

// Consumer outcomes, the label set of ConsumerMetrics.SQSMessage.
const (
	// OutcomeProcessed is a message whose operation committed.
	OutcomeProcessed = "processed"
	// OutcomeDuplicate is a redelivery absorbed by the inbox.
	OutcomeDuplicate = "duplicate"
	// OutcomeRetry is a transient failure left for redelivery with backoff.
	OutcomeRetry = "retry"
	// OutcomeDLQ is an invalid message or permanent failure copied to the DLQ.
	OutcomeDLQ = "dlq"
	// OutcomeReleased is a message made visible again without processing
	// (shutdown, or the tail of a group whose head was retried).
	OutcomeReleased = "released"
)

// Default budgets applied when the configuration leaves them at zero.
const (
	// DefaultAckTimeout bounds the broker follow-up of one message (delete,
	// visibility change, DLQ copy).
	DefaultAckTimeout = 5 * time.Second
	// deadLetterGroup is the DLQ MessageGroupId of messages without one.
	deadLetterGroup = "dead-letters"
	// failureReasonLimit caps the failureReason attribute copied to the DLQ.
	failureReasonLimit = 256
	// redriveHeadroom is how far the queue redrive threshold sits above the
	// consumer's own limit. See RedriveMaxReceiveCount.
	redriveHeadroom = 20
)

// RedriveMaxReceiveCount is the maxReceiveCount to provision on the input
// queue for a consumer that dead-letters at maxReceiveCount receives.
//
// In a FIFO queue a receive returns the group's head together with the
// messages behind it, and every receive increments ApproximateReceiveCount
// of all of them. While a head is being retried, the messages behind it are
// received (and released) with it, so their counts climb without them ever
// being attempted. The consumer therefore dead-letters a failing message
// itself, with its failureReason, at maxReceiveCount receives, and the
// broker threshold sits above that: a message behind a failing head
// accumulates at most maxReceiveCount receives from that head, plus one per
// message ahead of it within the batch (at most 9) that fails its single
// remaining attempt, plus a few releases on shutdown. The redrive stays as
// a safety net (for example when the DLQ copy itself keeps failing).
func RedriveMaxReceiveCount(maxReceiveCount int) int {
	return maxReceiveCount + redriveHeadroom
}

// Processor handles a decoded message (app.WagerService).
type Processor interface {
	// ConsumeMessage runs the operation carried by msg exactly once per
	// (consumer, messageId) through the inbox. A transient error
	// (app.IsTransient) leaves the message for a retry; any other error
	// sends it to the DLQ.
	ConsumeMessage(ctx context.Context, msg app.InboundMessage) (app.ConsumeResult, error)
}

// ConsumerMetrics counts consumed messages.
type ConsumerMetrics interface {
	// SQSMessage counts one message by outcome: one of OutcomeProcessed,
	// OutcomeDuplicate, OutcomeRetry, OutcomeDLQ or OutcomeReleased.
	SQSMessage(outcome string)
}

// ConsumerConfig configures the consumer.
type ConsumerConfig struct {
	// Name is the inbox consumer_name: the deduplication scope of messageId.
	Name string
	// QueueURL is the FIFO input queue; DLQURL its dead-letter queue.
	QueueURL string
	DLQURL   string
	// MaxMessages is the receive batch size (1..10, an SQS limit).
	MaxMessages int32
	// WaitTime is the long-poll duration (0..20s). With 0 an empty receive
	// is followed by a RetryBase pause instead of an immediate re-poll.
	WaitTime time.Duration
	// VisibilityTimeout overrides the queue attribute for every receive. It
	// must agree with the provisioned queue value (SQS_VISIBILITY_TIMEOUT)
	// and exceed MaxMessages * (ProcessTimeout + AckTimeout), because this
	// consumer handles a received batch serially.
	VisibilityTimeout time.Duration
	// ProcessTimeout bounds one message. Together with AckTimeout and
	// MaxMessages it forms the full-batch budget that VisibilityTimeout must
	// exceed; the per-message relationship alone does not prevent overlap.
	ProcessTimeout time.Duration
	// AckTimeout bounds the broker follow-up of a message (delete, retry
	// visibility change, DLQ copy). It is a budget of its own, so a message
	// that exhausted ProcessTimeout still gets its backoff applied. Zero
	// means DefaultAckTimeout.
	AckTimeout time.Duration
	// RetryBase is the visibility delay after the first transient failure;
	// it doubles with the receive count (app.Backoff) up to RetryMax, which
	// must not exceed the SQS maximum of 12h.
	RetryBase time.Duration
	RetryMax  time.Duration
	// MaxReceiveCount is the receive count at which a transient failure is
	// dead-lettered by the consumer instead of retried. The queue redrive
	// must be provisioned above it (RedriveMaxReceiveCount).
	MaxReceiveCount int
	// Senders binds broker identities to the providers they may act for.
	Senders SenderPolicy
}

// ackTimeout returns the configured ack budget or its default.
func (c ConsumerConfig) ackTimeout() time.Duration {
	if c.AckTimeout > 0 {
		return c.AckTimeout
	}
	return DefaultAckTimeout
}

// Consumer polls the FIFO input queue. A message is deleted only after its
// handling committed; transient failures are retried with exponential
// backoff through the visibility timeout until MaxReceiveCount, when the
// consumer copies the message to the DLQ with its failure reason. Invalid
// messages and permanent errors go to the DLQ immediately. When a message is left in the
// queue (retry, or a failed DLQ copy), the rest of its MessageGroupId in the
// batch is released unprocessed so the group is redelivered in order.
type Consumer struct {
	api     API
	cfg     ConsumerConfig
	proc    Processor
	logger  *slog.Logger
	metrics ConsumerMetrics
}

// NewConsumer builds a consumer.
func NewConsumer(api API, cfg ConsumerConfig, proc Processor, logger *slog.Logger, metrics ConsumerMetrics) *Consumer {
	return &Consumer{api: api, cfg: cfg, proc: proc, logger: logger, metrics: metrics}
}

// Run polls until ctx is cancelled (SIGTERM): it then stops fetching,
// finishes the message in progress within ProcessTimeout plus AckTimeout
// (the broker follow-up has its own budget, detached from ctx) and releases
// the visibility of the messages it had not started.
func (c *Consumer) Run(ctx context.Context) {
	for ctx.Err() == nil {
		c.PollOnce(ctx)
	}
}

// PollOnce receives and handles one batch. It is exported for tests; Run is
// the loop. Without long polling (WaitTime 0) an empty receive pauses for
// RetryBase so an idle consumer does not spin.
func (c *Consumer) PollOnce(ctx context.Context) {
	out, err := c.api.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(c.cfg.QueueURL),
		MaxNumberOfMessages: c.cfg.MaxMessages,
		WaitTimeSeconds:     int32(c.cfg.WaitTime.Seconds()),
		VisibilityTimeout:   int32(c.cfg.VisibilityTimeout.Seconds()),
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{
			types.MessageSystemAttributeNameApproximateReceiveCount, types.MessageSystemAttributeNameSenderId,
			types.MessageSystemAttributeNameMessageGroupId, types.MessageSystemAttributeNameMessageDeduplicationId,
		},
	})
	if err != nil {
		c.receiveFailed(ctx, err)
		return
	}
	if len(out.Messages) == 0 && c.cfg.WaitTime <= 0 {
		worker.Sleep(ctx, c.cfg.RetryBase)
		return
	}
	c.process(ctx, out.Messages)
}

// process handles a batch in order. The receive visibility protects the
// worst-case budget of the complete batch. A batch can carry several messages
// of the same MessageGroupId (the in-order tail of a wallet): once a message
// is left in the queue, the rest of its group is released instead of processed,
// so the group is redelivered in order after its head.
func (c *Consumer) process(ctx context.Context, msgs []types.Message) {
	blocked := map[string]bool{}
	for i, m := range msgs {
		if ctx.Err() != nil {
			c.release(ctx, msgs[i:])
			return
		}
		c.step(ctx, m, blocked)
	}
}

func (c *Consumer) step(ctx context.Context, m types.Message, blocked map[string]bool) {
	group := groupID(m)
	if blocked[group] {
		c.release(ctx, []types.Message{m})
		return
	}
	if !c.handle(ctx, m) && group != "" {
		blocked[group] = true
	}
}

func (c *Consumer) receiveFailed(ctx context.Context, err error) {
	if ctx.Err() != nil {
		return
	}
	c.logger.WarnContext(ctx, "sqs receive failed", "error", err)
	worker.Sleep(ctx, c.cfg.RetryBase)
}

// handle processes one message detached from the shutdown signal so the
// message in progress completes (or times out) instead of being aborted
// mid-way. Processing runs within ProcessTimeout; the broker follow-up gets
// its own AckTimeout budget so a processing deadline never skips the ack,
// the retry backoff or the DLQ copy. It reports false when the message was
// left in the queue (retry or failed DLQ copy) so its group tail is released.
func (c *Consumer) handle(parent context.Context, m types.Message) bool {
	ctx := observability.WithAttrs(parent, slog.String("sqsMessageId", aws.ToString(m.MessageId)))
	msg, err := c.decode(m)
	if err != nil {
		return c.deadLetter(ctx, m, err)
	}
	ctx = observability.WithAttrs(ctx, slog.String("messageId", msg.MessageID), slog.String("correlationId", msg.MessageID),
		slog.String("providerId", msg.Command.ProviderID), slog.String("walletId", msg.Command.WalletID.String()))
	procCtx, cancel := worker.Detach(ctx, c.cfg.ProcessTimeout)
	defer cancel()
	res, err := c.proc.ConsumeMessage(procCtx, msg)
	switch {
	case err == nil:
		c.ack(ctx, m, res)
		return true
	case app.IsTransient(err):
		if n := receiveCount(m); n >= c.cfg.MaxReceiveCount {
			return c.deadLetter(ctx, m, fmt.Errorf("gave up after %d receives: %w", n, err))
		}
		c.retry(ctx, m, err)
		return false
	default:
		return c.deadLetter(ctx, m, err)
	}
}

// decode validates the message and binds its providerId to the sender.
func (c *Consumer) decode(m types.Message) (app.InboundMessage, error) {
	msg, err := DecodeMessage(c.cfg.Name, aws.ToString(m.Body))
	if err != nil {
		return app.InboundMessage{}, err
	}
	if err := validateFIFOIdentity(m, msg); err != nil {
		return app.InboundMessage{}, err
	}
	sender := m.Attributes[string(types.MessageSystemAttributeNameSenderId)]
	return msg, c.cfg.Senders.Authorize(sender, msg.Command.ProviderID)
}

func validateFIFOIdentity(m types.Message, msg app.InboundMessage) error {
	// SQS orders by the group string, so every spelling of a wallet UUID must
	// use the same canonical group even when the JSON walletId is uppercase.
	if groupID(m) != msg.Command.WalletID.String() {
		return fmt.Errorf("%w: MessageGroupId must be the canonical walletId", ErrInvalidMessage)
	}
	if dedupID(m) != msg.MessageID {
		return fmt.Errorf("%w: MessageDeduplicationId must equal messageId", ErrInvalidMessage)
	}
	return nil
}

// ack deletes a handled message within the ack budget.
func (c *Consumer) ack(parent context.Context, m types.Message, res app.ConsumeResult) {
	ctx, cancel := worker.Detach(parent, c.cfg.ackTimeout())
	defer cancel()
	outcome := OutcomeProcessed
	if res.Duplicate {
		outcome = OutcomeDuplicate
	} else {
		ctx = observability.WithAttrs(ctx, slog.String("transactionId", res.Result.Transaction.ID().String()))
	}
	c.logger.InfoContext(ctx, "sqs message handled", "outcome", outcome)
	c.metrics.SQSMessage(outcome)
	c.delete(ctx, m)
}

func (c *Consumer) delete(ctx context.Context, m types.Message) bool {
	_, err := c.api.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: aws.String(c.cfg.QueueURL), ReceiptHandle: m.ReceiptHandle})
	if err != nil {
		// The source message remains available regardless of whether this path
		// followed a committed operation or a copy to the DLQ.
		c.logger.WarnContext(ctx, "sqs delete failed; message remains available for redelivery", "error", err)
		return false
	}
	return true
}

// retry hides the message for an exponential backoff based on its receive
// count.
func (c *Consumer) retry(parent context.Context, m types.Message, cause error) {
	ctx, cancel := worker.Detach(parent, c.cfg.ackTimeout())
	defer cancel()
	delay := c.backoff(receiveCount(m))
	c.logger.WarnContext(ctx, "sqs message will be retried", "error", cause, "delaySeconds", int(delay.Seconds()))
	c.metrics.SQSMessage(OutcomeRetry)
	c.changeVisibility(ctx, m, delay)
}

// backoff is the visibility delay for the count-th receive: RetryBase on
// the first, doubling afterwards up to RetryMax.
func (c *Consumer) backoff(count int) time.Duration {
	return app.Backoff(c.cfg.RetryBase, c.cfg.RetryMax, count-1)
}

// receiveCount reads ApproximateReceiveCount; a missing or malformed value
// counts as the first receive.
func receiveCount(m types.Message) int {
	n, err := strconv.Atoi(m.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)])
	if err != nil || n < 1 {
		return 1
	}
	return n
}

func groupID(m types.Message) string {
	return m.Attributes[string(types.MessageSystemAttributeNameMessageGroupId)]
}

func dedupID(m types.Message) string {
	return m.Attributes[string(types.MessageSystemAttributeNameMessageDeduplicationId)]
}

func (c *Consumer) changeVisibility(ctx context.Context, m types.Message, d time.Duration) {
	_, err := c.api.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl: aws.String(c.cfg.QueueURL), ReceiptHandle: m.ReceiptHandle, VisibilityTimeout: int32(d.Seconds()),
	})
	if err != nil {
		c.logger.WarnContext(ctx, "sqs change visibility failed", "error", err)
	}
}

// deadLetter copies the message to the DLQ with the failure reason, keeping
// its MessageGroupId (so a DLQ replay preserves the wallet order), then
// removes it from the input queue. It reports whether the message left the
// input queue: if the copy or source deletion fails the message stays, its
// group tail is released and the redrive policy eventually moves it.
func (c *Consumer) deadLetter(parent context.Context, m types.Message, cause error) bool {
	ctx, cancel := worker.Detach(parent, c.cfg.ackTimeout())
	defer cancel()
	c.logger.ErrorContext(ctx, "sqs message sent to DLQ", "error", cause)
	group := groupID(m)
	if group == "" {
		group = deadLetterGroup
	}
	_, err := c.api.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:               aws.String(c.cfg.DLQURL),
		MessageBody:            m.Body,
		MessageGroupId:         aws.String(group),
		MessageDeduplicationId: m.MessageId,
		MessageAttributes: map[string]types.MessageAttributeValue{
			"failureReason": {DataType: aws.String("String"), StringValue: aws.String(truncate(cause.Error(), failureReasonLimit))},
		},
	})
	if err != nil {
		c.logger.ErrorContext(ctx, "sqs DLQ send failed", "error", err)
		return false
	}
	c.metrics.SQSMessage(OutcomeDLQ)
	return c.delete(ctx, m)
}

// truncate replaces invalid bytes and cuts s to at most n bytes without
// splitting a multi-byte rune, since SQS rejects attributes that are not
// valid UTF-8.
func truncate(s string, n int) string {
	s = strings.ToValidUTF8(s, "\uFFFD")
	if len(s) <= n {
		return s
	}
	cut := n
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut]
}

// release makes unstarted messages visible again for a safe redelivery.
func (c *Consumer) release(parent context.Context, msgs []types.Message) {
	ctx, cancel := worker.Detach(parent, c.cfg.ackTimeout())
	defer cancel()
	for _, m := range msgs {
		c.metrics.SQSMessage(OutcomeReleased)
		c.changeVisibility(ctx, m, 0)
	}
}
