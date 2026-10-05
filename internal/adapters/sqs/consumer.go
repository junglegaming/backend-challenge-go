package sqs

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/VictorM0nteiro/backend-challenge-go/internal/adapters/postgres"
	"github.com/VictorM0nteiro/backend-challenge-go/internal/logctx"
)

const (
	defaultVisibilityTimeout = 30 // seconds a received message stays hidden from others
	defaultWaitSeconds       = 10 // long polling: ReceiveMessage waits this long for work
	maxRetryDelay            = 60 * time.Second
	ackTimeout               = 5 * time.Second
)

// Consumer receives wager messages and applies them through the same processor
// the HTTP handler uses.
type Consumer struct {
	client     *awssqs.Client
	queueURL   string
	processor  *postgres.WagerProcessor
	visibility int32
	waitSecs   int32
	done       chan struct{}
}

// NewConsumer builds a consumer for queueURL. It does not start receiving; Run does.
func NewConsumer(client *awssqs.Client, queueURL string, processor *postgres.WagerProcessor) *Consumer {
	return &Consumer{
		client:     client,
		queueURL:   queueURL,
		processor:  processor,
		visibility: defaultVisibilityTimeout,
		waitSecs:   defaultWaitSeconds,
		done:       make(chan struct{}),
	}
}

// Run receives until ctx is cancelled. It takes one message at a time, so the
// messages of a group (one wallet) are applied in the order SQS delivered them.
// Batching would let a retried message fall behind the ones after it.
func (c *Consumer) Run(ctx context.Context) {
	defer close(c.done)

	for ctx.Err() == nil {
		out, err := c.client.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
			QueueUrl:            aws.String(c.queueURL),
			MaxNumberOfMessages: 1,
			WaitTimeSeconds:     c.waitSecs,
			VisibilityTimeout:   c.visibility,
			MessageSystemAttributeNames: []types.MessageSystemAttributeName{
				types.MessageSystemAttributeNameApproximateReceiveCount,
			},
		})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error("receive messages", "error", err)
			sleepCtx(ctx, time.Second)
			continue
		}

		for _, msg := range out.Messages {
			c.handle(ctx, msg)
		}
	}
}

// handle applies one message and then settles it: delete it on success, leave
// it for the redrive policy on a permanent failure, and postpone it with
// backoff on a transient one.
//
// The envelope's messageId is the correlation id: it is the identity the inbox
// uses, and it is what ties every log line, and later every event, to this
// message. The SQS id is logged too, to correlate with AWS.
func (c *Consumer) handle(ctx context.Context, msg types.Message) {
	ctx = logctx.NewContext(ctx)
	env, err := parse(aws.ToString(msg.Body))

	sqsID := aws.ToString(msg.MessageId)
	correlationID := env.MessageID
	if correlationID == "" {
		correlationID = sqsID
	}
	logctx.SetCorrelationID(ctx, correlationID)
	logctx.Add(ctx, slog.String("sqsMessageId", sqsID))
	if err == nil {
		logctx.Add(ctx,
			slog.String("messageId", env.MessageID),
			slog.String("providerId", env.Data.ProviderID),
			slog.String("walletId", env.Data.WalletID.String()),
			slog.String("kind", string(env.Data.Kind)),
		)
	}

	var out postgres.Outcome
	if err == nil {
		out, err = c.apply(ctx, env)
	}
	if err == nil {
		logctx.Add(ctx, slog.String("transactionId", out.TransactionID.String()))
	}

	switch {
	case err == nil:
		// Done or rejected by the business rules. Both are terminal outcomes.
		slog.InfoContext(ctx, "message settled", "status", out.StatusCode, "replayed", out.Replayed)
		c.delete(ctx, msg)
	case ctx.Err() != nil:
		// Shutdown interrupted the work. Make the message visible now so another
		// instance can take it, instead of waiting for the timeout to run out.
		slog.InfoContext(ctx, "shutdown during message, releasing it")
		c.setVisibility(ctx, msg, 0)
	case isPermanent(err):
		slog.ErrorContext(ctx, "permanent failure, message left for the DLQ", "error", err)
	default:
		slog.WarnContext(ctx, "transient failure, retrying with backoff", "error", err, "receiveCount", receiveCount(msg))
		c.setVisibility(ctx, msg, retryDelay(receiveCount(msg)))
	}
}

// apply maps a parsed envelope to the processor's input and runs it. A rejection
// by the business rules is not an error here: Process returns it as an outcome.
func (c *Consumer) apply(ctx context.Context, env envelope) (postgres.Outcome, error) {
	req, err := toRequest(env)
	if err != nil {
		return postgres.Outcome{}, err
	}
	return c.processor.Process(ctx, req)
}

// delete runs only after Process returned, so the operation is already
// committed. The context survives shutdown: a committed message is always
// acknowledged. If the delete fails, the message comes back, and the inbox
// recognises it and only deletes it again.
func (c *Consumer) delete(ctx context.Context, msg types.Message) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), ackTimeout)
	defer cancel()

	_, err := c.client.DeleteMessage(ctx, &awssqs.DeleteMessageInput{
		QueueUrl:      aws.String(c.queueURL),
		ReceiptHandle: msg.ReceiptHandle,
	})
	if err != nil {
		slog.ErrorContext(ctx, "delete message", "error", err)
	}
}

// setVisibility changes how long the message stays hidden. Zero makes it
// visible at once. A postponed message is retried after the chosen delay.
func (c *Consumer) setVisibility(ctx context.Context, msg types.Message, seconds int32) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), ackTimeout)
	defer cancel()

	_, err := c.client.ChangeMessageVisibility(ctx, &awssqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(c.queueURL),
		ReceiptHandle:     msg.ReceiptHandle,
		VisibilityTimeout: seconds,
	})
	if err != nil {
		slog.ErrorContext(ctx, "change message visibility", "error", err)
	}
}

// receiveCount is how many times SQS has delivered this message so far.
func receiveCount(msg types.Message) int {
	n, err := strconv.Atoi(msg.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)])
	if err != nil || n < 1 {
		return 1
	}
	return n
}

// retryDelay doubles with each delivery: 2s, 4s, 8s, and so on, capped at 60s.
// The redrive policy caps the total number of attempts, so a message that keeps
// failing cannot circulate forever.
func retryDelay(receiveCount int) int32 {
	delay := 2 * time.Second
	for i := 1; i < receiveCount && delay < maxRetryDelay; i++ {
		delay *= 2
	}
	if delay > maxRetryDelay {
		delay = maxRetryDelay
	}
	return int32(delay / time.Second)
}

// Wait blocks until Run has returned, or until ctx ends.
func (c *Consumer) Wait(ctx context.Context) error {
	select {
	case <-c.done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("sqs: consumer did not stop: %w", ctx.Err())
	}
}

func sleepCtx(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

// Explicação
// - Um por vez. MaxNumberOfMessages: 1. Com lote, se a mensagem 1 de uma carteira falha e vai para retry, a 2 da mesma carteira já foi entregue e seria aplicada antes.
// Um por vez mantém a ordem dentro do grupo, que é o que o MessageGroupId promete.
// - Três destinos depois do processamento. Sucesso ou rejeição de negócio: apaga. Erro permanente: deixa e o redrive move para a DLQ. Erro transitório: adia com backoff.
// - Backoff pelo ChangeMessageVisibility. Não existe sleep no consumidor. O adiamento fica na própria fila, e a mensagem volta quando o tempo acaba. O atraso dobra a cada entrega,
// de 2 s até 60 s.
// - Shutdown. Se o contexto é cancelado no meio do processamento, o Process aborta e a transação desfaz tudo. Então a mensagem é liberada imediatamente (visibility 0), e outra instância
// pega. Isso é o "liberar visibilidade" do README.
// - context.WithoutCancel no apagar e no liberar. Depois do commit, a confirmação precisa acontecer mesmo com o shutdown em curso. Sem isso, uma mensagem já processada poderia voltar
// e depender do inbox para não repetir.
