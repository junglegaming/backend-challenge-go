package sqs

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"

	"github.com/VictorM0nteiro/backend-challenge-go/internal/adapters/postgres"
	"github.com/VictorM0nteiro/backend-challenge-go/internal/domain"
	"github.com/VictorM0nteiro/backend-challenge-go/internal/testutil"
)

const testRegion = "us-east-1"

var (
	localStackOnce     sync.Once
	localStackEndpoint string
	localStackErr      error
)

// sharedLocalStack starts one LocalStack for the integration tests of this
// package. Booting it is slow; each test gets its own queues, so they stay isolated.
func sharedLocalStack(t *testing.T) string {
	t.Helper()
	localStackOnce.Do(func() {
		// LocalStack accepts any credentials, but the SDK refuses to sign without some.
		_ = os.Setenv("AWS_ACCESS_KEY_ID", "test")
		_ = os.Setenv("AWS_SECRET_ACCESS_KEY", "test")

		ls, err := testutil.StartLocalStack(context.Background())
		if err != nil {
			localStackErr = err
			return
		}
		localStackEndpoint = ls.Endpoint
	})
	if localStackErr != nil {
		t.Fatalf("start localstack: %v", localStackErr)
	}
	return localStackEndpoint
}

func newTestClient(t *testing.T, endpoint string) *awssqs.Client {
	t.Helper()
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(), awsconfig.WithRegion(testRegion))
	if err != nil {
		t.Fatalf("load aws config: %v", err)
	}
	return awssqs.NewFromConfig(cfg, func(o *awssqs.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// harness is one provider, one funded wallet, and a FIFO queue with its DLQ.
type harness struct {
	t        *testing.T
	client   *awssqs.Client
	pool     *postgres.Pool
	queueURL string
	dlqURL   string
	walletID uuid.UUID
	playerID uuid.UUID
}

func newHarness(t *testing.T, opening string) *harness {
	t.Helper()
	ctx := context.Background()

	pool, err := postgres.NewPool(ctx, postgres.PoolConfig{
		DSN:            testutil.StartPostgres(t),
		MaxConns:       5,
		AcquireTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	t.Cleanup(pool.Close)

	h := &harness{t: t, pool: pool, playerID: uuid.New()}

	money, err := domain.ParseExternalAmount(opening, "BRL")
	if err != nil {
		t.Fatalf("ParseExternalAmount: %v", err)
	}
	open, err := domain.OpenWallet(h.playerID, "BRL", money)
	if err != nil {
		t.Fatalf("OpenWallet: %v", err)
	}
	if err := postgres.NewWalletRepository(pool).OpenWallet(ctx, open); err != nil {
		t.Fatalf("persist wallet: %v", err)
	}
	h.walletID = open.Wallet.ID()

	h.client = newTestClient(t, sharedLocalStack(t))

	suffix := uuid.NewString()[:8]
	h.dlqURL = createQueue(t, h.client, "wager-"+suffix+"-dlq.fifo", nil)
	redrive := fmt.Sprintf(`{"deadLetterTargetArn":%q,"maxReceiveCount":"2"}`, queueArn(t, h.client, h.dlqURL))
	h.queueURL = createQueue(t, h.client, "wager-"+suffix+".fifo", map[string]string{"RedrivePolicy": redrive})
	return h
}

func createQueue(t *testing.T, c *awssqs.Client, name string, extra map[string]string) string {
	t.Helper()
	attrs := map[string]string{"FifoQueue": "true"}
	for k, v := range extra {
		attrs[k] = v
	}
	out, err := c.CreateQueue(context.Background(), &awssqs.CreateQueueInput{
		QueueName:  aws.String(name),
		Attributes: attrs,
	})
	if err != nil {
		t.Fatalf("create queue %s: %v", name, err)
	}
	return aws.ToString(out.QueueUrl)
}

func queueArn(t *testing.T, c *awssqs.Client, url string) string {
	t.Helper()
	out, err := c.GetQueueAttributes(context.Background(), &awssqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(url),
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	if err != nil {
		t.Fatalf("get queue arn: %v", err)
	}
	return out.Attributes[string(types.QueueAttributeNameQueueArn)]
}

// betMessage builds a message for a BET from this harness's wallet.
// betMessage builds a message for a BET from this harness's wallet.
func (h *harness) betMessage(messageID, key, amount string) string {
	return fmt.Sprintf(`{"messageId":%q,"type":"WagerTransactionRequested","occurredAt":"2026-09-08T12:00:00.000Z","data":{"providerId":"provider-a","externalTransactionId":%q,"idempotencyKey":%q,"playerId":%q,"walletId":%q,"roundId":"round-987","gameId":"fortune-chimp","kind":"BET","money":{"amount":%q,"currency":"BRL"}}}`,
		messageID, key, key, h.playerID.String(), h.walletID.String(), amount)
}

func (h *harness) send(body string) {
	h.t.Helper()
	_, err := h.client.SendMessage(context.Background(), &awssqs.SendMessageInput{
		QueueUrl:       aws.String(h.queueURL),
		MessageBody:    aws.String(body),
		MessageGroupId: aws.String(h.walletID.String()),
		// A fresh deduplication id per send. SQS would otherwise swallow a second
		// send of the same body within 5 minutes. Protecting the consumer from a
		// repeat is the inbox's job, and these tests check exactly that.
		MessageDeduplicationId: aws.String(uuid.NewString()),
	})
	if err != nil {
		h.t.Fatalf("send: %v", err)
	}
}

// startConsumer runs the consumer against the harness queue until the test ends.
// The visibility timeout is short, so a redelivery fits inside a test's budget.
func (h *harness) startConsumer() {
	c := NewConsumer(h.client, h.queueURL, postgres.NewWagerProcessor(h.pool))
	c.visibility = 1

	ctx, cancel := context.WithCancel(context.Background())
	go c.Run(ctx)
	h.t.Cleanup(func() {
		cancel()
		stopCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if err := c.Wait(stopCtx); err != nil {
			h.t.Errorf("consumer did not stop: %v", err)
		}
	})
}

// receiveDLQ waits for one message in the DLQ and returns its body, or "".
func (h *harness) receiveDLQ(d time.Duration) string {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		out, err := h.client.ReceiveMessage(context.Background(), &awssqs.ReceiveMessageInput{
			QueueUrl:            aws.String(h.dlqURL),
			MaxNumberOfMessages: 1,
			WaitTimeSeconds:     2,
		})
		if err != nil {
			h.t.Fatalf("receive dlq: %v", err)
		}
		if len(out.Messages) > 0 {
			return aws.ToString(out.Messages[0].Body)
		}
	}
	return ""
}

func (h *harness) balance() int64 {
	h.t.Helper()
	w, err := postgres.NewWalletRepository(h.pool).FindByID(context.Background(), h.walletID)
	if err != nil {
		h.t.Fatalf("FindByID: %v", err)
	}
	return w.Balance().AmountMinor()
}

func (h *harness) count(query string, args ...any) int {
	h.t.Helper()
	var n int
	if err := h.pool.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		h.t.Fatalf("count %q: %v", query, err)
	}
	return n
}

// eventually polls cond until it holds or d elapses.
func eventually(d time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return cond()
}

func TestConsumer_BetIsAppliedOnceEvenWhenTheMessageRepeats(t *testing.T) {
	h := newHarness(t, "1000.00")
	h.startConsumer()

	body := h.betMessage("msg-1", "provider-a:tx-1", "25.00")
	h.send(body)
	h.send(body) // a repeat of the same message: the inbox must recognise it

	if !eventually(20*time.Second, func() bool { return h.balance() == 97500 }) {
		t.Fatalf("balance = %d, want 97500", h.balance())
	}
	time.Sleep(2 * time.Second) // give the repeat time to be received and settled

	if got := h.balance(); got != 97500 {
		t.Fatalf("balance after the repeat = %d, want 97500", got)
	}
	// The opening also writes a wager row (kind OPENING), so count only the bets.
	if n := h.count(`SELECT count(*) FROM wager_transactions WHERE kind = 'BET'`); n != 1 {
		t.Fatalf("bets = %d, want 1", n)
	}
	if n := h.count(`SELECT count(*) FROM inbox WHERE message_id = 'msg-1' AND completed_at IS NOT NULL`); n != 1 {
		t.Fatalf("completed inbox rows = %d, want 1", n)
	}
}

func TestConsumer_SameOperationFromHTTPAndSQSAppliesOnce(t *testing.T) {
	h := newHarness(t, "1000.00")
	h.startConsumer()

	body := h.betMessage("msg-2", "provider-a:tx-2", "25.00")

	// The HTTP path: the same operation, the same key, the same hash. HTTP
	// requests carry no inbox row, so the inbox is cleared.
	env, err := parse(body)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	req, err := toRequest(env)
	if err != nil {
		t.Fatalf("toRequest: %v", err)
	}
	req.Inbox = nil
	if _, err := postgres.NewWagerProcessor(h.pool).Process(context.Background(), req); err != nil {
		t.Fatalf("HTTP-style process: %v", err)
	}
	if got := h.balance(); got != 97500 {
		t.Fatalf("balance after HTTP = %d, want 97500", got)
	}

	// Now the same operation arrives by SQS. It must be a replay, not a second debit.
	h.send(body)
	if !eventually(20*time.Second, func() bool {
		return h.count(`SELECT count(*) FROM inbox WHERE message_id = 'msg-2' AND completed_at IS NOT NULL`) == 1
	}) {
		t.Fatal("the SQS message was never settled")
	}

	if got := h.balance(); got != 97500 {
		t.Fatalf("balance after SQS = %d, want 97500 (debited once)", got)
	}
	if n := h.count(`SELECT count(*) FROM wager_transactions WHERE kind = 'BET'`); n != 1 {
		t.Fatalf("bets = %d, want 1", n)
	}
}

func TestConsumer_ReusedMessageIDWithAnotherBodyGoesToTheDLQ(t *testing.T) {
	h := newHarness(t, "1000.00")
	h.startConsumer()

	h.send(h.betMessage("msg-3", "provider-a:tx-3", "25.00"))
	if !eventually(20*time.Second, func() bool { return h.balance() == 97500 }) {
		t.Fatalf("first message not applied, balance = %d", h.balance())
	}

	// The same messageId, but a different operation. The producer reused an id.
	h.send(h.betMessage("msg-3", "provider-a:tx-3b", "30.00"))

	body := h.receiveDLQ(30 * time.Second)
	if body == "" {
		t.Fatal("the reused message never reached the DLQ")
	}
	if !strings.Contains(body, `"amount":"30.00"`) {
		t.Fatalf("DLQ body = %s, want the second message", body)
	}
	if got := h.balance(); got != 97500 {
		t.Fatalf("balance = %d, want 97500: the conflicting message must not apply", got)
	}
}
