package sqs

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"go.uber.org/fx"

	"github.com/Lzdevmendes/backend-challenge-go/internal/app/usecases"
	"github.com/Lzdevmendes/backend-challenge-go/internal/domain"
)

type SQSConfig struct {
	QueueURL   string
	Region     string
	Endpoint   string
	MaxRetries int
}

type Consumer struct {
	client   *sqs.Client
	queueURL string
	usecase  *usecases.ProcessTransactionUseCase
	cancel   context.CancelFunc
}

func NewClient(cfg SQSConfig) (*sqs.Client, error) {
	ctx := context.Background()

	customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		if cfg.Endpoint != "" {
			return aws.Endpoint{
				PartitionID:   "aws",
				URL:           cfg.Endpoint,
				SigningRegion: cfg.Region,
			}, nil
		}
		return aws.Endpoint{}, &aws.EndpointNotFoundError{}
	})

	awsCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(cfg.Region),
		config.WithEndpointResolverWithOptions(customResolver),
	)
	if err != nil {
		return nil, err
	}

	return sqs.NewFromConfig(awsCfg), nil
}

func NewConsumer(lc fx.Lifecycle, cfg SQSConfig, client *sqs.Client, uc *usecases.ProcessTransactionUseCase) (*Consumer, error) {
	consumer := &Consumer{
		client:   client,
		queueURL: cfg.QueueURL,
		usecase:  uc,
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			var cCtx context.Context
			cCtx, consumer.cancel = context.WithCancel(context.Background())
			go consumer.Start(cCtx)
			return nil
		},
		OnStop: func(ctx context.Context) error {
			if consumer.cancel != nil {
				consumer.cancel()
			}
			return nil
		},
	})

	return consumer, nil
}

type SQSMessageData struct {
	ProviderID            string `json:"providerId"`
	ExternalTransactionID string `json:"externalTransactionId"`
	IdempotencyKey        string `json:"idempotencyKey"`
	PlayerID              string `json:"playerId"`
	WalletID              string `json:"walletId"`
	RoundID               string `json:"roundId"`
	GameID                string `json:"gameId"`
	Kind                  string `json:"kind"`
	Money                 struct {
		Amount   string `json:"amount"`
		Currency string `json:"currency"`
	} `json:"money"`
	ReferenceExternalTransactionID string `json:"referenceExternalTransactionId"`
}

type SQSMessageEnvelope struct {
	MessageID  string         `json:"messageId"`
	Type       string         `json:"type"`
	OccurredAt string         `json:"occurredAt"`
	Data       SQSMessageData `json:"data"`
}

func (c *Consumer) Start(ctx context.Context) {
	log.Println("Starting SQS consumer on", c.queueURL)
	for {
		select {
		case <-ctx.Done():
			return
		default:
			msgResult, err := c.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
				QueueUrl:            aws.String(c.queueURL),
				MaxNumberOfMessages: 10,
				WaitTimeSeconds:     20,
				MessageSystemAttributeNames: []types.MessageSystemAttributeName{
					types.MessageSystemAttributeNameApproximateReceiveCount,
				},
			})

			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("failed to receive messages: %v", err)
				time.Sleep(2 * time.Second)
				continue
			}

			for _, msg := range msgResult.Messages {
				c.processMessage(ctx, msg)
			}
		}
	}
}

func (c *Consumer) processMessage(ctx context.Context, msg types.Message) {
	if msg.Body == nil {
		c.nackMessage(ctx, msg)
		return
	}

	var env SQSMessageEnvelope
	if err := json.Unmarshal([]byte(*msg.Body), &env); err != nil {
		log.Printf("invalid json in sqs message: %v", err)
		c.nackMessage(ctx, msg)
		return
	}

	playerID, err := uuid.Parse(env.Data.PlayerID)
	walletID, err := uuid.Parse(env.Data.WalletID)
	money, moneyErr := domain.ParseMoney(env.Data.Money.Amount, env.Data.Money.Currency)

	if err != nil || moneyErr != nil {
		log.Printf("invalid domain data in message %s", env.MessageID)
		c.nackMessage(ctx, msg)
		return
	}

	req := domain.ExternalRequest{
		ProviderID:                     env.Data.ProviderID,
		ExternalTransactionID:          env.Data.ExternalTransactionID,
		IdempotencyKey:                 env.Data.IdempotencyKey,
		PlayerID:                       playerID,
		WalletID:                       walletID,
		RoundID:                        env.Data.RoundID,
		GameID:                         env.Data.GameID,
		Kind:                           domain.Kind(env.Data.Kind),
		Money:                          money,
		ReferenceExternalTransactionID: env.Data.ReferenceExternalTransactionID,
	}

	msgID := env.MessageID
	if msgID == "" {
		msgID = *msg.MessageId
	}

	_, err = c.usecase.Execute(ctx, req, "WagerConsumer", msgID)
	if err != nil {
		log.Printf("transient error processing message %s: %v", env.MessageID, err)
		c.nackMessage(ctx, msg)
		return
	}

	c.deleteMessage(ctx, msg.ReceiptHandle)
}

func (c *Consumer) nackMessage(ctx context.Context, msg types.Message) {
	if msg.ReceiptHandle == nil {
		return
	}
	
	// Exponential backoff
	receiveCount := 1
	if countStr, ok := msg.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)]; ok {
		if c, err := strconv.Atoi(countStr); err == nil {
			receiveCount = c
		}
	}
	
	visibilityTimeout := int32(receiveCount * receiveCount * 2) // e.g. 2s, 8s, 18s...
	
	_, err := c.client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(c.queueURL),
		ReceiptHandle:     msg.ReceiptHandle,
		VisibilityTimeout: visibilityTimeout,
	})
	if err != nil {
		log.Printf("failed to nack message (change visibility): %v", err)
	}
}

func (c *Consumer) deleteMessage(ctx context.Context, receiptHandle *string) {
	if receiptHandle == nil {
		return
	}
	_, err := c.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(c.queueURL),
		ReceiptHandle: receiptHandle,
	})
	if err != nil {
		log.Printf("failed to delete message: %v", err)
	}
}
