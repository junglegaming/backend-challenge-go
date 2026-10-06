package sqs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
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
}

func NewConsumer(lc fx.Lifecycle, cfg SQSConfig, uc *usecases.ProcessTransactionUseCase) (*Consumer, error) {
	ctx := context.Background()

	customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		if cfg.Endpoint != "" {
			return aws.Endpoint{
				PartitionID:   "aws",
				URL:           cfg.Endpoint,
				SigningRegion: cfg.Region,
			}, nil
		}
		// returning EndpointNotFoundError will allow the service to fallback to its default resolution
		return aws.Endpoint{}, &aws.EndpointNotFoundError{}
	})

	awsCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(cfg.Region),
		config.WithEndpointResolverWithOptions(customResolver),
	)
	if err != nil {
		return nil, fmt.Errorf("unable to load SDK config: %w", err)
	}

	client := sqs.NewFromConfig(awsCfg)

	consumer := &Consumer{
		client:   client,
		queueURL: cfg.QueueURL,
		usecase:  uc,
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			go consumer.Start()
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

func (c *Consumer) Start() {
	log.Println("Starting SQS consumer on", c.queueURL)
	for {
		msgResult, err := c.client.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(c.queueURL),
			MaxNumberOfMessages: 10,
			WaitTimeSeconds:     20,
		})

		if err != nil {
			log.Printf("failed to receive messages: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}

		for _, msg := range msgResult.Messages {
			c.processMessage(msg)
		}
	}
}

func (c *Consumer) processMessage(msg types.Message) {
	if msg.Body == nil {
		return
	}

	var env SQSMessageEnvelope
	if err := json.Unmarshal([]byte(*msg.Body), &env); err != nil {
		log.Printf("invalid json in sqs message: %v", err)
		c.deleteMessage(msg.ReceiptHandle)
		return
	}

	playerID, err := uuid.Parse(env.Data.PlayerID)
	walletID, err := uuid.Parse(env.Data.WalletID)
	money, moneyErr := domain.ParseMoney(env.Data.Money.Amount, env.Data.Money.Currency)

	if err != nil || moneyErr != nil {
		log.Printf("invalid domain data in message %s", env.MessageID)
		c.deleteMessage(msg.ReceiptHandle)
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

	_, err = c.usecase.Execute(context.Background(), req, "WagerConsumer", env.MessageID)
	if err != nil {
		if errors.Is(err, domain.ErrInvalidArgument) || errors.Is(err, domain.ErrInvalidAmount) || errors.Is(err, domain.ErrCurrencyMismatch) || errors.Is(err, domain.ErrInsufficientFunds) {
			// Terminal business errors should not be retried
			c.deleteMessage(msg.ReceiptHandle)
			return
		}
		log.Printf("transient error processing message %s: %v", env.MessageID, err)
		return // Do not delete, let it be retried by visibility timeout
	}

	// Success
	c.deleteMessage(msg.ReceiptHandle)
}

func (c *Consumer) deleteMessage(receiptHandle *string) {
	if receiptHandle == nil {
		return
	}
	_, err := c.client.DeleteMessage(context.Background(), &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(c.queueURL),
		ReceiptHandle: receiptHandle,
	})
	if err != nil {
		log.Printf("failed to delete message: %v", err)
	}
}
