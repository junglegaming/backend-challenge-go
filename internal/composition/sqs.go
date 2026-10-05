package composition

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"go.uber.org/fx"

	"github.com/VictorM0nteiro/backend-challenge-go/internal/adapters/httpapi"
	"github.com/VictorM0nteiro/backend-challenge-go/internal/adapters/postgres"
	adapter "github.com/VictorM0nteiro/backend-challenge-go/internal/adapters/sqs"
	"github.com/VictorM0nteiro/backend-challenge-go/internal/config"
)

// ConsumerModule runs the SQS consumer as a lifecycle component. It stops taking
// messages before the pool closes, like the HTTP server does. It also provides
// the queue check that readiness uses.
var ConsumerModule = fx.Module("sqs",
	fx.Provide(
		newSQSClient,
		newConsumer,
		fx.Annotate(newQueuePinger, fx.As(new(httpapi.QueueChecker))),
	),
	fx.Invoke(startConsumer),
)

func newQueuePinger(client *awssqs.Client, cfg config.Config) *adapter.QueuePinger {
	return adapter.NewQueuePinger(client, cfg.SQSQueueURL)
}

func newSQSClient(cfg config.Config) (*awssqs.Client, error) {
	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(), awsconfig.WithRegion(cfg.AWSRegion))
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	return awssqs.NewFromConfig(awsCfg, func(o *awssqs.Options) {
		if cfg.SQSEndpoint != "" {
			o.BaseEndpoint = aws.String(cfg.SQSEndpoint)
		}
	}), nil
}

func newConsumer(client *awssqs.Client, cfg config.Config, processor *postgres.WagerProcessor) *adapter.Consumer {
	return adapter.NewConsumer(client, cfg.SQSQueueURL, processor)
}

// startConsumer starts Run on start and, on stop, cancels it and waits for the
// loop to return. A message being applied at that moment is released, not lost.
func startConsumer(lc fx.Lifecycle, c *adapter.Consumer) {
	ctx, cancel := context.WithCancel(context.Background())
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			go c.Run(ctx)
			return nil
		},
		OnStop: func(stopCtx context.Context) error {
			cancel()
			return c.Wait(stopCtx)
		},
	})
}
