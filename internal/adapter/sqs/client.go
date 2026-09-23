// Package sqs connects the service to AWS SQS (LocalStack locally): queue
// provisioning, the wager-transactions consumer and the outbox publisher.
package sqs

import (
	"context"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// API is the subset of the SQS client used by the service.
type API interface {
	CreateQueue(ctx context.Context, in *sqs.CreateQueueInput, opts ...func(*sqs.Options)) (*sqs.CreateQueueOutput, error)
	GetQueueUrl(ctx context.Context, in *sqs.GetQueueUrlInput, opts ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error)
	GetQueueAttributes(ctx context.Context, in *sqs.GetQueueAttributesInput, opts ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error)
	ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput, opts ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, in *sqs.DeleteMessageInput, opts ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
	ChangeMessageVisibility(ctx context.Context, in *sqs.ChangeMessageVisibilityInput, opts ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error)
	SendMessage(ctx context.Context, in *sqs.SendMessageInput, opts ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
	SetQueueAttributes(ctx context.Context, in *sqs.SetQueueAttributesInput, opts ...func(*sqs.Options)) (*sqs.SetQueueAttributesOutput, error)
}

// The SDK client satisfies API.
var _ API = (*sqs.Client)(nil)

// ClientConfig configures the SQS client. Credentials come from the default
// AWS chain (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY locally).
type ClientConfig struct {
	// Region is the AWS region of the queues.
	Region string
	// Endpoint overrides the SQS endpoint (LocalStack); empty means AWS.
	Endpoint string
}

// NewClient builds the SQS client.
func NewClient(ctx context.Context, cfg ClientConfig) (*sqs.Client, error) {
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.Region))
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	return sqs.NewFromConfig(awsCfg, func(o *sqs.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
	}), nil
}

// QueueNames are the logical queue names (all FIFO, ending in .fifo).
type QueueNames struct {
	// Input receives wager-transaction requests.
	Input string
	// DLQ receives the invalid and exhausted input messages.
	DLQ string
	// Events receives the outbox (wallet events).
	Events string
}

// Queues are the resolved queue URLs, in the same order as QueueNames.
type Queues struct {
	// Input is the URL of the wager-transactions queue.
	Input string
	// DLQ is the URL of its dead-letter queue.
	DLQ string
	// Events is the URL of the outbox events queue.
	Events string
}

// ResolveQueues looks up the URLs of existing queues.
func ResolveQueues(ctx context.Context, api API, names QueueNames) (Queues, error) {
	var q Queues
	for _, pair := range []struct {
		name string
		dst  *string
	}{{names.Input, &q.Input}, {names.DLQ, &q.DLQ}, {names.Events, &q.Events}} {
		out, err := api.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(pair.name)})
		if err != nil {
			return Queues{}, fmt.Errorf("resolve queue %s: %w", pair.name, err)
		}
		*pair.dst = aws.ToString(out.QueueUrl)
	}
	return q, nil
}

// ProvisionConfig configures queue creation.
type ProvisionConfig struct {
	// Names are the queues to create.
	Names QueueNames
	// MaxReceiveCount is the redrive threshold of the input queue; it must
	// sit above the consumer's limit (RedriveMaxReceiveCount).
	MaxReceiveCount int
	// VisibilityTimeout, in seconds, is the input queue attribute; the
	// consumer's ConsumerConfig.VisibilityTimeout must agree with it.
	VisibilityTimeout int
}

// Provision creates (idempotently) the FIFO input queue, its FIFO DLQ with
// the redrive policy, and the FIFO events queue that receives the outbox.
// Queues are created with only the immutable FifoQueue attribute and the
// mutable ones are then applied with SetQueueAttributes, so rerunning it
// after changing SQS_MAX_RECEIVE_COUNT or SQS_VISIBILITY_TIMEOUT reconciles
// existing queues instead of failing with QueueNameExists.
func Provision(ctx context.Context, api API, cfg ProvisionConfig) (Queues, error) {
	dlq, err := createFIFO(ctx, api, cfg.Names.DLQ, nil)
	if err != nil {
		return Queues{}, err
	}
	dlqArn, err := queueArn(ctx, api, dlq)
	if err != nil {
		return Queues{}, err
	}
	redrive := fmt.Sprintf(`{"deadLetterTargetArn":%q,"maxReceiveCount":"%d"}`, dlqArn, cfg.MaxReceiveCount)
	input, err := createFIFO(ctx, api, cfg.Names.Input, map[string]string{
		string(types.QueueAttributeNameRedrivePolicy):     redrive,
		string(types.QueueAttributeNameVisibilityTimeout): strconv.Itoa(cfg.VisibilityTimeout),
	})
	if err != nil {
		return Queues{}, err
	}
	events, err := createFIFO(ctx, api, cfg.Names.Events, nil)
	return Queues{Input: input, DLQ: dlq, Events: events}, err
}

func createFIFO(ctx context.Context, api API, name string, attrs map[string]string) (string, error) {
	out, err := api.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(name),
		Attributes: map[string]string{string(types.QueueAttributeNameFifoQueue): "true"}})
	if err != nil {
		return "", fmt.Errorf("create queue %s: %w", name, err)
	}
	url := aws.ToString(out.QueueUrl)
	if len(attrs) == 0 {
		return url, nil
	}
	if _, err := api.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{QueueUrl: aws.String(url), Attributes: attrs}); err != nil {
		return "", fmt.Errorf("configure queue %s: %w", name, err)
	}
	return url, nil
}

func queueArn(ctx context.Context, api API, url string) (string, error) {
	out, err := api.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(url), AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	if err != nil {
		return "", fmt.Errorf("queue arn: %w", err)
	}
	return out.Attributes[string(types.QueueAttributeNameQueueArn)], nil
}

// QueueCheck returns a readiness probe for a queue.
func QueueCheck(api API, url string) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		_, err := api.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
			QueueUrl: aws.String(url), AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
		})
		return err
	}
}
