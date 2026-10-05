package sqs

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// QueuePinger checks that the queue answers. Readiness uses it: a process that
// cannot reach its queue cannot consume, so it should not be considered ready.
type QueuePinger struct {
	client   *awssqs.Client
	queueURL string
}

// NewQueuePinger builds a pinger for queueURL.
func NewQueuePinger(client *awssqs.Client, queueURL string) *QueuePinger {
	return &QueuePinger{client: client, queueURL: queueURL}
}

// Ping asks for the queue's ARN: a cheap call that fails if the queue is
// missing or the endpoint is unreachable.
func (p *QueuePinger) Ping(ctx context.Context) error {
	_, err := p.client.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(p.queueURL),
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	return err
}

// Explicação

// - Um tipo separado, e não um método no Consumer. O consumidor é um loop de longa duração,
// e a readiness é uma pergunta de saúde. Misturar as duas coisas faria o /health/ready depender do estado interno do loop.
