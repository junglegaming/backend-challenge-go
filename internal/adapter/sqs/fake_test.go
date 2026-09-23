package sqs_test

import (
	"context"
	"errors"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

var errInvalidSend = errors.New("fake sqs: invalid SendMessage input")

// fakeAPI records calls and serves canned responses. Like the real client,
// every method fails with ctx.Err() once ctx is done.
type fakeAPI struct {
	mu         sync.Mutex
	receive    func(ctx context.Context) (*sqs.ReceiveMessageOutput, error)
	received   []*sqs.ReceiveMessageInput
	errs       map[string]error
	deleted    []string
	visibility map[string]int32
	sent       []*sqs.SendMessageInput
	// budgets is the time left on the context of each DeleteMessage call.
	budgets     map[string]time.Duration
	created     []*sqs.CreateQueueInput
	configured  []*sqs.SetQueueAttributesInput
	attrQueries int
	deleteCalls []string
	calls       []string
}

func newFakeAPI() *fakeAPI {
	return &fakeAPI{errs: map[string]error{}, visibility: map[string]int32{}, budgets: map[string]time.Duration{}}
}

// setReceive installs the receive stub under the mutex.
func (f *fakeAPI) setReceive(fn func(ctx context.Context) (*sqs.ReceiveMessageOutput, error)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.receive = fn
}

// err returns the canned error of op, or ctx.Err() when ctx is done.
func (f *fakeAPI) err(ctx context.Context, op string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.errs[op]
}

func (f *fakeAPI) CreateQueue(ctx context.Context, in *sqs.CreateQueueInput, _ ...func(*sqs.Options)) (*sqs.CreateQueueOutput, error) {
	if err := f.err(ctx, "create:"+aws.ToString(in.QueueName)); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.created = append(f.created, in)
	return &sqs.CreateQueueOutput{QueueUrl: aws.String("http://sqs/" + aws.ToString(in.QueueName))}, nil
}

func (f *fakeAPI) GetQueueUrl( //nolint:revive // name imposed by the AWS SDK interface
	ctx context.Context, in *sqs.GetQueueUrlInput, _ ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
	if err := f.err(ctx, "url:"+aws.ToString(in.QueueName)); err != nil {
		return nil, err
	}
	return &sqs.GetQueueUrlOutput{QueueUrl: aws.String("http://sqs/" + aws.ToString(in.QueueName))}, nil
}

func (f *fakeAPI) GetQueueAttributes(ctx context.Context, in *sqs.GetQueueAttributesInput, _ ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error) {
	f.mu.Lock()
	f.attrQueries++
	f.mu.Unlock()
	if err := f.err(ctx, "attrs"); err != nil {
		return nil, err
	}
	return &sqs.GetQueueAttributesOutput{Attributes: map[string]string{
		string(types.QueueAttributeNameQueueArn): "arn:aws:sqs:us-east-1:000000000000:" + aws.ToString(in.QueueUrl),
	}}, nil
}

func (f *fakeAPI) ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	f.received = append(f.received, in)
	receive := f.receive
	f.mu.Unlock()
	return receive(ctx)
}

func (f *fakeAPI) DeleteMessage(ctx context.Context, in *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	receipt := aws.ToString(in.ReceiptHandle)
	f.recordCall("delete:" + receipt)
	f.mu.Lock()
	f.deleteCalls = append(f.deleteCalls, receipt)
	f.mu.Unlock()
	if err := f.err(ctx, "delete"); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleted = append(f.deleted, receipt)
	if deadline, ok := ctx.Deadline(); ok {
		f.budgets[receipt] = time.Until(deadline)
	}
	return &sqs.DeleteMessageOutput{}, nil
}

func (f *fakeAPI) ChangeMessageVisibility(ctx context.Context, in *sqs.ChangeMessageVisibilityInput, _ ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	f.recordCall("visibility:" + aws.ToString(in.ReceiptHandle))
	if err := f.err(ctx, "visibility"); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.visibility[aws.ToString(in.ReceiptHandle)] = in.VisibilityTimeout
	return &sqs.ChangeMessageVisibilityOutput{}, nil
}

func (f *fakeAPI) SetQueueAttributes(ctx context.Context, in *sqs.SetQueueAttributesInput, _ ...func(*sqs.Options)) (*sqs.SetQueueAttributesOutput, error) {
	if err := f.err(ctx, "set:"+aws.ToString(in.QueueUrl)); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.configured = append(f.configured, in)
	return &sqs.SetQueueAttributesOutput{}, nil
}

// SendMessage enforces what a FIFO queue enforces: a group id, a
// deduplication id and UTF-8 string attributes.
func (f *fakeAPI) SendMessage(ctx context.Context, in *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	f.recordCall("send:" + aws.ToString(in.MessageDeduplicationId))
	if err := f.err(ctx, "send"); err != nil {
		return nil, err
	}
	if err := validateSend(in); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, in)
	return &sqs.SendMessageOutput{}, nil
}

func (f *fakeAPI) recordCall(call string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, call)
}

func validateSend(in *sqs.SendMessageInput) error {
	if aws.ToString(in.MessageGroupId) == "" || aws.ToString(in.MessageDeduplicationId) == "" {
		return errInvalidSend
	}
	for _, a := range in.MessageAttributes {
		if !utf8.ValidString(aws.ToString(a.StringValue)) {
			return errInvalidSend
		}
	}
	return nil
}
