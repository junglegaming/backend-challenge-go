package sqs_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"

	sqsadapter "github.com/Pantani/backend-challenge-go/internal/adapter/sqs"
	"github.com/Pantani/backend-challenge-go/internal/app"
	"github.com/Pantani/backend-challenge-go/internal/worker"
)

func TestPublisherClassifiesOnlyInvalidContentsAsPermanent(t *testing.T) {
	for _, tc := range []struct {
		name      string
		err       error
		permanent bool
	}{
		{"invalid contents", fmt.Errorf("send: %w", &types.InvalidMessageContents{}), true},
		{"timeout", context.DeadlineExceeded, false},
		{"unknown", errBoom, false},
		{"missing queue", &types.QueueDoesNotExist{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			api := newFakeAPI()
			api.errs["send"] = tc.err
			err := sqsadapter.NewPublisher(api, "events").Publish(context.Background(), app.OutboxMessage{})
			assert.ErrorIs(t, err, tc.err)
			assert.Equal(t, tc.permanent, errors.Is(err, worker.ErrPermanentPublish))
		})
	}
}
