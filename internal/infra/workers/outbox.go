package workers

import (
	"context"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"go.uber.org/fx"

	"github.com/Lzdevmendes/backend-challenge-go/internal/infra/db"
)

type OutboxWorker struct {
	db        *db.PostgresDB
	sqsClient *sqs.Client
	topicArn  string
	cancel    context.CancelFunc
}

func NewOutboxWorker(lc fx.Lifecycle, database *db.PostgresDB, sqsClient *sqs.Client) *OutboxWorker {
	worker := &OutboxWorker{
		db:        database,
		sqsClient: sqsClient,
		topicArn:  "http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/outbox-events.fifo",
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			var wCtx context.Context
			wCtx, worker.cancel = context.WithCancel(context.Background())
			go worker.Start(wCtx)
			return nil
		},
		OnStop: func(ctx context.Context) error {
			if worker.cancel != nil {
				worker.cancel()
			}
			return nil
		},
	})

	return worker
}

func (w *OutboxWorker) Start(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.processBatch(ctx)
		}
	}
}

func (w *OutboxWorker) processBatch(ctx context.Context) {
	batchCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	tx, err := w.db.Pool.Begin(batchCtx)
	if err != nil {
		log.Printf("outbox worker failed to begin tx: %v", err)
		return
	}
	defer tx.Rollback(batchCtx)

	query := `
		SELECT id, event_type, payload, attempts
		FROM outbox_events
		WHERE status = 'PENDING' AND next_attempt_at <= $1
		ORDER BY created_at ASC
		FOR UPDATE SKIP LOCKED
		LIMIT 20
	`

	rows, err := tx.Query(batchCtx, query, time.Now().UTC())
	if err != nil {
		log.Printf("outbox worker failed to query: %v", err)
		return
	}

	type outboxEvent struct {
		ID        string
		EventType string
		Payload   string
		Attempts  int
	}

	var events []outboxEvent
	for rows.Next() {
		var e outboxEvent
		if err := rows.Scan(&e.ID, &e.EventType, &e.Payload, &e.Attempts); err != nil {
			log.Printf("outbox worker scan error: %v", err)
			continue
		}
		events = append(events, e)
	}
	rows.Close()

	if len(events) == 0 {
		return
	}

	var processedIDs []string
	var failedEvents []outboxEvent

	for _, e := range events {
		_, err := w.sqsClient.SendMessage(batchCtx, &sqs.SendMessageInput{
			QueueUrl:               aws.String(w.topicArn),
			MessageBody:            aws.String(e.Payload),
			MessageGroupId:         aws.String("outbox-group"),
			MessageDeduplicationId: aws.String(e.ID),
		})

		if err != nil {
			log.Printf("failed to publish outbox event %s: %v", e.ID, err)
			failedEvents = append(failedEvents, e)
			continue
		}

		processedIDs = append(processedIDs, e.ID)
	}

	now := time.Now().UTC()

	if len(processedIDs) > 0 {
		updateQuery := `
			UPDATE outbox_events
			SET status = 'PUBLISHED', published_at = $1
			WHERE id = ANY($2)
		`
		if _, err := tx.Exec(batchCtx, updateQuery, now, processedIDs); err != nil {
			log.Printf("outbox worker failed to update published status: %v", err)
			return
		}
	}

	for _, e := range failedEvents {
		newAttempts := e.Attempts + 1
		nextAttempt := now.Add(time.Duration(newAttempts*2) * time.Second)
		failQuery := `
			UPDATE outbox_events
			SET attempts = $1, next_attempt_at = $2
			WHERE id = $3
		`
		if _, err := tx.Exec(batchCtx, failQuery, newAttempts, nextAttempt, e.ID); err != nil {
			log.Printf("outbox worker failed to update failed status: %v", err)
		}
	}

	if err := tx.Commit(batchCtx); err != nil {
		log.Printf("outbox worker failed to commit: %v", err)
	}
}
