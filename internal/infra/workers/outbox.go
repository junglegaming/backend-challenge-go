package workers

import (
	"context"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
)

type OutboxWorker struct {
	pool      *pgxpool.Pool
	sqsClient *sqs.Client
	topicArn  string // Or queueURL
}

func NewOutboxWorker(lc fx.Lifecycle, pool *pgxpool.Pool, sqsClient *sqs.Client) *OutboxWorker {
	worker := &OutboxWorker{
		pool:      pool,
		sqsClient: sqsClient,
		topicArn:  "http://localhost:4566/000000000000/outbox-events.fifo", // Using SQS directly for simplicity
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			go worker.Start()
			return nil
		},
	})

	return worker
}

func (w *OutboxWorker) Start() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		<-ticker.C
		w.processBatch()
	}
}

func (w *OutboxWorker) processBatch() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := w.pool.Begin(ctx)
	if err != nil {
		log.Printf("outbox worker failed to begin tx: %v", err)
		return
	}
	defer tx.Rollback(ctx)

	query := `
		SELECT id, event_type, payload
		FROM outbox_events
		WHERE status = 'PENDING'
		ORDER BY created_at ASC
		FOR UPDATE SKIP LOCKED
		LIMIT 10
	`

	rows, err := tx.Query(ctx, query)
	if err != nil {
		log.Printf("outbox worker failed to query: %v", err)
		return
	}

	type outboxEvent struct {
		ID        string
		EventType string
		Payload   string
	}

	var events []outboxEvent
	for rows.Next() {
		var e outboxEvent
		if err := rows.Scan(&e.ID, &e.EventType, &e.Payload); err != nil {
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

	for _, e := range events {
		_, err := w.sqsClient.SendMessage(ctx, &sqs.SendMessageInput{
			QueueUrl:               aws.String(w.topicArn),
			MessageBody:            aws.String(e.Payload),
			MessageGroupId:         aws.String(e.EventType),
			MessageDeduplicationId: aws.String(e.ID),
		})

		if err != nil {
			log.Printf("failed to publish outbox event %s: %v", e.ID, err)
			continue // Proceed to next event. The failed one will be retried on next loop.
		}

		processedIDs = append(processedIDs, e.ID)
	}

	if len(processedIDs) > 0 {
		updateQuery := `
			UPDATE outbox_events
			SET status = 'PUBLISHED'
			WHERE id = ANY($1)
		`
		if _, err := tx.Exec(ctx, updateQuery, processedIDs); err != nil {
			log.Printf("outbox worker failed to update status: %v", err)
			return
		}
	}

	if err := tx.Commit(ctx); err != nil {
		log.Printf("outbox worker failed to commit: %v", err)
	}
}
