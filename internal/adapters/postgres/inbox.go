package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// ErrMessageReused means a messageId arrived again with a different body. The
// same body arriving twice is normal redelivery; a different body under the
// same id is a producer bug, and it can never succeed.
var ErrMessageReused = errors.New("postgres: message id reused with a different body")

// InboxMessage identifies one received message.
type InboxMessage struct {
	Consumer  string
	MessageID string
	Hash      string
}

// claimInbox records the message inside the caller's transaction. The first
// delivery inserts the row. A redelivery finds it, and must carry the same hash.
// The primary key decides concurrent deliveries, as it does for idempotency keys.
func claimInbox(ctx context.Context, tx pgx.Tx, m InboxMessage) error {
	const insert = `INSERT INTO inbox (consumer_name, message_id, request_hash)
              VALUES ($1, $2, $3)
              ON CONFLICT DO NOTHING`

	tag, err := tx.Exec(ctx, insert, m.Consumer, m.MessageID, m.Hash)
	if err != nil {
		return fmt.Errorf("postgres: claim inbox: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}

	const query = `SELECT request_hash FROM inbox WHERE consumer_name = $1 AND message_id = $2`
	var hash string
	if err := tx.QueryRow(ctx, query, m.Consumer, m.MessageID).Scan(&hash); err != nil {
		return fmt.Errorf("postgres: load inbox row: %w", err)
	}
	if hash != m.Hash {
		return ErrMessageReused
	}
	return nil
}

// completeInbox marks the message as applied. It runs in the same transaction
// as the operation, so "applied" and "completed" cannot disagree.
func completeInbox(ctx context.Context, tx pgx.Tx, m InboxMessage) error {
	const query = `UPDATE inbox SET completed_at = now()
              WHERE consumer_name = $1 AND message_id = $2`

	if _, err := tx.Exec(ctx, query, m.Consumer, m.MessageID); err != nil {
		return fmt.Errorf("postgres: complete inbox: %w", err)
	}
	return nil
}

// Explicação

// - O inbox é uma linha por (consumidor, messageId). Ela entra na mesma transação da operação. Se o processo morre no meio, a transação some inteira,
// inclusive a linha do inbox. A mensagem volta a ser entregue, e o processamento recomeça do zero, sem dupla aplicação.
// - Entrega repetida com o mesmo corpo é normal no SQS: o hash bate, e o fluxo segue para o replay da chave de idempotência.
// - Mesmo messageId com corpo diferente é ErrMessageReused, que é erro permanente. O hash é o mesmo app.Fingerprint do HTTP, então "o mesmo corpo" tem um significado único.
