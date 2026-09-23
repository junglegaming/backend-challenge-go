package app

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/Pantani/backend-challenge-go/internal/domain/event"
	"github.com/Pantani/backend-challenge-go/internal/domain/money"
	"github.com/Pantani/backend-challenge-go/internal/domain/wager"
	"github.com/Pantani/backend-challenge-go/internal/domain/wallet"
)

// WalletRepository persists wallets inside a unit of work.
type WalletRepository interface {
	// Create inserts a wallet; ErrWalletExists on (player, currency) conflict.
	Create(ctx context.Context, w *wallet.Wallet) error
	// GetForUpdate loads and row-locks a wallet; ErrWalletNotFound if absent.
	GetForUpdate(ctx context.Context, id uuid.UUID) (*wallet.Wallet, error)
	// Save updates balance and version only if the stored version still
	// equals expectedVersion; ErrConflict otherwise.
	Save(ctx context.Context, w *wallet.Wallet, expectedVersion int64) error
}

// TransactionRepository persists wager transactions inside a unit of work.
type TransactionRepository interface {
	// Create inserts a transaction; ErrConflict on unique violations.
	Create(ctx context.Context, t *wager.Transaction) error
	// Save persists a transition of a non-terminal stored transaction.
	Save(ctx context.Context, t *wager.Transaction) error
	// FindExisting returns transactions of the provider matching the
	// idempotency key or the external id.
	FindExisting(ctx context.Context, providerID, idempotencyKey, externalID string) ([]*wager.Transaction, error)
	// GetByExternal returns ErrTransactionNotFound when absent.
	GetByExternal(ctx context.Context, providerID, externalID string) (*wager.Transaction, error)
	// HasProcessedReversal reports whether a REFUND or ROLLBACK of the
	// referenced transaction was already processed.
	HasProcessedReversal(ctx context.Context, referenceTxID uuid.UUID) (bool, error)
	// LockDuePending row-locks a PENDING_REFERENCE transaction that is due,
	// skipping rows locked by other instances; ErrNotDue otherwise.
	LockDuePending(ctx context.Context, id uuid.UUID, now time.Time) (*wager.Transaction, error)
	// WakeDependents makes pending operations referencing the given external
	// transaction due immediately.
	WakeDependents(ctx context.Context, providerID, externalID string, now time.Time) error
}

// LedgerRepository appends immutable ledger entries.
type LedgerRepository interface {
	// Append inserts one entry; a second entry for the same (wallet,
	// transaction) is ErrConflict.
	Append(ctx context.Context, e wallet.LedgerEntry) error
}

// OutboxRepository stores integration events in the same SQL transaction.
type OutboxRepository interface {
	// Append stores the records in order; the relay publishes them after
	// the commit.
	Append(ctx context.Context, records ...event.Record) error
}

// InboxEntry is the durable record of a consumed message.
type InboxEntry struct {
	// PayloadHash fingerprints the content the message was first seen with.
	PayloadHash string
	// Completed reports that the message was durably handled. Because the
	// row only becomes visible when committed together with Complete, every
	// entry returned by Register is completed; the field is kept for audit
	// and for stores with other visibility rules.
	Completed bool
}

// InboxRepository deduplicates consumed messages.
type InboxRepository interface {
	// Register inserts (consumer, messageID) with created=true, or returns
	// the existing entry with created=false. An entry is only visible once
	// its unit of work committed, together with its Complete, so a crash in
	// between leaves nothing behind and the redelivery is processed again.
	Register(ctx context.Context, consumer, messageID, payloadHash string, now time.Time) (entry InboxEntry, created bool, err error)
	// Complete marks the message as durably handled by transactionID.
	Complete(ctx context.Context, consumer, messageID string, transactionID uuid.UUID, now time.Time) error
}

// Repositories groups the repositories bound to one SQL transaction.
type Repositories interface {
	// Wallets persists wallets.
	Wallets() WalletRepository
	// Transactions persists wager transactions.
	Transactions() TransactionRepository
	// Ledger appends ledger entries.
	Ledger() LedgerRepository
	// Outbox stores integration events.
	Outbox() OutboxRepository
	// Inbox deduplicates consumed messages.
	Inbox() InboxRepository
}

// UnitOfWork runs fn inside a single SQL transaction.
type UnitOfWork interface {
	// Do commits when fn returns nil and rolls back otherwise (the error is
	// returned as is). A failed commit is reported as an error and leaves
	// nothing behind. Row locks taken by fn are held until then.
	Do(ctx context.Context, fn func(ctx context.Context, r Repositories) error) error
}

// LedgerRow is a ledger entry with its stable ordering key.
type LedgerRow struct {
	// Seq is the append order, used as the pagination cursor.
	Seq int64
	// Entry is the ledger entry.
	Entry wallet.LedgerEntry
}

// ReconciliationSnapshot is read in one consistent view of the data.
type ReconciliationSnapshot struct {
	// Stored is the balance kept on the wallet row.
	Stored money.Money
	// NetMinor is the exact sum of credits minus debits in minor units.
	// Historical turnover may exceed int64; only the net must fit.
	NetMinor int64
	// Entries is how many ledger entries were summed (opening included).
	Entries int64
}

// DueTransaction identifies a pending operation ready for a new attempt.
type DueTransaction struct {
	// ID is the transaction to retry.
	ID uuid.UUID
	// WalletID is the wallet to lock before retrying it.
	WalletID uuid.UUID
}

// Queries is the read side, executed outside units of work.
type Queries interface {
	// GetWallet returns ErrWalletNotFound when absent.
	GetWallet(ctx context.Context, id uuid.UUID) (*wallet.Wallet, error)
	// ListLedger returns up to limit entries of the wallet with Seq greater
	// than afterSeq, in Seq order.
	ListLedger(ctx context.Context, walletID uuid.UUID, afterSeq int64, limit int) ([]LedgerRow, error)
	// GetTransaction returns ErrTransactionNotFound when absent.
	GetTransaction(ctx context.Context, id uuid.UUID) (*wager.Transaction, error)
	// GetTransactionByExternal returns ErrTransactionNotFound when absent.
	GetTransactionByExternal(ctx context.Context, providerID, externalID string) (*wager.Transaction, error)
	// Reconcile reads the stored balance and the ledger sums of a wallet in
	// one consistent snapshot; ErrWalletNotFound when absent.
	Reconcile(ctx context.Context, walletID uuid.UUID) (ReconciliationSnapshot, error)
	// ListDuePending returns up to limit PENDING_REFERENCE transactions
	// whose next attempt is not after now, earliest next attempt first. It runs
	// outside any unit of work: each row is claimed again with
	// TransactionRepository.LockDuePending when it is processed.
	ListDuePending(ctx context.Context, now time.Time, limit int) ([]DueTransaction, error)
}

// Clock returns the current instant.
type Clock interface {
	// Now returns the current instant in UTC.
	Now() time.Time
}

// SystemClock is the wall clock in UTC.
type SystemClock struct{}

// Now implements Clock.
func (SystemClock) Now() time.Time { return time.Now().UTC() }

// IDGenerator creates identifiers.
type IDGenerator interface {
	// New returns a fresh, never repeated identifier.
	New() uuid.UUID
}

// UUIDv7 generates time-ordered UUIDs.
type UUIDv7 struct{}

// New implements IDGenerator.
func (UUIDv7) New() uuid.UUID { return uuid.Must(uuid.NewV7()) }

// Metrics records business metrics.
type Metrics interface {
	// TransactionResult counts a committed submission by source (SourceHTTP,
	// SourceSQS, SourceWorker) and resulting status; replay also counts an
	// idempotent duplicate.
	TransactionResult(source string, status wager.Status, replay bool)
	// ConcurrencyConflict counts a lost race that is being retried, by
	// operation ("submit", "consume", "pending").
	ConcurrencyConflict(operation string)
	// ProcessingDuration records the latency of a committed submission by
	// source.
	ProcessingDuration(source string, d time.Duration)
	// ReconciliationDivergence counts a wallet whose balance differs from
	// its ledger.
	ReconciliationDivergence()
	// PendingResolution counts one attempt of a due operation by the status
	// it ended in (PROCESSED, REJECTED, PENDING_REFERENCE or FAILED).
	PendingResolution(outcome wager.Status)
}

// Deps are the collaborators shared by every service.
type Deps struct {
	// UoW runs the write side.
	UoW UnitOfWork
	// Queries is the read side.
	Queries Queries
	// Clock provides every timestamp written by the service.
	Clock Clock
	// IDs provides every identifier written by the service.
	IDs IDGenerator
	// Metrics records business metrics.
	Metrics Metrics
	// Logger receives operational logs.
	Logger *slog.Logger
}

// OutboxMessage is a claimed, not yet published outbox record.
type OutboxMessage struct {
	// Seq is the append order of the record.
	Seq int64
	// EventID is the event identifier (the deduplication key downstream).
	EventID uuid.UUID
	// EventType is the event name (see package event).
	EventType string
	// AggregateType is the routing type of the aggregate.
	AggregateType string
	// AggregateID identifies the aggregate the event is about.
	AggregateID uuid.UUID
	// PartitionKey groups events that must keep their relative order.
	PartitionKey string
	// Payload is the serialized envelope.
	Payload []byte
	// OccurredAt is when the event happened.
	OccurredAt time.Time
	// Attempts counts the publications tried so far.
	Attempts int
	// ClaimID identifies this acquisition and fences stale relay mutations.
	ClaimID uuid.UUID
}

// OutboxStore is used by the relay, outside business transactions.
type OutboxStore interface {
	// Claim leases one due partition head to claimID until now+lease. Records
	// whose lease expired are claimable again with a different token.
	Claim(ctx context.Context, owner string, claimID uuid.UUID, now time.Time, lease time.Duration) (OutboxMessage, bool, error)
	// StartAttempt counts a publication and renews its lease to now+lease only
	// while claimID still owns eventID and its current lease is live at now.
	StartAttempt(
		ctx context.Context, eventID, claimID uuid.UUID, now time.Time, lease time.Duration,
	) (int, bool, error)
	// MarkPublished confirms a publication; false when the claim was lost.
	MarkPublished(ctx context.Context, eventID, claimID uuid.UUID, now time.Time) (bool, error)
	// MarkFailed releases the claim and schedules the next attempt.
	MarkFailed(ctx context.Context, eventID, claimID uuid.UUID, next time.Time, cause string) (bool, error)
	// MarkDead dead-letters a record that exhausted its attempts, so it stops
	// blocking the later records of its partition (kept for audit/replay).
	MarkDead(ctx context.Context, eventID, claimID uuid.UUID, now time.Time, cause string) (bool, error)
	// OldestPending returns the occurrence of the oldest unpublished record.
	OldestPending(ctx context.Context) (time.Time, bool, error)
}
