package app_test

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math/big"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/Pantani/backend-challenge-go/internal/app"
	"github.com/Pantani/backend-challenge-go/internal/domain/event"
	"github.com/Pantani/backend-challenge-go/internal/domain/money"
	"github.com/Pantani/backend-challenge-go/internal/domain/wager"
	"github.com/Pantani/backend-challenge-go/internal/domain/wallet"
)

// Errors emulating the database guards (triggers and CHECK constraints).
var (
	errTerminal = errors.New("memstore: terminal transaction cannot change")
	errCheck    = errors.New("memstore: check constraint violated")
)

type inboxRow struct {
	hash      string
	completed bool
}

// state is everything a unit of work may roll back.
type state struct {
	wallets map[uuid.UUID]wallet.Snapshot
	txs     map[uuid.UUID]wager.Snapshot
	order   []uuid.UUID
	ledger  []app.LedgerRow
	outbox  []event.Record
	inbox   map[string]inboxRow
}

func (s state) clone() state {
	return state{
		wallets: maps.Clone(s.wallets), txs: maps.Clone(s.txs), order: slices.Clone(s.order),
		ledger: slices.Clone(s.ledger), outbox: slices.Clone(s.outbox), inbox: maps.Clone(s.inbox),
	}
}

// memStore is an in-memory implementation of the persistence ports. One
// mutex serializes units of work (the strongest isolation), unique and
// CHECK constraints mirror the schema and failures can be injected per
// operation. Row lock semantics (FOR UPDATE, SKIP LOCKED, lock timeouts)
// are not modelled here: they are only covered by the integration tests
// against PostgreSQL.
type memStore struct {
	mu       sync.Mutex
	st       state
	failures map[string][]error
	// lockMutate alters the transaction returned by LockDuePending, to
	// exercise defensive transition errors.
	lockMutate func(s *wager.Snapshot)
}

func newMemStore() *memStore {
	return &memStore{st: state{
		wallets: map[uuid.UUID]wallet.Snapshot{}, txs: map[uuid.UUID]wager.Snapshot{}, inbox: map[string]inboxRow{},
	}, failures: map[string][]error{}}
}

// failOn queues errors returned by the next calls of op.
func (m *memStore) failOn(op string, errs ...error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failures[op] = append(m.failures[op], errs...)
}

func (m *memStore) fail(op string) error {
	errs := m.failures[op]
	if len(errs) == 0 {
		return nil
	}
	m.failures[op] = errs[1:]
	return errs[0]
}

// Do implements app.UnitOfWork. Like PostgreSQL, a failed commit leaves
// nothing behind: the state is restored from the backup taken before fn.
func (m *memStore) Do(ctx context.Context, fn func(ctx context.Context, r app.Repositories) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.fail("uow.do"); err != nil {
		return err
	}
	backup := m.st.clone()
	err := fn(ctx, memRepos{m})
	if err == nil {
		err = m.fail("uow.commit")
	}
	if err != nil {
		m.st = backup
	}
	return err
}

type memRepos struct{ m *memStore }

func (r memRepos) Wallets() app.WalletRepository           { return memWallets(r) }
func (r memRepos) Transactions() app.TransactionRepository { return memTxs(r) }
func (r memRepos) Ledger() app.LedgerRepository            { return memLedger(r) }
func (r memRepos) Outbox() app.OutboxRepository            { return memOutbox(r) }
func (r memRepos) Inbox() app.InboxRepository              { return memInbox(r) }

func walletSnapshot(w *wallet.Wallet) wallet.Snapshot {
	return wallet.Snapshot{ID: w.ID(), PlayerID: w.PlayerID(), Balance: w.Balance(), Version: w.Version(),
		CreatedAt: w.CreatedAt(), UpdatedAt: w.UpdatedAt()}
}

func txSnapshot(t *wager.Transaction) wager.Snapshot {
	return wager.Snapshot{ID: t.ID(), Origin: t.Origin(), Kind: t.Kind(), Status: t.Status(), WalletID: t.WalletID(),
		PlayerID: t.PlayerID(), Amount: t.Amount(), External: t.External(), ReferenceTxID: t.ReferenceTxID(),
		FailureCode: t.FailureCode(), ResultBalance: t.ResultBalance(), Attempts: t.Attempts(),
		NextAttemptAt: t.NextAttemptAt(), CorrelationID: t.CorrelationID(), CreatedAt: t.CreatedAt(), UpdatedAt: t.UpdatedAt()}
}

func mustTx(s wager.Snapshot) *wager.Transaction {
	t, err := wager.Rehydrate(s)
	if err != nil {
		panic(err)
	}
	return t
}

type memWallets memRepos

func (r memWallets) Create(_ context.Context, w *wallet.Wallet) error {
	if err := r.m.fail("wallets.create"); err != nil {
		return err
	}
	for _, s := range r.m.st.wallets {
		if s.PlayerID == w.PlayerID() && s.Balance.Currency() == w.Currency() {
			return app.ErrWalletExists
		}
	}
	r.m.st.wallets[w.ID()] = walletSnapshot(w)
	return nil
}

func (r memWallets) GetForUpdate(_ context.Context, id uuid.UUID) (*wallet.Wallet, error) {
	if err := r.m.fail("wallets.get"); err != nil {
		return nil, err
	}
	return r.m.getWallet(id)
}

func (m *memStore) getWallet(id uuid.UUID) (*wallet.Wallet, error) {
	s, ok := m.st.wallets[id]
	if !ok {
		return nil, app.ErrWalletNotFound
	}
	return wallet.Rehydrate(s)
}

// Save mirrors the optimistic UPDATE: the stored version must still be the
// expected one, the balance never goes negative and the version grows by
// exactly one when (and only when) the balance changes.
func (r memWallets) Save(_ context.Context, w *wallet.Wallet, expected int64) error {
	if err := r.m.fail("wallets.save"); err != nil {
		return err
	}
	stored := r.m.st.wallets[w.ID()]
	if stored.Version != expected {
		return app.ErrConflict
	}
	if w.Balance().IsNegative() {
		return errCheck
	}
	if bump := !stored.Balance.Equal(w.Balance()); w.Version() != stored.Version+versionDelta(bump) {
		return errCheck
	}
	r.m.st.wallets[w.ID()] = walletSnapshot(w)
	return nil
}

func versionDelta(bump bool) int64 {
	if bump {
		return 1
	}
	return 0
}

type memTxs memRepos

func (r memTxs) Create(_ context.Context, t *wager.Transaction) error {
	if err := r.m.fail("txs.create"); err != nil {
		return err
	}
	for _, s := range r.m.st.txs {
		if violatesUnique(s, t) {
			return app.ErrConflict
		}
	}
	row := txSnapshot(t)
	if err := checkRow(row); err != nil {
		return err
	}
	r.m.st.txs[t.ID()] = row
	r.m.st.order = append(r.m.st.order, t.ID())
	return nil
}

// rowChecks mirrors the CHECK constraints of wager_transactions.
var rowChecks = map[string]func(s wager.Snapshot) bool{
	"failure_code required in REJECTED/FAILED": func(s wager.Snapshot) bool {
		return s.Status.Terminal() && s.Status != wager.StatusProcessed && s.FailureCode == ""
	},
	"result balance required in PROCESSED": func(s wager.Snapshot) bool {
		return s.Status == wager.StatusProcessed && s.ResultBalance.Validate() != nil
	},
	"next_attempt_at required in PENDING_REFERENCE": func(s wager.Snapshot) bool {
		return s.Status == wager.StatusPendingReference && s.NextAttemptAt.IsZero()
	},
	"reference required for reversals": func(s wager.Snapshot) bool {
		return s.Kind.IsReversal() && s.External.ReferenceExternalID == ""
	},
	"PENDING is never stored": func(s wager.Snapshot) bool {
		return s.Status == wager.StatusPending
	},
}

// checkRow rejects rows the database CHECK constraints would reject.
func checkRow(s wager.Snapshot) error {
	for name, violated := range rowChecks {
		if violated(s) {
			return fmt.Errorf("%w: %s", errCheck, name)
		}
	}
	return nil
}

func violatesUnique(s wager.Snapshot, t *wager.Transaction) bool {
	return sameExternalKey(s, t) || duplicateReversal(s, t)
}

// sameExternalKey mirrors the (provider, externalId) and (provider, key) indexes.
func sameExternalKey(s wager.Snapshot, t *wager.Transaction) bool {
	a, b := s.External, t.External()
	if s.Origin != wager.OriginExternal || a.ProviderID != b.ProviderID {
		return false
	}
	return a.ExternalID == b.ExternalID || a.IdempotencyKey == b.IdempotencyKey
}

// duplicateReversal mirrors the single successful reversal index.
func duplicateReversal(s wager.Snapshot, t *wager.Transaction) bool {
	processed := s.Status == wager.StatusProcessed && t.Status() == wager.StatusProcessed
	return processed && t.Kind().IsReversal() && s.Kind.IsReversal() && s.ReferenceTxID == t.ReferenceTxID()
}

func (r memTxs) Save(_ context.Context, t *wager.Transaction) error {
	if err := r.m.fail("txs.save"); err != nil {
		return err
	}
	if r.m.st.txs[t.ID()].Status.Terminal() {
		return errTerminal
	}
	row := txSnapshot(t)
	if err := checkRow(row); err != nil {
		return err
	}
	r.m.st.txs[t.ID()] = row
	return nil
}

func (r memTxs) FindExisting(_ context.Context, providerID, key, externalID string) ([]*wager.Transaction, error) {
	if err := r.m.fail("txs.find"); err != nil {
		return nil, err
	}
	var out []*wager.Transaction
	for _, id := range r.m.st.order {
		s := r.m.st.txs[id]
		if s.External.ProviderID == providerID && (s.External.IdempotencyKey == key || s.External.ExternalID == externalID) {
			out = append(out, mustTx(s))
		}
	}
	return out, nil
}

func (r memTxs) GetByExternal(_ context.Context, providerID, externalID string) (*wager.Transaction, error) {
	if err := r.m.fail("txs.getByExternal"); err != nil {
		return nil, err
	}
	return r.m.byExternal(providerID, externalID)
}

func (m *memStore) byExternal(providerID, externalID string) (*wager.Transaction, error) {
	for _, s := range m.st.txs {
		if s.Origin == wager.OriginExternal && s.External.ProviderID == providerID && s.External.ExternalID == externalID {
			return mustTx(s), nil
		}
	}
	return nil, app.ErrTransactionNotFound
}

func (r memTxs) HasProcessedReversal(_ context.Context, ref uuid.UUID) (bool, error) {
	if err := r.m.fail("txs.reversed"); err != nil {
		return false, err
	}
	for _, s := range r.m.st.txs {
		if s.ReferenceTxID == ref && s.Status == wager.StatusProcessed && s.Kind.IsReversal() {
			return true, nil
		}
	}
	return false, nil
}

func (r memTxs) LockDuePending(_ context.Context, id uuid.UUID, now time.Time) (*wager.Transaction, error) {
	if err := r.m.fail("txs.lock"); err != nil {
		return nil, err
	}
	s, ok := r.m.st.txs[id]
	if !ok || !isDue(s, now) {
		return nil, app.ErrNotDue
	}
	if r.m.lockMutate != nil {
		r.m.lockMutate(&s)
	}
	return mustTx(s), nil
}

func isDue(s wager.Snapshot, now time.Time) bool {
	return s.Status == wager.StatusPendingReference && !s.NextAttemptAt.After(now)
}

func (r memTxs) WakeDependents(_ context.Context, providerID, externalID string, now time.Time) error {
	if err := r.m.fail("txs.wake"); err != nil {
		return err
	}
	for id, s := range r.m.st.txs {
		if s.Status == wager.StatusPendingReference && s.External.ProviderID == providerID && s.External.ReferenceExternalID == externalID {
			s.NextAttemptAt = now
			r.m.st.txs[id] = s
		}
	}
	return nil
}

type memLedger memRepos

func (r memLedger) Append(_ context.Context, e wallet.LedgerEntry) error {
	if err := r.m.fail("ledger.append"); err != nil {
		return err
	}
	for _, row := range r.m.st.ledger {
		if row.Entry.WalletID() == e.WalletID() && row.Entry.TransactionID() == e.TransactionID() {
			return app.ErrConflict
		}
	}
	r.m.st.ledger = append(r.m.st.ledger, app.LedgerRow{Seq: int64(len(r.m.st.ledger) + 1), Entry: e})
	return nil
}

type memOutbox memRepos

func (r memOutbox) Append(_ context.Context, records ...event.Record) error {
	if err := r.m.fail("outbox.append"); err != nil {
		return err
	}
	r.m.st.outbox = append(r.m.st.outbox, records...)
	return nil
}

type memInbox memRepos

func (r memInbox) Register(_ context.Context, consumer, id, hash string, _ time.Time) (app.InboxEntry, bool, error) {
	if err := r.m.fail("inbox.register"); err != nil {
		return app.InboxEntry{}, false, err
	}
	key := consumer + "/" + id
	if row, ok := r.m.st.inbox[key]; ok {
		return app.InboxEntry{PayloadHash: row.hash, Completed: row.completed}, false, nil
	}
	r.m.st.inbox[key] = inboxRow{hash: hash}
	return app.InboxEntry{PayloadHash: hash}, true, nil
}

func (r memInbox) Complete(_ context.Context, consumer, id string, _ uuid.UUID, _ time.Time) error {
	if err := r.m.fail("inbox.complete"); err != nil {
		return err
	}
	key := consumer + "/" + id
	row := r.m.st.inbox[key]
	row.completed = true
	r.m.st.inbox[key] = row
	return nil
}

// Queries (read side).

func (m *memStore) GetWallet(_ context.Context, id uuid.UUID) (*wallet.Wallet, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.fail("q.wallet"); err != nil {
		return nil, err
	}
	return m.getWallet(id)
}

func (m *memStore) ListLedger(_ context.Context, walletID uuid.UUID, after int64, limit int) ([]app.LedgerRow, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.fail("q.ledger"); err != nil {
		return nil, err
	}
	var out []app.LedgerRow
	for _, row := range m.st.ledger {
		if row.Entry.WalletID() == walletID && row.Seq > after && len(out) < limit {
			out = append(out, row)
		}
	}
	return out, nil
}

func (m *memStore) GetTransaction(_ context.Context, id uuid.UUID) (*wager.Transaction, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.fail("q.tx"); err != nil {
		return nil, err
	}
	s, ok := m.st.txs[id]
	if !ok {
		return nil, app.ErrTransactionNotFound
	}
	return mustTx(s), nil
}

func (m *memStore) GetTransactionByExternal(_ context.Context, providerID, externalID string) (*wager.Transaction, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.fail("q.txByExternal"); err != nil {
		return nil, err
	}
	return m.byExternal(providerID, externalID)
}

func (m *memStore) Reconcile(_ context.Context, walletID uuid.UUID) (app.ReconciliationSnapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.fail("q.reconcile"); err != nil {
		return app.ReconciliationSnapshot{}, err
	}
	s, ok := m.st.wallets[walletID]
	if !ok {
		return app.ReconciliationSnapshot{}, app.ErrWalletNotFound
	}
	snap := app.ReconciliationSnapshot{Stored: s.Balance}
	var net big.Int
	for _, row := range m.st.ledger {
		if row.Entry.WalletID() == walletID {
			snap.Entries++
			addSigned(&net, row.Entry)
		}
	}
	if !net.IsInt64() {
		return app.ReconciliationSnapshot{}, money.ErrOverflow
	}
	snap.NetMinor = net.Int64()
	return snap, nil
}

func addSigned(net *big.Int, e wallet.LedgerEntry) {
	amount := big.NewInt(e.Amount().Minor())
	if e.Direction() == wallet.Credit {
		net.Add(net, amount)
		return
	}
	net.Sub(net, amount)
}

func (m *memStore) ListDuePending(_ context.Context, now time.Time, limit int) ([]app.DueTransaction, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.fail("q.due"); err != nil {
		return nil, err
	}
	var out []app.DueTransaction
	for _, id := range m.st.order {
		s := m.st.txs[id]
		if s.Status == wager.StatusPendingReference && !s.NextAttemptAt.After(now) && len(out) < limit {
			out = append(out, app.DueTransaction{ID: s.ID, WalletID: s.WalletID})
		}
	}
	return out, nil
}

// Test helpers.

// txCount returns how many transaction rows are stored.
func (m *memStore) txCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.st.txs)
}

// outboxCount returns how many outbox records are stored.
func (m *memStore) outboxCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.st.outbox)
}

func (m *memStore) outboxTypes() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	types := make([]string, 0, len(m.st.outbox))
	for _, r := range m.st.outbox {
		types = append(types, r.EventType)
	}
	return types
}

func (m *memStore) ledgerCount(walletID uuid.UUID) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, row := range m.st.ledger {
		if row.Entry.WalletID() == walletID {
			n++
		}
	}
	return n
}

// putWalletBalance forces a stored balance to simulate a divergence.
func (m *memStore) putWalletBalance(id uuid.UUID, mutate func(s *wallet.Snapshot)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.st.wallets[id]
	mutate(&s)
	m.st.wallets[id] = s
}

// putTx overwrites a stored transaction snapshot.
func (m *memStore) putTx(id uuid.UUID, mutate func(s *wager.Snapshot)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.st.txs[id]
	mutate(&s)
	m.st.txs[id] = s
}
