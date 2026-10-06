package domain

import (
	"fmt"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"
)

type Kind string

const (
	KindOpening  Kind = "OPENING"
	KindBet      Kind = "BET"
	KindWin      Kind = "WIN"
	KindLoss     Kind = "LOSS"
	KindRefund   Kind = "REFUND"
	KindRollback Kind = "ROLLBACK"
)

func ParseExternalKind(s string) (Kind, error) {
	switch k := Kind(s); k {
	case KindBet, KindWin, KindLoss, KindRefund, KindRollback:
		return k, nil
	}
	return "", fmt.Errorf("%w: tipo %q não aceito em operações externas", ErrInvalidArgument, s)
}

type Status string

const (
	StatusPending          Status = "PENDING"
	StatusPendingReference Status = "PENDING_REFERENCE"
	StatusProcessed        Status = "PROCESSED"
	StatusRejected         Status = "REJECTED"
	StatusFailed           Status = "FAILED"
)

func (s Status) Terminal() bool {
	return s == StatusProcessed || s == StatusRejected || s == StatusFailed
}

type Origin string

const (
	OriginInternal Origin = "INTERNAL"
	OriginExternal Origin = "EXTERNAL"
)

// ExternalRequest é o conteúdo de negócio de uma operação vinda do provedor (HTTP ou SQS).
type ExternalRequest struct {
	ProviderID                     string
	ExternalTransactionID          string
	IdempotencyKey                 string
	PlayerID                       uuid.UUID
	WalletID                       uuid.UUID
	RoundID                        string
	GameID                         string
	Kind                           Kind
	Money                          Money
	ReferenceExternalTransactionID string
}

func (r ExternalRequest) Validate() error {
	for name, v := range map[string]string{
		"providerId": r.ProviderID, "externalTransactionId": r.ExternalTransactionID,
		"idempotencyKey": r.IdempotencyKey, "roundId": r.RoundID, "gameId": r.GameID,
	} {
		if err := validateIdentifier(name, v); err != nil {
			return err
		}
	}
	if r.PlayerID == uuid.Nil || r.WalletID == uuid.Nil {
		return fmt.Errorf("%w: playerId e walletId são obrigatórios", ErrInvalidArgument)
	}
	if _, err := ParseExternalKind(string(r.Kind)); err != nil {
		return err
	}
	if !r.Money.IsInitialized() {
		return ErrUninitialized
	}
	if r.Kind == KindLoss && !r.Money.IsZero() {
		return fmt.Errorf("%w: LOSS exige valor 0.00", ErrInvalidAmount)
	}
	if r.Kind != KindLoss && !r.Money.IsPositive() {
		return fmt.Errorf("%w: %s exige valor maior que zero", ErrInvalidAmount, r.Kind)
	}
	return r.validateReference()
}

func (r ExternalRequest) validateReference() error {
	ref := r.ReferenceExternalTransactionID
	switch r.Kind {
	case KindRefund, KindRollback:
		return validateIdentifier("referenceExternalTransactionId", ref)
	case KindWin:
		if ref == "" {
			return nil
		}
		return validateIdentifier("referenceExternalTransactionId", ref)
	}
	if ref != "" {
		return fmt.Errorf("%w: %s não aceita referência", ErrInvalidArgument, r.Kind)
	}
	return nil
}

func (r ExternalRequest) HasReference() bool { return r.ReferenceExternalTransactionID != "" }

func validateIdentifier(name, v string) error {
	if v == "" || len(v) > 128 || strings.TrimSpace(v) != v {
		return fmt.Errorf("%w: %s inválido", ErrInvalidArgument, name)
	}
	for _, r := range v {
		if r > unicode.MaxASCII || !unicode.IsPrint(r) {
			return fmt.Errorf("%w: %s deve conter apenas ASCII imprimível", ErrInvalidArgument, name)
		}
	}
	return nil
}

type WagerTransaction struct {
	id                  uuid.UUID
	origin              Origin
	request             ExternalRequest
	payloadHash         string
	walletID            uuid.UUID
	playerID            uuid.UUID
	kind                Kind
	money               Money
	resolvedReferenceID uuid.UUID
	status              Status
	failureCode         FailureCode
	resultBalance       Money
	createdAt           time.Time
	updatedAt           time.Time
}

func NewExternalTransaction(id uuid.UUID, req ExternalRequest, now time.Time) (*WagerTransaction, error) {
	if id == uuid.Nil {
		return nil, fmt.Errorf("%w: id da transação é obrigatório", ErrInvalidArgument)
	}
	if err := req.Validate(); err != nil {
		return nil, err
	}
	hash, err := PayloadHash(req)
	if err != nil {
		return nil, err
	}
	now = now.UTC()
	return &WagerTransaction{
		id: id, origin: OriginExternal, request: req, payloadHash: hash,
		walletID: req.WalletID, playerID: req.PlayerID, kind: req.Kind, money: req.Money,
		status: StatusPending, createdAt: now, updatedAt: now,
	}, nil
}

// NewOpeningTransaction cria o crédito inicial interno; não carrega metadados de provedor.
func NewOpeningTransaction(id uuid.UUID, w *Wallet, now time.Time) (*WagerTransaction, error) {
	if id == uuid.Nil || w == nil {
		return nil, fmt.Errorf("%w: abertura incompleta", ErrInvalidArgument)
	}
	if !w.Balance().IsPositive() {
		return nil, fmt.Errorf("%w: abertura exige saldo inicial positivo", ErrInvalidAmount)
	}
	now = now.UTC()
	return &WagerTransaction{
		id: id, origin: OriginInternal, walletID: w.ID(), playerID: w.PlayerID(),
		kind: KindOpening, money: w.Balance(), status: StatusPending, createdAt: now, updatedAt: now,
	}, nil
}

// TransactionSnapshot carrega o estado persistido para reidratação.
type TransactionSnapshot struct {
	ID                  uuid.UUID
	Origin              Origin
	Request             ExternalRequest
	PayloadHash         string
	WalletID            uuid.UUID
	PlayerID            uuid.UUID
	Kind                Kind
	Money               Money
	ResolvedReferenceID uuid.UUID
	Status              Status
	FailureCode         FailureCode
	ResultBalance       Money
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

func RehydrateTransaction(s TransactionSnapshot) (*WagerTransaction, error) {
	if s.ID == uuid.Nil || s.WalletID == uuid.Nil || !s.Money.IsInitialized() {
		return nil, fmt.Errorf("%w: transação persistida inválida", ErrInvalidArgument)
	}
	if s.Origin != OriginInternal && s.Origin != OriginExternal {
		return nil, fmt.Errorf("%w: origem %q", ErrInvalidArgument, s.Origin)
	}
	return &WagerTransaction{
		id: s.ID, origin: s.Origin, request: s.Request, payloadHash: s.PayloadHash,
		walletID: s.WalletID, playerID: s.PlayerID, kind: s.Kind, money: s.Money,
		resolvedReferenceID: s.ResolvedReferenceID, status: s.Status, failureCode: s.FailureCode,
		resultBalance: s.ResultBalance, createdAt: s.CreatedAt.UTC(), updatedAt: s.UpdatedAt.UTC(),
	}, nil
}

func (t *WagerTransaction) ID() uuid.UUID                  { return t.id }
func (t *WagerTransaction) Origin() Origin                 { return t.origin }
func (t *WagerTransaction) Request() ExternalRequest       { return t.request }
func (t *WagerTransaction) PayloadHash() string            { return t.payloadHash }
func (t *WagerTransaction) WalletID() uuid.UUID            { return t.walletID }
func (t *WagerTransaction) PlayerID() uuid.UUID            { return t.playerID }
func (t *WagerTransaction) Kind() Kind                     { return t.kind }
func (t *WagerTransaction) Money() Money                   { return t.money }
func (t *WagerTransaction) ResolvedReferenceID() uuid.UUID { return t.resolvedReferenceID }
func (t *WagerTransaction) Status() Status                 { return t.status }
func (t *WagerTransaction) FailureCode() FailureCode       { return t.failureCode }
func (t *WagerTransaction) ResultBalance() Money           { return t.resultBalance }
func (t *WagerTransaction) CreatedAt() time.Time           { return t.createdAt }
func (t *WagerTransaction) UpdatedAt() time.Time           { return t.updatedAt }

func (t *WagerTransaction) MarkPendingReference(now time.Time) error {
	if t.status != StatusPending {
		return t.transitionError(StatusPendingReference)
	}
	t.status, t.updatedAt = StatusPendingReference, now.UTC()
	return nil
}

func (t *WagerTransaction) MarkProcessed(balance Money, resolvedReference uuid.UUID, now time.Time) error {
	if err := t.leaveOpenState(StatusProcessed); err != nil {
		return err
	}
	if !balance.IsInitialized() {
		return ErrUninitialized
	}
	t.status, t.resultBalance, t.resolvedReferenceID, t.updatedAt = StatusProcessed, balance, resolvedReference, now.UTC()
	return nil
}

func (t *WagerTransaction) MarkRejected(code FailureCode, balance Money, now time.Time) error {
	if err := t.leaveOpenState(StatusRejected); err != nil {
		return err
	}
	if code == "" || code == FailureInternal {
		return fmt.Errorf("%w: código de rejeição inválido", ErrInvalidArgument)
	}
	t.status, t.failureCode, t.resultBalance, t.updatedAt = StatusRejected, code, balance, now.UTC()
	return nil
}

func (t *WagerTransaction) MarkFailed(now time.Time) error {
	if err := t.leaveOpenState(StatusFailed); err != nil {
		return err
	}
	t.status, t.failureCode, t.updatedAt = StatusFailed, FailureInternal, now.UTC()
	return nil
}

func (t *WagerTransaction) leaveOpenState(to Status) error {
	if t.status != StatusPending && t.status != StatusPendingReference {
		return t.transitionError(to)
	}
	return nil
}

func (t *WagerTransaction) transitionError(to Status) error {
	return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, t.status, to)
}
