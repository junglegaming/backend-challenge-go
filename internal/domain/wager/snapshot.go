package wager

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/Pantani/backend-challenge-go/internal/domain/money"
)

func (s Snapshot) validate() error {
	if err := validateCore(s.ID, s.WalletID, s.PlayerID, s.CreatedAt); err != nil {
		return err
	}
	if err := s.validateOperation(); err != nil {
		return err
	}
	if err := validateTimestamp(s.UpdatedAt, s.CreatedAt); err != nil {
		return err
	}
	if s.Attempts < 0 {
		return fmt.Errorf("%w: negative attempts", ErrInvalidTransaction)
	}
	return s.validateState()
}

func (s Snapshot) validateOperation() error {
	if !s.Kind.Valid() {
		return fmt.Errorf("%w: unknown kind %q", ErrInvalidTransaction, s.Kind)
	}
	if err := validateAmount(s.Kind, s.Amount); err != nil {
		return err
	}
	if s.Kind == KindOpening {
		return s.validateOpening()
	}
	if s.Origin != OriginExternal {
		return fmt.Errorf("%w: invalid external origin", ErrInvalidTransaction)
	}
	return s.External.validate(s.Kind)
}

func (s Snapshot) validateOpening() error {
	if s.Origin != OriginInternal || s.External != (External{}) {
		return fmt.Errorf("%w: opening requires internal origin and no external metadata", ErrInvalidTransaction)
	}
	return nil
}

func validateTimestamp(stamp, earliest time.Time) error {
	if stamp.IsZero() || stamp.Before(earliest) {
		return fmt.Errorf("%w: missing or regressing timestamp", ErrInvalidTransaction)
	}
	return nil
}

func (s Snapshot) validateState() error {
	switch s.Status {
	case StatusProcessed:
		return s.validateProcessed()
	case StatusRejected:
		return s.validateRejected()
	case StatusFailed:
		return s.validateFailed()
	case StatusPending:
		return s.validatePending()
	case StatusPendingReference:
		return s.validateWaiting()
	}
	return fmt.Errorf("%w: unknown status %q", ErrInvalidTransaction, s.Status)
}

func (s Snapshot) validateProcessed() error {
	if s.FailureCode != "" {
		return fmt.Errorf("%w: processed transaction has failure code", ErrInvalidTransaction)
	}
	if err := requireProcessedBalance(s.ResultBalance, s.Amount); err != nil {
		return err
	}
	tx := Transaction{kind: s.Kind, external: s.External}
	return tx.checkResolvedReference(s.ReferenceTxID)
}

func (s Snapshot) validateRejected() error {
	if err := requireCode(s.FailureCode, "rejection"); err != nil {
		return err
	}
	if s.ReferenceTxID != uuid.Nil {
		return fmt.Errorf("%w: rejected transaction has resolved reference", ErrInvalidTransaction)
	}
	return requireBalance(s.ResultBalance, "rejected")
}

func (s Snapshot) validateFailed() error {
	if err := requireCode(s.FailureCode, "failure"); err != nil {
		return err
	}
	return s.validateUnresolved()
}

func (s Snapshot) validatePending() error {
	if s.FailureCode != "" {
		return fmt.Errorf("%w: pending transaction has failure code", ErrInvalidTransaction)
	}
	return s.validateUnresolved()
}

func (s Snapshot) validateUnresolved() error {
	if s.ReferenceTxID != uuid.Nil || s.ResultBalance != (money.Money{}) {
		return fmt.Errorf("%w: unresolved transaction has a result", ErrInvalidTransaction)
	}
	return nil
}

func (s Snapshot) validateWaiting() error {
	if err := s.validatePending(); err != nil {
		return err
	}
	if !s.Kind.AcceptsReference() || s.External.ReferenceExternalID == "" {
		return fmt.Errorf("%w: waiting transaction has no reference", ErrInvalidTransaction)
	}
	if s.Attempts == 0 {
		return fmt.Errorf("%w: waiting transaction needs an attempt", ErrInvalidTransaction)
	}
	return validateTimestamp(s.NextAttemptAt, s.UpdatedAt)
}
