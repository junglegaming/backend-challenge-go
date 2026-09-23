package app

import (
	"context"
	"errors"
	"fmt"
)

// inTx runs fn in its own unit of work, retrying lost races, and returns
// its result once committed. op labels the conflict metric.
func inTx[T any](ctx context.Context, s *WagerService, op string, fn func(ctx context.Context, r Repositories) (T, error)) (T, error) {
	var out T
	err := retryConflicts(ctx, s.ConflictRetries, s.onConflict(op), func() error {
		return s.UoW.Do(ctx, func(ctx context.Context, r Repositories) error {
			var err error
			out, err = fn(ctx, r)
			return err
		})
	})
	return out, err
}

// retryConflicts runs fn again while it fails with ErrConflict, at most
// retries extra times and only while ctx is alive. Conflicts come from
// losing a race inside PostgreSQL (unique violation, stale version,
// deadlock, serialization failure); the retried transaction observes the
// committed winner and converges.
func retryConflicts(ctx context.Context, retries int, onConflict func(), fn func() error) error {
	err := fn()
	for i := 0; i < retries && errors.Is(err, ErrConflict); i++ {
		if ctx.Err() != nil {
			return fmt.Errorf("%w: %w", err, ctx.Err())
		}
		onConflict()
		err = fn()
	}
	return err
}

// sequence runs steps in order and stops at the first error. It keeps
// multi-step persistence flat and readable.
func sequence(steps ...func() error) error {
	for _, step := range steps {
		if err := step(); err != nil {
			return err
		}
	}
	return nil
}
