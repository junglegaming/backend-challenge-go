package worker

import (
	"context"
	"time"
)

// Detach returns a context that ignores the cancellation of parent but keeps
// its values (log attributes, trace ids) and expires after d. Loops use it
// for bounded work that must complete even while the service shuts down, such
// as publishing or durably finalizing a claimed record.
func Detach(parent context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(parent), d)
}

// Sleep waits for d or until ctx is done, whichever comes first.
func Sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
