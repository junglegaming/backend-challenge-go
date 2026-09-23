package worker

import (
	"context"
	"log/slog"
)

// PendingService resolves due pending references (app.WagerService).
type PendingService interface {
	// ResolveDue resolves one batch of PENDING_REFERENCE operations whose
	// retry time has passed and returns how many it resolved.
	ResolveDue(ctx context.Context) (int, error)
}

// PendingResolver periodically resolves PENDING_REFERENCE operations.
type PendingResolver struct {
	svc    PendingService
	logger *slog.Logger
}

// NewPendingResolver builds the resolver.
func NewPendingResolver(svc PendingService, logger *slog.Logger) *PendingResolver {
	return &PendingResolver{svc: svc, logger: logger}
}

// Tick resolves one batch. A failure caused by the shutdown of ctx is not
// logged as an error.
func (p *PendingResolver) Tick(ctx context.Context) {
	n, err := p.svc.ResolveDue(ctx)
	if err != nil && ctx.Err() == nil {
		p.logger.WarnContext(ctx, "pending reference batch failed", "error", err)
	}
	if n > 0 {
		p.logger.InfoContext(ctx, "pending references resolved", "count", n)
	}
}
