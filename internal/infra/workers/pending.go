package workers

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/fx"

	"github.com/Lzdevmendes/backend-challenge-go/internal/app/usecases"
)

type PendingReferenceWorker struct {
	uc     *usecases.PendingReferenceUseCase
	cancel context.CancelFunc
}

func NewPendingReferenceWorker(lc fx.Lifecycle, uc *usecases.PendingReferenceUseCase) *PendingReferenceWorker {
	worker := &PendingReferenceWorker{
		uc: uc,
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			workerCtx, cancel := context.WithCancel(context.Background())
			worker.cancel = cancel
			go worker.start(workerCtx)
			fmt.Println("PendingReferenceWorker started")
			return nil
		},
		OnStop: func(ctx context.Context) error {
			if worker.cancel != nil {
				worker.cancel()
			}
			fmt.Println("PendingReferenceWorker stopped")
			return nil
		},
	})

	return worker
}

func (w *PendingReferenceWorker) start(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Run batch process
			// Limit 50, Timeout 10 minutes
			_, err := w.uc.ProcessBatch(ctx, 50, 10*time.Minute)
			if err != nil {
				fmt.Printf("PendingReferenceWorker error: %v\n", err)
			}
		}
	}
}
