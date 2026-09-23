// Package worker runs the background loops: the outbox relay and the
// pending-reference resolver, under a supervisor with observable shutdown.
package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// Group supervises background goroutines. Stop cancels them and waits until
// every one returned or the deadline expires.
type Group struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	logger *slog.Logger

	mu      sync.Mutex
	running int
	stopped bool
}

// NewGroup creates a group whose goroutines live until Stop.
func NewGroup(parent context.Context, logger *slog.Logger) *Group {
	ctx, cancel := context.WithCancel(context.WithoutCancel(parent))
	return &Group{ctx: ctx, cancel: cancel, logger: logger}
}

// Go starts fn in a goroutine; fn must return when its context is done.
// Names are labels for the logs only: two workers may share one. A call
// after Stop is logged and dropped instead of racing the wait.
func (g *Group) Go(name string, fn func(ctx context.Context)) {
	if !g.start() {
		g.logger.Warn("worker not started: group already stopped", "worker", name)
		return
	}
	g.logger.Info("worker started", "worker", name)
	go func() {
		defer g.wg.Done()
		defer g.logger.Info("worker stopped", "worker", name)
		defer g.finish()
		fn(g.ctx)
	}()
}

// start registers a worker unless the group is stopped.
func (g *Group) start() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.stopped {
		return false
	}
	g.wg.Add(1)
	g.running++
	return true
}

func (g *Group) finish() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.running--
}

// Running returns how many workers have not returned yet.
func (g *Group) Running() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.running
}

// Cancel asks the workers to return without waiting for them; later Go
// calls are dropped. It is safe to call more than once.
func (g *Group) Cancel() {
	g.mu.Lock()
	g.stopped = true
	g.mu.Unlock()
	g.cancel()
}

// Stop cancels the workers and waits for them within ctx. It is safe to
// call more than once.
func (g *Group) Stop(ctx context.Context) error {
	g.Cancel()
	done := make(chan struct{})
	go func() {
		g.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("%d workers still running: %w", g.Running(), ctx.Err())
	}
}

// Loop calls tick immediately and then every interval until ctx is done.
func Loop(ctx context.Context, interval time.Duration, tick func(ctx context.Context)) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		tick(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
