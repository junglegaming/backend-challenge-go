package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"time"
)

// QueueChecker is the readiness probe for the message queue.
type QueueChecker interface {
	Ping(ctx context.Context) error
}

// live answers while the process can serve requests at all.
func (s *Server) live(w http.ResponseWriter, _ *http.Request) error {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	return nil
}

// ready checks every dependency the process needs and answers 503 if any is
// down. Each check is bounded, so a hung dependency makes readiness fail fast
// instead of hanging the probe. The body names the failing check.
func (s *Server) ready(w http.ResponseWriter, r *http.Request) error {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	checks := map[string]string{"database": "up", "queue": "up"}
	if err := s.pool.Ping(ctx); err != nil {
		slog.Warn("readiness: database down", "error", err)
		checks["database"] = "down"
	}
	if err := s.queue.Ping(ctx); err != nil {
		slog.Warn("readiness: queue down", "error", err)
		checks["queue"] = "down"
	}

	status, code := "ok", http.StatusOK
	if checks["database"] == "down" || checks["queue"] == "down" {
		status, code = "unavailable", http.StatusServiceUnavailable
	}
	writeJSON(w, code, map[string]any{"status": status, "checks": checks})
	return nil
}
