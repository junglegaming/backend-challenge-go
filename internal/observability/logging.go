// Package observability provides JSON logging with contextual identifiers
// and the Prometheus metrics of the service.
package observability

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
)

// ctxKey is the context key of the attributes stored by WithAttrs.
type ctxKey struct{}

// WithAttrs returns a context whose log records carry the given attributes
// (correlationId, messageId, transactionId, walletId, providerId...).
func WithAttrs(ctx context.Context, attrs ...slog.Attr) context.Context {
	existing, _ := ctx.Value(ctxKey{}).([]slog.Attr)
	merged := make([]slog.Attr, 0, len(existing)+len(attrs))
	merged = append(merged, existing...)
	merged = append(merged, attrs...)
	return context.WithValue(ctx, ctxKey{}, merged)
}

// contextHandler adds the attributes stored in the context to each record.
type contextHandler struct {
	slog.Handler
}

// Handle implements slog.Handler.
func (h contextHandler) Handle(ctx context.Context, r slog.Record) error {
	if attrs, ok := ctx.Value(ctxKey{}).([]slog.Attr); ok {
		r.AddAttrs(attrs...)
	}
	return h.Handler.Handle(ctx, r)
}

// WithAttrs implements slog.Handler.
func (h contextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return contextHandler{h.Handler.WithAttrs(attrs)}
}

// WithGroup implements slog.Handler.
func (h contextHandler) WithGroup(name string) slog.Handler {
	return contextHandler{h.Handler.WithGroup(name)}
}

// ParseLevel parses a log level name (debug, info, warn or error,
// case-insensitive) as slog does; config validation uses it.
func ParseLevel(level string) (slog.Level, error) {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(strings.ToUpper(level))); err != nil {
		return 0, fmt.Errorf("log level %q: %w", level, err)
	}
	return lvl, nil
}

// NewLogger builds a JSON logger writing to w at the given level (info when
// the level does not parse), tagged with the service and instance names.
func NewLogger(w io.Writer, level string, instance string) *slog.Logger {
	lvl, err := ParseLevel(level)
	if err != nil {
		lvl = slog.LevelInfo
	}
	h := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: lvl})
	return slog.New(contextHandler{h}).With("service", "wallet-service", "instance", instance)
}
