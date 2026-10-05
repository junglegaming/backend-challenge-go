package logctx

import (
	"context"
	"log/slog"
	"sync"
)

// Package logctx carries log attributes, and the correlation id, through a
// context, so that every log line written for a request or a message names the
// same identifiers without each call site repeating them.

type bagKey struct{}

// bag is the set of attributes attached to one request or message. It is
// shared, not copied: a handler deep in the call stack can add the transaction
// id, and the access log written by the outermost middleware, afterwards, still
// sees it.
type bag struct {
	mu            sync.Mutex
	attrs         []slog.Attr
	correlationID string
}

// NewContext returns a context with an empty bag. Call it once, at the edge:
// the HTTP middleware or the start of a message.
func NewContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, bagKey{}, &bag{})
}

func from(ctx context.Context) *bag {
	b, _ := ctx.Value(bagKey{}).(*bag)
	return b
}

// Add attaches attributes to the bag. It does nothing when the context has no
// bag, so callers never need to check.
func Add(ctx context.Context, attrs ...slog.Attr) {
	b := from(ctx)
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.attrs = append(b.attrs, attrs...)
}

// SetCorrelationID records the id that ties together every log line, and every
// event, produced for one request or message.
func SetCorrelationID(ctx context.Context, id string) {
	b := from(ctx)
	if b == nil {
		return
	}
	b.mu.Lock()
	b.correlationID = id
	b.mu.Unlock()
	Add(ctx, slog.String("correlationId", id))
}

// CorrelationID returns the id set for this context, or "" when there is none.
func CorrelationID(ctx context.Context) string {
	b := from(ctx)
	if b == nil {
		return ""
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.correlationID
}

// attrs returns a copy of the bag's attributes.
func attrs(ctx context.Context) []slog.Attr {
	b := from(ctx)
	if b == nil {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]slog.Attr, len(b.attrs))
	copy(out, b.attrs)
	return out
}

// Handler wraps a slog.Handler and adds the context's attributes to every
// record. Only the calls that take a context (slog.InfoContext and friends)
// carry them; a plain slog.Info has no context to read.
type Handler struct {
	slog.Handler
}

// NewHandler wraps next.
func NewHandler(next slog.Handler) Handler {
	return Handler{Handler: next}
}

// Handle adds the bag's attributes, then delegates.
func (h Handler) Handle(ctx context.Context, r slog.Record) error {
	r.AddAttrs(attrs(ctx)...)
	return h.Handler.Handle(ctx, r)
}

// WithAttrs keeps the wrapper, so attributes added by slog.With still flow
// through Handle.
func (h Handler) WithAttrs(a []slog.Attr) slog.Handler {
	return Handler{Handler: h.Handler.WithAttrs(a)}
}

// WithGroup keeps the wrapper for the same reason.
func (h Handler) WithGroup(name string) slog.Handler {
	return Handler{Handler: h.Handler.WithGroup(name)}
}

// Explicação

// - O ponto central é o bag, uma sacola compartilhada por ponteiro. Quem está fundo na pilha (o handler que descobre o transactionId)
// acrescenta à sacola, e o log de acesso do middleware mais externo, escrito depois, já vê o que foi acrescentado. Com um contexto imutável isso não seria possível.
// - Handler embrulha o JSONHandler e, em cada registro, acrescenta os atributos da sacola. Só as chamadas slog.InfoContext(ctx, ...) carregam o contexto;
// um slog.Info simples não tem como ler a sacola.
// - WithAttrs e WithGroup devolvem o wrapper, senão um logger criado com slog.With perderia o correlationId. Há um teste só para isso.
// - Add e SetCorrelationID não fazem nada sem sacola, então quem chama nunca precisa checar.
