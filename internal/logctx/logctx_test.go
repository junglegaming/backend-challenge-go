package logctx

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

// capture returns a logger that writes JSON lines to the returned buffer, with
// the context handler in front.
func capture() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	return slog.New(NewHandler(slog.NewJSONHandler(&buf, nil))), &buf
}

func decode(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	var line map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &line); err != nil {
		t.Fatalf("log line is not JSON: %v\n%s", err, buf.String())
	}
	return line
}

func TestHandler_AddsTheContextAttributesToTheLine(t *testing.T) {
	logger, buf := capture()
	ctx := NewContext(context.Background())
	SetCorrelationID(ctx, "corr-1")
	Add(ctx, slog.String("walletId", "w-1"))

	logger.InfoContext(ctx, "hello", "own", "value")

	line := decode(t, buf)
	for key, want := range map[string]string{"correlationId": "corr-1", "walletId": "w-1", "own": "value", "msg": "hello"} {
		if line[key] != want {
			t.Fatalf("%s = %v, want %q\n%s", key, line[key], want, buf.String())
		}
	}
}

// The access log is written by the outermost middleware, after the inner
// handlers ran. It must see what they added.
func TestAdd_IsVisibleToACallerHigherInTheStack(t *testing.T) {
	logger, buf := capture()
	outer := NewContext(context.Background())

	func(ctx context.Context) {
		Add(ctx, slog.String("transactionId", "t-1"))
	}(outer)

	logger.InfoContext(outer, "done")
	if got := decode(t, buf)["transactionId"]; got != "t-1" {
		t.Fatalf("transactionId = %v, want t-1", got)
	}
}

func TestWithoutABag_NothingIsAddedAndNothingPanics(t *testing.T) {
	logger, buf := capture()
	ctx := context.Background()

	Add(ctx, slog.String("ignored", "x"))
	SetCorrelationID(ctx, "ignored")
	logger.InfoContext(ctx, "plain")

	if strings.Contains(buf.String(), "ignored") {
		t.Fatalf("attributes leaked into a context with no bag: %s", buf.String())
	}
	if CorrelationID(ctx) != "" {
		t.Fatal("a context with no bag has no correlation id")
	}
}

func TestCorrelationID_RoundTrips(t *testing.T) {
	ctx := NewContext(context.Background())
	SetCorrelationID(ctx, "abc")
	if got := CorrelationID(ctx); got != "abc" {
		t.Fatalf("CorrelationID = %q, want abc", got)
	}
}

// slog.With must keep flowing through the wrapper, or the correlation id would
// disappear from every logger built with With.
func TestHandler_SurvivesWith(t *testing.T) {
	logger, buf := capture()
	ctx := NewContext(context.Background())
	SetCorrelationID(ctx, "corr-2")

	logger.With("component", "consumer").InfoContext(ctx, "hello")

	line := decode(t, buf)
	if line["correlationId"] != "corr-2" || line["component"] != "consumer" {
		t.Fatalf("line = %v", line)
	}
}
