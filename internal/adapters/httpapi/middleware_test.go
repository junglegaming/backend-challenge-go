package httpapi

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/VictorM0nteiro/backend-challenge-go/internal/logctx"
)

// logBuffer collects log output. The server logs from its own goroutines, so
// reads and writes need a lock.
type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *logBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *logBuffer) text() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

// lines decodes every JSON log line written so far.
func (l *logBuffer) lines(t *testing.T) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, raw := range strings.Split(strings.TrimSpace(l.text()), "\n") {
		if raw == "" {
			continue
		}
		var line map[string]any
		if err := json.Unmarshal([]byte(raw), &line); err != nil {
			t.Fatalf("log line is not JSON: %v\n%s", err, raw)
		}
		out = append(out, line)
	}
	return out
}

// captureLogs replaces the default logger with one that writes to a buffer,
// through the same context-aware handler main uses, and restores it afterwards.
func captureLogs(t *testing.T) *logBuffer {
	t.Helper()
	previous := slog.Default()
	buf := &logBuffer{}
	slog.SetDefault(slog.New(logctx.NewHandler(slog.NewJSONHandler(buf, nil))))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return buf
}

func TestCorrelation_GeneratesAnIdWhenAbsent(t *testing.T) {
	var seen string
	h := correlation(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = logctx.CorrelationID(r.Context())
	}))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	if seen == "" {
		t.Fatal("the handler saw no correlation id")
	}
	if got := rec.Header().Get(headerCorrelationID); got != seen {
		t.Fatalf("response header = %q, want the id the handler saw (%q)", got, seen)
	}
}

func TestCorrelation_KeepsAValidIdFromTheClient(t *testing.T) {
	var seen string
	h := correlation(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = logctx.CorrelationID(r.Context())
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set(headerCorrelationID, "client-trace-42")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if seen != "client-trace-42" || rec.Header().Get(headerCorrelationID) != "client-trace-42" {
		t.Fatalf("seen = %q, header = %q, want the client's id kept", seen, rec.Header().Get(headerCorrelationID))
	}
}

// The id is written into logs, so a value that could forge or break a line must
// be replaced by a generated one.
func TestCorrelation_ReplacesAnIdThatCouldBreakALogLine(t *testing.T) {
	cases := map[string]string{
		"com espaco":      "has space",
		"com quebra":      "line\nbreak",
		"muito longo":     strings.Repeat("a", maxCorrelationIDLen+1),
		"com nao ascii":   "caf\u00e9",
		"com tabulacao":   "tab\there",
		"vazio de espaco": " ",
	}
	for name, bad := range cases {
		t.Run(name, func(t *testing.T) {
			var seen string
			h := correlation(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				seen = logctx.CorrelationID(r.Context())
			}))
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.Header.Set(headerCorrelationID, bad)
			h.ServeHTTP(httptest.NewRecorder(), req)

			if seen == "" || seen == bad {
				t.Fatalf("seen = %q for header %q, want a generated id", seen, bad)
			}
		})
	}
}

func TestAccessLog_WritesOneLineWithRouteStatusAndWhatHandlersAdded(t *testing.T) {
	logs := captureLogs(t)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /things/{id}", func(w http.ResponseWriter, r *http.Request) {
		logctx.Add(r.Context(), slog.String("transactionId", "t-9"))
		w.WriteHeader(http.StatusNoContent)
	})

	req := httptest.NewRequest(http.MethodGet, "/things/12345", nil)
	req.Header.Set(headerCorrelationID, "trace-1")
	req.Header.Set("Authorization", "Bearer secret-token-xyz")
	correlation(accessLog(mux)).ServeHTTP(httptest.NewRecorder(), req)

	lines := logs.lines(t)
	if len(lines) != 1 {
		t.Fatalf("got %d log lines, want 1:\n%s", len(lines), logs.text())
	}
	line := lines[0]
	if line["method"] != "GET" || line["route"] != "GET /things/{id}" || line["status"] != float64(204) {
		t.Fatalf("line = %v", line)
	}
	if line["correlationId"] != "trace-1" || line["transactionId"] != "t-9" {
		t.Fatalf("line misses the correlation id or what the handler added: %v", line)
	}
	if _, ok := line["durationMs"]; !ok {
		t.Fatalf("line has no duration: %v", line)
	}

	// The raw path carries ids and the header carries a credential. Neither belongs here.
	for _, forbidden := range []string{"12345", "secret-token-xyz"} {
		if strings.Contains(logs.text(), forbidden) {
			t.Fatalf("the log contains %q:\n%s", forbidden, logs.text())
		}
	}
}

func TestAccessLog_AUnmatchedRouteIsLoggedWithoutItsPath(t *testing.T) {
	logs := captureLogs(t)
	mux := http.NewServeMux()

	correlation(accessLog(mux)).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/secret-path-777", nil))

	lines := logs.lines(t)
	if len(lines) != 1 || lines[0]["route"] != "unmatched" || lines[0]["status"] != float64(404) {
		t.Fatalf("lines = %v", lines)
	}
	if strings.Contains(logs.text(), "secret-path-777") {
		t.Fatalf("the log contains the raw path:\n%s", logs.text())
	}
}

// End to end, through the real routes, the real authentication and a real
// database: the line for an operation carries every identifier the enunciado
// asks for, and nothing sensitive.
func TestSubmitWager_LogCarriesTheIdentifiersAndNoSecrets(t *testing.T) {
	base := newTestAPI(t)
	walletID := openWallet(t, base, "1000.00")
	logs := captureLogs(t)

	headers := wagerHeaders("provider-a", "provider-a:log-1")
	headers[headerCorrelationID] = "trace-bet-1"
	req, err := http.NewRequest(http.MethodPost, base+wagersURL, bytes.NewReader(mustJSON(t, wagerBody(walletID, "log-1", "BET", "25.00"))))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201", resp.StatusCode)
	}
	if got := resp.Header.Get(headerCorrelationID); got != "trace-bet-1" {
		t.Fatalf("response correlation id = %q, want trace-bet-1", got)
	}

	var operation, access map[string]any
	for _, line := range logs.lines(t) {
		if line["correlationId"] != "trace-bet-1" {
			continue
		}
		switch line["msg"] {
		case "wager operation":
			operation = line
		case "http request":
			access = line
		}
	}
	if operation == nil || access == nil {
		t.Fatalf("missing log lines for the request:\n%s", logs.text())
	}

	for key, want := range map[string]any{
		"providerId": "provider-a", "walletId": walletID, "kind": "BET",
		"status": "PROCESSED", "replayed": false,
	} {
		if operation[key] != want {
			t.Fatalf("operation line %s = %v, want %v\n%v", key, operation[key], want, operation)
		}
	}
	if id, _ := operation["transactionId"].(string); id == "" {
		t.Fatalf("operation line has no transactionId: %v", operation)
	}
	if access["route"] != "POST /wagering/transactions" || access["status"] != float64(201) {
		t.Fatalf("access line = %v", access)
	}

	// No credential, and no financial value.
	for _, forbidden := range []string{testTokens["provider-a"], "25.00", "Bearer"} {
		if strings.Contains(logs.text(), forbidden) {
			t.Fatalf("the log contains %q:\n%s", forbidden, logs.text())
		}
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return raw
}
