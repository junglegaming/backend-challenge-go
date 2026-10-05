package httpapi

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/VictorM0nteiro/backend-challenge-go/internal/logctx"
	"github.com/google/uuid"
)

const (
	headerCorrelationID = "X-Correlation-Id"
	maxCorrelationIDLen = 128
)

// in X-Correlation-Id, so a request can be followed across services; anything
// that is not a short printable string is replaced, because the value ends up
// in logs and must not be able to forge or break a log line. The id is also
// returned in the response, so the caller can quote it when asking for help.
func correlation(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := logctx.NewContext(r.Context())

		id := r.Header.Get(headerCorrelationID)
		if !validCorrelationID(id) {
			id = uuid.NewString()
		}
		logctx.SetCorrelationID(ctx, id)
		w.Header().Set(headerCorrelationID, id)

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func validCorrelationID(id string) bool {
	if id == "" || len(id) > maxCorrelationIDLen {
		return false
	}
	for _, c := range id {
		// Printable ASCII without space: letters, digits and punctuation.
		if c <= ' ' || c >= 0x7f {
			return false
		}
	}
	return true
}

// statusRecorder remembers the status a handler wrote.
type statusRecorder struct {
	http.ResponseWriter
	status int
	wrote  bool
}

func (s *statusRecorder) WriteHeader(code int) {
	if !s.wrote {
		s.status, s.wrote = code, true
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(b []byte) (int, error) {
	if !s.wrote {
		s.status, s.wrote = http.StatusOK, true
	}
	return s.ResponseWriter.Write(b)
}

// accessLog writes one line per request, after it ends: method, route, status
// and duration, plus everything the handlers attached to the context along the
// way (the client, the wallet, the transaction, the error code).
//
// It logs the route pattern, never the raw path: the path carries ids, which
// would turn a log search into a scan and would also be a high-cardinality
// value. It never logs headers or bodies, so no credential and no amount can
// reach the log from here.
func accessLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		started := time.Now()

		next.ServeHTTP(rec, r)

		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}
		slog.LogAttrs(r.Context(), levelFor(rec.status), "http request",
			slog.String("method", r.Method),
			slog.String("route", route),
			slog.Int("status", rec.status),
			slog.Float64("durationMs", float64(time.Since(started).Microseconds())/1000),
		)
	})
}

// levelFor keeps 5xx visible at error level and everything else at info.
func levelFor(status int) slog.Level {
	if status >= 500 {
		return slog.LevelError
	}
	return slog.LevelInfo
}

// Explicação

// - correlation aceita o X-Correlation-Id do cliente, para seguir uma requisição entre serviços, mas troca qualquer valor que não seja uma
// string curta e imprimível. O valor vai parar no log, e um id com quebra de linha poderia forjar uma linha falsa. O id volta no cabeçalho da resposta,
//  para o cliente citar ao pedir ajuda.
// - accessLog escreve uma linha por requisição, depois que ela termina, com método, rota, status e duração, mais o que os handlers acrescentaram à sacola.
// - Ele loga o padrão da rota (GET /wallets/{walletId}), nunca o caminho cru. O caminho carrega ids, o que viraria um valor de cardinalidade alta e vazaria identificadores.
// E nunca loga cabeçalhos nem corpos, então credencial e valor monetário não chegam ao log por aqui.
// - O r.Pattern é preenchido pelo próprio ServeMux no mesmo ponteiro de requisição que o accessLog repassa, por isso funciona depois do next.ServeHTTP.
