package composition

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/VictorM0nteiro/backend-challenge-go/internal/adapters/httpapi"
	"github.com/VictorM0nteiro/backend-challenge-go/internal/config"
	"go.uber.org/fx"
)

// HTTPModule serves the API. The server is a lifecycle component: it starts
// accepting after the pool exists, and stops accepting before the pool closes.
var HTTPModule = fx.Module("httpapi",
	fx.Provide(
		newAuthenticator,
		httpapi.NewServer,
		newHTTPServer,
	),
)

// HTTPServer owns the listener, so tests can read the bound address when
// HTTP_ADDR is ":0".
type HTTPServer struct {
	srv             *http.Server
	ln              net.Listener
	shutdownTimeout time.Duration
}

// Addr returns the address the server is listening on.
func (s *HTTPServer) Addr() string {
	return s.ln.Addr().String()
}

// newHTTPServer binds the port during construction, so a port already in use
// fails the composition before anything starts.
// newHTTPServer binds the port during construction, so a port already in use
// fails the composition before anything starts.
func newHTTPServer(lc fx.Lifecycle, sd fx.Shutdowner, cfg config.Config, api *httpapi.Server) (*HTTPServer, error) {
	ln, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w", cfg.HTTPAddr, err)
	}

	s := &HTTPServer{
		srv: &http.Server{
			Handler:           api.Handler(),
			ReadHeaderTimeout: 5 * time.Second,
		},
		ln:              ln,
		shutdownTimeout: cfg.ShutdownTimeout,
	}

	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			go func() {
				// Serve returns ErrServerClosed on a normal shutdown. Anything
				// else means the server died, so the whole app stops with an error.
				if err := s.srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
					slog.Error("http server stopped unexpectedly", "error", err)
					_ = sd.Shutdown(fx.ExitCode(1))
				}
			}()
			return nil
		},
		OnStop: func(context.Context) error {
			// Stop taking new connections and wait for in-flight requests, but
			// only for SHUTDOWN_TIMEOUT. After that, Shutdown gives up.
			ctx, cancel := context.WithTimeout(context.Background(), s.shutdownTimeout)
			defer cancel()
			return s.srv.Shutdown(ctx)
		},
	})
	return s, nil
}
