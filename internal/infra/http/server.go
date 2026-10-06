package http

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"go.uber.org/fx"
)

func NewServer(lc fx.Lifecycle, handler *Handler, logger *slog.Logger) *http.Server {
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)

	srv := &http.Server{
		Addr:    ":8080",
		Handler: LoggerMiddleware(logger, mux),
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			go func() {
				if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
					fmt.Printf("HTTP server failed: %v\n", err)
				}
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			return srv.Shutdown(ctx)
		},
	})

	return srv
}
