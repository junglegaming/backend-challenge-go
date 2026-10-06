package http

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func RegisterMetrics(mux *http.ServeMux) {
	mux.Handle("GET /metrics", promhttp.Handler())
}
