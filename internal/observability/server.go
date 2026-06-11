package observability

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
)

// MetricsServer serves Prometheus metrics and Kubernetes health probes over HTTP.
// Running on a separate port from gRPC means a scrape surge or an overloaded
// data path cannot prevent the monitoring stack from seeing the problem.
type MetricsServer struct {
	httpServer *http.Server
	log        zerolog.Logger
}

// HealthChecker returns true if the node is healthy.
type HealthChecker func() bool

// NewMetricsServer creates an HTTP server for metrics and health endpoints.
func NewMetricsServer(addr string, m *Metrics, healthCheck HealthChecker, log zerolog.Logger) *MetricsServer {
	mux := http.NewServeMux()

	// /metrics — Prometheus scrape endpoint.
	// Uses our custom registry (not the default global one) so we only
	// expose DKV metrics, not random metrics from imported libraries.
	mux.Handle("/metrics", promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	}))

	// /healthz — Kubernetes liveness probe.
	// Returns 200 if the process is alive and the engine isn't closed.
	// This is NOT the same as readiness — a node can be alive but not
	// ready (e.g., still replaying WAL or waiting for Raft leader).
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if healthCheck != nil && !healthCheck() {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprint(w, "unhealthy")
			return
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "ok")
	})

	// /readyz — Kubernetes readiness probe.
	// Returns 200 only when the node is ready to serve traffic.
	// During startup (WAL replay, Raft election), this returns 503.
	// The K8s Service won't route traffic to this pod until readyz passes.
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if healthCheck != nil && !healthCheck() {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprint(w, "not ready")
			return
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "ready")
	})

	return &MetricsServer{
		httpServer: &http.Server{
			Addr:         addr,
			Handler:      mux,
			ReadTimeout:  5 * time.Second,
			WriteTimeout: 10 * time.Second,
			IdleTimeout:  30 * time.Second,
		},
		log: log.With().Str("component", "metrics-server").Logger(),
	}
}

// Serve starts the HTTP server. Blocks until the server is stopped.
func (s *MetricsServer) Serve() error {
	s.log.Info().Str("addr", s.httpServer.Addr).Msg("metrics server starting")
	if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("metrics server: %w", err)
	}
	return nil
}

// Shutdown gracefully stops the metrics server.
func (s *MetricsServer) Shutdown(ctx context.Context) error {
	s.log.Info().Msg("metrics server shutting down")
	return s.httpServer.Shutdown(ctx)
}