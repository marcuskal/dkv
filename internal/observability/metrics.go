// Package observability provides metrics, tracing, and log correlation for QUOLL.
//
// ARCHITECTURE:
//
//	gRPC interceptor ──▶ Prometheus counters/histograms (RED method)
//	Engine/Raft/Lock  ──▶ Custom Prometheus collectors (runtime stats)
//	All layers        ──▶ OpenTelemetry spans (distributed tracing)
//
// RED metrics (Rate, Errors, Duration) on every RPC boundary, plus domain-specific
// gauges for engine, Raft, and lock state. Traces via OpenTelemetry; logs
// correlated by trace ID.
package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Metrics holds all Prometheus metrics for QUOLL.
// Using a struct rather than global vars so metrics can be scoped per-node
// in tests and won't collide in multi-node integration tests.
//
// WHY NOT prometheus.MustRegister globals: Global registration panics if
// you register twice (common in tests). A custom registry avoids this entirely.
// This is a production pattern from the prometheus/client_golang docs.
type Metrics struct {
	Registry *prometheus.Registry

	// --- RED: gRPC request metrics ---

	// RPCTotal counts total RPCs by method and status code.
	// USE: rate(quoll_rpc_total[5m]) gives you requests/sec.
	// rate(quoll_rpc_total{code!="OK"}[5m]) gives error rate.
	RPCTotal *prometheus.CounterVec

	// RPCDuration tracks RPC latency distribution.
	// USE: histogram_quantile(0.99, rate(quoll_rpc_duration_seconds_bucket[5m]))
	// gives you p99 latency — the single most important SLI for a KV store.
	RPCDuration *prometheus.HistogramVec

	// RPCInFlight tracks concurrent in-flight RPCs.
	// USE: high values indicate backpressure or slow downstream deps.
	RPCInFlight *prometheus.GaugeVec

	// --- Domain-specific metrics ---

	// KeysStored is the current number of keys in the engine.
	// Exposed by the custom collector, but we also track it as a gauge
	// for direct dashboard use.
	KeysStored prometheus.Gauge

	// WALBytesWritten tracks total bytes written to the WAL.
	WALBytesWritten prometheus.Counter

	// WALSyncs counts WAL fsync operations.
	WALSyncs prometheus.Counter

	// RaftApplyDuration tracks time spent in FSM.Apply().
	// Separating this from RPC duration isolates Raft overhead from
	// network + serialization time.
	RaftApplyDuration prometheus.Histogram

	// RaftTerm is the current Raft term number.
	// Term jumps indicate leader elections — useful for correlating
	// latency spikes with election events.
	RaftTerm prometheus.Gauge

	// RaftState tracks the current Raft state (leader=3, candidate=2, follower=1).
	RaftState prometheus.Gauge

	// LockCount tracks the number of currently held distributed locks.
	LockCount prometheus.Gauge

	// SagaTotal counts saga executions by status (completed, compensated, failed).
	SagaTotal *prometheus.CounterVec

	// SagaDuration tracks saga execution time including compensations.
	SagaDuration prometheus.Histogram
}

// NewMetrics creates and registers all QUOLL metrics on a fresh registry.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()

	// Register the default Go runtime collectors (GC stats, goroutine count, etc.)
	// These are invaluable during incidents — "is GC thrashing?" is a common
	// first question during latency investigations.
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	m := &Metrics{
		Registry: reg,

		RPCTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "quoll",
			Name:      "rpc_total",
			Help:      "Total RPCs by method and gRPC status code.",
		}, []string{"method", "code"}),

		RPCDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "quoll",
			Name:      "rpc_duration_seconds",
			Help:      "RPC latency distribution in seconds.",
			// Buckets tuned for a KV store: most ops should be sub-ms.
			// If you see mass in the 100ms+ buckets, something is wrong.
			Buckets: []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0},
		}, []string{"method"}),

		RPCInFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "quoll",
			Name:      "rpc_in_flight",
			Help:      "Number of RPCs currently being processed.",
		}, []string{"method"}),

		KeysStored: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "quoll",
			Name:      "keys_stored",
			Help:      "Current number of keys in the engine.",
		}),

		WALBytesWritten: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "quoll",
			Name:      "wal_bytes_written_total",
			Help:      "Total bytes written to the WAL.",
		}),

		WALSyncs: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "quoll",
			Name:      "wal_syncs_total",
			Help:      "Total WAL fsync operations.",
		}),

		RaftApplyDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "quoll",
			Name:      "raft_apply_duration_seconds",
			Help:      "Time spent in FSM.Apply() (Raft commit to engine write).",
			Buckets:   []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1},
		}),

		RaftTerm: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "quoll",
			Name:      "raft_term",
			Help:      "Current Raft term number.",
		}),

		RaftState: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "quoll",
			Name:      "raft_state",
			Help:      "Current Raft state: 1=follower, 2=candidate, 3=leader.",
		}),

		LockCount: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "quoll",
			Name:      "locks_held",
			Help:      "Number of currently held distributed locks.",
		}),

		SagaTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "quoll",
			Name:      "saga_total",
			Help:      "Total saga executions by outcome.",
		}, []string{"status"}),

		SagaDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "quoll",
			Name:      "saga_duration_seconds",
			Help:      "Saga execution duration including compensations.",
			Buckets:   []float64{0.001, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0},
		}),
	}

	// Register all metrics.
	reg.MustRegister(
		m.RPCTotal,
		m.RPCDuration,
		m.RPCInFlight,
		m.KeysStored,
		m.WALBytesWritten,
		m.WALSyncs,
		m.RaftApplyDuration,
		m.RaftTerm,
		m.RaftState,
		m.LockCount,
		m.SagaTotal,
		m.SagaDuration,
	)

	return m
}
