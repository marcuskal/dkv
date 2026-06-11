package observability

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/trace/noop"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func testLogger() zerolog.Logger {
	return zerolog.New(io.Discard)
}

// --- Metrics registration tests ---

func TestMetrics_Registration(t *testing.T) {
	m := NewMetrics()
	if m.Registry == nil {
		t.Fatal("registry should not be nil")
	}

	// Verify metrics are queryable by collecting them.
	families, err := m.Registry.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}

	// Should have Go runtime metrics + our custom ones.
	if len(families) < 5 {
		t.Fatalf("expected at least 5 metric families, got %d", len(families))
	}
}

func TestMetrics_DoubleRegistration(t *testing.T) {
	// Verify that creating two separate Metrics instances doesn't panic.
	// This is why we use a custom registry instead of prometheus.DefaultRegisterer.
	m1 := NewMetrics()
	m2 := NewMetrics()

	if m1.Registry == m2.Registry {
		t.Fatal("registries should be independent")
	}
}

func TestMetrics_HTTPEndpoint(t *testing.T) {
	m := NewMetrics()

	// Simulate a few RPCs.
	m.RPCTotal.WithLabelValues("/dkv.DKV/Put", "OK").Inc()
	m.RPCTotal.WithLabelValues("/dkv.DKV/Get", "OK").Add(5)
	m.RPCTotal.WithLabelValues("/dkv.DKV/Get", "NotFound").Inc()
	m.RPCDuration.WithLabelValues("/dkv.DKV/Put").Observe(0.001)
	m.KeysStored.Set(42)

	handler := promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
	req := httptest.NewRequest("GET", "/metrics", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	body := w.Body.String()
	checks := []string{
		"dkv_rpc_total",
		"dkv_rpc_duration_seconds",
		"dkv_keys_stored",
		"go_goroutines",
	}
	for _, check := range checks {
		if !strings.Contains(body, check) {
			t.Errorf("expected %q in metrics output", check)
		}
	}
}

// --- Interceptor tests ---

func TestInterceptor_RecordsMetrics(t *testing.T) {
	m := NewMetrics()
	tracer := noop.NewTracerProvider().Tracer("test")
	log := testLogger()

	interceptor := UnaryServerInterceptor(m, tracer, log)

	info := &grpc.UnaryServerInfo{FullMethod: "/dkv.DKV/Put"}
	handler := func(ctx context.Context, req any) (any, error) {
		return "ok", nil
	}

	resp, err := interceptor(context.Background(), nil, info, handler)
	if err != nil {
		t.Fatalf("interceptor error: %v", err)
	}
	if resp != "ok" {
		t.Fatalf("expected 'ok', got %v", resp)
	}

	// Verify metrics were recorded.
	families, _ := m.Registry.Gather()
	found := false
	for _, f := range families {
		if f.GetName() == "dkv_rpc_total" {
			found = true
			if len(f.GetMetric()) == 0 {
				t.Fatal("rpc_total should have at least one metric")
			}
		}
	}
	if !found {
		t.Fatal("dkv_rpc_total metric not found")
	}
}

func TestInterceptor_RecordsErrors(t *testing.T) {
	m := NewMetrics()
	tracer := noop.NewTracerProvider().Tracer("test")
	log := testLogger()

	interceptor := UnaryServerInterceptor(m, tracer, log)

	info := &grpc.UnaryServerInfo{FullMethod: "/dkv.DKV/Get"}
	handler := func(ctx context.Context, req any) (any, error) {
		return nil, status.Error(codes.NotFound, "key not found")
	}

	_, err := interceptor(context.Background(), nil, info, handler)
	if err == nil {
		t.Fatal("expected error")
	}

	// Verify error was counted.
	families, _ := m.Registry.Gather()
	for _, f := range families {
		if f.GetName() == "dkv_rpc_total" {
			for _, metric := range f.GetMetric() {
				for _, label := range metric.GetLabel() {
					if label.GetName() == "code" && label.GetValue() == "NotFound" {
						return // found it
					}
				}
			}
		}
	}
	t.Fatal("NotFound error not recorded in metrics")
}

// --- Poller tests ---

type mockEngine struct{ keyCount int }

func (m *mockEngine) Len() int { return m.keyCount }

type mockRaft struct {
	leader bool
	term   uint64
	state  int
}

func (m *mockRaft) IsLeader() bool      { return m.leader }
func (m *mockRaft) CurrentTerm() uint64 { return m.term }
func (m *mockRaft) RaftStateInt() int   { return m.state }

type mockLocks struct{ count int }

func (m *mockLocks) ActiveLockCount() int { return m.count }

func TestStatsPoller_UpdatesMetrics(t *testing.T) {
	m := NewMetrics()
	eng := &mockEngine{keyCount: 100}
	raft := &mockRaft{leader: true, term: 5, state: 3}
	locks := &mockLocks{count: 3}
	log := testLogger()

	ctx := context.Background()
	cancel := StartStatsPoller(ctx, m, eng, raft, locks, 50*time.Millisecond, log)
	defer cancel()

	// Wait for at least one poll cycle.
	time.Sleep(100 * time.Millisecond)

	families, _ := m.Registry.Gather()
	for _, f := range families {
		switch f.GetName() {
		case "dkv_keys_stored":
			val := f.GetMetric()[0].GetGauge().GetValue()
			if val != 100 {
				t.Errorf("expected keys_stored=100, got %v", val)
			}
		case "dkv_raft_term":
			val := f.GetMetric()[0].GetGauge().GetValue()
			if val != 5 {
				t.Errorf("expected raft_term=5, got %v", val)
			}
		case "dkv_raft_state":
			val := f.GetMetric()[0].GetGauge().GetValue()
			if val != 3 {
				t.Errorf("expected raft_state=3, got %v", val)
			}
		case "dkv_locks_held":
			val := f.GetMetric()[0].GetGauge().GetValue()
			if val != 3 {
				t.Errorf("expected locks_held=3, got %v", val)
			}
		}
	}
}

func TestStatsPoller_CancelStops(t *testing.T) {
	m := NewMetrics()
	log := testLogger()

	ctx := context.Background()
	cancel := StartStatsPoller(ctx, m, nil, nil, nil, 10*time.Millisecond, log)

	// Cancel should return cleanly — no goroutine leak.
	cancel()
	time.Sleep(50 * time.Millisecond)
}

// --- Health check tests ---

func TestMetricsServer_HealthEndpoints(t *testing.T) {
	m := NewMetrics()
	log := testLogger()

	healthy := true
	srv := NewMetricsServer(":0", m, func() bool { return healthy }, log)

	// Test healthz when healthy.
	req := httptest.NewRequest("GET", "/healthz", nil)
	w := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	// Test healthz when unhealthy.
	healthy = false
	req = httptest.NewRequest("GET", "/healthz", nil)
	w = httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", w.Code)
	}
}
