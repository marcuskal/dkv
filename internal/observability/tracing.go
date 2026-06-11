package observability

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// TracerConfig configures the OpenTelemetry tracer provider.
// The tracer is initialized once at startup; a single client request produces
// a trace spanning gRPC handler → Raft Apply → FSM → Engine.Put → WAL.Append.
type TracerConfig struct {
	// ServiceName is the OTEL resource name (appears in Jaeger/Tempo).
	ServiceName string

	// NodeID distinguishes traces from different QUOLL nodes.
	NodeID string

	// Endpoint is the OTLP collector URL (e.g., "localhost:4318").
	// Empty string means no exporter — traces are discarded (useful for tests).
	Endpoint string

	// SampleRate controls what fraction of traces are exported.
	// 1.0 = all traces (dev), 0.01 = 1% (production).
	//
	// WHY NOT ALWAYS 100%: At 10k RPS, exporting every trace generates
	// ~30 MB/s of telemetry data. At 1% sampling, you still get 100 traces/sec
	// which is plenty for debugging, but your collector cost drops 100x.
	SampleRate float64
}

// InitTracer sets up the OTel tracer provider and returns a shutdown function.
//
// CRITICAL: Call the shutdown function during graceful shutdown to flush
// pending spans. If you skip this, the last few seconds of traces are lost.
//
// PATTERN: The returned shutdown function follows the same pattern as
// http.Server.Shutdown() — it takes a context with a deadline so you don't
// block forever if the collector is unreachable.
func InitTracer(cfg TracerConfig) (trace.Tracer, func(context.Context) error, error) {
	ctx := context.Background()

	// Resource describes this service instance in the trace backend.
	// service.name + service.instance.id let you filter traces per-node.
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(cfg.ServiceName),
			semconv.ServiceInstanceID(cfg.NodeID),
			semconv.ServiceVersion("0.7.0"),
		),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("create resource: %w", err)
	}

	opts := []sdktrace.TracerProviderOption{
		sdktrace.WithResource(res),
	}

	// Set up sampler. ParentBased means: if the incoming request already
	// has a sampling decision (from the client), respect it. Otherwise,
	// apply our ratio-based sampler.
	if cfg.SampleRate <= 0 {
		opts = append(opts, sdktrace.WithSampler(sdktrace.NeverSample()))
	} else if cfg.SampleRate >= 1.0 {
		opts = append(opts, sdktrace.WithSampler(sdktrace.AlwaysSample()))
	} else {
		opts = append(opts, sdktrace.WithSampler(
			sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.SampleRate)),
		))
	}

	// Set up exporter.
	var shutdownFn func(context.Context) error
	if cfg.Endpoint != "" {
		exporter, err := otlptracehttp.New(ctx,
			otlptracehttp.WithEndpoint(cfg.Endpoint),
			otlptracehttp.WithInsecure(), // dev mode — use TLS in prod
		)
		if err != nil {
			return nil, nil, fmt.Errorf("create OTLP exporter: %w", err)
		}

		// BatchSpanProcessor batches spans before export to reduce overhead.
		// MaxQueueSize and BatchTimeout control the trade-off between
		// latency (how fast spans appear in Jaeger) and throughput.
		bsp := sdktrace.NewBatchSpanProcessor(exporter,
			sdktrace.WithMaxQueueSize(2048),
			sdktrace.WithBatchTimeout(5*time.Second),
		)
		opts = append(opts, sdktrace.WithSpanProcessor(bsp))

		shutdownFn = func(ctx context.Context) error {
			if err := bsp.Shutdown(ctx); err != nil {
				return fmt.Errorf("shutdown batch processor: %w", err)
			}
			return exporter.Shutdown(ctx)
		}
	} else {
		// No exporter — noop shutdown. Still creates spans (useful for
		// in-process testing with span recorders).
		shutdownFn = func(context.Context) error { return nil }
	}

	tp := sdktrace.NewTracerProvider(opts...)

	// Set as global provider so otel.Tracer("name") works everywhere.
	// Also set the propagator so trace context crosses gRPC boundaries.
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	tracer := tp.Tracer("github.com/marcuskal/quoll")

	return tracer, shutdownFn, nil
}
