package observability

import (
	"context"
	"time"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	grpccodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// UnaryServerInterceptor creates an OTel span, records Prometheus RED metrics,
// and injects the trace ID into zerolog so every log line emitted during the RPC
// carries the same trace_id (enabling Loki → Tempo correlation in Grafana).
//
// Observability wraps Timeout so that a timeout-cancelled RPC still produces
// a complete span and a metric data point with the deadline-exceeded code.
func UnaryServerInterceptor(m *Metrics, tracer trace.Tracer, log zerolog.Logger) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		start := time.Now()
		method := info.FullMethod

		// 1. Start a span for this RPC.
		ctx, span := tracer.Start(ctx, method,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				attribute.String("rpc.system", "grpc"),
				attribute.String("rpc.method", method),
			),
		)
		defer span.End()

		// 2. Track in-flight RPCs.
		m.RPCInFlight.WithLabelValues(method).Inc()
		defer m.RPCInFlight.WithLabelValues(method).Dec()

		// 3. Extract trace ID for log correlation.
		spanCtx := span.SpanContext()
		rpcLog := log.With().
			Str("trace_id", spanCtx.TraceID().String()).
			Str("span_id", spanCtx.SpanID().String()).
			Str("method", method).
			Logger()

		// 4. Inject the correlated logger into context so downstream
		// code (engine, raft) can log with the same trace ID.
		ctx = rpcLog.WithContext(ctx)

		// 5. Call the actual handler.
		resp, err := handler(ctx, req)

		// 6. Record metrics and finalize span.
		duration := time.Since(start)
		code := status.Code(err)
		codeStr := code.String()

		m.RPCTotal.WithLabelValues(method, codeStr).Inc()
		m.RPCDuration.WithLabelValues(method).Observe(duration.Seconds())

		// Set span status and attributes.
		span.SetAttributes(
			attribute.String("rpc.grpc.status_code", codeStr),
			attribute.Float64("rpc.duration_ms", float64(duration.Milliseconds())),
		)

		if err != nil {
			span.RecordError(err)
			if code == grpccodes.Internal || code == grpccodes.Unavailable {
				span.SetStatus(codes.Error, err.Error())
			}
		} else {
			span.SetStatus(codes.Ok, "")
		}

		// 7. Log with correlation.
		event := rpcLog.Info()
		if err != nil {
			event = rpcLog.Warn()
		}
		event.
			Dur("duration", duration).
			Str("code", codeStr).
			Msg("rpc completed")

		return resp, err
	}
}
