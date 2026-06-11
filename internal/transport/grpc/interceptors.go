package server

import (
	"context"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Interceptor chain order: PanicRecovery → Logging → Timeout → Handler
//
// PanicRecovery is outermost: it must catch panics from every layer below,
// including the logger and timeout interceptors. If recovery were inside
// logging and the logger panicked, the process would crash.
//
// Logging wraps timeout so the log entry records whether a request
// completed normally or was killed by the deadline — critical for
// debugging latency issues in production.
//
// Timeout is innermost (closest to handler) so it applies the deadline
// the handler must respect via ctx.Done().

// PanicRecoveryInterceptor converts handler panics to gRPC Internal errors
// instead of crashing the process. Stack trace is captured inside the
// recovery function — after recover() returns the original stack is gone.
func PanicRecoveryInterceptor(log zerolog.Logger) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (resp any, err error) {
		defer func() {
			if r := recover(); r != nil {
				// Capture stack trace at the point of panic
				stack := debug.Stack()
				log.Error().
					Str("method", info.FullMethod).
					Str("panic", fmt.Sprintf("%v", r)).
					Bytes("stack", stack).
					Msg("panic recovered in gRPC handler")

				// Return Internal to the client. Do NOT leak the panic message
				// to external clients — it might contain sensitive info.
				err = status.Error(codes.Internal, "internal server error")
			}
		}()
		return handler(ctx, req)
	}
}

// LoggingInterceptor logs every RPC with method, duration, and status code.
// Errors are logged at Warn so they're easy to filter in a log aggregator.
// Request/response bodies are not logged by default — they may contain PII.
func LoggingInterceptor(log zerolog.Logger) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		start := time.Now()

		resp, err := handler(ctx, req)

		duration := time.Since(start)
		code := status.Code(err)

		// Log level based on outcome: errors get Warn so they're
		// easy to filter in your log aggregator.
		event := log.Info()
		if err != nil {
			event = log.Warn()
		}

		event.
			Str("method", info.FullMethod).
			Dur("duration", duration).
			Str("code", code.String()).
			Msg("rpc completed")

		return resp, err
	}
}

// TimeoutInterceptor enforces a server-side deadline on all RPCs.
// gRPC uses the minimum of client and server deadlines. This interceptor
// applies a server timeout only when the client has not already set one,
// acting as a safety net against unbounded handler execution.
func TimeoutInterceptor(timeout time.Duration) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		// Only apply our timeout if the client hasn't set a tighter one.
		if _, hasDeadline := ctx.Deadline(); !hasDeadline {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}
		return handler(ctx, req)
	}
}

// ChainUnaryInterceptors composes multiple interceptors into one.
// Execution order: first interceptor in the slice is outermost (runs first
// on the way in, last on the way out). Each interceptor calls `handler`,
// which is actually the next interceptor in the chain, not the final handler.
func ChainUnaryInterceptors(interceptors ...grpc.UnaryServerInterceptor) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		// Build the chain from inside out.
		// The innermost handler is the actual RPC handler.
		// Each interceptor wraps the next one.
		current := handler
		for i := len(interceptors) - 1; i >= 0; i-- {
			// Capture loop variable (Go < 1.22 gotcha; safe in 1.22+ but
			// explicit capture is clearer and more portable).
			interceptor := interceptors[i]
			next := current
			current = func(ctx context.Context, req any) (any, error) {
				return interceptor(ctx, req, info, next)
			}
		}
		return current(ctx, req)
	}
}
