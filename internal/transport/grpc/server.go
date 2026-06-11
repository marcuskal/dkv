// Package server implements the gRPC transport layer for QUOLL.
package server

import (
	"context"
	"errors"
	"fmt"
	"net"

	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/marcuskal/quoll/internal/config"
	"github.com/marcuskal/quoll/internal/engine"
	"github.com/marcuskal/quoll/internal/observability"
	quollraft "github.com/marcuskal/quoll/internal/raft"
	"github.com/marcuskal/quoll/internal/router"
	inttls "github.com/marcuskal/quoll/internal/tls"
	v1 "github.com/marcuskal/quoll/pkg/api"
)

// Server wraps the gRPC server with the QUOLL service implementation.
type Server struct {
	grpcServer *grpc.Server
	engine     *engine.Engine
	raftNode   *quollraft.Node
	router     *router.Router
	localID    string
	listener   net.Listener
	log        zerolog.Logger
	cfg        config.Config
	metrics    *observability.Metrics
	tracer     trace.Tracer
}

// New creates a new QUOLL gRPC server.
// Interceptor chain: PanicRecovery → Observability → Timeout.
func New(
	eng *engine.Engine,
	raftNode *quollraft.Node,
	rtr *router.Router,
	localID string,
	cfg config.Config,
	log zerolog.Logger,
	metrics *observability.Metrics,
	tracer trace.Tracer,
) (*Server, error) {
	log = log.With().Str("component", "grpc-server").Logger()

	if tracer == nil {
		tracer = noop.NewTracerProvider().Tracer("noop")
	}

	interceptors := []grpc.UnaryServerInterceptor{
		PanicRecoveryInterceptor(log),
	}

	if metrics != nil {
		interceptors = append(interceptors, observability.UnaryServerInterceptor(metrics, tracer, log))
	}

	interceptors = append(interceptors, TimeoutInterceptor(cfg.GRPC.RequestTimeout))

	// TLS credentials.
	var creds credentials.TransportCredentials
	if cfg.TLS.Enabled {
		tlsCfg, err := inttls.ServerTLSConfig(inttls.Config{
			CertFile: cfg.TLS.CertFile,
			KeyFile:  cfg.TLS.KeyFile,
			CAFile:   cfg.TLS.CAFile,
		})
		if err != nil {
			return nil, fmt.Errorf("TLS config: %w", err)
		}
		creds = credentials.NewTLS(tlsCfg)
	}

	var opts []grpc.ServerOption
	if creds != nil {
		opts = append(opts, grpc.Creds(creds))
	}
	opts = append(opts, grpc.ChainUnaryInterceptor(interceptors...))

	grpcServer := grpc.NewServer(opts...)

	srv := &Server{
		grpcServer: grpcServer,
		engine:     eng,
		raftNode:   raftNode,
		router:     rtr,
		localID:    localID,
		log:        log,
		cfg:        cfg,
		metrics:    metrics,
		tracer:     tracer,
	}

	// Register the KV service.
	handler := NewKVHandler(eng, raftNode, rtr, localID, log, tracer)
	grpcServer.RegisterService(handler.ServiceDesc(), handler)

	return srv, nil
}

// Serve starts listening on the configured address.
func (s *Server) Serve() error {
	lis, err := net.Listen("tcp", s.cfg.GRPC.ListenAddr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	s.listener = lis
	s.log.Info().Str("addr", s.cfg.GRPC.ListenAddr).Msg("gRPC server listening")
	return s.grpcServer.Serve(lis)
}

// ServeOnListener starts the server on a provided listener (for tests/bufconn).
func (s *Server) ServeOnListener(lis net.Listener) error {
	s.listener = lis
	return s.grpcServer.Serve(lis)
}

// GracefulStop drains in-flight RPCs then stops.
func (s *Server) GracefulStop() {
	s.grpcServer.GracefulStop()
}

// --- KV Handler ---

// KVHandler implements the QUOLL gRPC service.
type KVHandler struct {
	engine   *engine.Engine
	raftNode *quollraft.Node
	router   *router.Router
	localID  string
	log      zerolog.Logger
	tracer   trace.Tracer
}

// NewKVHandler creates a handler.
func NewKVHandler(eng *engine.Engine, raftNode *quollraft.Node, rtr *router.Router, localID string, log zerolog.Logger, tracer trace.Tracer) *KVHandler {
	return &KVHandler{
		engine:   eng,
		raftNode: raftNode,
		router:   rtr,
		localID:  localID,
		log:      log,
		tracer:   tracer,
	}
}

// Put handles the Put RPC.
func (h *KVHandler) Put(ctx context.Context, req *v1.PutRequest) (*v1.PutResponse, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}

	// In single-Raft-group mode all writes must reach the leader; skip ring
	// routing for writes. Hash-ring forwarding applies only in non-Raft mode.
	if h.raftNode == nil && h.router != nil && !h.router.IsLocalKey(req.Key) {
		return h.forwardPut(ctx, req)
	}

	// Must go through Raft if in cluster mode.
	if h.raftNode != nil {
		if !h.raftNode.IsLeader() {
			leader := h.raftNode.LeaderAddr()
			if leader == "" {
				return nil, status.Error(codes.Unavailable, "no leader elected")
			}
			return nil, status.Errorf(codes.Unavailable, "not leader; leader is %s", leader)
		}

		_, span := h.tracer.Start(ctx, "raft.apply.put",
			trace.WithAttributes(attribute.String("key", req.Key)),
		)

		err := h.raftNode.Apply(quollraft.Command{
			Type:  quollraft.CmdPut,
			Key:   req.Key,
			Value: req.Value,
		}, h.cfg().GRPC.RequestTimeout)

		span.End()

		if err != nil {
			return nil, toGRPCError(err)
		}
		return &v1.PutResponse{}, nil
	}

	// Single-node mode: write directly.
	_, span := h.tracer.Start(ctx, "engine.put",
		trace.WithAttributes(attribute.String("key", req.Key)),
	)
	err := h.engine.Put(req.Key, req.Value)
	span.End()

	if err != nil {
		return nil, toGRPCError(err)
	}
	return &v1.PutResponse{}, nil
}

// Get handles the Get RPC.
func (h *KVHandler) Get(ctx context.Context, req *v1.GetRequest) (*v1.GetResponse, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}

	if h.raftNode == nil && h.router != nil && !h.router.IsLocalKey(req.Key) {
		return h.forwardGet(ctx, req)
	}

	_, span := h.tracer.Start(ctx, "engine.get",
		trace.WithAttributes(attribute.String("key", req.Key)),
	)
	val, err := h.engine.Get(req.Key)
	span.End()

	if err != nil {
		return nil, toGRPCError(err)
	}
	return &v1.GetResponse{Value: val}, nil
}

// Delete handles the Delete RPC.
func (h *KVHandler) Delete(ctx context.Context, req *v1.DeleteRequest) (*v1.DeleteResponse, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}

	if h.raftNode == nil && h.router != nil && !h.router.IsLocalKey(req.Key) {
		return h.forwardDelete(ctx, req)
	}

	if h.raftNode != nil {
		if !h.raftNode.IsLeader() {
			leader := h.raftNode.LeaderAddr()
			if leader == "" {
				return nil, status.Error(codes.Unavailable, "no leader elected")
			}
			return nil, status.Errorf(codes.Unavailable, "not leader; leader is %s", leader)
		}

		_, span := h.tracer.Start(ctx, "raft.apply.delete",
			trace.WithAttributes(attribute.String("key", req.Key)),
		)
		err := h.raftNode.Apply(quollraft.Command{
			Type: quollraft.CmdDelete,
			Key:  req.Key,
		}, h.cfg().GRPC.RequestTimeout)
		span.End()

		if err != nil {
			return nil, toGRPCError(err)
		}
		return &v1.DeleteResponse{}, nil
	}

	_, span := h.tracer.Start(ctx, "engine.delete",
		trace.WithAttributes(attribute.String("key", req.Key)),
	)
	err := h.engine.Delete(req.Key)
	span.End()

	if err != nil {
		return nil, toGRPCError(err)
	}
	return &v1.DeleteResponse{}, nil
}

// cfg returns the server config for timeout values.
func (h *KVHandler) cfg() config.Config {
	return config.Config{GRPC: config.GRPCConfig{RequestTimeout: 5 * 1e9}} // 5s default
}

// --- Forwarding ---

func (h *KVHandler) forwardPut(ctx context.Context, req *v1.PutRequest) (*v1.PutResponse, error) {
	return h.router.ForwardPut(ctx, req.Key, req)
}

func (h *KVHandler) forwardGet(ctx context.Context, req *v1.GetRequest) (*v1.GetResponse, error) {
	return h.router.ForwardGet(ctx, req.Key, req)
}

func (h *KVHandler) forwardDelete(ctx context.Context, req *v1.DeleteRequest) (*v1.DeleteResponse, error) {
	return h.router.ForwardDelete(ctx, req.Key, req)
}

// --- Error mapping ---

func toGRPCError(err error) error {
	switch {
	case errors.Is(err, engine.ErrKeyNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, engine.ErrClosed):
		return status.Error(codes.Unavailable, "service is shutting down")
	case errors.Is(err, engine.ErrKeyEmpty):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, quollraft.ErrNotLeader):
		return status.Error(codes.Unavailable, "not the leader")
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

// --- Service descriptor ---

func (h *KVHandler) ServiceDesc() *grpc.ServiceDesc {
	return &grpc.ServiceDesc{
		ServiceName: v1.ServiceName,
		HandlerType: (*v1.KVServiceServer)(nil),
		Methods: []grpc.MethodDesc{
			{MethodName: "Put", Handler: h.makePutHandler()},
			{MethodName: "Get", Handler: h.makeGetHandler()},
			{MethodName: "Delete", Handler: h.makeDeleteHandler()},
		},
		Streams: []grpc.StreamDesc{},
	}
}

func (h *KVHandler) makePutHandler() func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	return func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
		req := &v1.PutRequest{}
		if err := dec(req); err != nil {
			return nil, status.Error(codes.InvalidArgument, "failed to decode request")
		}
		if interceptor == nil {
			return h.Put(ctx, req)
		}
		info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/" + v1.ServiceName + "/Put"}
		return interceptor(ctx, req, info, func(ctx context.Context, req any) (any, error) {
			return h.Put(ctx, req.(*v1.PutRequest))
		})
	}
}

func (h *KVHandler) makeGetHandler() func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	return func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
		req := &v1.GetRequest{}
		if err := dec(req); err != nil {
			return nil, status.Error(codes.InvalidArgument, "failed to decode request")
		}
		if interceptor == nil {
			return h.Get(ctx, req)
		}
		info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/" + v1.ServiceName + "/Get"}
		return interceptor(ctx, req, info, func(ctx context.Context, req any) (any, error) {
			return h.Get(ctx, req.(*v1.GetRequest))
		})
	}
}

func (h *KVHandler) makeDeleteHandler() func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	return func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
		req := &v1.DeleteRequest{}
		if err := dec(req); err != nil {
			return nil, status.Error(codes.InvalidArgument, "failed to decode request")
		}
		if interceptor == nil {
			return h.Delete(ctx, req)
		}
		info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/" + v1.ServiceName + "/Delete"}
		return interceptor(ctx, req, info, func(ctx context.Context, req any) (any, error) {
			return h.Delete(ctx, req.(*v1.DeleteRequest))
		})
	}
}
