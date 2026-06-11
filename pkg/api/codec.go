// Package api defines the dkv gRPC service contract without protoc.
//
// WHY NO PROTOC: In environments where protoc isn't available (some CI
// pipelines, minimal containers), you can register a custom codec with gRPC.
// This is not a hack — gRPC's codec interface is a public API. Twitch's Twirp
// and some Google-internal services use similar patterns.
//
// HOW IT WORKS:
//  1. We register a JSON codec via encoding.RegisterCodecV2 (called in init()).
//  2. We define request/response types as plain Go structs.
//  3. We build grpc.ServiceDesc manually — this is what protoc normally generates.
//  4. Clients connect with grpc.WithDefaultCallOptions(grpc.ForceCodecV2(...))
//     to tell gRPC to use our JSON codec instead of protobuf.
//
// protoc generates three things: Go structs for messages, a ServiceDesc that maps
// RPC method names to handler functions, and client stubs. All three are plain Go
// code. Building them by hand shows exactly what protoc generates under the hood.
package api

import (
	"context"
	"encoding/json"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"
)

func init() {
	// Register our JSON codec so gRPC knows how to marshal/unmarshal
	// using JSON instead of protobuf. The name "json" is the content-subtype
	// that appears in the Content-Type header: application/grpc+json
	encoding.RegisterCodecV2(JSONCodec{})
}

// --- Request/Response types (what protoc would generate as message types) ---

type PutRequest struct {
	Key   string `json:"key"`
	Value []byte `json:"value"` // base64-encoded in JSON automatically by encoding/json
}

type PutResponse struct{}

type GetRequest struct {
	Key string `json:"key"`
}

type GetResponse struct {
	Value []byte `json:"value"`
}

type DeleteRequest struct {
	Key string `json:"key"`
}

type DeleteResponse struct{}

// KVServiceServer defines the RPC surface for dkv's gRPC service.
//
// gRPC's ServiceDesc.HandlerType must point to an interface type, not a concrete
// handler struct. We keep this interface in the api package as part of the
// service contract (equivalent to what protoc-generated code provides).
type KVServiceServer interface {
	Put(context.Context, *PutRequest) (*PutResponse, error)
	Get(context.Context, *GetRequest) (*GetResponse, error)
	Delete(context.Context, *DeleteRequest) (*DeleteResponse, error)
}

// --- JSON Codec ---
//
// Implements grpc/encoding.CodecV2. JSON adds ~5-10x serialization overhead
// vs protobuf. This is the first thing to swap for production use.

type JSONCodec struct{}

func (JSONCodec) Name() string { return "json" }

func (JSONCodec) Marshal(v any) (mem.BufferSlice, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("json codec: marshal: %w", err)
	}
	return mem.BufferSlice{mem.SliceBuffer(data)}, nil
}

func (JSONCodec) Unmarshal(data mem.BufferSlice, v any) error {
	if err := json.Unmarshal(data.Materialize(), v); err != nil {
		return fmt.Errorf("json codec: unmarshal: %w", err)
	}
	return nil
}

// ServiceName is the fully qualified gRPC service name.
// Convention: "package.ServiceName" — matches what protoc would generate.
const ServiceName = "dkv.KVService"

// KVServiceClient defines the client API for KVService.
type KVServiceClient interface {
	Put(ctx context.Context, in *PutRequest, opts ...grpc.CallOption) (*PutResponse, error)
	Get(ctx context.Context, in *GetRequest, opts ...grpc.CallOption) (*GetResponse, error)
	Delete(ctx context.Context, in *DeleteRequest, opts ...grpc.CallOption) (*DeleteResponse, error)
}

type kvServiceClient struct {
	cc grpc.ClientConnInterface
}

// NewClient creates a client for KVService.
func NewClient(cc grpc.ClientConnInterface) KVServiceClient {
	return &kvServiceClient{cc: cc}
}

func (c *kvServiceClient) Put(ctx context.Context, in *PutRequest, opts ...grpc.CallOption) (*PutResponse, error) {
	out := new(PutResponse)
	err := c.cc.Invoke(ctx, "/"+ServiceName+"/Put", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *kvServiceClient) Get(ctx context.Context, in *GetRequest, opts ...grpc.CallOption) (*GetResponse, error) {
	out := new(GetResponse)
	err := c.cc.Invoke(ctx, "/"+ServiceName+"/Get", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *kvServiceClient) Delete(ctx context.Context, in *DeleteRequest, opts ...grpc.CallOption) (*DeleteResponse, error) {
	out := new(DeleteResponse)
	err := c.cc.Invoke(ctx, "/"+ServiceName+"/Delete", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// RegisterKVServiceServer registers the KVService implementation with a gRPC server.
func RegisterKVServiceServer(s grpc.ServiceRegistrar, srv KVServiceServer) {
	s.RegisterService(&grpc.ServiceDesc{
		ServiceName: ServiceName,
		HandlerType: (*KVServiceServer)(nil),
		Methods: []grpc.MethodDesc{
			{MethodName: "Put", Handler: _KVService_Put_Handler},
			{MethodName: "Get", Handler: _KVService_Get_Handler},
			{MethodName: "Delete", Handler: _KVService_Delete_Handler},
		},
		Streams:  []grpc.StreamDesc{},
		Metadata: "dkv/kvservice",
	}, srv)
}

func _KVService_Put_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PutRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(KVServiceServer).Put(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/" + ServiceName + "/Put",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(KVServiceServer).Put(ctx, req.(*PutRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _KVService_Get_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(KVServiceServer).Get(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/" + ServiceName + "/Get",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(KVServiceServer).Get(ctx, req.(*GetRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _KVService_Delete_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DeleteRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(KVServiceServer).Delete(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/" + ServiceName + "/Delete",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(KVServiceServer).Delete(ctx, req.(*DeleteRequest))
	}
	return interceptor(ctx, in, info, handler)
}
