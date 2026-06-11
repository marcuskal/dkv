// Package router provides server-side request forwarding.
//
// WHY SERVER-SIDE FORWARDING:
// Even with a smart client that does client-side routing (pkg/client),
// the ring view can be momentarily stale — a node just joined/left and
// the client hasn't updated yet. When a request arrives at the wrong node,
// the server-side router forwards it to the correct owner instead of
// returning an error. This is a fallback, not the primary routing path.
//
// Design: proxy pattern. The router holds gRPC connections to all known
// peers. When the local node isn't the owner, it forwards the request
// and returns the result transparently to the caller.
package router

import (
	"context"
	"fmt"
	"sync"

	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/marcuskal/dkv/internal/hashring"
	v1 "github.com/marcuskal/dkv/pkg/api"
)

// Router manages peer connections and forwards requests to the correct node.
type Router struct {
	mu        sync.RWMutex
	ring      *hashring.Ring
	localID   string
	peers     map[string]*grpc.ClientConn // nodeID → gRPC connection
	peerAddrs map[string]string           // nodeID → gRPC address
	log       zerolog.Logger
}

// New creates a router. localID is this node's ID in the hash ring.
func New(ring *hashring.Ring, localID string, log zerolog.Logger) *Router {
	return &Router{
		ring:      ring,
		localID:   localID,
		peers:     make(map[string]*grpc.ClientConn),
		peerAddrs: make(map[string]string),
		log:       log.With().Str("component", "router").Logger(),
	}
}

// AddPeer registers a peer's gRPC address. Connection is established lazily
// on first forward to minimize startup time.
func (r *Router) AddPeer(nodeID, grpcAddr string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if nodeID == r.localID {
		return // don't connect to ourselves
	}

	r.peerAddrs[nodeID] = grpcAddr
	r.log.Info().Str("node", nodeID).Str("addr", grpcAddr).Msg("peer registered")
}

// RemovePeer removes a peer and closes its connection.
func (r *Router) RemovePeer(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if conn, ok := r.peers[nodeID]; ok {
		conn.Close()
		delete(r.peers, nodeID)
	}
	delete(r.peerAddrs, nodeID)
	r.log.Info().Str("node", nodeID).Msg("peer removed")
}

// IsLocalKey checks if the given key belongs to this node according to the ring.
func (r *Router) IsLocalKey(key string) bool {
	owner, ok := r.ring.Lookup(key)
	if !ok {
		// Empty ring — treat as local (single-node fallback).
		return true
	}
	return owner == r.localID
}

// OwnerOf returns the node ID that owns the given key.
func (r *Router) OwnerOf(key string) (string, bool) {
	return r.ring.Lookup(key)
}

// ForwardPut forwards a Put request to the correct node.
func (r *Router) ForwardPut(ctx context.Context, key string, req *v1.PutRequest) (*v1.PutResponse, error) { //nolint
	owner, ok := r.ring.Lookup(key)
	if !ok {
		return nil, fmt.Errorf("no nodes in ring")
	}

	client, err := r.getClient(owner)
	if err != nil {
		return nil, fmt.Errorf("connect to %s: %w", owner, err)
	}

	r.log.Debug().Str("key", key).Str("forward_to", owner).Msg("forwarding put")
	return client.Put(ctx, req)
}

// ForwardGet forwards a Get request to the correct node.
func (r *Router) ForwardGet(ctx context.Context, key string, req *v1.GetRequest) (*v1.GetResponse, error) {
	owner, ok := r.ring.Lookup(key)
	if !ok {
		return nil, fmt.Errorf("no nodes in ring")
	}

	client, err := r.getClient(owner)
	if err != nil {
		return nil, fmt.Errorf("connect to %s: %w", owner, err)
	}

	r.log.Debug().Str("key", key).Str("forward_to", owner).Msg("forwarding get")
	return client.Get(ctx, req)
}

// ForwardDelete forwards a Delete request to the correct node.
func (r *Router) ForwardDelete(ctx context.Context, key string, req *v1.DeleteRequest) (*v1.DeleteResponse, error) {
	owner, ok := r.ring.Lookup(key)
	if !ok {
		return nil, fmt.Errorf("no nodes in ring")
	}

	client, err := r.getClient(owner)
	if err != nil {
		return nil, fmt.Errorf("connect to %s: %w", owner, err)
	}

	r.log.Debug().Str("key", key).Str("forward_to", owner).Msg("forwarding delete")
	return client.Delete(ctx, req)
}

// getClient returns a cached gRPC client for a peer, connecting lazily.
func (r *Router) getClient(nodeID string) (v1.KVServiceClient, error) {
	r.mu.RLock()
	conn, hasConn := r.peers[nodeID]
	addr, hasAddr := r.peerAddrs[nodeID]
	r.mu.RUnlock()

	if hasConn {
		return v1.NewClient(conn), nil
	}

	if !hasAddr {
		return nil, fmt.Errorf("unknown peer: %s", nodeID)
	}

	// Lazy connect — upgrade to write lock.
	r.mu.Lock()
	defer r.mu.Unlock()

	// Double-check after acquiring write lock (another goroutine may have connected).
	if conn, ok := r.peers[nodeID]; ok {
		return v1.NewClient(conn), nil
	}

	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.CallContentSubtype("json")),
	)
	if err != nil {
		return nil, fmt.Errorf("dial %s at %s: %w", nodeID, addr, err)
	}

	r.peers[nodeID] = conn
	r.log.Info().Str("node", nodeID).Str("addr", addr).Msg("peer connection established")
	return v1.NewClient(conn), nil
}

// Shutdown closes all peer connections.
func (r *Router) Shutdown() {
	r.mu.Lock()
	defer r.mu.Unlock()

	for id, conn := range r.peers {
		conn.Close()
		r.log.Debug().Str("node", id).Msg("peer connection closed")
	}
	r.peers = make(map[string]*grpc.ClientConn)
	r.peerAddrs = make(map[string]string)
}
