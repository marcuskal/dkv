// Package client provides a smart KV client with:
//   - Client-side consistent hashing (routes directly to the owning node)
//   - Connection pooling (one gRPC connection per node, reused)
//   - Exponential backoff with full jitter on retries
//   - Per-node circuit breakers (stop sending to a node that's failing)
//
// WHY CLIENT-SIDE ROUTING:
// Without it, every request goes through a random node, which then forwards
// to the correct owner (~50% of requests need a forward). With client-side
// routing, the client's hash ring directs requests to the owner directly,
// reducing latency by eliminating the forwarding hop.
//
// The server-side router (internal/router) is the fallback for when the
// client's ring is stale during membership changes.
package client

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/marcuskal/dkv/internal/hashring"
	v1 "github.com/marcuskal/dkv/pkg/api"
)

var (
	ErrCircuitOpen = errors.New("circuit breaker open")
	ErrNoNodes     = errors.New("no nodes available")
)

// Config for the smart client.
type Config struct {
	// Nodes is the initial set of node addresses (nodeID → gRPC address).
	Nodes map[string]string

	// VnodeCount for the client-side hash ring.
	VnodeCount int

	// Retry settings.
	MaxRetries  int
	BaseBackoff time.Duration // Starting backoff (e.g., 50ms)
	MaxBackoff  time.Duration // Cap (e.g., 2s)

	// Circuit breaker settings.
	CBFailThreshold int           // failures before opening (e.g., 5)
	CBResetTimeout  time.Duration // how long to wait before half-open (e.g., 10s)
}

// DefaultConfig returns sensible defaults.
func DefaultConfig() Config {
	return Config{
		Nodes:           make(map[string]string),
		VnodeCount:      128,
		MaxRetries:      3,
		BaseBackoff:     50 * time.Millisecond,
		MaxBackoff:      2 * time.Second,
		CBFailThreshold: 5,
		CBResetTimeout:  10 * time.Second,
	}
}

// Client is a smart, routing-aware KV client.
type Client struct {
	mu       sync.RWMutex
	ring     *hashring.Ring
	conns    map[string]*grpc.ClientConn // nodeID → connection
	addrs    map[string]string           // nodeID → gRPC address
	breakers map[string]*circuitBreaker  // nodeID → circuit breaker
	cfg      Config
	log      zerolog.Logger
}

// New creates a smart client and connects to all known nodes.
func New(cfg Config, log zerolog.Logger) (*Client, error) {
	log = log.With().Str("component", "client").Logger()

	ring := hashring.New(cfg.VnodeCount)
	c := &Client{
		ring:     ring,
		conns:    make(map[string]*grpc.ClientConn),
		addrs:    make(map[string]string),
		breakers: make(map[string]*circuitBreaker),
		cfg:      cfg,
		log:      log,
	}

	for nodeID, addr := range cfg.Nodes {
		if err := c.AddNode(nodeID, addr); err != nil {
			c.Close()
			return nil, fmt.Errorf("connect to %s: %w", nodeID, err)
		}
	}

	return c, nil
}

// AddNode adds a node to the client's ring and connection pool.
func (c *Client) AddNode(nodeID, addr string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.conns[nodeID]; exists {
		return nil
	}

	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.CallContentSubtype("json")),
	)
	if err != nil {
		return fmt.Errorf("dial %s: %w", addr, err)
	}

	c.conns[nodeID] = conn
	c.addrs[nodeID] = addr
	c.breakers[nodeID] = newCircuitBreaker(c.cfg.CBFailThreshold, c.cfg.CBResetTimeout)
	c.ring.AddNode(nodeID)

	c.log.Info().Str("node", nodeID).Str("addr", addr).Msg("node added")
	return nil
}

// RemoveNode removes a node from the ring and closes its connection.
func (c *Client) RemoveNode(nodeID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.ring.RemoveNode(nodeID)
	if conn, ok := c.conns[nodeID]; ok {
		conn.Close()
		delete(c.conns, nodeID)
	}
	delete(c.addrs, nodeID)
	delete(c.breakers, nodeID)

	c.log.Info().Str("node", nodeID).Msg("node removed")
}

// Put writes a key-value pair, routing to the correct node.
func (c *Client) Put(ctx context.Context, key string, value []byte) error {
	return c.withRetry(ctx, key, func(client v1.KVServiceClient) error {
		_, err := client.Put(ctx, &v1.PutRequest{Key: key, Value: value})
		return err
	})
}

// Get reads a value by key, routing to the correct node.
func (c *Client) Get(ctx context.Context, key string) ([]byte, error) {
	var result []byte
	err := c.withRetry(ctx, key, func(client v1.KVServiceClient) error {
		resp, err := client.Get(ctx, &v1.GetRequest{Key: key})
		if err != nil {
			return err
		}
		result = resp.Value
		return nil
	})
	return result, err
}

// Delete removes a key, routing to the correct node.
func (c *Client) Delete(ctx context.Context, key string) error {
	return c.withRetry(ctx, key, func(client v1.KVServiceClient) error {
		_, err := client.Delete(ctx, &v1.DeleteRequest{Key: key})
		return err
	})
}

// withRetry executes an RPC with exponential backoff and circuit breaking.
//
// RETRY LOGIC:
//  1. Hash key → find owner node
//  2. Check circuit breaker for that node
//  3. Execute RPC
//  4. On success: record success on breaker, return
//  5. On retryable failure (Unavailable, DeadlineExceeded): record failure,
//     backoff with full jitter, retry
//  6. On non-retryable failure (NotFound, InvalidArgument): return immediately
//
// WHY FULL JITTER:
// Exponential backoff alone causes "thundering herd" — all clients retry at
// the same time. Full jitter randomizes the backoff: sleep = rand(0, min(cap, base*2^attempt)).
// This spreads retries across the backoff window.
// See: https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
func (c *Client) withRetry(ctx context.Context, key string, fn func(v1.KVServiceClient) error) error {
	var lastErr error

	for attempt := 0; attempt <= c.cfg.MaxRetries; attempt++ {
		if attempt > 0 {
			backoff := c.computeBackoff(attempt)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
		}

		nodeID, ok := c.ring.Lookup(key)
		if !ok {
			return ErrNoNodes
		}

		client, cb, err := c.getClientForNode(nodeID)
		if err != nil {
			lastErr = err
			continue
		}

		// Circuit breaker check.
		if !cb.Allow() {
			lastErr = fmt.Errorf("node %s: %w", nodeID, ErrCircuitOpen)
			continue
		}

		err = fn(client)
		if err == nil {
			cb.RecordSuccess()
			return nil
		}

		lastErr = err

		// Check if error is retryable.
		if !isRetryable(err) {
			return err
		}

		cb.RecordFailure()
		c.log.Warn().Err(err).
			Str("key", key).
			Str("node", nodeID).
			Int("attempt", attempt+1).
			Msg("retryable error")
	}

	return fmt.Errorf("max retries exceeded: %w", lastErr)
}

func (c *Client) getClientForNode(nodeID string) (v1.KVServiceClient, *circuitBreaker, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	conn, ok := c.conns[nodeID]
	if !ok {
		return nil, nil, fmt.Errorf("no connection to node %s", nodeID)
	}

	cb, ok := c.breakers[nodeID]
	if !ok {
		return nil, nil, fmt.Errorf("no circuit breaker for node %s", nodeID)
	}

	return v1.NewClient(conn), cb, nil
}

// computeBackoff calculates exponential backoff with full jitter.
// backoff = rand(0, min(maxBackoff, baseBackoff * 2^attempt))
func (c *Client) computeBackoff(attempt int) time.Duration {
	exp := math.Pow(2, float64(attempt))
	maxDelay := float64(c.cfg.BaseBackoff) * exp
	if maxDelay > float64(c.cfg.MaxBackoff) {
		maxDelay = float64(c.cfg.MaxBackoff)
	}
	// Full jitter: uniform random in [0, maxDelay).
	return time.Duration(rand.Int63n(int64(maxDelay)))
}

// isRetryable returns true for errors that might succeed on retry.
func isRetryable(err error) bool {
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	switch st.Code() {
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted:
		return true
	default:
		return false
	}
}

// Close shuts down all connections.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for id, conn := range c.conns {
		conn.Close()
		delete(c.conns, id)
	}
	return nil
}

// --- Circuit Breaker ---
//
// States: CLOSED (normal) → OPEN (failing) → HALF-OPEN (testing)
//
// CLOSED:  Requests pass through. Track consecutive failures.
//          If failures >= threshold → switch to OPEN.
// OPEN:    All requests rejected immediately (fail-fast).
//          After resetTimeout → switch to HALF-OPEN.
// HALF-OPEN: Allow ONE request through.
//          If it succeeds → CLOSED (reset failures).
//          If it fails → back to OPEN (restart timer).
//
// WHY PER-NODE BREAKERS: If one node is down, we don't want retries
// piling up on it. The breaker stops sending traffic until the node
// recovers, letting the system degrade gracefully.

type cbState int

const (
	cbClosed cbState = iota
	cbOpen
	cbHalfOpen
)

type circuitBreaker struct {
	mu            sync.Mutex
	state         cbState
	failures      int
	failThreshold int
	resetTimeout  time.Duration
	lastFailTime  time.Time
}

func newCircuitBreaker(threshold int, resetTimeout time.Duration) *circuitBreaker {
	return &circuitBreaker{
		state:         cbClosed,
		failThreshold: threshold,
		resetTimeout:  resetTimeout,
	}
}

// Allow returns true if a request should be attempted.
func (cb *circuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case cbClosed:
		return true
	case cbOpen:
		// Check if enough time has passed to try again.
		if time.Since(cb.lastFailTime) > cb.resetTimeout {
			cb.state = cbHalfOpen
			return true
		}
		return false
	case cbHalfOpen:
		// Only one request allowed in half-open.
		return true
	}
	return false
}

func (cb *circuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failures = 0
	cb.state = cbClosed
}

func (cb *circuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failures++
	cb.lastFailTime = time.Now()

	if cb.failures >= cb.failThreshold {
		cb.state = cbOpen
	}
}
