// Package hashring implements a consistent hash ring.
//
// Consistent hashing solves the "which node owns this key?" problem in a way
// that minimizes disruption when nodes join or leave the cluster.
//
// HOW IT WORKS:
//  1. Hash each node's ID to a position on a virtual circle (0..2^64).
//  2. Each physical node gets `vnodeCount` positions (virtual nodes) for balance.
//  3. To find the owner of a key: hash the key → walk clockwise → first vnode wins.
//
// WHY VIRTUAL NODES (vnodes):
//
//	Without vnodes, 3 nodes would each own exactly 1/3 of the ring — only if their
//	hash positions happen to be evenly spaced (unlikely). With 128 vnodes per node,
//	the law of large numbers kicks in and distribution is much more even.
//	DynamoDB, Cassandra, and Riak all use this technique.
package hashring

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
)

// Ring is a consistent hash ring. All methods are safe for concurrent use.
type Ring struct {
	mu         sync.RWMutex
	vnodeCount int                 // virtual nodes per physical node
	vnodes     []vnode             // sorted by hash position
	nodes      map[string]struct{} // set of physical node IDs
}

// vnode is one position on the ring.
type vnode struct {
	hash   uint64
	nodeID string
}

// New creates a ring with the given number of virtual nodes per physical node.
// Higher vnodeCount = better key distribution but more memory.
// 128–256 is a good default for most clusters.
func New(vnodeCount int) *Ring {
	if vnodeCount <= 0 {
		vnodeCount = 128
	}
	return &Ring{
		vnodeCount: vnodeCount,
		nodes:      make(map[string]struct{}),
	}
}

// Add adds a physical node to the ring, placing vnodeCount virtual nodes.
// Returns the number of vnodes added (0 if already present).
// Alias for AddNode — test API.
func (r *Ring) Add(nodeID string) int {
	return r.addNode(nodeID)
}

// AddNode adds a physical node to the ring.
func (r *Ring) AddNode(nodeID string) {
	r.addNode(nodeID)
}

func (r *Ring) addNode(nodeID string) int {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.nodes[nodeID]; exists {
		return 0
	}
	r.nodes[nodeID] = struct{}{}

	for i := 0; i < r.vnodeCount; i++ {
		h := hashKey(fmt.Sprintf("%s#%d", nodeID, i))
		r.vnodes = append(r.vnodes, vnode{hash: h, nodeID: nodeID})
	}
	sort.Slice(r.vnodes, func(i, j int) bool {
		return r.vnodes[i].hash < r.vnodes[j].hash
	})
	return r.vnodeCount
}

// Remove removes a physical node and all its virtual nodes.
// Alias for RemoveNode — test API.
func (r *Ring) Remove(nodeID string) {
	r.RemoveNode(nodeID)
}

// RemoveNode removes a physical node and all its virtual nodes.
func (r *Ring) RemoveNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.nodes[nodeID]; !exists {
		return
	}
	delete(r.nodes, nodeID)

	filtered := r.vnodes[:0]
	for _, v := range r.vnodes {
		if v.nodeID != nodeID {
			filtered = append(filtered, v)
		}
	}
	r.vnodes = filtered
}

// Lookup returns the node responsible for the given key.
// Returns ("", false) if the ring is empty.
func (r *Ring) Lookup(key string) (string, bool) {
	return r.Locate(key)
}

// Locate returns the node responsible for the given key (test API alias).
func (r *Ring) Locate(key string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.vnodes) == 0 {
		return "", false
	}

	h := hashKey(key)
	// Binary search for the first vnode with hash >= h (clockwise walk).
	idx := sort.Search(len(r.vnodes), func(i int) bool {
		return r.vnodes[i].hash >= h
	})
	// Wrap around to the first vnode if we passed the last one.
	if idx == len(r.vnodes) {
		idx = 0
	}
	return r.vnodes[idx].nodeID, true
}

// LocateN returns up to n distinct physical nodes starting from the key's position.
// Useful for replication: LocateN("key", 3) gives the primary + 2 replicas.
func (r *Ring) LocateN(key string, n int) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.vnodes) == 0 {
		return nil
	}

	// Cap at the number of physical nodes.
	if n > len(r.nodes) {
		n = len(r.nodes)
	}

	h := hashKey(key)
	idx := sort.Search(len(r.vnodes), func(i int) bool {
		return r.vnodes[i].hash >= h
	})

	seen := make(map[string]struct{}, n)
	result := make([]string, 0, n)

	for len(result) < n {
		v := r.vnodes[idx%len(r.vnodes)]
		if _, already := seen[v.nodeID]; !already {
			seen[v.nodeID] = struct{}{}
			result = append(result, v.nodeID)
		}
		idx++
	}
	return result
}

// HasNode returns true if the node is in the ring.
func (r *Ring) HasNode(nodeID string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.nodes[nodeID]
	return ok
}

// NodeCount returns the number of physical nodes.
func (r *Ring) NodeCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.nodes)
}

// Size returns the number of physical nodes (test API alias).
func (r *Ring) Size() int {
	return r.NodeCount()
}

// GetNodes returns a sorted slice of all physical node IDs.
func (r *Ring) GetNodes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	nodes := make([]string, 0, len(r.nodes))
	for n := range r.nodes {
		nodes = append(nodes, n)
	}
	sort.Strings(nodes)
	return nodes
}

// hashKey produces a uint64 hash for any string key.
// SHA-256 gives excellent avalanche — small key changes → completely different hash.
func hashKey(key string) uint64 {
	h := sha256.Sum256([]byte(key))
	return binary.BigEndian.Uint64(h[:8])
}
