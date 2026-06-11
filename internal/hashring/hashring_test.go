package hashring

import (
	"fmt"
	"math"
	"testing"
)

// TestLocateBasic verifies that Locate returns a node and is deterministic.
func TestLocateBasic(t *testing.T) {
	r := New(150)
	r.Add("node1")
	r.Add("node2")
	r.Add("node3")

	// Same key must always map to the same node (determinism).
	node1, ok := r.Locate("user:123")
	if !ok {
		t.Fatal("Locate returned false on non-empty ring")
	}
	for i := 0; i < 100; i++ {
		node, _ := r.Locate("user:123")
		if node != node1 {
			t.Fatalf("non-deterministic: got %s and %s for same key", node1, node)
		}
	}
}

// TestLocateEmptyRing verifies Locate fails gracefully on an empty ring.
func TestLocateEmptyRing(t *testing.T) {
	r := New(150)
	_, ok := r.Locate("any-key")
	if ok {
		t.Fatal("Locate should return false on empty ring")
	}
}

// TestDistribution checks that keys distribute roughly evenly across nodes.
// With 150 vnodes and 3 nodes, each should own ~33% ± reasonable deviation.
func TestDistribution(t *testing.T) {
	r := New(150)
	nodes := []string{"node1", "node2", "node3"}
	for _, n := range nodes {
		r.Add(n)
	}

	counts := make(map[string]int)
	numKeys := 100_000
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		node, _ := r.Locate(key)
		counts[node]++
	}

	expected := float64(numKeys) / float64(len(nodes))
	for _, n := range nodes {
		count := counts[n]
		deviation := math.Abs(float64(count)-expected) / expected * 100
		t.Logf("  %s: %d keys (%.1f%% deviation from ideal)", n, count, deviation)
		// With 150 vnodes, deviation should be well under 20%.
		if deviation > 20 {
			t.Errorf("%s has %.1f%% deviation — vnodes not providing enough balance", n, deviation)
		}
	}
}

// TestMinimalDisruption verifies that adding a node moves only ~1/(N+1) of
// keys, not ~N/(N+1) as with modular hashing.
func TestMinimalDisruption(t *testing.T) {
	r := New(150)
	r.Add("node1")
	r.Add("node2")
	r.Add("node3")

	// Record ownership before adding node4.
	numKeys := 100_000
	before := make(map[string]string, numKeys)
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		node, _ := r.Locate(key)
		before[key] = node
	}

	// Add a 4th node.
	r.Add("node4")

	// Count how many keys moved.
	moved := 0
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		node, _ := r.Locate(key)
		if node != before[key] {
			moved++
		}
	}

	// With modular hashing (hash%N), ~75% would move (3→4 nodes).
	// With consistent hashing, ~25% should move (1/4 of keys go to node4).
	movedPct := float64(moved) / float64(numKeys) * 100
	t.Logf("  Keys moved: %d / %d (%.1f%%)", moved, numKeys, movedPct)

	// Allow up to 35% — some variance from vnodes, but way below the 75% modular threshold.
	if movedPct > 35 {
		t.Errorf("too many keys moved: %.1f%% — consistent hashing should move ~25%%", movedPct)
	}
	// Also check we're not moving too few (sanity — at least some keys should move).
	if movedPct < 10 {
		t.Errorf("suspiciously few keys moved: %.1f%% — node4 might not be on the ring", movedPct)
	}
}

// TestRemoveNode verifies that removing a node redistributes only its keys.
func TestRemoveNode(t *testing.T) {
	r := New(150)
	r.Add("node1")
	r.Add("node2")
	r.Add("node3")

	numKeys := 10_000
	before := make(map[string]string, numKeys)
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		node, _ := r.Locate(key)
		before[key] = node
	}

	r.Remove("node2")

	moved := 0
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		node, _ := r.Locate(key)
		if node != before[key] {
			moved++
			// Keys that moved must have been on node2 before.
			if before[key] != "node2" {
				t.Errorf("key %s moved from %s to %s — should only move FROM removed node", key, before[key], node)
			}
		}
	}
	t.Logf("  Keys moved after removing node2: %d / %d", moved, numKeys)
}

// TestLocateN verifies multi-node lookup for replication.
func TestLocateN(t *testing.T) {
	r := New(150)
	r.Add("node1")
	r.Add("node2")
	r.Add("node3")

	nodes := r.LocateN("test-key", 3)
	if len(nodes) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(nodes))
	}

	// All nodes should be distinct.
	seen := make(map[string]bool)
	for _, n := range nodes {
		if seen[n] {
			t.Errorf("duplicate node in LocateN result: %s", n)
		}
		seen[n] = true
	}
}

// TestLocateNCapped verifies LocateN caps at the number of physical nodes.
func TestLocateNCapped(t *testing.T) {
	r := New(150)
	r.Add("node1")
	r.Add("node2")

	// Asking for 5 nodes when only 2 exist should return 2.
	nodes := r.LocateN("test-key", 5)
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes (capped), got %d", len(nodes))
	}
}

// TestAddIdempotent verifies adding the same node twice is a no-op.
func TestAddIdempotent(t *testing.T) {
	r := New(150)
	added1 := r.Add("node1")
	added2 := r.Add("node1")

	if added2 != 0 {
		t.Errorf("second Add should return 0, got %d", added2)
	}
	if r.Size() != 1 {
		t.Errorf("expected 1 node, got %d", r.Size())
	}
	_ = added1
}