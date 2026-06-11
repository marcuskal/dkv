package engine

import (
	"errors"
	"fmt"
	"sync"

	"github.com/rs/zerolog"
)

// Common errors. Sentinel errors so callers can branch with errors.Is();
// wrapped errors with %w elsewhere to preserve the causal chain.
var (
	ErrKeyNotFound = errors.New("key not found")
	ErrKeyEmpty    = errors.New("key must not be empty")
	ErrClosed      = errors.New("engine is closed")
	ErrKeyTooLarge = errors.New("key too large")
	ErrValTooLarge = errors.New("value too large")
)

// Engine is the core single-node storage engine for quoll.
//
// ARCHITECTURE:
//
//	┌──────────────┐     ┌──────┐     ┌─────────────┐
//	│  Client Put() │────▶│  WAL │────▶│ In-Memory   │
//	│  Client Get() │     │(disk)│     │ Map (memtbl) │
//	└──────────────┘     └──────┘     └─────────────┘
//
// Write path: WAL append → fsync → apply to map → ACK to client
// Read path:  direct map lookup (no WAL involvement)
//
// WHY THIS ORDER: The WAL write MUST complete before the in-memory apply.
// If we applied to memory first and crashed before the WAL write, we'd have
// a phantom write — the client saw success but the data is gone after restart.
// This is the fundamental WAL invariant: "persisted before visible."
//
// CONCURRENCY: sync.RWMutex on the map.
//   - Get() takes RLock (multiple concurrent readers)
//   - Put()/Delete() take full Lock (exclusive writer)
//
// RWMutex outperforms sync.Map here: sync.Map is optimized for stable-key,
// high-read workloads; ours has high write contention with dynamic keys.
// RWMutex also lets us hold the lock across WAL+map operations atomically.
type Engine struct {
	mu     sync.RWMutex
	data   map[string][]byte
	wal    *WAL
	log    zerolog.Logger
	closed bool
}

// New creates or recovers an Engine.
// walSyncMode: "always" = fsync on every write, "none" = no fsync (faster, less durable).
func New(walDir string, walSyncMode string, maxKeySize, maxValueSize int, log zerolog.Logger) (*Engine, error) {
	syncOnWrite := walSyncMode == "always"
	return Open(walDir, syncOnWrite, 0, log)
}

// Open creates or recovers an Engine.
// If a WAL exists on disk, it replays all entries to restore state.
func Open(walDir string, syncOnWrite bool, maxSegBytes int64, log zerolog.Logger) (*Engine, error) {
	wal, err := OpenWAL(walDir, syncOnWrite, maxSegBytes)
	if err != nil {
		return nil, fmt.Errorf("engine: open wal: %w", err)
	}

	e := &Engine{
		data: make(map[string][]byte),
		wal:  wal,
		log:  log,
	}

	// Crash recovery: replay WAL into the in-memory map.
	if err := e.recover(); err != nil {
		wal.Close()
		return nil, fmt.Errorf("engine: recovery: %w", err)
	}

	return e, nil
}

// recover replays all WAL records into the in-memory map.
// Called exactly once at startup. Cost is linear in WAL entries; bounded in
// practice by periodic snapshotting + WAL truncation (see Snapshot/Restore).
func (e *Engine) recover() error {
	records, err := e.wal.ReadAll()
	if err != nil {
		return err
	}

	applied := 0
	for _, rec := range records {
		switch rec.Op {
		case OpPut:
			e.data[rec.Key] = rec.Value
			applied++
		case OpDelete:
			delete(e.data, rec.Key)
			applied++
		}
	}

	e.log.Info().
		Int("records_replayed", applied).
		Int("keys_restored", len(e.data)).
		Msg("WAL recovery complete")

	return nil
}

// Get retrieves the value for a key. Returns ErrKeyNotFound if absent.
// Thread-safe for concurrent reads.
func (e *Engine) Get(key string) ([]byte, error) {
	if key == "" {
		return nil, ErrKeyEmpty
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.closed {
		return nil, ErrClosed
	}

	val, ok := e.data[key]
	if !ok {
		return nil, ErrKeyNotFound
	}

	// Return a copy, not the slice header pointing into our map.
	// Without this, the caller could mutate the backing array and corrupt stored data.
	out := make([]byte, len(val))
	copy(out, val)
	return out, nil
}

// Put stores a key-value pair. Durable after return (WAL fsync'd).
// Thread-safe; blocks concurrent readers and writers.
func (e *Engine) Put(key string, value []byte) error {
	if key == "" {
		return ErrKeyEmpty
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return ErrClosed
	}

	// Step 1: Persist to WAL FIRST (the durability guarantee).
	if err := e.wal.Append(walRecord{Op: OpPut, Key: key, Value: value}); err != nil {
		return fmt.Errorf("engine: wal append: %w", err)
	}

	// Step 2: Apply to in-memory map.
	// Defensive copy on write too — caller might reuse the slice.
	stored := make([]byte, len(value))
	copy(stored, value)
	e.data[key] = stored

	return nil
}

// Delete removes a key. The delete is WAL'd as a tombstone.
func (e *Engine) Delete(key string) error {
	if key == "" {
		return ErrKeyEmpty
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return ErrClosed
	}

	// WAL the delete even if the key doesn't exist — idempotent and safe.
	// On recovery, delete of a non-existent key is a no-op.
	if err := e.wal.Append(walRecord{Op: OpDelete, Key: key}); err != nil {
		return fmt.Errorf("engine: wal append: %w", err)
	}

	delete(e.data, key)
	return nil
}

// Keys returns all keys currently in the store. Useful for debugging.
// Returns a snapshot — safe to iterate without holding the lock.
func (e *Engine) Keys() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()

	keys := make([]string, 0, len(e.data))
	for k := range e.data {
		keys = append(keys, k)
	}
	return keys
}

// Len returns the number of keys currently stored.
func (e *Engine) Len() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.data)
}

// Close flushes the WAL and marks the engine as closed.
// After Close(), all operations return ErrClosed.
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return nil // idempotent
	}

	e.closed = true
	return e.wal.Close()
}

// Snapshot returns a point-in-time copy of all data.
// The Raft FSM calls this during snapshot creation.
// RLock ensures no writes land between iteration start/end.
func (e *Engine) Snapshot() (map[string][]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.closed {
		return nil, ErrClosed
	}

	snap := make(map[string][]byte, len(e.data))
	for k, v := range e.data {
		cp := make([]byte, len(v))
		copy(cp, v)
		snap[k] = cp
	}
	return snap, nil
}

// Restore replaces all engine state from an external snapshot.
// The Raft FSM calls this when installing a snapshot from the leader.
func (e *Engine) Restore(data map[string][]byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return ErrClosed
	}

	e.data = data
	e.log.Info().Int("keys", len(data)).Msg("engine state restored from snapshot")
	return nil
}
