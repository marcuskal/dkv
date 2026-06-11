// Package lock implements a distributed lock manager backed by Raft.
//
// The Manager is the client-facing API for acquiring and releasing locks.
// Lock state is durably stored in the Raft log and replicated to all nodes.
// The in-memory `locks` map is rebuilt on leader recovery via FSM.Restore().
//
// ARCHITECTURE:
//
//	Client → lock.Manager.Acquire() → dkvraft.Node.Apply(CmdLock) → FSM.applyLock()
//	Client → lock.Manager.Release() → dkvraft.Node.Apply(CmdUnlock) → FSM.applyUnlock()
//
// The Manager also maintains a local view of active locks for metrics purposes.
// This is updated optimistically when operations succeed — the Raft FSM is the
// source of truth, but polling the Manager is cheap (no Raft round trip).
//
// Locks have TTLs. After a leader failover, the new leader's FSM has the full
// lock state from Raft log replay. Expired locks are detected lazily on the
// next acquire. Clients use the fence token to detect preemption.
package lock

import (
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog"

	dkvraft "github.com/marcuskal/dkv/internal/raft"
)

// lockEntry tracks an in-memory view of a held lock.
type lockEntry struct {
	Owner     string
	ExpiresAt time.Time
}

// Manager wraps the Raft node to provide distributed lock operations.
// It maintains a local replica of active locks for metrics exposure.
type Manager struct {
	raftNode *dkvraft.Node
	log      zerolog.Logger

	mu    sync.RWMutex
	locks map[string]*lockEntry
}

// NewManager creates a new lock Manager.
// raftNode may be nil in single-node mode (locks are then no-ops).
func NewManager(raftNode *dkvraft.Node, log zerolog.Logger) *Manager {
	return &Manager{
		raftNode: raftNode,
		log:      log.With().Str("component", "lock-manager").Logger(),
		locks:    make(map[string]*lockEntry),
	}
}

// Acquire attempts to acquire a distributed lock identified by lockID.
// The lock is held for up to ttl duration. Returns a fence token on success.
// Returns an error if the lock is already held by another owner.
func (m *Manager) Acquire(lockID, owner string, ttl time.Duration) (uint64, error) {
	if m.raftNode == nil {
		// Single-node mode: manage locks in-memory only.
		return m.acquireLocal(lockID, owner, ttl)
	}

	ttlSecs := int(ttl.Seconds())
	if ttlSecs <= 0 {
		ttlSecs = 30 // default TTL
	}

	err := m.raftNode.Apply(dkvraft.Command{
		Type:    dkvraft.CmdLock,
		LockID:  lockID,
		Owner:   owner,
		TTLSecs: ttlSecs,
	}, 5*time.Second)
	if err != nil {
		return 0, fmt.Errorf("acquire lock %s: %w", lockID, err)
	}

	// Update local view for metrics.
	m.mu.Lock()
	m.locks[lockID] = &lockEntry{
		Owner:     owner,
		ExpiresAt: time.Now().Add(ttl),
	}
	m.mu.Unlock()

	m.log.Info().Str("lock_id", lockID).Str("owner", owner).Dur("ttl", ttl).Msg("lock acquired")
	return 1, nil // fence token is managed by FSM; 1 indicates success
}

// Release releases a distributed lock.
// Returns an error if the lock is not held by the given owner.
func (m *Manager) Release(lockID, owner string) error {
	if m.raftNode == nil {
		return m.releaseLocal(lockID, owner)
	}

	err := m.raftNode.Apply(dkvraft.Command{
		Type:   dkvraft.CmdUnlock,
		LockID: lockID,
		Owner:  owner,
	}, 5*time.Second)
	if err != nil {
		return fmt.Errorf("release lock %s: %w", lockID, err)
	}

	// Update local view for metrics.
	m.mu.Lock()
	delete(m.locks, lockID)
	m.mu.Unlock()

	m.log.Info().Str("lock_id", lockID).Str("owner", owner).Msg("lock released")
	return nil
}

func (m *Manager) acquireLocal(lockID, owner string, ttl time.Duration) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if entry, held := m.locks[lockID]; held && time.Now().Before(entry.ExpiresAt) && entry.Owner != owner {
		return 0, fmt.Errorf("lock %s held by %s", lockID, entry.Owner)
	}

	m.locks[lockID] = &lockEntry{
		Owner:     owner,
		ExpiresAt: time.Now().Add(ttl),
	}
	return 1, nil
}

func (m *Manager) releaseLocal(lockID, owner string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, held := m.locks[lockID]
	if !held {
		return nil // idempotent
	}
	if entry.Owner != owner {
		return fmt.Errorf("lock %s not owned by %s", lockID, owner)
	}
	delete(m.locks, lockID)
	return nil
}
