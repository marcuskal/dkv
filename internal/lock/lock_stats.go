// lock_stats.go implements observability.LockStatsProvider.
package lock

// ActiveLockCount returns the number of currently held locks.
// Called by the observability poller to update the dkv_locks_held gauge.
func (m *Manager) ActiveLockCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.locks)
}