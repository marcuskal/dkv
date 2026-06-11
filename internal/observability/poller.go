package observability

import (
	"context"
	"time"

	"github.com/rs/zerolog"
)

// StatsProvider is implemented by components that expose runtime statistics.
// The background poller calls these methods periodically and updates
// the corresponding Prometheus gauges.
//
// A background poller is used instead of a custom prometheus.Collector because
// gathering stats (counting keys under a read lock, querying Raft) should not
// happen on every /metrics scrape. The poller decouples scrape frequency from
// stat-gathering cost.
type StatsProvider interface {
	// Len returns the number of keys stored.
	Len() int
}

// RaftStatsProvider exposes Raft-specific stats.
type RaftStatsProvider interface {
	// IsLeader returns true if this node is the Raft leader.
	IsLeader() bool
	// CurrentTerm returns the current Raft term.
	CurrentTerm() uint64
	// State returns the Raft state as an int (follower=1, candidate=2, leader=3).
	RaftStateInt() int
}

// LockStatsProvider exposes lock manager stats.
type LockStatsProvider interface {
	// ActiveLockCount returns the number of currently held locks.
	ActiveLockCount() int
}

// StartStatsPoller launches a goroutine that periodically updates metrics
// from the provided stats sources. Returns a cancel function.
//
// PATTERN: This follows the standard Go "background worker with context
// cancellation" pattern. The ticker ensures bounded polling frequency.
// The context cancellation ensures clean shutdown.
func StartStatsPoller(
	ctx context.Context,
	m *Metrics,
	engine StatsProvider,
	raftProvider RaftStatsProvider,
	lockProvider LockStatsProvider,
	interval time.Duration,
	log zerolog.Logger,
) context.CancelFunc {
	ctx, cancel := context.WithCancel(ctx)

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		log.Info().Dur("interval", interval).Msg("stats poller started")

		for {
			select {
			case <-ctx.Done():
				log.Info().Msg("stats poller stopped")
				return
			case <-ticker.C:
				// Engine stats.
				if engine != nil {
					m.KeysStored.Set(float64(engine.Len()))
				}

				// Raft stats.
				if raftProvider != nil {
					m.RaftTerm.Set(float64(raftProvider.CurrentTerm()))
					m.RaftState.Set(float64(raftProvider.RaftStateInt()))
				}

				// Lock stats.
				if lockProvider != nil {
					m.LockCount.Set(float64(lockProvider.ActiveLockCount()))
				}
			}
		}
	}()

	return cancel
}