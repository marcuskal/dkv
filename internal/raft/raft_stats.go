// raft_stats.go implements observability.RaftStatsProvider so the metrics
// poller can read Raft state without importing the raft package directly.
package dkvraft

import (
	"strconv"

	"github.com/hashicorp/raft"
)

// CurrentTerm returns the current Raft term number.
// Term changes indicate leader elections — useful for correlating
// latency spikes with election events on dashboards.
//
// WHY Stats() instead of a direct field: HashiCorp's raft library
// doesn't expose the term as a public field. Stats() returns a
// map[string]string with "term" as one of the keys.
func (n *Node) CurrentTerm() uint64 {
	stats := n.raft.Stats()
	termStr, ok := stats["term"]
	if !ok {
		return 0
	}
	term, err := strconv.ParseUint(termStr, 10, 64)
	if err != nil {
		return 0
	}
	return term
}

// RaftStateInt returns the current Raft state as an integer.
// 1=follower, 2=candidate, 3=leader.
// Matches the Prometheus gauge dkv_raft_state.
func (n *Node) RaftStateInt() int {
	switch n.raft.State() {
	case raft.Follower:
		return 1
	case raft.Candidate:
		return 2
	case raft.Leader:
		return 3
	default:
		return 0
	}
}