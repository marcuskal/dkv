// Package coordinator syncs Serf membership events to both the Raft voter
// set and the hash ring / router.
//
// A membership change triggers two subsystem updates:
//  1. Raft voter set (leader only — AddVoter / RemoveServer)
//  2. Hash ring + Router (every node — so local routing stays accurate)
//
// The membership package owns Serf interaction; this package owns the
// "what do we do when membership changes?" policy. Clients with a stale ring
// get server-side forwarding as a fallback until their ring catches up.
package coordinator

import (
	"github.com/hashicorp/serf/serf"
	"github.com/rs/zerolog"

	"github.com/marcuskal/dkv/internal/hashring"
	dkvraft "github.com/marcuskal/dkv/internal/raft"
	"github.com/marcuskal/dkv/internal/router"
)

// Serf tags used to discover peer addresses.
const (
	TagRaftAddr = "raft_addr"
	TagGRPCAddr = "grpc_addr"
)

// Coordinator translates Serf membership events into Raft config changes
// and hash ring / router updates.
type Coordinator struct {
	raftNode *dkvraft.Node
	ring     *hashring.Ring
	router   *router.Router
	localID  string
	log      zerolog.Logger
}

// New creates a coordinator.
func New(
	raftNode *dkvraft.Node,
	ring *hashring.Ring,
	rtr *router.Router,
	localID string,
	log zerolog.Logger,
) *Coordinator {
	return &Coordinator{
		raftNode: raftNode,
		ring:     ring,
		router:   rtr,
		localID:  localID,
		log:      log.With().Str("component", "coordinator").Logger(),
	}
}

// HandleEvent processes a Serf event. Called by the membership package's
// event loop.
func (c *Coordinator) HandleEvent(event serf.Event) {
	switch e := event.(type) {
	case serf.MemberEvent:
		for _, member := range e.Members {
			if member.Name == c.localID {
				continue
			}

			switch e.EventType() {
			case serf.EventMemberJoin:
				c.handleJoin(member)
			case serf.EventMemberLeave, serf.EventMemberFailed:
				c.handleLeave(member)
			}
		}
	}
}

func (c *Coordinator) handleJoin(member serf.Member) {
	raftAddr := member.Tags[TagRaftAddr]
	grpcAddr := member.Tags[TagGRPCAddr]

	// Always update ring + router (every node needs an accurate ring).
	c.ring.AddNode(member.Name)
	if grpcAddr != "" {
		c.router.AddPeer(member.Name, grpcAddr)
	}
	c.log.Info().
		Str("node", member.Name).
		Str("grpc_addr", grpcAddr).
		Msg("added to ring and router")

	// Only the leader modifies Raft voter set.
	if c.raftNode != nil && c.raftNode.IsLeader() && raftAddr != "" {
		if err := c.raftNode.AddVoter(member.Name, raftAddr); err != nil {
			c.log.Error().Err(err).
				Str("node", member.Name).
				Msg("failed to add raft voter")
		}
	}
}

func (c *Coordinator) handleLeave(member serf.Member) {
	// Always update ring + router.
	c.ring.RemoveNode(member.Name)
	c.router.RemovePeer(member.Name)
	c.log.Info().Str("node", member.Name).Msg("removed from ring and router")

	// Only the leader modifies Raft voter set.
	if c.raftNode != nil && c.raftNode.IsLeader() {
		if err := c.raftNode.RemoveServer(member.Name); err != nil {
			c.log.Error().Err(err).
				Str("node", member.Name).
				Msg("failed to remove raft server")
		}
	}
}
