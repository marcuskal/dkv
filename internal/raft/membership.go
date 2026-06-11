//go:build ignore
// +build ignore

// Package membership manages cluster membership via Serf (gossip protocol).
//
// ARCHITECTURE:
//
//	Serf (gossip)  ──join/leave events──▶  Handler  ──AddVoter/RemoveServer──▶  Raft
//
// Why Serf + Raft (two separate protocols):
//
//	Raft manages CONSENSUS among a known, fixed set of voters.
//	Serf manages DISCOVERY — detecting when nodes join, leave, or fail.
//
//	Raft's reconfiguration (AddVoter/RemoveServer) requires a leader and a
//	committed log entry. Serf answers "who is in the cluster?" using a gossip
//	protocol (SWIM) that scales to thousands of nodes with O(log N) convergence
//	time and no leader requirement. Same pattern used by HashiCorp Consul.
package membership

import (
	"fmt"
	"net"

	"github.com/hashicorp/serf/serf"
	"github.com/rs/zerolog"

	"github.com/marcuskal/quoll/internal/config"
	quollraft "github.com/marcuskal/quoll/internal/raft"
)

// Tags attached to each Serf member. Other nodes read these to discover
// the Raft address for AddVoter calls.
const (
	TagRaftAddr = "raft_addr"
	TagRole     = "role"
)

// Membership manages the Serf agent and translates membership events
// into Raft configuration changes.
type Membership struct {
	serf   *serf.Serf
	node   *quollraft.Node
	events chan serf.Event
	log    zerolog.Logger
	nodeID string
}

// New creates and starts a Serf agent.
// The agent immediately begins gossiping with seed nodes (cfg.JoinAddrs).
//
// FLOW:
//  1. Create Serf with event channel
//  2. Join seed nodes (if any — the first node has none)
//  3. Start event handler goroutine
//
// WHY event channel, not EventHandler interface: the channel approach lets
// us process events in a single goroutine, avoiding concurrency issues in
// the handler. Serf's EventHandler interface can fire callbacks concurrently.
func New(raftNode *quollraft.Node, cfg config.SerfConfig, raftAddr string, log zerolog.Logger) (*Membership, error) {
	log = log.With().Str("component", "membership").Logger()

	events := make(chan serf.Event, 256)

	serfCfg := serf.DefaultConfig()
	serfCfg.NodeName = cfg.NodeName
	serfCfg.EventCh = events

	// Tags let other nodes discover our Raft address.
	// When a new node sees us via Serf, it reads our raft_addr tag
	// to know where to send Raft RPCs.
	serfCfg.Tags = map[string]string{
		TagRaftAddr: raftAddr,
		TagRole:     "voter",
	}
	for k, v := range cfg.Tags {
		serfCfg.Tags[k] = v
	}

	// Bind address for Serf gossip traffic.
	host, port, err := net.SplitHostPort(cfg.BindAddr)
	if err != nil {
		return nil, fmt.Errorf("parse serf bind addr: %w", err)
	}
	serfCfg.MemberlistConfig.BindAddr = host
	portNum, _ := net.LookupPort("tcp", port)
	serfCfg.MemberlistConfig.BindPort = portNum

	s, err := serf.Create(serfCfg)
	if err != nil {
		return nil, fmt.Errorf("create serf: %w", err)
	}

	m := &Membership{
		serf:   s,
		node:   raftNode,
		events: events,
		log:    log,
		nodeID: cfg.NodeName,
	}

	// Join seed nodes. For the first node, JoinAddrs is empty — it's alone
	// until other nodes join. For subsequent nodes, they need at least one
	// seed to discover the cluster.
	//
	// Serf join is eventually consistent: a successful Join() means we
	// contacted at least one seed; full membership converges within seconds.
	if len(cfg.JoinAddrs) > 0 {
		numJoined, err := s.Join(cfg.JoinAddrs, true)
		if err != nil {
			// Non-fatal: we might be the first node, or seeds might be
			// temporarily unreachable. Serf will retry via gossip.
			log.Warn().Err(err).
				Int("joined", numJoined).
				Strs("seeds", cfg.JoinAddrs).
				Msg("partial or failed join")
		} else {
			log.Info().Int("joined", numJoined).Msg("joined cluster via seeds")
		}
	}

	// Start event handler in background.
	go m.handleEvents()

	return m, nil
}

// handleEvents processes Serf membership events and translates them
// into Raft cluster configuration changes.
//
// Only the leader processes join/leave events for Raft. Followers see the
// same Serf events but skip the Raft calls — only the leader can modify the
// voter set. If the leader crashes, the new leader picks up from the current
// Serf membership state.
func (m *Membership) handleEvents() {
	for event := range m.events {
		switch e := event.(type) {
		case serf.MemberEvent:
			for _, member := range e.Members {
				if member.Name == m.nodeID {
					// Skip ourselves — we're already in the cluster.
					continue
				}

				switch e.EventType() {
				case serf.EventMemberJoin:
					m.handleJoin(member)
				case serf.EventMemberLeave, serf.EventMemberFailed:
					m.handleLeave(member)
				}
			}
		}
	}
}

func (m *Membership) handleJoin(member serf.Member) {
	raftAddr, ok := member.Tags[TagRaftAddr]
	if !ok {
		m.log.Warn().Str("node", member.Name).Msg("member joined without raft_addr tag, skipping")
		return
	}

	// Only the leader modifies the Raft voter set.
	if !m.node.IsLeader() {
		return
	}

	if err := m.node.AddVoter(member.Name, raftAddr); err != nil {
		m.log.Error().Err(err).
			Str("node", member.Name).
			Str("raft_addr", raftAddr).
			Msg("failed to add voter")
		return
	}

	m.log.Info().
		Str("node", member.Name).
		Str("raft_addr", raftAddr).
		Msg("new member joined cluster")
}

func (m *Membership) handleLeave(member serf.Member) {
	if !m.node.IsLeader() {
		return
	}

	if err := m.node.RemoveServer(member.Name); err != nil {
		m.log.Error().Err(err).
			Str("node", member.Name).
			Msg("failed to remove server")
		return
	}

	m.log.Info().
		Str("node", member.Name).
		Msg("member left cluster")
}

// Members returns the current Serf member list.
func (m *Membership) Members() []serf.Member {
	return m.serf.Members()
}

// Leave gracefully leaves the Serf cluster.
// This triggers a MemberLeave event on other nodes (as opposed to
// MemberFailed, which happens on ungraceful departure).
//
// WHY the distinction matters: MemberLeave is intentional (deployment,
// scaling down). MemberFailed might be transient (network blip).
// Production systems often wait longer before removing a failed node
// from Raft, but remove a leaving node immediately.
func (m *Membership) Leave() error {
	return m.serf.Leave()
}

// Shutdown stops the Serf agent.
func (m *Membership) Shutdown() error {
	if err := m.serf.Leave(); err != nil {
		m.log.Warn().Err(err).Msg("serf leave failed during shutdown")
	}
	return m.serf.Shutdown()
}
