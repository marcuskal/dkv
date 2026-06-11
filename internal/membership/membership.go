// Package membership manages cluster membership via Serf (gossip protocol).
// Event handling is delegated to the coordinator, which owns the
// "what happens when membership changes" policy.
package membership

import (
	"fmt"
	"net"
	"time"

	"github.com/hashicorp/serf/serf"
	"github.com/rs/zerolog"

	"github.com/marcuskal/dkv/internal/config"
)

// EventHandler is called for each Serf event.
// Implemented by coordinator.Coordinator.
type EventHandler interface {
	HandleEvent(event serf.Event)
}

// Membership manages the Serf agent.
type Membership struct {
	serf    *serf.Serf
	handler EventHandler
	events  chan serf.Event
	log     zerolog.Logger
	nodeID  string
}

// New creates and starts a Serf agent.
func New(handler EventHandler, cfg config.SerfConfig, log zerolog.Logger) (*Membership, error) {
	log = log.With().Str("component", "membership").Logger()

	events := make(chan serf.Event, 256)

	serfCfg := serf.DefaultConfig()
	serfCfg.NodeName = cfg.NodeName
	serfCfg.EventCh = events

	// Copy tags from config, which now includes both raft_addr and grpc_addr.
	serfCfg.Tags = make(map[string]string)
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
		serf:    s,
		handler: handler,
		events:  events,
		log:     log,
		nodeID:  cfg.NodeName,
	}

	if len(cfg.JoinAddrs) > 0 {
		// Retry the join: pod-0's Serf may not be listening yet when pod-1/2
		// start (there is a brief race window between pod-0 becoming K8s-Ready
		// and its Serf agent binding port 9092). Five attempts at 2 s intervals
		// covers a ~10 s window which is more than enough.
		const maxJoinAttempts = 5
		const joinRetryInterval = 2 * time.Second
		for attempt := 1; attempt <= maxJoinAttempts; attempt++ {
			numJoined, joinErr := s.Join(cfg.JoinAddrs, true)
			if joinErr == nil {
				log.Info().Int("joined", numJoined).Msg("joined cluster via seeds")
				break
			}
			if attempt == maxJoinAttempts {
				log.Warn().Err(joinErr).
					Int("joined", numJoined).
					Strs("seeds", cfg.JoinAddrs).
					Msg("partial or failed join after all attempts")
				break
			}
			log.Info().Err(joinErr).
				Int("attempt", attempt).
				Dur("retry_in", joinRetryInterval).
				Msg("serf join failed, retrying")
			time.Sleep(joinRetryInterval)
		}
	}

	go m.handleEvents()

	return m, nil
}

// handleEvents reads Serf events and delegates to the coordinator.
func (m *Membership) handleEvents() {
	for event := range m.events {
		m.handler.HandleEvent(event)
	}
}

// Members returns the current Serf member list.
func (m *Membership) Members() []serf.Member {
	return m.serf.Members()
}

// Leave gracefully leaves the Serf cluster.
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
