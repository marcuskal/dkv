package dkvraft

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/rs/zerolog"

	"github.com/marcuskal/dkv/internal/config"
	"github.com/marcuskal/dkv/internal/engine"
)

var (
	ErrNotLeader = errors.New("not the leader")
)

// Node wraps a Raft instance.
//
// Bind vs advertise address split: in Kubernetes we listen on 0.0.0.0:9091
// (all pod interfaces) but peers must connect via the stable headless-service
// FQDN (e.g. dkv-0.dkv-headless.dkv.svc.cluster.local:9091). Without this
// split, Raft would advertise 0.0.0.0 to peers, which fails on any remote dial.
// After a pod restart the IP changes; the FQDN re-resolves on the next
// connection attempt — no cluster reconfig needed.
type Node struct {
	raft   *raft.Raft
	fsm    *FSM
	config config.RaftConfig
	log    zerolog.Logger
}

// NewNode creates and starts a Raft node.
func NewNode(eng *engine.Engine, cfg config.RaftConfig, log zerolog.Logger) (*Node, error) {
	log = log.With().Str("component", "raft").Logger()

	fsm := NewFSM(eng, log, nil, nil)

	// --- Raft configuration ---
	raftCfg := raft.DefaultConfig()
	raftCfg.LocalID = raft.ServerID(cfg.NodeID)
	raftCfg.HeartbeatTimeout = cfg.HeartbeatTimeout
	raftCfg.ElectionTimeout = cfg.ElectionTimeout
	raftCfg.LeaderLeaseTimeout = cfg.LeaderLeaseTimeout
	raftCfg.CommitTimeout = cfg.CommitTimeout
	raftCfg.SnapshotInterval = cfg.SnapshotInterval
	raftCfg.SnapshotThreshold = cfg.SnapshotThreshold
	raftCfg.TrailingLogs = cfg.TrailingLogs

	// --- Storage ---
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = filepath.Join(os.TempDir(), "dkv-raft", cfg.NodeID)
	}
	if err := os.MkdirAll(dataDir, 0750); err != nil {
		return nil, fmt.Errorf("create raft data dir: %w", err)
	}

	boltPath := filepath.Join(dataDir, "raft.db")
	boltStore, err := raftboltdb.NewBoltStore(boltPath)
	if err != nil {
		return nil, fmt.Errorf("create bolt store: %w", err)
	}

	snapshotStore, err := raft.NewFileSnapshotStore(dataDir, 2, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("create snapshot store: %w", err)
	}

	// --- Transport: bind/advertise split ---
	//
	// bindAddr is what we LISTEN on (e.g., "0.0.0.0:9091" in K8s).
	// advertise is what peers see (e.g., FQDN:port in K8s, or just bind in dev).
	//
	// In single-node/dev mode (cfg.AdvertiseAddr empty), advertise == bind.
	advertise, err := buildAdvertiseAddr(cfg)
	if err != nil {
		return nil, fmt.Errorf("build advertise addr: %w", err)
	}

	transport, err := raft.NewTCPTransport(cfg.BindAddr, advertise, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("create raft transport: %w", err)
	}

	log.Info().
		Str("bind", cfg.BindAddr).
		Str("advertise", advertise.String()).
		Msg("raft transport ready")

	// --- Create Raft instance ---
	r, err := raft.NewRaft(raftCfg, fsm, boltStore, boltStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("create raft instance: %w", err)
	}

	// --- Bootstrap (only on the seed node) ---
	if cfg.Bootstrap {
		// Use the stable FQDN (cfg.AdvertiseAddr) as the persisted address, not
		// transport.LocalAddr() which returns the ephemeral pod IP. If we store
		// the IP in the Raft log, a pod restart (new IP) leaves a stale address
		// that other nodes can't dial. The FQDN re-resolves to the current IP on
		// every connection attempt via K8s headless-service DNS.
		bootstrapAddr := raft.ServerAddress(cfg.AdvertiseAddr)
		if bootstrapAddr == "" {
			bootstrapAddr = raft.ServerAddress(cfg.BindAddr)
		}
		bootstrapCfg := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raft.ServerID(cfg.NodeID),
					Address: bootstrapAddr,
				},
			},
		}
		f := r.BootstrapCluster(bootstrapCfg)
		if err := f.Error(); err != nil {
			if !errors.Is(err, raft.ErrCantBootstrap) {
				return nil, fmt.Errorf("bootstrap cluster: %w", err)
			}
			log.Warn().Msg("cluster already bootstrapped, skipping")
		} else {
			log.Info().Msg("cluster bootstrapped successfully")
		}
	}

	return &Node{
		raft:   r,
		fsm:    fsm,
		config: cfg,
		log:    log,
	}, nil
}

// buildAdvertiseAddr returns the net.Addr peers use to identify this node.
//
// hashicorp/raft v1.6.1 requires the advertise addr to be a *net.TCPAddr —
// it does a type assertion inside newTCPTransport and returns errNotTCP for
// any other net.Addr implementation. We therefore always resolve to an IP.
//
// In K8s, cfg.AdvertiseAddr is the pod FQDN (e.g. dkv-0.dkv-headless.dkv.svc.cluster.local:9091).
// We resolve it once at startup. The IP is stable for the pod's lifetime;
// when the pod restarts the Serf coordinator re-adds the node with the new IP.
func buildAdvertiseAddr(cfg config.RaftConfig) (net.Addr, error) {
	addr := cfg.AdvertiseAddr
	if addr == "" {
		addr = cfg.BindAddr
	}
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("resolve advertise addr %q: %w", addr, err)
	}
	return tcpAddr, nil
}

// Apply submits a command to the Raft log.
func (n *Node) Apply(cmd Command, timeout time.Duration) error {
	if n.raft.State() != raft.Leader {
		return ErrNotLeader
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("marshal command: %w", err)
	}

	future := n.raft.Apply(data, timeout)
	if err := future.Error(); err != nil {
		return fmt.Errorf("raft apply: %w", err)
	}

	if resp := future.Response(); resp != nil {
		if err, ok := resp.(error); ok {
			return err
		}
	}
	return nil
}

func (n *Node) IsLeader() bool         { return n.raft.State() == raft.Leader }
func (n *Node) LeaderAddr() string     { addr, _ := n.raft.LeaderWithID(); return string(addr) }

// HasLeader returns true if Raft currently believes a leader exists.
// Used by the K8s readiness probe — we're "ready to serve" only when a
// leader is known, otherwise writes will fail with ErrNotLeader anyway.
func (n *Node) HasLeader() bool { return n.LeaderAddr() != "" }

func (n *Node) AddVoter(nodeID, addr string) error {
	if n.raft.State() != raft.Leader {
		return ErrNotLeader
	}
	f := n.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 10*time.Second)
	if err := f.Error(); err != nil {
		return fmt.Errorf("add voter %s at %s: %w", nodeID, addr, err)
	}
	n.log.Info().Str("node_id", nodeID).Str("addr", addr).Msg("voter added")
	return nil
}

func (n *Node) RemoveServer(nodeID string) error {
	if n.raft.State() != raft.Leader {
		return ErrNotLeader
	}
	f := n.raft.RemoveServer(raft.ServerID(nodeID), 0, 10*time.Second)
	if err := f.Error(); err != nil {
		return fmt.Errorf("remove server %s: %w", nodeID, err)
	}
	n.log.Info().Str("node_id", nodeID).Msg("server removed")
	return nil
}

func (n *Node) GetConfiguration() ([]raft.Server, error) {
	f := n.raft.GetConfiguration()
	if err := f.Error(); err != nil {
		return nil, err
	}
	return f.Configuration().Servers, nil
}

// Shutdown gracefully shuts down the Raft node.
// Transfers leadership first if this node is currently the leader, so the
// new leader is elected before this node disappears and the cluster avoids
// an unnecessary election timeout.
func (n *Node) Shutdown() error {
	n.log.Info().Msg("shutting down raft node")

	if n.raft.State() == raft.Leader {
		n.log.Info().Msg("transferring leadership before shutdown")
		if err := n.raft.LeadershipTransfer().Error(); err != nil {
			n.log.Warn().Err(err).Msg("leadership transfer failed, proceeding")
		}
	}

	return n.raft.Shutdown().Error()
}

func (n *Node) WaitForLeader(timeout time.Duration) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			return fmt.Errorf("timeout waiting for leader after %s", timeout)
		case <-ticker.C:
			if addr, _ := n.raft.LeaderWithID(); addr != "" {
				return nil
			}
		}
	}
}