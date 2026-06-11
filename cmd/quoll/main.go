// cmd/quoll/main.go — QUOLL node entry point.
//
// Boot sequence:
//  1. Load config (file + env var overrides)
//  2. Create logger
//  3. Detect K8s mode and rewrite config from pod identity
//  4. Start Engine (WAL recovery)
//  5. Create Hash Ring + Router
//  6. Start Raft node (with bind/advertise split)
//  7. Create Coordinator
//  8. Discover Serf seeds via DNS if in K8s mode
//  9. Start Serf membership
//  10. Start health/readiness HTTP server
//  11. Start gRPC server
//  12. Wait for OS signal
//  13. Graceful shutdown in reverse order
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/marcuskal/quoll/internal/config"
	"github.com/marcuskal/quoll/internal/coordinator"
	"github.com/marcuskal/quoll/internal/discovery"
	"github.com/marcuskal/quoll/internal/engine"
	"github.com/marcuskal/quoll/internal/hashring"
	"github.com/marcuskal/quoll/internal/membership"
	quollraft "github.com/marcuskal/quoll/internal/raft"
	"github.com/marcuskal/quoll/internal/router"
	server "github.com/marcuskal/quoll/internal/transport/grpc"
	"github.com/marcuskal/quoll/pkg/logger"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
)

// Set at build time via -ldflags.
var (
	version = "dev"
	commit  = "unknown"
)

func main() {
	configPath := flag.String("config", "quoll.yaml", "path to config file")
	flag.Parse()

	// --- 1. Load config ---
	cfg, err := config.Load(*configPath)
	if err != nil {
		panic("failed to load config: " + err.Error())
	}

	// --- 2. Logger ---
	log := logger.New(cfg.LogLevel, os.Stderr)
	log.Info().
		Str("version", version).
		Str("commit", commit).
		Str("config", *configPath).
		Msg("QUOLL node starting")

	// --- 3. K8s mode: rewrite config from pod identity ---
	var k8sIdentity *discovery.PodIdentity
	if cfg.K8s.Enabled {
		id, err := discovery.IdentityFromEnv()
		if err != nil {
			log.Fatal().Err(err).Msg("k8s.enabled=true but pod identity unavailable")
		}
		applyK8sIdentity(&cfg, id)
		k8sIdentity = &id
		log.Info().
			Str("pod", id.PodName).
			Str("ns", id.PodNamespace).
			Int("ordinal", id.Ordinal).
			Str("fqdn", id.FQDN()).
			Bool("bootstrap", cfg.Raft.Bootstrap).
			Msg("running in K8s mode")
	}

	// --- 4. Engine ---
	eng, err := engine.New(
		cfg.Engine.WALDir,
		cfg.Engine.WALSyncMode,
		cfg.Engine.MaxKeySize,
		cfg.Engine.MaxValueSize,
		log,
	)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create engine")
	}
	defer eng.Close()
	log.Info().Msg("engine started")

	// --- 5. Hash Ring + Router ---
	ring := hashring.New(cfg.HashRing.VnodeCount)
	nodeID := cfg.Raft.NodeID
	if nodeID == "" {
		nodeID = cfg.Serf.NodeName
	}
	if nodeID != "" {
		ring.AddNode(nodeID)
	}
	rtr := router.New(ring, nodeID, log)
	defer rtr.Shutdown()
	log.Info().Int("vnodes", cfg.HashRing.VnodeCount).Msg("hash ring and router created")

	// --- 6. Raft ---
	var raftNode *quollraft.Node
	if cfg.Raft.NodeID != "" {
		raftNode, err = quollraft.NewNode(eng, cfg.Raft, log)
		if err != nil {
			log.Fatal().Err(err).Msg("failed to create raft node")
		}
		defer raftNode.Shutdown()
		log.Info().Str("node_id", cfg.Raft.NodeID).Msg("raft node started")

		// Pod-0 may bootstrap; in that case wait for self-election.
		// Other pods skip this — they'll get added as voters by pod-0.
		if cfg.Raft.Bootstrap {
			if err := raftNode.WaitForLeader(cfg.Raft.ElectionTimeout * 5); err != nil {
				log.Warn().Err(err).Msg("no leader after bootstrap timeout, proceeding anyway")
			}
		}
	}

	// --- 7. Health server (liveness + readiness for K8s probes) ---
	//
	// MUST start before Serf discovery: DNS lookups for non-existent peers can
	// block for 15–30 s (ndots:5 search-domain expansion in the Go resolver).
	// K8s liveness probes begin at initialDelaySeconds=10 and kill the pod
	// after 3 failures. Starting the server here ensures probes are served
	// immediately regardless of how long peer discovery takes.
	healthSrv := startHealthServer(cfg.Health, raftNode, log)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = healthSrv.Shutdown(ctx)
	}()

	// --- 8. Coordinator + Serf membership (with K8s peer discovery) ---
	var membershipHandler *membership.Membership
	if cfg.Serf.NodeName != "" {
		// In K8s mode, replace static JoinAddrs with DNS-discovered peers.
		if k8sIdentity != nil {
			ctx, cancel := context.WithTimeout(context.Background(), cfg.K8s.DiscoveryTimeout)
			seeds := discoverK8sSeeds(ctx, *k8sIdentity, &cfg, log)
			cancel()
			cfg.Serf.JoinAddrs = seeds
			log.Info().Strs("seeds", seeds).Msg("k8s seed discovery complete")
		}

		coord := coordinator.New(raftNode, ring, rtr, cfg.Serf.NodeName, log)

		if cfg.Serf.Tags == nil {
			cfg.Serf.Tags = make(map[string]string)
		}
		// Tags carry our peer-visible addresses. These are the addrs other
		// nodes use to call us — must be FQDN in K8s, not the pod IP.
		cfg.Serf.Tags["raft_addr"] = coalesce(cfg.Raft.AdvertiseAddr, cfg.Raft.BindAddr)
		cfg.Serf.Tags["grpc_addr"] = coalesce(cfg.GRPC.AdvertiseAddr, cfg.GRPC.ListenAddr)

		membershipHandler, err = membership.New(coord, cfg.Serf, log)
		if err != nil {
			log.Fatal().Err(err).Msg("failed to create membership handler")
		}
		defer membershipHandler.Shutdown()
		log.Info().Str("node_name", cfg.Serf.NodeName).Msg("serf membership started")
	}

	// --- 9. gRPC server ---
	srv, err := server.New(eng, raftNode, rtr, nodeID, cfg, log, nil, nil)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create gRPC server")
	}
	go func() {
		if err := srv.Serve(); err != nil {
			log.Fatal().Err(err).Msg("gRPC server failed")
		}
	}()
	log.Info().Str("addr", cfg.GRPC.ListenAddr).Msg("gRPC server started")

	// --- 10. Block on signal ---
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	log.Info().Str("signal", sig.String()).Msg("shutting down")

	// --- 11. Graceful shutdown (deferred handlers fire in reverse order) ---
	srv.GracefulStop()
	log.Info().Msg("QUOLL node stopped")
	_ = membershipHandler // keep referenced for the linter
}

// applyK8sIdentity rewrites cfg in-place from the pod's K8s-injected identity.
//
// Decisions baked in here:
//   - NodeID = POD_NAME ("quoll-0") — stable across pod restarts.
//   - Raft binds to 0.0.0.0:port (all pod interfaces) but advertises FQDN.
//   - Serf does the same.
//   - Bootstrap is true ONLY for pod-0 (ordinal 0). hashicorp/raft makes
//     this idempotent — re-bootstrap is a no-op once the cluster exists.
//   - WAL/Raft/data dirs go under /var/lib/quoll (the mounted PVC).
func applyK8sIdentity(cfg *config.Config, id discovery.PodIdentity) {
	cfg.Raft.NodeID = id.PodName
	cfg.Serf.NodeName = id.PodName

	// Bind to all interfaces in the pod, advertise the FQDN.
	cfg.Raft.BindAddr = "0.0.0.0:9091"
	cfg.Raft.AdvertiseAddr = fmt.Sprintf("%s:9091", id.FQDN())

	cfg.GRPC.ListenAddr = "0.0.0.0:9090"
	cfg.GRPC.AdvertiseAddr = fmt.Sprintf("%s:9090", id.FQDN())

	cfg.Serf.BindAddr = "0.0.0.0:9092"
	cfg.Serf.AdvertiseAddr = fmt.Sprintf("%s:9092", id.FQDN())

	cfg.Raft.Bootstrap = id.IsBootstrapNode()

	// Persistent volume mount.
	cfg.DataDir = "/var/lib/quoll"
	cfg.Engine.WALDir = "/var/lib/quoll/wal"
	cfg.Raft.DataDir = "/var/lib/quoll/raft"
}

// discoverK8sSeeds resolves peer FQDNs and returns Serf join addresses.
//
// Bootstrap node (pod-0): waits up to discovery timeout but accepts 0 peers
// — it's allowed to start alone and let others find it.
//
// Follower nodes: wait for at least 1 peer (typically pod-0). If discovery
// times out without finding any peer, we still return what we have and log
// a warning. Failing to start would just trigger a CrashLoopBackOff.
func discoverK8sSeeds(ctx context.Context, id discovery.PodIdentity, cfg *config.Config, log zerolog.Logger) []string {
	pd := discovery.NewPeerDiscovery(id, cfg.K8s.ExpectedReplicas, 9092, log)

	minPeers := 1
	if id.IsBootstrapNode() {
		minPeers = 0
	}

	seeds, err := pd.WaitForPeers(ctx, minPeers, cfg.K8s.DiscoveryRetry)
	if err != nil {
		log.Warn().Err(err).
			Int("found", len(seeds)).
			Int("min_required", minPeers).
			Msg("seed discovery incomplete")
	}
	return seeds
}

// startHealthServer exposes /healthz (liveness) and /ready (readiness)
// on a separate HTTP port.
//
//	/healthz (liveness):   Always 200 if the process is responsive.
//	                       K8s restarts the pod if this fails.
//	                       Must not depend on Raft state.
//
//	/ready   (readiness):  200 only if a Raft leader is known.
//	                       K8s removes us from Service endpoints if this
//	                       fails, but does not restart us.
//
// The liveness/readiness split avoids a deadly embrace: if liveness depended
// on cluster quorum, a partition would fail liveness on every pod, K8s would
// restart all pods simultaneously, and the cluster would never recover.
func startHealthServer(cfg config.HealthConfig, raftNode *quollraft.Node, log zerolog.Logger) *http.Server {
	mux := http.NewServeMux()

	// Prometheus scrape endpoint — uses the default registry which includes
	// Go runtime metrics (goroutines, memory, GC). QUOLL-specific metrics are
	// added when the observability layer is fully wired in.
	mux.Handle("/metrics", promhttp.Handler())

	mux.HandleFunc(cfg.LivenessPath, func(w http.ResponseWriter, r *http.Request) {
		// Process is alive if we got here. No external dependencies.
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	mux.HandleFunc(cfg.ReadinessPath, func(w http.ResponseWriter, r *http.Request) {
		// Single-node mode: always ready once the process is up.
		if raftNode == nil {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ready"))
			return
		}
		// Cluster mode: ready iff a leader is known.
		if !raftNode.HasLeader() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("no leader"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
	})

	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Info().Str("addr", cfg.ListenAddr).Msg("health server started")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error().Err(err).Msg("health server failed")
		}
	}()

	return srv
}

func coalesce(a, b string) string {
	if a != "" {
		return a
	}
	return b
}
