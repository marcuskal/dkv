// internal/discovery/k8s.go — DNS-based peer discovery for K8s.
package discovery

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/rs/zerolog"
)

// Resolver is the DNS interface we depend on. Decoupled so tests can
// inject a fake. The standard library's net.DefaultResolver implements it.
type Resolver interface {
	LookupHost(ctx context.Context, host string) ([]string, error)
}

// PeerDiscovery resolves Serf seed addresses via headless-service DNS.
//
// FLOW:
//  1. For each ordinal in [0, expectedReplicas):
//     - Skip ourselves
//     - Resolve <name>-<ordinal>.<headless>.<ns>.svc.<domain>
//     - If it resolves, add to seeds; if not, skip (peer not up yet)
//  2. Return the resolved seed addresses
//
// We don't fail if some peers can't be resolved — Serf's gossip will catch
// them up later. The goal is to give Serf at least one live seed so it
// can join the cluster.
//
// StatefulSets default to OrderedReady (pod-N waits until pod-(N-1) is Ready),
// but discovery does not depend on that ordering — if no peers resolve yet,
// WaitForPeers retries, making startup resilient to pod-0 restarts and
// non-OrderedReady policies.
type PeerDiscovery struct {
	identity         PodIdentity
	expectedReplicas int
	serfPort         int
	resolver         Resolver
	log              zerolog.Logger
}

// NewPeerDiscovery creates a discovery handle.
//
// expectedReplicas should match the StatefulSet replica count (typically 3).
// We pass it explicitly rather than querying the K8s API — that would
// require a ServiceAccount with `get statefulsets` RBAC, which is overkill
// for what's really a static configuration value.
func NewPeerDiscovery(identity PodIdentity, expectedReplicas, serfPort int, log zerolog.Logger) *PeerDiscovery {
	return &PeerDiscovery{
		identity:         identity,
		expectedReplicas: expectedReplicas,
		serfPort:         serfPort,
		resolver:         net.DefaultResolver,
		log:              log.With().Str("component", "discovery").Logger(),
	}
}

// SetResolver overrides the DNS resolver — used in tests.
func (d *PeerDiscovery) SetResolver(r Resolver) {
	d.resolver = r
}

// lookupTimeout caps each individual DNS probe. With ndots:5, Go's pure
// resolver tries up to 4 search-domain variants per FQDN. CoreDNS returns
// NXDOMAIN quickly for each, but on some configurations the resolver retries
// on SERVFAIL with exponential backoff, blocking for up to 5 s per attempt.
// A 1 s per-lookup cap keeps DiscoverSeeds fast even when most peers don't
// exist yet (e.g., pod-0 probing pod-1/pod-2 during initial bootstrap).
const lookupTimeout = 1 * time.Second

// DiscoverSeeds returns Serf join addresses for all peers that currently
// have DNS records. Skips this pod itself.
//
// The returned strings are suitable for passing to serf.Serf.Join().
// Format: "<fqdn>:<serfPort>"
func (d *PeerDiscovery) DiscoverSeeds(ctx context.Context) []string {
	var seeds []string

	for i := 0; i < d.expectedReplicas; i++ {
		if i == d.identity.Ordinal {
			continue // skip ourselves
		}

		fqdn := d.identity.PeerFQDN(i)

		// Cap each individual lookup so a single unresolvable peer can't block
		// the entire discovery pass. If the outer ctx is already shorter, use it.
		lookupCtx, lookupCancel := context.WithTimeout(ctx, lookupTimeout)
		ips, err := d.resolver.LookupHost(lookupCtx, fqdn)
		lookupCancel()

		if err != nil {
			d.log.Debug().
				Err(err).
				Str("peer", fqdn).
				Int("ordinal", i).
				Msg("peer not resolvable yet, skipping")
			continue
		}
		if len(ips) == 0 {
			continue
		}

		// Use the FQDN, NOT the IP. The IP can change on pod restart;
		// the FQDN is stable. Serf will re-resolve on each gossip cycle.
		seedAddr := fmt.Sprintf("%s:%d", fqdn, d.serfPort)
		seeds = append(seeds, seedAddr)
		d.log.Info().
			Str("peer", fqdn).
			Strs("ips", ips).
			Int("ordinal", i).
			Msg("discovered peer")
	}

	return seeds
}

// WaitForPeers blocks until at least minPeers are discoverable, or the
// context is cancelled.
//
// Pod-0 (the bootstrap node) typically calls this with minPeers=0 — it
// just bootstraps and waits for others to find it. Followers call it with
// minPeers=1 to ensure at least one seed is up before joining Serf.
//
// WaitForPeers times out via the context rather than retrying forever.
// Coming up with fewer peers than expected and letting gossip catch up
// is safer than blocking the pod (which would fail readiness probes).
func (d *PeerDiscovery) WaitForPeers(ctx context.Context, minPeers int, retryInterval time.Duration) ([]string, error) {
	for {
		seeds := d.DiscoverSeeds(ctx)
		if len(seeds) >= minPeers {
			return seeds, nil
		}

		select {
		case <-ctx.Done():
			// Return what we have, even if below minPeers.
			// Caller decides whether to fail or proceed.
			return seeds, errors.Join(ctx.Err(),
				fmt.Errorf("only %d/%d peers found before deadline", len(seeds), minPeers))
		case <-time.After(retryInterval):
			// retry
		}
	}
}
