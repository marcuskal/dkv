// Package discovery provides Kubernetes-aware peer discovery for DKV.
//
// PROBLEM: In K8s, pods get scheduled with random IPs that change on
// restart. Raft and Serf need stable peer identifiers. The solution is
// the StatefulSet + headless Service pattern:
//
//   - StatefulSet gives each pod a stable name: dkv-0, dkv-1, dkv-2.
//   - Headless Service (clusterIP: None) creates DNS records for each pod:
//     dkv-0.dkv-headless.dkv.svc.cluster.local
//     dkv-1.dkv-headless.dkv.svc.cluster.local
//   - On restart, the pod gets a new IP but the DNS record is updated to
//     point at it. Peers reconnect transparently.
//
// A regular Service load-balances via a VIP, which breaks Raft (which needs
// to dial a specific peer). Headless services return per-pod DNS records
// directly, giving each peer a stable, individually-addressable identity.
package discovery

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// PodIdentity captures the K8s-injected metadata about this pod.
//
// All fields come from the downward API or the pod's environment.
// See the StatefulSet spec for the env var injection.
type PodIdentity struct {
	PodName         string // e.g. "dkv-0"          (from POD_NAME)
	PodNamespace    string // e.g. "dkv"            (from POD_NAMESPACE)
	PodIP           string // e.g. "10.244.0.5"     (from POD_IP)
	StatefulSetName string // e.g. "dkv"           (parsed from PodName)
	Ordinal         int    // e.g. 0                (parsed from PodName)
	HeadlessSvc     string // e.g. "dkv-headless"   (from HEADLESS_SVC env, or derived)
	ClusterDomain   string // e.g. "cluster.local"  (from CLUSTER_DOMAIN env, default "cluster.local")
}

// FQDN returns this pod's stable DNS name within the headless service.
// Example: "dkv-0.dkv-headless.dkv.svc.cluster.local"
//
// This is what Raft advertises to peers and what Serf gossips. It's stable
// across pod restarts — even though the underlying IP changes, the FQDN
// always resolves to the current IP.
func (p PodIdentity) FQDN() string {
	return fmt.Sprintf("%s.%s.%s.svc.%s",
		p.PodName, p.HeadlessSvc, p.PodNamespace, p.ClusterDomain)
}

// PeerFQDN returns the FQDN of another pod in the same StatefulSet by ordinal.
func (p PodIdentity) PeerFQDN(ordinal int) string {
	peerName := fmt.Sprintf("%s-%d", p.StatefulSetName, ordinal)
	return fmt.Sprintf("%s.%s.%s.svc.%s",
		peerName, p.HeadlessSvc, p.PodNamespace, p.ClusterDomain)
}

// IsBootstrapNode returns true if this pod should bootstrap the Raft cluster.
//
// CONVENTION: ordinal 0 is the bootstrap node. The hashicorp/raft library
// rejects bootstrap on an already-bootstrapped cluster (returns ErrCantBootstrap),
// which is handled in raft.go. So even if pod-0 restarts after the cluster
// is up, calling Bootstrap is safe — it's a no-op.
//
// If pod-0 dies before the cluster bootstraps and the other pods were running,
// they're stuck (no quorum without a bootstrap config). Deploy all 3 pods
// together — do not roll them out one at a time.
func (p PodIdentity) IsBootstrapNode() bool {
	return p.Ordinal == 0
}

// IdentityFromEnv builds a PodIdentity from environment variables.
// Returns an error if required vars are missing — that means we're not
// running in K8s and should fall back to file-based config.
//
// Required env vars (all injected by the StatefulSet spec):
//
//	POD_NAME       — via downward API: metadata.name
//	POD_NAMESPACE  — via downward API: metadata.namespace
//	POD_IP         — via downward API: status.podIP
//
// Optional env vars (with defaults):
//
//	HEADLESS_SVC   — defaults to "<statefulset-name>-headless"
//	CLUSTER_DOMAIN — defaults to "cluster.local"
func IdentityFromEnv() (PodIdentity, error) {
	podName := os.Getenv("POD_NAME")
	if podName == "" {
		return PodIdentity{}, fmt.Errorf("POD_NAME not set; not running in K8s")
	}
	ns := os.Getenv("POD_NAMESPACE")
	if ns == "" {
		return PodIdentity{}, fmt.Errorf("POD_NAMESPACE not set")
	}

	stsName, ord, err := parseStatefulSetPodName(podName)
	if err != nil {
		return PodIdentity{}, fmt.Errorf("parse pod name %q: %w", podName, err)
	}

	headless := os.Getenv("HEADLESS_SVC")
	if headless == "" {
		headless = stsName + "-headless"
	}

	domain := os.Getenv("CLUSTER_DOMAIN")
	if domain == "" {
		domain = "cluster.local"
	}

	return PodIdentity{
		PodName:         podName,
		PodNamespace:    ns,
		PodIP:           os.Getenv("POD_IP"), // optional; fine if empty
		StatefulSetName: stsName,
		Ordinal:         ord,
		HeadlessSvc:     headless,
		ClusterDomain:   domain,
	}, nil
}

// parseStatefulSetPodName splits "dkv-0" into ("dkv", 0).
//
// EDGE CASE: StatefulSet names can contain hyphens themselves
// (e.g., "my-app-0" → ("my-app", 0)). We split on the LAST hyphen
// because the ordinal is always trailing.
func parseStatefulSetPodName(podName string) (string, int, error) {
	idx := strings.LastIndex(podName, "-")
	if idx == -1 {
		return "", 0, fmt.Errorf("no hyphen in pod name")
	}
	stsName := podName[:idx]
	if strings.HasSuffix(stsName, "-") {
		return "", 0, fmt.Errorf("malformed pod name %q: consecutive hyphens", podName)
	}
	ordStr := podName[idx+1:]
	ord, err := strconv.Atoi(ordStr)
	if err != nil {
		return "", 0, fmt.Errorf("ordinal %q not numeric: %w", ordStr, err)
	}
	if ord < 0 {
		return "", 0, fmt.Errorf("negative ordinal: %d", ord)
	}
	return stsName, ord, nil
}
