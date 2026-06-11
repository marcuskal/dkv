package discovery

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func TestParseStatefulSetPodName(t *testing.T) {
	tests := []struct {
		name    string
		podName string
		wantSts string
		wantOrd int
		wantErr bool
	}{
		{"simple", "quoll-0", "quoll", 0, false},
		{"larger ordinal", "quoll-12", "quoll", 12, false},
		{"hyphenated name", "my-app-3", "my-app", 3, false},
		{"no hyphen", "quoll", "", 0, true},
		{"non-numeric", "quoll-foo", "", 0, true},
		{"negative", "quoll--1", "", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sts, ord, err := parseStatefulSetPodName(tt.podName)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil {
				return
			}
			if sts != tt.wantSts || ord != tt.wantOrd {
				t.Errorf("got (%q, %d), want (%q, %d)", sts, ord, tt.wantSts, tt.wantOrd)
			}
		})
	}
}

func TestIdentityFromEnv(t *testing.T) {
	t.Setenv("POD_NAME", "quoll-2")
	t.Setenv("POD_NAMESPACE", "production")
	t.Setenv("POD_IP", "10.0.5.42")

	id, err := IdentityFromEnv()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if id.Ordinal != 2 {
		t.Errorf("ordinal: got %d, want 2", id.Ordinal)
	}
	if id.StatefulSetName != "quoll" {
		t.Errorf("sts name: got %q, want %q", id.StatefulSetName, "quoll")
	}
	if id.HeadlessSvc != "quoll-headless" {
		t.Errorf("headless: got %q, want %q", id.HeadlessSvc, "quoll-headless")
	}
	if id.ClusterDomain != "cluster.local" {
		t.Errorf("domain: got %q, want %q", id.ClusterDomain, "cluster.local")
	}

	want := "quoll-2.quoll-headless.production.svc.cluster.local"
	if got := id.FQDN(); got != want {
		t.Errorf("FQDN: got %q, want %q", got, want)
	}

	// Pod-0 is the bootstrap node.
	t.Setenv("POD_NAME", "quoll-0")
	id2, _ := IdentityFromEnv()
	if !id2.IsBootstrapNode() {
		t.Error("quoll-0 should be bootstrap node")
	}
	if id.IsBootstrapNode() {
		t.Error("quoll-2 should NOT be bootstrap node")
	}
}

func TestIdentityFromEnv_Missing(t *testing.T) {
	// Ensure POD_NAME is unset (t.Setenv with "" still sets it).
	os.Unsetenv("POD_NAME")
	if _, err := IdentityFromEnv(); err == nil {
		t.Error("expected error when POD_NAME unset")
	}
}

// fakeResolver is a stub for testing peer discovery without real DNS.
type fakeResolver struct {
	hosts map[string][]string // FQDN → IPs (empty/missing = NXDOMAIN)
}

func (f *fakeResolver) LookupHost(_ context.Context, host string) ([]string, error) {
	ips, ok := f.hosts[host]
	if !ok || len(ips) == 0 {
		return nil, errors.New("no such host")
	}
	return ips, nil
}

func TestDiscoverSeeds_SkipsSelf(t *testing.T) {
	id := PodIdentity{
		PodName:         "quoll-1",
		PodNamespace:    "quoll",
		StatefulSetName: "quoll",
		Ordinal:         1,
		HeadlessSvc:     "quoll-headless",
		ClusterDomain:   "cluster.local",
	}

	resolver := &fakeResolver{
		hosts: map[string][]string{
			"quoll-0.quoll-headless.quoll.svc.cluster.local": {"10.0.0.1"},
			"quoll-1.quoll-headless.quoll.svc.cluster.local": {"10.0.0.2"},
			"quoll-2.quoll-headless.quoll.svc.cluster.local": {"10.0.0.3"},
		},
	}

	d := NewPeerDiscovery(id, 3, 9092, zerolog.Nop())
	d.SetResolver(resolver)

	seeds := d.DiscoverSeeds(context.Background())
	if len(seeds) != 2 {
		t.Fatalf("got %d seeds, want 2 (excluding self)", len(seeds))
	}

	// Verify self is not in the list.
	for _, s := range seeds {
		if strings.Contains(s, "quoll-1.") {
			t.Errorf("self-seed leaked into result: %s", s)
		}
	}
}

func TestDiscoverSeeds_PartialResolution(t *testing.T) {
	// Only pod-0 is resolvable yet — simulates startup race.
	id := PodIdentity{
		PodName:         "quoll-2",
		PodNamespace:    "quoll",
		StatefulSetName: "quoll",
		Ordinal:         2,
		HeadlessSvc:     "quoll-headless",
		ClusterDomain:   "cluster.local",
	}

	resolver := &fakeResolver{
		hosts: map[string][]string{
			"quoll-0.quoll-headless.quoll.svc.cluster.local": {"10.0.0.1"},
			// quoll-1 not resolvable yet
		},
	}

	d := NewPeerDiscovery(id, 3, 9092, zerolog.Nop())
	d.SetResolver(resolver)

	seeds := d.DiscoverSeeds(context.Background())
	if len(seeds) != 1 {
		t.Fatalf("got %d seeds, want 1", len(seeds))
	}
	if !strings.Contains(seeds[0], "quoll-0.") {
		t.Errorf("expected quoll-0 seed, got %s", seeds[0])
	}
}

func TestWaitForPeers_TimeoutReturnsPartial(t *testing.T) {
	id := PodIdentity{
		PodName:         "quoll-1",
		PodNamespace:    "quoll",
		StatefulSetName: "quoll",
		Ordinal:         1,
		HeadlessSvc:     "quoll-headless",
		ClusterDomain:   "cluster.local",
	}

	// No peers ever resolve.
	resolver := &fakeResolver{hosts: map[string][]string{}}

	d := NewPeerDiscovery(id, 3, 9092, zerolog.Nop())
	d.SetResolver(resolver)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	seeds, err := d.WaitForPeers(ctx, 1, 10*time.Millisecond)
	if err == nil {
		t.Error("expected error on timeout")
	}
	if len(seeds) != 0 {
		t.Errorf("expected 0 seeds, got %d", len(seeds))
	}
}
