package config

import (
	"strings"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	DataDir  string         `mapstructure:"data_dir"`
	LogLevel string         `mapstructure:"log_level"`
	Engine   EngineConfig   `mapstructure:"engine"`
	GRPC     GRPCConfig     `mapstructure:"grpc"`
	TLS      TLSConfig      `mapstructure:"tls"`
	Raft     RaftConfig     `mapstructure:"raft"`
	Serf     SerfConfig     `mapstructure:"serf"`
	HashRing HashRingConfig `mapstructure:"hashring"`
	K8s      K8sConfig      `mapstructure:"k8s"`
	Health   HealthConfig   `mapstructure:"health"`
}

type EngineConfig struct {
	WALDir       string `mapstructure:"wal_dir"`
	WALSyncMode  string `mapstructure:"wal_sync_mode"`
	MaxKeySize   int    `mapstructure:"max_key_size"`
	MaxValueSize int    `mapstructure:"max_value_size"`
}

type GRPCConfig struct {
	ListenAddr     string        `mapstructure:"listen_addr"`
	AdvertiseAddr  string        `mapstructure:"advertise_addr"` // peers see this (K8s FQDN)
	RequestTimeout time.Duration `mapstructure:"request_timeout"`
}

type TLSConfig struct {
	Enabled     bool   `mapstructure:"enabled"`
	CertFile    string `mapstructure:"cert_file"`
	KeyFile     string `mapstructure:"key_file"`
	CAFile      string `mapstructure:"ca_file"`
	MTLSEnabled bool   `mapstructure:"mtls_enabled"`
}

type RaftConfig struct {
	NodeID             string        `mapstructure:"node_id"`
	BindAddr           string        `mapstructure:"bind_addr"`
	AdvertiseAddr      string        `mapstructure:"advertise_addr"` // peer-visible addr (K8s FQDN)
	DataDir            string        `mapstructure:"data_dir"`
	Bootstrap          bool          `mapstructure:"bootstrap"`
	HeartbeatTimeout   time.Duration `mapstructure:"heartbeat_timeout"`
	ElectionTimeout    time.Duration `mapstructure:"election_timeout"`
	LeaderLeaseTimeout time.Duration `mapstructure:"leader_lease_timeout"`
	CommitTimeout      time.Duration `mapstructure:"commit_timeout"`
	SnapshotInterval   time.Duration `mapstructure:"snapshot_interval"`
	SnapshotThreshold  uint64        `mapstructure:"snapshot_threshold"`
	TrailingLogs       uint64        `mapstructure:"trailing_logs"`
}

type SerfConfig struct {
	NodeName      string            `mapstructure:"node_name"`
	BindAddr      string            `mapstructure:"bind_addr"`
	AdvertiseAddr string            `mapstructure:"advertise_addr"` // K8s peer-visible addr
	Tags          map[string]string `mapstructure:"tags"`
	JoinAddrs     []string          `mapstructure:"join_addrs"`
}

type HashRingConfig struct {
	VnodeCount int `mapstructure:"vnode_count"`
}

// K8sConfig activates Kubernetes-aware boot mode.
//
// When Enabled=true, the node reads POD_NAME / POD_NAMESPACE from the
// environment, derives its identity, and uses headless-service DNS for peer
// discovery. Explicit NodeID / BindAddr / JoinAddrs values in config files are
// overridden with values derived from the pod's identity.
type K8sConfig struct {
	Enabled          bool          `mapstructure:"enabled"`
	ExpectedReplicas int           `mapstructure:"expected_replicas"`
	DiscoveryTimeout time.Duration `mapstructure:"discovery_timeout"`
	DiscoveryRetry   time.Duration `mapstructure:"discovery_retry"`
}

// HealthConfig sets the liveness/readiness HTTP server address and paths.
type HealthConfig struct {
	ListenAddr    string `mapstructure:"listen_addr"`
	LivenessPath  string `mapstructure:"liveness_path"`
	ReadinessPath string `mapstructure:"readiness_path"`
}

// Load reads config from the given file and merges in environment overrides.
// Keys follow DKV_ prefix convention: raft.bind_addr → DKV_RAFT_BIND_ADDR.
func Load(path string) (Config, error) {
	v := viper.New()
	setDefaults(v)

	if path != "" {
		v.SetConfigFile(path)
		if err := v.ReadInConfig(); err != nil {
			return Config{}, err
		}
	}

	// Env var binding: DKV_RAFT_BIND_ADDR → raft.bind_addr, etc.
	v.SetEnvPrefix("DKV")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func setDefaults(v *viper.Viper) {
	v.SetDefault("data_dir", "/tmp/dkv")
	v.SetDefault("log_level", "info")

	v.SetDefault("engine.wal_dir", "/tmp/dkv/wal")
	v.SetDefault("engine.wal_sync_mode", "always")
	v.SetDefault("engine.max_key_size", 1024)
	v.SetDefault("engine.max_value_size", 1048576)

	v.SetDefault("grpc.listen_addr", ":9090")
	v.SetDefault("grpc.request_timeout", "5s")

	v.SetDefault("tls.enabled", false)
	v.SetDefault("tls.mtls_enabled", false)

	v.SetDefault("raft.bind_addr", ":9091")
	v.SetDefault("raft.bootstrap", false)
	v.SetDefault("raft.heartbeat_timeout", "1s")
	v.SetDefault("raft.election_timeout", "1s")
	v.SetDefault("raft.leader_lease_timeout", "500ms")
	v.SetDefault("raft.commit_timeout", "50ms")
	v.SetDefault("raft.snapshot_interval", "2m")
	v.SetDefault("raft.snapshot_threshold", 8192)
	v.SetDefault("raft.trailing_logs", 10240)

	v.SetDefault("serf.bind_addr", ":9092")

	v.SetDefault("hashring.vnode_count", 128)

	v.SetDefault("k8s.enabled", false)
	v.SetDefault("k8s.expected_replicas", 3)
	v.SetDefault("k8s.discovery_timeout", "30s")
	v.SetDefault("k8s.discovery_retry", "2s")

	v.SetDefault("health.listen_addr", ":9093")
	v.SetDefault("health.liveness_path", "/healthz")
	v.SetDefault("health.readiness_path", "/ready")
}
