// Package config loads node configuration from a YAML file with environment
// variable overrides (CONVERGEKV_* takes precedence over the file).
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/memberlist"
	"gopkg.in/yaml.v3"
)

type Config struct {
	// DataDir holds the node's persistent state (identity, Pebble DB).
	DataDir string `yaml:"data_dir"`

	// ClientAddr is the listen address for the client-facing gRPC service.
	ClientAddr string `yaml:"client_addr"`
	// NodeAddr is the listen address for the node-facing gRPC service.
	NodeAddr string `yaml:"node_addr"`
	// GossipAddr is the bind address for memberlist gossip.
	GossipAddr string `yaml:"gossip_addr"`
	// AdminAddr serves Prometheus metrics and pprof; empty disables it.
	AdminAddr string `yaml:"admin_addr"`
	// AdvertiseAddr is the address other nodes use to reach this one.
	// Empty means derive from the bind address.
	AdvertiseAddr string `yaml:"advertise_addr"`

	// Seeds are gossip addresses of existing cluster members to join.
	// Empty means bootstrap a new cluster.
	Seeds []string `yaml:"seeds"`

	// Partitions is the fixed cluster-wide partition count P. It is set at
	// cluster bootstrap and gossiped; a mismatch with the cluster is fatal.
	Partitions uint16 `yaml:"partitions"`

	// CrashGracePeriod is how long a dead node keeps its ownership before
	// placement promotes a successor.
	CrashGracePeriod time.Duration `yaml:"crash_grace_period"`
	// AntiEntropyInterval is the base interval between Merkle exchange
	// rounds per partition (jittered).
	AntiEntropyInterval time.Duration `yaml:"anti_entropy_interval"`

	LogLevel string `yaml:"log_level"`

	// MemberlistConfig optionally overrides gossip tuning (tests use the
	// fast local profile). Not loadable from YAML.
	MemberlistConfig *memberlist.Config `yaml:"-"`
}

func Default() Config {
	return Config{
		DataDir:             "data",
		ClientAddr:          ":7000",
		NodeAddr:            ":7001",
		GossipAddr:          ":7946",
		AdminAddr:           ":7002",
		Partitions:          256,
		CrashGracePeriod:    10 * time.Minute,
		AntiEntropyInterval: 45 * time.Second,
		LogLevel:            "info",
	}
}

// Load reads the YAML file at path (skipped if path is empty), then applies
// environment overrides, then validates.
func Load(path string) (Config, error) {
	cfg := Default()
	if path != "" {
		b, err := os.ReadFile(path)
		if err != nil {
			return Config{}, fmt.Errorf("read config: %w", err)
		}
		if err := yaml.Unmarshal(b, &cfg); err != nil {
			return Config{}, fmt.Errorf("parse config: %w", err)
		}
	}
	if err := cfg.applyEnv(); err != nil {
		return Config{}, err
	}
	if err := cfg.validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c *Config) applyEnv() error {
	str := func(key string, dst *string) {
		if v, ok := os.LookupEnv(key); ok {
			*dst = v
		}
	}
	str("CONVERGEKV_DATA_DIR", &c.DataDir)
	str("CONVERGEKV_CLIENT_ADDR", &c.ClientAddr)
	str("CONVERGEKV_NODE_ADDR", &c.NodeAddr)
	str("CONVERGEKV_GOSSIP_ADDR", &c.GossipAddr)
	str("CONVERGEKV_ADMIN_ADDR", &c.AdminAddr)
	str("CONVERGEKV_ADVERTISE_ADDR", &c.AdvertiseAddr)
	str("CONVERGEKV_LOG_LEVEL", &c.LogLevel)

	if v, ok := os.LookupEnv("CONVERGEKV_SEEDS"); ok {
		c.Seeds = splitNonEmpty(v)
	}
	if v, ok := os.LookupEnv("CONVERGEKV_PARTITIONS"); ok {
		p, err := strconv.ParseUint(v, 10, 16)
		if err != nil {
			return fmt.Errorf("CONVERGEKV_PARTITIONS: %w", err)
		}
		c.Partitions = uint16(p)
	}
	dur := func(key string, dst *time.Duration) error {
		v, ok := os.LookupEnv(key)
		if !ok {
			return nil
		}
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("%s: %w", key, err)
		}
		*dst = d
		return nil
	}
	if err := dur("CONVERGEKV_CRASH_GRACE_PERIOD", &c.CrashGracePeriod); err != nil {
		return err
	}
	return dur("CONVERGEKV_ANTI_ENTROPY_INTERVAL", &c.AntiEntropyInterval)
}

func (c *Config) validate() error {
	if c.DataDir == "" {
		return fmt.Errorf("data_dir must not be empty")
	}
	if c.Partitions == 0 || c.Partitions&(c.Partitions-1) != 0 {
		return fmt.Errorf("partitions must be a power of two, got %d", c.Partitions)
	}
	if c.Partitions > 1024 {
		// Per-partition status flags must fit memberlist's 512-byte
		// metadata cap (see internal/cluster.NodeMeta).
		return fmt.Errorf("partitions must be <= 1024, got %d", c.Partitions)
	}
	return nil
}

func splitNonEmpty(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}
