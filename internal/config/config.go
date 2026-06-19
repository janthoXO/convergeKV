// Package config loads node configuration from CONVERGEKV_* environment
// variables into a struct (github.com/caarlos0/env).
package config

import (
	"fmt"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/hashicorp/memberlist"
)

type Config struct {
	// DataDir holds the node's persistent state (identity, Pebble DB).
	DataDir string `env:"DATA_DIR"`

	// ClientAddr is the listen address for the client-facing gRPC service.
	ClientAddr string `env:"CLIENT_ADDR"`
	// NodeAddr is the listen address for the node-facing gRPC service.
	NodeAddr string `env:"NODE_ADDR"`
	// GossipAddr is the bind address for memberlist gossip.
	GossipAddr string `env:"GOSSIP_ADDR"`
	// AdminAddr serves Prometheus metrics and pprof; empty disables it.
	AdminAddr string `env:"ADMIN_ADDR"`
	// AdvertiseAddr is the address other nodes use to reach this one.
	// Empty means derive from the bind address.
	AdvertiseAddr string `env:"ADVERTISE_ADDR"`

	// Seeds are gossip addresses of existing cluster members to join
	// (comma-separated). Empty means bootstrap a new cluster.
	Seeds []string `env:"SEEDS"`

	// Partitions is the fixed cluster-wide partition count P. It is set at
	// cluster bootstrap and gossiped; a mismatch with the cluster is fatal.
	Partitions uint16 `env:"PARTITIONS"`

	// CrashGracePeriod is how long a dead node keeps its ownership before
	// placement promotes a successor.
	CrashGracePeriod time.Duration `env:"CRASH_GRACE_PERIOD"`
	// AntiEntropyInterval is the base interval between Merkle exchange
	// rounds per partition (jittered).
	AntiEntropyInterval time.Duration `env:"ANTI_ENTROPY_INTERVAL"`
	// ReplicationMaxAge is how long a delta may sit in a peer's fan-out
	// retry queue before being dropped to the anti-entropy backstop. It
	// must be shorter than the shortest possible gap between two AE rounds
	// (AntiEntropyInterval/2, the jitter low bound): GC certifies on two
	// adjacent clean rounds, and no stale delta may outlive that.
	ReplicationMaxAge time.Duration `env:"REPLICATION_MAX_AGE"`

	LogLevel string `env:"LOG_LEVEL"`

	// MemberlistConfig optionally overrides gossip tuning (tests use the
	// fast local profile). Not loadable from the environment.
	MemberlistConfig *memberlist.Config `env:"-"`
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
		ReplicationMaxAge:   20 * time.Second,
		LogLevel:            "info",
	}
}

// Load starts from the defaults, applies CONVERGEKV_* environment variables,
// and validates the result.
func Load() (Config, error) {
	cfg := Default()
	if err := env.ParseWithOptions(&cfg, env.Options{Prefix: "CONVERGEKV_"}); err != nil {
		return Config{}, fmt.Errorf("parse environment: %w", err)
	}
	if err := cfg.validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c *Config) validate() error {
	if c.DataDir == "" {
		return fmt.Errorf("CONVERGEKV_DATA_DIR must not be empty")
	}
	if c.Partitions == 0 || c.Partitions&(c.Partitions-1) != 0 {
		return fmt.Errorf("partitions must be a power of two, got %d", c.Partitions)
	}
	if c.Partitions > 1024 {
		// Per-partition status flags must fit memberlist's 512-byte
		// metadata cap (see internal/cluster.NodeMeta).
		return fmt.Errorf("partitions must be <= 1024, got %d", c.Partitions)
	}
	if c.AntiEntropyInterval/2 <= c.ReplicationMaxAge {
		// GC certification needs every stale delta gone before two adjacent
		// AE rounds can both come up clean (jitter low bound = interval/2).
		return fmt.Errorf("anti-entropy interval/2 (%v) must exceed replication max age (%v)",
			c.AntiEntropyInterval/2, c.ReplicationMaxAge)
	}
	return nil
}
