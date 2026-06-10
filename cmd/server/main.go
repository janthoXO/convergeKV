// Package main is the ConvergeKV server entry point.
// Config parsing and signal handling live here; all wiring is in wire.go.
package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	cenv "github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
)

// config holds all server configuration parsed from environment variables.
//
// IMPORTANT: NUM_PARTITIONS must be identical on every node in the cluster.
// If two nodes disagree on this value they disagree on partition ownership and
// the cluster silently diverges. Treat it as fixed cluster configuration.
type config struct {
	ReplicaID     string   `env:"REPLICA_ID"`
	GRPCPort      int      `env:"GRPC_PORT"          envDefault:"50051"`
	GossipPort    int      `env:"GOSSIP_PORT"        envDefault:"7946"`
	GossipBind    string   `env:"GOSSIP_BIND"        envDefault:"0.0.0.0"`
	Seeds         []string `env:"SEEDS" envSeparator:","`
	DataDir       string   `env:"DATA_DIR"           envDefault:"/data"`
	RF            int      `env:"RF"                 envDefault:"3"`
	NumPartitions int      `env:"NUM_PARTITIONS"     envDefault:"512"`
	SyncMs        int      `env:"SYNC_MS"            envDefault:"2000"`
	IBLTCells     int      `env:"IBLT_CELLS"         envDefault:"512"`
	Debug         bool     `env:"DEBUG"      envDefault:"false"`
	DebugToken    string   `env:"DEBUG_TOKEN"`
}

func main() {
	if err := godotenv.Load(); err != nil && !os.IsNotExist(err) {
		slog.Warn(".env not loaded", "err", err)
	}

	var cfg config
	if err := cenv.Parse(&cfg); err != nil {
		log.Fatalf("config: %v", err)
	}
	if cfg.ReplicaID == "" {
		h, err := os.Hostname()
		if err != nil || h == "" {
			log.Fatalf("config: REPLICA_ID unset and os.Hostname() failed: %v", err)
		}
		cfg.ReplicaID = h
	}
	if cfg.NumPartitions <= 0 {
		log.Fatalf("config: NUM_PARTITIONS must be > 0, got %d", cfg.NumPartitions)
	}
	if cfg.RF < 1 {
		log.Fatalf("config: RF must be >= 1, got %d", cfg.RF)
	}
	if cfg.IBLTCells <= 0 {
		log.Fatalf("config: IBLT_CELLS must be > 0, got %d", cfg.IBLTCells)
	}
	if cfg.SyncMs <= 0 {
		log.Fatalf("config: SYNC_MS must be > 0, got %d", cfg.SyncMs)
	}
	if cfg.Debug && cfg.DebugToken == "" {
		log.Fatalf("config: DEBUG=true requires DEBUG_TOKEN to be set")
	}
	slog.Info("config",
		"replica", cfg.ReplicaID, "grpc", cfg.GRPCPort, "gossip", cfg.GossipPort, "seeds", cfg.Seeds, "dataDir", cfg.DataDir,
		"rf", cfg.RF, "partitions", cfg.NumPartitions, "syncMs", cfg.SyncMs, "ibltCells", cfg.IBLTCells, "debug", cfg.Debug)

	sigCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	sup, err := buildApp(cfg)
	if err != nil {
		log.Fatalf("[%s] startup: %v", cfg.ReplicaID, err)
	}

	if err := sup.Run(sigCtx); err != nil {
		slog.Error("supervisor error", "replica", cfg.ReplicaID, "err", err)
	}
	slog.Info("shutdown complete", "replica", cfg.ReplicaID)
}
