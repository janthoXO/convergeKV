// kvnode is the convergeKV node binary: it loads configuration, establishes
// the node identity, wires the subsystems together, and runs until signalled.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/janthoXO/convergeKV/internal/config"
	"github.com/janthoXO/convergeKV/internal/identity"
)

func main() {
	if err := run(); err != nil {
		slog.Error("node exited with error", "err", err)
		os.Exit(1)
	}
}

func run() error {
	configPath := flag.String("config", "", "path to YAML config file (optional)")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	setupLogging(cfg.LogLevel)

	nodeID, err := identity.LoadOrCreate(cfg.DataDir)
	if err != nil {
		return fmt.Errorf("node identity: %w", err)
	}
	slog.Info("node starting",
		"node_id", nodeID,
		"data_dir", cfg.DataDir,
		"partitions", cfg.Partitions,
		"client_addr", cfg.ClientAddr,
		"node_addr", cfg.NodeAddr,
		"gossip_addr", cfg.GossipAddr,
	)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Subsystem wiring (storage, cluster, placement, coordinator, ...) is
	// added milestone by milestone; until then the node just idles.
	<-ctx.Done()

	slog.Info("shutdown signal received, stopping")
	return nil
}

func setupLogging(level string) {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		lvl = slog.LevelInfo
	}
	h := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: lvl})
	slog.SetDefault(slog.New(h))
}
