// Package main is the ConvergeKV server entry point.
// It wires together the storage layer, node, replication handler,
// client API handler, and anti-entropy background loop, then serves gRPC.
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	cenv "github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	kvpb "github.com/janthoXO/convergeKV/gen/kv"
	repb "github.com/janthoXO/convergeKV/gen/replication"
	"github.com/janthoXO/convergeKV/internal/api"
	"github.com/janthoXO/convergeKV/internal/node"
	"github.com/janthoXO/convergeKV/internal/replication"
	"github.com/janthoXO/convergeKV/internal/storage"
)

// config holds all server configuration parsed from environment variables.
// Required fields cause a fatal error if unset; optional fields fall back
// to the value specified in the envDefault struct tag.
type config struct {
	ReplicaID string   `env:"REPLICA_ID,required"`
	Peers     []string `env:"PEERS"     envSeparator:","`
	GRPCPort  string   `env:"GRPC_PORT" envDefault:"50051"`
	DataDir   string   `env:"DATA_DIR"  envDefault:"/data"`
	SyncMs    int      `env:"SYNC_MS"   envDefault:"2000"`
}

func main() {
	// Load .env if present; silently ignored when the file doesn't exist.
	// Real environment variables (Docker Compose, systemd, etc.) always take
	// precedence because godotenv.Load does NOT overwrite existing vars.
	if err := godotenv.Load(); err != nil && !os.IsNotExist(err) {
		log.Printf("[config] .env not loaded: %v", err)
	}

	var cfg config
	if err := cenv.Parse(&cfg); err != nil {
		log.Fatalf("config: %v", err)
	}
	log.Printf("[config] replica=%s port=%s peers=%v dataDir=%s syncMs=%d",
		cfg.ReplicaID, cfg.GRPCPort, cfg.Peers, cfg.DataDir, cfg.SyncMs)

	// Storage
	store, err := storage.Open(cfg.DataDir)
	if err != nil {
		log.Fatalf("open storage: %v", err)
	}
	defer store.Close()

	// Node
	n, err := node.New(cfg.ReplicaID, store)
	if err != nil {
		log.Fatalf("create node: %v", err)
	}

	// Causal context
	causal := replication.NewCausalContext()

	// gRPC server
	srv := grpc.NewServer()
	kvpb.RegisterKVServiceServer(srv, api.NewHandler(n, cfg.Peers))
	repb.RegisterReplicationServiceServer(srv, replication.NewHandler(n))
	// Register reflection so grpcurl and other tools can discover services.
	reflection.Register(srv)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", cfg.GRPCPort))
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	log.Printf("[%s] listening on :%s  peers=%v", cfg.ReplicaID, cfg.GRPCPort, cfg.Peers)

	// Anti-entropy
	ae := replication.NewAntiEntropy(n, cfg.Peers, causal, time.Duration(cfg.SyncMs)*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go ae.Run(ctx)

	// Serve (blocking)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
