// Package main is the ConvergeKV server entry point.
// It wires together gossip-based cluster membership, rendezvous hashing for
// replica placement, IBLT-based anti-entropy, and the gRPC service layer.
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"

	cenv "github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	fwdpb "github.com/janthoXO/convergeKV/gen/forward"
	kvpb "github.com/janthoXO/convergeKV/gen/kv"
	repb "github.com/janthoXO/convergeKV/gen/replication"
	"github.com/janthoXO/convergeKV/internal/api"
	"github.com/janthoXO/convergeKV/internal/coordinator"
	"github.com/janthoXO/convergeKV/internal/gossip"
	"github.com/janthoXO/convergeKV/internal/iblt"
	"github.com/janthoXO/convergeKV/internal/node"
	"github.com/janthoXO/convergeKV/internal/storage"
	"github.com/janthoXO/convergeKV/internal/syncer"
)

// config holds all server configuration parsed from environment variables.
type config struct {
	ReplicaID  string `env:"REPLICA_ID,required"`
	GRPCPort   int    `env:"GRPC_PORT"     envDefault:"50051"`
	GossipPort int    `env:"GOSSIP_PORT"   envDefault:"7946"`
	GossipBind string `env:"GOSSIP_BIND"   envDefault:"0.0.0.0"`
	Seeds      string `env:"SEEDS"`        // comma-separated gossip host:port; empty for single-node
	DataDir    string `env:"DATA_DIR"      envDefault:"/data"`
	RF         int    `env:"RF"            envDefault:"3"`
	SyncMs     int    `env:"SYNC_MS"       envDefault:"2000"`
	IBLTCells  int    `env:"IBLT_CELLS"    envDefault:"512"`
}

func main() {
	// Load .env if present; silently ignored when the file doesn't exist.
	if err := godotenv.Load(); err != nil && !os.IsNotExist(err) {
		log.Printf("[config] .env not loaded: %v", err)
	}

	var cfg config
	if err := cenv.Parse(&cfg); err != nil {
		log.Fatalf("config: %v", err)
	}
	log.Printf("[config] replica=%s grpc=%d gossip=%d seeds=%q dataDir=%s rf=%d syncMs=%d ibltCells=%d",
		cfg.ReplicaID, cfg.GRPCPort, cfg.GossipPort, cfg.Seeds, cfg.DataDir, cfg.RF, cfg.SyncMs, cfg.IBLTCells)

	// Enforce the default cell count minimum.
	if cfg.IBLTCells <= 0 {
		cfg.IBLTCells = iblt.DefaultCells
	}

	// ── 1. Storage ─────────────────────────────────────────────────────────────
	store, err := storage.Open(cfg.DataDir)
	if err != nil {
		log.Fatalf("open storage: %v", err)
	}
	defer store.Close()

	// ── 2. Node ────────────────────────────────────────────────────────────────
	n, err := node.New(cfg.ReplicaID, store)
	if err != nil {
		log.Fatalf("create node: %v", err)
	}

	// ── 3. IBLT State — build from persisted node data ────────────────────────
	ibltState := syncer.BuildFromSnapshot(n.Snapshot(), cfg.IBLTCells)
	n.SetIBLTState(ibltState)
	log.Printf("[iblt] built IBLT from %d existing records (%d cells)", len(n.Snapshot()), cfg.IBLTCells)

	// ── 4. Seeds ───────────────────────────────────────────────────────────────
	var seeds []string
	if cfg.Seeds != "" {
		for _, s := range strings.Split(cfg.Seeds, ",") {
			s = strings.TrimSpace(s)
			if s != "" {
				seeds = append(seeds, s)
			}
		}
	}

	// ── 5. Gossip ─────────────────────────────────────────────────────────────
	// OnChange is a no-op: HRW reads gossip.Members() on every request.
	g, err := gossip.Start(gossip.Config{
		BindAddr:  cfg.GossipBind,
		BindPort:  cfg.GossipPort,
		LocalMeta: gossip.NodeMeta{ReplicaID: cfg.ReplicaID, GRPCPort: cfg.GRPCPort},
		Seeds:     seeds,
		OnChange: func(members []gossip.MemberInfo) {
			log.Printf("[gossip] membership changed: %d members", len(members))
		},
	})
	if err != nil {
		log.Fatalf("start gossip: %v", err)
	}
	defer g.Leave(3 * time.Second)

	// ── 6. Syncer ─────────────────────────────────────────────────────────────
	sync := syncer.NewSyncer(n, g, ibltState, cfg.RF, time.Duration(cfg.SyncMs)*time.Millisecond)
	defer sync.Close()

	// ── 7. Forwarder and Coordinator ──────────────────────────────────────────
	fwd := coordinator.NewForwarder()
	defer fwd.Close()

	coord := coordinator.New(n, g, fwd, sync, cfg.RF)

	// ── 8. gRPC server ────────────────────────────────────────────────────────
	srv := grpc.NewServer()
	kvpb.RegisterKVServiceServer(srv, api.NewHandler(coord, n, seeds))
	fwdpb.RegisterForwardServiceServer(srv, api.NewForwardHandler(coord))
	repb.RegisterSyncServiceServer(srv, syncer.NewHandler(n, ibltState))
	reflection.Register(srv)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.GRPCPort))
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	log.Printf("[%s] gRPC listening on :%d", cfg.ReplicaID, cfg.GRPCPort)

	// ── 9. Anti-entropy loop ──────────────────────────────────────────────────
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go sync.Run(ctx)

	// ── 10. Serve (blocking) ──────────────────────────────────────────────────
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
