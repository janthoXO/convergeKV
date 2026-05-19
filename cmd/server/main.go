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
	"os/signal"
	"strings"
	"syscall"
	"time"

	cenv "github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	fwdpb "github.com/janthoXO/convergeKV/gen/forward"
	kvpb "github.com/janthoXO/convergeKV/gen/kv"
	repb "github.com/janthoXO/convergeKV/gen/replication"
	"github.com/janthoXO/convergeKV/internal/api"
	"github.com/janthoXO/convergeKV/internal/connpool"
	"github.com/janthoXO/convergeKV/internal/coordinator"
	"github.com/janthoXO/convergeKV/internal/gossip"
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
	Seeds      string `env:"SEEDS"` // comma-separated gossip host:port; empty for single-node
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

	// ── 1. Storage ─────────────────────────────────────────────────────────────
	store, err := storage.Open(cfg.DataDir)
	if err != nil {
		log.Fatalf("open storage: %v", err)
	}

	// ── 2. HLC floor — find highest persisted timestamp ───────────────────────
	// If the system clock was corrected backwards (NTP), a freshly initialised
	// HLC could issue timestamps older than ones already written to storage,
	// silently breaking LWW causality. Seeding from the highest persisted
	// timestamp guarantees the clock stays monotone across restarts.
	hlcFloor, err := store.MaxTimestamp()
	if err != nil {
		log.Fatalf("scan HLC floor: %v", err)
	}
	log.Printf("[hlc] seeded floor ts=%d.%d", hlcFloor.PhysicalMs, hlcFloor.Logical)

	// ── 3. Node ────────────────────────────────────────────────────────────────
	n, err := node.New(cfg.ReplicaID, store, node.WithHLCFloor(hlcFloor))
	if err != nil {
		log.Fatalf("create node: %v", err)
	}

	// ── 4. IBLT State — build by streaming persisted data from Badger ─────────
	ibltState, err := syncer.BuildFromStore(store, cfg.IBLTCells)
	if err != nil {
		log.Fatalf("build IBLT: %v", err)
	}
	n.SetIBLTState(ibltState)
	log.Printf("[iblt] built IBLT from persisted records (%d cells)", cfg.IBLTCells)

	// ── 5. Seeds ───────────────────────────────────────────────────────────────
	var seeds []string
	if cfg.Seeds != "" {
		for _, s := range strings.Split(cfg.Seeds, ",") {
			s = strings.TrimSpace(s)
			if s != "" {
				seeds = append(seeds, s)
			}
		}
	}

	// ── 6. Shared gRPC connection pool ────────────────────────────────────────
	pool := connpool.New()

	// ── 6. Gossip ─────────────────────────────────────────────────────────────
	// OnChange evicts connections for departed peers; HRW reads Members() live.
	g, err := gossip.Start(gossip.Config{
		BindAddr:  cfg.GossipBind,
		BindPort:  cfg.GossipPort,
		LocalMeta: gossip.NodeMeta{ReplicaID: cfg.ReplicaID, GRPCPort: cfg.GRPCPort},
		Seeds:     seeds,
		OnChange: func(members []gossip.MemberInfo) {
			log.Printf("[gossip] membership changed: %d members", len(members))
			keep := make(map[string]struct{}, len(members))
			for _, m := range members {
				keep[m.GRPCAddr] = struct{}{}
			}
			pool.EvictAbsent(keep)
		},
	})
	if err != nil {
		log.Fatalf("start gossip: %v", err)
	}

	// ── 7. Signal context (used by syncer, coordinator, and serve loop) ────────
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// ── 8. Syncer ─────────────────────────────────────────────────────────────
	sync := syncer.NewSyncer(n, g, ibltState, store, pool, time.Duration(cfg.SyncMs)*time.Millisecond)

	// ── 9. Forwarder and Coordinator ──────────────────────────────────────────
	fwd := coordinator.NewForwarder(pool)

	coord := coordinator.New(ctx, n, g, fwd, sync, cfg.RF)

	// ── 10. gRPC server ───────────────────────────────────────────────────────
	srv := grpc.NewServer()
	kvpb.RegisterKVServiceServer(srv, api.NewHandler(coord, n, g))
	fwdpb.RegisterForwardServiceServer(srv, api.NewForwardHandler(coord))
	repb.RegisterSyncServiceServer(srv, syncer.NewHandler(n, ibltState, store))
	reflection.Register(srv)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.GRPCPort))
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	log.Printf("[%s] gRPC listening on :%d", cfg.ReplicaID, cfg.GRPCPort)

	// ── 11. Anti-entropy loop ─────────────────────────────────────────────────
	go sync.Run(ctx)

	// ── 12. Serve (blocking until signal) ─────────────────────────────────────
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(lis) }()

	select {
	case err := <-serveErr:
		log.Fatalf("serve: %v", err)
	case <-ctx.Done():
		log.Printf("[%s] shutting down…", cfg.ReplicaID)
	}

	// Shutdown order: stop RPCs → drain push goroutines → pool → gossip → storage
	srv.GracefulStop()
	coord.Close()
	pool.Close()
	g.Leave(3 * time.Second)
	store.Close()
}
