// Package main is the ConvergeKV server entry point.
// It wires together gossip-based cluster membership, rendezvous hashing for
// replica placement over virtual partitions, per-partition IBLT-based
// anti-entropy, and the gRPC service layer.
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
	"github.com/janthoXO/convergeKV/internal/iblt"
	"github.com/janthoXO/convergeKV/internal/node"
	"github.com/janthoXO/convergeKV/internal/storage"
	"github.com/janthoXO/convergeKV/internal/syncer"
)

// config holds all server configuration parsed from environment variables.
//
// IMPORTANT: NUM_PARTITIONS is part of the placement function and must be
// identical on every node in the cluster. If two nodes disagree on this value
// they disagree on partition ownership and the cluster silently diverges.
// Treat it as fixed cluster configuration; changing it requires a fresh data
// directory on all nodes.
type config struct {
	ReplicaID     string `env:"REPLICA_ID"` // optional; falls back to os.Hostname()
	GRPCPort      int    `env:"GRPC_PORT"          envDefault:"50051"`
	GossipPort    int    `env:"GOSSIP_PORT"        envDefault:"7946"`
	GossipBind    string `env:"GOSSIP_BIND"        envDefault:"0.0.0.0"`
	Seeds         string `env:"SEEDS"` // comma-separated gossip host:port; empty for single-node
	DataDir       string `env:"DATA_DIR"           envDefault:"/data"`
	RF            int    `env:"RF"                 envDefault:"3"`
	NumPartitions int    `env:"NUM_PARTITIONS"     envDefault:"512"`
	SyncMs        int    `env:"SYNC_MS"            envDefault:"2000"`
	IBLTCells     int    `env:"IBLT_CELLS"         envDefault:"512"` // cells per partition
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
	log.Printf("[config] replica=%s grpc=%d gossip=%d seeds=%q dataDir=%s rf=%d numPartitions=%d syncMs=%d ibltCells=%d",
		cfg.ReplicaID, cfg.GRPCPort, cfg.GossipPort, cfg.Seeds, cfg.DataDir, cfg.RF, cfg.NumPartitions, cfg.SyncMs, cfg.IBLTCells)

	// ── 1. Storage ─────────────────────────────────────────────────────────────
	store, err := storage.Open(cfg.DataDir)
	if err != nil {
		log.Fatalf("open storage: %v", err)
	}

	// ── 2. HLC floor — find highest persisted timestamp ───────────────────────
	hlcFloor, err := store.MaxTimestamp()
	if err != nil {
		log.Fatalf("scan HLC floor: %v", err)
	}
	log.Printf("[hlc] seeded floor ts=%d.%d", hlcFloor.PhysicalMs, hlcFloor.Logical)

	// ── 3. Seeds ───────────────────────────────────────────────────────────────
	var seeds []string
	if cfg.Seeds != "" {
		for s := range strings.SplitSeq(cfg.Seeds, ",") {
			s = strings.TrimSpace(s)
			if s == "" {
				continue
			}
			seeds = append(seeds, s)
		}
	}

	// ── 4. Shared gRPC connection pool ────────────────────────────────────────
	pool := connpool.New()

	// ── 5. Ownership cache ────────────────────────────────────────────────────
	ownership := syncer.NewOwnership(cfg.ReplicaID, cfg.RF, cfg.NumPartitions)

	// ── 6. Signal context ─────────────────────────────────────────────────────
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// ── 7. Gossip ─────────────────────────────────────────────────────────────
	// OnChange evicts connections for departed peers and refreshes ownership.
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
			go pool.EvictAbsent(keep)
			go ownership.Update(members)
		},
	})
	if err != nil {
		log.Fatalf("start gossip: %v", err)
	}

	// Compute initial ownership from the first gossip view (populated by gossip.Start).
	ownership.Update(g.Members())

	// ── 8. IBLT State — build per owned partition from Badger ─────────────────
	ownedPids := ownership.Owned()
	ibltState, err := iblt.BuildFromStore(store, ownedPids, cfg.IBLTCells)
	if err != nil {
		log.Fatalf("build IBLT: %v", err)
	}
	log.Printf("[iblt] built per-partition IBLTs for %d owned partitions (%d cells each)", len(ownedPids), cfg.IBLTCells)

	// ── 9. Node ────────────────────────────────────────────────────────────────
	n := node.New(cfg.ReplicaID, store, ibltState, node.WithHLCFloor(hlcFloor))

	// ── 10. Syncer ────────────────────────────────────────────────────────────
	sync := syncer.New(n, g, pool, ownership, cfg.NumPartitions,
		syncer.WithContext(ctx),
		syncer.WithSyncInterval(time.Duration(cfg.SyncMs)*time.Millisecond),
	)

	// ── 11. Forwarder and Coordinator ─────────────────────────────────────────
	fwd := coordinator.NewForwarder(pool)
	coord := coordinator.New(n, g, fwd, sync, cfg.RF, cfg.NumPartitions)

	// ── 12. gRPC server ───────────────────────────────────────────────────────
	srv := grpc.NewServer()
	kvpb.RegisterKVServiceServer(srv, api.NewHandler(coord, n, g))
	fwdpb.RegisterForwardServiceServer(srv, api.NewForwardHandler(coord))
	repb.RegisterSyncServiceServer(srv, syncer.NewHandler(n))
	reflection.Register(srv)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.GRPCPort))
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	log.Printf("[%s] gRPC listening on :%d", cfg.ReplicaID, cfg.GRPCPort)

	// ── 13. Anti-entropy loop ─────────────────────────────────────────────────
	sync.Run()

	// ── 14. Serve (blocking until signal) ─────────────────────────────────────
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(lis) }()

	select {
	case err := <-serveErr:
		log.Fatalf("serve: %v", err)
	case <-ctx.Done():
		log.Printf("[%s] shutting down…", cfg.ReplicaID)
	}

	// Shutdown order: stop RPCs → drain syncer goroutines → pool → gossip → storage
	srv.GracefulStop()
	sync.Close()
	pool.Close()
	g.Leave(3 * time.Second)
	store.Close()
}
