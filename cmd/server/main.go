// Package main is the ConvergeKV server entry point.
// It wires together gossip-based cluster membership, a consistent-hash ring,
// a coordinator for request routing, and the anti-entropy replication loop.
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
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
	"github.com/janthoXO/convergeKV/internal/node"
	"github.com/janthoXO/convergeKV/internal/replication"
	"github.com/janthoXO/convergeKV/internal/ring"
	"github.com/janthoXO/convergeKV/internal/storage"
)

// config holds all server configuration parsed from environment variables.
type config struct {
	ReplicaID   string `env:"REPLICA_ID,required"`
	GRPCPort    string `env:"GRPC_PORT"    envDefault:"50051"`
	GossipPort  int    `env:"GOSSIP_PORT"  envDefault:"7946"`
	GossipBind  string `env:"GOSSIP_BIND"  envDefault:"0.0.0.0"`
	Seeds       string `env:"SEEDS"` // comma-separated host:port; empty for single-node
	DataDir     string `env:"DATA_DIR"     envDefault:"/data"`
	RF          int    `env:"RF"           envDefault:"3"`
	SyncMs      int    `env:"SYNC_MS"      envDefault:"2000"`
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
	log.Printf("[config] replica=%s grpc=%s gossip=%d seeds=%q dataDir=%s rf=%d syncMs=%d",
		cfg.ReplicaID, cfg.GRPCPort, cfg.GossipPort, cfg.Seeds, cfg.DataDir, cfg.RF, cfg.SyncMs)

	// ── 1. Parse GRPC port as int for gossip metadata ─────────────────────────
	grpcPortInt, err := strconv.Atoi(cfg.GRPCPort)
	if err != nil {
		log.Fatalf("invalid GRPC_PORT %q: %v", cfg.GRPCPort, err)
	}

	// ── 2. Storage ─────────────────────────────────────────────────────────────
	store, err := storage.Open(cfg.DataDir)
	if err != nil {
		log.Fatalf("open storage: %v", err)
	}
	defer store.Close()

	// ── 3. Node (ring not yet set) ─────────────────────────────────────────────
	n, err := node.New(cfg.ReplicaID, store)
	if err != nil {
		log.Fatalf("create node: %v", err)
	}

	// ── 4. Ring (empty on startup) ─────────────────────────────────────────────
	r := ring.NewRing()

	// ── 5. Gossip + OnChange callback that rebuilds the ring ───────────────────
	var seeds []string
	if cfg.Seeds != "" {
		for _, s := range strings.Split(cfg.Seeds, ",") {
			s = strings.TrimSpace(s)
			if s != "" {
				seeds = append(seeds, s)
			}
		}
	}

	onMemberChange := func(members []gossip.MemberInfo) {
		ringMembers := make([]ring.Member, len(members))
		for i, m := range members {
			ringMembers[i] = ring.Member{
				ReplicaID: m.ReplicaID,
				GRPCAddr:  m.GRPCAddr,
			}
		}
		r.Rebuild(ringMembers, cfg.RF)
		log.Printf("[ring] rebuilt with %d members", len(ringMembers))
	}

	g, err := gossip.Start(gossip.Config{
		BindAddr:  cfg.GossipBind,
		BindPort:  cfg.GossipPort,
		LocalMeta: gossip.NodeMeta{ReplicaID: cfg.ReplicaID, GRPCPort: grpcPortInt},
		Seeds:     seeds,
		OnChange:  onMemberChange,
	})
	if err != nil {
		log.Fatalf("start gossip: %v", err)
	}
	defer g.Leave(3 * time.Second)

	// Bootstrap the ring with just ourselves so the node can handle requests
	// before gossip converges.
	onMemberChange(g.Members())

	// ── 6. Inject ring into node ───────────────────────────────────────────────
	n.SetRing(r)

	// ── 7. Forwarder and Coordinator ──────────────────────────────────────────
	fwd := coordinator.NewForwarder()
	defer fwd.Close()

	coord := coordinator.New(n, r, fwd)

	// ── 8. Causal context + anti-entropy ──────────────────────────────────────
	causal := replication.NewCausalContext()

	// Derive peer list from initial gossip view; it is refreshed when gossip
	// fires the OnChange callback but we keep a separate list for anti-entropy
	// since AntiEntropy needs host:grpc_port addresses.
	// For now AntiEntropy will re-read the ring's member list on each tick
	// via the peers closure below (Release 3 will make this fully dynamic).
	// For Release 2, pass peers as initially known (pre-gossip convergence they
	// may be empty; anti-entropy tolerates failed dials with a log and retry).
	peerAddrs := func() []string {
		members := g.Members()
		addrs := make([]string, 0, len(members))
		for _, m := range members {
			if m.ReplicaID != cfg.ReplicaID {
				addrs = append(addrs, m.GRPCAddr)
			}
		}
		return addrs
	}

	ae := replication.NewAntiEntropy(n, peerAddrs(), causal, time.Duration(cfg.SyncMs)*time.Millisecond)

	// ── 9. gRPC server ────────────────────────────────────────────────────────
	srv := grpc.NewServer()
	kvpb.RegisterKVServiceServer(srv, api.NewHandler(coord, n, seeds))
	fwdpb.RegisterForwardServiceServer(srv, api.NewForwardHandler(coord))
	repb.RegisterReplicationServiceServer(srv, replication.NewHandler(n, r))
	reflection.Register(srv)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", cfg.GRPCPort))
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	log.Printf("[%s] gRPC listening on :%s", cfg.ReplicaID, cfg.GRPCPort)

	// ── 10. Anti-entropy loop ─────────────────────────────────────────────────
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go ae.Run(ctx)

	// ── 11. Serve (blocking) ──────────────────────────────────────────────────
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
