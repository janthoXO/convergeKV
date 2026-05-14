// Package main is the ConvergeKV server entry point.
// It wires together gossip-based cluster membership, a fixed slot map,
// a coordinator for request routing, and the anti-entropy replication loop.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
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
	"github.com/janthoXO/convergeKV/internal/partition"
	"github.com/janthoXO/convergeKV/internal/replication"
	"github.com/janthoXO/convergeKV/internal/storage"
)

// config holds all server configuration parsed from environment variables.
type config struct {
	ReplicaID    string `env:"REPLICA_ID,required"`
	GRPCPort     int `env:"GRPC_PORT"     envDefault:"50051"`
	GossipPort   int    `env:"GOSSIP_PORT"   envDefault:"7946"`
	GossipBind   string `env:"GOSSIP_BIND"   envDefault:"0.0.0.0"`
	Seeds        string `env:"SEEDS"`        // comma-separated gossip host:port; empty for single-node
	InitialNodes string `env:"INITIAL_NODES"` // comma-separated replica IDs for first-boot slot map
	DataDir      string `env:"DATA_DIR"      envDefault:"/data"`
	RF           int    `env:"RF"            envDefault:"3"`
	SyncMs       int    `env:"SYNC_MS"       envDefault:"2000"`
}

const slotMapFile = "slotmap.json"

func main() {
	// Load .env if present; silently ignored when the file doesn't exist.
	if err := godotenv.Load(); err != nil && !os.IsNotExist(err) {
		log.Printf("[config] .env not loaded: %v", err)
	}

	var cfg config
	if err := cenv.Parse(&cfg); err != nil {
		log.Fatalf("config: %v", err)
	}
	log.Printf("[config] replica=%s grpc=%d gossip=%d seeds=%q dataDir=%s rf=%d syncMs=%d",
		cfg.ReplicaID, cfg.GRPCPort, cfg.GossipPort, cfg.Seeds, cfg.DataDir, cfg.RF, cfg.SyncMs)

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

	// ── 3. Slot map — load persisted or generate initial ───────────────────────
	smPath := filepath.Join(cfg.DataDir, slotMapFile)
	initialSM := loadOrCreateSlotMap(smPath, cfg.InitialNodes, cfg.RF)
	n.UpdateSlotMap(initialSM)
	log.Printf("[slotmap] version=%d loaded", initialSM.Version)

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

	// ── 5. Gossip with SlotMap push/pull ──────────────────────────────────────
	onSlotMapChange := func(sm partition.SlotMap) {
		n.UpdateSlotMap(sm)
		if err := saveSlotMap(smPath, sm); err != nil {
			log.Printf("[slotmap] persist error: %v", err)
		}
		log.Printf("[slotmap] updated to version %d", sm.Version)
	}

	g, err := gossip.Start(gossip.Config{
		BindAddr:        cfg.GossipBind,
		BindPort:        cfg.GossipPort,
		LocalMeta:       gossip.NodeMeta{ReplicaID: cfg.ReplicaID, GRPCPort: cfg.GRPCPort},
		Seeds:           seeds,
		InitialSlotMap:  initialSM,
		OnSlotMapChange: onSlotMapChange,
	})
	if err != nil {
		log.Fatalf("start gossip: %v", err)
	}
	defer g.Leave(3 * time.Second)

	// ── 6. Slot map accessor and address resolver for coordinator/AE ──────────
	getSlotMap := func() partition.SlotMap {
		return g.CurrentSlotMap()
	}

	// resolveAddr maps a replicaID to its gRPC address via the live gossip view.
	resolveAddr := func(replicaID string) (string, bool) {
		for _, m := range g.Members() {
			if m.ReplicaID == replicaID {
				return m.GRPCAddr, true
			}
		}
		return "", false
	}

	// ── 7. Forwarder and Coordinator ──────────────────────────────────────────
	fwd := coordinator.NewForwarder()
	defer fwd.Close()

	coord := coordinator.New(n, getSlotMap, resolveAddr, fwd)

	// ── 8. Causal context + anti-entropy ──────────────────────────────────────
	causal := replication.NewCausalContext()
	ae := replication.NewAntiEntropy(n, g, getSlotMap, causal, time.Duration(cfg.SyncMs)*time.Millisecond)

	// ── 9. gRPC server ───────────────────────────────────────────────────────
	srv := grpc.NewServer()
	kvpb.RegisterKVServiceServer(srv, api.NewHandler(coord, n, seeds))
	fwdpb.RegisterForwardServiceServer(srv, api.NewForwardHandler(coord))
	repb.RegisterReplicationServiceServer(srv, replication.NewHandler(n, getSlotMap))
	reflection.Register(srv)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.GRPCPort))
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	log.Printf("[%s] gRPC listening on :%d", cfg.ReplicaID, cfg.GRPCPort)

	// ── 10. Anti-entropy loop ──────────────────────────────────────────────────
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go ae.Run(ctx)

	// ── 11. Serve (blocking) ──────────────────────────────────────────────────
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

// loadOrCreateSlotMap loads the slot map from disk, or generates an initial one.
func loadOrCreateSlotMap(path, initialNodes string, rf int) partition.SlotMap {
	// Try to load persisted map.
	if b, err := os.ReadFile(path); err == nil {
		sm, err := partition.Decode(b)
		if err == nil {
			return sm
		}
		log.Printf("[slotmap] ignoring corrupt persisted file: %v", err)
	}

	// Generate initial assignment from INITIAL_NODES env var.
	var nodeIDs []string
	for _, id := range strings.Split(initialNodes, ",") {
		id = strings.TrimSpace(id)
		if id != "" {
			nodeIDs = append(nodeIDs, id)
		}
	}
	if len(nodeIDs) == 0 {
		log.Printf("[slotmap] INITIAL_NODES not set — single-node mode")
		return partition.SlotMap{Version: 0} // empty map; accepts all writes
	}

	sm := partition.InitialAssignment(nodeIDs, rf)
	if err := saveSlotMap(path, sm); err != nil {
		log.Printf("[slotmap] initial persist failed: %v", err)
	}
	log.Printf("[slotmap] generated initial assignment: %d nodes rf=%d", len(nodeIDs), rf)
	return sm
}

// saveSlotMap persists the slot map to disk as JSON.
func saveSlotMap(path string, sm partition.SlotMap) error {
	b, err := json.Marshal(sm)
	if err != nil {
		return fmt.Errorf("marshal slot map: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}
