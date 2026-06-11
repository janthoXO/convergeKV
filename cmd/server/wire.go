package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	debugpb "github.com/janthoXO/convergeKV/gen/debug"
	fwdpb "github.com/janthoXO/convergeKV/gen/forward"
	kvpb "github.com/janthoXO/convergeKV/gen/kv"
	repb "github.com/janthoXO/convergeKV/gen/replication"
	"github.com/janthoXO/convergeKV/internal/adapter/badger"
	"github.com/janthoXO/convergeKV/internal/adapter/connpool"
	"github.com/janthoXO/convergeKV/internal/adapter/forward"
	"github.com/janthoXO/convergeKV/internal/adapter/memberlist"
	"github.com/janthoXO/convergeKV/internal/adapter/replication/antientropy"
	"github.com/janthoXO/convergeKV/internal/adapter/replication/grpcsrv"
	"github.com/janthoXO/convergeKV/internal/adapter/replication/streampush"
	"github.com/janthoXO/convergeKV/internal/cluster/ownership"
	"github.com/janthoXO/convergeKV/internal/cluster/partitions"
	"github.com/janthoXO/convergeKV/internal/core/coordinator"
	"github.com/janthoXO/convergeKV/internal/core/replica"
	"github.com/janthoXO/convergeKV/internal/domain/hlc"
	"github.com/janthoXO/convergeKV/internal/lifecycle"
	tgrpc "github.com/janthoXO/convergeKV/internal/transport/grpc"
)

// buildApp constructs every component and returns a Supervisor whose Run method
// starts and stops them in dependency order. seeds is the parsed SEEDS list.
//
// Services start sequentially; each must call ready() before the next begins.
// On SIGTERM/SIGINT the supervisor stops them in reverse (see main.go, which
// passes the signal context to Supervisor.Run).
func buildApp(cfg config) (*lifecycle.Supervisor, error) {
	// Bind the listener before starting any service so we fail-fast on port conflicts.
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.GRPCPort))
	if err != nil {
		return nil, fmt.Errorf("listen: %w", err)
	}

	// Captured vars: each service's Start writes here; later services read here.
	var store *badger.Store
	var g *memberlist.Gossip
	pool := connpool.New()
	var node *replica.Replica
	var own *ownership.Ownership
	var fanout *streampush.Fanout
	var ae *antientropy.Syncer
	var pl *partitions.Lifecycle

	healthSrv := health.NewServer()
	healthSrv.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)

	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(tgrpc.NullByteInterceptor),
	)

	return lifecycle.New(
		// ── 1. Storage ────────────────────────────────────────────────────────────
		lifecycle.Service{
			Name: "storage",
			Start: func(ctx context.Context, ready func()) error {
				s, err := badger.Open(cfg.DataDir)
				if err != nil {
					return fmt.Errorf("open storage: %w", err)
				}
				store = s
				ready()
				<-ctx.Done()
				return nil
			},
			Close: func() error { return store.Close() },
		},

		// ── 2. Gossip ─────────────────────────────────────────────────────────────
		lifecycle.Service{
			Name: "gossip",
			Start: func(ctx context.Context, ready func()) error {
				gossip, err := memberlist.Start(ctx, memberlist.Config{
					BindAddr: cfg.GossipBind,
					BindPort: cfg.GossipPort,
					LocalMeta: memberlist.NodeMeta{
						ReplicaID:         cfg.ReplicaID,
						GRPCPort:          cfg.GRPCPort,
						ConfigFingerprint: memberlist.ConfigFingerprint(cfg.NumPartitions, cfg.RF, cfg.IBLTCells),
					},
					Seeds: cfg.Seeds,
				})
				if err != nil {
					return fmt.Errorf("start gossip: %w", err)
				}
				g = gossip
				ready()
				<-ctx.Done()
				return nil
			},
			Close: func() error { return g.Leave(3 * time.Second) },
		},

		// ── 3. Connection pool ────────────────────────────────────────────────────
		lifecycle.Service{
			Name: "pool",
			Start: func(ctx context.Context, ready func()) error {
				ready() // pool is already constructed above
				<-ctx.Done()
				return nil
			},
			Close: func() error { pool.Close(); return nil },
		},

		// ── 4. HLC + Replica ──────────────────────────────────────────────────────
		lifecycle.Service{
			Name: "replica",
			Start: func(ctx context.Context, ready func()) error {
				hlcFloor, err := replica.RecoverHLCFloor(ctx, store)
				if err != nil {
					return fmt.Errorf("scan HLC floor: %w", err)
				}
				slog.Info("hlc floor seeded", "physicalMs", hlcFloor.PhysicalMs, "logical", hlcFloor.Logical)
				node = replica.New(cfg.ReplicaID, hlc.New(), store, cfg.IBLTCells,
					replica.WithHLCFloor(hlcFloor),
					replica.WithTombstoneGrace(time.Duration(cfg.TombstoneGraceMs)*time.Millisecond))
				ready()
				<-ctx.Done()
				return nil
			},
			Close: func() error { node.Close(); return nil },
		},

		// ── 5. HLC checkpointer ───────────────────────────────────────────────────
		// Periodically persists the current HLC so RecoverHLCFloor can seed in
		// O(1) on restart instead of scanning every entry.
		lifecycle.Service{
			Name: "checkpoint",
			Start: func(ctx context.Context, ready func()) error {
				ready()
				ticker := time.NewTicker(replica.CheckpointInterval)
				defer ticker.Stop()
				for {
					select {
					case <-ctx.Done():
						return nil
					case <-ticker.C:
						if err := store.SaveCheckpoint(ctx, node.HLCNow()); err != nil {
							slog.Warn("save HLC checkpoint failed", "err", err)
						}
					}
				}
			},
			Close: func() error { return nil },
		},

		// ── 6. Ownership ──────────────────────────────────────────────────────────
		// Computes initial partition ownership synchronously (before ready);
		// the actual IBLT seeding happens in the "partitions" service below.
		lifecycle.Service{
			Name: "ownership",
			Start: func(ctx context.Context, ready func()) error {
				own = ownership.New(cfg.ReplicaID, cfg.RF, cfg.NumPartitions)
				own.Update(g.Members())
				ready()
				<-ctx.Done()
				return nil
			},
			Close: func() error { return nil },
		},

		// ── 7. Write-path fanout ──────────────────────────────────────────────────
		// Coordinator calls fanout.Enqueue directly after a successful local
		// write (see step 9 below). No eventbus indirection — writes cannot be
		// silently coalesced/dropped under back-pressure.
		lifecycle.Service{
			Name: "fanout",
			Start: func(ctx context.Context, ready func()) error {
				fanout = streampush.New(ctx, cfg.ReplicaID, pool)
				ready()
				<-ctx.Done()
				return nil
			},
			Close: func() error { fanout.Close(); return nil },
		},

		// ── 8. Anti-entropy syncer ────────────────────────────────────────────────
		lifecycle.Service{
			Name: "antientropy",
			Start: func(ctx context.Context, ready func()) error {
				ae = antientropy.New(ctx, node, pool, own,
					antientropy.WithSyncInterval(time.Duration(cfg.SyncMs)*time.Millisecond),
					antientropy.WithTombstoneGrace(time.Duration(cfg.TombstoneGraceMs)*time.Millisecond))
				ready()
				ae.Run()
				<-ctx.Done()
				return nil
			},
			Close: func() error { ae.Close(); return nil },
		},

		// ── 9. Partition lifecycle ────────────────────────────────────────────────
		// Seeds the initial owned set synchronously (before ready) so IBLTs are
		// built before the replica serves any requests, then reacts to further
		// ownership changes published by Ownership.Update — off the
		// membership-watcher loop, so a slow Badger scan never blocks pool
		// eviction for subsequent membership changes.
		lifecycle.Service{
			Name: "partitions",
			Start: func(ctx context.Context, ready func()) error {
				pl = partitions.New(node, ae)
				pl.Sync(ctx, own.Owned())
				slog.Info("initial ownership", "ownedPartitions", len(own.Owned()), "ibltCells", cfg.IBLTCells)
				sub := own.Subscribe(ctx)
				ready()
				pl.Run(ctx, sub)
				return nil
			},
			Close: func() error { return nil }, // ctx cancellation closes sub
		},

		// ── 10. Tombstone GC ──────────────────────────────────────────────────────
		// Periodically purges tombstones older than TOMBSTONE_GRACE_MS from each
		// owned partition, keeping the store and per-partition IBLT in lockstep
		// (see Replica.GCTombstones). See CLAUDE.md for the rejoin-fresh
		// operational contract this depends on.
		lifecycle.Service{
			Name: "tombstonegc",
			Start: func(ctx context.Context, ready func()) error {
				ready()
				ticker := time.NewTicker(time.Duration(cfg.GCIntervalMs) * time.Millisecond)
				defer ticker.Stop()
				grace := uint64(cfg.TombstoneGraceMs)
				for {
					select {
					case <-ctx.Done():
						return nil
					case <-ticker.C:
						now := uint64(time.Now().UnixMilli())
						if now <= grace {
							continue
						}
						cutoff := now - grace
						for _, pid := range own.Owned() {
							n, err := node.GCTombstones(ctx, pid, cutoff)
							if err != nil {
								slog.Warn("tombstone gc failed", "partition", pid, "err", err)
								continue
							}
							if n > 0 {
								slog.Info("tombstone gc", "partition", pid, "purged", n)
							}
						}
					}
				}
			},
			Close: func() error { return nil },
		},

		// ── 11. Membership watcher ────────────────────────────────────────────────
		// Evicts absent peers from the pool and fanout, and updates ownership on
		// every gossip membership change.
		lifecycle.Service{
			Name: "memberwatch",
			Start: func(ctx context.Context, ready func()) error {
				sub := g.Subscribe(ctx)
				ready()
				for members := range sub {
					slog.Info("membership changed", "members", len(members))
					keep := make(map[string]struct{}, len(members))
					for _, m := range members {
						keep[m.GRPCAddr] = struct{}{}
					}
					pool.EvictAbsent(keep)
					fanout.EvictAbsent(keep)
					own.Update(members)
				}
				return nil
			},
			Close: func() error { return nil }, // ctx cancellation closes sub
		},

		// ── 12. gRPC server ───────────────────────────────────────────────────────
		// Registers all handlers, flips health to SERVING, then calls srv.Serve.
		// Close sets NOT_SERVING first so load balancers drain before GracefulStop.
		lifecycle.Service{
			Name: "grpc",
			Start: func(_ context.Context, ready func()) error {
				fwd := forward.New(pool)
				coord := coordinator.New(node, g, own, fwd, fanout, cfg.NumPartitions)

				kvpb.RegisterKVServiceServer(srv, tgrpc.NewKVHandler(coord))
				fwdpb.RegisterForwardServiceServer(srv, tgrpc.NewForwardHandler(coord))
				repb.RegisterSyncServiceServer(srv, grpcsrv.New(node))
				healthpb.RegisterHealthServer(srv, healthSrv)

				if cfg.Debug {
					debugpb.RegisterDebugServiceServer(srv, tgrpc.NewDebugHandler(store, cfg.DebugToken))
					slog.Info("DebugService registered")
				}
				reflection.Register(srv)

				slog.Info("gRPC listening", "replica", cfg.ReplicaID, "port", cfg.GRPCPort)
				healthSrv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
				ready()
				return srv.Serve(lis)
			},
			Close: func() error {
				healthSrv.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
				srv.GracefulStop()
				return nil
			},
		},
	), nil
}
