// Package node assembles one convergeKV node: storage, identity, clock,
// gossip membership, placement watching, coordinator, replication fan-out,
// and the two gRPC servers. cmd/kvnode and the test harness both build on it.
package node

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"

	"github.com/janthoXO/convergeKV/internal/api"
	"github.com/janthoXO/convergeKV/internal/cluster"
	"github.com/janthoXO/convergeKV/internal/config"
	"github.com/janthoXO/convergeKV/internal/coordinator"
	"github.com/janthoXO/convergeKV/internal/crdt"
	"github.com/janthoXO/convergeKV/internal/hlc"
	"github.com/janthoXO/convergeKV/internal/identity"
	"github.com/janthoXO/convergeKV/internal/placement"
	"github.com/janthoXO/convergeKV/internal/replication"
	"github.com/janthoXO/convergeKV/internal/storage"
	pb "github.com/janthoXO/convergeKV/pkg/proto"
)

// hlcRestartBump is added to the persisted HLC checkpoint on restart to
// cover writes between the last checkpoint and the crash.
const hlcRestartBump = uint64(10_000) << 16 // 10s of physical time

type Node struct {
	ID    identity.NodeID
	Log   *slog.Logger
	Store *storage.Store
	Clock *hlc.Clock
	Coord *coordinator.Coordinator

	cluster    *cluster.Cluster
	view       atomic.Pointer[placement.View]
	pool       *api.Pool
	fanout     *replication.Fanout
	clientSrv  *grpc.Server
	nodeSrv    *grpc.Server
	clientAddr string
	nodeAddr   string
	stopped    chan struct{}
	cancel     context.CancelFunc
}

// Start brings a node fully up: listeners first (to learn the real ports),
// then storage and identity, then gossip, then serving.
func Start(cfg config.Config, log *slog.Logger) (*Node, error) {
	if log == nil {
		log = slog.Default()
	}

	clientLn, err := net.Listen("tcp", cfg.ClientAddr)
	if err != nil {
		return nil, fmt.Errorf("node: client listener: %w", err)
	}
	nodeLn, err := net.Listen("tcp", cfg.NodeAddr)
	if err != nil {
		_ = clientLn.Close()
		return nil, fmt.Errorf("node: node listener: %w", err)
	}

	id, err := identity.LoadOrCreate(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	log = log.With("node_id", id.String()[:8])

	store, err := storage.Open(cfg.DataDir + "/db")
	if err != nil {
		return nil, err
	}
	if err := store.BindNodeID(id); err != nil {
		return nil, err
	}
	if err := store.BindPartitionCount(cfg.Partitions); err != nil {
		return nil, err
	}

	// HLC restart rule: max(checkpoint, wall) + safety bump.
	clock := hlc.New()
	if checkpoint, err := store.HLC(); err != nil {
		return nil, err
	} else if checkpoint > 0 {
		clock.SetAtLeast(checkpoint + hlcRestartBump)
	}

	lastSeq, err := store.DotSeq()
	if err != nil {
		return nil, err
	}
	dots := coordinator.NewDotSource(crdt.ActorID(id), lastSeq, store.PersistDotSeq)

	cl, err := cluster.Join(cluster.Config{
		NodeID:     id,
		Partitions: cfg.Partitions,
		RPCAddr:    advertised(nodeLn.Addr().String(), cfg.NodeAddr),
		BindAddr:   cfg.GossipAddr,
		Advertise:  cfg.AdvertiseAddr,
		Seeds:      cfg.Seeds,
		Memberlist: cfg.MemberlistConfig,
		Logger:     log,
	})
	if err != nil {
		_ = store.Close()
		return nil, err
	}

	n := &Node{
		ID:         id,
		Log:        log,
		Store:      store,
		Clock:      clock,
		cluster:    cl,
		pool:       api.NewPool(),
		clientAddr: clientLn.Addr().String(),
		nodeAddr:   nodeLn.Addr().String(),
		stopped:    make(chan struct{}),
	}
	n.fanout = replication.NewFanout(replication.Config{Logger: log}, n.pool.SendDelta)
	n.Coord = coordinator.New(id, cfg.Partitions, store, clock, dots,
		n.View, n.pool, n.fanout, log)

	n.recomputeView(cfg.Partitions)

	ctx, cancel := context.WithCancel(context.Background())
	n.cancel = cancel
	go n.watch(ctx, cfg.Partitions)
	go n.checkpointHLC(ctx)

	n.clientSrv = grpc.NewServer()
	pb.RegisterKVServer(n.clientSrv, &api.KVServer{Coord: n.Coord})
	n.nodeSrv = grpc.NewServer()
	pb.RegisterNodeServer(n.nodeSrv, &api.NodeServer{Coord: n.Coord, Partitions: cfg.Partitions})
	go func() { _ = n.clientSrv.Serve(clientLn) }()
	go func() { _ = n.nodeSrv.Serve(nodeLn) }()

	log.Info("node started",
		"client_addr", n.clientAddr, "node_addr", n.nodeAddr, "partitions", cfg.Partitions)
	return n, nil
}

func (n *Node) ClientAddr() string { return n.clientAddr }
func (n *Node) NodeAddr() string   { return n.nodeAddr }

// View returns the current placement view.
func (n *Node) View() *placement.View { return n.view.Load() }

// Cluster exposes the membership layer (harness and later milestones).
func (n *Node) Cluster() *cluster.Cluster { return n.cluster }

// Stop gracefully shuts the node down. With graceful=false the node drops
// off the network without a leave broadcast (crash simulation in tests).
func (n *Node) Stop(graceful bool) {
	n.cancel()
	if graceful {
		_ = n.cluster.Leave(2 * time.Second)
	} else {
		_ = n.cluster.Shutdown()
	}
	n.clientSrv.Stop()
	n.nodeSrv.Stop()
	n.fanout.Close()
	n.pool.Close()
	_ = n.Store.PersistHLC(n.Clock.Last())
	_ = n.Store.Close()
	close(n.stopped)
}

// watch recomputes placement on membership changes and keeps our gossiped
// partition status flags in line with ownership. Until M7 adds bootstrap
// transfer, newly gained partitions are marked active immediately.
func (n *Node) watch(ctx context.Context, p uint16) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-n.cluster.Changed():
			n.recomputeView(p)
		}
	}
}

func (n *Node) recomputeView(p uint16) {
	v := placement.Compute(p, n.cluster.Members())
	n.view.Store(v)

	self := n.cluster.Self()
	var toActivate, toClear []uint16
	for pid := uint16(0); pid < p; pid++ {
		owned := v.IsOwner(pid, n.ID)
		switch st := self.Flags.Get(pid); {
		case owned && st == cluster.StatusNone:
			toActivate = append(toActivate, pid)
		case !owned && st != cluster.StatusNone:
			toClear = append(toClear, pid)
		}
	}
	if len(toActivate) > 0 || len(toClear) > 0 {
		err := n.cluster.UpdateFlags(func(f cluster.PartitionFlags) {
			for _, pid := range toActivate {
				f.Set(pid, cluster.StatusActive)
			}
			for _, pid := range toClear {
				f.Set(pid, cluster.StatusNone)
			}
		})
		if err != nil {
			n.Log.Warn("flag update failed", "err", err)
		}
		// Flags changed our own meta: recompute so the local view sees them.
		n.view.Store(placement.Compute(p, n.cluster.Members()))
	}
}

// checkpointHLC persists the clock periodically; the restart bump covers the
// uncheckpointed window.
func (n *Node) checkpointHLC(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := n.Store.PersistHLC(n.Clock.Last()); err != nil {
				n.Log.Warn("hlc checkpoint failed", "err", err)
			}
		}
	}
}

// advertised picks the address peers should dial: the listener's concrete
// address, unless it bound a wildcard host in which case the configured host
// is kept. Tests always bind 127.0.0.1 explicitly.
func advertised(listenerAddr, configured string) string {
	host, _, err := net.SplitHostPort(listenerAddr)
	if err != nil || host == "::" || host == "0.0.0.0" || host == "" {
		return configured
	}
	return listenerAddr
}
