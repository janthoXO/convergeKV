// Package node assembles one convergeKV node: storage, identity, clock,
// gossip membership, placement watching, coordinator, replication fan-out,
// and the two gRPC servers. cmd/kvnode and the test harness both build on it.
package node

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"

	"github.com/janthoXO/convergeKV/internal/antientropy"
	"github.com/janthoXO/convergeKV/internal/api"
	"github.com/janthoXO/convergeKV/internal/cluster"
	"github.com/janthoXO/convergeKV/internal/config"
	"github.com/janthoXO/convergeKV/internal/coordinator"
	"github.com/janthoXO/convergeKV/internal/crdt"
	"github.com/janthoXO/convergeKV/internal/gc"
	"github.com/janthoXO/convergeKV/internal/hlc"
	"github.com/janthoXO/convergeKV/internal/identity"
	"github.com/janthoXO/convergeKV/internal/metrics"
	"github.com/janthoXO/convergeKV/internal/placement"
	"github.com/janthoXO/convergeKV/internal/replication"
	"github.com/janthoXO/convergeKV/internal/storage"
	"github.com/janthoXO/convergeKV/internal/transfer"
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
	AE    *antientropy.Engine

	cluster    *cluster.Cluster
	view       atomic.Pointer[placement.View]
	pool       *api.Pool
	fanout     *replication.Fanout
	transfer   *transfer.Manager
	partitions uint16
	// owned is the persisted ownership bitmap: partitions this node serves
	// plus partitions lost less than one watch tick ago (the flap window).
	// A bit clears only when the partition's data is dropped, so a set bit
	// always means "the local data is current enough for AE to close the
	// gap" — the no-transfer-storm shortcut on restart within grace.
	owned      []byte
	lostSince  map[uint16]time.Time // partitions lost but not yet dropped
	ctx        context.Context
	wg         sync.WaitGroup
	clientSrv  *grpc.Server
	nodeSrv    *grpc.Server
	clientAddr string
	nodeAddr   string
	adminAddr  string
	stopped    chan struct{}
	cancel     context.CancelFunc
}

// Start brings a node fully up: listeners first (to learn the real ports),
// then storage and identity, then gossip, then serving. Every acquired
// resource lands on a cleanup stack that runs (in reverse) on any later
// failure and is disarmed on success.
func Start(cfg config.Config, log *slog.Logger) (*Node, error) {
	if log == nil {
		log = slog.Default()
	}

	var cleanups []func()
	started := false
	defer func() {
		if started {
			return
		}
		for i := len(cleanups) - 1; i >= 0; i-- {
			cleanups[i]()
		}
	}()

	clientLn, err := net.Listen("tcp", cfg.ClientAddr)
	if err != nil {
		return nil, fmt.Errorf("node: client listener: %w", err)
	}
	cleanups = append(cleanups, func() { _ = clientLn.Close() })
	nodeLn, err := net.Listen("tcp", cfg.NodeAddr)
	if err != nil {
		return nil, fmt.Errorf("node: node listener: %w", err)
	}
	cleanups = append(cleanups, func() { _ = nodeLn.Close() })

	id, err := identity.LoadOrCreate(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	log = log.With("node_id", id.String()[:8])

	store, err := storage.Open(cfg.DataDir + "/db")
	if err != nil {
		return nil, err
	}
	cleanups = append(cleanups, func() { _ = store.Close() })
	if err := store.BindNodeID(id); err != nil {
		return nil, err
	}
	if err := store.BindPartitionCount(cfg.Partitions); err != nil {
		return nil, err
	}

	// Tombstone-grace rule: a node that was gone longer than the crash
	// grace period may hold state that predates cluster-wide GC; serving it
	// would resurrect deleted documents. And the cluster may have RETIRED
	// this actor from causal contexts — if it minted new dots, peers could
	// never compact them (the retired prefix is gone). So: wipe the data
	// AND re-enter as a brand-new actor.
	if lastAlive, err := store.LastAlive(); err != nil {
		return nil, err
	} else if !lastAlive.IsZero() && time.Since(lastAlive) > cfg.CrashGracePeriod {
		log.Warn("liveness lease expired; wiping data and rotating identity",
			"last_alive", lastAlive, "grace", cfg.CrashGracePeriod, "old_id", id.String()[:8])
		if err := store.WipeData(); err != nil {
			return nil, err
		}
		if id, err = identity.Rotate(cfg.DataDir); err != nil {
			return nil, err
		}
		if err := store.RebindNodeID(id); err != nil {
			return nil, err
		}
		log = log.With("node_id", id.String()[:8])
	}
	if err := store.PersistLastAlive(time.Now()); err != nil {
		return nil, err
	}

	// HLC restart rule: max(checkpoint, wall) + safety bump.
	clock := hlc.New()
	if checkpoint, err := store.HLC(); err != nil {
		return nil, err
	} else if checkpoint > 0 {
		clock.SetAtLeast(checkpoint + hlcRestartBump)
	}

	cl, err := cluster.Join(cluster.Config{
		NodeID:          id,
		Partitions:      cfg.Partitions,
		RPCAddr:         advertised(nodeLn.Addr().String(), cfg.NodeAddr),
		BindAddr:        cfg.GossipAddr,
		Advertise:       cfg.AdvertiseAddr,
		Seeds:           cfg.Seeds,
		DeadGracePeriod: cfg.CrashGracePeriod,
		Memberlist:      cfg.MemberlistConfig,
		Logger:          log,
	})
	if err != nil {
		return nil, err
	}
	cleanups = append(cleanups, func() { _ = cl.Shutdown() })

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
	n.fanout = replication.NewFanout(replication.Config{
		MaxAge: cfg.ReplicationMaxAge,
		Logger: log,
	}, n.pool.SendDelta)
	n.Coord = coordinator.New(id, cfg.Partitions, store, clock,
		n.View, n.pool, n.fanout, log)

	ctx, cancel := context.WithCancel(context.Background())
	n.cancel = cancel
	n.ctx = ctx
	cleanups = append(cleanups, func() { cancel(); n.wg.Wait() })
	n.partitions = cfg.Partitions
	n.lostSince = make(map[uint16]time.Time)
	prevOwned, err := store.Owned()
	if err != nil {
		return nil, err
	}
	n.owned = make([]byte, (int(cfg.Partitions)+7)/8)
	copy(n.owned, prevOwned)
	n.AE = antientropy.New(id, cfg.Partitions, store, n.Coord, n.View, n.pool,
		cfg.AntiEntropyInterval, log)
	n.AE.SetGC(gc.New(store, n.Coord, n.knownActors, log))
	n.transfer = transfer.New(id, n.Coord, n.View, n.pool, cl, log)

	n.recomputeView(cfg.Partitions)
	n.bg(func() { n.watch(ctx, cfg.Partitions) })
	// The heartbeat must beat the lease: several ticks per grace period, or
	// an ordinary restart would look lease-expired and wipe.
	n.bg(func() { n.checkpointHLC(ctx, min(time.Second, cfg.CrashGracePeriod/4)) })
	n.bg(func() { n.AE.Run(ctx) })

	if cfg.AdminAddr != "" {
		n.adminAddr, err = metrics.Serve(ctx, cfg.AdminAddr, metrics.Sources{
			Fanout:   n.fanout,
			AE:       n.AE,
			Transfer: n.transfer,
			Cluster:  cl,
		}, log)
		if err != nil {
			return nil, fmt.Errorf("node: admin endpoint: %w", err)
		}
	}

	n.clientSrv = grpc.NewServer()
	pb.RegisterKVServer(n.clientSrv, &api.KVServer{Coord: n.Coord})
	n.nodeSrv = grpc.NewServer()
	pb.RegisterNodeServer(n.nodeSrv, &api.NodeServer{Coord: n.Coord, Store: store, Partitions: cfg.Partitions})
	go func() { _ = n.clientSrv.Serve(clientLn) }()
	go func() { _ = n.nodeSrv.Serve(nodeLn) }()

	log.Info("node started",
		"client_addr", n.clientAddr, "node_addr", n.nodeAddr, "partitions", cfg.Partitions)
	started = true // disarm the cleanup stack; Stop owns shutdown now
	return n, nil
}

func (n *Node) ClientAddr() string { return n.clientAddr }
func (n *Node) NodeAddr() string   { return n.nodeAddr }
func (n *Node) AdminAddr() string  { return n.adminAddr }

// View returns the current placement view.
func (n *Node) View() *placement.View { return n.view.Load() }

// Cluster exposes the membership layer (harness and later milestones).
func (n *Node) Cluster() *cluster.Cluster { return n.cluster }

// Transfer exposes the bootstrap manager (test assertions).
func (n *Node) Transfer() *transfer.Manager { return n.transfer }

// Stop shuts the node down. Graceful: drain (mark partitions draining, wait
// for successors to go active), broadcast a leave, then exit. Otherwise the
// node drops off the network without a word (crash simulation in tests).
func (n *Node) Stop(graceful bool) {
	if graceful {
		owned := n.View().OwnedPartitions(n.ID)
		transfer.Drain(n.ID, n.View, n.cluster, owned, 15*time.Second, n.Log)
	}
	n.cancel()
	if graceful {
		_ = n.cluster.Leave(2 * time.Second)
	} else {
		_ = n.cluster.Shutdown()
	}
	// GracefulStop waits for in-flight handlers (which touch the store);
	// plain Stop would only cancel them and race the store close below.
	stopServer(n.clientSrv)
	stopServer(n.nodeSrv)
	n.fanout.Close()
	n.pool.Close()
	// Background goroutines (watch, HLC checkpoint, AE, bootstraps) touch
	// the store; they must be fully out before it closes.
	n.wg.Wait()
	n.transfer.Wait()
	_ = n.Store.PersistHLC(n.Clock.Last())
	_ = n.Store.Close()
	close(n.stopped)
}

func stopServer(s *grpc.Server) {
	done := make(chan struct{})
	go func() {
		s.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		s.Stop() // stragglers (e.g. stuck streams) get the hard stop
	}
}

func (n *Node) bg(f func()) {
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		f()
	}()
}

// watch recomputes placement on membership changes and keeps our gossiped
// partition status flags in line with ownership.
func (n *Node) watch(ctx context.Context, p uint16) {
	// The slow tick retries work that has no membership trigger of its own
	// (e.g. a failed bootstrap rolled back to StatusNone).
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-n.cluster.Changed():
			n.recomputeView(p)
		case <-tick.C:
			n.recomputeView(p)
		}
	}
}

// lossGrace is how long a partition must stay un-owned before its local
// data is dropped: one watch tick, so a membership flap that takes the
// partition away and immediately back does not cost a re-fetch.
const lossGrace = 2 * time.Second

func (n *Node) recomputeView(p uint16) {
	v := placement.Compute(p, n.cluster.Members(), n.cluster.DeadMembers())
	n.view.Store(v)

	self := n.cluster.Self()
	var toActivate, toClear, toBootstrap []uint16
	for pid := uint16(0); pid < p; pid++ {
		isOwner := v.IsOwner(pid, n.ID)
		st := self.Flags.Get(pid)
		switch {
		case isOwner && st == cluster.StatusNone:
			delete(n.lostSince, pid)
			_, hasSource := bootstrapSource(v, pid, n.ID)
			switch {
			case n.wasOwner(pid):
				// Restart/rejoin of a previous owner whose data was never
				// dropped: the data (if any) is here, AE closes any gap —
				// no transfer storm.
				toActivate = append(toActivate, pid)
				n.setOwned(pid, true)
			case hasSource:
				// Gained anew. Any local leftover predates an ownership
				// loss and may be staler than cluster-wide GC — it must
				// not merge into the bootstrap (resurrection). The owned
				// bit stays clear until the bootstrap flips us to active
				// (below): a crash mid-bootstrap then re-bootstraps rather
				// than activating incomplete data.
				if hasData, err := n.Store.HasPartitionData(pid); err != nil {
					n.Log.Warn("partition data probe failed", "partition", pid, "err", err)
					continue
				} else if hasData {
					if err := n.Store.DropPartition(pid); err != nil {
						n.Log.Warn("stale partition drop failed", "partition", pid, "err", err)
						continue
					}
				}
				toBootstrap = append(toBootstrap, pid)
			default:
				// Fresh partition with no serving owner anywhere.
				toActivate = append(toActivate, pid)
				n.setOwned(pid, true)
			}
		case isOwner:
			delete(n.lostSince, pid) // serving (or bootstrapping) as usual
			// Mark the data current only once we actually serve it; while
			// bootstrapping (StatusBootstrapping) the copy is incomplete.
			if st == cluster.StatusActive || st == cluster.StatusDraining {
				n.setOwned(pid, true)
			}
		default: // not an owner
			if st != cluster.StatusNone {
				toClear = append(toClear, pid)
			}
			if !n.wasOwner(pid) {
				break // nothing held, nothing to drop
			}
			since, marked := n.lostSince[pid]
			switch {
			case !marked:
				n.lostSince[pid] = time.Now()
			case time.Since(since) >= lossGrace:
				// The loss survived a full watch tick: drop the data. From
				// here on a regain must bootstrap from a serving owner.
				if err := n.Store.DropPartition(pid); err != nil {
					n.Log.Warn("partition drop failed", "partition", pid, "err", err)
					break
				}
				n.Log.Info("partition ownership lost, data dropped", "partition", pid)
				n.setOwned(pid, false)
				delete(n.lostSince, pid)
			}
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
		n.view.Store(placement.Compute(p, n.cluster.Members(), n.cluster.DeadMembers()))
	}
	for _, pid := range toBootstrap {
		n.transfer.Bootstrap(n.ctx, pid)
	}
	n.persistOwnership()
	n.evictDepartedPeers()
}

// evictDepartedPeers drops per-peer state (fan-out queues + workers, pooled
// connections) of nodes no longer in the membership view, so a long-lived
// node in a churning cluster does not grow without bound. Dead-within-grace
// members are kept: they may come straight back.
func (n *Node) evictDepartedPeers() {
	keep := make(map[string]struct{})
	for _, m := range n.cluster.Members() {
		keep[m.Meta.RPCAddr] = struct{}{}
	}
	for _, m := range n.cluster.DeadMembers() {
		keep[m.Meta.RPCAddr] = struct{}{}
	}
	n.fanout.Retain(keep)
	n.pool.Retain(keep)
}

// wasOwner reports whether the ownership bitmap covers the partition (i.e.
// this node holds data for it that was never dropped).
func (n *Node) wasOwner(pid uint16) bool {
	return n.owned[pid/8]&(1<<(pid%8)) != 0
}

func (n *Node) setOwned(pid uint16, v bool) {
	if v {
		n.owned[pid/8] |= 1 << (pid % 8)
	} else {
		n.owned[pid/8] &^= 1 << (pid % 8)
	}
}

// persistOwnership checkpoints the ownership bitmap for restart-within-grace.
func (n *Node) persistOwnership() {
	if err := n.Store.PersistOwned(n.owned); err != nil {
		n.Log.Warn("ownership checkpoint failed", "err", err)
	}
}

// bootstrapSource reports whether some other owner could serve a snapshot.
func bootstrapSource(v *placement.View, pid uint16, self [16]byte) (placement.Owner, bool) {
	for _, o := range v.Owners(pid) {
		if o.ID == self || o.Dead {
			continue
		}
		if o.Status == cluster.StatusActive || o.Status == cluster.StatusDraining {
			return o, true
		}
	}
	return placement.Owner{}, false
}

// checkpointHLC persists the clock and the liveness lease periodically; the
// restart bump covers the uncheckpointed window.
func (n *Node) checkpointHLC(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := n.Store.PersistHLC(n.Clock.Last()); err != nil {
				n.Log.Warn("hlc checkpoint failed", "err", err)
			}
			if err := n.Store.PersistLastAlive(time.Now()); err != nil {
				n.Log.Warn("liveness lease heartbeat failed", "err", err)
			}
		}
	}
}

// knownActors returns every actor that must not be retired: alive members
// and members dead within the grace period.
func (n *Node) knownActors() map[crdt.ActorID]bool {
	out := make(map[crdt.ActorID]bool)
	for _, m := range n.cluster.Members() {
		out[m.Meta.ID] = true
	}
	for _, m := range n.cluster.DeadMembers() {
		out[m.Meta.ID] = true
	}
	return out
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
