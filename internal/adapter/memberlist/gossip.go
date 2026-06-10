// Package memberlist provides a gossip-based cluster membership adapter built
// on HashiCorp memberlist. It satisfies ports.MemberView and publishes changes
// via a context-managed subscription (Gossip.Subscribe) so consumers never
// leak goroutines on shutdown.
package memberlist

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"slices"
	"strconv"
	"sync"
	"time"

	ml "github.com/hashicorp/memberlist"
	"github.com/janthoXO/convergeKV/internal/core/ports"
	"github.com/janthoXO/convergeKV/internal/util/eventbus"
)

// Gossip wraps a memberlist instance and exposes a stable membership view.
// All exported methods are safe for concurrent use.
type Gossip struct {
	mu        sync.RWMutex
	list      *ml.Memberlist
	members   map[string]ports.MemberInfo
	localMeta NodeMeta

	// changeTopic fans membership snapshots to all context-bound subscribers.
	changeTopic eventbus.Topic[[]ports.MemberInfo]

	// changeCh is written by the eventDelegate (inside memberlist's lock) and
	// drained by the changeWorker goroutine (outside memberlist's lock) to
	// avoid calling list.Members() while memberlist holds its own internal lock.
	changeCh chan struct{}
	stopCh   chan struct{}
}

// Config holds the parameters for starting a Gossip instance.
type Config struct {
	BindAddr  string
	BindPort  int
	LocalMeta NodeMeta
	Seeds     []string
}

// Start creates and starts a memberlist node, then joins via seeds. If seeds
// are unreachable it retries with exponential backoff until ctx is cancelled,
// in which case it tears down the partially-started memberlist and returns
// ctx.Err().
func Start(ctx context.Context, cfg Config) (*Gossip, error) {
	g := &Gossip{
		members:   make(map[string]ports.MemberInfo),
		localMeta: cfg.LocalMeta,
		changeCh:  make(chan struct{}, 16),
		stopCh:    make(chan struct{}),
	}

	mlCfg := ml.DefaultLANConfig()
	mlCfg.BindAddr = cfg.BindAddr
	mlCfg.BindPort = cfg.BindPort
	mlCfg.Name = cfg.LocalMeta.ReplicaID
	mlCfg.Delegate = &gossipDelegate{gossip: g, meta: EncodeMeta(cfg.LocalMeta)}
	mlCfg.Events = &eventDelegate{gossip: g}
	mlCfg.Logger = nil

	list, err := ml.Create(mlCfg)
	if err != nil {
		return nil, fmt.Errorf("memberlist: create: %w", err)
	}
	g.mu.Lock()
	g.list = list
	g.members[cfg.LocalMeta.ReplicaID] = ports.MemberInfo{
		ReplicaID:  cfg.LocalMeta.ReplicaID,
		GossipAddr: net.JoinHostPort(cfg.BindAddr, strconv.Itoa(cfg.BindPort)),
		GRPCAddr:   net.JoinHostPort(cfg.BindAddr, strconv.Itoa(cfg.LocalMeta.GRPCPort)),
	}
	g.mu.Unlock()

	go g.changeWorker()

	if len(cfg.Seeds) > 0 {
		if err := g.joinWithRetry(ctx, cfg.Seeds); err != nil {
			close(g.stopCh)
			_ = g.list.Shutdown()
			return nil, err
		}
	}

	return g, nil
}

// Members returns a snapshot of the current membership including the local node.
// Satisfies ports.MemberView.
func (g *Gossip) Members() []ports.MemberInfo {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return slices.Collect(maps.Values(g.members))
}

// Subscribe returns a channel that receives a full membership snapshot on
// every change. The channel is closed when ctx is cancelled.
func (g *Gossip) Subscribe(ctx context.Context) <-chan []ports.MemberInfo {
	return g.changeTopic.Subscribe(ctx)
}

// Leave gracefully announces departure and shuts down memberlist.
func (g *Gossip) Leave(timeout time.Duration) error {
	close(g.stopCh)
	if err := g.list.Leave(timeout); err != nil {
		return err
	}
	return g.list.Shutdown()
}

func (g *Gossip) joinWithRetry(ctx context.Context, seeds []string) error {
	backoff := 500 * time.Millisecond
	for {
		if _, err := g.list.Join(seeds); err == nil {
			slog.Info("joined cluster", "seeds", seeds)
			return nil
		} else {
			slog.Warn("join failed, retrying", "err", err, "backoff", backoff)
		}

		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func (g *Gossip) changeWorker() {
	for {
		select {
		case <-g.stopCh:
			return
		case <-g.changeCh:
			g.rebuildMembers()
		}
	}
}

func (g *Gossip) rebuildMembers() {
	g.mu.RLock()
	list := g.list
	g.mu.RUnlock()

	nodes := list.Members()
	newMembers := make(map[string]ports.MemberInfo, len(nodes))
	for _, n := range nodes {
		meta, err := DecodeMeta(n.Meta)
		if err != nil || meta.ReplicaID == "" {
			continue
		}
		if meta.ReplicaID != g.localMeta.ReplicaID && meta.ConfigFingerprint != g.localMeta.ConfigFingerprint {
			slog.Warn("config fingerprint mismatch — NUM_PARTITIONS/RF/IBLT_CELLS must be identical across the cluster",
				"peer", meta.ReplicaID, "peerFingerprint", meta.ConfigFingerprint, "localFingerprint", g.localMeta.ConfigFingerprint)
		}
		ipCopy := make(net.IP, len(n.Addr))
		copy(ipCopy, n.Addr)
		newMembers[meta.ReplicaID] = ports.MemberInfo{
			ReplicaID:  meta.ReplicaID,
			GossipAddr: net.JoinHostPort(ipCopy.String(), strconv.Itoa(int(n.Port))),
			GRPCAddr:   net.JoinHostPort(ipCopy.String(), strconv.Itoa(meta.GRPCPort)),
		}
	}

	snapshot := slices.Collect(maps.Values(newMembers))

	g.mu.Lock()
	g.members = newMembers
	g.mu.Unlock()

	g.changeTopic.Publish(snapshot)
}

func (g *Gossip) signal() {
	select {
	case g.changeCh <- struct{}{}:
	default:
	}
}

// ── memberlist.Delegate ───────────────────────────────────────────────────────

type gossipDelegate struct {
	gossip *Gossip
	meta   []byte
}

func (d *gossipDelegate) NodeMeta(_ int) []byte             { return d.meta }
func (d *gossipDelegate) NotifyMsg([]byte)                  {}
func (d *gossipDelegate) GetBroadcasts(_, _ int) [][]byte   { return nil }
func (d *gossipDelegate) LocalState(_ bool) []byte          { return nil }
func (d *gossipDelegate) MergeRemoteState(_ []byte, _ bool) {}

// ── memberlist.EventDelegate ──────────────────────────────────────────────────

type eventDelegate struct{ gossip *Gossip }

func (e *eventDelegate) NotifyJoin(_ *ml.Node)   { e.gossip.signal() }
func (e *eventDelegate) NotifyLeave(_ *ml.Node)  { e.gossip.signal() }
func (e *eventDelegate) NotifyUpdate(_ *ml.Node) { e.gossip.signal() }
