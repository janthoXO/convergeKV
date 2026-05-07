package gossip

import (
	"fmt"
	"log"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
)

// MemberInfo holds the information about a discovered cluster member that
// the rest of the system cares about.
type MemberInfo struct {
	ReplicaID  string
	GossipAddr string // host:gossip_port
	GRPCAddr   string // host:grpc_port
}

// ChangeHandler is called whenever the membership view changes (join or leave).
// The argument is the current complete membership list, including the local node.
type ChangeHandler func(members []MemberInfo)

// Gossip wraps a memberlist instance and exposes a stable membership view.
type Gossip struct {
	mu        sync.RWMutex
	list      *memberlist.Memberlist
	members   map[string]MemberInfo // keyed by ReplicaID
	onChange  ChangeHandler
	localMeta NodeMeta

	// changeCh is written by the eventDelegate (inside memberlist's lock) and
	// read by the background worker goroutine (outside memberlist's lock).
	// This prevents calling list.Members() while memberlist holds its own lock.
	changeCh chan struct{}
	stopCh   chan struct{}
}

// Config holds the parameters for starting a Gossip instance.
type Config struct {
	BindAddr  string
	BindPort  int
	LocalMeta NodeMeta      // this node's metadata
	Seeds     []string      // host:port of seed nodes; may be empty
	OnChange  ChangeHandler // called on every membership change
}

// Start creates and starts a memberlist node, then joins via seeds.
// It returns once the node is participating in gossip (seeds may be empty).
func Start(cfg Config) (*Gossip, error) {
	g := &Gossip{
		members:   make(map[string]MemberInfo),
		onChange:  cfg.OnChange,
		localMeta: cfg.LocalMeta,
		changeCh:  make(chan struct{}, 16), // buffered; drops coalesced signals
		stopCh:    make(chan struct{}),
	}

	mlCfg := memberlist.DefaultLANConfig()
	mlCfg.BindAddr = cfg.BindAddr
	mlCfg.BindPort = cfg.BindPort
	mlCfg.Name = cfg.LocalMeta.ReplicaID
	mlCfg.Delegate = &delegate{meta: EncodeMeta(cfg.LocalMeta)}
	mlCfg.Events = &eventDelegate{gossip: g}
	mlCfg.Logger = nil // suppress memberlist's own logger in tests

	list, err := memberlist.Create(mlCfg)
	if err != nil {
		return nil, fmt.Errorf("gossip: create memberlist: %w", err)
	}
	g.mu.Lock()
	g.list = list
	g.mu.Unlock()

	// Seed ourselves into the members map immediately.
	g.mu.Lock()
	g.members[cfg.LocalMeta.ReplicaID] = MemberInfo{
		ReplicaID:  cfg.LocalMeta.ReplicaID,
		GossipAddr: net.JoinHostPort(cfg.BindAddr, strconv.Itoa(cfg.BindPort)),
		GRPCAddr:   net.JoinHostPort(cfg.BindAddr, strconv.Itoa(cfg.LocalMeta.GRPCPort)),
	}
	g.mu.Unlock()

	// Background goroutine: drains changeCh and rebuilds the membership view
	// outside memberlist's internal lock.
	go g.changeWorker()

	// Join seeds with retries.
	if len(cfg.Seeds) > 0 {
		go g.joinWithRetry(cfg.Seeds)
	}

	return g, nil
}

// Members returns a snapshot of the current membership, including the local node.
func (g *Gossip) Members() []MemberInfo {
	g.mu.RLock()
	defer g.mu.RUnlock()
	out := make([]MemberInfo, 0, len(g.members))
	for _, m := range g.members {
		out = append(out, m)
	}
	return out
}

// Leave gracefully announces departure and shuts down the memberlist.
func (g *Gossip) Leave(timeout time.Duration) error {
	close(g.stopCh)
	if err := g.list.Leave(timeout); err != nil {
		return err
	}
	return g.list.Shutdown()
}

// joinWithRetry attempts to join the seed nodes, retrying with backoff.
func (g *Gossip) joinWithRetry(seeds []string) {
	backoff := 500 * time.Millisecond
	for {
		_, err := g.list.Join(seeds)
		if err == nil {
			log.Printf("[gossip] joined cluster via seeds %v", seeds)
			return
		}
		log.Printf("[gossip] join failed (%v), retrying in %v", err, backoff)
		time.Sleep(backoff)
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

// changeWorker is a long-running goroutine that processes membership change
// signals from the eventDelegate. It runs outside memberlist's internal lock,
// making it safe to call list.Members().
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

// rebuildMembers rebuilds g.members from the live memberlist state and
// invokes onChange. Safe to call outside memberlist's internal lock.
func (g *Gossip) rebuildMembers() {
	g.mu.RLock()
	list := g.list
	g.mu.RUnlock()
	if list == nil {
		return
	}

	// Call list.Members() outside g.mu to avoid AB-BA deadlock.
	nodes := list.Members()

	newMembers := make(map[string]MemberInfo, len(nodes))
	for _, n := range nodes {
		meta, err := DecodeMeta(n.Meta)
		if err != nil || meta.ReplicaID == "" {
			continue
		}
		newMembers[meta.ReplicaID] = MemberInfo{
			ReplicaID:  meta.ReplicaID,
			GossipAddr: net.JoinHostPort(n.Addr.String(), strconv.Itoa(int(n.Port))),
			GRPCAddr:   net.JoinHostPort(n.Addr.String(), strconv.Itoa(meta.GRPCPort)),
		}
	}

	snapshot := make([]MemberInfo, 0, len(newMembers))
	for _, m := range newMembers {
		snapshot = append(snapshot, m)
	}

	g.mu.Lock()
	g.members = newMembers
	g.mu.Unlock()

	if g.onChange != nil {
		g.onChange(snapshot)
	}
}

// signal enqueues a membership-change signal. Called from memberlist's
// event goroutine; must NOT block or call any memberlist method.
func (g *Gossip) signal() {
	select {
	case g.changeCh <- struct{}{}:
	default: // channel full — a rebuild is already pending
	}
}

// ── memberlist.Delegate ───────────────────────────────────────────────────────

type delegate struct{ meta []byte }

func (d *delegate) NodeMeta(_ int) []byte           { return d.meta }
func (d *delegate) NotifyMsg([]byte)                {}
func (d *delegate) GetBroadcasts(_, _ int) [][]byte { return nil }
func (d *delegate) LocalState(_ bool) []byte        { return nil }
func (d *delegate) MergeRemoteState(_ []byte, _ bool) {}

// ── memberlist.EventDelegate ──────────────────────────────────────────────────

type eventDelegate struct{ gossip *Gossip }

func (e *eventDelegate) NotifyJoin(_ *memberlist.Node)   { e.gossip.signal() }
func (e *eventDelegate) NotifyLeave(_ *memberlist.Node)  { e.gossip.signal() }
func (e *eventDelegate) NotifyUpdate(_ *memberlist.Node) { e.gossip.signal() }
