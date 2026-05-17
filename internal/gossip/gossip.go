package gossip

import (
	"fmt"
	"log"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"

	"github.com/janthoXO/convergeKV/internal/partition"
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

// Gossip wraps a memberlist instance and exposes a stable membership view
// and the shared SlotMap propagated via the push/pull full-state mechanism.
type Gossip struct {
	mu          sync.RWMutex
	list        *memberlist.Memberlist
	members     map[string]MemberInfo // keyed by ReplicaID
	onChange    ChangeHandler
	localMeta   NodeMeta
	slotMap     partition.SlotMap
	onSlotMapCh func(partition.SlotMap) // called when slot map version advances

	// changeCh is written by the eventDelegate (inside memberlist's lock) and
	// read by the background worker goroutine (outside memberlist's lock).
	// This prevents calling list.Members() while memberlist holds its own lock.
	changeCh chan struct{}
	stopCh   chan struct{}
}

// Config holds the parameters for starting a Gossip instance.
type Config struct {
	BindAddr        string
	BindPort        int
	LocalMeta       NodeMeta                // this node's metadata
	Seeds           []string                // host:port of seed nodes; may be empty
	OnChange        ChangeHandler           // called on every membership change
	InitialSlotMap  partition.SlotMap       // starting slot map for this node
	OnSlotMapChange func(partition.SlotMap) // fired when slot map version advances
}

// Start creates and starts a memberlist node, then joins via seeds.
// It returns once the node is participating in gossip (seeds may be empty).
func Start(cfg Config) (*Gossip, error) {
	g := &Gossip{
		members:     make(map[string]MemberInfo),
		onChange:    cfg.OnChange,
		localMeta:   cfg.LocalMeta,
		slotMap:     cfg.InitialSlotMap,
		onSlotMapCh: cfg.OnSlotMapChange,
		changeCh:    make(chan struct{}, 16), // buffered; drops coalesced signals
		stopCh:      make(chan struct{}),
	}

	mlCfg := memberlist.DefaultLANConfig()
	mlCfg.BindAddr = cfg.BindAddr
	mlCfg.BindPort = cfg.BindPort
	mlCfg.Name = cfg.LocalMeta.ReplicaID

	// Use the gossipDelegate which handles both node metadata AND slot map exchange.
	mlCfg.Delegate = &gossipDelegate{gossip: g, meta: EncodeMeta(cfg.LocalMeta)}
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

// CurrentSlotMap returns a snapshot of the current slot map.
func (g *Gossip) CurrentSlotMap() partition.SlotMap {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.slotMap
}

// ProposeSlotMap stores a new slot map if its version is higher than the current one,
// then triggers an immediate push/pull with peers to propagate it.
func (g *Gossip) ProposeSlotMap(sm partition.SlotMap) {
	g.mu.Lock()
	if sm.Version <= g.slotMap.Version {
		g.mu.Unlock()
		return
	}
	g.slotMap = sm
	list := g.list
	g.mu.Unlock()

	if g.onSlotMapCh != nil {
		g.onSlotMapCh(sm)
	}
	if list != nil {
		// Force an immediate push/pull cycle to propagate the new map.
		list.UpdateNode(0)
	}
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
		// Copy the IP bytes before calling String() to avoid a data race with
		// memberlist's internal goroutine that may mutate the n.Addr slice.
		ipCopy := make(net.IP, len(n.Addr))
		copy(ipCopy, n.Addr)
		port := int(n.Port)
		newMembers[meta.ReplicaID] = MemberInfo{
			ReplicaID:  meta.ReplicaID,
			GossipAddr: net.JoinHostPort(ipCopy.String(), strconv.Itoa(port)),
			GRPCAddr:   net.JoinHostPort(ipCopy.String(), strconv.Itoa(meta.GRPCPort)),
		}
	}

	snapshot := make([]MemberInfo, 0, len(newMembers))
	for _, m := range newMembers {
		snapshot = append(snapshot, m)
	}

	g.mu.Lock()
	g.members = newMembers
	currentSM := g.slotMap
	g.mu.Unlock()

	if g.onChange != nil {
		g.onChange(snapshot)
	}
	// Also notify slot map listeners so they get a refresh after membership change.
	if g.onSlotMapCh != nil {
		g.onSlotMapCh(currentSM)
	}
}

// mergeRemoteSlotMap merges a received slot map and fires the callback if the
// version advances. Called from gossipDelegate.MergeRemoteState.
func (g *Gossip) mergeRemoteSlotMap(b []byte) {
	if len(b) == 0 {
		return
	}
	incoming, err := partition.Decode(b)
	if err != nil {
		// Silently ignore the common case of a peer that hasn't configured a slot
		// map yet (bootstrap mode sends a zero-slot JSON object).
		return
	}

	g.mu.Lock()
	prev := g.slotMap
	merged := prev.Merge(incoming)
	if merged.Version == prev.Version {
		g.mu.Unlock()
		return // no change — identical content or incoming was older
	}
	g.slotMap = merged
	list := g.list
	g.mu.Unlock()

	log.Printf("[gossip] slot map version advanced to %d", merged.Version)
	if g.onSlotMapCh != nil {
		g.onSlotMapCh(merged)
	}

	// If the merge resolved a split-brain (version bumped above both inputs),
	// push the result immediately so peers converge without waiting for the
	// next scheduled push/pull cycle.
	if merged.Version > incoming.Version && list != nil {
		list.UpdateNode(0)
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

// gossipDelegate handles both node metadata (NodeMeta) and full-state slot map
// exchange (LocalState/MergeRemoteState). Implements memberlist.Delegate.
type gossipDelegate struct {
	gossip *Gossip
	meta   []byte // pre-encoded NodeMeta
}

func (d *gossipDelegate) NodeMeta(_ int) []byte           { return d.meta }
func (d *gossipDelegate) NotifyMsg([]byte)                {}
func (d *gossipDelegate) GetBroadcasts(_, _ int) [][]byte { return nil }

// LocalState is called by memberlist during push/pull. Return the serialised SlotMap.
func (d *gossipDelegate) LocalState(_ bool) []byte {
	d.gossip.mu.RLock()
	sm := d.gossip.slotMap
	d.gossip.mu.RUnlock()

	b, err := sm.Encode()
	if err != nil {
		log.Printf("[gossip] LocalState: encode slot map: %v", err)
		return nil
	}
	return b
}

// MergeRemoteState is called by memberlist when remote state arrives.
func (d *gossipDelegate) MergeRemoteState(buf []byte, _ bool) {
	d.gossip.mergeRemoteSlotMap(buf)
}

// ── memberlist.EventDelegate ──────────────────────────────────────────────────

type eventDelegate struct{ gossip *Gossip }

func (e *eventDelegate) NotifyJoin(_ *memberlist.Node)   { e.gossip.signal() }
func (e *eventDelegate) NotifyLeave(_ *memberlist.Node)  { e.gossip.signal() }
func (e *eventDelegate) NotifyUpdate(_ *memberlist.Node) { e.gossip.signal() }
