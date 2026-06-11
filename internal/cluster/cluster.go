// Package cluster wraps hashicorp/memberlist: SWIM gossip membership with a
// metadata payload carrying the node UUID, the cluster partition count P,
// the node generation, and per-partition status flags.
//
// Consumers (placement) subscribe to a coalescing change signal and pull a
// consistent snapshot of the membership view — no per-event delivery, so a
// slow consumer can never lose membership state, only see it late.
package cluster

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
)

type Config struct {
	NodeID     [16]byte
	Partitions uint16
	RPCAddr    string // node-service gRPC address gossiped to peers
	BindAddr   string // host:port for gossip; port 0 picks a free port
	Advertise  string // optional host:port others should use
	Seeds      []string
	// Memberlist overrides the base memberlist config (tests use
	// DefaultLocalConfig for fast convergence); nil means DefaultLANConfig.
	Memberlist *memberlist.Config
	Logger     *slog.Logger
}

// Member is one alive cluster member.
type Member struct {
	Meta NodeMeta
	Addr string
}

type Cluster struct {
	ml    *memberlist.Memberlist
	log   *slog.Logger
	nodeP uint16

	mu     sync.RWMutex
	meta   NodeMeta // our gossiped metadata
	alive  map[[16]byte]Member
	dead   map[[16]byte]time.Time
	change chan struct{}
}

// Join starts gossip, joins the seeds (if any), and verifies the cluster
// agrees on P. A partition-count mismatch is a fatal error: the caller must
// shut down.
func Join(cfg Config) (*Cluster, error) {
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	c := &Cluster{
		log:    log,
		nodeP:  cfg.Partitions,
		alive:  make(map[[16]byte]Member),
		dead:   make(map[[16]byte]time.Time),
		change: make(chan struct{}, 1),
		meta: NodeMeta{
			ID:         cfg.NodeID,
			Partitions: cfg.Partitions,
			Generation: uint64(time.Now().UnixMilli()),
			RPCAddr:    cfg.RPCAddr,
			Flags:      NewPartitionFlags(cfg.Partitions),
		},
	}

	mlc := cfg.Memberlist
	if mlc == nil {
		mlc = memberlist.DefaultLANConfig()
	}
	mlc.Name = fmt.Sprintf("%x", cfg.NodeID)
	if cfg.BindAddr != "" {
		host, port, err := splitHostPort(cfg.BindAddr)
		if err != nil {
			return nil, fmt.Errorf("cluster: bind addr: %w", err)
		}
		mlc.BindAddr, mlc.BindPort = host, port
	}
	if cfg.Advertise != "" {
		host, port, err := splitHostPort(cfg.Advertise)
		if err != nil {
			return nil, fmt.Errorf("cluster: advertise addr: %w", err)
		}
		mlc.AdvertiseAddr, mlc.AdvertisePort = host, port
	}
	mlc.Delegate = (*delegate)(c)
	mlc.Events = (*eventDelegate)(c)
	mlc.Alive = (*aliveDelegate)(c)
	mlc.Merge = (*mergeDelegate)(c)
	mlc.LogOutput = slogWriter{log}

	ml, err := memberlist.Create(mlc)
	if err != nil {
		return nil, fmt.Errorf("cluster: create memberlist: %w", err)
	}
	c.ml = ml

	if len(cfg.Seeds) > 0 {
		if _, err := ml.Join(cfg.Seeds); err != nil {
			_ = ml.Shutdown()
			return nil, fmt.Errorf("cluster: join seeds (partition count mismatch is fatal): %w", err)
		}
	}
	return c, nil
}

// GossipAddr returns the address this node's gossip listener is reachable at
// (the seed address for joining nodes).
func (c *Cluster) GossipAddr() string { return c.ml.LocalNode().Address() }

// Self returns our own current metadata.
func (c *Cluster) Self() NodeMeta {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return cloneMeta(c.meta)
}

// Members returns all alive members (including self) with decoded metadata.
// The view is maintained from gossip event callbacks — memberlist's own node
// list mutates its Meta in place and must not be read directly.
func (c *Cluster) Members() []Member {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]Member, 0, len(c.alive))
	for _, m := range c.alive {
		out = append(out, cloneMember(m))
	}
	return out
}

// DeadSince returns when a currently-dead node was declared dead, if it is.
func (c *Cluster) DeadSince(id [16]byte) (time.Time, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	t, ok := c.dead[id]
	return t, ok
}

// Changed returns a coalescing signal channel: it receives (at least) one
// value after any membership or metadata change. Consumers re-pull Members().
func (c *Cluster) Changed() <-chan struct{} { return c.change }

// SetPartitionStatus updates our gossiped status flag for one partition.
func (c *Cluster) SetPartitionStatus(pid uint16, s Status) error {
	return c.UpdateFlags(func(f PartitionFlags) { f.Set(pid, s) })
}

// UpdateFlags applies a bulk mutation to our partition status flags and
// pushes one gossip metadata update.
func (c *Cluster) UpdateFlags(mutate func(PartitionFlags)) error {
	c.mu.Lock()
	mutate(c.meta.Flags)
	if self, ok := c.alive[c.meta.ID]; ok { // our own view updates immediately
		self.Meta = cloneMeta(c.meta)
		c.alive[c.meta.ID] = self
	}
	c.mu.Unlock()
	c.notify()
	// Push the new meta through gossip (bounded wait; propagation continues
	// asynchronously regardless).
	return c.ml.UpdateNode(2 * time.Second)
}

// Leave broadcasts a graceful leave, then stops gossip.
func (c *Cluster) Leave(timeout time.Duration) error {
	if err := c.ml.Leave(timeout); err != nil {
		return err
	}
	return c.ml.Shutdown()
}

// Shutdown stops gossip without announcing (crash-like, for tests).
func (c *Cluster) Shutdown() error { return c.ml.Shutdown() }

func (c *Cluster) notify() {
	select {
	case c.change <- struct{}{}:
	default: // a signal is already pending; consumers re-pull anyway
	}
}

func cloneMeta(m NodeMeta) NodeMeta {
	m.Flags = m.Flags.Clone()
	return m
}

func cloneMember(m Member) Member {
	m.Meta = cloneMeta(m.Meta)
	return m
}
