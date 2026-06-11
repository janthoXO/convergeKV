package cluster

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/hashicorp/memberlist"
)

// delegate serves our metadata to gossip.
type delegate Cluster

func (d *delegate) NodeMeta(limit int) []byte {
	c := (*Cluster)(d)
	c.mu.RLock()
	defer c.mu.RUnlock()
	b := c.meta.Encode()
	if len(b) > limit {
		// Encoding size is bounded by config validation; reaching this is a bug.
		panic(fmt.Sprintf("cluster: meta %d bytes exceeds gossip limit %d", len(b), limit))
	}
	return b
}

func (d *delegate) NotifyMsg([]byte)                           {}
func (d *delegate) GetBroadcasts(overhead, limit int) [][]byte { return nil }
func (d *delegate) LocalState(join bool) []byte                { return nil }
func (d *delegate) MergeRemoteState(buf []byte, join bool)     {}

// eventDelegate maintains the dead-node table and pokes subscribers.
type eventDelegate Cluster

func (e *eventDelegate) NotifyJoin(n *memberlist.Node) {
	c := (*Cluster)(e)
	if meta, err := DecodeMeta(n.Meta); err == nil {
		c.mu.Lock()
		delete(c.dead, meta.ID)
		c.mu.Unlock()
	}
	c.log.Info("member joined", "node", n.Name, "addr", n.Address())
	c.notify()
}

func (e *eventDelegate) NotifyLeave(n *memberlist.Node) {
	c := (*Cluster)(e)
	if meta, err := DecodeMeta(n.Meta); err == nil {
		c.mu.Lock()
		c.dead[meta.ID] = time.Now()
		c.mu.Unlock()
	}
	c.log.Info("member left or died", "node", n.Name, "addr", n.Address())
	c.notify()
}

func (e *eventDelegate) NotifyUpdate(n *memberlist.Node) {
	c := (*Cluster)(e)
	c.log.Debug("member meta updated", "node", n.Name)
	c.notify()
}

// aliveDelegate rejects gossip about nodes with a mismatched partition count,
// keeping them out of our membership view.
type aliveDelegate Cluster

func (a *aliveDelegate) NotifyAlive(peer *memberlist.Node) error {
	c := (*Cluster)(a)
	meta, err := DecodeMeta(peer.Meta)
	if err != nil {
		return fmt.Errorf("cluster: rejecting node %s: %w", peer.Name, err)
	}
	if meta.Partitions != c.nodeP {
		return fmt.Errorf("cluster: rejecting node %s: partition count %d != ours %d",
			peer.Name, meta.Partitions, c.nodeP)
	}
	return nil
}

// mergeDelegate rejects whole-cluster merges (join-time push/pull) on
// partition count mismatch, which makes a misconfigured joiner's Join fail
// with a clear error so it can shut down.
type mergeDelegate Cluster

func (m *mergeDelegate) NotifyMerge(peers []*memberlist.Node) error {
	c := (*Cluster)(m)
	for _, peer := range peers {
		meta, err := DecodeMeta(peer.Meta)
		if err != nil {
			return fmt.Errorf("cluster: refusing merge with %s: %w", peer.Name, err)
		}
		if meta.Partitions != c.nodeP {
			return fmt.Errorf("cluster: refusing merge with %s: partition count %d != ours %d",
				peer.Name, meta.Partitions, c.nodeP)
		}
	}
	return nil
}

// slogWriter adapts memberlist's log output to slog at debug level.
type slogWriter struct{ log *slog.Logger }

func (w slogWriter) Write(p []byte) (int, error) {
	w.log.Debug("memberlist", "msg", string(p))
	return len(p), nil
}
