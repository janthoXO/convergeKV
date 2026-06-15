package cluster

import (
	"encoding/hex"
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

// The callbacks run inside memberlist's state-update path: n.Meta is stable
// for their duration but mutated in place afterwards, so DecodeMeta's copy
// here is the only safe place to capture it.

func (e *eventDelegate) NotifyJoin(n *memberlist.Node) {
	c := (*Cluster)(e)
	meta, err := DecodeMeta(n.Meta)
	if err != nil {
		c.log.Warn("ignoring joined member with bad meta", "node", n.Name, "err", err)
		return
	}
	c.mu.Lock()
	c.alive[meta.ID] = Member{Meta: meta, Addr: n.Address()}
	delete(c.dead, meta.ID)
	c.mu.Unlock()
	c.log.Info("member joined", "node", n.Name, "addr", n.Address())
	c.notify()
}

func (e *eventDelegate) NotifyLeave(n *memberlist.Node) {
	c := (*Cluster)(e)
	var id [16]byte
	if meta, err := DecodeMeta(n.Meta); err == nil {
		id = meta.ID
		c.mu.Lock()
		// A graceful leave releases the placement slot immediately; only
		// unannounced deaths hold it for the grace period.
		if n.State != memberlist.StateLeft {
			if m, ok := c.alive[meta.ID]; ok {
				c.dead[meta.ID] = deadEntry{member: m, since: time.Now()}
			} else {
				c.dead[meta.ID] = deadEntry{member: Member{Meta: meta, Addr: n.Address()}, since: time.Now()}
			}
		}
		delete(c.alive, meta.ID)
		c.mu.Unlock()
	} else if raw, err := hex.DecodeString(n.Name); err == nil && len(raw) == len(id) {
		copy(id[:], raw)
		// Corrupt meta must not leave the member in the view forever; the
		// memberlist name is the hex node ID, so eviction works without it.
		c.log.Warn("leaving member has bad meta; evicting by name", "node", n.Name)
		c.mu.Lock()
		if m, ok := c.alive[id]; ok && n.State != memberlist.StateLeft {
			c.dead[id] = deadEntry{member: m, since: time.Now()}
		}
		delete(c.alive, id)
		c.mu.Unlock()
	} else {
		c.log.Warn("leaving member has bad meta and unparseable name; cannot evict", "node", n.Name)
	}
	c.log.Info("member left or died", "node", n.Name, "addr", n.Address())
	c.notify()
}

func (e *eventDelegate) NotifyUpdate(n *memberlist.Node) {
	c := (*Cluster)(e)
	meta, err := DecodeMeta(n.Meta)
	if err != nil {
		c.log.Warn("ignoring meta update with bad meta", "node", n.Name, "err", err)
		return
	}
	c.mu.Lock()
	c.alive[meta.ID] = Member{Meta: meta, Addr: n.Address()}
	c.mu.Unlock()
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
