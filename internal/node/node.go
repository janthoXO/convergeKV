// Package node implements the central state holder for a ConvergeKV replica.
// It owns the HLC, the in-memory AWLWWMap state, and the storage layer.
// All exported methods are safe for concurrent use.
package node

import (
	"maps"
	"sync"

	"github.com/janthoXO/convergeKV/internal/crdt"
	"github.com/janthoXO/convergeKV/internal/hlc"
	"github.com/janthoXO/convergeKV/internal/merkle"
	"github.com/janthoXO/convergeKV/internal/storage"
)

// Node is the central state holder. It is safe for concurrent use.
//
// Concurrency model
//
//   - stateMu protects the outer state map (the Go map itself).
//     It is held for the briefest possible window — only the map read/write
//     operations, never across I/O or computation.
//     Multiple goroutines can read concurrently via RLock.
//
//   - keyLocks provides per-key write serialisation.
//     A write to key "A" and a write to key "B" proceed concurrently.
//     Two concurrent writes to key "A" are serialised.
//
//   - The HLC has its own internal mutex and is always safe to call without
//     holding any of the above locks.
type Node struct {
	replicaID string
	hlc       *hlc.HLC

	// stateMu guards the state map structure. Held only for map reads/writes.
	stateMu sync.RWMutex
	state   map[string]crdt.AWLWWMap

	// locksMu guards the keyLocks map itself (not the per-key locks).
	locksMu  sync.Mutex
	keyLocks map[string]*sync.RWMutex

	store *storage.Store
	tree  *merkle.MerkleTree // live Merkle tree over all (key, field) entries
}

// New constructs a Node, loading existing state from the store.
func New(replicaID string, store *storage.Store) (*Node, error) {
	existing, err := store.LoadAll()
	if err != nil {
		return nil, err
	}

	// Rebuild the Merkle tree from persisted state.
	tree := merkle.NewMerkleTree()
	for key, m := range existing {
		for field, e := range m.Fields {
			tree.Update(key, field, e.ReplicaID, e.Timestamp.PhysicalMs, e.Timestamp.Logical)
		}
	}

	return &Node{
		replicaID: replicaID,
		hlc:       hlc.New(),
		state:     existing,
		keyLocks:  make(map[string]*sync.RWMutex),
		store:     store,
		tree:      tree,
	}, nil
}

// ReplicaID returns the node's stable identifier.
func (n *Node) ReplicaID() string { return n.replicaID }

// HLCNow returns the node's current HLC value.
func (n *Node) HLCNow() hlc.Timestamp { return n.hlc.Now() }

// ReceiveHLC advances the node's HLC with a remote timestamp.
func (n *Node) ReceiveHLC(remote hlc.Timestamp) hlc.Timestamp {
	return n.hlc.Receive(remote)
}

// MerkleTree returns the node's live Merkle tree.
// The replication layer reads from this to build sync messages.
func (n *Node) MerkleTree() *merkle.MerkleTree {
	return n.tree
}

// SnapshotBuckets returns all DeltaRecords whose key maps to one of the given buckets.
// Used by the replication handler to answer Phase 2 requests.
func (n *Node) SnapshotBuckets(buckets []int) []KeyFieldEntryTuple {
	// Build a set for O(1) lookup.
	wanted := make(map[int]struct{}, len(buckets))
	for _, b := range buckets {
		wanted[b] = struct{}{}
	}

	n.stateMu.RLock()
	defer n.stateMu.RUnlock()

	var out []KeyFieldEntryTuple
	for key, m := range n.state {
		if _, ok := wanted[merkle.BucketIndex(key)]; !ok {
			continue
		}
		for field, entry := range m.Fields {
			out = append(out, KeyFieldEntryTuple{Key: key, Field: field, Entry: entry})
		}
	}
	return out
}

// updateTree adds the hash of entry to the tree.
// Must be called under n.mu write lock.
// If replacing an existing entry, call removeTree first.
func (n *Node) updateTree(key, field string, e crdt.FieldEntry) {
	n.tree.Update(key, field, e.ReplicaID, e.Timestamp.PhysicalMs, e.Timestamp.Logical)
}

// removeTree removes the hash of entry from the tree.
// Must be called under n.mu write lock.
func (n *Node) removeTree(key, field string, e crdt.FieldEntry) {
	n.tree.Remove(key, field, e.ReplicaID, e.Timestamp.PhysicalMs, e.Timestamp.Logical)
}

// getKeyLock returns the per-key RWMutex for key, creating it lazily.
// Must NOT be called while holding stateMu or another getKeyLock.
func (n *Node) getKeyLock(key string) *sync.RWMutex {
	n.locksMu.Lock()
	defer n.locksMu.Unlock()

	if l, ok := n.keyLocks[key]; ok {
		return l
	}

	l := &sync.RWMutex{}
	n.keyLocks[key] = l
	return l
}

// snapshotKey reads the current AWLWWMap for key under a brief stateMu.RLock
// and returns a deep copy safe to modify without holding any lock.
// Returns an empty map if the key doesn't exist.
func (n *Node) snapshotKey(key string) crdt.AWLWWMap {
	n.stateMu.RLock()
	m, ok := n.state[key]
	n.stateMu.RUnlock()

	// Deep-copy the Fields map so callers can modify it freely without
	// affecting the shared state or racing with concurrent readers.
	dst := crdt.NewAWLWWMap()
	if ok {
		maps.Copy(dst.Fields, m.Fields)
	}

	return dst
}

// commitKey writes an updated AWLWWMap for key under a brief stateMu.Lock.
func (n *Node) commitKey(key string, m crdt.AWLWWMap) {
	n.stateMu.Lock()
	n.state[key] = m
	n.stateMu.Unlock()
}

// getMap returns the AWLWWMap for key (shallow copy) for read-only use.
// No per-key lock required since readers tolerate seeing a consistent
// snapshot of whichever version the map holds at call time.
func (n *Node) getMap(key string) crdt.AWLWWMap {
	n.stateMu.RLock()
	m, ok := n.state[key]
	n.stateMu.RUnlock()
	if !ok {
		return crdt.NewAWLWWMap()
	}
	return m
}
