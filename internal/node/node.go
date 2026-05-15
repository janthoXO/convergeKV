// Package node implements the central state holder for a ConvergeKV replica.
// It owns the HLC, the in-memory AWLWWMap state, and the storage layer.
// All exported methods are safe for concurrent use.
package node

import (
	"maps"
	"sync"

	"github.com/janthoXO/convergeKV/internal/crdt"
	"github.com/janthoXO/convergeKV/internal/hlc"
	"github.com/janthoXO/convergeKV/internal/storage"
)

// IBLTUpdater is the minimal interface that the IBLT state object must satisfy.
// Defined here to avoid a circular import between node and syncer.
// syncer.IBLTState implements this interface.
type IBLTUpdater interface {
	InsertEntry(key, field string, e crdt.FieldEntry)
	RemoveEntry(key, field string, e crdt.FieldEntry)
}

// DeltaRecord is a flat (key, field, entry) triple used in replication.
// Replaces the old KeyFieldEntryTuple name for clarity.
type DeltaRecord = KeyFieldEntryTuple

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

	// ibltState mirrors the node's in-memory state in an IBLT for sync.
	// It is injected after construction via SetIBLTState; nil means not yet set.
	ibltState IBLTUpdater

	// stateMu guards the state map structure. Held only for map reads/writes.
	stateMu sync.RWMutex
	state   map[string]crdt.AWLWWMap

	// locksMu guards the keyLocks map itself (not the per-key locks).
	locksMu  sync.Mutex
	keyLocks map[string]*sync.RWMutex

	store *storage.Store
}

// New constructs a Node, loading existing state from the store.
func New(replicaID string, store *storage.Store) (*Node, error) {
	existing, err := store.LoadAll()
	if err != nil {
		return nil, err
	}

	return &Node{
		replicaID: replicaID,
		hlc:       hlc.New(),
		state:     existing,
		keyLocks:  make(map[string]*sync.RWMutex),
		store:     store,
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

// SetIBLTState injects the IBLT state object. Called from main.go after
// both node and IBLTState are constructed. Thread-safe: called once at startup
// before any concurrent requests.
func (n *Node) SetIBLTState(s IBLTUpdater) {
	n.ibltState = s
}

// Snapshot returns a flat list of all current (key, field, entry) triples.
// Used by the full-state sync fallback and IBLT state reconstruction.
func (n *Node) Snapshot() []KeyFieldEntryTuple {
	n.stateMu.RLock()
	defer n.stateMu.RUnlock()

	var out []KeyFieldEntryTuple
	for key, m := range n.state {
		for field, entry := range m.Fields {
			out = append(out, KeyFieldEntryTuple{Key: key, Field: field, Entry: entry})
		}
	}
	return out
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
