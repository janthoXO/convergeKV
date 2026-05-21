// Package node implements the central state holder for a ConvergeKV replica.
// It owns the HLC, the storage layer, and the IBLT mirror.
// All state is persisted in BadgerDB; there is no in-memory copy of the CRDT map.
// All exported methods are safe for concurrent use.
package node

import (
	"sync"

	"github.com/janthoXO/convergeKV/internal/crdt"
	"github.com/janthoXO/convergeKV/internal/hlc"
	"github.com/janthoXO/convergeKV/internal/iblt"
	"github.com/janthoXO/convergeKV/internal/storage"
)

// keyEntry is the per-key lock with a reference count.
// refs counts goroutines that have acquired (or are waiting to acquire) the lock.
// When refs drops to zero the entry is removed from keyLocks.
type keyEntry struct {
	mu   sync.Mutex
	refs int
}

// Node is the central state holder. It is safe for concurrent use.
//
// Concurrency model
//
//   - keyLocks provides per-key write serialisation.
//     A write to key "A" and a write to key "B" proceed concurrently.
//     Two concurrent writes to key "A" are serialised.
//     Entries are reference-counted and removed from the map when no goroutine
//     holds or awaits the lock, so the map stays bounded to hot keys only.
//
//   - The HLC has its own internal mutex and is always safe to call without
//     holding any of the above locks.
//
//   - BadgerDB provides MVCC-consistent reads without any additional locking.
type Node struct {
	replicaID string
	hlc       *hlc.HLC
	ibltState *iblt.IBLTState

	// locksMu guards the keyLocks map itself (not the per-key locks).
	locksMu  sync.Mutex
	keyLocks map[string]*keyEntry

	store *storage.Store
}

// NodeOption configures a Node at construction time.
type NodeOption func(*Node)

// WithHLCFloor seeds the HLC so it will never issue a timestamp below floor.
// Pass the highest timestamp found in durable storage to restore monotonicity
// across restarts when the system clock has been corrected backwards by NTP.
func WithHLCFloor(floor hlc.Timestamp) NodeOption {
	return func(n *Node) {
		if hlc.Less(n.hlc.Now(), floor) {
			n.hlc.Seed(floor)
		}
	}
}

// New constructs a Node. State is served directly from BadgerDB — no upfront
// bulk load into memory. ibltState must be pre-built via iblt.BuildFromStore.
func New(replicaID string, store *storage.Store, ibltState *iblt.IBLTState, opts ...NodeOption) *Node {
	n := &Node{
		replicaID: replicaID,
		hlc:       hlc.New(),
		ibltState: ibltState,
		keyLocks:  make(map[string]*keyEntry),
		store:     store,
	}
	for _, opt := range opts {
		opt(n)
	}
	return n
}

// ReplicaID returns the node's stable identifier.
func (n *Node) ReplicaID() string { return n.replicaID }

// HLCNow returns the node's current HLC value.
func (n *Node) HLCNow() hlc.Timestamp { return n.hlc.Now() }

// ReceiveHLC advances the node's HLC with a remote timestamp.
func (n *Node) ReceiveHLC(remote hlc.Timestamp) (hlc.Timestamp, error) {
	return n.hlc.Receive(remote)
}

// IBLTSnapshot returns a consistent point-in-time copy of the IBLT for sync.
func (n *Node) IBLTSnapshot() *iblt.IBLT {
	return n.ibltState.Snapshot()
}

// GetField reads a single field entry directly from storage.
func (n *Node) GetField(key, field string) (crdt.FieldEntry, bool, error) {
	return n.store.GetField(key, field)
}

// IterateAll streams all persisted entries to fn. Paginates internally.
func (n *Node) IterateAll(fn func(key, field string, entry crdt.FieldEntry) error) error {
	return n.store.IterateAll(fn)
}

// acquireKey locks the per-key mutex and returns a release function.
func (n *Node) acquireKey(key string) func() {
	// Increment the ref count while holding locksMu so the entry cannot be
	// deleted between the lookup and the mu.Lock() call below.
	n.locksMu.Lock()
	e, ok := n.keyLocks[key]
	if !ok {
		e = &keyEntry{}
		n.keyLocks[key] = e
	}
	e.refs++
	n.locksMu.Unlock()

	e.mu.Lock()

	return func() {
		e.mu.Unlock()
		n.locksMu.Lock()
		e.refs--
		if e.refs == 0 {
			delete(n.keyLocks, key)
		}
		n.locksMu.Unlock()
	}
}
