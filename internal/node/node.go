// Package node implements the central state holder for a ConvergeKV replica.
// It owns the HLC and the storage layer. All state is persisted in BadgerDB;
// there is no in-memory copy of the CRDT map.
// All exported methods are safe for concurrent use.
package node

import (
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

	// ibltState mirrors the node's durable state in an IBLT for sync.
	// It is injected after construction via SetIBLTState; nil means not yet set.
	ibltState IBLTUpdater

	// locksMu guards the keyLocks map itself (not the per-key locks).
	locksMu  sync.Mutex
	keyLocks map[string]*keyEntry

	store *storage.Store
}

// New constructs a Node. State is served directly from BadgerDB — no upfront
// bulk load into memory.
func New(replicaID string, store *storage.Store) (*Node, error) {
	return &Node{
		replicaID: replicaID,
		hlc:       hlc.New(),
		keyLocks:  make(map[string]*keyEntry),
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

// Store returns the underlying storage handle. Used by syncer and coordinator
// to perform direct Badger reads without going through the node's write path.
func (n *Node) Store() *storage.Store { return n.store }

// acquireKey locks the per-key mutex and returns a release function that unlocks
// it and decrements the reference count, removing the entry from keyLocks if
// the count reaches zero.
//
// Usage:
//
//	release := n.acquireKey(key)
//	defer release()
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
