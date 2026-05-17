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

// Node is the central state holder. It is safe for concurrent use.
//
// Concurrency model
//
//   - keyLocks provides per-key write serialisation.
//     A write to key "A" and a write to key "B" proceed concurrently.
//     Two concurrent writes to key "A" are serialised.
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
	keyLocks map[string]*sync.RWMutex

	store *storage.Store
}

// New constructs a Node. State is served directly from BadgerDB — no upfront
// bulk load into memory.
func New(replicaID string, store *storage.Store) (*Node, error) {
	return &Node{
		replicaID: replicaID,
		hlc:       hlc.New(),
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

// Store returns the underlying storage handle. Used by syncer and coordinator
// to perform direct Badger reads without going through the node's write path.
func (n *Node) Store() *storage.Store { return n.store }

// getKeyLock returns the per-key RWMutex for key, creating it lazily.
// Must NOT be called while holding another getKeyLock.
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
