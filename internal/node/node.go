package node

import (
    "sync"

    "github.com/janthoXO/convergeKV/internal/crdt"
    "github.com/janthoXO/convergeKV/internal/hlc"
    "github.com/janthoXO/convergeKV/internal/storage"
)

// Node is the central state holder. It is safe for concurrent use.
type Node struct {
    mu        sync.RWMutex
    replicaID string
    hlc       *hlc.HLC
    state     map[string]crdt.AWLWWMap // key -> per-field CRDT map
    store     *storage.Store
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

// getMap returns (a copy of) the AWLWWMap for key, or an empty map.
// Caller must not hold n.mu.
func (n *Node) getMap(key string) crdt.AWLWWMap {
    n.mu.RLock()
    defer n.mu.RUnlock()
    m, ok := n.state[key]
    if !ok {
        return crdt.NewAWLWWMap()
    }
    // shallow copy: Fields map reference is safe because we only read it
    return m
}
