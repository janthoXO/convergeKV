package node

import (
	"github.com/janthoXO/convergeKV/internal/crdt"
	"github.com/janthoXO/convergeKV/internal/storage"
)

// ApplyDelta merges a single incoming field entry from a peer into local state.
// It advances the HLC with the remote timestamp.
// Returns true if the incoming entry actually changed local state.
//
// Concurrent ApplyDelta calls for different keys proceed without blocking each other.
func (n *Node) ApplyDelta(key, field string, incoming crdt.FieldEntry) (bool, error) {
	_ = n.hlc.Receive(incoming.Timestamp) // advance HLC; has its own internal mutex

	kl := n.getKeyLock(key)
	kl.Lock()
	defer kl.Unlock()

	m := n.snapshotKey(key)

	existing, exists := m.Fields[field]
	if exists && !crdt.WinsOver(incoming, existing) {
		return false, nil // local entry already wins; no change
	}

	// incoming wins — update the Merkle tree.
	if exists {
		n.removeTree(key, field, existing)
	}
	crdt.Apply(&m, field, incoming)
	n.updateTree(key, field, incoming)
	n.commitKey(key, m)

	err := n.store.SaveBatch([]storage.FieldUpdate{
		{Key: key, Field: field, Entry: incoming},
	})

	return true, err
}
