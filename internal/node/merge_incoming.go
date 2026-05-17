package node

import (
	"github.com/janthoXO/convergeKV/internal/crdt"
	"github.com/janthoXO/convergeKV/internal/storage"
)

// ApplyDelta merges a single incoming field entry from a peer into local state.
// It advances the HLC with the remote timestamp.
// Returns true if the incoming entry actually changed local state.
//
// Under quorum=1 multi-writer with HRW, the coordinator and push routing ensure
// that entries normally arrive only at nodes that are HRW replicas for the key.
// However, entries arriving via IBLT sync may transiently come for keys where
// this node is not currently a replica (due to membership view differences).
// The CRDT merge is safe to apply regardless; IBLT sync self-corrects once
// membership views converge.
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

	// incoming wins — update the IBLT.
	if n.ibltState != nil {
		if exists {
			n.ibltState.RemoveEntry(key, field, existing)
		}
		n.ibltState.InsertEntry(key, field, incoming)
	}

	crdt.Apply(&m, field, incoming)
	n.commitKey(key, m)

	err := n.store.SaveBatch([]storage.FieldUpdate{
		{Key: key, Field: field, Entry: incoming},
	})

	return true, err
}
