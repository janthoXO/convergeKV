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

	crdt.Apply(&m, field, incoming)
	n.commitKey(key, m)

	err := n.store.SaveBatch([]storage.FieldUpdate{
		{Key: key, Field: field, Entry: incoming},
	})

	return true, err
}

// Snapshot returns a flat list of every (key, field, FieldEntry) the node holds.
// Used by the anti-entropy sender to compute deltas for a peer.
// It holds stateMu.RLock for the duration of the iteration, which is acceptable
// since Snapshot is called infrequently (every anti-entropy interval).
func (n *Node) Snapshot() []DeltaRecord {
	n.stateMu.RLock()
	defer n.stateMu.RUnlock()

	var out []DeltaRecord
	for key, m := range n.state {
		for field, entry := range m.Fields {
			out = append(out, DeltaRecord{Key: key, Field: field, Entry: entry})
		}
	}
	return out
}

// DeltaRecord is a flat (key, field, entry) triple used in replication.
type DeltaRecord struct {
	Key   string
	Field string
	Entry crdt.FieldEntry
}
