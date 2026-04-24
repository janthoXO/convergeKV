package node

import (
	"fmt"

	"github.com/janthoXO/convergeKV/internal/crdt"
	"github.com/janthoXO/convergeKV/internal/hlc"
	"github.com/janthoXO/convergeKV/internal/storage"
)

// Delete marks all current fields of key as tombstones.
// Fields added after the delete timestamp are NOT affected (they will win on merge).
func (n *Node) Delete(key string) (hlc.Timestamp, error) {
	ts := n.hlc.Send()

	n.mu.Lock()
	defer n.mu.Unlock()

	m, ok := n.state[key]
	if !ok {
		return ts, nil // nothing to delete
	}

	var batch []storage.FieldUpdate
	for field := range m.Fields {
		tombstone := crdt.FieldEntry{
			Value:     nil,
			Timestamp: ts,
			ReplicaID: n.replicaID,
			Deleted:   true,
		}
		m.Fields[field] = tombstone
		batch = append(batch, storage.FieldUpdate{Key: key, Field: field, Entry: tombstone})
	}
	n.state[key] = m

	if err := n.store.SaveBatch(batch); err != nil {
		return hlc.Timestamp{}, fmt.Errorf("delete: storage error: %w", err)
	}
	return ts, nil
}
