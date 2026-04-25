package node

import (
	"fmt"

	"github.com/janthoXO/convergeKV/internal/crdt"
	"github.com/janthoXO/convergeKV/internal/hlc"
	"github.com/janthoXO/convergeKV/internal/storage"
)

// Delete marks all current fields of key as tombstones.
// Fields added after the delete timestamp are NOT affected (they will win on merge).
//
// Concurrent Deletes to different keys proceed without blocking each other.
func (n *Node) Delete(key string) (hlc.Timestamp, error) {
	ts := n.hlc.Send()

	kl := n.getKeyLock(key)
	kl.Lock()
	defer kl.Unlock()

	m := n.snapshotKey(key)
	if len(m.Fields) == 0 {
		return ts, nil // nothing to delete
	}

	var batch []storage.FieldUpdate
	for field := range m.Fields {
		old := m.Fields[field] // capture old live entry
		tombstone := crdt.FieldEntry{
			Value:     nil,
			Timestamp: ts,
			ReplicaID: n.replicaID,
			Deleted:   true,
		}

		m.Fields[field] = tombstone

		// replace old tree entry with tombstone
		n.removeTree(key, field, old)
		n.updateTree(key, field, tombstone)
		
		batch = append(batch, storage.FieldUpdate{Key: key, Field: field, Entry: tombstone})
	}

	n.commitKey(key, m)

	if err := n.store.SaveBatch(batch); err != nil {
		return hlc.Timestamp{}, fmt.Errorf("delete: storage error: %w", err)
	}

	return ts, nil
}
