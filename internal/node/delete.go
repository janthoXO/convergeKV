package node

import (
	"fmt"

	"github.com/janthoXO/convergeKV/internal/crdt"
	"github.com/janthoXO/convergeKV/internal/hlc"
	"github.com/janthoXO/convergeKV/internal/storage"
)

// Delete marks all current fields of key as tombstones.
// Fields added after the delete timestamp are NOT affected (they will win on merge).
// Returns the HLC timestamp and the exact tombstone entries written, so callers
// can push them to peers without a second Badger read.
//
// Concurrent Deletes to different keys proceed without blocking each other.
func (n *Node) Delete(key string) (hlc.Timestamp, []storage.FieldUpdate, error) {
	release := n.acquireKey(key)
	defer release()

	ts, err := n.hlc.Send()
	if err != nil {
		return hlc.Timestamp{}, nil, fmt.Errorf("delete: %w", err)
	}

	m, err := n.store.GetKey(key)
	if err != nil {
		return hlc.Timestamp{}, nil, fmt.Errorf("delete: read error: %w", err)
	}
	if len(m.Fields) == 0 {
		return ts, nil, nil // nothing to delete
	}

	var batch []storage.FieldUpdate
	for field := range m.Fields {
		batch = append(batch, storage.FieldUpdate{Key: key, Field: field, Entry: crdt.FieldEntry{
			Value:     nil,
			Timestamp: ts,
			ReplicaID: n.replicaID,
			Deleted:   true,
		}})
	}

	// Persist first, then update IBLT.
	if err := n.store.SaveBatch(batch); err != nil {
		return hlc.Timestamp{}, nil, fmt.Errorf("delete: storage error: %w", err)
	}

	for _, u := range batch {
		n.ibltState.RemoveEntry(key, u.Field, m.Fields[u.Field])
		n.ibltState.InsertEntry(key, u.Field, u.Entry)
	}

	return ts, batch, nil
}
