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
	ts := n.hlc.Send()

	release := n.acquireKey(key)
	defer release()

	m, err := n.store.GetKey(key)
	if err != nil {
		return hlc.Timestamp{}, nil, fmt.Errorf("delete: read error: %w", err)
	}
	if len(m.Fields) == 0 {
		return ts, nil, nil // nothing to delete
	}

	type ibltDelta struct {
		field     string
		tombstone crdt.FieldEntry
		old       crdt.FieldEntry
	}
	var deltas []ibltDelta
	var batch []storage.FieldUpdate

	for field, old := range m.Fields {
		tombstone := crdt.FieldEntry{
			Value:     nil,
			Timestamp: ts,
			ReplicaID: n.replicaID,
			Deleted:   true,
		}
		deltas = append(deltas, ibltDelta{field, tombstone, old})
		batch = append(batch, storage.FieldUpdate{Key: key, Field: field, Entry: tombstone})
	}

	// Persist first, then update IBLT.
	if err := n.store.SaveBatch(batch); err != nil {
		return hlc.Timestamp{}, nil, fmt.Errorf("delete: storage error: %w", err)
	}

	if n.ibltState != nil {
		for _, d := range deltas {
			n.ibltState.RemoveEntry(key, d.field, d.old)
			n.ibltState.InsertEntry(key, d.field, d.tombstone)
		}
	}

	return ts, batch, nil
}
