package node

import (
	"encoding/json"
	"fmt"

	"github.com/janthoXO/convergeKV/internal/crdt"
	"github.com/janthoXO/convergeKV/internal/hlc"
	"github.com/janthoXO/convergeKV/internal/storage"
)

// Put writes a JSON object value to key. The value must be a JSON object
// (i.e., start with '{'). Each field is stored independently.
// Returns the HLC timestamp assigned to this write.
//
// Concurrent Puts to different keys proceed without blocking each other.
// Concurrent Puts to the same key are serialised by the per-key lock.
func (n *Node) Put(key, valueJSON string) (hlc.Timestamp, error) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal([]byte(valueJSON), &obj); err != nil {
		return hlc.Timestamp{}, fmt.Errorf("put: value must be a JSON object: %w", err)
	}

	ts := n.hlc.Send()

	// Serialise writes to this key; writes to other keys are unaffected.
	kl := n.getKeyLock(key)
	kl.Lock()
	defer kl.Unlock()

	// Deep copy current state so we can modify it freely.
	awlwwmap := n.snapshotKey(key)

	var batch []storage.FieldUpdate
	for field, raw := range obj {
		entry := crdt.FieldEntry{
			Value:     raw,
			Timestamp: ts,
			ReplicaID: n.replicaID,
			Deleted:   false,
		}

		// Capture old entry before overwriting so we can remove its hash.
		old, hadOld := awlwwmap.Fields[field]
		crdt.Apply(&awlwwmap, field, entry)

		// replace old tree entry with new one
		if hadOld {
			n.removeTree(key, field, old)
		}
		n.updateTree(key, field, entry)
		
		batch = append(batch, storage.FieldUpdate{Key: key, Field: field, Entry: entry})
	}

	// Commit the new map (brief lock) before the Badger write so readers
	// immediately see the updated in-memory state.
	n.commitKey(key, awlwwmap)

	if err := n.store.SaveBatch(batch); err != nil {
		return hlc.Timestamp{}, fmt.Errorf("put: storage error: %w", err)
	}

	return ts, nil
}
