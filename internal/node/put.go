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
// Returns the HLC timestamp and the exact entries written, so callers can
// push them to peers without a second Badger read.
//
// Concurrent Puts to different keys proceed without blocking each other.
// Concurrent Puts to the same key are serialised by the per-key lock.
func (n *Node) Put(partitionId uint32, key string, valueJSON string) (hlc.Timestamp, []storage.FieldUpdate, error) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal([]byte(valueJSON), &obj); err != nil {
		return hlc.Timestamp{}, nil, fmt.Errorf("put: value must be a JSON object: %w", err)
	}

	// Serialise writes to this key; writes to other keys are unaffected.
	release := n.acquireKey(key)
	defer release()

	ts, err := n.hlc.Send()
	if err != nil {
		return hlc.Timestamp{}, nil, fmt.Errorf("put: %w", err)
	}

	// Read current field values from Badger so we can compute IBLT removals.
	existing, err := n.store.GetKey(partitionId, key)
	if err != nil {
		return hlc.Timestamp{}, nil, fmt.Errorf("put: read error: %w", err)
	}

	var batch []storage.FieldUpdate
	for field, raw := range obj {
		batch = append(batch, storage.FieldUpdate{PartitionID: partitionId, Key: key, Field: field, Entry: crdt.FieldEntry{
			Value:     raw,
			Timestamp: ts,
			ReplicaID: n.replicaID,
			Deleted:   false,
		}})
	}

	// Persist to Badger first; only update the IBLT on success.
	if err := n.store.SaveBatch(batch); err != nil {
		return hlc.Timestamp{}, nil, fmt.Errorf("put: storage error: %w", err)
	}

	for _, u := range batch {
		if old, hadOld := existing.Fields[u.Field]; hadOld {
			n.ibltState.RemoveEntry(partitionId, key, u.Field, old)
		}
		n.ibltState.InsertEntry(partitionId, key, u.Field, u.Entry)
	}

	return ts, batch, nil
}
