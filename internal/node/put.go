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
func (n *Node) Put(key, valueJSON string) (hlc.Timestamp, []storage.FieldUpdate, error) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal([]byte(valueJSON), &obj); err != nil {
		return hlc.Timestamp{}, nil, fmt.Errorf("put: value must be a JSON object: %w", err)
	}

	ts := n.hlc.Send()

	// Serialise writes to this key; writes to other keys are unaffected.
	release := n.acquireKey(key)
	defer release()

	// Read current field values from Badger so we can compute IBLT removals.
	existing, err := n.store.GetKey(key)
	if err != nil {
		return hlc.Timestamp{}, nil, fmt.Errorf("put: read error: %w", err)
	}

	type ibltDelta struct {
		field    string
		newEntry crdt.FieldEntry
		oldEntry crdt.FieldEntry
		hadOld   bool
	}
	var deltas []ibltDelta
	var batch []storage.FieldUpdate

	for field, raw := range obj {
		entry := crdt.FieldEntry{
			Value:     raw,
			Timestamp: ts,
			ReplicaID: n.replicaID,
			Deleted:   false,
		}
		old, hadOld := existing.Fields[field]
		deltas = append(deltas, ibltDelta{field, entry, old, hadOld})
		batch = append(batch, storage.FieldUpdate{Key: key, Field: field, Entry: entry})
	}

	// Persist to Badger first; only update the IBLT on success.
	if err := n.store.SaveBatch(batch); err != nil {
		return hlc.Timestamp{}, nil, fmt.Errorf("put: storage error: %w", err)
	}

	if n.ibltState != nil {
		for _, d := range deltas {
			if d.hadOld {
				n.ibltState.RemoveEntry(key, d.field, d.oldEntry)
			}
			n.ibltState.InsertEntry(key, d.field, d.newEntry)
		}
	}

	return ts, batch, nil
}
