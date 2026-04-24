package node

import (
    "encoding/json"
    "fmt"

    "github.com/janthoXO/convergeKV/internal/crdt"
    "github.com/janthoXO/convergeKV/internal/hlc"
    "github.com/janthoXO/convergeKV/internal/storage"
)

// Put writes a JSON object value to key. The value must be a JSON object
// (i.e., start with '{'}). Each field is stored independently.
// Returns the HLC timestamp assigned to this write.
func (n *Node) Put(key, valueJSON string) (hlc.Timestamp, error) {
    var obj map[string]json.RawMessage
    if err := json.Unmarshal([]byte(valueJSON), &obj); err != nil {
        return hlc.Timestamp{}, fmt.Errorf("put: value must be a JSON object: %w", err)
    }

    ts := n.hlc.Send()

    n.mu.Lock()
    defer n.mu.Unlock()

    m, ok := n.state[key]
    if !ok {
        m = crdt.NewAWLWWMap()
    }

    var batch []storage.FieldUpdate
    for field, raw := range obj {
        entry := crdt.FieldEntry{
            Value:     raw,
            Timestamp: ts,
            ReplicaID: n.replicaID,
            Deleted:   false,
        }
        crdt.Apply(&m, field, entry)
        batch = append(batch, storage.FieldUpdate{Key: key, Field: field, Entry: entry})
    }

    n.state[key] = m
    if err := n.store.SaveBatch(batch); err != nil {
        return hlc.Timestamp{}, fmt.Errorf("put: storage error: %w", err)
    }
    return ts, nil
}
