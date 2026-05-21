package crdt

import "encoding/json"

// ToJSON reconstructs the JSON object represented by m.
// Deleted (tombstone) fields are omitted.
// Returns nil if all fields are tombstones or the map is empty.
func ToJSON(m AWLWWMap) ([]byte, bool) {
	obj := make(map[string]json.RawMessage)
	for field, entry := range m.Fields {
		if !entry.Deleted {
			obj[field] = entry.Value
		}
	}
	if len(obj) == 0 {
		return nil, false
	}
	b, _ := json.Marshal(obj)
	return b, true
}
