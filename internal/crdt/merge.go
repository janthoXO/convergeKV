package crdt

import "encoding/json"

// Merge computes the join of two AWLWWMap values.
//
// Properties guaranteed by this implementation:
//   - Commutative:  Merge(a, b) == Merge(b, a)
//   - Associative:  Merge(Merge(a, b), c) == Merge(a, Merge(b, c))
//   - Idempotent:   Merge(a, a) == a
//
// For each field, the entry with the strictly greater (Timestamp, ReplicaID)
// tuple wins. Both non-overlapping fields are preserved.
// Tombstones compete under the same rule: a tombstone with a higher timestamp
// beats a live entry, and vice versa.
func Merge(a, b AWLWWMap) AWLWWMap {
	result := NewAWLWWMap()

	// Start with a copy of a.
	for field, entry := range a.Fields {
		result.Fields[field] = entry
	}

	// Merge in every field from b.
	for field, bEntry := range b.Fields {
		aEntry, exists := result.Fields[field]
		if !exists || WinsOver(bEntry, aEntry) {
			result.Fields[field] = bEntry
		}
	}
	return result
}

// Apply merges a single FieldEntry into an existing AWLWWMap in-place.
// Use this on the hot write path instead of a full Merge() call.
func Apply(m *AWLWWMap, field string, incoming FieldEntry) {
	if m.Fields == nil {
		m.Fields = make(map[string]FieldEntry)
	}
	existing, exists := m.Fields[field]
	if !exists || WinsOver(incoming, existing) {
		m.Fields[field] = incoming
	}
}

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
