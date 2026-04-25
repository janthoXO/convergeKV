// Package crdt implements the Add-Wins Last-Write-Wins Map (AWLWWMap) CRDT.
// Each key stores a field-wise AWLWWMap where individual JSON object fields
// compete independently under a Last-Write-Wins rule broken by replica ID.
package crdt

import (
	"encoding/json"

	"github.com/janthoXO/convergeKV/internal/hlc"
)

// FieldEntry is the per-field CRDT value stored for each (key, field) pair.
type FieldEntry struct {
	// Value holds the raw JSON bytes of this field's value (e.g. `"Alice"`, `30`, `true`).
	// It is nil for a tombstone.
	Value     json.RawMessage
	Timestamp hlc.Timestamp
	ReplicaID string
	Deleted   bool // true = this entry is a tombstone (field was deleted)
}

// WinsOver returns true if candidate should replace existing under the LWW rule:
// compare (Timestamp, ReplicaID) lexicographically; ReplicaID breaks ties.
func WinsOver(candidate, existing FieldEntry) bool {
	if hlc.Less(existing.Timestamp, candidate.Timestamp) {
		return true
	}

	if hlc.Equal(existing.Timestamp, candidate.Timestamp) {
		return candidate.ReplicaID > existing.ReplicaID
	}

	return false
}
