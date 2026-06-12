package crdt

import (
	"encoding/json"

	"github.com/janthoXO/convergeKV/internal/domain/hlc"
)

// FieldEntry is the per-field CRDT value stored for each (key, field) pair.
type FieldEntry struct {
	// Value holds the raw JSON bytes of this field's value (e.g. `"Alice"`, `30`, `true`).
	// It is nil for a tombstone.
	Value     json.RawMessage
	Timestamp hlc.Timestamp
	ReplicaID string // for tie-breaking under LWW
	Deleted   bool // true = this entry is a tombstone (field was deleted)
}

// NewFieldEntry constructs a FieldEntry from its flat storage components,
// so callers don't need to import the hlc package to name hlc.Timestamp.
func NewFieldEntry(value json.RawMessage, physMs uint64, logical uint32, replicaID string, deleted bool) FieldEntry {
	return FieldEntry{
		Value:     value,
		Timestamp: hlc.Timestamp{PhysicalMs: physMs, Logical: logical},
		ReplicaID: replicaID,
		Deleted:   deleted,
	}
}

// FieldUpdate is a flat (partitionID, key, field, entry) record used when
// writing or replicating a field.  It lives in the domain layer so that the
// coordinator and replica do not need to import the storage adapter.
type FieldUpdate struct {
	PartitionID uint32
	Key         string
	Field       string
	Entry       FieldEntry
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
