package storage

import "github.com/janthoXO/convergeKV/internal/crdt"

// FieldUpdate is a flat (partitionID, key, field, entry) record.
// PartitionID must be set by the caller; the storage layer treats it as opaque.
type FieldUpdate struct {
	PartitionID uint32
	Key         string
	Field       string
	Entry       crdt.FieldEntry
}
