package storage

import "github.com/janthoXO/convergeKV/internal/crdt"

// FieldUpdate is a helper tuple used in SaveBatch.
type FieldUpdate struct {
	Key   string
	Field string
	Entry crdt.FieldEntry
}
