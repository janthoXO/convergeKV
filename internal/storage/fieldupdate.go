package storage

import "github.com/janthoXO/convergeKV/internal/crdt"

// FieldUpdate is a flat (key, field, entry) triple
type FieldUpdate struct {
	Key   string
	Field string
	Entry crdt.FieldEntry
}
