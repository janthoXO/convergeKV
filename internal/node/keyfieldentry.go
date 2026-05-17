package node

import "github.com/janthoXO/convergeKV/internal/crdt"

// KeyFieldEntryTuple is a flat (key, field, entry) triple used in replication.
type KeyFieldEntryTuple struct {
	Key   string
	Field string
	Entry crdt.FieldEntry
}
