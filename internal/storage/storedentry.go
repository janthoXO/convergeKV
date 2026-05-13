package storage

// StoredEntry is the on-disk representation of a single FieldEntry.
type StoredEntry struct {
	ValueJSON  []byte `json:"value"` // raw JSON bytes
	PhysicalMs uint64 `json:"phys_ms"`
	Logical    uint32 `json:"logical"`
	ReplicaID  string `json:"replica_id"`
	Deleted    bool   `json:"deleted"`
}