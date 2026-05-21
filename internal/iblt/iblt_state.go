package iblt

import (
	"encoding/binary"

	"github.com/janthoXO/convergeKV/internal/crdt"
	"github.com/janthoXO/convergeKV/internal/storage"
)

// IBLTState is an in-memory mirror of the node's durable store encoded as an
// IBLT. Every persisted (key, field, entry) triple is serialised to a
// fixed-width binary item and tracked in the IBLT so that the anti-entropy
// syncer can compute set differences cheaply.
type IBLTState struct {
	t *IBLT
}

// NewIBLTState constructs an empty IBLTState.
func NewIBLTState(numCells int) *IBLTState {
	return &IBLTState{t: New(numCells)}
}

// serialiseItem produces the canonical fixed-width binary representation of a
// field entry for IBLT membership. The format is:
//
//	key_len       uint32
//	key           []byte
//	field_len     uint32
//	field         []byte
//	physical_ms   uint64
//	logical       uint32
//	replicaID_len uint32
//	replicaID     []byte
//	deleted       uint8  (0 or 1)
func serialiseItem(key, field string, e crdt.FieldEntry) []byte {
	kb := []byte(key)
	fb := []byte(field)
	rb := []byte(e.ReplicaID)

	size := 4 + len(kb) + 4 + len(fb) + 8 + 4 + 4 + len(rb) + 1
	buf := make([]byte, size)
	pos := 0

	binary.BigEndian.PutUint32(buf[pos:], uint32(len(kb)))
	pos += 4
	copy(buf[pos:], kb)
	pos += len(kb)

	binary.BigEndian.PutUint32(buf[pos:], uint32(len(fb)))
	pos += 4
	copy(buf[pos:], fb)
	pos += len(fb)

	binary.BigEndian.PutUint64(buf[pos:], e.Timestamp.PhysicalMs)
	pos += 8

	binary.BigEndian.PutUint32(buf[pos:], e.Timestamp.Logical)
	pos += 4

	binary.BigEndian.PutUint32(buf[pos:], uint32(len(rb)))
	pos += 4
	copy(buf[pos:], rb)
	pos += len(rb)

	if e.Deleted {
		buf[pos] = 1
	} else {
		buf[pos] = 0
	}

	return buf
}

// DeserialiseItem parses the canonical serialisation back into its components.
// Returns (key, field, replicaID, physicalMs, logical, deleted, ok).
func DeserialiseItem(b []byte) (key, field, replicaID string, physMs uint64, logical uint32, deleted bool, ok bool) {
	pos := 0
	readStr := func() (string, bool) {
		if pos+4 > len(b) {
			return "", false
		}
		l := int(binary.BigEndian.Uint32(b[pos:]))
		pos += 4
		if pos+l > len(b) {
			return "", false
		}
		s := string(b[pos : pos+l])
		pos += l
		return s, true
	}

	var valid bool
	key, valid = readStr()
	if !valid {
		return
	}
	field, valid = readStr()
	if !valid {
		return
	}
	if pos+12 > len(b) {
		return
	}
	physMs = binary.BigEndian.Uint64(b[pos:])
	pos += 8
	logical = binary.BigEndian.Uint32(b[pos:])
	pos += 4

	replicaID, valid = readStr()
	if !valid {
		return
	}
	if pos+1 > len(b) {
		return
	}
	deleted = b[pos] == 1
	ok = true
	return
}

// InsertEntry adds an entry to the IBLT.
func (s *IBLTState) InsertEntry(key, field string, e crdt.FieldEntry) {
	s.t.Insert(serialiseItem(key, field, e))
}

// RemoveEntry removes an entry from the IBLT.
func (s *IBLTState) RemoveEntry(key, field string, e crdt.FieldEntry) {
	s.t.Delete(serialiseItem(key, field, e))
}

// Snapshot returns a consistent deep copy of the underlying IBLT.
func (s *IBLTState) Snapshot() *IBLT {
	return s.t.Snapshot()
}

// BuildFromStore constructs an IBLTState by streaming all entries from storage.
// Called once at startup to initialise the IBLT from persisted data.
func BuildFromStore(store *storage.Store, numCells int) (*IBLTState, error) {
	s := NewIBLTState(numCells)
	err := store.IterateAll(func(key, field string, entry crdt.FieldEntry) error {
		s.InsertEntry(key, field, entry)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return s, nil
}
