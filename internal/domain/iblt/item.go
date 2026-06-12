package iblt

import (
	"encoding/binary"

	"github.com/janthoXO/convergeKV/internal/domain/crdt"
)

// SerialiseItem produces the canonical fixed-width binary representation of a
// (key, field, entry) triple used as an IBLT membership item. The encoding is
// independent of partition: partition identity is implicit in which per-
// partition IBLT receives the item.
func SerialiseItem(key, field string, e crdt.FieldEntry) []byte {
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
	}
	return buf
}

// DeserialiseItem parses the canonical serialisation back into its components.
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
