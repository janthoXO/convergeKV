// Package syncer manages the IBLT-based anti-entropy synchronisation protocol.
// It maintains an incremental IBLT that mirrors the node's in-memory state,
// and runs a background loop that reconciles state with every cluster peer.
package syncer

import (
	"encoding/binary"
	"sync"

	"github.com/janthoXO/convergeKV/internal/crdt"
	"github.com/janthoXO/convergeKV/internal/iblt"
	"github.com/janthoXO/convergeKV/internal/node"
)

// IBLTState wraps a live IBLT and an RWMutex.
// It mirrors the entries in node.Node.state and is kept in sync with every write.
// IBLTState implements node.IBLTUpdater.
type IBLTState struct {
	mu       sync.RWMutex
	t        *iblt.IBLT
	numCells int
}

// NewIBLTState constructs an empty IBLTState.
func NewIBLTState(numCells int) *IBLTState {
	return &IBLTState{t: iblt.New(numCells), numCells: numCells}
}

// serialiseItem produces the canonical fixed-width binary representation of a
// field entry for IBLT membership. The format is:
//
//	key_len     uint32
//	key         []byte
//	field_len   uint32
//	field       []byte
//	physical_ms uint64
//	logical     uint32
//	replicaID_len uint32
//	replicaID   []byte
//	deleted     uint8  (0 or 1)
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
// Returns (key, field, physicalMs, logical, replicaID, deleted, ok).
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

// InsertEntry adds an entry to the IBLT. Implements node.IBLTUpdater.
func (s *IBLTState) InsertEntry(key, field string, e crdt.FieldEntry) {
	item := serialiseItem(key, field, e)
	s.mu.Lock()
	s.t.Insert(item)
	s.mu.Unlock()
}

// RemoveEntry removes an entry from the IBLT. Implements node.IBLTUpdater.
func (s *IBLTState) RemoveEntry(key, field string, e crdt.FieldEntry) {
	item := serialiseItem(key, field, e)
	s.mu.Lock()
	s.t.Delete(item)
	s.mu.Unlock()
}

// Snapshot returns a copy of the current IBLT for transmission.
// The copy is taken under a read lock to prevent data races with concurrent writes.
func (s *IBLTState) Snapshot() *iblt.IBLT {
	s.mu.RLock()
	defer s.mu.RUnlock()
	// Use Subtract with empty to produce a deep clone.
	return s.t.Subtract(iblt.New(s.numCells))
}

// BuildFromSnapshot constructs an IBLTState from a full node snapshot.
// Used on startup after loading all entries from BadgerDB.
func BuildFromSnapshot(records []node.KeyFieldEntryTuple, numCells int) *IBLTState {
	s := NewIBLTState(numCells)
	for _, r := range records {
		s.InsertEntry(r.Key, r.Field, r.Entry)
	}
	return s
}
