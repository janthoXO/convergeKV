package iblt

import (
	"sync"

	"github.com/janthoXO/convergeKV/internal/crdt"
	"github.com/janthoXO/convergeKV/internal/storage"
)

// IBLTState is an in-memory mirror of the node's durable store encoded as a
// collection of per-partition IBLTs. Each (key, field, entry) triple is
// serialised to a fixed-width binary item and tracked in the IBLT for its
// partition so that the anti-entropy syncer can compute set differences
// cheaply without scanning Badger.
//
// Locking discipline (two levels):
//
//	mapMu  — protects the iblts map itself (pointer lookup and lazy creation).
//	         Held for the minimum time needed to find or create the *IBLT entry.
//	         Never held when calling methods on an IBLT.
//	per-IBLT — each *IBLT has its own globalMu / per-cell mutexes (see iblt.go).
//	           Operations on different partitions are fully parallel; operations
//	           on the same partition serialise inside that IBLT's own locks.
type IBLTState struct {
	mapMu    sync.RWMutex
	iblts    map[uint32]*IBLT
	numCells int
}

// NewIBLTState constructs an empty IBLTState.
func NewIBLTState(numCells int) *IBLTState {
	return &IBLTState{
		iblts:    make(map[uint32]*IBLT),
		numCells: numCells,
	}
}

// ibltFor returns the IBLT for partitionId, creating it lazily if not present.
func (s *IBLTState) ibltFor(partitionId uint32) *IBLT {
	s.mapMu.RLock()
	t := s.iblts[partitionId]
	s.mapMu.RUnlock()
	if t != nil {
		return t
	}

	// Double-checked lock: another goroutine may have created it between the
	// RUnlock and Lock below.
	s.mapMu.Lock()
	if t = s.iblts[partitionId]; t == nil {
		t = New(s.numCells)
		s.iblts[partitionId] = t
	}
	s.mapMu.Unlock()
	return t
}

// InsertEntry adds an entry to the IBLT for the given partition.
func (s *IBLTState) InsertEntry(partitionId uint32, key, field string, e crdt.FieldEntry) {
	s.ibltFor(partitionId).Insert(serialiseItem(key, field, e))
}

// RemoveEntry removes an entry from the IBLT for the given partition.
func (s *IBLTState) RemoveEntry(partitionId uint32, key, field string, e crdt.FieldEntry) {
	s.ibltFor(partitionId).Delete(serialiseItem(key, field, e))
}

// Snapshot returns a consistent deep copy of the IBLT for the given partition.
// Returns an empty (all-zero) IBLT if no entries for partitionId exist yet, so callers
// can always call Subtract and Decode without nil-checking.
func (s *IBLTState) Snapshot(partitionId uint32) *IBLT {
	s.mapMu.RLock()
	t := s.iblts[partitionId]
	s.mapMu.RUnlock()
	if t == nil {
		return New(s.numCells)
	}
	return t.Snapshot()
}

// BuildFromStore constructs an IBLTState by streaming entries for each owned
// partition from storage. Only owned partitions are populated; stale on-disk
// bytes for unowned partitions are ignored.
// Called once at startup to initialise the IBLT from persisted data.
func BuildFromStore(store *storage.Store, ownedPartitionIds []uint32, numCells int) (*IBLTState, error) {
	s := NewIBLTState(numCells)
	for _, partitionId := range ownedPartitionIds {
		err := store.IteratePartition(partitionId, func(key, field string, entry crdt.FieldEntry) error {
			s.InsertEntry(partitionId, key, field, entry)
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return s, nil
}
