package replica

import (
	"errors"
	"sync"

	domiblt "github.com/janthoXO/convergeKV/internal/domain/iblt"
)

// ErrNotOwned is returned when an operation targets a partition that this
// replica does not currently serve — either because Ownership never assigned
// it, or because it was dropped after a membership change. The Replica refuses
// to serve unowned partitions; the coordinator's HRW routing is the source of
// truth for where a key lives.
var ErrNotOwned = errors.New("replica: partition not owned")

// partition is the per-partition aggregate: an identity, an RWMutex serialising
// store+IBLT mutations against themselves while letting concurrent reads share,
// and the IBLT projection of the partition's bytes.
//
// Lifecycle: created by Replica.EnsurePartition, removed by Replica.DropPartition.
// DropPartition acquires p.mu to set closed=true, which drains any operation
// already holding the lock and fences any later acquirer: every mutator
// re-checks closed immediately after locking and aborts with ErrNotOwned if
// it's set, rather than mutating a dead epoch's store records or IBLT.
type partition struct {
	id     uint32
	mu     sync.RWMutex
	closed bool // set by DropPartition under mu; mutators must re-check after locking
	iblt   *domiblt.IBLT
}

func newPartition(id uint32, ibltCells int) *partition {
	return &partition{id: id, iblt: domiblt.New(ibltCells)}
}

// snapshot returns a consistent deep copy of the partition's IBLT. IBLT
// itself is not safe for concurrent use, so the copy is taken under p.mu's
// read lock — shared with concurrent reads, exclusive against any in-flight
// Put/Delete/ApplyDelta batch (which hold the write lock).
func (p *partition) snapshot() *domiblt.IBLT {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.iblt.Snapshot()
}
