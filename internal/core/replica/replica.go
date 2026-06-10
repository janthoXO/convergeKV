// Package replica implements the per-partition aggregate model for ConvergeKV.
// Each owned partition is a self-contained unit (RWMutex + IBLT) held behind a
// single map on the Replica.
//
// Concurrency invariants:
//
//   - r.mu protects pointer lookup on r.partitions only. Never held during I/O.
//   - p.mu (RWMutex) serialises store+IBLT mutation against itself; concurrent
//     reads on the same partition share the RLock. Cross-partition operations
//     are fully parallel.
//   - There are no per-partition goroutines. The Replica owns no goroutines at
//     all; goleak surface on this package is empty.
//   - DropPartition removes the pointer from r.partitions; an in-flight
//     operation that already holds p.mu finishes against the orphan partition.
//     The store accepts the write either way and anti-entropy reconciles.
//   - r.clock has its own internal mutex; safe from any goroutine.
//
// Partition lifecycle: partitions only come into existence via EnsurePartition,
// which is called from Ownership.Update when a membership change makes this
// node a new owner. The Replica deliberately does NOT lazily create partitions
// on first write — an unowned-partition access returns ErrNotOwned so the
// underlying routing bug surfaces at the coordinator instead of being papered
// over.
package replica

import (
	"context"
	"sync"
	"time"

	"github.com/janthoXO/convergeKV/internal/core/ports"
	"github.com/janthoXO/convergeKV/internal/domain/crdt"
	"github.com/janthoXO/convergeKV/internal/domain/hlc"
	domiblt "github.com/janthoXO/convergeKV/internal/domain/iblt"
)

// Replica is the central state holder for a ConvergeKV node.
// It owns the clock, the storage layer, and the per-partition aggregate map.
// All exported methods are safe for concurrent use.
type Replica struct {
	replicaID string
	clock     ports.Clock
	store     ports.Store
	ibltCells int

	mu         sync.RWMutex
	partitions map[uint32]*partition
}

// ReplicaOption configures a Replica at construction time.
type ReplicaOption func(*Replica)

// WithHLCFloor seeds the clock so it will never issue a timestamp below floor.
func WithHLCFloor(floor hlc.Timestamp) ReplicaOption {
	return func(r *Replica) {
		r.clock.Seed(floor)
	}
}

// New constructs a Replica. ibltCells is the per-partition IBLT size; it is a
// cluster-wide constant (see IBLT_CELLS in CLAUDE.md).
func New(replicaID string, clock ports.Clock, store ports.Store, ibltCells int, opts ...ReplicaOption) *Replica {
	r := &Replica{
		replicaID:  replicaID,
		clock:      clock,
		store:      store,
		ibltCells:  ibltCells,
		partitions: make(map[uint32]*partition),
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// ReplicaID returns the node's stable identifier.
func (r *Replica) ReplicaID() string { return r.replicaID }

// HLCNow returns the current clock value without advancing it.
func (r *Replica) HLCNow() hlc.Timestamp { return r.clock.Now() }

// Owns reports whether the local node currently serves the given partition.
// Source of truth for "is this partition mine?" — used by inbound replication
// handlers and by tests.
func (r *Replica) Owns(partitionId uint32) bool {
	r.mu.RLock()
	_, ok := r.partitions[partitionId]
	r.mu.RUnlock()
	return ok
}

// IBLTSnapshot returns a consistent point-in-time copy of the IBLT for
// partitionId, or an empty IBLT if the partition is not owned. Safe to call
// from any goroutine. Used by anti-entropy on the initiator side to compute
// local-vs-remote diffs.
func (r *Replica) IBLTSnapshot(partitionId uint32) *domiblt.IBLT {
	r.mu.RLock()
	p, ok := r.partitions[partitionId]
	r.mu.RUnlock()
	if !ok {
		return domiblt.New(r.ibltCells)
	}
	
	return p.snapshot()
}

// StateOverview returns the opaque wire-format bytes of this partition's state
// overview. Currently an encoded IBLT; callers treat it as opaque.
func (r *Replica) StateOverview(partitionId uint32) []byte {
	return r.IBLTSnapshot(partitionId).Encode()
}

// EnsurePartition constructs a Partition for partitionId if not already
// present, seeding its IBLT from the store. Idempotent. This is the only
// legitimate path to partition creation — called from Ownership.Update.
//
// The seed scan runs *before* the pointer is published into r.partitions so
// readers never see a half-built IBLT. If a concurrent EnsurePartition wins
// the race, this call discards its work.
func (r *Replica) EnsurePartition(ctx context.Context, partitionId uint32) error {
	r.mu.RLock()
	_, exists := r.partitions[partitionId]
	r.mu.RUnlock()
	if exists {
		return nil
	}

	p := newPartition(partitionId, r.ibltCells)
	if err := r.store.IteratePartition(ctx, partitionId, func(key, field string, entry crdt.FieldEntry) error {
		p.iblt.Insert(domiblt.SerialiseItem(key, field, entry))
		return nil
	}); err != nil {
		return err
	}

	r.mu.Lock()
	if _, exists := r.partitions[partitionId]; exists {
		// Lost the race; discard our work, keep the existing partition.
		r.mu.Unlock()
		return nil
	}
	r.partitions[partitionId] = p
	r.mu.Unlock()
	return nil
}

// DropPartition removes the Partition for partitionId from the owned set.
// On-disk data in the store is unaffected. Idempotent.
//
// An in-flight operation holding p.mu finishes against the now-orphaned
// partition pointer — the store gets the write (it doesn't care about
// ownership) and the orphan IBLT update is unobservable. The orphan is GC'd
// once the holder releases the lock.
func (r *Replica) DropPartition(partitionId uint32) {
	r.mu.Lock()
	delete(r.partitions, partitionId)
	r.mu.Unlock()
}

// Close releases all partition references. Safe to call once.
func (r *Replica) Close() {
	r.mu.Lock()
	r.partitions = make(map[uint32]*partition)
	r.mu.Unlock()
}

// partitionFor looks up the *partition for partitionId, returning ErrNotOwned
// if it isn't in the map. No lazy creation; no I/O on the lookup path.
func (r *Replica) partitionFor(partitionId uint32) (*partition, error) {
	r.mu.RLock()
	p := r.partitions[partitionId]
	r.mu.RUnlock()
	if p == nil {
		return nil, ErrNotOwned
	}
	return p, nil
}

// CheckpointMargin is added to a recovered checkpoint's PhysicalMs to bound
// the HLC floor above any timestamp written between the last checkpoint and a
// crash. It must be larger than CheckpointInterval so that even a write made
// just before the process died is covered.
const CheckpointMargin = 2 * CheckpointInterval

// CheckpointInterval is how often the periodic checkpointer
// (see cmd/server/wire.go) persists the current HLC via SaveCheckpoint.
const CheckpointInterval = 30 * time.Second

// RecoverHLCFloor seeds the HLC floor at startup. If a checkpoint exists
// (the normal case after a clean or crashed restart with a non-empty
// DATA_DIR), it is used directly — O(1), no scan — bumped by CheckpointMargin
// to cover writes made after the last checkpoint. Otherwise (a fresh
// DATA_DIR) it falls back to scanning every persisted entry for the highest
// timestamp.
func RecoverHLCFloor(ctx context.Context, store ports.Store) (hlc.Timestamp, error) {
	if ckpt, ok, err := store.LoadCheckpoint(ctx); err != nil {
		return hlc.Timestamp{}, err
	} else if ok {
		return hlc.Timestamp{PhysicalMs: ckpt.PhysicalMs + uint64(CheckpointMargin.Milliseconds())}, nil
	}

	var floor hlc.Timestamp
	err := store.IterateAll(ctx, func(_, _ string, e crdt.FieldEntry) error {
		if hlc.Less(floor, e.Timestamp) {
			floor = e.Timestamp
		}
		return nil
	})
	return floor, err
}
