package replica

import (
	"context"
	"fmt"

	"github.com/janthoXO/convergeKV/internal/domain/crdt"
)

// Get returns the current JSON representation of key.
// Returns ("", false, nil) if the key does not exist or all fields are
// tombstones. Returns ErrNotOwned if the partition is not served by this node.
//
// Reads take p.mu.RLock so they observe a consistent view across concurrent
// writers to the same partition. Reads on different partitions never block
// each other.
func (r *Replica) Get(ctx context.Context, partitionId uint32, key string) (string, bool, error) {
	p, err := r.partitionFor(partitionId)
	if err != nil {
		return "", false, err
	}
	p.mu.RLock()
	defer p.mu.RUnlock()

	m, err := r.store.GetKey(ctx, p.id, key)
	if err != nil {
		return "", false, fmt.Errorf("get: %w", err)
	}
	b, ok := crdt.ToJSON(m)
	if !ok {
		return "", false, nil
	}
	return string(b), true, nil
}

// GetField reads a single field entry directly from storage. Returns
// ErrNotOwned if the partition is not served by this node.
func (r *Replica) GetField(ctx context.Context, partitionId uint32, key, field string) (crdt.FieldEntry, bool, error) {
	p, err := r.partitionFor(partitionId)
	if err != nil {
		return crdt.FieldEntry{}, false, err
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	return r.store.GetField(ctx, p.id, key, field)
}

// IteratePartition streams all entries for partitionId to fn. Returns
// ErrNotOwned if the partition is not served by this node.
//
// Holds p.mu.RLock for the duration of the scan. This blocks concurrent writes
// to the partition until the iteration completes; in practice this path is
// only taken during the anti-entropy full-fallback (IBLT decode failure) so
// long-blocking iterations are rare.
func (r *Replica) IteratePartition(ctx context.Context, partitionId uint32, fn func(key, field string, entry crdt.FieldEntry) error) error {
	p, err := r.partitionFor(partitionId)
	if err != nil {
		return err
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	return r.store.IteratePartition(ctx, p.id, fn)
}

// IterateAll streams every persisted entry to fn. This intentionally does NOT
// go through partitionFor: it is used at startup by RecoverHLCFloor before any
// partition exists, and by the debug handler to dump the full store. No
// per-partition lock is taken; Badger's MVCC provides snapshot isolation per
// page.
func (r *Replica) IterateAll(ctx context.Context, fn func(key, field string, entry crdt.FieldEntry) error) error {
	return r.store.IterateAll(ctx, fn)
}
