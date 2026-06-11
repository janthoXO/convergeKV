package replica

import (
	"context"
	"time"

	"github.com/janthoXO/convergeKV/internal/domain/crdt"
	domiblt "github.com/janthoXO/convergeKV/internal/domain/iblt"
)

// gcPageSize bounds how many tombstone identifiers are purged per p.mu
// acquisition, mirroring the store's own paginated scans (iterPageSize).
const gcPageSize = 1000

// isExpiredTombstone reports whether e is a tombstone older than the
// configured grace period. A zero tombstoneGraceMs disables the check.
func (r *Replica) isExpiredTombstone(e crdt.FieldEntry) bool {
	if !e.Deleted || r.tombstoneGraceMs == 0 {
		return false
	}
	now := uint64(time.Now().UnixMilli())
	return now > e.Timestamp.PhysicalMs && now-e.Timestamp.PhysicalMs > r.tombstoneGraceMs
}

// fieldID identifies a single (key, field) record within a partition.
type fieldID struct {
	key   string
	field string
}

// GCTombstones permanently removes tombstones older than cutoffMs from
// partitionId, keeping the store and the partition's IBLT in lockstep.
//
// It returns (0, nil) if the partition is not currently owned (e.g. ownership
// changed mid-pass) — this is not an error, just nothing to do.
//
// The candidate scan runs without holding p.mu (paginated, like the store's
// own scans); each candidate is re-checked under p.mu immediately before
// deletion so a concurrent Put/ApplyDelta that supersedes the tombstone
// between scan and lock is never lost.
func (r *Replica) GCTombstones(ctx context.Context, partitionId uint32, cutoffMs uint64) (int, error) {
	p, err := r.partitionFor(partitionId)
	if err != nil {
		if err == ErrNotOwned {
			return 0, nil
		}
		return 0, err
	}

	var candidates []fieldID
	if err := r.store.IteratePartition(ctx, partitionId, func(key, field string, entry crdt.FieldEntry) error {
		if entry.Deleted && entry.Timestamp.PhysicalMs < cutoffMs {
			candidates = append(candidates, fieldID{key: key, field: field})
		}
		return nil
	}); err != nil {
		return 0, err
	}

	purged := 0
	for start := 0; start < len(candidates); start += gcPageSize {
		if ctx.Err() != nil {
			return purged, ctx.Err()
		}
		end := min(start+gcPageSize, len(candidates))
		n, err := r.gcChunk(ctx, p, candidates[start:end], cutoffMs)
		purged += n
		if err != nil {
			return purged, err
		}
	}
	return purged, nil
}

// gcChunk purges the still-expired-tombstone subset of ids under p.mu.
func (r *Replica) gcChunk(ctx context.Context, p *partition, ids []fieldID, cutoffMs uint64) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// If ownership was dropped and re-gained mid-pass, DropPartition closed
	// this epoch under p.mu before EnsurePartition built a fresh *partition
	// (with its own seed-scanned IBLT). Deleting store records under the
	// closed p here would race that seed scan and could leave the new IBLT
	// holding an item whose store record we just removed. Bail out of the
	// closed epoch entirely.
	if p.closed {
		return 0, nil
	}

	toDelete := make([]crdt.FieldUpdate, 0, len(ids))
	entries := make([]crdt.FieldEntry, 0, len(ids))
	for _, id := range ids {
		cur, found, err := r.store.GetField(ctx, p.id, id.key, id.field)
		if err != nil {
			return 0, err
		}
		// Re-check: a concurrent Put/ApplyDelta may have superseded the
		// tombstone since the unlocked scan. Only purge if it's still an
		// expired tombstone.
		if !found || !cur.Deleted || cur.Timestamp.PhysicalMs >= cutoffMs {
			continue
		}
		toDelete = append(toDelete, crdt.FieldUpdate{PartitionID: p.id, Key: id.key, Field: id.field})
		entries = append(entries, cur)
	}
	if len(toDelete) == 0 {
		return 0, nil
	}

	if err := r.store.DeleteBatch(ctx, toDelete); err != nil {
		return 0, err
	}
	for i, u := range toDelete {
		p.iblt.Delete(domiblt.SerialiseItem(u.Key, u.Field, entries[i]))
	}
	return len(toDelete), nil
}
