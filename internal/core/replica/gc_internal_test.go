package replica

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/janthoXO/convergeKV/internal/adapter/badger"
	"github.com/janthoXO/convergeKV/internal/domain/crdt"
	"github.com/janthoXO/convergeKV/internal/domain/hlc"
	domiblt "github.com/janthoXO/convergeKV/internal/domain/iblt"
)

// TestGCChunkBailsOnStalePartition is a white-box test for the closed-epoch
// guard in gcChunk: if ownership of a partition is dropped and re-gained
// between GCTombstones' unlocked scan and gcChunk's lock acquisition,
// DropPartition has marked the *partition captured by the scan closed before
// EnsurePartition built a fresh one with its own seed-scanned IBLT. gcChunk
// must detect the closed flag and do nothing, rather than mutate the
// store/IBLT of a dead epoch.
func TestGCChunkBailsOnStalePartition(t *testing.T) {
	store, err := badger.Open(t.TempDir())
	if err != nil {
		t.Fatalf("badger.Open: %v", err)
	}
	defer store.Close()

	r := New("test-node", hlc.New(), store, 64)
	defer r.Close()

	ctx := context.Background()
	const pid = uint32(1)
	const key = "user:1"

	if err := r.EnsurePartition(ctx, pid); err != nil {
		t.Fatalf("EnsurePartition: %v", err)
	}
	if _, _, err := r.Put(ctx, pid, key, `{"name":"Alice"}`); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, _, err := r.Delete(ctx, pid, key); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	e, found, err := store.GetField(ctx, pid, key, "name")
	if err != nil || !found || !e.Deleted {
		t.Fatalf("expected tombstone before GC, found=%v err=%v", found, err)
	}

	// Capture the "stale" partition pointer, as GCTombstones would after its
	// unlocked scan.
	stale, err := r.partitionFor(pid)
	if err != nil {
		t.Fatalf("partitionFor: %v", err)
	}

	// Simulate ownership churn between the scan and the lock: drop and
	// re-ensure the partition, yielding a new *partition with a freshly
	// seed-scanned IBLT (which now includes the tombstone, since it's still
	// on disk).
	r.DropPartition(pid)
	if err := r.EnsurePartition(ctx, pid); err != nil {
		t.Fatalf("re-EnsurePartition: %v", err)
	}
	fresh, err := r.partitionFor(pid)
	if err != nil {
		t.Fatalf("partitionFor (fresh): %v", err)
	}
	if fresh == stale {
		t.Fatal("expected EnsurePartition to build a new *partition")
	}
	if !stale.closed {
		t.Fatal("expected DropPartition to mark the stale epoch closed")
	}

	// gcChunk, still holding the stale pointer, must bail without touching
	// the store or the fresh IBLT.
	purged, err := r.gcChunk(ctx, stale, []fieldID{{key: key, field: "name"}}, e.Timestamp.PhysicalMs+1)
	if err != nil {
		t.Fatalf("gcChunk: %v", err)
	}
	if purged != 0 {
		t.Fatalf("expected gcChunk to bail on stale partition, purged=%d", purged)
	}

	if _, found, err := store.GetField(ctx, pid, key, "name"); err != nil || !found {
		t.Fatalf("tombstone should remain in store, found=%v err=%v", found, err)
	}

	// The fresh IBLT (seeded from disk, which still has the tombstone) must
	// be untouched by the bailed-out gcChunk.
	freshSnap := r.IBLTSnapshot(pid)
	wantSnap := newPartition(pid, 64)
	if err := store.IteratePartition(ctx, pid, func(key, field string, entry crdt.FieldEntry) error {
		wantSnap.iblt.Insert(domiblt.SerialiseItem(key, field, entry))
		return nil
	}); err != nil {
		t.Fatalf("IteratePartition: %v", err)
	}
	for i := range freshSnap.Cells {
		if freshSnap.Cells[i].Count != wantSnap.iblt.Cells[i].Count || freshSnap.Cells[i].HashSum != wantSnap.iblt.Cells[i].HashSum {
			t.Fatalf("cell %d diverged: got count=%d hashSum=%d, want count=%d hashSum=%d",
				i, freshSnap.Cells[i].Count, freshSnap.Cells[i].HashSum, wantSnap.iblt.Cells[i].Count, wantSnap.iblt.Cells[i].HashSum)
		}
	}

	// A direct write via the stale epoch must also be rejected, not just GC.
	stale.mu.Lock()
	_, _, err = r.doPut(ctx, stale, "user:2", map[string]json.RawMessage{"name": json.RawMessage(`"Bob"`)})
	stale.mu.Unlock()
	if !errors.Is(err, ErrNotOwned) {
		t.Fatalf("doPut on closed epoch: expected ErrNotOwned, got %v", err)
	}
}
