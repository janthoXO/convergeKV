package replica_test

import (
	"context"
	"testing"
	"time"

	"github.com/janthoXO/convergeKV/internal/core/replica"
	"github.com/janthoXO/convergeKV/internal/domain/crdt"
	"github.com/janthoXO/convergeKV/internal/domain/hlc"
)

// TestGCTombstonesPurgesExpired verifies that an expired tombstone is removed
// from both the store and the partition's IBLT, leaving the IBLT identical to
// a freshly-built (empty) one.
func TestGCTombstonesPurgesExpired(t *testing.T) {
	r, _, store := newTestReplica(t)
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
		t.Fatalf("expected tombstone before GC, got found=%v entry=%+v err=%v", found, e, err)
	}

	// cutoff strictly greater than the tombstone's timestamp purges it.
	purged, err := r.GCTombstones(ctx, pid, e.Timestamp.PhysicalMs+1)
	if err != nil {
		t.Fatalf("GCTombstones: %v", err)
	}
	if purged != 1 {
		t.Fatalf("expected 1 purged entry, got %d", purged)
	}

	if _, found, err := store.GetField(ctx, pid, key, "name"); err != nil || found {
		t.Fatalf("expected tombstone purged from store, found=%v err=%v", found, err)
	}

	// IBLT lockstep: insert(live) - delete(live) + insert(tombstone) -
	// delete(tombstone) == 0, so every cell's Count/HashSum should have
	// returned to zero (KeySum may retain zeroed-but-non-nil padding from the
	// XOR history, so compare counters rather than raw Encode() bytes).
	snap := r.IBLTSnapshot(pid)
	for i, c := range snap.Cells {
		if c.Count != 0 || c.HashSum != 0 {
			t.Errorf("cell %d not empty after GC purge: count=%d hashSum=%d", i, c.Count, c.HashSum)
		}
	}
}

// TestGCTombstonesCutoffRespected verifies that a tombstone newer than the
// cutoff is left untouched.
func TestGCTombstonesCutoffRespected(t *testing.T) {
	r, _, store := newTestReplica(t)
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
		t.Fatalf("expected tombstone before GC, got found=%v entry=%+v err=%v", found, e, err)
	}

	// cutoff == timestamp does not purge (strict less-than required); neither
	// does a cutoff below the timestamp.
	for _, cutoff := range []uint64{e.Timestamp.PhysicalMs, e.Timestamp.PhysicalMs - 1} {
		purged, err := r.GCTombstones(ctx, pid, cutoff)
		if err != nil {
			t.Fatalf("GCTombstones(cutoff=%d): %v", cutoff, err)
		}
		if purged != 0 {
			t.Fatalf("cutoff=%d: expected 0 purged, got %d", cutoff, purged)
		}
	}

	if _, found, err := store.GetField(ctx, pid, key, "name"); err != nil || !found {
		t.Fatalf("expected tombstone to remain, found=%v err=%v", found, err)
	}
}

// TestGCTombstonesDoesNotPurgeLiveValue verifies that a field which was
// deleted and then written again (now live) is never purged, even with a
// cutoff that would have purged the original tombstone.
func TestGCTombstonesDoesNotPurgeLiveValue(t *testing.T) {
	r, _, store := newTestReplica(t)
	ctx := context.Background()
	const pid = uint32(1)
	const key = "user:1"

	if err := r.EnsurePartition(ctx, pid); err != nil {
		t.Fatalf("EnsurePartition: %v", err)
	}
	if _, _, err := r.Put(ctx, pid, key, `{"name":"Alice"}`); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, batch, err := r.Delete(ctx, pid, key); err != nil {
		t.Fatalf("Delete: %v", err)
	} else if len(batch) != 1 {
		t.Fatalf("expected 1 tombstone, got %d", len(batch))
	}
	if _, _, err := r.Put(ctx, pid, key, `{"name":"Bob"}`); err != nil {
		t.Fatalf("re-Put: %v", err)
	}

	e, found, err := store.GetField(ctx, pid, key, "name")
	if err != nil || !found || e.Deleted {
		t.Fatalf("expected live entry after re-Put, found=%v entry=%+v err=%v", found, e, err)
	}

	// A generous cutoff that would have purged the original tombstone.
	purged, err := r.GCTombstones(ctx, pid, e.Timestamp.PhysicalMs+1000)
	if err != nil {
		t.Fatalf("GCTombstones: %v", err)
	}
	if purged != 0 {
		t.Fatalf("expected 0 purged (live value must survive), got %d", purged)
	}

	if got, found, err := store.GetField(ctx, pid, key, "name"); err != nil || !found || got.Deleted {
		t.Fatalf("live value lost after GC: found=%v entry=%+v err=%v", found, got, err)
	}
}

// TestApplyDeltaDropsExpiredTombstones verifies the apply-boundary filter:
// an incoming tombstone older than the configured grace period is dropped
// without being persisted, while a fresh tombstone is applied normally.
func TestApplyDeltaDropsExpiredTombstones(t *testing.T) {
	clk := &fakeClock{}
	store := newFakeStore()
	r := replica.New("test-node", clk, store, testIBLTCells, replica.WithTombstoneGrace(time.Second))
	t.Cleanup(r.Close)

	ctx := context.Background()
	const pid = uint32(1)

	if err := r.EnsurePartition(ctx, pid); err != nil {
		t.Fatalf("EnsurePartition: %v", err)
	}

	now := uint64(time.Now().UnixMilli())

	// Old tombstone (5s old, grace is 1s): dropped, nothing persisted.
	oldTombstone := crdt.FieldEntry{
		Value:     nil,
		Timestamp: hlc.Timestamp{PhysicalMs: now - 5000},
		ReplicaID: "peer",
		Deleted:   true,
	}
	changed, err := r.ApplyDelta(ctx, pid, "user:old", "name", oldTombstone)
	if err != nil {
		t.Fatalf("ApplyDelta(old tombstone): %v", err)
	}
	if changed {
		t.Error("expected expired tombstone to be dropped (changed=false)")
	}
	if _, found, _ := store.GetField(ctx, pid, "user:old", "name"); found {
		t.Error("expired tombstone must not be persisted")
	}

	// Fresh tombstone (just now): applied normally.
	freshTombstone := crdt.FieldEntry{
		Value:     nil,
		Timestamp: hlc.Timestamp{PhysicalMs: now},
		ReplicaID: "peer",
		Deleted:   true,
	}
	changed, err = r.ApplyDelta(ctx, pid, "user:fresh", "name", freshTombstone)
	if err != nil {
		t.Fatalf("ApplyDelta(fresh tombstone): %v", err)
	}
	if !changed {
		t.Error("expected fresh tombstone to be applied (changed=true)")
	}
	if e, found, _ := store.GetField(ctx, pid, "user:fresh", "name"); !found || !e.Deleted {
		t.Errorf("fresh tombstone not persisted: found=%v entry=%+v", found, e)
	}
}

// TestFakeStoreDeleteBatch verifies the test fake's DeleteBatch removes only
// the targeted (partition, key, field) records.
func TestFakeStoreDeleteBatch(t *testing.T) {
	store := newFakeStore()
	ctx := context.Background()
	const pid = uint32(1)

	entry := crdt.FieldEntry{Timestamp: hlc.Timestamp{PhysicalMs: 1}, ReplicaID: "r"}
	batch := []crdt.FieldUpdate{
		{PartitionID: pid, Key: "k1", Field: "f1", Entry: entry},
		{PartitionID: pid, Key: "k1", Field: "f2", Entry: entry},
		{PartitionID: pid, Key: "k2", Field: "f1", Entry: entry},
	}
	if err := store.SaveBatch(ctx, batch); err != nil {
		t.Fatalf("SaveBatch: %v", err)
	}

	if err := store.DeleteBatch(ctx, []crdt.FieldUpdate{
		{PartitionID: pid, Key: "k1", Field: "f1"},
	}); err != nil {
		t.Fatalf("DeleteBatch: %v", err)
	}

	if _, found, _ := store.GetField(ctx, pid, "k1", "f1"); found {
		t.Error("k1/f1 should have been deleted")
	}
	if _, found, _ := store.GetField(ctx, pid, "k1", "f2"); !found {
		t.Error("k1/f2 should remain")
	}
	if _, found, _ := store.GetField(ctx, pid, "k2", "f1"); !found {
		t.Error("k2/f1 should remain")
	}
}
