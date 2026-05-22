package storage

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/janthoXO/convergeKV/internal/crdt"
	"github.com/janthoXO/convergeKV/internal/hlc"
	"github.com/janthoXO/convergeKV/internal/partition"
)

var testEntries = []FieldUpdate{
	{PartitionID: partition.Of("user:1", testNumPartitions), Key: "user:1", Field: "name", Entry: crdt.FieldEntry{
		Value:     json.RawMessage(`"Alice"`),
		Timestamp: hlc.Timestamp{PhysicalMs: 100, Logical: 0},
		ReplicaID: "r1",
		Deleted:   false,
	}},
	{PartitionID: partition.Of("user:1", testNumPartitions), Key: "user:1", Field: "age", Entry: crdt.FieldEntry{
		Value:     json.RawMessage(`30`),
		Timestamp: hlc.Timestamp{PhysicalMs: 200, Logical: 1},
		ReplicaID: "r2",
		Deleted:   false,
	}},
	{PartitionID: partition.Of("user:1", testNumPartitions), Key: "user:1", Field: "email", Entry: crdt.FieldEntry{
		Value:     json.RawMessage(`"alice@example.com"`),
		Timestamp: hlc.Timestamp{PhysicalMs: 150, Logical: 0},
		ReplicaID: "r1",
		Deleted:   false,
	}},
	{PartitionID: partition.Of("order:42", testNumPartitions), Key: "order:42", Field: "status", Entry: crdt.FieldEntry{
		Value:     json.RawMessage(`"pending"`),
		Timestamp: hlc.Timestamp{PhysicalMs: 300, Logical: 0},
		ReplicaID: "r3",
		Deleted:   false,
	}},
	{PartitionID: partition.Of("order:42", testNumPartitions), Key: "order:42", Field: "amount", Entry: crdt.FieldEntry{
		Value:     nil,
		Timestamp: hlc.Timestamp{PhysicalMs: 400, Logical: 2},
		ReplicaID: "r1",
		Deleted:   true,
	}},
}

const testNumPartitions = 512

func openTestStore(t *testing.T) *Store {
	t.Helper()
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

// TestIterateAllRoundtrip saves five entries then verifies IterateAll recovers all of them.
func TestIterateAllRoundtrip(t *testing.T) {
	store := openTestStore(t)
	if err := store.SaveBatch(testEntries); err != nil {
		t.Fatalf("SaveBatch: %v", err)
	}

	type kf struct{ key, field string }
	seen := make(map[kf]crdt.FieldEntry)
	if err := store.IterateAll(func(key, field string, entry crdt.FieldEntry) error {
		seen[kf{key, field}] = entry
		return nil
	}); err != nil {
		t.Fatalf("IterateAll: %v", err)
	}

	if len(seen) != 5 {
		t.Fatalf("expected 5 entries, got %d", len(seen))
	}
	e := seen[kf{"user:1", "name"}]
	checkField(t, crdt.AWLWWMap{Fields: map[string]crdt.FieldEntry{"name": e}}, "name", `"Alice"`, 100, 0, "r1", false)
}

// TestIteratePartition verifies that IteratePartition returns only the entries
// for the requested partition and stops at the partition boundary.
func TestIteratePartition(t *testing.T) {
	store := openTestStore(t)
	if err := store.SaveBatch(testEntries); err != nil {
		t.Fatalf("SaveBatch: %v", err)
	}

	// Compute expected count per partition using the same partition function the
	// store uses (both call partition.Of with testNumPartitions).
	partitionIdCounts := make(map[uint32]int)
	for _, u := range testEntries {
		partitionId := partition.Of(u.Key, testNumPartitions)
		partitionIdCounts[partitionId]++
	}

	// Verify each occupied partition returns exactly those entries.
	for partitionId, want := range partitionIdCounts {
		count := 0
		if err := store.IteratePartition(partitionId, func(key, _ string, _ crdt.FieldEntry) error {
			if keyPartitionId := partition.Of(key, testNumPartitions); keyPartitionId != partitionId {
				t.Errorf("IteratePartition(%d) returned key %q from partition %d", partitionId, key, keyPartitionId)
			}
			count++
			return nil
		}); err != nil {
			t.Fatalf("IteratePartition(%d): %v", partitionId, err)
		}
		if count != want {
			t.Errorf("partition %d: got %d entries, want %d", partitionId, count, want)
		}
	}

	// Verify an empty partition returns nothing.
	for partitionId := uint32(0); partitionId < testNumPartitions; partitionId++ {
		if partitionIdCounts[partitionId] != 0 {
			continue
		}
		got := 0
		if err := store.IteratePartition(partitionId, func(_, _ string, _ crdt.FieldEntry) error {
			got++
			return nil
		}); err != nil {
			t.Fatalf("IteratePartition empty partitionId=%d: %v", partitionId, err)
		}
		if got != 0 {
			t.Errorf("empty partition %d returned %d entries", partitionId, got)
		}
		break // checking one empty partition is enough
	}
}

// TestGetKey reads a single key's AWLWWMap from Badger.
func TestGetKey(t *testing.T) {
	store := openTestStore(t)
	if err := store.SaveBatch(testEntries); err != nil {
		t.Fatalf("SaveBatch: %v", err)
	}

	u1, err := store.GetKey(partition.Of("user:1", testNumPartitions), "user:1")
	if err != nil {
		t.Fatalf("GetKey user:1: %v", err)
	}
	checkField(t, u1, "name", `"Alice"`, 100, 0, "r1", false)
	checkField(t, u1, "age", `30`, 200, 1, "r2", false)
	checkField(t, u1, "email", `"alice@example.com"`, 150, 0, "r1", false)

	o42, err := store.GetKey(partition.Of("order:42", testNumPartitions), "order:42")
	if err != nil {
		t.Fatalf("GetKey order:42: %v", err)
	}
	checkField(t, o42, "status", `"pending"`, 300, 0, "r3", false)
	checkTombstone(t, o42, "amount", 400, 2, "r1")

	// Missing key returns empty map, not an error.
	empty, err := store.GetKey(partition.Of("nonexistent", testNumPartitions), "nonexistent")
	if err != nil {
		t.Fatalf("GetKey nonexistent: %v", err)
	}
	if len(empty.Fields) != 0 {
		t.Errorf("expected empty map for nonexistent key, got %d fields", len(empty.Fields))
	}
}

// TestGetField reads individual (key, field) records.
func TestGetField(t *testing.T) {
	store := openTestStore(t)
	if err := store.SaveBatch(testEntries); err != nil {
		t.Fatalf("SaveBatch: %v", err)
	}

	entry, found, err := store.GetField(partition.Of("user:1", testNumPartitions), "user:1", "name")
	if err != nil {
		t.Fatalf("GetField: %v", err)
	}
	if !found {
		t.Fatal("GetField: not found")
	}
	checkField(t, crdt.AWLWWMap{Fields: map[string]crdt.FieldEntry{"name": entry}}, "name", `"Alice"`, 100, 0, "r1", false)

	// Tombstone is also retrievable.
	tombEntry, found, err := store.GetField(partition.Of("order:42", testNumPartitions), "order:42", "amount")
	if err != nil {
		t.Fatalf("GetField tombstone: %v", err)
	}
	if !found {
		t.Fatal("GetField tombstone: not found")
	}
	checkTombstone(t, crdt.AWLWWMap{Fields: map[string]crdt.FieldEntry{"amount": tombEntry}}, "amount", 400, 2, "r1")

	// Missing returns (zero, false, nil).
	_, found, err = store.GetField(partition.Of("user:1", testNumPartitions), "user:1", "nonexistent")
	if err != nil {
		t.Fatalf("GetField missing: %v", err)
	}
	if found {
		t.Error("expected found=false for missing field")
	}
}

// TestSaveBatchSingleField verifies a one-entry SaveBatch write is readable via GetField.
func TestSaveBatchSingleField(t *testing.T) {
	store := openTestStore(t)

	entry := crdt.FieldEntry{
		Value:     json.RawMessage(`"Bob"`),
		Timestamp: hlc.Timestamp{PhysicalMs: 999, Logical: 3},
		ReplicaID: "r9",
		Deleted:   false,
	}
	if err := store.SaveBatch([]FieldUpdate{{PartitionID: partition.Of("user:2", testNumPartitions), Key: "user:2", Field: "name", Entry: entry}}); err != nil {
		t.Fatalf("SaveBatch: %v", err)
	}

	got, found, err := store.GetField(partition.Of("user:2", testNumPartitions), "user:2", "name")
	if err != nil {
		t.Fatalf("GetField: %v", err)
	}
	if !found {
		t.Fatal("missing key user:2/name after SaveBatch")
	}
	checkField(t, crdt.AWLWWMap{Fields: map[string]crdt.FieldEntry{"name": got}}, "name", `"Bob"`, 999, 3, "r9", false)
}

func checkField(t *testing.T, m crdt.AWLWWMap, field, wantValue string, wantPhys uint64, wantLogical uint32, wantReplica string, wantDeleted bool) {
	t.Helper()
	e, ok := m.Fields[field]
	if !ok {
		t.Errorf("missing field %q", field)
		return
	}
	if !bytes.Equal(e.Value, json.RawMessage(wantValue)) {
		t.Errorf("field %q: value=%s, want %s", field, string(e.Value), wantValue)
	}
	if e.Timestamp.PhysicalMs != wantPhys || e.Timestamp.Logical != wantLogical {
		t.Errorf("field %q: timestamp=%+v, want {%d,%d}", field, e.Timestamp, wantPhys, wantLogical)
	}
	if e.ReplicaID != wantReplica {
		t.Errorf("field %q: replicaID=%s, want %s", field, e.ReplicaID, wantReplica)
	}
	if e.Deleted != wantDeleted {
		t.Errorf("field %q: deleted=%v, want %v", field, e.Deleted, wantDeleted)
	}
}

func checkTombstone(t *testing.T, m crdt.AWLWWMap, field string, wantPhys uint64, wantLogical uint32, wantReplica string) {
	t.Helper()
	e, ok := m.Fields[field]
	if !ok {
		t.Errorf("missing field %q", field)
		return
	}
	if !e.Deleted {
		t.Errorf("field %q: expected tombstone", field)
	}
	if e.Timestamp.PhysicalMs != wantPhys || e.Timestamp.Logical != wantLogical {
		t.Errorf("field %q: timestamp=%+v, want {%d,%d}", field, e.Timestamp, wantPhys, wantLogical)
	}
	if e.ReplicaID != wantReplica {
		t.Errorf("field %q: replicaID=%s, want %s", field, e.ReplicaID, wantReplica)
	}
}
