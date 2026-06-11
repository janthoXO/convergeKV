package badger_test

import (
	"bytes"
	"encoding/json"
	"testing"

	"go.uber.org/goleak"

	"github.com/janthoXO/convergeKV/internal/adapter/badger"
	"github.com/janthoXO/convergeKV/internal/domain/crdt"
	"github.com/janthoXO/convergeKV/internal/domain/hlc"
	"github.com/janthoXO/convergeKV/internal/domain/keyspace"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

const testNumPartitions = 512

var testEntries = []crdt.FieldUpdate{
	{PartitionID: keyspace.Of("user:1", testNumPartitions), Key: "user:1", Field: "name", Entry: crdt.FieldEntry{
		Value:     json.RawMessage(`"Alice"`),
		Timestamp: hlc.Timestamp{PhysicalMs: 100, Logical: 0},
		ReplicaID: "r1",
		Deleted:   false,
	}},
	{PartitionID: keyspace.Of("user:1", testNumPartitions), Key: "user:1", Field: "age", Entry: crdt.FieldEntry{
		Value:     json.RawMessage(`30`),
		Timestamp: hlc.Timestamp{PhysicalMs: 200, Logical: 1},
		ReplicaID: "r2",
		Deleted:   false,
	}},
	{PartitionID: keyspace.Of("user:1", testNumPartitions), Key: "user:1", Field: "email", Entry: crdt.FieldEntry{
		Value:     json.RawMessage(`"alice@example.com"`),
		Timestamp: hlc.Timestamp{PhysicalMs: 150, Logical: 0},
		ReplicaID: "r1",
		Deleted:   false,
	}},
	{PartitionID: keyspace.Of("order:42", testNumPartitions), Key: "order:42", Field: "status", Entry: crdt.FieldEntry{
		Value:     json.RawMessage(`"pending"`),
		Timestamp: hlc.Timestamp{PhysicalMs: 300, Logical: 0},
		ReplicaID: "r3",
		Deleted:   false,
	}},
	{PartitionID: keyspace.Of("order:42", testNumPartitions), Key: "order:42", Field: "amount", Entry: crdt.FieldEntry{
		Value:     nil,
		Timestamp: hlc.Timestamp{PhysicalMs: 400, Logical: 2},
		ReplicaID: "r1",
		Deleted:   true,
	}},
}

func openTestStore(t *testing.T) *badger.Store {
	t.Helper()
	store, err := badger.Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func TestIterateAllRoundtrip(t *testing.T) {
	ctx := t.Context()
	store := openTestStore(t)
	if err := store.SaveBatch(ctx, testEntries); err != nil {
		t.Fatalf("SaveBatch: %v", err)
	}

	type kf struct{ key, field string }
	seen := make(map[kf]crdt.FieldEntry)
	if err := store.IterateAll(ctx, func(key, field string, entry crdt.FieldEntry) error {
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

func TestIteratePartition(t *testing.T) {
	ctx := t.Context()
	store := openTestStore(t)
	if err := store.SaveBatch(ctx, testEntries); err != nil {
		t.Fatalf("SaveBatch: %v", err)
	}

	pidCounts := make(map[uint32]int)
	for _, u := range testEntries {
		pidCounts[keyspace.Of(u.Key, testNumPartitions)]++
	}

	for pid, want := range pidCounts {
		count := 0
		if err := store.IteratePartition(ctx, pid, func(key, _ string, _ crdt.FieldEntry) error {
			if keyspace.Of(key, testNumPartitions) != pid {
				t.Errorf("IteratePartition(%d) returned key %q from wrong partition", pid, key)
			}
			count++
			return nil
		}); err != nil {
			t.Fatalf("IteratePartition(%d): %v", pid, err)
		}
		if count != want {
			t.Errorf("partition %d: got %d entries, want %d", pid, count, want)
		}
	}
}

func TestGetKey(t *testing.T) {
	ctx := t.Context()
	store := openTestStore(t)
	if err := store.SaveBatch(ctx, testEntries); err != nil {
		t.Fatalf("SaveBatch: %v", err)
	}

	u1, err := store.GetKey(ctx, keyspace.Of("user:1", testNumPartitions), "user:1")
	if err != nil {
		t.Fatalf("GetKey user:1: %v", err)
	}
	checkField(t, u1, "name", `"Alice"`, 100, 0, "r1", false)
	checkField(t, u1, "age", `30`, 200, 1, "r2", false)
	checkField(t, u1, "email", `"alice@example.com"`, 150, 0, "r1", false)

	empty, err := store.GetKey(ctx, keyspace.Of("nonexistent", testNumPartitions), "nonexistent")
	if err != nil {
		t.Fatalf("GetKey nonexistent: %v", err)
	}
	if len(empty.Fields) != 0 {
		t.Errorf("expected empty map for nonexistent key, got %d fields", len(empty.Fields))
	}
}

func TestGetField(t *testing.T) {
	ctx := t.Context()
	store := openTestStore(t)
	if err := store.SaveBatch(ctx, testEntries); err != nil {
		t.Fatalf("SaveBatch: %v", err)
	}

	entry, found, err := store.GetField(ctx, keyspace.Of("user:1", testNumPartitions), "user:1", "name")
	if err != nil {
		t.Fatalf("GetField: %v", err)
	}
	if !found {
		t.Fatal("GetField: not found")
	}
	checkField(t, crdt.AWLWWMap{Fields: map[string]crdt.FieldEntry{"name": entry}}, "name", `"Alice"`, 100, 0, "r1", false)

	_, found, err = store.GetField(ctx, keyspace.Of("user:1", testNumPartitions), "user:1", "nonexistent")
	if err != nil {
		t.Fatalf("GetField missing: %v", err)
	}
	if found {
		t.Error("expected found=false for missing field")
	}
}

func TestDeleteBatchRemovesOnlyTargeted(t *testing.T) {
	ctx := t.Context()
	store := openTestStore(t)
	if err := store.SaveBatch(ctx, testEntries); err != nil {
		t.Fatalf("SaveBatch: %v", err)
	}

	user1 := keyspace.Of("user:1", testNumPartitions)
	order42 := keyspace.Of("order:42", testNumPartitions)

	if err := store.DeleteBatch(ctx, []crdt.FieldUpdate{
		{PartitionID: user1, Key: "user:1", Field: "name"},
	}); err != nil {
		t.Fatalf("DeleteBatch: %v", err)
	}

	if _, found, err := store.GetField(ctx, user1, "user:1", "name"); err != nil || found {
		t.Fatalf("user:1/name should be deleted: found=%v err=%v", found, err)
	}

	// Other fields on the same key, and other keys (including a different
	// partition), must be untouched — exercises that DeleteBatch's key
	// encoding (badgerKey) doesn't over- or under-match.
	if _, found, err := store.GetField(ctx, user1, "user:1", "age"); err != nil || !found {
		t.Fatalf("user:1/age should remain: found=%v err=%v", found, err)
	}
	if _, found, err := store.GetField(ctx, user1, "user:1", "email"); err != nil || !found {
		t.Fatalf("user:1/email should remain: found=%v err=%v", found, err)
	}
	if _, found, err := store.GetField(ctx, order42, "order:42", "status"); err != nil || !found {
		t.Fatalf("order:42/status should remain: found=%v err=%v", found, err)
	}
	if _, found, err := store.GetField(ctx, order42, "order:42", "amount"); err != nil || !found {
		t.Fatalf("order:42/amount should remain: found=%v err=%v", found, err)
	}

	seen := make(map[string]int)
	if err := store.IterateAll(ctx, func(key, field string, _ crdt.FieldEntry) error {
		seen[key+"/"+field]++
		return nil
	}); err != nil {
		t.Fatalf("IterateAll: %v", err)
	}
	if len(seen) != 4 {
		t.Fatalf("expected 4 remaining records, got %d: %v", len(seen), seen)
	}
	if _, ok := seen["user:1/name"]; ok {
		t.Error("user:1/name should not appear in IterateAll")
	}
}

func TestCheckpointRoundtrip(t *testing.T) {
	ctx := t.Context()
	store := openTestStore(t)

	if _, found, err := store.LoadCheckpoint(ctx); err != nil {
		t.Fatalf("LoadCheckpoint: %v", err)
	} else if found {
		t.Fatal("expected found=false for fresh store")
	}

	want := hlc.Timestamp{PhysicalMs: 123456, Logical: 7}
	if err := store.SaveCheckpoint(ctx, want); err != nil {
		t.Fatalf("SaveCheckpoint: %v", err)
	}

	got, found, err := store.LoadCheckpoint(ctx)
	if err != nil {
		t.Fatalf("LoadCheckpoint: %v", err)
	}
	if !found {
		t.Fatal("expected found=true after SaveCheckpoint")
	}
	if got != want {
		t.Errorf("got checkpoint %+v, want %+v", got, want)
	}
}

func TestCheckpointDoesNotAppearInIterateAll(t *testing.T) {
	ctx := t.Context()
	store := openTestStore(t)
	if err := store.SaveBatch(ctx, testEntries); err != nil {
		t.Fatalf("SaveBatch: %v", err)
	}
	if err := store.SaveCheckpoint(ctx, hlc.Timestamp{PhysicalMs: 999, Logical: 1}); err != nil {
		t.Fatalf("SaveCheckpoint: %v", err)
	}

	count := 0
	if err := store.IterateAll(ctx, func(_, _ string, _ crdt.FieldEntry) error {
		count++
		return nil
	}); err != nil {
		t.Fatalf("IterateAll: %v", err)
	}
	if count != len(testEntries) {
		t.Errorf("got %d entries, want %d (checkpoint record leaked into IterateAll)", count, len(testEntries))
	}
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
