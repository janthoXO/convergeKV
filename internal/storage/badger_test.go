package storage

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/janthoXO/convergeKV/internal/crdt"
	"github.com/janthoXO/convergeKV/internal/hlc"
)

// TestLoadAllRoundtrip opens a BadgerDB in a temp dir, saves five field entries
// across two keys, then calls LoadAll and verifies all entries are recovered
// with correct field values, timestamps, and replica IDs.
func TestLoadAllRoundtrip(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	entries := []FieldUpdate{
		{Key: "user:1", Field: "name", Entry: crdt.FieldEntry{
			Value:     json.RawMessage(`"Alice"`),
			Timestamp: hlc.Timestamp{PhysicalMs: 100, Logical: 0},
			ReplicaID: "r1",
			Deleted:   false,
		}},
		{Key: "user:1", Field: "age", Entry: crdt.FieldEntry{
			Value:     json.RawMessage(`30`),
			Timestamp: hlc.Timestamp{PhysicalMs: 200, Logical: 1},
			ReplicaID: "r2",
			Deleted:   false,
		}},
		{Key: "user:1", Field: "email", Entry: crdt.FieldEntry{
			Value:     json.RawMessage(`"alice@example.com"`),
			Timestamp: hlc.Timestamp{PhysicalMs: 150, Logical: 0},
			ReplicaID: "r1",
			Deleted:   false,
		}},
		{Key: "order:42", Field: "status", Entry: crdt.FieldEntry{
			Value:     json.RawMessage(`"pending"`),
			Timestamp: hlc.Timestamp{PhysicalMs: 300, Logical: 0},
			ReplicaID: "r3",
			Deleted:   false,
		}},
		{Key: "order:42", Field: "amount", Entry: crdt.FieldEntry{
			Value:     nil,
			Timestamp: hlc.Timestamp{PhysicalMs: 400, Logical: 2},
			ReplicaID: "r1",
			Deleted:   true, // tombstone
		}},
	}

	if err := store.SaveBatch(entries); err != nil {
		t.Fatalf("SaveBatch: %v", err)
	}

	loaded, err := store.LoadAll()
	if err != nil {
		t.Fatalf("LoadAll: %v", err)
	}

	// Verify user:1
	u1, ok := loaded["user:1"]
	if !ok {
		t.Fatal("missing key user:1")
	}
	checkField(t, u1, "name", `"Alice"`, 100, 0, "r1", false)
	checkField(t, u1, "age", `30`, 200, 1, "r2", false)
	checkField(t, u1, "email", `"alice@example.com"`, 150, 0, "r1", false)

	// Verify order:42
	o42, ok := loaded["order:42"]
	if !ok {
		t.Fatal("missing key order:42")
	}
	checkField(t, o42, "status", `"pending"`, 300, 0, "r3", false)
	checkTombstone(t, o42, "amount", 400, 2, "r1")
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

// TestSaveFieldSingle verifies the single-field save path.
func TestSaveFieldSingle(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	entry := crdt.FieldEntry{
		Value:     json.RawMessage(`"Bob"`),
		Timestamp: hlc.Timestamp{PhysicalMs: 999, Logical: 3},
		ReplicaID: "r9",
		Deleted:   false,
	}
	if err := store.SaveField("user:2", "name", entry); err != nil {
		t.Fatalf("SaveField: %v", err)
	}

	loaded, err := store.LoadAll()
	if err != nil {
		t.Fatalf("LoadAll: %v", err)
	}
	u2, ok := loaded["user:2"]
	if !ok {
		t.Fatal("missing key user:2")
	}
	checkField(t, u2, "name", `"Bob"`, 999, 3, "r9", false)
}
