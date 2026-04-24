package storage

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/janthoXO/convergeKV/internal/crdt"
	"github.com/janthoXO/convergeKV/internal/hlc"
)

func TestBadgerStore(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Initial data
	entries := []FieldUpdate{
		{Key: "user:1", Field: "name", Entry: crdt.FieldEntry{Value: json.RawMessage(`"Alice"`), Timestamp: hlc.Timestamp{PhysicalMs: 100, Logical: 0}, ReplicaID: "r1"}},
		{Key: "user:1", Field: "age", Entry: crdt.FieldEntry{Value: json.RawMessage(`30`), Timestamp: hlc.Timestamp{PhysicalMs: 105, Logical: 1}, ReplicaID: "r2"}},
		{Key: "user:2", Field: "name", Entry: crdt.FieldEntry{Value: json.RawMessage(`"Bob"`), Timestamp: hlc.Timestamp{PhysicalMs: 200, Logical: 0}, ReplicaID: "r1"}},
		{Key: "user:2", Field: "deleted_field", Entry: crdt.FieldEntry{Deleted: true, Timestamp: hlc.Timestamp{PhysicalMs: 210, Logical: 0}, ReplicaID: "r1"}},
	}

	if err := store.SaveBatch(entries); err != nil {
		t.Fatalf("SaveBatch failed: %v", err)
	}

	// Add another single field to test SaveField
	extra := crdt.FieldEntry{Value: json.RawMessage(`true`), Timestamp: hlc.Timestamp{PhysicalMs: 250, Logical: 0}, ReplicaID: "r3"}
	if err := store.SaveField("user:2", "active", extra); err != nil {
		t.Fatalf("SaveField failed: %v", err)
	}

	// Test reload
	allData, err := store.LoadAll()
	if err != nil {
		t.Fatalf("LoadAll failed: %v", err)
	}

	if len(allData) != 2 {
		t.Errorf("Expected 2 keys, got %d", len(allData))
	}

	user1, ok := allData["user:1"]
	if !ok {
		t.Fatalf("Missing user:1 key")
	}
	if len(user1.Fields) != 2 {
		t.Errorf("Expected 2 fields for user:1, got %d", len(user1.Fields))
	}

	user2, ok := allData["user:2"]
	if !ok {
		t.Fatalf("Missing user:2 key")
	}
	if len(user2.Fields) != 3 {
		t.Errorf("Expected 3 fields for user:2, got %d", len(user2.Fields))
	}

	expectedBobEntry := crdt.FieldEntry{Value: json.RawMessage(`"Bob"`), Timestamp: hlc.Timestamp{PhysicalMs: 200, Logical: 0}, ReplicaID: "r1", Deleted: false}
	if !reflect.DeepEqual(user2.Fields["name"], expectedBobEntry) {
		t.Errorf("user:2 name field mismatch. Expected %v, got %v", expectedBobEntry, user2.Fields["name"])
	}
}
