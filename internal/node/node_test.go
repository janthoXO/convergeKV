package node

import (
	"encoding/json"
	"testing"

	"github.com/janthoXO/convergeKV/internal/crdt"
	"github.com/janthoXO/convergeKV/internal/hlc"
	"github.com/janthoXO/convergeKV/internal/storage"
)

// openNode creates a temporary Node backed by a temp-dir BadgerDB store.
func openNode(t *testing.T, replicaID string) *Node {
	t.Helper()
	store, err := storage.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	n, err := New(replicaID, store)
	if err != nil {
		t.Fatalf("new node: %v", err)
	}
	return n
}

// TestPutThenGet verifies a Put followed by a Get on a single node.
func TestPutThenGet(t *testing.T) {
	n := openNode(t, "r1")
	_, err := n.Put("user:1", `{"name":"Alice","age":30}`)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	v, found := n.Get("user:1")
	if !found {
		t.Fatal("Get: not found after Put")
	}

	var obj map[string]json.RawMessage
	if err := json.Unmarshal([]byte(v), &obj); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if string(obj["name"]) != `"Alice"` {
		t.Errorf("name: got %s, want \"Alice\"", string(obj["name"]))
	}
	if string(obj["age"]) != `30` {
		t.Errorf("age: got %s, want 30", string(obj["age"]))
	}
}

// TestPutOverlappingFields verifies that two Puts with overlapping fields merge correctly.
func TestPutOverlappingFields(t *testing.T) {
	n := openNode(t, "r1")

	_, err := n.Put("user:1", `{"name":"Alice"}`)
	if err != nil {
		t.Fatalf("Put 1: %v", err)
	}
	_, err = n.Put("user:1", `{"age":30}`)
	if err != nil {
		t.Fatalf("Put 2: %v", err)
	}

	v, found := n.Get("user:1")
	if !found {
		t.Fatal("Get: not found")
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal([]byte(v), &obj); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := obj["name"]; !ok {
		t.Error("expected name field")
	}
	if _, ok := obj["age"]; !ok {
		t.Error("expected age field")
	}
}

// TestDeleteThenGet verifies that after Delete, Get returns found=false.
func TestDeleteThenGet(t *testing.T) {
	n := openNode(t, "r1")

	_, err := n.Put("user:1", `{"name":"Alice"}`)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	_, err = n.Delete("user:1")
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_, found := n.Get("user:1")
	if found {
		t.Error("expected found=false after Delete")
	}
}

// TestApplyDeltaHigherTimestampWins verifies ApplyDelta with a higher-timestamp entry overwrites local.
func TestApplyDeltaHigherTimestampWins(t *testing.T) {
	n := openNode(t, "r1")

	_, err := n.Put("user:1", `{"name":"Alice"}`)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Incoming delta from a peer with a timestamp far in the future (year ~2999),
	// ensuring it beats whatever wall-clock time the Put assigned.
	incoming := crdt.FieldEntry{
		Value:     json.RawMessage(`"Bob"`),
		Timestamp: hlc.Timestamp{PhysicalMs: 32503680000000, Logical: 0}, // ~year 2999
		ReplicaID: "r2",
		Deleted:   false,
	}
	changed, err := n.ApplyDelta("user:1", "name", incoming)
	if err != nil {
		t.Fatalf("ApplyDelta: %v", err)
	}
	if !changed {
		t.Error("expected changed=true")
	}

	v, found := n.Get("user:1")
	if !found {
		t.Fatal("Get: not found")
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal([]byte(v), &obj); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if string(obj["name"]) != `"Bob"` {
		t.Errorf("expected name=Bob after delta, got %s", string(obj["name"]))
	}
}

// TestApplyDeltaLowerTimestampIsNoop verifies ApplyDelta with a lower-timestamp entry is a no-op.
func TestApplyDeltaLowerTimestampIsNoop(t *testing.T) {
	n := openNode(t, "r1")

	_, err := n.Put("user:1", `{"name":"Alice"}`)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Incoming delta from a peer with very old timestamp.
	incoming := crdt.FieldEntry{
		Value:     json.RawMessage(`"Charlie"`),
		Timestamp: hlc.Timestamp{PhysicalMs: 1, Logical: 0},
		ReplicaID: "r2",
		Deleted:   false,
	}
	changed, err := n.ApplyDelta("user:1", "name", incoming)
	if err != nil {
		t.Fatalf("ApplyDelta: %v", err)
	}
	if changed {
		t.Error("expected changed=false for lower-timestamp delta")
	}

	v, found := n.Get("user:1")
	if !found {
		t.Fatal("Get: not found")
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal([]byte(v), &obj); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if string(obj["name"]) != `"Alice"` {
		t.Errorf("expected name=Alice (unchanged), got %s", string(obj["name"]))
	}
}
