package node

import (
	"encoding/json"
	"testing"

	"github.com/janthoXO/convergeKV/internal/crdt"
	"github.com/janthoXO/convergeKV/internal/hlc"
	"github.com/janthoXO/convergeKV/internal/storage"
)

func setupTestNode(t *testing.T, replicaID string) *Node {
	t.Helper()
	store, err := storage.Open(t.TempDir())
	if err != nil {
		t.Fatalf("Failed to open storage: %v", err)
	}
	n, err := New(replicaID, store)
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}
	return n
}

func TestNodePutGet(t *testing.T) {
	n := setupTestNode(t, "n1")

	// Put value
	ts, err := n.Put("k1", `{"a": 1}`)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if ts.PhysicalMs == 0 {
		t.Fatalf("Put returned zero timestamp")
	}

	// Get value
	v, found := n.Get("k1")
	if !found {
		t.Fatalf("Get found=false for k1")
	}

	// Verify json
	var obj map[string]int
	if err := json.Unmarshal([]byte(v), &obj); err != nil {
		t.Fatalf("Failed to unmarshal obtained value: %v", err)
	}
	if obj["a"] != 1 {
		t.Fatalf("Expected a=1, got %v", obj["a"])
	}
}

func TestNodeMergeTwoPuts(t *testing.T) {
	n := setupTestNode(t, "n1")
	
	n.Put("k1", `{"a": 1}`)
	n.Put("k1", `{"b": 2}`)

	v, found := n.Get("k1")
	if !found {
		t.Fatalf("Get found=false for k1")
	}

	var obj map[string]int
	if err := json.Unmarshal([]byte(v), &obj); err != nil {
		t.Fatalf("Failed to unmarshal obtained value: %v", err)
	}
	if obj["a"] != 1 || obj["b"] != 2 {
		t.Fatalf("Expected a=1, b=2, got %v", obj)
	}
}

func TestNodeDelete(t *testing.T) {
	n := setupTestNode(t, "n1")

	n.Put("k1", `{"a": 1}`)
	n.Delete("k1")

	_, found := n.Get("k1")
	if found {
		t.Fatalf("Get found=true for deleted k1")
	}
}

func TestNodeApplyDelta(t *testing.T) {
	n := setupTestNode(t, "n1")
	
	// Apply an incoming delta
	incoming := crdt.FieldEntry{
		Value:     json.RawMessage(`"peer_val"`),
		Timestamp: hlc.Timestamp{PhysicalMs: 1000, Logical: 0},
		ReplicaID: "peer",
	}

	applied, err := n.ApplyDelta("k1", "field1", incoming)
	if err != nil {
		t.Fatalf("ApplyDelta failed: %v", err)
	}
	if !applied {
		t.Fatalf("Expected delta to be applied")
	}

	v, found := n.Get("k1")
	if !found {
		t.Fatalf("Expected k1 to be found")
	}

	var obj map[string]string
	if err := json.Unmarshal([]byte(v), &obj); err != nil {
		t.Fatalf("Failed to unmarshal obtained value: %v", err)
	}

	if obj["field1"] != "peer_val" {
		t.Fatalf("Expected field1=peer_val, got %s", obj["field1"])
	}

	// Try to apply a delta with lower timestamp (should not apply)
	incomingLower := crdt.FieldEntry{
		Value:     json.RawMessage(`"late_val"`),
		Timestamp: hlc.Timestamp{PhysicalMs: 500, Logical: 0},
		ReplicaID: "peer2",
	}

	appliedLower, err := n.ApplyDelta("k1", "field1", incomingLower)
	if err != nil {
		t.Fatalf("ApplyDelta lower failed: %v", err)
	}
	if appliedLower {
		t.Fatalf("Expected lower delta NOT to be applied")
	}

	// Read again, value should be unchanged
	v, _ = n.Get("k1")
	json.Unmarshal([]byte(v), &obj)
	if obj["field1"] != "peer_val" {
		t.Fatalf("Value got overwritten by lower timestamp: %v", obj["field1"])
	}
}
