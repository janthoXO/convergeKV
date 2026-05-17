package node

import (
	"encoding/json"
	"fmt"
	"sync"
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

// TestConcurrentWritesDifferentKeys verifies that writes to distinct keys
// proceed concurrently without data races. The -race detector will flag any
// violation. It also checks that every key is readable after all goroutines
// complete, confirming no write was lost.
func TestConcurrentWritesDifferentKeys(t *testing.T) {
	n := openNode(t, "r1")
	const numKeys = 50

	var wg sync.WaitGroup
	wg.Add(numKeys)
	for i := 0; i < numKeys; i++ {
		i := i
		go func() {
			defer wg.Done()
			key := fmt.Sprintf("key:%d", i)
			val := fmt.Sprintf(`{"n":%d}`, i)
			if _, err := n.Put(key, val); err != nil {
				t.Errorf("Put %s: %v", key, err)
			}
		}()
	}
	wg.Wait()

	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key:%d", i)
		v, found := n.Get(key)
		if !found {
			t.Errorf("key %s not found after concurrent writes", key)
			continue
		}
		var obj map[string]json.RawMessage
		if err := json.Unmarshal([]byte(v), &obj); err != nil {
			t.Errorf("key %s: unmarshal: %v", key, err)
		}
		if string(obj["n"]) != fmt.Sprintf("%d", i) {
			t.Errorf("key %s: n=%s, want %d", key, string(obj["n"]), i)
		}
	}
}

// TestMerkleTreeNonZeroAfterPuts verifies that the Merkle root is non-zero after
// two Put calls for the same key with different fields.
func TestMerkleTreeNonZeroAfterPuts(t *testing.T) {
	n := openNode(t, "r1")
	_, err := n.Put("user:1", `{"name":"Alice"}`)
	if err != nil {
		t.Fatalf("Put name: %v", err)
	}
	_, err = n.Put("user:1", `{"city":"Geneva"}`)
	if err != nil {
		t.Fatalf("Put city: %v", err)
	}

	var zeroHash [32]byte
	root := n.MerkleTree().Root()
	if root == zeroHash {
		t.Error("expected non-zero Merkle root after two Puts, got zero hash")
	}
}

// TestMerkleTreeDifferentRootsForDifferentValues verifies that two nodes with
// different values for the same (key, field) produce different Merkle roots.
func TestMerkleTreeDifferentRootsForDifferentValues(t *testing.T) {
	nA := openNode(t, "r1")
	nB := openNode(t, "r2")

	if _, err := nA.Put("user:1", `{"name":"Alice"}`); err != nil {
		t.Fatalf("nodeA Put: %v", err)
	}
	if _, err := nB.Put("user:1", `{"name":"Bob"}`); err != nil {
		t.Fatalf("nodeB Put: %v", err)
	}

	rootA := nA.MerkleTree().Root()
	rootB := nB.MerkleTree().Root()
	if rootA == rootB {
		t.Error("expected different Merkle roots for nodes with different values, but roots were equal")
	}
}
