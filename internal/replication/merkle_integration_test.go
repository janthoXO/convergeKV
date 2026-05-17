package replication

import (
	"context"
	"encoding/json"
	"testing"

	repb "github.com/janthoXO/convergeKV/gen/replication"
	"github.com/janthoXO/convergeKV/internal/crdt"
	"github.com/janthoXO/convergeKV/internal/hlc"
	"github.com/janthoXO/convergeKV/internal/merkle"
	"github.com/janthoXO/convergeKV/internal/node"
	"github.com/janthoXO/convergeKV/internal/storage"
)

// openTestNode creates a Node backed by a temp BadgerDB dir.
func openTestNode(t *testing.T, replicaID string) *node.Node {
	t.Helper()
	store, err := storage.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	n, err := node.New(replicaID, store)
	if err != nil {
		t.Fatalf("new node: %v", err)
	}
	return n
}

// hashesToBytes converts []merkle.Hash to [][]byte for the wire format.
func hashesToBytes(hashes []merkle.Hash) [][]byte {
	out := make([][]byte, len(hashes))
	for i, h := range hashes {
		cp := make([]byte, 32)
		copy(cp, h[:])
		out[i] = cp
	}
	return out
}

// applyDeltasToNode applies a slice of proto DeltaEntry messages to a node.
func applyDeltasToNode(t *testing.T, n *node.Node, deltas []*repb.DeltaEntry) {
	t.Helper()
	for _, d := range deltas {
		ts := hlc.Timestamp{
			PhysicalMs: d.GetTimestamp().GetPhysicalMs(),
			Logical:    d.GetTimestamp().GetLogical(),
		}
		entry := crdt.FieldEntry{
			Value:     d.GetValueJson(),
			Timestamp: ts,
			ReplicaID: d.GetReplicaId(),
			Deleted:   d.GetDeleted(),
		}
		if _, err := n.ApplyDelta(d.GetKey(), d.GetField(), entry); err != nil {
			t.Fatalf("ApplyDelta key=%s field=%s: %v", d.GetKey(), d.GetField(), err)
		}
	}
}

// TestMerkleSync exercises the full two-phase Merkle sync protocol in-process,
// calling handler methods directly without any network.
func TestMerkleSync(t *testing.T) {
	// 1. Create two in-process nodes with unique replica IDs.
	nodeA := openTestNode(t, "replica-A")
	nodeB := openTestNode(t, "replica-B")

	// 2. Put {"name": "Alice"} on nodeA for key "user:1".
	if _, err := nodeA.Put("user:1", `{"name":"Alice"}`); err != nil {
		t.Fatalf("nodeA Put name: %v", err)
	}

	// 3. Put {"city": "Berlin"} on nodeB for key "user:1".
	if _, err := nodeB.Put("user:1", `{"city":"Berlin"}`); err != nil {
		t.Fatalf("nodeB Put city: %v", err)
	}

	// Roots must differ at this point.
	if nodeA.MerkleTree().Root() == nodeB.MerkleTree().Root() {
		t.Fatal("expected different Merkle roots before sync")
	}

	// 4. Build a Handler for nodeB. Call HashSync with nodeA's bucket hashes.
	handlerB := NewHandler(nodeB)

	hashRespB, err := handlerB.HashSync(context.Background(), &repb.HashSyncRequest{
		ReplicaId:    "replica-A",
		BucketHashes: hashesToBytes(nodeA.MerkleTree().AllBucketHashes()),
	})
	if err != nil {
		t.Fatalf("HashSync (A→B): %v", err)
	}

	// Response must list exactly one divergent bucket — the one for "user:1".
	expectedBucket := merkle.BucketIndex("user:1")
	if len(hashRespB.DivergentBuckets) != 1 {
		t.Fatalf("expected 1 divergent bucket from nodeB, got %d: %v",
			len(hashRespB.DivergentBuckets), hashRespB.DivergentBuckets)
	}
	if int(hashRespB.DivergentBuckets[0]) != expectedBucket {
		t.Errorf("divergent bucket: want %d, got %d", expectedBucket, hashRespB.DivergentBuckets[0])
	}

	// 5. Call DeltaSync for that bucket — must contain the city field from nodeB.
	deltaRespB, err := handlerB.DeltaSync(context.Background(), &repb.DeltaSyncRequest{
		ReplicaId: "replica-A",
		Buckets:   hashRespB.DivergentBuckets,
	})
	if err != nil {
		t.Fatalf("DeltaSync (A→B): %v", err)
	}
	var foundCity bool
	for _, d := range deltaRespB.Deltas {
		if d.Field == "city" {
			foundCity = true
		}
	}
	if !foundCity {
		t.Error("DeltaSync from nodeB: expected city field, not found")
	}

	// 6. Apply nodeB's deltas to nodeA.
	applyDeltasToNode(t, nodeA, deltaRespB.Deltas)

	// 7. Repeat in the other direction: nodeA → nodeB for the name field.
	handlerA := NewHandler(nodeA)

	hashRespA, err := handlerA.HashSync(context.Background(), &repb.HashSyncRequest{
		ReplicaId:    "replica-B",
		BucketHashes: hashesToBytes(nodeB.MerkleTree().AllBucketHashes()),
	})
	if err != nil {
		t.Fatalf("HashSync (B→A): %v", err)
	}

	if len(hashRespA.DivergentBuckets) != 1 {
		t.Fatalf("expected 1 divergent bucket from nodeA (reverse), got %d: %v",
			len(hashRespA.DivergentBuckets), hashRespA.DivergentBuckets)
	}

	deltaRespA, err := handlerA.DeltaSync(context.Background(), &repb.DeltaSyncRequest{
		ReplicaId: "replica-B",
		Buckets:   hashRespA.DivergentBuckets,
	})
	if err != nil {
		t.Fatalf("DeltaSync (B→A): %v", err)
	}
	var foundName bool
	for _, d := range deltaRespA.Deltas {
		if d.Field == "name" {
			foundName = true
		}
	}
	if !foundName {
		t.Error("DeltaSync from nodeA: expected name field, not found")
	}

	// Apply nodeA's deltas to nodeB.
	applyDeltasToNode(t, nodeB, deltaRespA.Deltas)

	// 8. Both nodes must now contain {"name":"Alice","city":"Berlin"}.
	for _, n := range []*node.Node{nodeA, nodeB} {
		v, found := n.Get("user:1")
		if !found {
			t.Errorf("node %s: Get user:1 not found after sync", n.ReplicaID())
			continue
		}
		var obj map[string]json.RawMessage
		if err := json.Unmarshal([]byte(v), &obj); err != nil {
			t.Errorf("node %s: unmarshal: %v", n.ReplicaID(), err)
			continue
		}
		if string(obj["name"]) != `"Alice"` {
			t.Errorf("node %s: name=%s, want \"Alice\"", n.ReplicaID(), obj["name"])
		}
		if string(obj["city"]) != `"Berlin"` {
			t.Errorf("node %s: city=%s, want \"Berlin\"", n.ReplicaID(), obj["city"])
		}
	}

	// 9. Merkle roots must be equal after full sync.
	rootA := nodeA.MerkleTree().Root()
	rootB := nodeB.MerkleTree().Root()
	if rootA != rootB {
		t.Errorf("Merkle roots differ after sync: A=%x B=%x", rootA, rootB)
	}
}
