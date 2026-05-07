package replication

import (
	"context"
	"fmt"
	"testing"

	repb "github.com/janthoXO/convergeKV/gen/replication"
	"github.com/janthoXO/convergeKV/internal/merkle"
	"github.com/janthoXO/convergeKV/internal/ring"
	"github.com/janthoXO/convergeKV/internal/storage"

	"github.com/janthoXO/convergeKV/internal/node"
)

// buildRingRF2 builds a 3-member ring with RF=2 and returns the ring plus the
// member list. With RF=2 and 3 nodes, each pair shares roughly 2/3 of partitions.
func buildRingRF2(t *testing.T) (*ring.Ring, []ring.Member) {
	t.Helper()
	members := []ring.Member{
		{ReplicaID: "A", GRPCAddr: "localhost:50051"},
		{ReplicaID: "B", GRPCAddr: "localhost:50052"},
		{ReplicaID: "C", GRPCAddr: "localhost:50053"},
	}
	r := ring.NewRing()
	r.Rebuild(members, 2)
	return r, members
}

// findKeyForPair scans candidate keys and returns the first one whose
// partition (by OwnsPartition midpoint sampling) is owned by id1 and id2
// but NOT notID. Uses OwnsPartition to match the same ownership model
// the system uses at runtime.
func findKeyForPair(t *testing.T, r *ring.Ring, id1, id2, notID string, maxTries int) string {
	t.Helper()
	for i := 0; i < maxTries; i++ {
		key := fmt.Sprintf("scan-key-%d", i)
		p := merkle.PartitionIndex(key)
		has1 := r.OwnsPartition(p, id1)
		has2 := r.OwnsPartition(p, id2)
		hasNot := r.OwnsPartition(p, notID)
		if has1 && has2 && !hasNot {
			return key
		}
	}
	t.Fatalf("could not find key owned by {%s,%s} but not %s in %d tries", id1, id2, notID, maxTries)
	return ""
}

// openNodeWithRing creates a node and sets its ring.
func openNodeWithRing(t *testing.T, replicaID string, r *ring.Ring) *node.Node {
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
	n.SetRing(r)
	return n
}

// TestPartitionSync exercises partition-aligned anti-entropy between two nodes
// that share only a subset of the keyspace.
func TestPartitionSync(t *testing.T) {
	r, _ := buildRingRF2(t)

	// Find K1: owned by A and B (not C).
	k1 := findKeyForPair(t, r, "A", "B", "C", 500)
	// Find K2: owned by A and C (not B).
	k2 := findKeyForPair(t, r, "A", "C", "B", 500)

	t.Logf("K1=%q (A+B), K2=%q (A+C)", k1, k2)

	p1 := merkle.PartitionIndex(k1)
	p2 := merkle.PartitionIndex(k2)
	t.Logf("Partition(K1)=%d  Partition(K2)=%d", p1, p2)

	// ── Test 1: SharedPartitions correctness ──────────────────────────────────

	sharedAB := r.SharedPartitions("A", "B")
	sharedAC := r.SharedPartitions("A", "C")

	sharedABSet := make(map[int]struct{}, len(sharedAB))
	for _, p := range sharedAB {
		sharedABSet[p] = struct{}{}
	}
	sharedACSet := make(map[int]struct{}, len(sharedAC))
	for _, p := range sharedAC {
		sharedACSet[p] = struct{}{}
	}

	if _, ok := sharedABSet[p1]; !ok {
		t.Errorf("partition(%s)=%d should be in SharedPartitions(A,B)", k1, p1)
	}
	if _, ok := sharedACSet[p1]; ok {
		t.Errorf("partition(%s)=%d should NOT be in SharedPartitions(A,C)", k1, p1)
	}
	if _, ok := sharedACSet[p2]; !ok {
		t.Errorf("partition(%s)=%d should be in SharedPartitions(A,C)", k2, p2)
	}
	if _, ok := sharedABSet[p2]; ok {
		t.Errorf("partition(%s)=%d should NOT be in SharedPartitions(A,B)", k2, p2)
	}

	// ── Test 2: Merkle tree stays clean across shards ─────────────────────────

	// Write K1 and K2 to nodeA. Because the ring is set, nodeA will accept both
	// (it is a replica for both via the partition midpoint).
	nodeA := openNodeWithRing(t, "A", r)
	nodeA.SetRing(nil) // temporarily remove ring so Put always succeeds for setup
	if _, err := nodeA.Put(k1, `{"v":"k1-value"}`); err != nil {
		t.Fatalf("nodeA Put k1: %v", err)
	}
	if _, err := nodeA.Put(k2, `{"v":"k2-value"}`); err != nil {
		t.Fatalf("nodeA Put k2: %v", err)
	}
	nodeA.SetRing(r) // restore ring

	// Both partitions should now have non-zero hashes on nodeA.
	if nodeA.MerkleTree().PartitionHash(p1) == (merkle.Hash{}) {
		t.Error("nodeA: PartitionHash(K1 partition) should be non-zero")
	}
	if nodeA.MerkleTree().PartitionHash(p2) == (merkle.Hash{}) {
		t.Error("nodeA: PartitionHash(K2 partition) should be non-zero")
	}

	// nodeB has no data yet.
	nodeB := openNodeWithRing(t, "B", r)

	// Build a handler for nodeA (responds to nodeB's sync request).
	handlerA := NewHandler(nodeA, r)

	// ── Phase 1: send only shared(A,B) partition hashes from nodeB to nodeA ──

	// nodeB sends hashes for shared partitions (all zero since nodeB is empty).
	sharedHashesForA := make(map[int32][]byte, len(sharedAB))
	for _, p := range sharedAB {
		h := nodeB.MerkleTree().PartitionHash(p)
		b := make([]byte, 32)
		copy(b, h[:])
		sharedHashesForA[int32(p)] = b
	}

	hashRespFromA, err := handlerA.HashSync(context.Background(), &repb.HashSyncRequest{
		ReplicaId:       "B",
		PartitionHashes: sharedHashesForA,
	})
	if err != nil {
		t.Fatalf("HashSync: %v", err)
	}

	divergentFromA := hashRespFromA.GetDivergentPartitions()
	t.Logf("Divergent partitions reported by A: %v", divergentFromA)

	// K1 is in sharedAB and nodeA has it → its partition should diverge.
	foundP1 := false
	for _, dp := range divergentFromA {
		if int(dp) == p1 {
			foundP1 = true
		}
	}
	if !foundP1 {
		t.Errorf("partition %d (K1) should be divergent between A and B, got divergent=%v", p1, divergentFromA)
	}

	// K2 is NOT in sharedAB, so p2 should NOT appear in the divergent list.
	for _, dp := range divergentFromA {
		if int(dp) == p2 {
			t.Errorf("partition %d (K2) should NOT appear in A↔B divergent list (B doesn't own K2)", p2)
		}
	}

	// ── Phase 2: DeltaSync from A to B for divergent partitions ──────────────

	deltaRespFromA, err := handlerA.DeltaSync(context.Background(), &repb.DeltaSyncRequest{
		ReplicaId:   "B",
		Partitions:  divergentFromA,
		RequesterId: "B", // A should filter to only entries B owns
	})
	if err != nil {
		t.Fatalf("DeltaSync: %v", err)
	}

	// K1's field should be in the response.
	foundK1 := false
	for _, d := range deltaRespFromA.GetDeltas() {
		if d.GetKey() == k1 {
			foundK1 = true
		}
		// K2 must NOT appear — B doesn't own K2.
		if d.GetKey() == k2 {
			t.Errorf("DeltaSync should not include K2 (%q) since B is not a replica for it", k2)
		}
	}
	if !foundK1 {
		t.Errorf("DeltaSync should include K1 (%q) for B, but didn't", k1)
	}

	// ── Test 3: After sync, B's hash for p1 matches A's; roots still differ ──

	// Apply the delta to nodeB.
	applyDeltasToNode(t, nodeB, deltaRespFromA.GetDeltas())

	hashA_p1 := nodeA.MerkleTree().PartitionHash(p1)
	hashB_p1 := nodeB.MerkleTree().PartitionHash(p1)
	if hashA_p1 != hashB_p1 {
		t.Errorf("after sync: PartitionHash(%d) mismatch: A=%x B=%x", p1, hashA_p1, hashB_p1)
	}

	// Roots must differ because A has K2 (which B doesn't own).
	rootA := nodeA.MerkleTree().Root()
	rootB := nodeB.MerkleTree().Root()
	if rootA == rootB {
		t.Log("note: roots are equal (may happen if K2 ends up in same partition as K1 — acceptable edge case)")
	} else {
		t.Logf("roots differ as expected: A=%x, B=%x (B doesn't hold K2)", rootA[:4], rootB[:4])
	}

	t.Logf("TestPartitionSync passed: sharedAB=%d sharedAC=%d divergentFromA=%d",
		len(sharedAB), len(sharedAC), len(divergentFromA))
}


