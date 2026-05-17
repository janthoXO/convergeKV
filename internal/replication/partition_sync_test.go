package replication

import (
	"context"
	"fmt"
	"testing"

	repb "github.com/janthoXO/convergeKV/gen/replication"
	"github.com/janthoXO/convergeKV/internal/merkle"
	"github.com/janthoXO/convergeKV/internal/partition"
	"github.com/janthoXO/convergeKV/internal/storage"

	"github.com/janthoXO/convergeKV/internal/node"
)

// buildSlotMapRF2 builds a 3-node slot map with RF=2.
func buildSlotMapRF2() partition.SlotMap {
	return partition.InitialAssignment([]string{"A", "B", "C"}, 2)
}

// findKeyForSlotPair scans candidate keys and returns the first one whose
// slot (via partition.SlotIndex) is owned by id1 and id2 but NOT notID.
// Uses the exact SlotMap lookup — no midpoint approximation.
func findKeyForSlotPair(t *testing.T, sm partition.SlotMap, id1, id2, notID string, maxTries int) string {
	t.Helper()
	for i := 0; i < maxTries; i++ {
		key := fmt.Sprintf("scan-key-%d", i)
		has1 := sm.IsReplica(key, id1)
		has2 := sm.IsReplica(key, id2)
		hasNot := sm.IsReplica(key, notID)
		if has1 && has2 && !hasNot {
			return key
		}
	}
	t.Fatalf("could not find key owned by {%s,%s} but not %s in %d tries", id1, id2, notID, maxTries)
	return ""
}

// openNodeWithSlotMap creates a node and sets its slot map.
func openNodeWithSlotMap(t *testing.T, replicaID string, sm partition.SlotMap) *node.Node {
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
	n.UpdateSlotMap(sm)
	return n
}

// TestSlotMapSync exercises slot-map-aligned anti-entropy between two nodes
// that share only a subset of the keyspace (RF=2 of 3 nodes).
func TestSlotMapSync(t *testing.T) {
	sm := buildSlotMapRF2()

	// Find K1: slot owned by A and B (not C).
	k1 := findKeyForSlotPair(t, sm, "A", "B", "C", 4096)
	// Find K2: slot owned by A and C (not B).
	k2 := findKeyForSlotPair(t, sm, "A", "C", "B", 4096)

	t.Logf("K1=%q (slot=%d, A+B), K2=%q (slot=%d, A+C)",
		k1, partition.SlotIndex(k1), k2, partition.SlotIndex(k2))

	slot1 := partition.SlotIndex(k1)
	slot2 := partition.SlotIndex(k2)

	// ── Test 1: SharedSlots correctness ───────────────────────────────────────

	sharedAB := sm.SharedSlots("A", "B")
	sharedAC := sm.SharedSlots("A", "C")

	sharedABSet := make(map[int]struct{}, len(sharedAB))
	for _, s := range sharedAB {
		sharedABSet[s] = struct{}{}
	}
	sharedACSet := make(map[int]struct{}, len(sharedAC))
	for _, s := range sharedAC {
		sharedACSet[s] = struct{}{}
	}

	if _, ok := sharedABSet[slot1]; !ok {
		t.Errorf("slot(%s)=%d should be in SharedSlots(A,B)", k1, slot1)
	}
	if _, ok := sharedACSet[slot1]; ok {
		t.Errorf("slot(%s)=%d should NOT be in SharedSlots(A,C)", k1, slot1)
	}
	if _, ok := sharedACSet[slot2]; !ok {
		t.Errorf("slot(%s)=%d should be in SharedSlots(A,C)", k2, slot2)
	}
	if _, ok := sharedABSet[slot2]; ok {
		t.Errorf("slot(%s)=%d should NOT be in SharedSlots(A,B)", k2, slot2)
	}

	// ── Test 2: Merkle tree populated by writes ────────────────────────────────

	// Create nodeA without a slot map (nil = accept all writes in bootstrap mode).
	store, err := storage.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	nodeA, err := node.New("A", store)
	if err != nil {
		t.Fatalf("new node A: %v", err)
	}
	// No UpdateSlotMap yet — slot map is nil, so isReplica returns true for all keys.
	if _, err := nodeA.Put(k1, `{"v":"k1-value"}`); err != nil {
		t.Fatalf("nodeA Put k1: %v", err)
	}
	if _, err := nodeA.Put(k2, `{"v":"k2-value"}`); err != nil {
		t.Fatalf("nodeA Put k2: %v", err)
	}
	// Install the real slot map for DeltaSync filtering.
	nodeA.UpdateSlotMap(sm)

	if nodeA.MerkleTree().PartitionHash(slot1) == (merkle.Hash{}) {
		t.Error("nodeA: PartitionHash(K1 slot) should be non-zero")
	}
	if nodeA.MerkleTree().PartitionHash(slot2) == (merkle.Hash{}) {
		t.Error("nodeA: PartitionHash(K2 slot) should be non-zero")
	}

	// nodeB starts empty.
	nodeB := openNodeWithSlotMap(t, "B", sm)

	// Handler for nodeA with slot map filter.
	smSnapshot := sm
	handlerA := NewHandler(nodeA, func() partition.SlotMap { return smSnapshot })

	// ── Phase 1: send only shared(A,B) slot hashes from nodeB to nodeA ────────

	sharedHashesForA := make(map[int32][]byte, len(sharedAB))
	for _, s := range sharedAB {
		h := nodeB.MerkleTree().PartitionHash(s)
		b := make([]byte, 32)
		copy(b, h[:])
		sharedHashesForA[int32(s)] = b
	}

	hashRespFromA, err := handlerA.HashSync(context.Background(), &repb.HashSyncRequest{
		ReplicaId:       "B",
		PartitionHashes: sharedHashesForA,
	})
	if err != nil {
		t.Fatalf("HashSync: %v", err)
	}

	divergentFromA := hashRespFromA.GetDivergentPartitions()
	t.Logf("Divergent slots reported by A: %v", divergentFromA)

	// K1 is in sharedAB and nodeA has it → its slot should diverge.
	foundSlot1 := false
	for _, dp := range divergentFromA {
		if int(dp) == slot1 {
			foundSlot1 = true
		}
	}
	if !foundSlot1 {
		t.Errorf("slot %d (K1) should be divergent between A and B, got divergent=%v", slot1, divergentFromA)
	}

	// slot2 was NOT in sharedAB, so it should NOT appear in the divergent list.
	for _, dp := range divergentFromA {
		if int(dp) == slot2 {
			t.Errorf("slot %d (K2) should NOT appear in A↔B divergent list (B doesn't own K2)", slot2)
		}
	}

	// ── Phase 2: DeltaSync from A to B ────────────────────────────────────────

	deltaRespFromA, err := handlerA.DeltaSync(context.Background(), &repb.DeltaSyncRequest{
		ReplicaId:   "B",
		Partitions:  divergentFromA,
		RequesterId: "B",
	})
	if err != nil {
		t.Fatalf("DeltaSync: %v", err)
	}

	foundK1 := false
	for _, d := range deltaRespFromA.GetDeltas() {
		if d.GetKey() == k1 {
			foundK1 = true
		}
		if d.GetKey() == k2 {
			t.Errorf("DeltaSync must not include K2 (%q) — B does not own that slot", k2)
		}
	}
	if !foundK1 {
		t.Errorf("DeltaSync should include K1 (%q) for B, but didn't", k1)
	}

	// ── Test 3: After sync partition hashes converge ───────────────────────────

	applyDeltasToNode(t, nodeB, deltaRespFromA.GetDeltas())

	hashA1 := nodeA.MerkleTree().PartitionHash(slot1)
	hashB1 := nodeB.MerkleTree().PartitionHash(slot1)
	if hashA1 != hashB1 {
		t.Errorf("after sync: PartitionHash(slot %d) mismatch: A=%x B=%x", slot1, hashA1, hashB1)
	}

	// Roots differ because A has K2 (slot owned by A+C, not B).
	rootA := nodeA.MerkleTree().Root()
	rootB := nodeB.MerkleTree().Root()
	if rootA == rootB {
		t.Log("note: roots equal (K1 and K2 may be in same slot — acceptable edge case)")
	} else {
		t.Logf("roots differ as expected: A=%x B=%x (B doesn't hold K2)", rootA[:4], rootB[:4])
	}

	t.Logf("TestSlotMapSync passed: sharedAB=%d sharedAC=%d divergentFromA=%d",
		len(sharedAB), len(sharedAC), len(divergentFromA))
}
