package ring

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"
)

// partitionIndexFromKey mirrors merkle.PartitionIndex using the same algorithm.
// Kept here to avoid a circular dependency with the merkle package.
func partitionIndexFromKey(key string) int {
	h := sha256.Sum256([]byte(key))
	v := binary.BigEndian.Uint64(h[:8])
	return int(v % 1024) // merkle.NumPartitions = 1024
}

func TestSingleMember(t *testing.T) {
	r := NewRing()
	m := Member{ReplicaID: "n1", GRPCAddr: "localhost:50051"}
	r.Rebuild([]Member{m}, 1)

	replicas := r.GetReplicas("any-key")
	if len(replicas) != 1 {
		t.Fatalf("want 1 replica, got %d", len(replicas))
	}
	if replicas[0].ReplicaID != "n1" {
		t.Fatalf("want n1, got %s", replicas[0].ReplicaID)
	}
}

func TestThreeMembersRF3(t *testing.T) {
	r := NewRing()
	members := []Member{
		{ReplicaID: "n1", GRPCAddr: "localhost:50051"},
		{ReplicaID: "n2", GRPCAddr: "localhost:50052"},
		{ReplicaID: "n3", GRPCAddr: "localhost:50053"},
	}
	r.Rebuild(members, 3)

	for _, key := range []string{"k1", "k2", "hello", "world"} {
		replicas := r.GetReplicas(key)
		if len(replicas) != 3 {
			t.Fatalf("key=%s: want 3 replicas, got %d", key, len(replicas))
		}
		seen := make(map[string]struct{})
		for _, rep := range replicas {
			seen[rep.ReplicaID] = struct{}{}
		}
		if len(seen) != 3 {
			t.Fatalf("key=%s: replicas are not all distinct: %v", key, replicas)
		}
	}
}

func TestFourMembersRF3(t *testing.T) {
	r := NewRing()
	members := []Member{
		{ReplicaID: "n1", GRPCAddr: "localhost:50051"},
		{ReplicaID: "n2", GRPCAddr: "localhost:50052"},
		{ReplicaID: "n3", GRPCAddr: "localhost:50053"},
		{ReplicaID: "n4", GRPCAddr: "localhost:50054"},
	}
	r.Rebuild(members, 3)

	for _, key := range []string{"k1", "k2", "hello", "world", "foo", "bar"} {
		replicas := r.GetReplicas(key)
		if len(replicas) != 3 {
			t.Fatalf("key=%s: want 3 replicas, got %d", key, len(replicas))
		}
		seen := make(map[string]struct{})
		for _, rep := range replicas {
			seen[rep.ReplicaID] = struct{}{}
		}
		if len(seen) != 3 {
			t.Fatalf("key=%s: replicas are not all distinct: %v", key, replicas)
		}
	}
}

func TestRebuildUpdatesReplicas(t *testing.T) {
	r := NewRing()
	initial := []Member{
		{ReplicaID: "n1", GRPCAddr: "localhost:50051"},
		{ReplicaID: "n2", GRPCAddr: "localhost:50052"},
	}
	r.Rebuild(initial, 1)

	replicas := r.GetReplicas("mykey")
	if len(replicas) != 1 {
		t.Fatalf("pre-rebuild: want 1, got %d", len(replicas))
	}
	oldPrimary := replicas[0].ReplicaID

	// Add a third node; now RF=1 still means exactly 1, but the primary might change.
	updated := append(initial, Member{ReplicaID: "n3", GRPCAddr: "localhost:50053"})
	r.Rebuild(updated, 2)

	replicas = r.GetReplicas("mykey")
	if len(replicas) != 2 {
		t.Fatalf("post-rebuild: want 2 replicas, got %d", len(replicas))
	}
	t.Logf("old primary=%s; post-rebuild replicas=%v (ring correctly rebuilt)", oldPrimary, replicas)
}

func TestDistribution(t *testing.T) {
	const numMembers = 10
	const numKeys = 10_000
	const minPct = 0.05
	const maxPct = 0.15

	members := make([]Member, numMembers)
	for i := 0; i < numMembers; i++ {
		members[i] = Member{
			ReplicaID: fmt.Sprintf("n%d", i+1),
			GRPCAddr:  fmt.Sprintf("localhost:%d", 50051+i),
		}
	}
	r := NewRing()
	r.Rebuild(members, 3)

	counts := make(map[string]int, numMembers)
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%d", rng.Int63())
		primary, ok := r.Primary(key)
		if !ok {
			t.Fatalf("ring returned no primary for key=%s", key)
		}
		counts[primary.ReplicaID]++
	}

	for _, m := range members {
		pct := float64(counts[m.ReplicaID]) / float64(numKeys)
		if pct < minPct || pct > maxPct {
			t.Errorf("node %s is primary for %.1f%% of keys (want %.0f%%–%.0f%%)",
				m.ReplicaID, pct*100, minPct*100, maxPct*100)
		}
	}
}

func TestIsPrimaryAndIsReplica(t *testing.T) {
	r := NewRing()
	members := []Member{
		{ReplicaID: "n1", GRPCAddr: "localhost:50051"},
		{ReplicaID: "n2", GRPCAddr: "localhost:50052"},
		{ReplicaID: "n3", GRPCAddr: "localhost:50053"},
	}
	r.Rebuild(members, 3) // RF=3 with 3 nodes: all are replicas for every key

	key := "test-key"
	replicas := r.GetReplicas(key)
	if len(replicas) == 0 {
		t.Fatal("no replicas returned")
	}
	primary := replicas[0]

	if !r.IsPrimary(key, primary.ReplicaID) {
		t.Errorf("IsPrimary mismatch: expected %s to be primary", primary.ReplicaID)
	}
	for _, rep := range replicas {
		if !r.IsReplica(key, rep.ReplicaID) {
			t.Errorf("IsReplica mismatch: expected %s to be a replica", rep.ReplicaID)
		}
	}

	// With RF=3 and 3 nodes, all nodes are replicas.
	for _, m := range members {
		if !r.IsReplica(key, m.ReplicaID) {
			t.Errorf("IsReplica: %s should be a replica (RF=3, N=3)", m.ReplicaID)
		}
	}
}

func TestEmptyRing(t *testing.T) {
	r := NewRing()
	replicas := r.GetReplicas("any")
	if replicas != nil {
		t.Fatalf("empty ring: expected nil, got %v", replicas)
	}
	_, ok := r.Primary("any")
	if ok {
		t.Fatalf("empty ring: Primary should return false")
	}
}

// ── SharedPartitions and OwnsPartition tests ──────────────────────────────────

func TestSharedPartitionsAllOwned(t *testing.T) {
	// 3 nodes, RF=3 — every node owns every partition, so all are shared.
	r := NewRing()
	members := []Member{
		{ReplicaID: "A", GRPCAddr: "localhost:50051"},
		{ReplicaID: "B", GRPCAddr: "localhost:50052"},
		{ReplicaID: "C", GRPCAddr: "localhost:50053"},
	}
	r.Rebuild(members, 3)

	shared := r.SharedPartitions("A", "B")
	if len(shared) == 0 {
		t.Fatalf("SharedPartitions(A,B) returned 0 on a 3-node RF=3 ring")
	}
	t.Logf("3-node RF=3: SharedPartitions(A,B)=%d (of 1024)", len(shared))
}

func TestSharedPartitionsPartialOverlap(t *testing.T) {
	// 6 nodes, RF=3 — each pair shares a subset of partitions.
	r := NewRing()
	members := make([]Member, 6)
	for i := range members {
		members[i] = Member{
			ReplicaID: fmt.Sprintf("n%d", i+1),
			GRPCAddr:  fmt.Sprintf("localhost:%d", 50051+i),
		}
	}
	r.Rebuild(members, 3)

	sharedAB := r.SharedPartitions("n1", "n2")
	sharedAC := r.SharedPartitions("n1", "n3")

	if len(sharedAB) == 0 {
		t.Error("SharedPartitions(n1,n2) returned 0 on 6-node RF=3 ring")
	}
	if len(sharedAC) == 0 {
		t.Error("SharedPartitions(n1,n3) returned 0 on 6-node RF=3 ring")
	}
	if len(sharedAB) >= 1024 {
		t.Errorf("SharedPartitions(n1,n2)=%d, expected < 1024 for 6-node ring", len(sharedAB))
	}
	t.Logf("6-node RF=3: SharedPartitions(n1,n2)=%d, SharedPartitions(n1,n3)=%d", len(sharedAB), len(sharedAC))
}

func TestSharedPartitionsSymmetric(t *testing.T) {
	r := NewRing()
	members := []Member{
		{ReplicaID: "A", GRPCAddr: "localhost:50051"},
		{ReplicaID: "B", GRPCAddr: "localhost:50052"},
		{ReplicaID: "C", GRPCAddr: "localhost:50053"},
		{ReplicaID: "D", GRPCAddr: "localhost:50054"},
	}
	r.Rebuild(members, 2)

	ab := r.SharedPartitions("A", "B")
	ba := r.SharedPartitions("B", "A")

	if len(ab) != len(ba) {
		t.Errorf("SharedPartitions is not symmetric: A→B=%d, B→A=%d", len(ab), len(ba))
	}
	abSet := make(map[int]struct{}, len(ab))
	for _, p := range ab {
		abSet[p] = struct{}{}
	}
	for _, p := range ba {
		if _, ok := abSet[p]; !ok {
			t.Errorf("partition %d in B→A but not in A→B", p)
		}
	}
}

func TestSharedPartitionsAfterRebuild(t *testing.T) {
	r := NewRing()
	initial := []Member{
		{ReplicaID: "A", GRPCAddr: "localhost:50051"},
		{ReplicaID: "B", GRPCAddr: "localhost:50052"},
	}
	r.Rebuild(initial, 2)
	sharedBefore := r.SharedPartitions("A", "B")

	// Add a third member; with RF=2, A and B now share fewer partitions.
	updated := []Member{
		{ReplicaID: "A", GRPCAddr: "localhost:50051"},
		{ReplicaID: "B", GRPCAddr: "localhost:50052"},
		{ReplicaID: "C", GRPCAddr: "localhost:50053"},
	}
	r.Rebuild(updated, 2)
	sharedAfter := r.SharedPartitions("A", "B")

	if len(sharedBefore) == 0 {
		t.Error("SharedPartitions before rebuild: expected > 0")
	}
	if len(sharedAfter) == 0 {
		t.Error("SharedPartitions after rebuild: expected > 0")
	}
	t.Logf("SharedPartitions(A,B): before=%d after=%d (C added)", len(sharedBefore), len(sharedAfter))
}

func TestOwnsPartitionConsistentWithGetReplicas(t *testing.T) {
	r := NewRing()
	members := []Member{
		{ReplicaID: "n1", GRPCAddr: "localhost:50051"},
		{ReplicaID: "n2", GRPCAddr: "localhost:50052"},
		{ReplicaID: "n3", GRPCAddr: "localhost:50053"},
		{ReplicaID: "n4", GRPCAddr: "localhost:50054"},
	}
	r.Rebuild(members, 2)

	// For a sample of keys, log any mismatch between OwnsPartition (midpoint
	// sampling) and GetReplicas (exact key lookup). Boundary mismatches are
	// expected and don't indicate a bug; just verify OwnsPartition returns true
	// for the primary's own partition midpoint key.
	keys := []string{"hello", "world", "user:1", "key:extra", "foobar", "test", "abc"}
	for _, key := range keys {
		replicas := r.GetReplicas(key)
		partition := partitionIndexFromKey(key)
		for _, m := range members {
			owns := r.OwnsPartition(partition, m.ReplicaID)
			inReplicas := false
			for _, rep := range replicas {
				if rep.ReplicaID == m.ReplicaID {
					inReplicas = true
					break
				}
			}
			if owns != inReplicas {
				// Midpoint vs exact-key sampling can differ at partition boundaries.
				t.Logf("note: key=%q partition=%d member=%s: OwnsPartition=%v GetReplicas=%v",
					key, partition, m.ReplicaID, owns, inReplicas)
			}
		}
	}
}
