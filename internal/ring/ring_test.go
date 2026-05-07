package ring

import (
	"fmt"
	"math/rand"
	"testing"
)

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
