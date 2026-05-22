package hrw_test

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/janthoXO/convergeKV/internal/gossip"
	"github.com/janthoXO/convergeKV/internal/hrw"
)

func makeMembers(n int) []gossip.MemberInfo {
	members := make([]gossip.MemberInfo, n)
	for i := 0; i < n; i++ {
		members[i] = gossip.MemberInfo{
			ReplicaID:  fmt.Sprintf("replica%d", i),
			GossipAddr: fmt.Sprintf("10.0.0.%d:7946", i),
			GRPCAddr:   fmt.Sprintf("10.0.0.%d:50051", i),
		}
	}
	return members
}

// TestOwners_Deterministic verifies that Owners returns the same result for
// the same (partitionID, members, rf) inputs.
func TestOwners_Deterministic(t *testing.T) {
	members := makeMembers(5)
	const rf = 2
	for partitionId := uint32(0); partitionId < 100; partitionId++ {
		a := hrw.Owners(partitionId, members, rf)
		b := hrw.Owners(partitionId, members, rf)
		if len(a) != len(b) {
			t.Fatalf("partitionId %d: length mismatch %d vs %d", partitionId, len(a), len(b))
		}
		for i := range a {
			if a[i].ReplicaID != b[i].ReplicaID {
				t.Errorf("partitionId %d: position %d: %q vs %q", partitionId, i, a[i].ReplicaID, b[i].ReplicaID)
			}
		}
	}
}

// TestOwners_UniformDistribution verifies that no member is the top scorer
// for more than 15% of 10,000 partition IDs with 10 members.
func TestOwners_UniformDistribution(t *testing.T) {
	members := makeMembers(10)
	const numPartitions = 10_000
	const maxFraction = 0.15

	counts := make(map[string]int, len(members))
	for partitionId := uint32(0); partitionId < numPartitions; partitionId++ {
		top := hrw.Owners(partitionId, members, 1)
		if len(top) > 0 {
			counts[top[0].ReplicaID]++
		}
	}

	for id, count := range counts {
		frac := float64(count) / numPartitions
		if frac > maxFraction {
			t.Errorf("member %s scored top for %.2f%% of partitions (max allowed: %.0f%%)",
				id, frac*100, maxFraction*100)
		}
	}
}

// TestOwners_StabilityAfterRemoveReadd verifies that Owners returns the same set
// after a member is removed and re-added.
func TestOwners_StabilityAfterRemoveReadd(t *testing.T) {
	members := makeMembers(5)
	rf := 2
	const numPartitions = 1000

	original := make(map[uint32]map[string]bool, numPartitions)
	for partitionId := uint32(0); partitionId < numPartitions; partitionId++ {
		reps := hrw.Owners(partitionId, members, rf)
		set := make(map[string]bool, len(reps))
		for _, m := range reps {
			set[m.ReplicaID] = true
		}
		original[partitionId] = set
	}

	removed := members[2]
	membersWithout := append([]gossip.MemberInfo{}, members[:2]...)
	membersWithout = append(membersWithout, members[3:]...)
	membersRestored := append([]gossip.MemberInfo{}, membersWithout...)
	membersRestored = append(membersRestored, removed)

	for partitionId := uint32(0); partitionId < numPartitions; partitionId++ {
		if original[partitionId][removed.ReplicaID] {
			continue
		}
		restored := hrw.Owners(partitionId, membersRestored, rf)
		restoredSet := make(map[string]bool, len(restored))
		for _, m := range restored {
			restoredSet[m.ReplicaID] = true
		}
		for id := range original[partitionId] {
			if !restoredSet[id] {
				t.Errorf("partitionId %d: member %s was in original set but not after restore", partitionId, id)
			}
		}
	}
}

// TestOwners_OrderIndependence verifies Owners returns the same set regardless
// of the order of the members slice.
func TestOwners_OrderIndependence(t *testing.T) {
	members := makeMembers(6)
	rf := 3
	rng := rand.New(rand.NewSource(7))
	for partitionId := uint32(0); partitionId < 200; partitionId++ {
		shuffled := make([]gossip.MemberInfo, len(members))
		copy(shuffled, members)
		rng.Shuffle(len(shuffled), func(a, b int) { shuffled[a], shuffled[b] = shuffled[b], shuffled[a] })

		orig := hrw.Owners(partitionId, members, rf)
		shuf := hrw.Owners(partitionId, shuffled, rf)

		origSet := make(map[string]bool)
		shufSet := make(map[string]bool)
		for _, m := range orig {
			origSet[m.ReplicaID] = true
		}
		for _, m := range shuf {
			shufSet[m.ReplicaID] = true
		}
		for id := range origSet {
			if !shufSet[id] {
				t.Errorf("partitionId %d: order-dependent result; %s in orig but not shuffled", partitionId, id)
			}
		}
	}
}

// TestOwners_RF verifies that the returned slice never exceeds rf and never
// exceeds the number of members.
func TestOwners_RF(t *testing.T) {
	members := makeMembers(4)
	for _, tc := range []struct{ rf, want int }{
		{1, 1}, {2, 2}, {4, 4}, {6, 4}, // 6 > len(members)=4, so capped at 4
	} {
		got := hrw.Owners(0, members, tc.rf)
		if len(got) != tc.want {
			t.Errorf("rf=%d: got %d owners, want %d", tc.rf, len(got), tc.want)
		}
	}
}
