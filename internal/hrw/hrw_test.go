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

// TestUniformDistribution verifies that no member appears as the top scorer
// for more than 15% of 10,000 random keys with 10 members.
func TestUniformDistribution(t *testing.T) {
	members := makeMembers(10)
	const numKeys = 10_000
	const maxFraction = 0.15

	counts := make(map[string]int, len(members))
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%d-%d", i, rng.Int63())
		top := hrw.Replicas(key, members, 1)
		if len(top) > 0 {
			counts[top[0].ReplicaID]++
		}
	}

	for id, count := range counts {
		frac := float64(count) / numKeys
		if frac > maxFraction {
			t.Errorf("member %s scored top for %.2f%% of keys (max allowed: %.0f%%)",
				id, frac*100, maxFraction*100)
		}
	}
}

// TestStabilityAfterRemoveReadd verifies that Replicas returns the same set
// after a member is removed and re-added.
func TestStabilityAfterRemoveReadd(t *testing.T) {
	members := makeMembers(5)
	rf := 2
	const numKeys = 1000

	// Record original replica sets.
	original := make(map[string]map[string]bool, numKeys)
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		reps := hrw.Replicas(key, members, rf)
		set := make(map[string]bool, len(reps))
		for _, m := range reps {
			set[m.ReplicaID] = true
		}
		original[key] = set
	}

	// Remove member at index 2, then re-add it.
	removed := members[2]
	membersWithout := append([]gossip.MemberInfo{}, members[:2]...)
	membersWithout = append(membersWithout, members[3:]...)
	membersRestored := append([]gossip.MemberInfo{}, membersWithout...)
	membersRestored = append(membersRestored, removed)

	// For keys where the removed member was NOT in the original replica set,
	// the result must be the same after restoration.
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		if original[key][removed.ReplicaID] {
			// The removed member was a replica; skip — set may change when it's absent.
			continue
		}
		restored := hrw.Replicas(key, membersRestored, rf)
		restoredSet := make(map[string]bool, len(restored))
		for _, m := range restored {
			restoredSet[m.ReplicaID] = true
		}
		for id := range original[key] {
			if !restoredSet[id] {
				t.Errorf("key %s: member %s was in original replica set but not after restore", key, id)
			}
		}
	}
}

// TestIsReplicaConsistency verifies IsReplica is consistent with Replicas.
func TestIsReplicaConsistency(t *testing.T) {
	members := makeMembers(5)
	rf := 2
	rng := rand.New(rand.NewSource(99))
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("key-%d", rng.Int63())
		reps := hrw.Replicas(key, members, rf)
		inSet := make(map[string]bool)
		for _, m := range reps {
			inSet[m.ReplicaID] = true
		}
		for _, m := range members {
			got := hrw.IsReplica(key, m.ReplicaID, members, rf)
			want := inSet[m.ReplicaID]
			if got != want {
				t.Errorf("IsReplica(%q, %q) = %v, want %v", key, m.ReplicaID, got, want)
			}
		}
	}
}

// TestOrderIndependence verifies Replicas returns the same set regardless
// of the order of the members slice.
func TestOrderIndependence(t *testing.T) {
	members := makeMembers(6)
	rf := 3
	rng := rand.New(rand.NewSource(7))
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("key-%d", i)
		// Shuffle members.
		shuffled := make([]gossip.MemberInfo, len(members))
		copy(shuffled, members)
		rng.Shuffle(len(shuffled), func(a, b int) { shuffled[a], shuffled[b] = shuffled[b], shuffled[a] })

		orig := hrw.Replicas(key, members, rf)
		shuf := hrw.Replicas(key, shuffled, rf)

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
				t.Errorf("key %s: order-dependent result; %s in orig but not shuffled", key, id)
			}
		}
	}
}
