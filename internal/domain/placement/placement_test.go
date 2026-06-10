package placement_test

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/janthoXO/convergeKV/internal/domain/placement"
)

type fakeMember struct{ id, addr string }

func (f fakeMember) ID() string   { return f.id }
func (f fakeMember) Addr() string { return f.addr }

func makeMembers(n int) []fakeMember {
	members := make([]fakeMember, n)
	for i := range n {
		members[i] = fakeMember{
			id:   fmt.Sprintf("replica%d", i),
			addr: fmt.Sprintf("10.0.0.%d:50051", i),
		}
	}
	return members
}

func TestOwners_Deterministic(t *testing.T) {
	members := makeMembers(5)
	const rf = 2
	for partitionId := uint32(0); partitionId < 100; partitionId++ {
		a := placement.Owners(partitionId, members, rf)
		b := placement.Owners(partitionId, members, rf)
		if len(a) != len(b) {
			t.Fatalf("partitionId %d: length mismatch %d vs %d", partitionId, len(a), len(b))
		}
		for i := range a {
			if a[i].ID() != b[i].ID() {
				t.Errorf("partitionId %d: position %d: %q vs %q", partitionId, i, a[i].ID(), b[i].ID())
			}
		}
	}
}

func TestOwners_UniformDistribution(t *testing.T) {
	members := makeMembers(10)
	const numPartitions = 10_000
	const maxFraction = 0.15

	counts := make(map[string]int, len(members))
	for partitionId := uint32(0); partitionId < numPartitions; partitionId++ {
		top := placement.Owners(partitionId, members, 1)
		if len(top) > 0 {
			counts[top[0].ID()]++
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

func TestOwners_StabilityAfterRemoveReadd(t *testing.T) {
	members := makeMembers(5)
	rf := 2
	const numPartitions = 1000

	original := make(map[uint32]map[string]bool, numPartitions)
	for partitionId := uint32(0); partitionId < numPartitions; partitionId++ {
		reps := placement.Owners(partitionId, members, rf)
		set := make(map[string]bool, len(reps))
		for _, m := range reps {
			set[m.ID()] = true
		}
		original[partitionId] = set
	}

	removed := members[2]
	membersWithout := append([]fakeMember{}, members[:2]...)
	membersWithout = append(membersWithout, members[3:]...)
	membersRestored := append([]fakeMember{}, membersWithout...)
	membersRestored = append(membersRestored, removed)

	for partitionId := uint32(0); partitionId < numPartitions; partitionId++ {
		if original[partitionId][removed.ID()] {
			continue
		}
		restored := placement.Owners(partitionId, membersRestored, rf)
		restoredSet := make(map[string]bool, len(restored))
		for _, m := range restored {
			restoredSet[m.ID()] = true
		}
		for id := range original[partitionId] {
			if !restoredSet[id] {
				t.Errorf("partitionId %d: member %s was in original set but not after restore", partitionId, id)
			}
		}
	}
}

func TestOwners_OrderIndependence(t *testing.T) {
	members := makeMembers(6)
	rf := 3
	rng := rand.New(rand.NewSource(7))
	for partitionId := uint32(0); partitionId < 200; partitionId++ {
		shuffled := make([]fakeMember, len(members))
		copy(shuffled, members)
		rng.Shuffle(len(shuffled), func(a, b int) { shuffled[a], shuffled[b] = shuffled[b], shuffled[a] })

		orig := placement.Owners(partitionId, members, rf)
		shuf := placement.Owners(partitionId, shuffled, rf)

		origSet := make(map[string]bool)
		shufSet := make(map[string]bool)
		for _, m := range orig {
			origSet[m.ID()] = true
		}
		for _, m := range shuf {
			shufSet[m.ID()] = true
		}
		for id := range origSet {
			if !shufSet[id] {
				t.Errorf("partitionId %d: order-dependent result; %s in orig but not shuffled", partitionId, id)
			}
		}
	}
}

func TestOwners_RF(t *testing.T) {
	members := makeMembers(4)
	for _, tc := range []struct{ rf, want int }{
		{1, 1}, {2, 2}, {4, 4}, {6, 4},
	} {
		got := placement.Owners(0, members, tc.rf)
		if len(got) != tc.want {
			t.Errorf("rf=%d: got %d owners, want %d", tc.rf, len(got), tc.want)
		}
	}
}
