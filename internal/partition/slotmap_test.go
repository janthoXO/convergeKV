package partition_test

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/janthoXO/convergeKV/internal/partition"
)

// TestSlotIndexConsistency verifies SlotIndex == sha256(key)[:8_as_uint64 % NSlots]
// and that it agrees with merkle.PartitionIndex across 10,000 random keys.
// The merkle agreement is checked via a separate test file in the merkle package
// that imports partition — here we just confirm internal consistency.
func TestSlotIndexRange(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 10_000; i++ {
		key := fmt.Sprintf("key-%d", rng.Int63())
		idx := partition.SlotIndex(key)
		if idx < 0 || idx >= partition.NSlots {
			t.Fatalf("SlotIndex(%q) = %d, want [0, %d)", key, idx, partition.NSlots)
		}
	}
}

func TestSharedSlotsSymmetric(t *testing.T) {
	sm := partition.InitialAssignment([]string{"A", "B", "C"}, 2)

	ab := sm.SharedSlots("A", "B")
	ba := sm.SharedSlots("B", "A")

	if len(ab) != len(ba) {
		t.Fatalf("SharedSlots not symmetric: A→B=%d B→A=%d", len(ab), len(ba))
	}
	abSet := make(map[int]struct{}, len(ab))
	for _, s := range ab {
		abSet[s] = struct{}{}
	}
	for _, s := range ba {
		if _, ok := abSet[s]; !ok {
			t.Errorf("slot %d in B→A but not A→B", s)
		}
	}
}

func TestSharedSlots3NodeRF2(t *testing.T) {
	// 3 nodes, RF=2: each node pair shares ~1365 of 4096 slots.
	sm := partition.InitialAssignment([]string{"A", "B", "C"}, 2)

	ab := sm.SharedSlots("A", "B")
	bc := sm.SharedSlots("B", "C")
	ca := sm.SharedSlots("C", "A")

	// Together they should cover all 4096 slots exactly once.
	total := len(ab) + len(bc) + len(ca)
	if total != partition.NSlots {
		t.Errorf("shared slot counts %d+%d+%d = %d, want %d", len(ab), len(bc), len(ca), total, partition.NSlots)
	}
	// Each pair should share exactly NSlots/3 = 1365 or 1366 slots (rounding).
	for _, pair := range [][2]string{{"A", "B"}, {"B", "C"}, {"C", "A"}} {
		n := len(sm.SharedSlots(pair[0], pair[1]))
		if n < 1360 || n > 1370 {
			t.Errorf("SharedSlots(%s,%s) = %d, want ~1365", pair[0], pair[1], n)
		}
	}
}

func TestIsReplicaConsistentWithReplicasForKey(t *testing.T) {
	sm := partition.InitialAssignment([]string{"n1", "n2", "n3", "n4"}, 2)

	rng := rand.New(rand.NewSource(99))
	for i := 0; i < 1_000; i++ {
		key := fmt.Sprintf("k-%d", rng.Int63())
		replicas := sm.ReplicasForKey(key)
		for _, id := range []string{"n1", "n2", "n3", "n4"} {
			isRep := sm.IsReplica(key, id)
			inList := false
			for _, r := range replicas {
				if r == id {
					inList = true
				}
			}
			if isRep != inList {
				t.Errorf("IsReplica(%q, %q) = %v, want %v", key, id, isRep, inList)
			}
		}
	}
}

func TestMergeHigherVersionWins(t *testing.T) {
	low := partition.InitialAssignment([]string{"A", "B"}, 2) // version 1
	high := partition.RebalanceForJoin(low, "C", 2)           // version 2

	if got := low.Merge(high); got.Version != high.Version {
		t.Errorf("low.Merge(high): want version %d, got %d", high.Version, got.Version)
	}
	if got := high.Merge(low); got.Version != high.Version {
		t.Errorf("high.Merge(low): should keep version %d, got %d", high.Version, got.Version)
	}
}

func TestMergeIdenticalVersionIdempotent(t *testing.T) {
	sm := partition.InitialAssignment([]string{"A", "B", "C"}, 2)
	got := sm.Merge(sm)
	// Identical content at same version must not bump the version.
	if got.Version != sm.Version {
		t.Errorf("Merge(self): version bumped from %d to %d, want no change", sm.Version, got.Version)
	}
}

// TestMergeSplitBrain is the key regression test for the concurrent-join scenario.
// Two nodes independently call RebalanceForJoin on the same base map, producing
// two different maps both at version N+1. Merge must resolve them to a single
// map at version N+2 that is identical regardless of call order.
func TestMergeSplitBrain(t *testing.T) {
	base := partition.InitialAssignment([]string{"A", "B", "C"}, 2) // version 1

	// Node X sees D join; node Y sees E join — both produce version 2.
	smD := partition.RebalanceForJoin(base, "D", 2) // version 2, D added
	smE := partition.RebalanceForJoin(base, "E", 2) // version 2, E added

	if smD.Version != smE.Version {
		t.Fatalf("precondition: both maps must be version %d", smD.Version)
	}

	mergedDE := smD.Merge(smE)
	mergedED := smE.Merge(smD)

	// Version must be bumped.
	if mergedDE.Version != smD.Version+1 {
		t.Errorf("merged version: want %d, got %d", smD.Version+1, mergedDE.Version)
	}

	// Merge must be commutative: D.Merge(E) == E.Merge(D).
	if mergedDE.Version != mergedED.Version {
		t.Errorf("commutativity: version mismatch %d vs %d", mergedDE.Version, mergedED.Version)
	}
	for i := range mergedDE.Slots {
		if len(mergedDE.Slots[i]) != len(mergedED.Slots[i]) {
			t.Fatalf("commutativity: slot %d length differs", i)
		}
		for j := range mergedDE.Slots[i] {
			if mergedDE.Slots[i][j] != mergedED.Slots[i][j] {
				t.Fatalf("commutativity: slot %d replica[%d] differs: %q vs %q",
					i, j, mergedDE.Slots[i][j], mergedED.Slots[i][j])
			}
		}
	}

	// After merge, no slot should have fewer than 2 replicas.
	for i, replicas := range mergedDE.Slots {
		if len(replicas) < 2 {
			t.Errorf("slot %d: only %d replicas after split-brain merge", i, len(replicas))
		}
	}

	// Merging the result with either input must not downgrade.
	mergedAgain := mergedDE.Merge(smD)
	if mergedAgain.Version < mergedDE.Version {
		t.Errorf("re-merge downgraded from %d to %d", mergedDE.Version, mergedAgain.Version)
	}
}

// TestMergeIdempotentAfterSplitBrain verifies that once the conflict is resolved,
// further merges of the same pair are idempotent.
func TestMergeIdempotentAfterSplitBrain(t *testing.T) {
	base := partition.InitialAssignment([]string{"A", "B"}, 2)
	v2a := partition.RebalanceForJoin(base, "C", 2)
	v2b := partition.RebalanceForJoin(base, "D", 2)

	resolved := v2a.Merge(v2b) // version 3
	again := resolved.Merge(v2a.Merge(v2b))
	if again.Version != resolved.Version {
		t.Errorf("re-merge of identical resolved maps bumped version: %d → %d", resolved.Version, again.Version)
	}
}

func TestEncodeDecodeRoundtrip(t *testing.T) {
	sm := partition.InitialAssignment([]string{"A", "B", "C"}, 2)
	b, err := sm.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	sm2, err := partition.Decode(b)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if sm2.Version != sm.Version {
		t.Errorf("version mismatch: %d != %d", sm2.Version, sm.Version)
	}
	for i := range sm.Slots {
		if len(sm2.Slots[i]) != len(sm.Slots[i]) {
			t.Errorf("slot %d length mismatch", i)
			break
		}
	}
}
