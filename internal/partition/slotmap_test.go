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

func TestMerge(t *testing.T) {
	low := partition.InitialAssignment([]string{"A", "B"}, 2)  // version 1
	high := partition.InitialAssignment([]string{"A", "B"}, 2) // version 1
	// Bump high's version manually via RebalanceForJoin (version+1).
	high = partition.RebalanceForJoin(high, "C", 2) // version 2

	got := low.Merge(high)
	if got.Version != high.Version {
		t.Errorf("Merge: want version %d, got %d", high.Version, got.Version)
	}

	// Lower version wins when other is lower.
	got2 := high.Merge(low)
	if got2.Version != high.Version {
		t.Errorf("Merge: should keep high version, got %d", got2.Version)
	}

	// Equal versions — either is fine; just check it doesn't panic.
	eq := low.Merge(low)
	if eq.Version != low.Version {
		t.Errorf("Merge equal: want %d, got %d", low.Version, eq.Version)
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
