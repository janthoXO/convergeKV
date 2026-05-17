package partition_test

import (
	"testing"

	"github.com/janthoXO/convergeKV/internal/partition"
)

func TestInitialAssignmentRFReplicas(t *testing.T) {
	nodes := []string{"A", "B", "C"}
	sm := partition.InitialAssignment(nodes, 2)

	if len(sm.Slots) != partition.NSlots {
		t.Fatalf("want %d slots, got %d", partition.NSlots, len(sm.Slots))
	}
	for i, replicas := range sm.Slots {
		if len(replicas) != 2 {
			t.Errorf("slot %d: want 2 replicas, got %d", i, len(replicas))
		}
	}
}

func TestInitialAssignmentDistribution(t *testing.T) {
	nodes := []string{"A", "B", "C"}
	sm := partition.InitialAssignment(nodes, 2)

	counts := make(map[string]int)
	for _, replicas := range sm.Slots {
		for _, id := range replicas {
			counts[id]++
		}
	}
	// Each node should appear in exactly NSlots*RF/N = 4096*2/3 ≈ 2730–2731 slots.
	expected := partition.NSlots * 2 / 3
	for _, id := range nodes {
		got := counts[id]
		if got < expected-5 || got > expected+5 {
			t.Errorf("node %s: appears in %d slots, want ~%d", id, got, expected)
		}
	}
}

func TestRebalanceForJoinDataSafety(t *testing.T) {
	// No slot should have fewer than RF replicas after join.
	sm := partition.InitialAssignment([]string{"A", "B", "C"}, 2)
	sm2 := partition.RebalanceForJoin(sm, "D", 2)

	if sm2.Version != sm.Version+1 {
		t.Errorf("version: want %d, got %d", sm.Version+1, sm2.Version)
	}
	for i, replicas := range sm2.Slots {
		if len(replicas) < 2 {
			t.Errorf("slot %d: only %d replicas after join (data safety violation)", i, len(replicas))
		}
	}

	// D should appear in approximately NSlots/(3+1) = 1024 slots.
	dCount := 0
	for _, replicas := range sm2.Slots {
		for _, id := range replicas {
			if id == "D" {
				dCount++
			}
		}
	}
	if dCount < 900 || dCount > 1150 {
		t.Errorf("D appears in %d slots after join, want ~1024", dCount)
	}
}

func TestRebalanceForLeaveRemovesNode(t *testing.T) {
	sm := partition.InitialAssignment([]string{"A", "B", "C"}, 2)
	sm2 := partition.RebalanceForLeave(sm, "C", 2)

	if sm2.Version != sm.Version+1 {
		t.Errorf("version: want %d, got %d", sm.Version+1, sm2.Version)
	}
	for i, replicas := range sm2.Slots {
		for _, id := range replicas {
			if id == "C" {
				t.Errorf("slot %d still contains departed node C", i)
			}
		}
	}
}
