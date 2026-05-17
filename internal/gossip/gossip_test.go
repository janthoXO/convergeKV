package gossip

import (
	"fmt"
	"testing"
	"time"

	"github.com/janthoXO/convergeKV/internal/partition"
)

func TestTwoNodeGossip(t *testing.T) {
	portA := 17946
	portB := 17947

	metaA := NodeMeta{ReplicaID: "nodeA", GRPCPort: 15051}
	metaB := NodeMeta{ReplicaID: "nodeB", GRPCPort: 15052}

	gA, err := Start(Config{
		BindAddr:  "127.0.0.1",
		BindPort:  portA,
		LocalMeta: metaA,
	})
	if err != nil {
		t.Fatalf("start A: %v", err)
	}
	defer gA.Leave(2 * time.Second)

	gB, err := Start(Config{
		BindAddr:  "127.0.0.1",
		BindPort:  portB,
		LocalMeta: metaB,
		Seeds:     []string{fmt.Sprintf("127.0.0.1:%d", portA)},
	})
	if err != nil {
		t.Fatalf("start B: %v", err)
	}
	defer gB.Leave(2 * time.Second)

	// Wait up to 3 seconds for B to discover A.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		members := gB.Members()
		if len(members) >= 2 {
			// Verify both replica IDs are present.
			ids := make(map[string]struct{}, len(members))
			for _, m := range members {
				ids[m.ReplicaID] = struct{}{}
			}
			if _, ok := ids["nodeA"]; !ok {
				t.Logf("waiting: nodeA not yet visible to B")
			} else if _, ok := ids["nodeB"]; !ok {
				t.Logf("waiting: nodeB not yet visible in its own view")
			} else {
				return // success
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("B.Members() never contained both nodes; final view: %v", gB.Members())
}

// TestSlotMapGossip verifies that a higher-versioned slot map on node A
// propagates to node B when B joins, via the push/pull LocalState/MergeRemoteState
// mechanism. Also verifies that a lower-versioned map does not downgrade B.
func TestSlotMapGossip(t *testing.T) {
	portA := 17948
	portB := 17949

	smLow := partition.InitialAssignment([]string{"nodeA", "nodeB"}, 2) // version 1
	smHigh := partition.RebalanceForJoin(smLow, "nodeC", 2)             // version 2

	// A starts with the higher-versioned map.
	gA, err := Start(Config{
		BindAddr:       "127.0.0.1",
		BindPort:       portA,
		LocalMeta:      NodeMeta{ReplicaID: "nodeA", GRPCPort: 16051},
		InitialSlotMap: smHigh,
	})
	if err != nil {
		t.Fatalf("start A: %v", err)
	}
	defer gA.Leave(2 * time.Second)

	// B starts with the lower-versioned map and will receive A's map at join.
	gB, err := Start(Config{
		BindAddr:       "127.0.0.1",
		BindPort:       portB,
		LocalMeta:      NodeMeta{ReplicaID: "nodeB", GRPCPort: 16052},
		Seeds:          []string{fmt.Sprintf("127.0.0.1:%d", portA)},
		InitialSlotMap: smLow,
	})
	if err != nil {
		t.Fatalf("start B: %v", err)
	}
	defer gB.Leave(2 * time.Second)

	// Wait up to 5s for B to receive the higher version from A's push/pull.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if gB.CurrentSlotMap().Version >= smHigh.Version {
			t.Logf("B adopted slot map version %d ✓", gB.CurrentSlotMap().Version)
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if gB.CurrentSlotMap().Version < smHigh.Version {
		t.Errorf("B slot map version = %d, want >= %d (propagation via push/pull failed)",
			gB.CurrentSlotMap().Version, smHigh.Version)
		return
	}

	// Downgrade resistance: proposing a lower version on B must not apply.
	gB.ProposeSlotMap(smLow)
	time.Sleep(200 * time.Millisecond)
	if gB.CurrentSlotMap().Version < smHigh.Version {
		t.Errorf("B was downgraded to version %d from %d", gB.CurrentSlotMap().Version, smHigh.Version)
	}
}
