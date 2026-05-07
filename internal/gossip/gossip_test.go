package gossip

import (
	"fmt"
	"testing"
	"time"
)

// findFreePort finds two available gossip ports to use in tests.
func gossipPort(base int) int { return base }

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
