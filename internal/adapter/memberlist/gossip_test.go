package memberlist_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/janthoXO/convergeKV/internal/adapter/memberlist"
	"github.com/janthoXO/convergeKV/internal/core/ports"
)

func TestMain(m *testing.M) {
	// Ignore goroutines spawned internally by the hashicorp/memberlist library;
	// those are managed by gA/gB.Leave() calls and are not our leaks.
	goleak.VerifyTestMain(m,
		goleak.IgnoreTopFunction("github.com/hashicorp/memberlist.(*Memberlist).triggerMergeCheck"),
		goleak.IgnoreTopFunction("github.com/hashicorp/memberlist.(*Memberlist).streamListen"),
		goleak.IgnoreTopFunction("github.com/hashicorp/memberlist.(*Memberlist).packetListen"),
		goleak.IgnoreTopFunction("github.com/hashicorp/memberlist.(*Memberlist).packetHandler"),
	)
}

func TestTwoNodeGossip(t *testing.T) {
	portA := 17946
	portB := 17947

	ctx := context.Background()

	gA, err := memberlist.Start(ctx, memberlist.Config{
		BindAddr:  "127.0.0.1",
		BindPort:  portA,
		LocalMeta: memberlist.NodeMeta{ReplicaID: "nodeA", GRPCPort: 15051},
	})
	if err != nil {
		t.Fatalf("start A: %v", err)
	}
	defer gA.Leave(2 * time.Second)

	gB, err := memberlist.Start(ctx, memberlist.Config{
		BindAddr:  "127.0.0.1",
		BindPort:  portB,
		LocalMeta: memberlist.NodeMeta{ReplicaID: "nodeB", GRPCPort: 15052},
		Seeds:     []string{fmt.Sprintf("127.0.0.1:%d", portA)},
	})
	if err != nil {
		t.Fatalf("start B: %v", err)
	}
	defer gB.Leave(2 * time.Second)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if hasIDs(gA.Members(), "nodeA", "nodeB") && hasIDs(gB.Members(), "nodeA", "nodeB") {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timeout; A: %v  B: %v", gA.Members(), gB.Members())
}

func hasIDs(members []ports.MemberInfo, ids ...string) bool {
	got := make(map[string]struct{}, len(members))
	for _, m := range members {
		got[m.ReplicaID] = struct{}{}
	}
	for _, id := range ids {
		if _, ok := got[id]; !ok {
			return false
		}
	}
	return true
}
