package ownership_test

import (
	"context"
	"slices"
	"testing"

	"go.uber.org/goleak"

	"github.com/janthoXO/convergeKV/internal/cluster/ownership"
	"github.com/janthoXO/convergeKV/internal/core/ports"
	"github.com/janthoXO/convergeKV/internal/domain/placement"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

const (
	testLocalID       = "node-a"
	testRF            = 2
	testNumPartitions = 16
)

func members(ids ...string) []ports.MemberInfo {
	out := make([]ports.MemberInfo, len(ids))
	for i, id := range ids {
		out[i] = ports.MemberInfo{ReplicaID: id, GRPCAddr: id + ":50051"}
	}
	return out
}

// expectedOwned computes which partitions a node should own via the same HRW
// logic as the Ownership cache.
func expectedOwned(localID string, ms []ports.MemberInfo, rf, numP int) []uint32 {
	var owned []uint32
	for pid := uint32(0); pid < uint32(numP); pid++ {
		for _, m := range placement.Owners(pid, ms, rf) {
			if m.ReplicaID == localID {
				owned = append(owned, pid)
				break
			}
		}
	}
	return owned
}

func TestInitialOwnership(t *testing.T) {
	o := ownership.New(testLocalID, testRF, testNumPartitions)

	ms := members(testLocalID, "node-b", "node-c")
	o.Update(ms)

	want := expectedOwned(testLocalID, ms, testRF, testNumPartitions)
	got := o.Owned()

	slices.Sort(want)
	slices.Sort(got)

	if len(got) != len(want) {
		t.Fatalf("Owned() len=%d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("Owned()[%d]=%d, want %d", i, got[i], want[i])
		}
	}
}

func TestMemberLeaveGrowsOwnedSet(t *testing.T) {
	o := ownership.New(testLocalID, testRF, testNumPartitions)

	// 3 nodes, RF=2: local node owns ~2/3 of partitions.
	full := members(testLocalID, "node-b", "node-c")
	o.Update(full)
	ownedFull := o.Owned()

	// node-b leaves; with 2 nodes and RF=2, local node now owns all partitions.
	reduced := members(testLocalID, "node-c")
	o.Update(reduced)
	ownedReduced := o.Owned()

	if len(ownedReduced) <= len(ownedFull) {
		t.Errorf("expected more partitions when node-b leaves (had %d, now %d)",
			len(ownedFull), len(ownedReduced))
	}
	// Every partition previously owned should still be owned.
	for _, pid := range ownedFull {
		if !slices.Contains(ownedReduced, pid) {
			t.Errorf("partition %d was owned before and should still be owned", pid)
		}
	}
}

func TestSameMembershipNoOp(t *testing.T) {
	o := ownership.New(testLocalID, testRF, testNumPartitions)

	ms := members(testLocalID, "node-b")
	o.Update(ms)
	first := append([]uint32(nil), o.Owned()...)

	// Second update with the same membership: owned set unchanged.
	o.Update(ms)
	second := o.Owned()

	slices.Sort(first)
	slices.Sort(second)
	if !slices.Equal(first, second) {
		t.Errorf("same-membership update changed owned set: %v -> %v", first, second)
	}
}

func TestSubscribePublishesOwnedSet(t *testing.T) {
	o := ownership.New(testLocalID, testRF, testNumPartitions)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sub := o.Subscribe(ctx)

	ms := members(testLocalID, "node-b", "node-c")
	o.Update(ms)

	want := expectedOwned(testLocalID, ms, testRF, testNumPartitions)
	slices.Sort(want)

	select {
	case got := <-sub:
		slices.Sort(got)
		if !slices.Equal(got, want) {
			t.Errorf("Subscribe got %v, want %v", got, want)
		}
	default:
		t.Fatal("expected a published owned set after Update")
	}
}

func TestPeersExcludesLocal(t *testing.T) {
	o := ownership.New(testLocalID, testRF, testNumPartitions)
	ms := members(testLocalID, "node-b", "node-c")
	o.Update(ms)

	for _, pid := range o.Owned() {
		for _, peer := range o.Peers(pid) {
			if peer.ReplicaID == testLocalID {
				t.Errorf("Peers(pid=%d) includes local node %q", pid, testLocalID)
			}
		}
	}
}

func TestIsOwnerMatchesOwned(t *testing.T) {
	o := ownership.New(testLocalID, testRF, testNumPartitions)
	ms := members(testLocalID, "node-b", "node-c")
	o.Update(ms)

	// IsOwner must agree with the HRW ground truth for every partition.
	for pid := uint32(0); pid < uint32(testNumPartitions); pid++ {
		wantOwned := false
		for _, m := range placement.Owners(pid, ms, testRF) {
			if m.ReplicaID == testLocalID {
				wantOwned = true
				break
			}
		}
		if got := o.IsOwner(pid); got != wantOwned {
			t.Errorf("IsOwner(pid=%d)=%v, want %v", pid, got, wantOwned)
		}
	}
}

func TestPeersMatchHRW(t *testing.T) {
	o := ownership.New(testLocalID, testRF, testNumPartitions)
	ms := members(testLocalID, "node-b", "node-c")
	o.Update(ms)

	for _, pid := range o.Owned() {
		allOwners := placement.Owners(pid, ms, testRF)
		var wantPeers []string
		for _, m := range allOwners {
			if m.ReplicaID != testLocalID {
				wantPeers = append(wantPeers, m.ReplicaID)
			}
		}
		slices.Sort(wantPeers)

		var gotPeers []string
		for _, m := range o.Peers(pid) {
			gotPeers = append(gotPeers, m.ReplicaID)
		}
		slices.Sort(gotPeers)

		if len(gotPeers) != len(wantPeers) {
			t.Errorf("pid=%d Peers=%v, want %v", pid, gotPeers, wantPeers)
		}
	}
}
