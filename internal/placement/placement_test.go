package placement

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/janthoXO/convergeKV/internal/cluster"
)

func members(n int, p uint16, status cluster.Status) []cluster.Member {
	out := make([]cluster.Member, n)
	for i := range out {
		flags := cluster.NewPartitionFlags(p)
		for pid := uint16(0); pid < p; pid++ {
			flags.Set(pid, status)
		}
		out[i] = cluster.Member{
			Meta: cluster.NodeMeta{
				ID:         [16]byte{byte(i + 1), 0xA0},
				Partitions: p,
				Flags:      flags,
			},
			Addr: fmt.Sprintf("10.0.0.%d:7001", i+1),
		}
	}
	return out
}

// digest summarizes a whole owners table for determinism comparisons.
func digest(v *View, p uint16) string {
	h := sha256.New()
	for pid := uint16(0); pid < p; pid++ {
		for _, o := range v.Owners(pid) {
			h.Write(o.ID[:])
		}
	}
	return hex.EncodeToString(h.Sum(nil))
}

func TestDeterministicAcrossNodesAndPermutations(t *testing.T) {
	const p = 256
	ms := members(9, p, cluster.StatusActive)
	want := digest(Compute(p, ms), p)

	// Same members in a different slice order: identical table.
	perm := append([]cluster.Member{}, ms[4:]...)
	perm = append(perm, ms[:4]...)
	if got := digest(Compute(p, perm), p); got != want {
		t.Fatal("owners table depends on membership slice order")
	}
	if got := digest(Compute(p, ms), p); got != want {
		t.Fatal("owners table not reproducible")
	}
}

// Golden value: placement is part of the cross-node contract. If this test
// breaks, the change reshuffles every partition in existing clusters.
func TestGoldenPlacement(t *testing.T) {
	const p = 256
	v := Compute(p, members(9, p, cluster.StatusActive))
	const golden = "7df296d83db7038b35310fdda2ab39987a0c3e14c5dee67fd80fbfa358e2c245"
	if got := digest(v, p); got != golden {
		t.Fatalf("placement contract changed: digest %s, want %s", got, golden)
	}
}

func TestReplicationFactor(t *testing.T) {
	const p = 64
	for _, n := range []int{1, 2, 3, 9} {
		v := Compute(p, members(n, p, cluster.StatusActive))
		wantRF := min(RF, n)
		for pid := uint16(0); pid < p; pid++ {
			owners := v.Owners(pid)
			if len(owners) != wantRF {
				t.Fatalf("n=%d pid=%d: %d owners, want %d", n, pid, len(owners), wantRF)
			}
			seen := map[[16]byte]bool{}
			for _, o := range owners {
				if seen[o.ID] {
					t.Fatalf("pid %d: duplicate owner", pid)
				}
				seen[o.ID] = true
			}
		}
	}
}

func TestMinimalMovementOnJoin(t *testing.T) {
	const p = 256
	nine := members(9, p, cluster.StatusActive)
	ten := members(10, p, cluster.StatusActive)

	before := Compute(p, nine)
	after := Compute(p, ten)

	moved := 0
	for pid := uint16(0); pid < p; pid++ {
		was := map[[16]byte]bool{}
		for _, o := range before.Owners(pid) {
			was[o.ID] = true
		}
		for _, o := range after.Owners(pid) {
			if !was[o.ID] {
				moved++
			}
		}
	}
	// Expect ≈ P·RF/10 gained replicas, ±20%.
	want := float64(p) * RF / 10
	if f := float64(moved); f < want*0.8 || f > want*1.2 {
		t.Fatalf("moved %d partition-replicas, want %.0f ±20%%", moved, want)
	}
	t.Logf("moved %d (ideal %.0f)", moved, want)
}

func TestBalancedDistribution(t *testing.T) {
	const p = 256
	v := Compute(p, members(8, p, cluster.StatusActive))
	count := map[[16]byte]int{}
	for pid := uint16(0); pid < p; pid++ {
		for _, o := range v.Owners(pid) {
			count[o.ID]++
		}
	}
	ideal := float64(p) * RF / 8
	for id, c := range count {
		if f := float64(c); f < ideal*0.6 || f > ideal*1.4 {
			t.Fatalf("node %x owns %d partitions, ideal %.0f", id[:2], c, ideal)
		}
	}
}

func TestStatusSets(t *testing.T) {
	const p = 16
	ms := members(3, p, cluster.StatusActive)
	// Node 0 is bootstrapping everywhere, node 2 draining everywhere.
	for pid := uint16(0); pid < p; pid++ {
		ms[0].Meta.Flags.Set(pid, cluster.StatusBootstrapping)
		ms[2].Meta.Flags.Set(pid, cluster.StatusDraining)
	}
	v := Compute(p, ms)

	for pid := uint16(0); pid < p; pid++ {
		for _, o := range v.ReadSet(pid) {
			if o.Status != cluster.StatusActive {
				t.Fatal("read set must contain only active owners")
			}
			if o.ID == ms[0].Meta.ID {
				t.Fatal("bootstrapping owner in read set")
			}
		}
		for _, o := range v.WriteSet(pid) {
			if o.Status == cluster.StatusDraining {
				t.Fatal("draining owner in write set")
			}
		}
		if a, ok := v.Applier(pid); ok {
			if a.Status != cluster.StatusActive {
				t.Fatal("applier must be active")
			}
			// And it must be the FIRST active owner in rank order.
			for _, o := range v.Owners(pid) {
				if o.Status == cluster.StatusActive {
					if o.ID != a.ID {
						t.Fatal("applier is not the first active owner in rank order")
					}
					break
				}
			}
		}
	}
}

func TestApplierAbsentWhenNoActiveOwner(t *testing.T) {
	const p = 4
	ms := members(2, p, cluster.StatusBootstrapping)
	v := Compute(p, ms)
	if _, ok := v.Applier(0); ok {
		t.Fatal("no active owner: applier must be absent")
	}
}

func TestPartitionFunction(t *testing.T) {
	const p = 256
	counts := make([]int, p)
	for i := 0; i < 100_000; i++ {
		counts[Partition(fmt.Appendf(nil, "key-%d", i), p)]++
	}
	for pid, c := range counts {
		if c == 0 {
			t.Fatalf("partition %d never hit", pid)
		}
	}
	// Pin the concrete mapping: like placement, partition assignment is a
	// cross-node, cross-version contract.
	if got := Partition([]byte("stable-key"), p); got != 236 {
		t.Fatalf("Partition(stable-key) = %d, want 236", got)
	}
}
