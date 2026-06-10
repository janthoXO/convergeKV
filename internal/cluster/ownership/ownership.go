// Package ownership tracks which virtual partitions the local node currently
// owns and who the co-owners of each partition are. It recomputes whenever
// the gossip membership changes.
package ownership

import (
	"context"
	"maps"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/janthoXO/convergeKV/internal/core/ports"
	"github.com/janthoXO/convergeKV/internal/domain/placement"
	"github.com/janthoXO/convergeKV/internal/util/eventbus"
)

// ownershipState is an immutable snapshot of the current ownership view.
// Readers swap the pointer atomically and never block writers.
type ownershipState struct {
	owned map[uint32]struct{}
	// owners stores HRW peers for every partition with the local node excluded.
	// For owned partitions these are the co-owners; for non-owned partitions
	// these are the forwarding targets. At default settings this is ~512 × RF
	// MemberInfo values — well under a MB.
	owners map[uint32][]ports.MemberInfo
}

// Ownership tracks which virtual partitions the local node currently owns and
// who the peers of each partition are.
// All exported methods are safe for concurrent use.
//
// Update only recomputes and publishes the owned set; it does not perform any
// I/O. Partition lifecycle (seeding/dropping per-partition state) is handled
// by a separate subscriber via Subscribe — see internal/cluster/partitions —
// so a slow Badger scan never blocks the membership-watcher loop.
type Ownership struct {
	// state is swapped atomically on every Update so readers never block.
	state atomic.Pointer[ownershipState]

	// changeTopic publishes the owned partition set on every Update.
	changeTopic eventbus.Topic[[]uint32]

	// updateMu serialises concurrent calls to Update/update.
	updateMu      sync.Mutex
	localID       string
	rf            int
	numPartitions int
}

// New constructs an empty Ownership.
func New(localID string, rf, numPartitions int) *Ownership {
	o := &Ownership{
		localID:       localID,
		rf:            rf,
		numPartitions: numPartitions,
	}
	o.state.Store(&ownershipState{
		owned:  make(map[uint32]struct{}),
		owners: make(map[uint32][]ports.MemberInfo),
	})
	return o
}

// Update recomputes owned partitions and publishes the new owned set to any
// Subscribe-rs. Safe for concurrent use.
func (o *Ownership) Update(members []ports.MemberInfo) {
	o.updateMu.Lock()
	defer o.updateMu.Unlock()

	newOwned := make(map[uint32]struct{}, o.numPartitions/2)
	newOwners := make(map[uint32][]ports.MemberInfo, o.numPartitions)

	for partitionId := uint32(0); partitionId < uint32(o.numPartitions); partitionId++ {
		allOwners := placement.Owners(partitionId, members, o.rf)
		local := false
		var peers []ports.MemberInfo
		for _, m := range allOwners {
			if m.ReplicaID == o.localID {
				local = true
			} else {
				peers = append(peers, m)
			}
		}
		newOwners[partitionId] = peers
		if local {
			newOwned[partitionId] = struct{}{}
		}
	}

	o.state.Store(&ownershipState{owned: newOwned, owners: newOwners})
	o.changeTopic.Publish(slices.Collect(maps.Keys(newOwned)))
}

// Subscribe returns a channel that receives the full owned-partition-ID set
// on every Update. The channel is closed when ctx is cancelled.
func (o *Ownership) Subscribe(ctx context.Context) <-chan []uint32 {
	return o.changeTopic.Subscribe(ctx)
}

// Owned returns a snapshot of the currently owned partition IDs.
func (o *Ownership) Owned() []uint32 {
	s := o.state.Load()
	return slices.Collect(maps.Keys(s.owned))
}

// IsOwner reports whether the local node owns partitionId.
func (o *Ownership) IsOwner(partitionId uint32) bool {
	s := o.state.Load()
	_, ok := s.owned[partitionId]
	return ok
}

// Peers returns the HRW members for partitionId excluding the local node.
// For owned partitions these are the co-owners; for non-owned partitions
// these are the forwarding targets. Returns nil if partitionId is unknown.
func (o *Ownership) Peers(partitionId uint32) []ports.MemberInfo {
	s := o.state.Load()
	peers := s.owners[partitionId]
	if len(peers) == 0 {
		return nil
	}
	out := make([]ports.MemberInfo, len(peers))
	copy(out, peers)
	return out
}
