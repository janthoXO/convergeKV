package syncer

import (
	"sync"

	"github.com/janthoXO/convergeKV/internal/gossip"
	"github.com/janthoXO/convergeKV/internal/hrw"
)

// Ownership tracks which virtual partitions the local node currently owns and
// who the co-owners of each partition are. It is recomputed on every gossip
// membership change via Update.
//
// All exported methods are safe for concurrent use.
type Ownership struct {
	mu            sync.RWMutex
	owned         []uint32
	coOwners      map[uint32][]gossip.MemberInfo
	localID       string
	rf            int
	numPartitions int
}

// NewOwnership constructs an empty Ownership. Call Update with the initial
// gossip membership to populate it before the syncer loop starts.
func NewOwnership(localID string, rf int, numPartitions int) *Ownership {
	return &Ownership{
		coOwners:      make(map[uint32][]gossip.MemberInfo),
		localID:       localID,
		rf:            rf,
		numPartitions: numPartitions,
	}
}

// Update recomputes owned partitions and their co-owner lists from the current
// gossip membership view. Safe to call from any goroutine (including the
// gossip OnChange callback).
func (o *Ownership) Update(members []gossip.MemberInfo) {
	owned := make([]uint32, 0, o.numPartitions)
	coOwners := make(map[uint32][]gossip.MemberInfo)

	for partitionId := uint32(0); partitionId < uint32(o.numPartitions); partitionId++ {
		owners := hrw.Owners(partitionId, members, o.rf)
		local := false
		var peers []gossip.MemberInfo
		for _, m := range owners {
			if m.ReplicaID == o.localID {
				local = true
			} else {
				peers = append(peers, m)
			}
		}
		if local {
			owned = append(owned, partitionId)
			coOwners[partitionId] = peers
		}
	}

	o.mu.Lock()
	o.owned = owned
	o.coOwners = coOwners
	o.mu.Unlock()
}

// Owned returns a snapshot of the currently owned partition IDs.
func (o *Ownership) Owned() []uint32 {
	o.mu.RLock()
	defer o.mu.RUnlock()

	out := make([]uint32, len(o.owned))
	copy(out, o.owned)

	return out
}

// CoOwners returns the other owners of partitionId (excluding the local node).
// Returns nil if partitionId is not owned by the local node.
func (o *Ownership) CoOwners(partitionId uint32) []gossip.MemberInfo {
	o.mu.RLock()
	defer o.mu.RUnlock()

	peers := o.coOwners[partitionId]
	if len(peers) == 0 {
		return nil
	}

	out := make([]gossip.MemberInfo, len(peers))
	copy(out, peers)

	return out
}
