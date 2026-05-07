package ring

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
	"sync/atomic"

	"github.com/janthoXO/convergeKV/internal/merkle"
)

const (
	VnodesPerNode   = 150 // virtual nodes per physical node
	DefaultReplicas = 3   // replication factor
)

// Member is one physical node on the ring.
type Member struct {
	ReplicaID string
	GRPCAddr  string
}

// ring is an immutable snapshot of the consistent hash ring.
// All reads are lock-free via atomic load of the *ring pointer.
type ring struct {
	vnodes  []vnode  // sorted by hash position
	members []Member // indexed by member index (unique per ReplicaID)
	rf      int

	// partitionOwners[i] is the slice of member indices (into .members)
	// that are replicas for partition i. Precomputed on Rebuild.
	partitionOwners [][]int // length = merkle.NumPartitions
}

type vnode struct {
	hash      uint64
	memberIdx int
}

// Ring is an atomic reference to an immutable ring snapshot.
// Rebuild() replaces the snapshot; GetReplicas() reads it lock-free.
type Ring struct {
	ptr atomic.Pointer[ring]
}

// NewRing returns an empty Ring.
func NewRing() *Ring { return &Ring{} }

// Rebuild replaces the ring with a fresh snapshot built from members.
// Call this every time gossip reports a membership change.
func (r *Ring) Rebuild(members []Member, rf int) {
	if rf < 1 {
		rf = 1
	}
	if rf > len(members) {
		rf = len(members)
	}

	vnodes := make([]vnode, 0, len(members)*VnodesPerNode)
	for idx, m := range members {
		for i := 0; i < VnodesPerNode; i++ {
			h := vnodeHash(m.ReplicaID, i)
			vnodes = append(vnodes, vnode{hash: h, memberIdx: idx})
		}
	}
	sort.Slice(vnodes, func(i, j int) bool { return vnodes[i].hash < vnodes[j].hash })

	snap := &ring{
		vnodes:  vnodes,
		members: members,
		rf:      rf,
	}

	// Precompute partition ownership for Merkle-aligned anti-entropy.
	owners := make([][]int, merkle.NumPartitions)
	for i := 0; i < merkle.NumPartitions; i++ {
		midToken := merkle.PartitionMidToken(i)
		replicas := snap.getReplicasByToken(midToken)
		idxs := make([]int, 0, len(replicas))
		for _, repl := range replicas {
			for k, m := range members {
				if m.ReplicaID == repl.ReplicaID {
					idxs = append(idxs, k)
					break
				}
			}
		}
		owners[i] = idxs
	}
	snap.partitionOwners = owners

	r.ptr.Store(snap)
}

// GetReplicas returns the RF Members responsible for key, in ring order.
// The first member is the primary. Returns nil if the ring is empty.
func (r *Ring) GetReplicas(key string) []Member {
	snap := r.ptr.Load()
	if snap == nil || len(snap.vnodes) == 0 {
		return nil
	}
	return snap.getReplicas(key)
}

// IsPrimary returns true if replicaID is the primary (first replica) for key.
func (r *Ring) IsPrimary(key, replicaID string) bool {
	replicas := r.GetReplicas(key)
	return len(replicas) > 0 && replicas[0].ReplicaID == replicaID
}

// IsReplica returns true if replicaID is any of the RF replicas for key.
func (r *Ring) IsReplica(key, replicaID string) bool {
	for _, m := range r.GetReplicas(key) {
		if m.ReplicaID == replicaID {
			return true
		}
	}
	return false
}

// Primary returns the primary Member for key, or the zero Member if ring is empty.
func (r *Ring) Primary(key string) (Member, bool) {
	replicas := r.GetReplicas(key)
	if len(replicas) == 0 {
		return Member{}, false
	}
	return replicas[0], true
}

// SharedPartitions returns the partition indices where both localID and peerID
// are in the replica set. These are the only partitions whose Merkle hashes
// need to be compared during anti-entropy between these two nodes.
//
// The result is computed from the precomputed partition cache — O(NumPartitions).
// Call this once per anti-entropy round after confirming the peer is alive.
func (r *Ring) SharedPartitions(localID, peerID string) []int {
	snap := r.ptr.Load()
	if snap == nil {
		return nil
	}
	var out []int
	for i, ownerIdxs := range snap.partitionOwners {
		hasLocal, hasPeer := false, false
		for _, idx := range ownerIdxs {
			rid := snap.members[idx].ReplicaID
			if rid == localID {
				hasLocal = true
			}
			if rid == peerID {
				hasPeer = true
			}
		}
		if hasLocal && hasPeer {
			out = append(out, i)
		}
	}
	return out
}

// OwnsPartition returns true if replicaID is in the replica set for partition i.
// Faster than IsReplica because it uses the precomputed partition cache.
func (r *Ring) OwnsPartition(partition int, replicaID string) bool {
	snap := r.ptr.Load()
	if snap == nil || partition < 0 || partition >= merkle.NumPartitions {
		return false
	}
	for _, idx := range snap.partitionOwners[partition] {
		if snap.members[idx].ReplicaID == replicaID {
			return true
		}
	}
	return false
}

// ── immutable ring methods ────────────────────────────────────────────────────

func (rg *ring) getReplicas(key string) []Member {
	h := keyHash(key)
	return rg.getReplicasByToken(h)
}

// getReplicasByToken looks up replicas by raw ring token (used both for key
// lookups and for partition midpoint sampling during Rebuild).
func (rg *ring) getReplicasByToken(token uint64) []Member {
	if len(rg.vnodes) == 0 {
		return nil
	}
	// Binary search: find the first vnode with hash >= token
	idx := sort.Search(len(rg.vnodes), func(i int) bool { return rg.vnodes[i].hash >= token })
	if idx == len(rg.vnodes) {
		idx = 0 // wrap around
	}

	// Walk clockwise, collecting up to rf distinct physical members.
	seen := make(map[int]struct{}, rg.rf)
	var result []Member
	for i := 0; i < len(rg.vnodes) && len(result) < rg.rf; i++ {
		vn := rg.vnodes[(idx+i)%len(rg.vnodes)]
		if _, ok := seen[vn.memberIdx]; ok {
			continue
		}
		seen[vn.memberIdx] = struct{}{}
		result = append(result, rg.members[vn.memberIdx])
	}
	return result
}

// vnodeHash hashes "<replicaID>-<index>" to a uint64 position on the ring.
func vnodeHash(replicaID string, idx int) uint64 {
	h := sha256.Sum256([]byte(fmt.Sprintf("%s-%d", replicaID, idx)))
	return binary.BigEndian.Uint64(h[:8])
}

// keyHash hashes a key to a uint64 position on the ring.
// This produces the same value as merkle.RingToken — they share the same algorithm.
func keyHash(key string) uint64 {
	h := sha256.Sum256([]byte(key))
	return binary.BigEndian.Uint64(h[:8])
}
