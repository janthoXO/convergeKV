package ring

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
	"sync/atomic"
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

// ── immutable ring methods ────────────────────────────────────────────────────

func (rg *ring) getReplicas(key string) []Member {
	h := keyHash(key)
	// Binary search: find the first vnode with hash >= h
	idx := sort.Search(len(rg.vnodes), func(i int) bool { return rg.vnodes[i].hash >= h })
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
func keyHash(key string) uint64 {
	h := sha256.Sum256([]byte(key))
	return binary.BigEndian.Uint64(h[:8])
}
