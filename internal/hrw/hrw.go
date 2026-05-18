// Package hrw implements Rendezvous (Highest Random Weight) hashing for
// replica placement. All functions are pure and stateless; the caller supplies
// the current membership list on every call.
package hrw

import (
	"sort"

	"github.com/cespare/xxhash/v2"
	"github.com/janthoXO/convergeKV/internal/gossip"
)

// score computes a stable uint64 score for the (key, member) pair using
// xxhash over key ∥ replicaID. xxhash is already a project dependency and
// is ~10× faster than SHA-256 for this use case.
func score(key, replicaID string) uint64 {
	h := xxhash.New()
	h.WriteString(key)
	h.WriteString(replicaID)
	return h.Sum64()
}

// Replicas returns the rf members with the highest HRW scores for key.
// If len(members) < rf, all members are returned.
// The returned slice is ordered by descending score (highest scorer first).
func Replicas(key string, members []gossip.MemberInfo, rf int) []gossip.MemberInfo {
	if len(members) == 0 {
		return nil
	}

	type scored struct {
		member gossip.MemberInfo
		s      uint64
	}

	ranked := make([]scored, len(members))
	for i, m := range members {
		ranked[i] = scored{member: m, s: score(key, m.ReplicaID)}
	}
	// Sort descending by score; break ties deterministically by ReplicaID.
	sort.Slice(ranked, func(i, j int) bool {
		if ranked[i].s != ranked[j].s {
			return ranked[i].s > ranked[j].s
		}
		return ranked[i].member.ReplicaID > ranked[j].member.ReplicaID
	})

	if rf > len(ranked) {
		rf = len(ranked)
	}
	out := make([]gossip.MemberInfo, rf)
	for i := 0; i < rf; i++ {
		out[i] = ranked[i].member
	}
	return out
}

// IsReplica returns true if nodeID appears in Replicas(key, members, rf).
func IsReplica(key, nodeID string, members []gossip.MemberInfo, rf int) bool {
	for _, m := range Replicas(key, members, rf) {
		if m.ReplicaID == nodeID {
			return true
		}
	}
	return false
}
