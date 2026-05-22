// Package hrw implements Rendezvous (Highest Random Weight) hashing for
// replica placement over virtual partitions. All functions are pure and
// stateless; the caller supplies the current membership list on every call.
package hrw

import (
	"encoding/binary"
	"sort"

	"github.com/cespare/xxhash/v2"
	"github.com/janthoXO/convergeKV/internal/gossip"
)

// score computes a stable uint64 score for the (partitionID, replicaID) pair.
// partitionID is encoded as 4 big-endian bytes so the hash space is independent
// of the string representation of the partition number.
func score(partitionID uint32, replicaID string) uint64 {
	var partitionIdBytes [4]byte
	binary.BigEndian.PutUint32(partitionIdBytes[:], partitionID)
	h := xxhash.New()
	h.Write(partitionIdBytes[:])
	h.WriteString(replicaID)
	return h.Sum64()
}

// Owners returns the rf members with the highest HRW scores for partitionID.
// If len(members) < rf, all members are returned.
// The returned slice is ordered by descending score (highest scorer first).
func Owners(partitionID uint32, members []gossip.MemberInfo, rf int) []gossip.MemberInfo {
	if len(members) == 0 {
		return nil
	}

	type scored struct {
		member gossip.MemberInfo
		s      uint64
	}

	ranked := make([]scored, len(members))
	for i, m := range members {
		ranked[i] = scored{member: m, s: score(partitionID, m.ReplicaID)}
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
