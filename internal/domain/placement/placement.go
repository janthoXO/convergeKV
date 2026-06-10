// Package placement implements Rendezvous (Highest Random Weight) hashing for
// replica placement over virtual partitions. All functions are pure and
// stateless; the caller supplies the current membership list on every call.
package placement

import (
	"encoding/binary"
	"sort"

	"github.com/cespare/xxhash/v2"
)

// Member is any cluster member that has a stable identity and an address.
type Member interface {
	ID() string
	Addr() string
}

func score(partitionID uint32, memberID string) uint64 {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], partitionID)
	h := xxhash.New()
	h.Write(buf[:])
	h.WriteString(memberID)
	return h.Sum64()
}

// Owners returns the rf members with the highest HRW scores for partitionID.
// If len(members) < rf, all members are returned.
// The returned slice is ordered by descending score (highest scorer first).
func Owners[M Member](partitionID uint32, members []M, rf int) []M {
	if len(members) == 0 {
		return nil
	}

	type scored struct {
		member M
		s      uint64
	}

	ranked := make([]scored, len(members))
	for i, m := range members {
		ranked[i] = scored{member: m, s: score(partitionID, m.ID())}
	}
	sort.Slice(ranked, func(i, j int) bool {
		if ranked[i].s != ranked[j].s {
			return ranked[i].s > ranked[j].s
		}
		return ranked[i].member.ID() > ranked[j].member.ID()
	})

	if rf > len(ranked) {
		rf = len(ranked)
	}
	out := make([]M, rf)
	for i := range rf {
		out[i] = ranked[i].member
	}
	return out
}
