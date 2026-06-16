// Package placement maps partitions to owners with rendezvous hashing (HRW)
// over partitions: for each partition every alive node is ranked by
// xxhash64(nodeID ‖ partitionID), the top RF are the owners. The computation
// is a pure function of the membership view, so every node derives an
// identical table from an identical view.
package placement

import (
	"encoding/binary"
	"sort"

	"github.com/cespare/xxhash/v2"
	"github.com/janthoXO/convergeKV/internal/cluster"
)

// RF is the replication factor: owners per partition.
const RF = 3

// Partition returns the fixed partition of a user key.
func Partition(key []byte, p uint16) uint16 {
	return uint16(xxhash.Sum64(key) % uint64(p))
}

// Owner is one ranked owner of a partition.
type Owner struct {
	ID     [16]byte
	Addr   string
	Status cluster.Status
	// Dead marks a node within its post-crash grace period: it holds its
	// placement slot (no transfer storm on a quick restart) but serves
	// nothing and is skipped by every set below.
	Dead bool
}

// View is an immutable owners table for one membership snapshot.
type View struct {
	p      uint16
	owners [][]Owner // per partition, HRW rank order
}

// Compute builds the owners table for a membership snapshot: the alive set
// (including self) plus the dead-within-grace set, whose members still rank
// and hold their slots. When an owner is draining (planned leave), the
// owners list extends past RF so its successor starts bootstrapping while
// the leaver still serves.
func Compute(p uint16, members []cluster.Member, dead []cluster.Member) *View {
	v := &View{p: p, owners: make([][]Owner, p)}

	type ranked struct {
		hash   uint64
		member cluster.Member
		dead   bool
	}
	buf := make([]byte, 18)
	rank := make([]ranked, len(members)+len(dead))

	for pid := uint16(0); pid < p && p > 0; pid++ {
		for i, m := range members {
			copy(buf, m.Meta.ID[:])
			binary.BigEndian.PutUint16(buf[16:], pid)
			rank[i] = ranked{hash: xxhash.Sum64(buf), member: m}
		}
		for i, m := range dead {
			copy(buf, m.Meta.ID[:])
			binary.BigEndian.PutUint16(buf[16:], pid)
			rank[len(members)+i] = ranked{hash: xxhash.Sum64(buf), member: m, dead: true}
		}
		sort.Slice(rank, func(i, j int) bool {
			if rank[i].hash != rank[j].hash {
				return rank[i].hash > rank[j].hash
			}
			// Hash ties broken by ID so the order stays total and identical
			// on every node.
			return string(rank[i].member.Meta.ID[:]) > string(rank[j].member.Meta.ID[:])
		})

		var owners []Owner
		held := 0 // slots held by non-leaving owners (dead phantoms count:
		// they hold their slot for the whole grace period — no promotion)
		for i := 0; i < len(rank) && len(owners) < 2*RF; i++ {
			if len(owners) >= RF && held >= RF {
				break
			}
			m := rank[i].member
			o := Owner{
				ID:     m.Meta.ID,
				Addr:   m.Meta.RPCAddr, // the node-service address peers dial
				Status: m.Meta.Flags.Get(pid),
				Dead:   rank[i].dead,
			}
			if len(owners) >= RF && o.Dead {
				// Never extend INTO a dead node; only live successors step
				// up for a draining leaver.
				continue
			}
			owners = append(owners, o)
			if o.Dead || o.Status != cluster.StatusDraining {
				held++
			}
		}
		v.owners[pid] = owners
	}
	return v
}

// Owners returns the partition's owners in HRW rank order.
func (v *View) Owners(pid uint16) []Owner { return v.owners[pid] }

// WriteSet returns the owners that must receive writes and deltas: active,
// bootstrapping, and draining (a leaver keeps serving until its successor is
// active). Dead phantoms receive nothing.
func (v *View) WriteSet(pid uint16) []Owner {
	return v.filter(pid, func(s cluster.Status) bool {
		return s == cluster.StatusActive || s == cluster.StatusBootstrapping || s == cluster.StatusDraining
	})
}

// ReadSet returns the owners allowed to serve reads: active and draining
// (bootstrapping owners have incomplete data).
func (v *View) ReadSet(pid uint16) []Owner {
	return v.filter(pid, cluster.Status.Serving)
}

// Applier returns the partition's dot-minting applier: the first fully
// serving (active or draining) owner in HRW rank order.
func (v *View) Applier(pid uint16) (Owner, bool) {
	for _, o := range v.owners[pid] {
		if !o.Dead && o.Status.Serving() {
			return o, true
		}
	}
	return Owner{}, false
}

// IsOwner reports whether the node owns the partition in this view.
func (v *View) IsOwner(pid uint16, id [16]byte) bool {
	for _, o := range v.owners[pid] {
		if o.ID == id {
			return true
		}
	}
	return false
}

// OwnedPartitions returns all partitions a node owns in this view.
func (v *View) OwnedPartitions(id [16]byte) []uint16 {
	var out []uint16
	for pid := range v.owners {
		if v.IsOwner(uint16(pid), id) {
			out = append(out, uint16(pid))
		}
	}
	return out
}

func (v *View) filter(pid uint16, keep func(cluster.Status) bool) []Owner {
	var out []Owner
	for _, o := range v.owners[pid] {
		if !o.Dead && keep(o.Status) {
			out = append(out, o)
		}
	}
	return out
}
