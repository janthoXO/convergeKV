package ports

// MemberInfo holds the cluster-membership information that the core layer
// needs to route requests and drive anti-entropy.  It satisfies the
// domain/placement.Member interface structurally.
type MemberInfo struct {
	ReplicaID  string
	GossipAddr string
	GRPCAddr   string
}

// ID satisfies domain/placement.Member.
func (m MemberInfo) ID() string { return m.ReplicaID }

// Addr satisfies domain/placement.Member.
func (m MemberInfo) Addr() string { return m.GRPCAddr }

// MemberView is the read-only membership snapshot port used by the
// coordinator and ownership tracker.
type MemberView interface {
	Members() []MemberInfo
}

// OwnerLookup is the coordinator's view of partition ownership. The concrete
// implementation lives in internal/cluster/ownership; the coordinator depends
// only on this port to avoid recomputing placement.Owners on every request.
type OwnerLookup interface {
	// IsOwner reports whether the local node owns the given partition.
	IsOwner(partitionId uint32) bool
	// Peers returns the HRW members for partitionId excluding the local node.
	// For owned partitions these are co-owners; for non-owned partitions these
	// are forwarding targets.
	Peers(partitionId uint32) []MemberInfo
}
