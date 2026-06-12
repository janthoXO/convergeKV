package memberlist

import (
	"encoding/json"
	"fmt"

	"github.com/cespare/xxhash/v2"
)

// NodeMeta is the metadata broadcast by each node via memberlist.
// It is encoded as JSON and stored in memberlist.Node.Meta.
type NodeMeta struct {
	ReplicaID string `json:"id"`
	GRPCPort  int    `json:"grpc"`

	// ConfigFingerprint hashes the cluster-wide invariants (NUM_PARTITIONS,
	// RF, IBLT_CELLS — see CLAUDE.md). Two nodes that disagree on any of these
	// silently diverge rather than erroring, so rebuildMembers logs loudly on
	// a mismatch the moment a peer is discovered, rather than waiting for the
	// first anti-entropy round to fail (see also iblt.Subtract's
	// ErrSizeMismatch).
	ConfigFingerprint uint64 `json:"cfg"`
}

// ConfigFingerprint hashes the cluster-wide config invariants that every node
// must agree on.
func ConfigFingerprint(numPartitions, rf, ibltCells int) uint64 {
	return xxhash.Sum64String(fmt.Sprintf("%d|%d|%d", numPartitions, rf, ibltCells))
}

// EncodeMeta serialises m to JSON. Panics if encoding fails (fields are always valid).
func EncodeMeta(m NodeMeta) []byte {
	b, err := json.Marshal(m)
	if err != nil {
		panic("memberlist: EncodeMeta: " + err.Error())
	}
	return b
}

// DecodeMeta deserialises a NodeMeta from raw bytes.
func DecodeMeta(b []byte) (NodeMeta, error) {
	var m NodeMeta
	return m, json.Unmarshal(b, &m)
}
