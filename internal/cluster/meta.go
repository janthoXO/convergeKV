package cluster

import (
	"encoding/binary"
	"fmt"

	"github.com/janthoXO/convergeKV/internal/nodeid"
)

// Status is a node's per-partition replication status, gossiped to the
// cluster. Two bits per partition.
type Status uint8

const (
	// StatusNone: the node does not own the partition.
	StatusNone Status = iota
	// StatusBootstrapping: owner that receives writes/deltas but does not
	// serve reads and is never chosen as applier.
	StatusBootstrapping
	// StatusActive: fully serving owner.
	StatusActive
	// StatusDraining: owner on its way out (planned leave).
	StatusDraining
)

// Serving reports whether an owner in this status answers reads and writes:
// active or draining (a leaver keeps serving until its successor is active).
// Bootstrapping owners have incomplete data and None owners hold nothing.
func (s Status) Serving() bool {
	return s == StatusActive || s == StatusDraining
}

func (s Status) String() string {
	switch s {
	case StatusNone:
		return "none"
	case StatusBootstrapping:
		return "bootstrapping"
	case StatusActive:
		return "active"
	case StatusDraining:
		return "draining"
	}
	return fmt.Sprintf("status(%d)", uint8(s))
}

// PartitionFlags packs one Status per partition, four per byte.
type PartitionFlags []byte

func NewPartitionFlags(p uint16) PartitionFlags {
	return make(PartitionFlags, (int(p)+3)/4)
}

func (f PartitionFlags) Get(pid uint16) Status {
	return Status(f[pid/4] >> ((pid % 4) * 2) & 0b11)
}

func (f PartitionFlags) Set(pid uint16, s Status) {
	shift := (pid % 4) * 2
	f[pid/4] = f[pid/4]&^(0b11<<shift) | byte(s)<<shift
}

func (f PartitionFlags) Clone() PartitionFlags {
	out := make(PartitionFlags, len(f))
	copy(out, f)
	return out
}

// NodeMeta is the gossip payload attached to every member.
//
// Wire size: 28 + len(RPCAddr) + P/4 bytes. memberlist caps metadata at 512
// bytes, so P is limited to 1024 with this encoding (config enforces it).
type NodeMeta struct {
	ID         nodeid.ID
	Partitions uint16 // cluster-wide P this node was bootstrapped with
	Generation uint64 // node start time (ms); orders restarts of one node
	RPCAddr    string // node-service gRPC address peers must dial
	Flags      PartitionFlags
}

const metaVersion = 1

func (m *NodeMeta) Encode() []byte {
	b := make([]byte, 0, 28+len(m.RPCAddr)+len(m.Flags))
	b = append(b, metaVersion)
	b = append(b, m.ID[:]...)
	b = binary.BigEndian.AppendUint16(b, m.Partitions)
	b = binary.BigEndian.AppendUint64(b, m.Generation)
	b = append(b, byte(len(m.RPCAddr)))
	b = append(b, m.RPCAddr...)
	return append(b, m.Flags...)
}

func DecodeMeta(b []byte) (NodeMeta, error) {
	var m NodeMeta
	if len(b) < 28 {
		return m, fmt.Errorf("cluster: meta too short (%d bytes)", len(b))
	}
	if b[0] != metaVersion {
		return m, fmt.Errorf("cluster: unsupported meta version %d", b[0])
	}
	copy(m.ID[:], b[1:17])
	m.Partitions = binary.BigEndian.Uint16(b[17:19])
	m.Generation = binary.BigEndian.Uint64(b[19:27])
	addrLen := int(b[27])
	if len(b) < 28+addrLen {
		return m, fmt.Errorf("cluster: meta truncated in rpc addr")
	}
	m.RPCAddr = string(b[28 : 28+addrLen])
	flags := b[28+addrLen:]
	if want := (int(m.Partitions) + 3) / 4; len(flags) != want {
		return m, fmt.Errorf("cluster: flags length %d, want %d for P=%d", len(flags), want, m.Partitions)
	}
	m.Flags = PartitionFlags(flags).Clone()
	return m, nil
}
