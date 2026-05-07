package merkle

import (
	"crypto/sha256"
	"encoding/binary"
)

// NumPartitions is the number of leaf nodes in the Merkle tree.
// Must be a power of 2. 1024 gives fine enough granularity that
// a 10-node cluster with RF=3 shares ~70 partitions per peer pair,
// while keeping the tree small (2×1024×32 bytes = 64 KB per node).
const NumPartitions = 1024

// RingToken returns the 64-bit consistent-hash token for a key.
// This is the same value the ring uses for vnode placement, ensuring
// that Merkle partition boundaries align with ring ownership arcs.
func RingToken(key string) uint64 {
	h := sha256.Sum256([]byte(key))
	return binary.BigEndian.Uint64(h[:8])
}

// PartitionIndex maps a key to a leaf partition index in [0, NumPartitions).
// Keys in partition i have ring tokens in [i×W, (i+1)×W) where W = MaxUint64/NumPartitions.
func PartitionIndex(key string) int {
	return int(RingToken(key) % NumPartitions)
}

// PartitionMidToken returns a representative ring token for partition i.
// Used by the ring to precompute which nodes own each partition.
//
// MaxUint64/NumPartitions overflows uint64 arithmetic because MaxUint64+1 = 0.
// Use (1<<63)/N*2 which gives the correct partition width for powers-of-2 N
// without overflow.
func PartitionMidToken(i int) uint64 {
	const W = (1 << 63) / NumPartitions * 2 // ≈ MaxUint64 / NumPartitions
	return uint64(i)*W + W/2
}
