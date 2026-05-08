package merkle

import "github.com/janthoXO/convergeKV/internal/partition"

// NumPartitions is the number of leaf nodes in the Merkle tree.
// Must equal partition.NSlots so that Merkle leaf i == slot i exactly.
// This invariant is verified by TestSlotIndexAgreement in this package.
const NumPartitions = partition.NSlots // 4096

// PartitionIndex maps a key to a leaf index in [0, NumPartitions).
// Delegates to partition.SlotIndex so there is only one implementation of the
// key→slot hash. The two values are equal by construction.
func PartitionIndex(key string) int {
	return partition.SlotIndex(key)
}
