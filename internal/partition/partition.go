// Package partition maps string keys to fixed-width virtual partition IDs.
// The partition function hashes the key only (never the field) so that all
// (key, field) entries for a given key always land in the same partition.
// NUM_PARTITIONS must be identical on every node in the cluster; disagreement
// silently causes placement divergence.
package partition

import "github.com/cespare/xxhash/v2"

// Of returns the partition ID for key in a cluster configured with
// numPartitions total partitions.
// The result is in [0, numPartitions).
// numPartitions must be > 0; the caller is responsible for enforcing this
// (validated once at server startup in cmd/server/main.go).
func Of(key string, numPartitions int) uint32 {
	return uint32(xxhash.Sum64String(key) % uint64(numPartitions))
}
