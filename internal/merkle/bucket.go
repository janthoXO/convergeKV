package merkle

import (
	"crypto/sha256"
	"encoding/binary"
)

// NumBuckets is the number of leaf nodes in the tree.
// Must be a power of 2. 256 is a reasonable default for a small cluster;
// increase to 1024 for larger datasets.
const NumBuckets = 256

// BucketIndex returns the leaf bucket (0 to NumBuckets-1) for a given key.
// Uses the first 8 bytes of SHA-256 as a stable, uniform hash.
func BucketIndex(key string) int {
	h := sha256.Sum256([]byte(key))
	v := binary.BigEndian.Uint64(h[:8])
	return int(v % NumBuckets)
}
