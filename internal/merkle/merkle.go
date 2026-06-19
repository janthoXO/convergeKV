// Package merkle defines the anti-entropy hashing primitives shared by the
// write path (incremental leaf maintenance) and the exchange engine.
//
// Each partition has a fixed vector of Buckets leaf hashes. A document's
// hash covers its key and FULL canonical encoding rather than its context
// alone: with multi-value register sets, two replicas can hold identical
// contexts with different register subsets, which a context-only hash can
// never detect, leaving the divergence permanently invisible to
// anti-entropy. A leaf is the XOR of its
// documents' hashes, which makes updates incremental: leaf ^= oldDocHash ^
// newDocHash in the same batch as the document write. The root is the hash
// of the leaf vector.
package merkle

import (
	"encoding/binary"

	"github.com/cespare/xxhash/v2"
	"lukechampine.com/blake3"
)

// Buckets is the fixed leaf count per partition (2^10).
const Buckets = 1024

// HashSize is the size of leaf, document, and root hashes.
const HashSize = 32

type Hash = [HashSize]byte

// Bucket maps a key to its leaf bucket.
func Bucket(key []byte) uint16 {
	return uint16(xxhash.Sum64(key) % Buckets)
}

// DocHash hashes one document's identity: key and FULL canonical document
// encoding (see the package comment for why context-only is not enough).
func DocHash(key, canonicalDoc []byte) Hash {
	h := blake3.New(HashSize, nil)
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], uint32(len(key)))
	_, _ = h.Write(n[:])
	_, _ = h.Write(key)
	_, _ = h.Write(canonicalDoc)
	var out Hash
	copy(out[:], h.Sum(nil))
	return out
}

// XOR folds b into a in place.
func XOR(a *Hash, b Hash) {
	for i := range a {
		a[i] ^= b[i]
	}
}

// Root hashes the leaf vector.
func Root(leaves *[Buckets]Hash) Hash {
	h := blake3.New(HashSize, nil)
	for i := range leaves {
		_, _ = h.Write(leaves[i][:])
	}
	var out Hash
	copy(out[:], h.Sum(nil))
	return out
}

// BucketPath is the storage key suffix for a leaf under the 'm' prefix.
func BucketPath(bucket uint16) []byte {
	return binary.BigEndian.AppendUint16(nil, bucket)
}
