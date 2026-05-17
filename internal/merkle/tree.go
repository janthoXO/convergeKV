package merkle

import (
	"crypto/sha256"
	"encoding/binary"
	"sync"
)

// Hash is a 32-byte SHA-256 digest used at each tree node.
type Hash [32]byte

// emptyHash is the zero value, representing an empty bucket.
var emptyHash Hash

// MerkleTree is a thread-safe incremental Merkle tree over the key space.
// Leaf i corresponds to bucket i. Each leaf stores the XOR of the
// content hashes of all (key, field, replica_id, timestamp) tuples in that bucket.
type MerkleTree struct {
	mu    sync.RWMutex
	nodes []Hash // length = 2 * NumBuckets; index 0 unused; root at index 1
}

// NewMerkleTree returns an empty tree with all hashes zeroed.
func NewMerkleTree() *MerkleTree {
	return &MerkleTree{
		nodes: make([]Hash, 2*NumBuckets),
	}
}

// Root returns the current root hash. A matching root between two nodes means
// their entire state is identical (with overwhelming probability).
func (t *MerkleTree) Root() Hash {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.nodes[1]
}

// Update incorporates a single field entry into the tree.
// If replacing an existing entry, call Remove first, then Update.
func (t *MerkleTree) Update(key, field, replicaID string, physMs uint64, logical uint32) {
	h := entryHash(key, field, replicaID, physMs, logical)
	bucket := BucketIndex(key)

	t.mu.Lock()
	defer t.mu.Unlock()

	leaf := NumBuckets + bucket
	t.nodes[leaf] = xorHash(t.nodes[leaf], h)

	t.bubbleUp(leaf)
}

// Remove removes a single field entry from the tree.
// XOR is its own inverse: XOR-ing the same hash twice cancels it out.
func (t *MerkleTree) Remove(key, field, replicaID string, physMs uint64, logical uint32) {
	h := entryHash(key, field, replicaID, physMs, logical)
	bucket := BucketIndex(key)

	t.mu.Lock()
	defer t.mu.Unlock()

	leaf := NumBuckets + bucket
	t.nodes[leaf] = xorHash(t.nodes[leaf], h) // XOR cancels

	t.bubbleUp(leaf)
}

// BucketHash returns the hash of a single leaf bucket.
// Used during Phase 1 binary search to compare individual leaves.
func (t *MerkleTree) BucketHash(bucket int) Hash {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.nodes[NumBuckets+bucket]
}

// DivergentBuckets compares this tree against a peer's bucket hashes (a flat
// slice of NumBuckets hashes, indexed by bucket number) and returns the list
// of bucket indices where the hashes differ.
// The peer sends its bucket hashes during Phase 1; this function identifies
// which ranges need a full delta exchange in Phase 2.
func (t *MerkleTree) DivergentBuckets(peerBuckets []Hash) []int {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var out []int
	for i, ph := range peerBuckets {
		if i >= NumBuckets {
			break
		}
		if t.nodes[NumBuckets+i] != ph {
			out = append(out, i)
		}
	}

	return out
}

// AllBucketHashes returns a snapshot of all leaf hashes, indexed by bucket.
// Sent to a peer during Phase 1 so they can call DivergentBuckets.
func (t *MerkleTree) AllBucketHashes() []Hash {
	t.mu.RLock()
	defer t.mu.RUnlock()

	out := make([]Hash, NumBuckets)
	copy(out, t.nodes[NumBuckets:])

	return out
}

// ── internal helpers ──────────────────────────────────────────────────────────

// bubbleUp recomputes all ancestor hashes from leaf up to the root.
// If both children are the zero hash, the parent is set to zero as well,
// preserving the invariant that an empty tree (all leaves zero) has a zero root.
// Caller must hold t.mu write lock.
func (t *MerkleTree) bubbleUp(leaf int) {
	for i := leaf >> 1; i >= 1; i >>= 1 {
		left, right := 2*i, 2*i+1
		if t.nodes[left] == emptyHash && t.nodes[right] == emptyHash {
			t.nodes[i] = emptyHash
		} else {
			t.nodes[i] = sha256.Sum256(append(t.nodes[left][:], t.nodes[right][:]...))
		}
	}
}

// entryHash produces a stable 32-byte hash for a single field entry.
// All four fields are included so that any change to timestamp or replica_id
// produces a different hash.
func entryHash(key, field, replicaID string, physMs uint64, logical uint32) Hash {
	h := sha256.New()
	h.Write([]byte(key))
	h.Write([]byte{0}) // separator
	h.Write([]byte(field))
	h.Write([]byte{0})
	h.Write([]byte(replicaID))

	var buf [12]byte
	binary.BigEndian.PutUint64(buf[:8], physMs)
	binary.BigEndian.PutUint32(buf[8:], logical)

	h.Write(buf[:])

	var out Hash
	copy(out[:], h.Sum(nil))

	return out
}

// xorHash returns a XOR b.
func xorHash(a, b Hash) (out Hash) {
	for i := range a {
		out[i] = a[i] ^ b[i]
	}
	return
}
