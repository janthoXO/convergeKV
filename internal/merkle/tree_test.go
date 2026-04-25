package merkle

import (
	"testing"
)

// TestEmptyTreeRootIsZero verifies that a freshly created tree has the zero root.
func TestEmptyTreeRootIsZero(t *testing.T) {
	tree := NewMerkleTree()
	if tree.Root() != emptyHash {
		t.Errorf("expected zero root for empty tree, got %x", tree.Root())
	}
}

// TestUpdateThenRemoveCancels verifies that Update followed by Remove with the
// same arguments returns the root to the zero hash (XOR self-inverse property).
func TestUpdateThenRemoveCancels(t *testing.T) {
	tree := NewMerkleTree()
	tree.Update("key:1", "name", "r1", 1000, 0)
	if tree.Root() == emptyHash {
		t.Fatal("root should be non-zero after Update")
	}
	tree.Remove("key:1", "name", "r1", 1000, 0)
	if tree.Root() != emptyHash {
		t.Errorf("root should be zero after Remove cancels Update, got %x", tree.Root())
	}
}

// TestCommutativity verifies two trees built by inserting the same entries in
// different orders produce the same root.
func TestCommutativity(t *testing.T) {
	treeA := NewMerkleTree()
	treeA.Update("key:1", "name", "r1", 1000, 0)
	treeA.Update("key:2", "city", "r2", 2000, 1)
	treeA.Update("key:3", "age", "r1", 3000, 0)

	treeB := NewMerkleTree()
	treeB.Update("key:3", "age", "r1", 3000, 0)
	treeB.Update("key:1", "name", "r1", 1000, 0)
	treeB.Update("key:2", "city", "r2", 2000, 1)

	if treeA.Root() != treeB.Root() {
		t.Errorf("roots differ: A=%x B=%x", treeA.Root(), treeB.Root())
	}
}

// TestDivergentBuckets verifies that inserting one extra entry on tree B makes
// DivergentBuckets return exactly the bucket containing that entry's key.
func TestDivergentBuckets(t *testing.T) {
	sharedKey := "key:shared"
	extraKey := "key:extra"

	treeA := NewMerkleTree()
	treeA.Update(sharedKey, "name", "r1", 1000, 0)

	treeB := NewMerkleTree()
	treeB.Update(sharedKey, "name", "r1", 1000, 0)
	treeB.Update(extraKey, "city", "r2", 2000, 0) // extra entry only in B

	// From A's perspective: which of A's buckets differ from B's?
	divergentFromA := treeA.DivergentBuckets(treeB.AllBucketHashes())
	expectedBucket := BucketIndex(extraKey)

	if len(divergentFromA) != 1 {
		t.Fatalf("expected 1 divergent bucket, got %d: %v", len(divergentFromA), divergentFromA)
	}
	if divergentFromA[0] != expectedBucket {
		t.Errorf("expected divergent bucket %d (for key %q), got %d", expectedBucket, extraKey, divergentFromA[0])
	}
}

// TestAllBucketHashesLength verifies that AllBucketHashes always returns exactly NumBuckets hashes.
func TestAllBucketHashesLength(t *testing.T) {
	tree := NewMerkleTree()
	hashes := tree.AllBucketHashes()
	if len(hashes) != NumBuckets {
		t.Errorf("expected %d bucket hashes, got %d", NumBuckets, len(hashes))
	}

	// Also verify after some updates.
	tree.Update("somekey", "field", "r1", 999, 0)
	hashes = tree.AllBucketHashes()
	if len(hashes) != NumBuckets {
		t.Errorf("expected %d bucket hashes after update, got %d", NumBuckets, len(hashes))
	}
}

// TestNoDivergenceWhenEqual verifies that two identical trees report no divergent buckets.
func TestNoDivergenceWhenEqual(t *testing.T) {
	treeA := NewMerkleTree()
	treeA.Update("key:1", "name", "r1", 1000, 0)

	treeB := NewMerkleTree()
	treeB.Update("key:1", "name", "r1", 1000, 0)

	divergent := treeA.DivergentBuckets(treeB.AllBucketHashes())
	if len(divergent) != 0 {
		t.Errorf("expected no divergent buckets for identical trees, got %v", divergent)
	}
}
