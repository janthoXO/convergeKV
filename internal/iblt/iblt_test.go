package iblt_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/janthoXO/convergeKV/internal/iblt"
)

const IBLT_DEFAULT_CELLS = 512

func item(s string) []byte { return []byte(s) }

// TestSymmetricDifference verifies the core use-case:
// Insert 100 items into A, 90 of those same items plus 10 different items into B.
// Subtract, decode. onlyInA should have the 10 unique-to-A items, onlyInB should
// have the 10 unique-to-B items.
func TestSymmetricDifference(t *testing.T) {
	const numCommon = 90
	const numUniqueA = 10
	const numUniqueB = 10

	a := iblt.New(IBLT_DEFAULT_CELLS)
	b := iblt.New(IBLT_DEFAULT_CELLS)

	// Common items.
	for i := 0; i < numCommon; i++ {
		it := item(fmt.Sprintf("common-%03d", i))
		a.Insert(it)
		b.Insert(it)
	}
	// Items only in A.
	onlyA := make([][]byte, numUniqueA)
	for i := 0; i < numUniqueA; i++ {
		onlyA[i] = item(fmt.Sprintf("only-in-a-%03d", i))
		a.Insert(onlyA[i])
	}
	// Items only in B.
	onlyB := make([][]byte, numUniqueB)
	for i := 0; i < numUniqueB; i++ {
		onlyB[i] = item(fmt.Sprintf("only-in-b-%03d", i))
		b.Insert(onlyB[i])
	}

	diff := a.SubtractUnsafe(b)
	gotA, gotB, ok := diff.Decode()
	if !ok {
		t.Fatal("Decode failed; expected success for small difference")
	}

	if len(gotA) != numUniqueA {
		t.Errorf("onlyInA: got %d items, want %d", len(gotA), numUniqueA)
	}
	if len(gotB) != numUniqueB {
		t.Errorf("onlyInB: got %d items, want %d", len(gotB), numUniqueB)
	}

	// Build sets for comparison.
	setA := make(map[string]bool)
	for _, it := range gotA {
		setA[string(it)] = true
	}
	setB := make(map[string]bool)
	for _, it := range gotB {
		setB[string(it)] = true
	}
	for _, it := range onlyA {
		if !setA[string(it)] {
			t.Errorf("expected %q in onlyInA but not found", it)
		}
	}
	for _, it := range onlyB {
		if !setB[string(it)] {
			t.Errorf("expected %q in onlyInB but not found", it)
		}
	}
}

// TestDecodeSmallSucceedsLargeFails verifies capacity boundaries.
// With 512 cells and k=3, the IBLT reliably decodes diffs of up to ~40 items.
// Larger diffs fall back to full-state sync (FullStateSync RPC).
func TestDecodeSmallSucceedsLargeFails(t *testing.T) {
	// Small N = 40 reliably decodes with 512 cells + k=3.
	a := iblt.New(IBLT_DEFAULT_CELLS)
	for i := 0; i < 40; i++ {
		a.Insert(item(fmt.Sprintf("item-%d", i)))
	}
	empty := iblt.New(IBLT_DEFAULT_CELLS)
	diff := a.SubtractUnsafe(empty)
	_, _, ok := diff.Decode()
	if !ok {
		t.Error("expected Decode success for 40-item difference, got failure")
	}

	// Large N = 450 reliably fails (well beyond capacity of ~55 items).
	big := iblt.New(IBLT_DEFAULT_CELLS)
	for i := 0; i < 450; i++ {
		big.Insert(item(fmt.Sprintf("big-item-%d", i)))
	}
	diff2 := big.SubtractUnsafe(iblt.New(IBLT_DEFAULT_CELLS))
	_, _, ok2 := diff2.Decode()
	if ok2 {
		t.Error("expected Decode failure for 450-item difference, got success")
	}
}

// TestEmptyXorEmpty verifies that subtracting two empty IBLTs decodes to empty sets.
func TestEmptyXorEmpty(t *testing.T) {
	a := iblt.New(IBLT_DEFAULT_CELLS)
	b := iblt.New(IBLT_DEFAULT_CELLS)
	diff := a.SubtractUnsafe(b)
	onlyA, onlyB, ok := diff.Decode()
	if !ok {
		t.Fatal("Decode of empty XOR empty failed")
	}
	if len(onlyA) != 0 || len(onlyB) != 0 {
		t.Errorf("expected empty sets, got onlyA=%d onlyB=%d", len(onlyA), len(onlyB))
	}
}

// TestInsertDeleteZero verifies that after Insert then Delete of the same item,
// the IBLT equals the zero IBLT (all cells zero).
func TestInsertDeleteZero(t *testing.T) {
	t1 := iblt.New(IBLT_DEFAULT_CELLS)
	it := item("hello-world")
	t1.Insert(it)
	t1.Delete(it)

	zero := iblt.New(IBLT_DEFAULT_CELLS) // all cells zero
	diff := t1.SubtractUnsafe(zero)
	onlyA, onlyB, ok := diff.Decode()
	if !ok {
		t.Fatal("Decode failed after Insert+Delete")
	}
	if len(onlyA) != 0 || len(onlyB) != 0 {
		t.Errorf("expected empty after insert+delete, got onlyA=%d onlyB=%d", len(onlyA), len(onlyB))
	}
}

// TestIdempotence verifies that inserting the same item twice then deleting
// it once leaves it in the set.
func TestIdempotence(t *testing.T) {
	t1 := iblt.New(IBLT_DEFAULT_CELLS)
	it := item("idempotent-key")
	t1.Insert(it)
	t1.Insert(it) // insert twice
	t1.Delete(it) // delete once → should still be present once

	empty := iblt.New(IBLT_DEFAULT_CELLS)
	diff := t1.SubtractUnsafe(empty)
	onlyA, _, ok := diff.Decode()
	if !ok {
		t.Fatal("Decode failed in idempotence test")
	}
	if len(onlyA) != 1 {
		t.Errorf("expected 1 item in onlyA (item still present), got %d", len(onlyA))
	}
}

// TestEncodeDecodeRoundTrip verifies wire format round-trip.
func TestEncodeDecodeRoundTrip(t *testing.T) {
	original := iblt.New(IBLT_DEFAULT_CELLS)
	for i := 0; i < 50; i++ {
		original.Insert(item(fmt.Sprintf("roundtrip-item-%d", i)))
	}

	encoded := original.Encode()
	decoded, err := iblt.DecodeIBLT(encoded)
	if err != nil {
		t.Fatalf("DecodeIBLT error: %v", err)
	}

	// The decoded IBLT should have the same cells.
	if decoded.NumCells != original.NumCells {
		t.Errorf("NumCells mismatch: got %d, want %d", decoded.NumCells, original.NumCells)
	}
	for i := range original.Cells {
		if original.Cells[i].Count != decoded.Cells[i].Count {
			t.Errorf("cell %d Count mismatch", i)
		}
		if original.Cells[i].HashSum != decoded.Cells[i].HashSum {
			t.Errorf("cell %d HashSum mismatch", i)
		}
		if !bytes.Equal(original.Cells[i].KeySum, decoded.Cells[i].KeySum) {
			t.Errorf("cell %d KeySum mismatch", i)
		}
	}
}
