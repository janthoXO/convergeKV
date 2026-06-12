package iblt_test

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/janthoXO/convergeKV/internal/domain/iblt"
)

const ibltDefaultCells = 512

func item(s string) []byte { return []byte(s) }

func TestSymmetricDifference(t *testing.T) {
	const numCommon = 90
	const numUniqueA = 10
	const numUniqueB = 10

	a := iblt.New(ibltDefaultCells)
	b := iblt.New(ibltDefaultCells)

	for i := range numCommon {
		it := item(fmt.Sprintf("common-%03d", i))
		a.Insert(it)
		b.Insert(it)
	}
	onlyA := make([][]byte, numUniqueA)
	for i := range numUniqueA {
		onlyA[i] = item(fmt.Sprintf("only-in-a-%03d", i))
		a.Insert(onlyA[i])
	}
	onlyB := make([][]byte, numUniqueB)
	for i := range numUniqueB {
		onlyB[i] = item(fmt.Sprintf("only-in-b-%03d", i))
		b.Insert(onlyB[i])
	}

	diff, err := a.Subtract(b)
	if err != nil {
		t.Fatalf("Subtract: %v", err)
	}
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

func TestDecodeSmallSucceedsLargeFails(t *testing.T) {
	a := iblt.New(ibltDefaultCells)
	for i := range 40 {
		a.Insert(item(fmt.Sprintf("item-%d", i)))
	}
	empty := iblt.New(ibltDefaultCells)
	diff, err := a.Subtract(empty)
	if err != nil {
		t.Fatalf("Subtract: %v", err)
	}
	_, _, ok := diff.Decode()
	if !ok {
		t.Error("expected Decode success for 40-item difference, got failure")
	}

	big := iblt.New(ibltDefaultCells)
	for i := range 450 {
		big.Insert(item(fmt.Sprintf("big-item-%d", i)))
	}
	diff2, err := big.Subtract(iblt.New(ibltDefaultCells))
	if err != nil {
		t.Fatalf("Subtract: %v", err)
	}
	_, _, ok2 := diff2.Decode()
	if ok2 {
		t.Error("expected Decode failure for 450-item difference, got success")
	}
}

func TestEmptyXorEmpty(t *testing.T) {
	a := iblt.New(ibltDefaultCells)
	b := iblt.New(ibltDefaultCells)
	diff, err := a.Subtract(b)
	if err != nil {
		t.Fatalf("Subtract: %v", err)
	}
	onlyA, onlyB, ok := diff.Decode()
	if !ok {
		t.Fatal("Decode of empty XOR empty failed")
	}
	if len(onlyA) != 0 || len(onlyB) != 0 {
		t.Errorf("expected empty sets, got onlyA=%d onlyB=%d", len(onlyA), len(onlyB))
	}
}

func TestSubtractSizeMismatch(t *testing.T) {
	a := iblt.New(ibltDefaultCells)
	b := iblt.New(ibltDefaultCells * 2)

	if _, err := a.Subtract(b); !errors.Is(err, iblt.ErrSizeMismatch) {
		t.Fatalf("Subtract: got err=%v, want ErrSizeMismatch", err)
	}
}

func TestInsertDeleteZero(t *testing.T) {
	t1 := iblt.New(ibltDefaultCells)
	it := item("hello-world")
	t1.Insert(it)
	t1.Delete(it)

	zero := iblt.New(ibltDefaultCells)
	diff, err := t1.Subtract(zero)
	if err != nil {
		t.Fatalf("Subtract: %v", err)
	}
	onlyA, onlyB, ok := diff.Decode()
	if !ok {
		t.Fatal("Decode failed after Insert+Delete")
	}
	if len(onlyA) != 0 || len(onlyB) != 0 {
		t.Errorf("expected empty after insert+delete, got onlyA=%d onlyB=%d", len(onlyA), len(onlyB))
	}
}

func TestIdempotence(t *testing.T) {
	t1 := iblt.New(ibltDefaultCells)
	it := item("idempotent-key")
	t1.Insert(it)
	t1.Insert(it)
	t1.Delete(it)

	empty := iblt.New(ibltDefaultCells)
	diff, err := t1.Subtract(empty)
	if err != nil {
		t.Fatalf("Subtract: %v", err)
	}
	onlyA, _, ok := diff.Decode()
	if !ok {
		t.Fatal("Decode failed in idempotence test")
	}
	if len(onlyA) != 1 {
		t.Errorf("expected 1 item in onlyA (item still present), got %d", len(onlyA))
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	original := iblt.New(ibltDefaultCells)
	for i := range 50 {
		original.Insert(item(fmt.Sprintf("roundtrip-item-%d", i)))
	}

	encoded := original.Encode()
	decoded, err := iblt.DecodeIBLT(encoded)
	if err != nil {
		t.Fatalf("DecodeIBLT error: %v", err)
	}

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
