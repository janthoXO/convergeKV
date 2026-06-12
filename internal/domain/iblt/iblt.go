// Package iblt implements a general-purpose Invertible Bloom Lookup Table.
// The caller provides items as byte slices; the IBLT does not interpret content.
// With the default 512 cells and k=3 hash functions, the IBLT can reliably
// decode symmetric differences of up to ~250 items with >99% probability.
package iblt

import (
	"encoding/binary"
	"errors"
	"fmt"
	"slices"

	"github.com/cespare/xxhash/v2"
)

const numHashFuncs = 3

// Cell is a single cell in the IBLT.
type Cell struct {
	Count   int64
	KeySum  []byte
	HashSum uint64
}

// IBLT is an Invertible Bloom Lookup Table.
//
// IBLT is a pure domain type and is NOT safe for concurrent use. Callers must
// provide their own synchronization; in this codebase that's the owning
// partition's mutex (internal/core/replica), which already serializes every
// mutation alongside the corresponding store write.
type IBLT struct {
	Cells    []Cell
	NumCells int
}

// New constructs an empty IBLT with the given number of cells.
func New(numCells int) *IBLT {
	return &IBLT{
		Cells:    make([]Cell, numCells),
		NumCells: numCells,
	}
}

// itemHashes returns the fingerprint and exactly numHashFuncs deduplicated cell
// indices for item using enhanced double-hashing.
func itemHashes(item []byte, numCells int) (fingerprint uint64, indices [numHashFuncs]int) {
	h1 := xxhash.Sum64(item)

	var d xxhash.Digest
	d.Reset()
	d.Write([]byte{0xff})
	d.Write(item)
	h2 := d.Sum64()
	h2 |= 1

	fingerprint = h1
	count := 0
	for i := uint64(0); count < numHashFuncs; i++ {
		idx := int((h1 + i*h2) % uint64(numCells))
		if !slices.Contains(indices[:count], idx) {
			indices[count] = idx
			count++
		}
	}
	return
}

func lenPrefixed(item []byte) []byte {
	buf := make([]byte, 4+len(item))
	binary.BigEndian.PutUint32(buf, uint32(len(item)))
	copy(buf[4:], item)
	return buf
}

// Insert adds item to the IBLT.
func (t *IBLT) Insert(item []byte) {
	hv, indices := itemHashes(item, t.NumCells)
	lp := lenPrefixed(item)
	for _, idx := range indices {
		t.Cells[idx].Count++
		t.Cells[idx].KeySum = xorBytes(t.Cells[idx].KeySum, lp)
		t.Cells[idx].HashSum ^= hv
	}
}

// Delete removes item from the IBLT.
func (t *IBLT) Delete(item []byte) {
	hv, indices := itemHashes(item, t.NumCells)
	lp := lenPrefixed(item)
	for _, idx := range indices {
		t.Cells[idx].Count--
		t.Cells[idx].KeySum = xorBytes(t.Cells[idx].KeySum, lp)
		t.Cells[idx].HashSum ^= hv
	}
}

// ErrSizeMismatch is returned by Subtract when the two IBLTs have different
// cell counts — typically a sign that a peer is misconfigured with a
// different IBLT_CELLS value.
var ErrSizeMismatch = errors.New("iblt: subtract called on IBLTs with different sizes")

// Subtract returns a new IBLT equal to (t − other). It returns
// ErrSizeMismatch if the two IBLTs have different cell counts; this can
// happen when a peer is configured with a different IBLT_CELLS value.
func (t *IBLT) Subtract(other *IBLT) (*IBLT, error) {
	if t.NumCells != other.NumCells {
		return nil, fmt.Errorf("%w (%d vs %d)", ErrSizeMismatch, t.NumCells, other.NumCells)
	}
	result := New(t.NumCells)
	for i := range t.Cells {
		result.Cells[i].Count = t.Cells[i].Count - other.Cells[i].Count
		result.Cells[i].KeySum = xorBytes(t.Cells[i].KeySum, other.Cells[i].KeySum)
		result.Cells[i].HashSum = t.Cells[i].HashSum ^ other.Cells[i].HashSum
	}
	return result, nil
}

func isAllZero(b []byte) bool {
	for _, v := range b {
		if v != 0 {
			return false
		}
	}
	return true
}

func stripLenPrefix(b []byte) ([]byte, bool) {
	if len(b) < 4 {
		return nil, false
	}
	l := int(binary.BigEndian.Uint32(b[:4]))
	if 4+l > len(b) {
		return nil, false
	}
	return b[4 : 4+l], true
}

func isPure(c *Cell) bool {
	if c.Count != 1 && c.Count != -1 {
		return false
	}
	if c.HashSum == 0 && isAllZero(c.KeySum) {
		return false
	}
	item, ok := stripLenPrefix(c.KeySum)
	if !ok {
		return false
	}
	return xxhash.Sum64(item) == c.HashSum
}

// Decode attempts to peel the IBLT to recover the symmetric difference.
func (t *IBLT) Decode() (onlyInA [][]byte, onlyInB [][]byte, ok bool) {
	work := t.Snapshot()

	for {
		progress := false
		for i := range work.Cells {
			c := &work.Cells[i]
			if !isPure(c) {
				continue
			}
			item, ok := stripLenPrefix(c.KeySum)
			if !ok {
				continue
			}
			item = append([]byte(nil), item...)

			isInA := c.Count == 1
			if isInA {
				onlyInA = append(onlyInA, item)
			} else {
				onlyInB = append(onlyInB, item)
			}
			if isInA {
				work.Delete(item)
			} else {
				work.Insert(item)
			}
			progress = true
		}
		if !progress {
			break
		}
	}

	for i := range work.Cells {
		c := &work.Cells[i]
		if c.Count != 0 || c.HashSum != 0 || !isAllZero(c.KeySum) {
			return nil, nil, false
		}
	}
	return onlyInA, onlyInB, true
}

// Encode serialises the IBLT to a byte slice for transmission.
func (t *IBLT) Encode() []byte {
	cp := t.Snapshot()
	buf := make([]byte, 0, 4+t.NumCells*32)
	buf = binary.BigEndian.AppendUint32(buf, uint32(cp.NumCells))
	for i := range cp.Cells {
		c := &cp.Cells[i]
		buf = binary.BigEndian.AppendUint64(buf, uint64(c.Count))
		buf = binary.BigEndian.AppendUint64(buf, c.HashSum)
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(c.KeySum)))
		buf = append(buf, c.KeySum...)
	}
	return buf
}

// DecodeIBLT deserialises an IBLT from the wire format produced by Encode.
func DecodeIBLT(b []byte) (*IBLT, error) {
	if len(b) < 4 {
		return nil, errors.New("iblt: too short to decode")
	}
	numCells := int(binary.BigEndian.Uint32(b[:4]))
	if numCells <= 0 || numCells > 1<<20 {
		return nil, fmt.Errorf("iblt: invalid cell count %d", numCells)
	}

	t := New(numCells)
	pos := 4
	for i := range numCells {
		if pos+20 > len(b) {
			return nil, fmt.Errorf("iblt: truncated at cell %d", i)
		}
		count := int64(binary.BigEndian.Uint64(b[pos:]))
		pos += 8
		hashSum := binary.BigEndian.Uint64(b[pos:])
		pos += 8
		keyLen := int(binary.BigEndian.Uint32(b[pos:]))
		pos += 4
		if pos+keyLen > len(b) {
			return nil, fmt.Errorf("iblt: truncated KeySum at cell %d", i)
		}
		keySum := make([]byte, keyLen)
		copy(keySum, b[pos:pos+keyLen])
		pos += keyLen
		t.Cells[i].Count = count
		t.Cells[i].HashSum = hashSum
		t.Cells[i].KeySum = keySum
	}
	return t, nil
}

func (t *IBLT) clone() *IBLT {
	c := New(t.NumCells)
	for i := range t.Cells {
		c.Cells[i].Count = t.Cells[i].Count
		c.Cells[i].HashSum = t.Cells[i].HashSum
		if len(t.Cells[i].KeySum) > 0 {
			c.Cells[i].KeySum = make([]byte, len(t.Cells[i].KeySum))
			copy(c.Cells[i].KeySum, t.Cells[i].KeySum)
		}
	}
	return c
}

// Snapshot returns a deep copy of the IBLT. Callers needing a consistent
// snapshot against concurrent mutation must hold the same lock that guards
// Insert/Delete (see the IBLT doc comment).
func (t *IBLT) Snapshot() *IBLT {
	return t.clone()
}

func xorBytes(a, b []byte) []byte {
	if len(b) > len(a) {
		a, b = b, a
	}
	if len(a) == 0 {
		return nil
	}
	out := make([]byte, len(a))
	copy(out, a)
	for i := 0; i < len(b); i++ {
		out[i] ^= b[i]
	}
	return out
}
