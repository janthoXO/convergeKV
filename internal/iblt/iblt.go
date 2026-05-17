// Package iblt implements a general-purpose Invertible Bloom Lookup Table.
// The caller provides items as byte slices; the IBLT does not interpret content.
// With the default 512 cells and k=3 hash functions, the IBLT can reliably
// decode symmetric differences of up to ~250 items with >99% probability.
package iblt

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"sync"
)

const (
	// Larger differences trigger the full-state fallback (FullStateSync RPC).
	// numHashFuncs is fixed at 3, the standard choice.
	numHashFuncs = 3
)

// Cell is a single cell in the IBLT.
type Cell struct {
	Count   int64
	KeySum  []byte
	HashSum uint64
}

// IBLT is an Invertible Bloom Lookup Table.
type IBLT struct {
	Cells    []Cell
	NumCells int
	mu       []sync.Mutex // one per cell
	globalMu sync.RWMutex // for operations that need to lock the whole table
}

// New constructs an empty IBLT with the given number of cells.
// All cells start zeroed. KeySum is nil to represent the empty XOR identity.
func New(numCells int) *IBLT {
	return &IBLT{
		Cells:    make([]Cell, numCells),
		NumCells: numCells,
		mu:       make([]sync.Mutex, numCells),
	}
}

// hashItem returns the uint64 fingerprint of an item used in HashSum.
func hashItem(item []byte) uint64 {
	h := sha256.Sum256(item)
	return binary.BigEndian.Uint64(h[:8])
}

// cellIndex returns the i-th (0-indexed) cell index for item.
// Uses SHA-256(seed_byte || item) to derive a stable, independent index.
func cellIndex(item []byte, funcIdx int, numCells int) int {
	h := sha256.New()
	h.Write([]byte{byte(funcIdx)})
	h.Write(item)
	sum := h.Sum(nil)
	idx := binary.BigEndian.Uint64(sum[:8])
	return int(idx % uint64(numCells))
}

// cellIndices returns the deduplicated cell indices for item across all hash functions.
// Duplicates occur when two hash functions map to the same cell; applying the same
// cell twice corrupts Count and cancels the XOR.
func cellIndices(item []byte, numCells int) []int {
	out := make([]int, 0, numHashFuncs)

	for i := range numHashFuncs {
		idx := cellIndex(item, i, numCells)
		if !slices.Contains(out, idx) {
			out = append(out, idx)
		}
	}

	return out
}

// lenPrefixed returns a new byte slice with a 4-byte big-endian length prefix followed by the item bytes.
func lenPrefixed(item []byte) []byte {
	buf := make([]byte, 4+len(item))
	binary.BigEndian.PutUint32(buf, uint32(len(item)))
	copy(buf[4:], item)
	return buf
}

// Insert adds item to the IBLT.
func (t *IBLT) Insert(item []byte) {
	hv := hashItem(item)
	lp := lenPrefixed(item)

	t.globalMu.RLock()
	defer t.globalMu.RUnlock()

	for _, idx := range cellIndices(item, t.NumCells) {
		t.mu[idx].Lock()
		t.Cells[idx].Count++
		t.Cells[idx].KeySum = xorBytes(t.Cells[idx].KeySum, lp)
		t.Cells[idx].HashSum ^= hv
		t.mu[idx].Unlock()
	}
}

func (t *IBLT) insertUnsafe(item []byte) {
	hv := hashItem(item)
	lp := lenPrefixed(item)

	for _, idx := range cellIndices(item, t.NumCells) {
		t.Cells[idx].Count++
		t.Cells[idx].KeySum = xorBytes(t.Cells[idx].KeySum, lp)
		t.Cells[idx].HashSum ^= hv
	}
}

// Delete removes item from the IBLT.
// Equivalent to Insert but decrements Count (XOR is its own inverse).
func (t *IBLT) Delete(item []byte) {
	hv := hashItem(item)
	lp := lenPrefixed(item)

	t.globalMu.RLock()
	defer t.globalMu.RUnlock()

	for _, idx := range cellIndices(item, t.NumCells) {
		t.mu[idx].Lock()
		t.Cells[idx].Count--
		t.Cells[idx].KeySum = xorBytes(t.Cells[idx].KeySum, lp)
		t.Cells[idx].HashSum ^= hv
		t.mu[idx].Unlock()
	}
}

func (t *IBLT) deleteUnsafe(item []byte) {
	hv := hashItem(item)
	lp := lenPrefixed(item)

	for _, idx := range cellIndices(item, t.NumCells) {
		t.Cells[idx].Count--
		t.Cells[idx].KeySum = xorBytes(t.Cells[idx].KeySum, lp)
		t.Cells[idx].HashSum ^= hv
	}
}

// SubtractUnsafe returns a new IBLT equal to (t − other), representing the
// symmetric difference of the two underlying sets. Each cell is computed as:
//
//	result.Count   = t.Count   - other.Count
//	result.KeySum  = t.KeySum  XOR other.KeySum
//	result.HashSum = t.HashSum XOR other.HashSum
func (t *IBLT) SubtractUnsafe(other *IBLT) *IBLT {
	if t.NumCells != other.NumCells {
		panic(fmt.Sprintf("iblt: Subtract called on IBLTs with different sizes (%d vs %d)", t.NumCells, other.NumCells))
	}
	result := New(t.NumCells)
	for i := range t.Cells {
		result.Cells[i].Count = t.Cells[i].Count - other.Cells[i].Count
		result.Cells[i].KeySum = xorBytes(t.Cells[i].KeySum, other.Cells[i].KeySum)
		result.Cells[i].HashSum = t.Cells[i].HashSum ^ other.Cells[i].HashSum
	}
	return result
}

// isAllZero returns true if b is nil or contains only zero bytes.
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
		// defense against malformed KeySum
		return nil, false
	}

	l := int(binary.BigEndian.Uint32(b[:4]))
	if 4+l > len(b) {
		return nil, false
	}

	return b[4 : 4+l], true
}

// isPure returns true if cell c contains exactly one item (|Count|==1 and
// the hash of the KeySum matches the HashSum).
func isPure(c *Cell) bool {
	if c.Count != 1 && c.Count != -1 {
		return false
	}

	// An all-zero or empty KeySum with zero HashSum means an empty cell — not pure.
	if c.HashSum == 0 && isAllZero(c.KeySum) {
		return false
	}

	item, ok := stripLenPrefix(c.KeySum)
	if !ok {
		return false
	}
	return hashItem(item) == c.HashSum
}

// Decode attempts to peel the IBLT to recover the symmetric difference.
// Returns:
//
//	onlyInA — items present in A but not B  (Count==+1 cells)
//	onlyInB — items present in B but not A  (Count==-1 cells)
//	ok      — whether decoding completed successfully
//
// If ok is false, the symmetric difference is too large to decode and the
// caller should fall back to a full-state exchange.
func (t *IBLT) Decode() (onlyInA [][]byte, onlyInB [][]byte, ok bool) {
	// Snapshot acquires the global lock, ensuring we read a consistent state.
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
			item = append([]byte(nil), item...) // copy to avoid aliasing

			isInA := c.Count == 1

			if isInA {
				onlyInA = append(onlyInA, item)
			} else {
				onlyInB = append(onlyInB, item)
			}

			// Peel this item from all its cells.
			if isInA {
				work.deleteUnsafe(item)
			} else {
				work.insertUnsafe(item) // reverses the subtraction effect
			}
			progress = true
		}

		if !progress {
			break
		}
	}

	// Check if all cells are zero.
	for i := range work.Cells {
		c := &work.Cells[i]
		if c.Count != 0 || c.HashSum != 0 || !isAllZero(c.KeySum) {
			return nil, nil, false
		}
	}
	return onlyInA, onlyInB, true
}

// Encode serialises the IBLT to a byte slice for transmission.
// Wire format: [numCells uint32] then for each cell:
//
//	[Count int64] [HashSum uint64] [KeySumLen uint32] [KeySum bytes]
func (t *IBLT) Encode() []byte {
	cp := t.Snapshot()

	// Estimate size: 4 + numCells * (8 + 8 + 4 + avg_key_len)
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

// clone returns a deep copy of the IBLT.
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

func (t *IBLT) Snapshot() *IBLT {
	t.globalMu.Lock()
	defer t.globalMu.Unlock()

	return t.clone()
}

// xorBytes returns a XOR b, padding the shorter slice with zeros.
// The result has length max(len(a), len(b)).
// The result is never nil — an empty/cancelled cell is represented as all-zero bytes.
func xorBytes(a, b []byte) []byte {
	// Normalise so a is the longer slice.
	if len(b) > len(a) {
		a, b = b, a
	}

	if len(a) == 0 {
		// Both operands are empty/nil.
		return nil
	}

	out := make([]byte, len(a))
	copy(out, a)
	
	for i := 0; i < len(b); i++ {
		out[i] ^= b[i]
	}

	return out
}
