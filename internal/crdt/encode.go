package crdt

import (
	"encoding/binary"
	"sort"
)

// Canonical encodings are deterministic byte representations (sorted keys,
// fixed-width integers, big-endian). They exist for hashing — the Merkle
// leaf hash is computed over the context encoding — and for byte-level
// equality in tests. They are not the storage or wire format (that is
// protobuf, in internal/codec).

// AppendCanonical appends the canonical encoding of the context:
//
//	uint32 #vv  ‖ (actor[16] ‖ seq[8])* sorted by actor
//	uint32 #cloud ‖ (actor[16] ‖ seq[8])* sorted by (actor, seq)
func (c *CausalContext) AppendCanonical(b []byte) []byte {
	actors := make([]ActorID, 0, len(c.VV))
	for a := range c.VV {
		actors = append(actors, a)
	}
	sort.Slice(actors, func(i, j int) bool { return less16(actors[i], actors[j]) })
	b = binary.BigEndian.AppendUint32(b, uint32(len(actors)))
	for _, a := range actors {
		b = append(b, a[:]...)
		b = binary.BigEndian.AppendUint64(b, c.VV[a])
	}

	dots := make([]Dot, 0, len(c.Cloud))
	for d := range c.Cloud {
		dots = append(dots, d)
	}
	sort.Slice(dots, func(i, j int) bool { return lessDot(dots[i], dots[j]) })
	b = binary.BigEndian.AppendUint32(b, uint32(len(dots)))
	for _, d := range dots {
		b = append(b, d.Actor[:]...)
		b = binary.BigEndian.AppendUint64(b, d.Seq)
	}
	return b
}

func (c *CausalContext) Canonical() []byte {
	return c.AppendCanonical(nil)
}

// AppendCanonical appends the canonical encoding of the whole document:
//
//	uint32 #fields ‖ (uint32 len ‖ name ‖ uint32 #regs ‖
//	                  (actor[16] ‖ seq[8] ‖ hlc[8] ‖ uint32 len ‖ value)*
//	                  sorted by dot)* sorted by name
//	context canonical encoding
func (d *Document) AppendCanonical(b []byte) []byte {
	names := make([]string, 0, len(d.Fields))
	for f := range d.Fields {
		names = append(names, f)
	}
	sort.Strings(names)
	b = binary.BigEndian.AppendUint32(b, uint32(len(names)))
	for _, f := range names {
		regs := d.Fields[f]
		b = binary.BigEndian.AppendUint32(b, uint32(len(f)))
		b = append(b, f...)
		b = binary.BigEndian.AppendUint32(b, uint32(len(regs)))
		for _, r := range regs { // already sorted by dot (merge invariant)
			b = append(b, r.Dot.Actor[:]...)
			b = binary.BigEndian.AppendUint64(b, r.Dot.Seq)
			b = binary.BigEndian.AppendUint64(b, r.HLC)
			b = binary.BigEndian.AppendUint32(b, uint32(len(r.Value)))
			b = append(b, r.Value...)
		}
	}
	return d.Context.AppendCanonical(b)
}

func (d *Document) Canonical() []byte {
	return d.AppendCanonical(nil)
}

func less16(a, b ActorID) bool {
	for i := range a {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return false
}

func lessDot(a, b Dot) bool {
	if a.Actor != b.Actor {
		return less16(a.Actor, b.Actor)
	}
	return a.Seq < b.Seq
}
