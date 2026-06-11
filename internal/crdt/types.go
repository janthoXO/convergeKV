// Package crdt implements the causal CRDT document model: per-document
// causal contexts (dots), an OR-Map of top-level fields, and LWW-over-HLC
// registers at the leaves. Deltas and merge follow sections 2.2 and 2.3 of
// docs/IMPLEMENTATION_PLAN.md exactly.
//
// This package is pure: stdlib imports only, no I/O.
package crdt

import "bytes"

// ActorID identifies a replica (the node UUID's raw bytes).
type ActorID [16]byte

// HLC is a packed hybrid logical clock timestamp
// (48-bit physical ms << 16 | 16-bit logical).
type HLC = uint64

// Dot is a globally unique write event: the Seq'th event minted by Actor.
type Dot struct {
	Actor ActorID
	Seq   uint64
}

// Register holds one top-level field's current value.
type Register struct {
	Dot   Dot    // the write event that produced this value
	HLC   HLC    // assignment timestamp for LWW arbitration
	Value []byte // opaque encoded scalar, array, or nested object
}

// supersedes reports whether r wins LWW arbitration against o under true
// concurrency: higher HLC, then bytes-compare of Actor, then Seq as a final
// total-order safety net.
func (r Register) supersedes(o Register) bool {
	if r.HLC != o.HLC {
		return r.HLC > o.HLC
	}
	if c := bytes.Compare(r.Dot.Actor[:], o.Dot.Actor[:]); c != 0 {
		return c > 0
	}
	return r.Dot.Seq > o.Dot.Seq
}

// Minter mints dots for one actor. Persisting Seq across restarts (so a dot
// is never reused) is the caller's responsibility.
type Minter struct {
	Actor ActorID
	Seq   uint64 // last minted sequence number
}

func (m *Minter) Next() Dot {
	m.Seq++
	return Dot{Actor: m.Actor, Seq: m.Seq}
}
