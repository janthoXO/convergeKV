package crdt

import "sort"

// Document is the CRDT state for one key: top-level fields plus the causal
// context of every event ever observed. Empty Fields with a non-empty
// Context means the document is deleted (a residual context, GC'd later).
//
// Spec correction (to sections 2.1–2.3): each field holds the SET of
// concurrent registers (a multi-value leaf), not a single one. Collapsing
// concurrency to one register at merge time via LWW is not convergent under
// op-delta delivery — an LWW loser's dot is recorded only in the origin's
// full context, which deltas never carry, so a replica that adopts the loser
// after seeing the field's removal keeps it forever (found by
// TestPropertyConvergence). Instead the dot store keeps every concurrent
// register, merge is the standard dotted-store join (provably a lattice),
// and LWW arbitration happens at read time via Get. Client-visible semantics
// are exactly the spec's: one whole winner per field, no partial mixing.
// Any subsequent Put covers the whole set, collapsing it again.
//
// Invariant: for every register r in Fields, Context.Contains(r.Dot).
type Document struct {
	// which data the node shows to the client (multiple cause after a merge others might be deleted)
	Fields  map[string][]Register // concurrent registers, sorted by dot
	// which events the node has seen
	Context CausalContext
}

func NewDocument() *Document {
	return &Document{
		Fields:  make(map[string][]Register),
		Context: NewContext(),
	}
}

func (d *Document) ensure() {
	if d.Fields == nil {
		d.Fields = make(map[string][]Register)
	}
	d.Context.ensure()
}

// Get returns the field's current value: the LWW winner (HLC, then actor
// bytes, then seq) among the concurrent registers.
func (d *Document) Get(field string) (Register, bool) {
	regs := d.Fields[field]
	if len(regs) == 0 {
		return Register{}, false
	}
	win := regs[0]
	for _, r := range regs[1:] {
		if r.supersedes(win) {
			win = r
		}
	}
	return win, true
}

// Deleted reports whether the document holds no fields but has observed
// events (i.e. reads as "not found").
func (d *Document) Deleted() bool {
	return len(d.Fields) == 0 && !d.Context.Empty()
}

func (d *Document) Clone() *Document {
	out := NewDocument()
	for f, regs := range d.Fields {
		cp := make([]Register, len(regs))
		copy(cp, regs)
		out.Fields[f] = cp
	}
	out.Context = d.Context.Clone()
	return out
}

// --- Local operations (applier only, under the partition lock) -------------
//
// Each returns the delta document to replicate to the other owners.

// Put assigns value to one field using a freshly minted dot. The new register
// replaces every register currently in the field's set; the delta context
// carries their dots plus the new one, which is what lets every peer collapse
// the same set.
func (d *Document) Put(field string, value []byte, dot Dot, ts HLC) *Document {
	d.ensure()
	delta := NewDocument()
	for _, old := range d.Fields[field] {
		delta.Context.Add(old.Dot)
	}
	reg := Register{Dot: dot, HLC: ts, Value: value}
	d.Fields[field] = []Register{reg}
	d.Context.Add(dot)

	delta.Fields[field] = []Register{reg}
	delta.Context.Add(dot)
	return delta
}

// PutMulti applies one client write touching several fields: one freshly
// minted dot per mutated field (fields are removed independently later, and
// removal is communicated per dot), one shared HLC, one combined delta.
func (d *Document) PutMulti(fields map[string][]byte, mint func() Dot, ts HLC) *Document {
	names := make([]string, 0, len(fields))
	for f := range fields {
		names = append(names, f)
	}
	sort.Strings(names)

	delta := NewDocument()
	for _, f := range names {
		delta.Merge(d.Put(f, fields[f], mint(), ts))
	}
	return delta
}

// RemoveField deletes one field. The removed registers' dots MUST be in the
// delta context — that is what communicates the removal to peers.
func (d *Document) RemoveField(field string, dot Dot) *Document {
	d.ensure()
	delta := NewDocument()
	for _, old := range d.Fields[field] {
		delta.Context.Add(old.Dot)
	}
	delete(d.Fields, field)
	d.Context.Add(dot)
	delta.Context.Add(dot)
	return delta
}

// Delete removes the whole document. The delta context carries the dots of
// all removed registers plus the delete event itself; the local state keeps
// its full context as a residual (GC'd after clean anti-entropy rounds).
func (d *Document) Delete(dot Dot) *Document {
	d.ensure()
	delta := NewDocument()
	for _, regs := range d.Fields {
		for _, reg := range regs {
			delta.Context.Add(reg.Dot)
		}
	}
	clear(d.Fields)
	d.Context.Add(dot)
	delta.Context.Add(dot)
	return delta
}

// --- Merge (the join) -------------------------------------------------------

// Merge folds remote into d: the standard dotted-store join. A register
// survives iff it is present on both sides, or present on one side and not
// covered by the other side's causal context. Commutative, associative,
// idempotent (property-tested). remote is not modified.
func (d *Document) Merge(remote *Document) {
	d.ensure()

	for f := range remote.Fields {
		if merged := mergeField(d.Fields[f], &d.Context, remote.Fields[f], &remote.Context); len(merged) > 0 {
			d.Fields[f] = merged
		} else {
			delete(d.Fields, f)
		}
	}
	for f, regs := range d.Fields {
		if _, inRemote := remote.Fields[f]; inRemote {
			continue // handled above
		}
		if merged := mergeField(regs, &d.Context, nil, &remote.Context); len(merged) > 0 {
			d.Fields[f] = merged
		} else {
			delete(d.Fields, f)
		}
	}

	d.Context.UnionWith(remote.Context)
}

func mergeField(local []Register, localCtx *CausalContext, remote []Register, remoteCtx *CausalContext) []Register {
	out := make([]Register, 0, len(local)+len(remote))
	for _, l := range local {
		if hasDot(remote, l.Dot) || !remoteCtx.Contains(l.Dot) {
			out = append(out, l)
		}
	}
	for _, r := range remote {
		if !hasDot(local, r.Dot) && !localCtx.Contains(r.Dot) {
			out = append(out, r)
		}
	}
	if len(out) == 0 {
		return nil
	}
	sort.Slice(out, func(i, j int) bool { return lessDot(out[i].Dot, out[j].Dot) })
	return out
}

func hasDot(regs []Register, d Dot) bool {
	for _, r := range regs {
		if r.Dot == d {
			return true
		}
	}
	return false
}
