package crdt

// CausalContext records every write event this replica has observed for a
// document: a version vector for the contiguous prefix per actor, plus a
// cloud of out-of-order dots that compacts into the VV as gaps fill.
type CausalContext struct {
	VV    map[ActorID]uint64
	Cloud map[Dot]struct{}
}

func NewContext() CausalContext {
	return CausalContext{
		VV:    make(map[ActorID]uint64),
		Cloud: make(map[Dot]struct{}),
	}
}

// Contains reports whether the event d has been observed.
func (c *CausalContext) Contains(d Dot) bool {
	if d.Seq <= c.VV[d.Actor] {
		return true
	}
	_, ok := c.Cloud[d]
	return ok
}

// Add records the event d, compacting the cloud into the VV when the
// contiguous prefix grows.
func (c *CausalContext) Add(d Dot) {
	c.ensure()
	if d.Seq <= c.VV[d.Actor] {
		return
	}
	if d.Seq == c.VV[d.Actor]+1 {
		c.VV[d.Actor] = d.Seq
		c.compactActor(d.Actor)
		return
	}
	c.Cloud[d] = struct{}{}
}

// Next returns the next unused sequence number for an actor in this
// document's context (the dot-store "next dot"). The applier mints with it
// under the partition lock; the document's persisted VV is the crash-safe
// checkpoint because it commits in the same batch as the op. The cloud scan
// is defensive: locally minted dots are always contiguous, so it is empty in
// practice.
func (c *CausalContext) Next(a ActorID) uint64 {
	next := c.VV[a] + 1
	for d := range c.Cloud {
		if d.Actor == a && d.Seq >= next {
			next = d.Seq + 1
		}
	}
	return next
}

// UnionWith merges another context into this one: pointwise VV max, cloud
// union, then compaction.
func (c *CausalContext) UnionWith(o CausalContext) {
	c.ensure()
	for a, s := range o.VV {
		if s > c.VV[a] {
			c.VV[a] = s
		}
	}
	for d := range o.Cloud {
		if d.Seq > c.VV[d.Actor] {
			c.Cloud[d] = struct{}{}
		}
	}
	c.compact()
}

func (c *CausalContext) Clone() CausalContext {
	out := NewContext()
	for a, s := range c.VV {
		out.VV[a] = s
	}
	for d := range c.Cloud {
		out.Cloud[d] = struct{}{}
	}
	return out
}

// Empty reports whether no event has ever been observed.
func (c *CausalContext) Empty() bool {
	return len(c.VV) == 0 && len(c.Cloud) == 0
}

func (c *CausalContext) ensure() {
	if c.VV == nil {
		c.VV = make(map[ActorID]uint64)
	}
	
	if c.Cloud == nil {
		c.Cloud = make(map[Dot]struct{})
	}
}

// compactActor folds cloud dots for one actor into the VV while contiguous.
func (c *CausalContext) compactActor(a ActorID) {
	for {
		next := Dot{Actor: a, Seq: c.VV[a] + 1}
		if _, ok := c.Cloud[next]; !ok {
			return
		}
		delete(c.Cloud, next)
		c.VV[a] = next.Seq
	}
}

// compact drops covered cloud dots and folds contiguous ones into the VV.
func (c *CausalContext) compact() {
	for changed := true; changed; {
		changed = false
		for d := range c.Cloud {
			switch {
			case d.Seq <= c.VV[d.Actor]:
				delete(c.Cloud, d)
				changed = true
			case d.Seq == c.VV[d.Actor]+1:
				c.VV[d.Actor] = d.Seq
				delete(c.Cloud, d)
				changed = true
			}
		}
	}
}
