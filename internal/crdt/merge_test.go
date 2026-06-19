package crdt

import (
	"bytes"
	"testing"
)

var (
	actorA = ActorID{0xAA}
	actorB = ActorID{0xBB}
)

func dot(a ActorID, seq uint64) Dot { return Dot{Actor: a, Seq: seq} }

func docEqual(t *testing.T, got, want *Document) {
	t.Helper()
	if !bytes.Equal(got.Canonical(), want.Canonical()) {
		t.Fatalf("documents differ:\n got: %+v\nwant: %+v", got, want)
	}
}

// --- merge case: L present, R absent ----------------------------------------

func TestMergeRemoteSawAndRemoved(t *testing.T) {
	local := NewDocument()
	local.Put("f", []byte("v"), dot(actorA, 1), 100)

	// Remote observed dot (A,1) and has no field f: it removed f.
	remote := NewDocument()
	remote.Context.Add(dot(actorA, 1))
	remote.Context.Add(dot(actorB, 1)) // the remove event

	local.Merge(remote)
	if _, ok := local.Fields["f"]; ok {
		t.Fatal("field should be dropped: remote saw it and removed it")
	}
	if !local.Context.Contains(dot(actorB, 1)) {
		t.Fatal("context union missing remove event")
	}
}

func TestMergeRemoteNeverSawKeepsLocal(t *testing.T) {
	local := NewDocument()
	local.Put("f", []byte("v"), dot(actorA, 1), 100)

	remote := NewDocument()
	remote.Context.Add(dot(actorB, 7)) // unrelated history

	local.Merge(remote)
	if _, ok := local.Fields["f"]; !ok {
		t.Fatal("field must survive: remote never saw it")
	}
}

// --- merge case: L absent, R present ----------------------------------------

func TestMergeWeRemovedStaysAbsent(t *testing.T) {
	reg := Register{Dot: dot(actorB, 3), HLC: 50, Value: []byte("old")}

	local := NewDocument()
	local.Context.Add(dot(actorB, 3)) // we saw the write...
	local.Context.Add(dot(actorA, 1)) // ...and removed it

	remote := NewDocument()
	remote.Fields["f"] = []Register{reg}
	remote.Context.Add(dot(actorB, 3))

	local.Merge(remote)
	if _, ok := local.Fields["f"]; ok {
		t.Fatal("removed field must not resurrect")
	}
}

func TestMergeNewFieldAdopted(t *testing.T) {
	local := NewDocument()

	remote := NewDocument()
	remote.Put("f", []byte("v"), dot(actorB, 1), 100)

	local.Merge(remote)
	if got, ok := local.Get("f"); !ok || !bytes.Equal(got.Value, []byte("v")) {
		t.Fatalf("field not adopted: %+v", got)
	}
}

// --- merge case: both present ------------------------------------------------

func TestMergeSameDotKeepsLocal(t *testing.T) {
	local := NewDocument()
	local.Put("f", []byte("v"), dot(actorA, 1), 100)
	remote := local.Clone()

	local.Merge(remote)
	docEqual(t, local, remote)
}

func TestMergeLocalSupersededRemoteWins(t *testing.T) {
	// Remote has seen our write (A,1) and overwrote it with (B,1).
	local := NewDocument()
	local.Put("f", []byte("old"), dot(actorA, 1), 200) // higher HLC — must NOT matter

	remote := NewDocument()
	remote.Fields["f"] = []Register{{Dot: dot(actorB, 1), HLC: 100, Value: []byte("new")}}
	remote.Context.Add(dot(actorB, 1))
	remote.Context.Add(dot(actorA, 1)) // proof it saw ours

	local.Merge(remote)
	if got, _ := local.Get("f"); !bytes.Equal(got.Value, []byte("new")) {
		t.Fatal("causally superseding write must win regardless of HLC")
	}
	if len(local.Fields["f"]) != 1 {
		t.Fatal("superseded register must leave the set")
	}
}

func TestMergeRemoteIsOldNewsLocalWins(t *testing.T) {
	// We already saw remote's write (B,1) and overwrote it with (A,2).
	local := NewDocument()
	local.Context.Add(dot(actorB, 1))
	local.Fields["f"] = []Register{{Dot: dot(actorA, 2), HLC: 100, Value: []byte("ours")}}
	local.Context.Add(dot(actorA, 2))

	remote := NewDocument()
	remote.Put("f", []byte("theirs"), dot(actorB, 1), 300)

	local.Merge(remote)
	if got, _ := local.Get("f"); !bytes.Equal(got.Value, []byte("ours")) {
		t.Fatal("superseded remote write must lose regardless of HLC")
	}
	if len(local.Fields["f"]) != 1 {
		t.Fatal("stale remote register must not enter the set")
	}
}

func TestMergeTrueConcurrencyLWW(t *testing.T) {
	mk := func(actor ActorID, hlcTS HLC, v string) *Document {
		d := NewDocument()
		d.Put("f", []byte(v), dot(actor, 1), hlcTS)
		return d
	}
	a := mk(actorA, 200, "a-wins")
	b := mk(actorB, 100, "b-loses")

	merged := a.Clone()
	merged.Merge(b)
	if got, _ := merged.Get("f"); !bytes.Equal(got.Value, []byte("a-wins")) {
		t.Fatal("higher HLC must win")
	}
	if len(merged.Fields["f"]) != 2 {
		t.Fatal("both concurrent registers must remain in the set until covered")
	}
	// And in the other direction (commutativity of the outcome).
	merged2 := b.Clone()
	merged2.Merge(a)
	docEqual(t, merged2, merged)

	// Loser's dot is in the context — it can never resurrect.
	if !merged.Context.Contains(dot(actorB, 1)) {
		t.Fatal("loser's dot must enter the context")
	}
}

func TestMergeTrueConcurrencyHLCTieActorBytesBreak(t *testing.T) {
	a := NewDocument()
	a.Put("f", []byte("a"), dot(actorA, 1), 100)
	b := NewDocument()
	b.Put("f", []byte("b"), dot(actorB, 1), 100)

	merged := a.Clone()
	merged.Merge(b)
	if got, _ := merged.Get("f"); !bytes.Equal(got.Value, []byte("b")) {
		t.Fatal("on HLC tie, higher actor bytes must win (0xBB > 0xAA)")
	}
}

// --- delta semantics ----------------------------------------------------------

func TestPutDeltaConveysWrite(t *testing.T) {
	src := NewDocument()
	delta := src.Put("f", []byte("v"), dot(actorA, 1), 100)

	dst := NewDocument()
	dst.Merge(delta)
	docEqual(t, dst, src)
}

func TestRemoveFieldDeltaConveysRemoval(t *testing.T) {
	src := NewDocument()
	put := src.Put("f", []byte("v"), dot(actorA, 1), 100)

	peer := NewDocument()
	peer.Merge(put)

	rm := src.RemoveField("f", dot(actorA, 2))
	if !rm.Context.Contains(dot(actorA, 1)) {
		t.Fatal("remove delta must carry the removed register's dot")
	}
	peer.Merge(rm)
	if _, ok := peer.Fields["f"]; ok {
		t.Fatal("peer must drop removed field")
	}
	docEqual(t, peer, src)
}

func TestRemoveThenLateOriginalPutNoResurrection(t *testing.T) {
	src := NewDocument()
	put := src.Put("f", []byte("v"), dot(actorA, 1), 100)
	rm := src.RemoveField("f", dot(actorA, 2))

	// Peer sees the remove BEFORE the original put.
	peer := NewDocument()
	peer.Merge(rm)
	peer.Merge(put)
	if _, ok := peer.Fields["f"]; ok {
		t.Fatal("late-arriving put of a removed register must not resurrect")
	}
	docEqual(t, peer, src)
}

func TestDeleteDocumentDelta(t *testing.T) {
	src := NewDocument()
	p1 := src.Put("a", []byte("1"), dot(actorA, 1), 100)
	p2 := src.Put("b", []byte("2"), dot(actorA, 2), 101)

	peer := NewDocument()
	peer.Merge(p1)
	peer.Merge(p2)

	del := src.Delete(dot(actorA, 3))
	if !src.Deleted() {
		t.Fatal("local document must read as deleted with residual context")
	}
	peer.Merge(del)
	if len(peer.Fields) != 0 {
		t.Fatalf("peer must have no fields after delete, has %v", peer.Fields)
	}
	docEqual(t, peer, src)
}

func TestPutMultiMintsOneDotPerField(t *testing.T) {
	m := &Minter{Actor: actorA}
	src := NewDocument()
	delta := src.PutMulti(map[string][]byte{"x": []byte("1"), "y": []byte("2")}, m.Next, 100)

	if m.Seq != 2 {
		t.Fatalf("expected 2 dots minted, got %d", m.Seq)
	}
	x, _ := delta.Get("x")
	y, _ := delta.Get("y")
	if x.Dot == y.Dot {
		t.Fatal("fields must not share a dot")
	}
	peer := NewDocument()
	peer.Merge(delta)
	docEqual(t, peer, src)

	// Later: remove one field; the other must survive everywhere.
	rm := src.RemoveField("x", m.Next())
	peer.Merge(rm)
	if _, ok := peer.Fields["x"]; ok {
		t.Fatal("x must be removed")
	}
	if _, ok := peer.Fields["y"]; !ok {
		t.Fatal("y must survive x's removal")
	}
}

// --- context ------------------------------------------------------------------

func TestContextCloudCompaction(t *testing.T) {
	c := NewContext()
	c.Add(dot(actorA, 3)) // gap: goes to cloud
	c.Add(dot(actorA, 2))
	if c.VV[actorA] != 0 || len(c.Cloud) != 2 {
		t.Fatalf("expected gap in cloud, got VV=%d cloud=%d", c.VV[actorA], len(c.Cloud))
	}
	c.Add(dot(actorA, 1)) // fills the gap
	if c.VV[actorA] != 3 || len(c.Cloud) != 0 {
		t.Fatalf("expected full compaction, got VV=%d cloud=%d", c.VV[actorA], len(c.Cloud))
	}
	if !c.Contains(dot(actorA, 2)) || c.Contains(dot(actorA, 4)) {
		t.Fatal("Contains wrong after compaction")
	}
}

func TestContextUnionCompacts(t *testing.T) {
	a := NewContext()
	a.Add(dot(actorA, 2)) // cloud
	b := NewContext()
	b.Add(dot(actorA, 1)) // VV=1
	a.UnionWith(b)
	if a.VV[actorA] != 2 || len(a.Cloud) != 0 {
		t.Fatalf("union must compact: VV=%d cloud=%d", a.VV[actorA], len(a.Cloud))
	}
}

func TestCanonicalEncodingDeterministic(t *testing.T) {
	build := func() *Document {
		d := NewDocument()
		d.Put("b", []byte("2"), dot(actorB, 1), 101)
		d.Put("a", []byte("1"), dot(actorA, 1), 100)
		d.Context.Add(dot(actorB, 9)) // cloud entry
		return d
	}
	if !bytes.Equal(build().Canonical(), build().Canonical()) {
		t.Fatal("canonical encoding not deterministic")
	}
}
