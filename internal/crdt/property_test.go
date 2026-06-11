package crdt

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"pgregory.net/rapid"
)

// history simulates the appliers of one partition's three owners executing
// random local ops, collecting the deltas they would replicate.
type history struct {
	replicas []*Document
	minters  []*Minter
	deltas   []*Document
	// removedDots are dots of registers that some op removed; per invariant 3
	// they must never reappear as a field's dot after convergence.
	removedDots map[Dot]struct{}
	ts          HLC
}

func newHistory(n int) *history {
	h := &history{removedDots: make(map[Dot]struct{}), ts: 1 << 16}
	for i := 0; i < n; i++ {
		h.replicas = append(h.replicas, NewDocument())
		h.minters = append(h.minters, &Minter{Actor: ActorID{byte(0x10 * (i + 1))}})
	}
	return h
}

var fieldPool = []string{"a", "b", "c"}

// step applies one random op on one replica. kind/replica/field/value/tsJump
// are externally chosen so both rapid and plain rand can drive it.
func (h *history) step(kind, replica, field int, value []byte, tsJump uint64) {
	h.ts += tsJump // tsJump may be 0: concurrent ops can carry equal HLCs
	r := h.replicas[replica]
	m := h.minters[replica]
	f := fieldPool[field]

	var delta *Document
	switch kind {
	case 0:
		delta = r.Put(f, value, m.Next(), h.ts)
	case 1:
		for _, old := range r.Fields[f] {
			h.removedDots[old.Dot] = struct{}{}
		}
		delta = r.RemoveField(f, m.Next())
	default:
		for _, regs := range r.Fields {
			for _, reg := range regs {
				h.removedDots[reg.Dot] = struct{}{}
			}
		}
		delta = r.Delete(m.Next())
	}
	h.deltas = append(h.deltas, delta)
}

// converge delivers every delta to every replica in an independently shuffled
// order with duplication, then asserts all replicas are byte-identical and no
// removed register resurrected.
func (h *history) converge(t interface {
	Fatalf(format string, args ...any)
}, rng *rand.Rand) {
	for _, r := range h.replicas {
		order := rng.Perm(len(h.deltas))
		for _, i := range order {
			r.Merge(h.deltas[i])
			if rng.Intn(4) == 0 {
				r.Merge(h.deltas[i]) // duplicate delivery
			}
		}
	}
	want := h.replicas[0].Canonical()
	for i, r := range h.replicas[1:] {
		if !bytes.Equal(r.Canonical(), want) {
			t.Fatalf("replica %d diverged:\n%v\nvs\n%v", i+1, h.replicas[0], r)
		}
	}
	for f, regs := range h.replicas[0].Fields {
		for _, reg := range regs {
			if _, removed := h.removedDots[reg.Dot]; removed {
				t.Fatalf("resurrection: field %q carries removed dot %v", f, reg.Dot)
			}
		}
	}
}

// Invariants 1, 3, 5 (convergence, no resurrection, LWW determinism) under
// rapid-generated op sequences and delivery orders.
func TestPropertyConvergence(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		h := newHistory(3)
		n := rapid.IntRange(1, 12).Draw(t, "ops")
		for i := 0; i < n; i++ {
			h.step(
				rapid.IntRange(0, 2).Draw(t, fmt.Sprintf("kind%d", i)),
				rapid.IntRange(0, 2).Draw(t, fmt.Sprintf("replica%d", i)),
				rapid.IntRange(0, len(fieldPool)-1).Draw(t, fmt.Sprintf("field%d", i)),
				[]byte{byte(rapid.IntRange(0, 255).Draw(t, fmt.Sprintf("value%d", i)))},
				uint64(rapid.IntRange(0, 2).Draw(t, fmt.Sprintf("ts%d", i)))<<16,
			)
		}
		rng := rand.New(rand.NewSource(rapid.Int64().Draw(t, "shuffle")))
		h.converge(t, rng)
	})
}

// Spec M1 acceptance: 10k randomized op sequences across 3 simulated replicas.
func TestConvergence10k(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for iter := 0; iter < 10_000; iter++ {
		h := newHistory(3)
		for i, n := 0, 2+rng.Intn(8); i < n; i++ {
			h.step(rng.Intn(3), rng.Intn(3), rng.Intn(len(fieldPool)),
				[]byte{byte(rng.Intn(256))}, uint64(rng.Intn(3))<<16)
		}
		h.converge(t, rng)
	}
}

// Invariant 4: concurrent puts to different fields both survive everywhere.
func TestPropertyConcurrentFieldsSurvive(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		h := newHistory(3)
		// Replica 0 and 1 write disjoint fields concurrently (equal HLC allowed).
		h.step(0, 0, 0, []byte("x"), uint64(rapid.IntRange(0, 1).Draw(t, "t1"))<<16)
		h.step(0, 1, 1, []byte("y"), uint64(rapid.IntRange(0, 1).Draw(t, "t2"))<<16)
		rng := rand.New(rand.NewSource(rapid.Int64().Draw(t, "shuffle")))
		h.converge(t, rng)
		doc := h.replicas[0]
		if _, ok := doc.Fields[fieldPool[0]]; !ok {
			t.Fatalf("field %q lost", fieldPool[0])
		}
		if _, ok := doc.Fields[fieldPool[1]]; !ok {
			t.Fatalf("field %q lost", fieldPool[1])
		}
	})
}

// Invariant 2: merge is commutative, associative, idempotent over documents
// generated from consistent op histories.
func TestPropertyMergeLaws(t *testing.T) {
	genDoc := func(t *rapid.T, label string, actorByte byte) *Document {
		d := NewDocument()
		m := &Minter{Actor: ActorID{actorByte}}
		n := rapid.IntRange(0, 6).Draw(t, label+"ops")
		for i := 0; i < n; i++ {
			f := fieldPool[rapid.IntRange(0, len(fieldPool)-1).Draw(t, fmt.Sprintf("%sf%d", label, i))]
			ts := HLC(rapid.IntRange(1, 4).Draw(t, fmt.Sprintf("%st%d", label, i))) << 16
			switch rapid.IntRange(0, 2).Draw(t, fmt.Sprintf("%sk%d", label, i)) {
			case 0:
				d.Put(f, []byte{byte(i)}, m.Next(), ts)
			case 1:
				d.RemoveField(f, m.Next())
			default:
				d.Delete(m.Next())
			}
		}
		return d
	}

	rapid.Check(t, func(t *rapid.T) {
		a := genDoc(t, "a", 0x11)
		b := genDoc(t, "b", 0x22)
		c := genDoc(t, "c", 0x33)

		// Commutativity: a ⊔ b == b ⊔ a
		ab := a.Clone()
		ab.Merge(b)
		ba := b.Clone()
		ba.Merge(a)
		if !bytes.Equal(ab.Canonical(), ba.Canonical()) {
			t.Fatalf("not commutative:\na⊔b=%v\nb⊔a=%v", ab, ba)
		}

		// Associativity: (a ⊔ b) ⊔ c == a ⊔ (b ⊔ c)
		abc1 := ab.Clone()
		abc1.Merge(c)
		bc := b.Clone()
		bc.Merge(c)
		abc2 := a.Clone()
		abc2.Merge(bc)
		if !bytes.Equal(abc1.Canonical(), abc2.Canonical()) {
			t.Fatalf("not associative:\n(a⊔b)⊔c=%v\na⊔(b⊔c)=%v", abc1, abc2)
		}

		// Idempotence: a ⊔ a == a
		aa := a.Clone()
		aa.Merge(a)
		if !bytes.Equal(aa.Canonical(), a.Canonical()) {
			t.Fatalf("not idempotent:\na⊔a=%v\na=%v", aa, a)
		}
	})
}

// Invariant 7 (steady-state shape): context VV never has more entries than
// actors that ever wrote, and the cloud is empty once all deltas from all
// actors are delivered contiguously.
func TestPropertyContextBounded(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		h := newHistory(3)
		n := rapid.IntRange(1, 12).Draw(t, "ops")
		for i := 0; i < n; i++ {
			h.step(
				rapid.IntRange(0, 2).Draw(t, fmt.Sprintf("kind%d", i)),
				rapid.IntRange(0, 2).Draw(t, fmt.Sprintf("replica%d", i)),
				rapid.IntRange(0, len(fieldPool)-1).Draw(t, fmt.Sprintf("field%d", i)),
				[]byte{1},
				1<<16,
			)
		}
		rng := rand.New(rand.NewSource(rapid.Int64().Draw(t, "shuffle")))
		h.converge(t, rng)
		doc := h.replicas[0]
		if len(doc.Context.VV) > len(h.replicas) {
			t.Fatalf("VV has %d entries for %d actors", len(doc.Context.VV), len(h.replicas))
		}
		if len(doc.Context.Cloud) != 0 {
			t.Fatalf("cloud not compacted after full delivery: %v", doc.Context.Cloud)
		}
	})
}
