package coordinator

import (
	"errors"
	"sync"
	"testing"

	"github.com/janthoXO/convergeKV/internal/crdt"
)

func TestDotSourceUniqueAcrossConcurrentMints(t *testing.T) {
	var persisted uint64
	src := NewDotSource(crdt.ActorID{1}, 0, func(seq uint64) error {
		persisted = seq
		return nil
	})

	var mu sync.Mutex
	seen := map[crdt.Dot]bool{}
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				mint, err := src.Mint(2)
				if err != nil {
					t.Error(err)
					return
				}
				d1, d2 := mint(), mint()
				mu.Lock()
				if seen[d1] || seen[d2] {
					t.Errorf("duplicate dot minted: %v %v", d1, d2)
				}
				seen[d1], seen[d2] = true, true
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	// The persisted reservation must cover every minted dot.
	for d := range seen {
		if d.Seq > persisted {
			t.Fatalf("dot %d above persisted reservation %d", d.Seq, persisted)
		}
	}
}

func TestDotSourceSkipsReservedBlockAfterRestart(t *testing.T) {
	persist := func(uint64) error { return nil }
	a := NewDotSource(crdt.ActorID{1}, 0, persist)
	mint, _ := a.Mint(1)
	last := mint()

	// "Restart" from the reservation that the first source persisted.
	b := NewDotSource(crdt.ActorID{1}, last.Seq+4096, persist)
	mint2, _ := b.Mint(1)
	if d := mint2(); d.Seq <= last.Seq+4096 {
		t.Fatalf("restarted source minted %d inside the old reservation", d.Seq)
	}
}

func TestDotSourceMintFailsWhenPersistFails(t *testing.T) {
	src := NewDotSource(crdt.ActorID{1}, 0, func(uint64) error {
		return errors.New("disk gone")
	})
	if _, err := src.Mint(1); err == nil {
		t.Fatal("Mint must fail when the reservation cannot be persisted")
	}
}
