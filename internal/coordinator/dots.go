package coordinator

import (
	"sync"

	"github.com/janthoXO/convergeKV/internal/crdt"
)

// DotSource mints globally unique dots for this node across concurrent
// partition appliers.
//
// Persistence note (refines spec §3.4 "persisted in the same batch as every
// applied op"): per-batch seq writes from concurrently committing partitions
// can land out of order, letting an older batch overwrite a newer checkpoint
// — which WOULD allow dot reuse after a crash. Instead the source persists a
// synced reservation ahead of use (one tiny write per blockSize dots); a
// crash skips at most one unused block, and a dot can never be minted twice.
type DotSource struct {
	mu        sync.Mutex
	actor     crdt.ActorID
	seq       uint64 // last minted
	reserved  uint64 // persisted upper bound (inclusive)
	blockSize uint64
	persist   func(uint64) error // synced write of the reservation
}

func NewDotSource(actor crdt.ActorID, lastReserved uint64, persist func(uint64) error) *DotSource {
	return &DotSource{
		actor:     actor,
		seq:       lastReserved, // skip the whole previous reservation
		reserved:  lastReserved,
		blockSize: 4096,
		persist:   persist,
	}
}

// Mint reserves n dots and returns a closure handing them out one by one.
// The closure is only valid for n calls and must be consumed by the caller
// while it holds its partition lock.
func (s *DotSource) Mint(n int) (func() crdt.Dot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.seq+uint64(n) > s.reserved {
		next := s.seq + uint64(n) + s.blockSize
		if err := s.persist(next); err != nil {
			return nil, err
		}
		s.reserved = next
	}
	start := s.seq
	s.seq += uint64(n)
	i := uint64(0)
	return func() crdt.Dot {
		i++
		return crdt.Dot{Actor: s.actor, Seq: start + i}
	}, nil
}
