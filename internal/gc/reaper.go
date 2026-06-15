// Package gc implements the two explicit garbage-collection mechanisms:
//
//  1. Residual contexts: a deleted document keeps its causal context until
//     the partition has had two clean anti-entropy rounds covering it (all
//     owners reachable AND byte-equal — an offline owner freezes GC, which
//     is what makes resurrection impossible). Deletion also spreads by
//     contagion: a residual the peer no longer has was certified by the
//     peer's own GC and is dropped immediately.
//  2. Actor retirement: version-vector entries of nodes dead past the grace
//     period are dropped from contexts once the owners are in sync, unless
//     the actor still has a live register in the document.
//
// Both predicates are monotone, so the transient asymmetry while owners GC
// at slightly different times self-heals instead of ping-ponging.
package gc

import (
	"log/slog"

	"github.com/janthoXO/convergeKV/internal/crdt"
	"github.com/janthoXO/convergeKV/internal/storage"
)

// cleanRoundsToGC is how many clean AE rounds certify a residual for removal.
const cleanRoundsToGC = 2

// Coordinator is the locked mutation surface the reaper drives.
type Coordinator interface {
	GCDocument(pid uint16, key []byte) error
	RetireActors(pid uint16, key []byte, retired func(crdt.ActorID) bool) error
}

type Reaper struct {
	store *storage.Store
	coord Coordinator
	// knownActors returns the actors that must NOT be retired: alive
	// members and dead-within-grace members.
	knownActors func() map[crdt.ActorID]bool
	log         *slog.Logger
}

func New(store *storage.Store, coord Coordinator, knownActors func() map[crdt.ActorID]bool, log *slog.Logger) *Reaper {
	if log == nil {
		log = slog.Default()
	}
	return &Reaper{store: store, coord: coord, knownActors: knownActors, log: log}
}

// OnCleanRound advances GC for one partition after a certified-clean AE
// round: residual-context counters increment (and reap at the threshold),
// retired actors are dropped, and counters of re-created documents are
// cleared.
func (r *Reaper) OnCleanRound(pid uint16) {
	known := r.knownActors()
	retired := func(a crdt.ActorID) bool { return !known[a] }

	type action struct {
		key      []byte
		residual bool
		counter  uint32
	}
	var actions []action
	err := r.store.ScanPartition(pid, func(key []byte, doc *crdt.Document) error {
		counter, err := r.store.GCCounter(pid, key)
		if err != nil {
			return err
		}
		actions = append(actions, action{key: key, residual: len(doc.Fields) == 0, counter: counter})
		return nil
	})
	if err != nil {
		r.log.Warn("gc scan failed", "partition", pid, "err", err)
		return
	}

	for _, a := range actions {
		switch {
		case a.residual && a.counter+1 >= cleanRoundsToGC:
			if err := r.coord.GCDocument(pid, a.key); err != nil {
				r.log.Warn("residual gc failed", "partition", pid, "err", err)
			}
		case a.residual:
			if err := r.bumpCounter(pid, a.key, a.counter+1); err != nil {
				r.log.Warn("gc counter bump failed", "partition", pid, "err", err)
			}
		case a.counter > 0:
			// Document was re-created: certification void.
			b := r.store.NewBatch()
			b.DeleteGCCounter(pid, a.key)
			if err := r.store.Commit(b); err != nil {
				r.log.Warn("gc counter clear failed", "partition", pid, "err", err)
			}
		}
		if err := r.coord.RetireActors(pid, a.key, retired); err != nil {
			r.log.Warn("actor retirement failed", "partition", pid, "err", err)
		}
	}
}

// OnDirtyRound voids every key's certification progress for the partition:
// the plan requires two CONSECUTIVE clean rounds per key, and the durable
// counters would otherwise survive a dirty round in between (two adjacent
// clean rounds can be as little as interval/2 apart — less than a stale
// delta can sit in a peer's retry queue).
func (r *Reaper) OnDirtyRound(pid uint16) {
	if err := r.store.ClearGCCounters(pid); err != nil {
		r.log.Warn("gc counter reset failed", "partition", pid, "err", err)
	}
}

// OnPeerBucket implements GC contagion: keys we hold as residuals that the
// peer no longer has (it sent its complete bucket) were certified and
// removed by the peer's GC — drop them here too instead of pushing them
// back and freezing everyone's counters.
func (r *Reaper) OnPeerBucket(pid, bucket uint16, peerKeys map[string]struct{}) {
	err := r.store.ScanBucket(pid, bucket, func(key []byte, doc *crdt.Document) error {
		if _, peerHas := peerKeys[string(key)]; peerHas {
			return nil
		}
		if len(doc.Fields) != 0 {
			return nil
		}
		// No local certification needed: an ACTIVE peer that lacks a
		// residual either GC'd it (its certification required byte-equality
		// with us, so it covers us too) or never saw the document at all
		// (then no peer holds registers this residual still guards against;
		// a stale in-flight put is bounded by the retry queue age and
		// healed by AE).
		r.log.Debug("gc contagion delete", "partition", pid, "key", string(key))
		return r.coord.GCDocument(pid, key)
	})
	if err != nil {
		r.log.Warn("gc contagion failed", "partition", pid, "err", err)
	}
}

// OnPeerDoc implements retirement contagion: entries of retirable actors
// that the peer's copy of the document no longer carries were dropped under
// the peer's clean-round certification (which required byte-equality with
// us), so we drop them too instead of pushing them back and flapping.
func (r *Reaper) OnPeerDoc(pid uint16, key, peerDoc []byte) {
	peer, err := crdt.DecodeDocument(peerDoc)
	if err != nil {
		return
	}
	known := r.knownActors()
	peerHas := make(map[crdt.ActorID]bool, len(peer.Context.VV))
	for a := range peer.Context.VV {
		peerHas[a] = true
	}
	for d := range peer.Context.Cloud {
		peerHas[d.Actor] = true
	}
	err = r.coord.RetireActors(pid, key, func(a crdt.ActorID) bool {
		return !known[a] && !peerHas[a]
	})
	if err != nil {
		r.log.Warn("retirement contagion failed", "partition", pid, "err", err)
	}
}

func (r *Reaper) bumpCounter(pid uint16, key []byte, value uint32) error {
	b := r.store.NewBatch()
	b.SetGCCounter(pid, key, value)
	return r.store.Commit(b)
}
