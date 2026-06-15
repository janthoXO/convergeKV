// Package antientropy implements the Merkle exchange that is this system's
// ONLY repair mechanism (no read repair, no persistent delta buffers): per
// partition, each owner periodically compares tree roots with its peer
// owners, exchanges the leaf vector when they differ, and merges the full
// documents of diverging buckets in both directions.
//
// Clean rounds cost one root hash per peer — O(1), not O(keys). After
// repairing a bucket the local leaf is recomputed from the documents
// themselves, which heals any drift between leaves and data (XOR leaves
// cannot self-correct otherwise).
package antientropy

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/janthoXO/convergeKV/internal/cluster"
	"github.com/janthoXO/convergeKV/internal/coordinator"
	"github.com/janthoXO/convergeKV/internal/crdt"
	"github.com/janthoXO/convergeKV/internal/merkle"
	"github.com/janthoXO/convergeKV/internal/placement"
	"github.com/janthoXO/convergeKV/internal/storage"
)

// Peer is the node-service surface the engine needs (implemented by the
// gRPC pool; faked in tests).
type Peer interface {
	MerkleRoot(ctx context.Context, addr string, pid uint16) ([]byte, error)
	MerkleLeaves(ctx context.Context, addr string, pid uint16) ([]byte, error)
	SyncBucket(ctx context.Context, addr string, pid, bucket uint16, fn func(key, doc []byte) error) error
	ApplyDelta(ctx context.Context, addr string, pid uint16, key, delta []byte) error
}

// GC receives garbage-collection triggers from the exchange (implemented by
// internal/gc; optional).
type GC interface {
	OnCleanRound(pid uint16)
	// OnDirtyRound voids per-key certification progress: "2 clean rounds"
	// must mean consecutive ones.
	OnDirtyRound(pid uint16)
	OnPeerBucket(pid, bucket uint16, peerKeys map[string]struct{})
	OnPeerDoc(pid uint16, key, peerDoc []byte)
}

type Engine struct {
	self  [16]byte
	p     uint16
	store *storage.Store
	coord *coordinator.Coordinator
	view  func() *placement.View
	peer  Peer
	gc    GC
	log   *slog.Logger

	interval time.Duration

	mu          sync.Mutex
	cleanRounds map[uint16]int

	// metrics
	keysRepaired atomic.Uint64
	leafFetches  atomic.Uint64
	rootChecks   atomic.Uint64
}

func New(self [16]byte, p uint16, store *storage.Store, coord *coordinator.Coordinator,
	view func() *placement.View, peer Peer, interval time.Duration, log *slog.Logger) *Engine {
	if log == nil {
		log = slog.Default()
	}
	return &Engine{
		self:        self,
		p:           p,
		store:       store,
		coord:       coord,
		view:        view,
		peer:        peer,
		log:         log,
		interval:    interval,
		cleanRounds: make(map[uint16]int),
	}
}

// SetGC attaches the garbage-collection hooks (wired by the node after both
// components exist).
func (e *Engine) SetGC(gc GC) { e.gc = gc }

// KeysRepaired returns the number of documents changed by repairs.
func (e *Engine) KeysRepaired() uint64 { return e.keysRepaired.Load() }

// LeafFetches returns how many leaf vectors were fetched (zero across clean
// rounds — the acceptance bound for clean-round traffic).
func (e *Engine) LeafFetches() uint64 { return e.leafFetches.Load() }

// RootChecks returns the number of root comparisons performed.
func (e *Engine) RootChecks() uint64 { return e.rootChecks.Load() }

// CleanRounds returns how many consecutive clean rounds the partition has
// had (the M8 GC trigger).
func (e *Engine) CleanRounds(pid uint16) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.cleanRounds[pid]
}

// Run schedules jittered rounds over all owned partitions until ctx ends.
func (e *Engine) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(jitter(e.interval)):
		}
		v := e.view()
		for _, pid := range v.OwnedPartitions(e.self) {
			if ctx.Err() != nil {
				return
			}
			if err := e.RunRound(ctx, pid); err != nil {
				e.log.Warn("anti-entropy round failed", "partition", pid, "err", err)
			}
		}
	}
}

func jitter(d time.Duration) time.Duration {
	return d/2 + rand.N(d)
}

// RunRound performs one exchange round for a partition against every other
// owner. A round is clean when every reachable peer agreed on the root.
func (e *Engine) RunRound(ctx context.Context, pid uint16) error {
	v := e.view()
	clean := true
	var firstErr error
	for _, o := range v.Owners(pid) {
		if o.ID == e.self || o.Status == cluster.StatusNone {
			continue
		}
		repaired, err := e.exchange(ctx, pid, o)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			clean = false // unreachable peer: cannot certify the round
			continue
		}
		if repaired {
			clean = false
		}
	}

	e.mu.Lock()
	if clean {
		e.cleanRounds[pid]++
	} else {
		e.cleanRounds[pid] = 0
	}
	e.mu.Unlock()

	if e.gc != nil {
		if clean {
			e.gc.OnCleanRound(pid)
		} else {
			e.gc.OnDirtyRound(pid)
		}
	}
	return firstErr
}

// exchange compares trees with one peer and repairs both sides. Returns
// whether any divergence was found.
func (e *Engine) exchange(ctx context.Context, pid uint16, o placement.Owner) (bool, error) {
	localLeaves, err := e.store.MerkleLeaves(pid)
	if err != nil {
		return false, err
	}
	localRoot := merkle.Root(localLeaves)

	e.rootChecks.Add(1)
	remoteRoot, err := e.peer.MerkleRoot(ctx, o.Addr, pid)
	if err != nil {
		return false, err
	}
	if bytes.Equal(remoteRoot, localRoot[:]) {
		return false, nil
	}

	e.leafFetches.Add(1)
	remoteLeavesRaw, err := e.peer.MerkleLeaves(ctx, o.Addr, pid)
	if err != nil {
		return false, err
	}
	if len(remoteLeavesRaw) != merkle.Buckets*merkle.HashSize {
		return false, fmt.Errorf("antientropy: peer leaf vector has %d bytes", len(remoteLeavesRaw))
	}

	for b := uint16(0); b < merkle.Buckets; b++ {
		remote := remoteLeavesRaw[int(b)*merkle.HashSize : (int(b)+1)*merkle.HashSize]
		if bytes.Equal(remote, localLeaves[b][:]) {
			continue
		}
		if err := e.repairBucket(ctx, pid, b, o); err != nil {
			return true, err
		}
	}
	return true, nil
}

// repairBucket merges the peer's documents of one bucket into local state,
// pushes the (now merged) local documents back, then rebuilds the local leaf
// from the data.
func (e *Engine) repairBucket(ctx context.Context, pid, bucket uint16, o placement.Owner) error {
	contagion := e.gc != nil && (o.Status == cluster.StatusActive || o.Status == cluster.StatusDraining)
	peerKeys := make(map[string]struct{})
	err := e.peer.SyncBucket(ctx, o.Addr, pid, bucket, func(key, doc []byte) error {
		peerKeys[string(key)] = struct{}{}
		changed, err := e.coord.MergeDelta(pid, key, doc)
		if err != nil {
			return err
		}
		if changed {
			e.keysRepaired.Add(1)
		}
		if contagion {
			e.gc.OnPeerDoc(pid, key, doc)
		}
		return nil
	})
	if err != nil {
		return err
	}

	// GC contagion before the push: residuals a fully-serving peer no
	// longer has must not be pushed back (a bootstrapping peer's bucket may
	// simply be incomplete — its absences certify nothing).
	if contagion {
		e.gc.OnPeerBucket(pid, bucket, peerKeys)
	}

	// Push our merged view of the bucket back; the peer's MergeDelta keeps
	// its own leaf consistent.
	err = e.store.ScanBucket(pid, bucket, func(key []byte, doc *crdt.Document) error {
		return e.peer.ApplyDelta(ctx, o.Addr, pid, key, doc.Canonical())
	})
	if err != nil {
		return err
	}

	// Self-heal the local leaf from the documents.
	return e.coord.RecomputeMerkleLeaf(pid, bucket)
}
