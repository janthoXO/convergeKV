// Package partitions seeds and drops per-partition state (IBLT, anti-entropy
// triggers) in response to ownership changes. It is the off-loop subscriber
// referred to in internal/cluster/ownership: Ownership.Update only recomputes
// and publishes the owned set; the slow Badger scans that EnsurePartition can
// trigger run here, off the membership-watcher loop.
package partitions

import (
	"context"
	"log/slog"

	"golang.org/x/sync/errgroup"
)

// seedConcurrency caps how many EnsurePartition seed scans run in parallel
// for a single owned-set change. Each scan walks one partition's range in the
// store, so this bounds churn latency without spawning unbounded goroutines.
const seedConcurrency = 4

// Manager seeds or drops local state for a partition when ownership changes.
type Manager interface {
	EnsurePartition(ctx context.Context, partitionId uint32) error
	DropPartition(partitionId uint32)
}

// AntiEntropyTrigger requests an out-of-band anti-entropy round for the given
// partitions, so a freshly-owned partition doesn't wait for the next regular
// sync tick to start converging.
type AntiEntropyTrigger interface {
	TriggerPartitions(pids []uint32)
}

// Lifecycle reacts to successive owned-partition-set snapshots, diffing each
// against the previous one to seed newly-owned partitions and drop ones the
// local node no longer owns.
//
// Lifecycle is not safe for concurrent use; Sync is expected to be called
// sequentially from a single goroutine (e.g. the loop driven by Run).
type Lifecycle struct {
	mgr   Manager
	ae    AntiEntropyTrigger // may be nil
	owned map[uint32]struct{}
}

// New returns a Lifecycle with an empty baseline owned set. ae may be nil if
// out-of-band anti-entropy triggering is not needed (e.g. in tests).
func New(mgr Manager, ae AntiEntropyTrigger) *Lifecycle {
	return &Lifecycle{mgr: mgr, ae: ae, owned: make(map[uint32]struct{})}
}

// Sync seeds newly-owned partitions and drops no-longer-owned ones, relative
// to the owned set passed to the previous call (or empty, on the first call).
// It blocks until all EnsurePartition calls for newly-owned partitions
// complete.
func (l *Lifecycle) Sync(ctx context.Context, owned []uint32) {
	newSet := make(map[uint32]struct{}, len(owned))
	for _, pid := range owned {
		newSet[pid] = struct{}{}
	}

	// Drops first — fast and synchronous; releases ownership before we start
	// seeding adds, so anti-entropy stops syncing the dropped pids.
	for pid := range l.owned {
		if _, stillOwned := newSet[pid]; !stillOwned {
			l.mgr.DropPartition(pid)
		}
	}

	// Seed adds in parallel, capped at seedConcurrency.
	var adds []uint32
	for pid := range newSet {
		if _, had := l.owned[pid]; !had {
			adds = append(adds, pid)
		}
	}
	if len(adds) > 0 {
		var g errgroup.Group
		g.SetLimit(seedConcurrency)
		for _, pid := range adds {
			g.Go(func() error {
				if err := l.mgr.EnsurePartition(ctx, pid); err != nil {
					slog.Error("EnsurePartition failed", "partitionId", pid, "err", err)
				}
				return nil
			})
		}
		_ = g.Wait()

		if l.ae != nil {
			l.ae.TriggerPartitions(adds)
		}
	}

	l.owned = newSet
}

// Run calls Sync for every owned-set snapshot received on ch, until ch is
// closed (e.g. because the subscription's context was cancelled).
func (l *Lifecycle) Run(ctx context.Context, ch <-chan []uint32) {
	for owned := range ch {
		l.Sync(ctx, owned)
	}
}
