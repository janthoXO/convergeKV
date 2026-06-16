// Package transfer bootstraps newly gained partitions: gossip the
// bootstrapping flag, stream a snapshot from the best source owner while
// live deltas arrive concurrently (merge makes the overlap safe — no
// sequencing), then flip to active.
package transfer

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/janthoXO/convergeKV/internal/cluster"
	"github.com/janthoXO/convergeKV/internal/coordinator"
	"github.com/janthoXO/convergeKV/internal/nodeid"
	"github.com/janthoXO/convergeKV/internal/placement"
)

// Source streams a partition snapshot from a peer (gRPC pool in production).
type Source interface {
	Snapshot(ctx context.Context, addr string, pid uint16, fn func(key, doc []byte) error) error
}

// Flags is the gossip surface the manager drives.
type Flags interface {
	SetPartitionStatus(pid uint16, s cluster.Status) error
}

type Manager struct {
	self   nodeid.ID
	coord  *coordinator.Coordinator
	view   func() *placement.View
	source Source
	flags  Flags
	log    *slog.Logger

	mu      sync.Mutex
	pending map[uint16]bool // partitions with a bootstrap in flight
	wg      sync.WaitGroup

	started atomic.Uint64
	bytes   atomic.Uint64
}

// Wait blocks until all in-flight bootstraps have finished (shutdown).
func (m *Manager) Wait() { m.wg.Wait() }

// Started returns how many bootstrap transfers this node has begun
// (the no-transfer-storm assertion for restarts within grace).
func (m *Manager) Started() uint64 { return m.started.Load() }

// Bytes returns the total document bytes received via bootstrap snapshots.
func (m *Manager) Bytes() uint64 { return m.bytes.Load() }

func New(self nodeid.ID, coord *coordinator.Coordinator, view func() *placement.View,
	source Source, flags Flags, log *slog.Logger) *Manager {
	if log == nil {
		log = slog.Default()
	}
	return &Manager{
		self:    self,
		coord:   coord,
		view:    view,
		source:  source,
		flags:   flags,
		log:     log,
		pending: make(map[uint16]bool),
	}
}

// Bootstrap starts an asynchronous bootstrap for a partition unless one is
// already in flight.
func (m *Manager) Bootstrap(ctx context.Context, pid uint16) {
	m.mu.Lock()
	if m.pending[pid] {
		m.mu.Unlock()
		return
	}
	m.pending[pid] = true
	m.mu.Unlock()
	m.started.Add(1)

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		defer func() {
			m.mu.Lock()
			delete(m.pending, pid)
			m.mu.Unlock()
		}()
		if err := m.run(ctx, pid); err != nil {
			m.log.Warn("partition bootstrap failed; will retry on next view change",
				"partition", pid, "err", err)
			// Roll back to none so the watcher retries.
			_ = m.flags.SetPartitionStatus(pid, cluster.StatusNone)
		}
	}()
}

func (m *Manager) run(ctx context.Context, pid uint16) error {
	if err := m.flags.SetPartitionStatus(pid, cluster.StatusBootstrapping); err != nil {
		return err
	}
	// From this point we receive live deltas for the partition (write set
	// includes bootstrapping owners).

	src, ok := m.pickSource(pid)
	if !ok {
		// No serving owner has the data (fresh partition): become active.
		return m.flags.SetPartitionStatus(pid, cluster.StatusActive)
	}

	var streamed int
	err := m.source.Snapshot(ctx, src.Addr, pid, func(key, doc []byte) error {
		_, err := m.coord.MergeDelta(pid, key, doc)
		if err == nil {
			streamed++
			m.bytes.Add(uint64(len(key) + len(doc)))
		}
		return err
	})
	if err != nil {
		return fmt.Errorf("transfer: snapshot from %s: %w", src.Addr, err)
	}
	m.log.Info("partition bootstrapped", "partition", pid, "docs", streamed, "source", src.Addr)
	return m.flags.SetPartitionStatus(pid, cluster.StatusActive)
}

// pickSource returns the highest-ranked owner that can serve a snapshot.
func (m *Manager) pickSource(pid uint16) (placement.Owner, bool) {
	for _, o := range m.view().Owners(pid) {
		if o.ID == m.self || o.Dead {
			continue
		}
		if o.Status.Serving() {
			return o, true
		}
	}
	return placement.Owner{}, false
}

// Drain marks every owned partition draining and blocks until each one has
// RF serving owners besides this node, or the timeout passes.
func Drain(self nodeid.ID, view func() *placement.View, flags interface {
	UpdateFlags(func(cluster.PartitionFlags)) error
}, owned []uint16, timeout time.Duration, log *slog.Logger) {
	err := flags.UpdateFlags(func(f cluster.PartitionFlags) {
		for _, pid := range owned {
			f.Set(pid, cluster.StatusDraining)
		}
	})
	if err != nil {
		log.Warn("drain flag update failed", "err", err)
	}

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		v := view()
		ready := true
		for _, pid := range owned {
			actives := 0
			for _, o := range v.Owners(pid) {
				if o.ID != self && !o.Dead && o.Status == cluster.StatusActive {
					actives++
				}
			}
			if actives < min(placement.RF, len(v.Owners(pid))-1) {
				ready = false
				break
			}
		}
		if ready {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	log.Warn("drain timed out; leaving anyway")
}
