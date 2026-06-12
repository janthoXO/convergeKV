// Package streampush implements write-path replication with one long-lived
// goroutine per peer and a bounded per-peer queue.
//
// Design:
//   - One goroutine per peer is spawned on the first Enqueue call for that
//     peer and torn down when the peer is evicted (EvictAbsent/Evict) or when
//     Close is called.
//   - Each goroutine holds one long-lived gRPC PushEntries stream, reusing the
//     HTTP/2 connection from the pool. On stream error it reopens once and drops
//     the current item (anti-entropy reconciles).
//   - Each goroutine owns a bounded channel (cap QueueDepth). Overflow policy:
//     drop the oldest entry and count the drop — anti-entropy will reconcile.
//   - Enqueue is the inbound API; the coordinator calls it directly after a
//     successful local write. No eventbus indirection so writes are never
//     silently coalesced under back-pressure.
package streampush

import (
	"context"
	"log/slog"
	"sync"

	repb "github.com/janthoXO/convergeKV/gen/replication"
	"github.com/janthoXO/convergeKV/internal/adapter/connpool"
	reproto "github.com/janthoXO/convergeKV/internal/adapter/replication/proto"
	"github.com/janthoXO/convergeKV/internal/core/ports"
	"github.com/janthoXO/convergeKV/internal/domain/crdt"
)

const defaultQueueDepth = 1024

// pushItem is a (partitionID, batch) pair queued for a peer.
type pushItem struct {
	partitionId uint32
	batch       []crdt.FieldUpdate
}

// peerStreamer is the per-peer goroutine and its queue.
type peerStreamer struct {
	queue  chan pushItem
	cancel context.CancelFunc
}

// Fanout fans write-path updates to peer replicas and maintains one
// long-lived gRPC stream per peer.
type Fanout struct {
	localID    string
	pool       *connpool.Pool
	queueDepth int

	mu         sync.Mutex
	peers      map[string]*peerStreamer // keyed by GRPCAddr
	rootCtx    context.Context
	rootCancel context.CancelFunc
	wg         sync.WaitGroup
}

// Option configures a Fanout.
type Option func(*Fanout)

// WithQueueDepth sets the per-peer queue capacity.
func WithQueueDepth(n int) Option { return func(f *Fanout) { f.queueDepth = n } }

// New constructs a Fanout. ctx controls the lifetime of all peer goroutines.
func New(ctx context.Context, localID string, pool *connpool.Pool, opts ...Option) *Fanout {
	rootCtx, rootCancel := context.WithCancel(ctx)
	f := &Fanout{
		localID:    localID,
		pool:       pool,
		queueDepth: defaultQueueDepth,
		peers:      make(map[string]*peerStreamer),
		rootCtx:    rootCtx,
		rootCancel: rootCancel,
	}

	for _, o := range opts {
		o(f)
	}

	return f
}

// Enqueue groups updates by partition and enqueues (partitionId, batch) to
// each peer. Called by the coordinator immediately after a successful local
// write — satisfies ports.WriteSink.
//
// PartitionID is carried on each FieldUpdate (set by the replica at write time),
// so this groups directly by u.PartitionID rather than recomputing the keyspace
// hash. Single source of truth: keyspace.Of is called exactly once per write,
// at the coordinator.
func (f *Fanout) Enqueue(updates []crdt.FieldUpdate, peers []ports.MemberInfo) {
	if len(updates) == 0 || len(peers) == 0 {
		return
	}

	byPID := make(map[uint32][]crdt.FieldUpdate)
	for _, u := range updates {
		byPID[u.PartitionID] = append(byPID[u.PartitionID], u)
	}

	f.mu.Lock()
	for _, peer := range peers {
		if peer.ReplicaID == f.localID {
			continue
		}
		ps := f.getOrCreateLocked(peer.GRPCAddr)
		for partitionId, batch := range byPID {
			item := pushItem{partitionId: partitionId, batch: batch}
			select {
			case ps.queue <- item:
			default:
				// Drain one slot and insert the new item (drop-oldest policy).
				select {
				case <-ps.queue:
				default:
				}
				select {
				case ps.queue <- item:
				default:
				}
				slog.Warn("queue full, dropped oldest entry", "peer", peer.GRPCAddr, "partitionId", partitionId)
			}
		}
	}
	f.mu.Unlock()
}

func (f *Fanout) getOrCreateLocked(addr string) *peerStreamer {
	if ps, ok := f.peers[addr]; ok {
		return ps
	}

	ctx, cancel := context.WithCancel(f.rootCtx)
	ps := &peerStreamer{
		queue:  make(chan pushItem, f.queueDepth),
		cancel: cancel,
	}
	f.peers[addr] = ps
	f.wg.Go(func() {
		f.peerLoop(ctx, addr, ps.queue)
	})

	return ps
}

// EvictAbsent closes streamer goroutines for every address not in keep.
func (f *Fanout) EvictAbsent(keep map[string]struct{}) {
	f.mu.Lock()
	var toCancel []context.CancelFunc
	for addr, ps := range f.peers {
		if _, ok := keep[addr]; !ok {
			toCancel = append(toCancel, ps.cancel)
			delete(f.peers, addr)
		}
	}
	f.mu.Unlock()
	for _, cancel := range toCancel {
		cancel()
	}
}

// Close cancels all goroutines (peer loops and the topic consumer) and waits
// for them to finish.
func (f *Fanout) Close() {
	f.rootCancel()
	f.wg.Wait()
}

// peerLoop maintains one long-lived PushEntries stream to addr.
// It re-opens the stream on error; anti-entropy reconciles any dropped items.
func (f *Fanout) peerLoop(ctx context.Context, addr string, queue <-chan pushItem) {
	var stream repb.SyncService_PushEntriesClient

	openStream := func() bool {
		conn, err := f.pool.Get(addr)
		if err != nil {
			slog.Warn("dial failed", "addr", addr, "err", err)
			return false
		}
		s, err := repb.NewSyncServiceClient(conn).PushEntries(ctx)
		if err != nil {
			slog.Warn("open stream failed", "addr", addr, "err", err)
			return false
		}
		stream = s
		return true
	}

	closeStream := func() {
		if stream == nil {
			return
		}
		stream.CloseAndRecv() //nolint:errcheck
		stream = nil
	}
	defer closeStream()

	for {
		select {
		case <-ctx.Done():
			return
		case item, ok := <-queue:
			if !ok {
				return
			}

			if stream == nil && !openStream() {
				continue // drop item; anti-entropy reconciles
			}

			err := f.sendBatch(stream, item)
			if err != nil {
				slog.Warn("send failed, reopening stream", "addr", addr, "partitionId", item.partitionId, "err", err)
				closeStream()
				// Try once more on a fresh stream.
				if !openStream() {
					continue
				}

				err = f.sendBatch(stream, item)
				if err != nil {
					slog.Warn("retry send failed", "addr", addr, "partitionId", item.partitionId, "err", err)
					closeStream()
				}
			}
		}
	}
}

// sendBatch writes a PushHeader followed by all entries on stream.
func (f *Fanout) sendBatch(stream repb.SyncService_PushEntriesClient, item pushItem) error {
	err := stream.Send(&repb.PushChunk{
		Payload: &repb.PushChunk_Header{
			Header: &repb.PushHeader{PartitionId: item.partitionId},
		},
	})
	if err != nil {
		return err
	}

	for _, u := range item.batch {
		err := stream.Send(&repb.PushChunk{
			Payload: &repb.PushChunk_Entry{
				Entry: reproto.EntryToProto(u.Key, u.Field, u.Entry),
			},
		})
		if err != nil {
			return err
		}
	}
	return nil
}
