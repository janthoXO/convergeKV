// Package antientropy runs the per-partition IBLT reconciliation loop.
package antientropy

import (
	"context"
	"io"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	repb "github.com/janthoXO/convergeKV/gen/replication"
	"github.com/janthoXO/convergeKV/internal/adapter/connpool"
	reproto "github.com/janthoXO/convergeKV/internal/adapter/replication/proto"
	"github.com/janthoXO/convergeKV/internal/core/ports"
	"github.com/janthoXO/convergeKV/internal/domain/crdt"
	domiblt "github.com/janthoXO/convergeKV/internal/domain/iblt"
)

const (
	defaultSyncInterval     = 2 * time.Second
	defaultRoundTimeout     = 30 * time.Second
	defaultFullStateTimeout = 5 * time.Minute
)

// Ownership is the minimal interface the syncer needs to read owned partitions
// and their peers.
type Ownership interface {
	Owned() []uint32
	Peers(partitionId uint32) []ports.MemberInfo
}

// Node is the subset of the local replica that anti-entropy needs.
type Node interface {
	ReplicaID() string
	IBLTSnapshot(partitionId uint32) *domiblt.IBLT
	GetField(ctx context.Context, partitionId uint32, key, field string) (crdt.FieldEntry, bool, error)
	ApplyDelta(ctx context.Context, partitionId uint32, key, field string, e crdt.FieldEntry) (bool, error)
	IteratePartition(ctx context.Context, partitionId uint32, fn func(key, field string, entry crdt.FieldEntry) error) error
}

// Syncer runs the IBLT-based anti-entropy loop.
type Syncer struct {
	node      Node
	pool      *connpool.Pool
	ownership Ownership

	interval         time.Duration
	roundTimeout     time.Duration
	fullStateTimeout time.Duration
	tombstoneGraceMs uint64

	inflight   map[peerPartition]struct{}
	inflightMu sync.Mutex

	// triggerCh carries out-of-band sync requests for specific partitions
	// (e.g. from internal/cluster/partitions when ownership is gained), so a
	// new owner doesn't wait up to interval to start converging. Best-effort:
	// a full send is dropped, since the next regular tick covers it anyway.
	triggerCh chan []uint32

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// Option configures a Syncer.
type Option func(*Syncer)

// WithSyncInterval sets how often the anti-entropy loop runs.
func WithSyncInterval(d time.Duration) Option {
	return func(s *Syncer) { s.interval = d }
}

// WithRoundTimeout sets the per-peer timeout for the normal IBLT-diff path.
func WithRoundTimeout(d time.Duration) Option {
	return func(s *Syncer) { s.roundTimeout = d }
}

// WithTombstoneGrace sets the tombstone grace period (see TOMBSTONE_GRACE_MS
// in CLAUDE.md). Tombstones older than this are excluded from toPush/toPull
// during reconciliation, since the GC pass on either side may have already
// purged them. A zero value disables the filter.
func WithTombstoneGrace(d time.Duration) Option {
	return func(s *Syncer) {
		if d > 0 {
			s.tombstoneGraceMs = uint64(d.Milliseconds())
		}
	}
}

// New constructs a Syncer bound to ctx.
func New(ctx context.Context, n Node, pool *connpool.Pool, ownership Ownership, opts ...Option) *Syncer {
	s := &Syncer{
		node:             n,
		pool:             pool,
		ownership:        ownership,
		interval:         defaultSyncInterval,
		roundTimeout:     defaultRoundTimeout,
		fullStateTimeout: defaultFullStateTimeout,
		inflight:         make(map[peerPartition]struct{}),
		triggerCh:        make(chan []uint32, 16),
	}
	s.ctx, s.cancel = context.WithCancel(ctx)
	for _, opt := range opts {
		opt(s)
	}
	return s
}

type peerPartition struct {
	partitionId uint32
	peer        ports.MemberInfo
}

func (s *Syncer) tryAcquire(key peerPartition) bool {
	s.inflightMu.Lock()
	defer s.inflightMu.Unlock()
	if _, ok := s.inflight[key]; ok {
		return false
	}
	s.inflight[key] = struct{}{}
	return true
}

func (s *Syncer) release(key peerPartition) {
	s.inflightMu.Lock()
	delete(s.inflight, key)
	s.inflightMu.Unlock()
}

// Run starts the anti-entropy loop. Blocks until Close is called or ctx expires.
func (s *Syncer) Run() {
	s.wg.Go(func() {
		syncPairs := func(pairs []peerPartition) {
			rand.Shuffle(len(pairs), func(i, j int) { pairs[i], pairs[j] = pairs[j], pairs[i] })
			for _, pp := range pairs {
				if !s.tryAcquire(pp) {
					continue
				}
				pp := pp
				s.wg.Go(func() {
					defer s.release(pp)
					s.syncPair(pp.partitionId, pp.peer)
				})
			}
		}
		syncAll := func() { syncPairs(s.buildPairs()) }

		syncAll()
		ticker := time.NewTicker(s.interval)
		defer ticker.Stop()
		for {
			select {
			case <-s.ctx.Done():
				return
			case <-ticker.C:
				syncAll()
			case pids := <-s.triggerCh:
				syncPairs(s.pairsForPartitions(pids))
			}
		}
	})
}

// TriggerPartitions schedules an out-of-band sync round for the given
// partitions, e.g. when this node has just gained ownership and shouldn't
// wait up to interval to start converging. Best-effort: if the trigger queue
// is full the request is dropped and the next regular tick covers it.
func (s *Syncer) TriggerPartitions(pids []uint32) {
	select {
	case s.triggerCh <- pids:
	default:
	}
}

// pairsForPartitions returns the (partition, peer) pairs from buildPairs
// whose partitionId is in pids.
func (s *Syncer) pairsForPartitions(pids []uint32) []peerPartition {
	want := make(map[uint32]struct{}, len(pids))
	for _, pid := range pids {
		want[pid] = struct{}{}
	}
	var pairs []peerPartition
	for _, pp := range s.buildPairs() {
		if _, ok := want[pp.partitionId]; ok {
			pairs = append(pairs, pp)
		}
	}
	return pairs
}

// Close stops the anti-entropy loop and waits for the goroutine to exit.
func (s *Syncer) Close() {
	s.cancel()
	s.wg.Wait()
}

// buildPairs enumerates (partition, peer) pairs to sync. Each pair's
// reconciliation is bidirectional (push and pull both run), so only the
// lexicographically-lower ReplicaID needs to initiate — the other side's
// state is reconciled as a side effect. This halves anti-entropy traffic for
// every owned partition with no loss of convergence.
func (s *Syncer) buildPairs() []peerPartition {
	owned := s.ownership.Owned()
	localID := s.node.ReplicaID()
	var pairs []peerPartition
	for _, partitionId := range owned {
		for _, peer := range s.ownership.Peers(partitionId) {
			if localID >= peer.ReplicaID {
				continue
			}
			pairs = append(pairs, peerPartition{partitionId: partitionId, peer: peer})
		}
	}
	return pairs
}

// syncPair runs the three-step IBLT reconciliation for one (partition, peer) pair.
func (s *Syncer) syncPair(partitionId uint32, peer ports.MemberInfo) {
	ctx, cancel := context.WithTimeout(s.ctx, s.roundTimeout)
	defer cancel()

	conn, err := s.pool.Get(peer.GRPCAddr)
	if err != nil {
		slog.Warn("dial failed", "addr", peer.GRPCAddr, "err", err)
		return
	}
	client := repb.NewSyncServiceClient(conn)

	localSnap := s.node.IBLTSnapshot(partitionId)

	overviewResp, err := client.GetStateOverview(ctx, &repb.StateOverviewRequest{
		ReplicaId:   s.node.ReplicaID(),
		PartitionId: partitionId,
	})
	if err != nil {
		slog.Warn("GetStateOverview failed", "partitionId", partitionId, "addr", peer.GRPCAddr, "err", err)
		return
	}

	remoteIBLT, err := domiblt.DecodeIBLT(overviewResp.GetOverviewData())
	if err != nil {
		slog.Warn("decode state overview failed", "partitionId", partitionId, "addr", peer.GRPCAddr, "err", err)
		return
	}

	// fieldKey identifies a (key, field) pair fetched during reconciliation.
	type fieldKey struct{ key, field string }
	fetched := make(map[fieldKey]crdt.FieldEntry)
	getField := func(key, field string) (crdt.FieldEntry, bool) {
		entry, found, err := s.node.GetField(ctx, partitionId, key, field)
		if err != nil || !found {
			return crdt.FieldEntry{}, false
		}
		fetched[fieldKey{key, field}] = entry
		return entry, true
	}

	var cutoffMs uint64
	if s.tombstoneGraceMs > 0 {
		now := uint64(time.Now().UnixMilli())
		if now > s.tombstoneGraceMs {
			cutoffMs = now - s.tombstoneGraceMs
		}
	}

	toPush, toPull, fallback, err := domiblt.Reconcile(localSnap, remoteIBLT, getField, cutoffMs)
	if err != nil {
		// Most likely a peer with a different IBLT_CELLS. Log loudly and skip
		// this pair — falling back to a full exchange would mask the
		// misconfiguration. See CLAUDE.md: IBLT_CELLS is a cluster-wide
		// invariant, like NUM_PARTITIONS.
		slog.Warn("IBLT reconcile failed — check IBLT_CELLS matches across the cluster", "partitionId", partitionId, "peer", peer.ReplicaID, "addr", peer.GRPCAddr, "err", err)
		return
	}
	if fallback {
		slog.Info("IBLT diff too large, falling back to full exchange", "partitionId", partitionId, "peer", peer.ReplicaID)

		s.fullFallback(client, partitionId, peer)
		return
	}

	// Build push batch from the entries fetched during the staleness check.
	pushBatch := make([]*repb.DeltaEntry, 0, len(toPush))
	for _, id := range toPush {
		entry := fetched[fieldKey{id.Key, id.Field}]
		pushBatch = append(pushBatch, reproto.EntryToProto(id.Key, id.Field, entry))
	}

	// Build pull request.
	pullIDs := make([]*repb.ItemIdentifier, 0, len(toPull))
	for _, id := range toPull {
		pullIDs = append(pullIDs, &repb.ItemIdentifier{
			Key:        id.Key,
			Field:      id.Field,
			PhysicalMs: id.PhysicalMs,
			Logical:    id.Logical,
			ReplicaId:  id.ReplicaID,
		})
	}

	pushErr := make(chan error, 1)
	go func() { pushErr <- s.pushEntries(ctx, client, partitionId, pushBatch) }()

	if len(pullIDs) > 0 {
		s.pullAndApply(ctx, client, partitionId, peer, pullIDs)
	}

	if err := <-pushErr; err != nil {
		slog.Warn("PushEntries failed", "partitionId", partitionId, "addr", peer.GRPCAddr, "err", err)
	}
}

func (s *Syncer) fullFallback(client repb.SyncServiceClient, partitionId uint32, peer ports.MemberInfo) {
	ctx, cancel := context.WithTimeout(s.ctx, s.fullStateTimeout)
	defer cancel()

	pushErr := make(chan error, 1)
	go func() {
		pushErr <- s.openAndPush(ctx, client, partitionId, func(send func(*repb.DeltaEntry) error) error {
			return s.node.IteratePartition(ctx, partitionId, func(key, field string, entry crdt.FieldEntry) error {
				return send(reproto.EntryToProto(key, field, entry))
			})
		})
	}()

	s.pullAndApply(ctx, client, partitionId, peer, nil)

	if err := <-pushErr; err != nil {
		slog.Warn("full push failed", "partitionId", partitionId, "addr", peer.GRPCAddr, "err", err)
	}
}

func (s *Syncer) pullAndApply(ctx context.Context, client repb.SyncServiceClient, partitionId uint32, peer ports.MemberInfo, ids []*repb.ItemIdentifier) {
	stream, err := client.PullEntries(ctx, &repb.PullRequest{
		ReplicaId:   s.node.ReplicaID(),
		PartitionId: partitionId,
		Identifiers: ids,
	})
	if err != nil {
		slog.Warn("PullEntries failed", "partitionId", partitionId, "addr", peer.GRPCAddr, "err", err)
		return
	}
	received := 0
	for {
		d, err := stream.Recv()
		if err != nil {
			if err != io.EOF {
				slog.Warn("PullEntries recv failed", "partitionId", partitionId, "addr", peer.GRPCAddr, "err", err)
			}
			break
		}
		if _, err := s.node.ApplyDelta(ctx, partitionId, d.GetKey(), d.GetField(), reproto.ProtoToEntry(d)); err != nil {
			slog.Warn("apply delta failed", "key", d.GetKey(), "err", err)
		}
		received++
	}
	if ids == nil {
		slog.Debug("full pull complete", "partitionId", partitionId, "peer", peer.ReplicaID, "received", received)
	}
}

func (s *Syncer) openAndPush(ctx context.Context, client repb.SyncServiceClient, partitionId uint32, send func(func(*repb.DeltaEntry) error) error) error {
	stream, err := client.PushEntries(ctx)
	if err != nil {
		return err
	}
	if err := stream.Send(&repb.PushChunk{
		Payload: &repb.PushChunk_Header{Header: &repb.PushHeader{PartitionId: partitionId}},
	}); err != nil {
		return err
	}
	sendErr := send(func(e *repb.DeltaEntry) error {
		return stream.Send(&repb.PushChunk{Payload: &repb.PushChunk_Entry{Entry: e}})
	})
	if _, closeErr := stream.CloseAndRecv(); closeErr != nil && sendErr == nil {
		sendErr = closeErr
	}
	return sendErr
}

func (s *Syncer) pushEntries(ctx context.Context, client repb.SyncServiceClient, partitionId uint32, entries []*repb.DeltaEntry) error {
	if len(entries) == 0 {
		return nil
	}
	return s.openAndPush(ctx, client, partitionId, func(send func(*repb.DeltaEntry) error) error {
		for _, e := range entries {
			if err := send(e); err != nil {
				return err
			}
		}
		return nil
	})
}
