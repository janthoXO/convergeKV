package syncer

import (
	"context"
	"io"
	"log"
	"math/rand"
	"sync"
	"time"

	repb "github.com/janthoXO/convergeKV/gen/replication"
	"github.com/janthoXO/convergeKV/internal/connpool"
	"github.com/janthoXO/convergeKV/internal/crdt"
	"github.com/janthoXO/convergeKV/internal/gossip"
	"github.com/janthoXO/convergeKV/internal/iblt"
	"github.com/janthoXO/convergeKV/internal/node"
	"github.com/janthoXO/convergeKV/internal/partition"
)

const (
	defaultSyncInterval     = 2 * time.Second
	defaultRoundTimeout     = 30 * time.Second
	defaultFullStateTimeout = 5 * time.Minute
	defaultPushTimeout      = 2 * time.Second
)

// Syncer runs the IBLT-based anti-entropy loop and handles push-on-write.
// It owns all goroutines it spawns; call Close to drain them.
type Syncer struct {
	node      *node.Node
	gossip    *gossip.Gossip
	pool      *connpool.Pool
	ownership *Ownership

	numPartitions int

	interval         time.Duration
	roundTimeout     time.Duration
	fullStateTimeout time.Duration
	pushTimeout      time.Duration

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// Option configures a Syncer.
type Option func(*Syncer)

// WithContext sets the root context. The Syncer wraps it with its own cancel.
// Defaults to context.Background().
func WithContext(ctx context.Context) Option {
	return func(s *Syncer) { s.ctx, s.cancel = context.WithCancel(ctx) }
}

// WithSyncInterval sets how often the anti-entropy loop runs.
func WithSyncInterval(d time.Duration) Option {
	return func(s *Syncer) { s.interval = d }
}

// WithRoundTimeout sets the per-peer timeout for the normal IBLT-diff sync path.
func WithRoundTimeout(d time.Duration) Option {
	return func(s *Syncer) { s.roundTimeout = d }
}

// WithFullStateTimeout sets the timeout for the full-state fallback exchange.
func WithFullStateTimeout(d time.Duration) Option {
	return func(s *Syncer) { s.fullStateTimeout = d }
}

// WithPushTimeout sets the per-peer timeout for write-path push fanout.
func WithPushTimeout(d time.Duration) Option {
	return func(s *Syncer) { s.pushTimeout = d }
}

// New constructs a Syncer. Apply WithContext to bind it to a server lifetime.
func New(n *node.Node, g *gossip.Gossip, pool *connpool.Pool, ownership *Ownership, numPartitions int, opts ...Option) *Syncer {
	s := &Syncer{
		node:             n,
		gossip:           g,
		pool:             pool,
		ownership:        ownership,
		numPartitions:    numPartitions,
		interval:         defaultSyncInterval,
		roundTimeout:     defaultRoundTimeout,
		fullStateTimeout: defaultFullStateTimeout,
		pushTimeout:      defaultPushTimeout,
	}
	s.ctx, s.cancel = context.WithCancel(context.Background())

	for _, opt := range opts {
		opt(s)
	}
	return s
}

// peerPartition is a (partition, peer) pair to sync.
type peerPartition struct {
	partitionId uint32
	peer        gossip.MemberInfo
}

// Run starts the anti-entropy loop. Call in a goroutine.
// Stops when the context passed via WithContext is cancelled, or Close is called.
func (s *Syncer) Run() {
	s.wg.Go(func() {
		syncAll := func() {
			pairs := s.buildSyncPairs()
			// Shuffle to distribute load across peers evenly across rounds.
			rand.Shuffle(len(pairs), func(i, j int) { pairs[i], pairs[j] = pairs[j], pairs[i] })
			for _, pp := range pairs {
				s.SyncPartitionWithPeer(pp.partitionId, pp.peer)
			}
		}

		syncAll() // catch up immediately; don't wait for the first tick

		ticker := time.NewTicker(s.interval)
		defer ticker.Stop()
		for {
			select {
			case <-s.ctx.Done():
				return
			case <-ticker.C:
				syncAll()
			}
		}
	})
}

// buildSyncPairs returns all (partitionId, peer) combinations for owned partitions.
func (s *Syncer) buildSyncPairs() []peerPartition {
	owned := s.ownership.Owned()
	var pairs []peerPartition
	for _, partitionId := range owned {
		for _, peer := range s.ownership.CoOwners(partitionId) {
			pairs = append(pairs, peerPartition{partitionId: partitionId, peer: peer})
		}
	}
	return pairs
}

// Close cancels the syncer's context and waits for all goroutines to finish.
func (s *Syncer) Close() {
	s.cancel()
	s.wg.Wait()
}

// SyncPartitionWithPeer runs the three-step IBLT reconciliation protocol for
// a single partition as the initiator.
//
//  1. Fetch peer's IBLT for the partition via GetIBLT.
//  2. Compute the symmetric difference locally.
//  3. Concurrently push what the peer is missing and pull what we are missing.
//
// If the diff is undecodable (too large), falls back to a full-partition exchange.
func (s *Syncer) SyncPartitionWithPeer(partitionId uint32, peer gossip.MemberInfo) {
	ctx, cancel := context.WithTimeout(s.ctx, s.roundTimeout)
	defer cancel()

	conn, err := s.pool.Get(peer.GRPCAddr)
	if err != nil {
		log.Printf("[syncer] dial %s: %v", peer.GRPCAddr, err)
		return
	}
	client := repb.NewSyncServiceClient(conn)

	// ── Step 1: snapshot local IBLT before fetching remote ──────────────────
	localSnap := s.node.IBLTSnapshot(partitionId)

	ibltResp, err := client.GetIBLT(ctx, &repb.GetIBLTRequest{
		ReplicaId:   s.node.ReplicaID(),
		PartitionId: partitionId,
	})
	if err != nil {
		log.Printf("[syncer] GetIBLT partitionId=%d %s: %v", partitionId, peer.GRPCAddr, err)
		return
	}

	remoteIBLT, err := iblt.DecodeIBLT(ibltResp.GetIbltData())
	if err != nil {
		log.Printf("[syncer] decode remote IBLT partitionId=%d from %s: %v", partitionId, peer.GRPCAddr, err)
		return
	}

	// ── Step 2: compute symmetric difference locally ─────────────────────────
	diff := localSnap.SubtractUnsafe(remoteIBLT)
	onlyLocal, onlyRemote, ok := diff.Decode()

	if !ok {
		log.Printf("[syncer] IBLT diff too large partitionId=%d with %s — falling back to full partition exchange", partitionId, peer.ReplicaID)
		s.fullPartitionFallback(client, partitionId, peer)
		return
	}

	// ── Step 3a: build push batch (entries peer is missing) ─────────────────
	var pushBatch []*repb.DeltaEntry
	for _, itemBytes := range onlyLocal {
		key, field, replicaID, physMs, logical, deleted, valid := iblt.DeserialiseItem(itemBytes)
		if !valid {
			continue
		}
		entry, found, err := s.node.GetField(partitionId, key, field)
		if err != nil || !found {
			continue
		}
		// Only forward the exact version the IBLT encodes.
		if entry.Timestamp.PhysicalMs != physMs ||
			entry.Timestamp.Logical != logical ||
			entry.ReplicaID != replicaID ||
			entry.Deleted != deleted {
			continue
		}
		pushBatch = append(pushBatch, entryToProto(key, field, entry))
	}

	// ── Step 3b: build pull request (items we are missing) ──────────────────
	pullIDs := make([]*repb.ItemIdentifier, 0, len(onlyRemote))
	for _, itemBytes := range onlyRemote {
		key, field, replicaID, physMs, logical, _, valid := iblt.DeserialiseItem(itemBytes)
		if !valid {
			continue
		}
		pullIDs = append(pullIDs, &repb.ItemIdentifier{
			Key:        key,
			Field:      field,
			PhysicalMs: physMs,
			Logical:    logical,
			ReplicaId:  replicaID,
		})
	}

	// ── Step 3c: push and pull concurrently ─────────────────────────────────
	pushErr := make(chan error, 1)
	go func() {
		pushErr <- s.pushEntries(ctx, client, partitionId, pushBatch)
	}()

	// An empty identifiers list is the wire signal for "send your whole partition"
	// (full-partition fallback). Never send that on the normal diff path.
	if len(pullIDs) > 0 {
		s.pullAndApply(ctx, client, partitionId, peer, pullIDs)
	}

	if err := <-pushErr; err != nil {
		log.Printf("[syncer] PushEntries partitionId=%d to %s: %v", partitionId, peer.GRPCAddr, err)
	}
}

// openAndPush opens a PushEntries stream, sends the partition header, calls send
// to deliver entries, then closes the stream.
func (s *Syncer) openAndPush(ctx context.Context, client repb.SyncServiceClient, partitionId uint32, send func(func(*repb.DeltaEntry) error) error) error {
	stream, err := client.PushEntries(ctx)
	if err != nil {
		return err
	}
	if err := stream.Send(&repb.PushChunk{
		Payload: &repb.PushChunk_Header{
			Header: &repb.PushHeader{PartitionId: partitionId},
		},
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

// pushEntries sends a pre-built slice of entries to a peer for the given partition.
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

// pullAndApply calls PullEntries and applies every streamed entry.
// ids may be nil/empty to request the peer's full partition state (fallback).
func (s *Syncer) pullAndApply(ctx context.Context, client repb.SyncServiceClient, partitionId uint32, peer gossip.MemberInfo, ids []*repb.ItemIdentifier) {
	pullStream, err := client.PullEntries(ctx, &repb.PullRequest{
		ReplicaId:   s.node.ReplicaID(),
		PartitionId: partitionId,
		Identifiers: ids,
	})
	if err != nil {
		log.Printf("[syncer] PullEntries partitionId=%d %s: %v", partitionId, peer.GRPCAddr, err)
		return
	}
	received := 0
	for {
		d, err := pullStream.Recv()
		if err != nil {
			if err != io.EOF {
				log.Printf("[syncer] PullEntries recv partitionId=%d %s: %v", partitionId, peer.GRPCAddr, err)
			}
			break
		}
		s.applyDeltaEntry(partitionId, d)
		received++
	}
	if ids == nil {
		log.Printf("[syncer] full-partition pull partitionId=%d from %s: received %d entries", partitionId, peer.ReplicaID, received)
	}
}

// fullPartitionFallback exchanges complete partition snapshots when the IBLT
// diff is undecodable. Uses a longer dedicated timeout.
func (s *Syncer) fullPartitionFallback(client repb.SyncServiceClient, partitionId uint32, peer gossip.MemberInfo) {
	ctx, cancel := context.WithTimeout(s.ctx, s.fullStateTimeout)
	defer cancel()

	pushErr := make(chan error, 1)
	go func() {
		pushErr <- s.openAndPush(ctx, client, partitionId, func(send func(*repb.DeltaEntry) error) error {
			return s.node.IteratePartition(partitionId, func(key, field string, entry crdt.FieldEntry) error {
				return send(entryToProto(key, field, entry))
			})
		})
	}()

	s.pullAndApply(ctx, client, partitionId, peer, nil)

	if err := <-pushErr; err != nil {
		log.Printf("[syncer] full-partition push partitionId=%d to %s: %v", partitionId, peer.GRPCAddr, err)
	}
}

// PushToPeers sends entries directly to a set of replica peers (write-path fanout).
// Entries are grouped by partition before sending so each stream carries a
// consistent partition header.
func (s *Syncer) PushToPeers(_ context.Context, entries []*repb.DeltaEntry, replicas []gossip.MemberInfo) {
	byPartitionId := make(map[uint32][]*repb.DeltaEntry)
	for _, e := range entries {
		partitionId := partition.Of(e.GetKey(), s.numPartitions)
		byPartitionId[partitionId] = append(byPartitionId[partitionId], e)
	}

	localID := s.node.ReplicaID()
	for _, peer := range replicas {
		if peer.ReplicaID == localID {
			continue
		}
		peer := peer
		for partitionId, batch := range byPartitionId {
			partitionId, batch := partitionId, batch
			s.wg.Go(func() {
				ctx, cancel := context.WithTimeout(s.ctx, s.pushTimeout)
				defer cancel()
				conn, err := s.pool.Get(peer.GRPCAddr)
				if err != nil {
					log.Printf("[syncer] push dial %s: %v", peer.GRPCAddr, err)
					return
				}
				client := repb.NewSyncServiceClient(conn)
				if err := s.pushEntries(ctx, client, partitionId, batch); err != nil {
					log.Printf("[syncer] push partitionId=%d to %s: %v (IBLT sync will reconcile)", partitionId, peer.ReplicaID, err)
				}
			})
		}
	}
}

// ── helpers ──────────────────────────────────────────────────────────────────

func (s *Syncer) applyDeltaEntry(partitionId uint32, d *repb.DeltaEntry) {
	if _, err := s.node.ApplyDelta(partitionId, d.GetKey(), d.GetField(), protoToEntry(d)); err != nil {
		log.Printf("[syncer] apply delta key=%s field=%s: %v", d.GetKey(), d.GetField(), err)
	}
}
