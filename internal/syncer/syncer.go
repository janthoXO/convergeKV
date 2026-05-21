package syncer

import (
	"context"
	"io"
	"log"
	"sync"
	"time"

	repb "github.com/janthoXO/convergeKV/gen/replication"
	"github.com/janthoXO/convergeKV/internal/connpool"
	"github.com/janthoXO/convergeKV/internal/crdt"
	"github.com/janthoXO/convergeKV/internal/gossip"
	"github.com/janthoXO/convergeKV/internal/iblt"
	"github.com/janthoXO/convergeKV/internal/node"
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
	node   *node.Node
	gossip *gossip.Gossip
	pool   *connpool.Pool

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
func New(n *node.Node, g *gossip.Gossip, pool *connpool.Pool, opts ...Option) *Syncer {
	s := &Syncer{
		node:             n,
		gossip:           g,
		pool:             pool,
		interval:         defaultSyncInterval,
		roundTimeout:     defaultRoundTimeout,
		fullStateTimeout: defaultFullStateTimeout,
		pushTimeout:      defaultPushTimeout,
	}
	// Default context — callers should override with WithContext(serverCtx).
	s.ctx, s.cancel = context.WithCancel(context.Background())

	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Run starts the anti-entropy loop. Call in a goroutine.
// Stops when the context passed via WithContext is cancelled, or Close is called.
func (s *Syncer) Run() {
	s.wg.Go(func() {
		localID := s.node.ReplicaID()
		syncAll := func() {
			for _, peer := range s.gossip.Members() {
				if peer.ReplicaID == localID {
					continue
				}

				s.SyncWithPeer(peer)
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

// Close cancels the syncer's context and waits for all goroutines to finish.
func (s *Syncer) Close() {
	s.cancel()
	s.wg.Wait()
}

// SyncWithPeer runs the three-step IBLT reconciliation protocol as the initiator.
//
//  1. Fetch peer's IBLT via GetIBLT.
//  2. Compute the symmetric difference locally.
//  3. Concurrently push what the peer is missing and pull what we are missing.
//
// If the diff is undecodable (too large), falls back to a full-state exchange.
func (s *Syncer) SyncWithPeer(peer gossip.MemberInfo) {
	ctx, cancel := context.WithTimeout(s.ctx, s.roundTimeout)
	defer cancel()

	conn, err := s.pool.Get(peer.GRPCAddr)
	if err != nil {
		log.Printf("[syncer] dial %s: %v", peer.GRPCAddr, err)
		return
	}
	client := repb.NewSyncServiceClient(conn)

	// ── Step 1: snapshot local IBLT before fetching remote ──────────────────
	// Must be taken first so the diff reflects a consistent reference point.
	localSnap := s.node.IBLTSnapshot()

	ibltResp, err := client.GetIBLT(ctx, &repb.GetIBLTRequest{ReplicaId: s.node.ReplicaID()})
	if err != nil {
		log.Printf("[syncer] GetIBLT %s: %v", peer.GRPCAddr, err)
		return
	}

	remoteIBLT, err := iblt.DecodeIBLT(ibltResp.GetIbltData())
	if err != nil {
		log.Printf("[syncer] decode remote IBLT from %s: %v", peer.GRPCAddr, err)
		return
	}

	// ── Step 2: compute symmetric difference locally ─────────────────────────
	diff := localSnap.SubtractUnsafe(remoteIBLT)
	onlyLocal, onlyRemote, ok := diff.Decode()

	if !ok {
		log.Printf("[syncer] IBLT diff too large with %s — falling back to full state exchange", peer.ReplicaID)
		s.fullStateFallback(client, peer)
		return
	}

	// ── Step 3a: build push batch (entries peer is missing) ─────────────────
	var pushBatch []*repb.DeltaEntry
	for _, itemBytes := range onlyLocal {
		key, field, replicaID, physMs, logical, deleted, valid := iblt.DeserialiseItem(itemBytes)
		if !valid {
			continue
		}
		entry, found, err := s.node.GetField(key, field)
		if err != nil || !found {
			continue
		}
		// Only forward the exact version the IBLT encodes; if it's been superseded
		// since the snapshot was taken, the next sync round will reconcile it.
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
	// Run push in a goroutine so it overlaps with pull.
	// pushEntries is a no-op for an empty batch so this is always safe to spawn.
	pushErr := make(chan error, 1)
	go func() {
		pushErr <- s.pushEntries(ctx, client, pushBatch)
	}()

	// Only call PullEntries when there are specific items to fetch.
	// An empty identifiers list is the wire signal for "send your whole state"
	// (full-state fallback). Never send that on the normal diff path.
	if len(pullIDs) > 0 {
		s.pullAndApply(ctx, client, peer, pullIDs)
	}

	if err := <-pushErr; err != nil {
		log.Printf("[syncer] PushEntries to %s: %v", peer.GRPCAddr, err)
	}
}

// pushEntries opens a client-streaming PushEntries call and sends each entry.
func (s *Syncer) pushEntries(ctx context.Context, client repb.SyncServiceClient, entries []*repb.DeltaEntry) error {
	if len(entries) == 0 {
		return nil
	}
	stream, err := client.PushEntries(ctx)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if err := stream.Send(e); err != nil {
			return err
		}
	}
	if _, err := stream.CloseAndRecv(); err != nil {
		return err
	}
	return nil
}

// pullAndApply calls PullEntries and applies every streamed entry.
// ids may be empty to request the peer's full state (fallback).
func (s *Syncer) pullAndApply(ctx context.Context, client repb.SyncServiceClient, peer gossip.MemberInfo, ids []*repb.ItemIdentifier) {
	pullStream, err := client.PullEntries(ctx, &repb.PullRequest{
		ReplicaId:   s.node.ReplicaID(),
		Identifiers: ids,
	})
	if err != nil {
		log.Printf("[syncer] PullEntries %s: %v", peer.GRPCAddr, err)
		return
	}
	received := 0
	for {
		d, err := pullStream.Recv()
		if err != nil {
			if err != io.EOF {
				log.Printf("[syncer] PullEntries recv %s: %v", peer.GRPCAddr, err)
			}
			break
		}
		s.applyDeltaEntry(d)
		received++
	}
	if ids == nil {
		log.Printf("[syncer] full-state pull from %s complete: received %d entries", peer.ReplicaID, received)
	}
}

// fullStateFallback exchanges complete snapshots when the IBLT diff is undecodable.
// Uses a longer dedicated timeout since a full store iteration can exceed the round budget.
// client is already dialed by the caller; we reuse it to avoid a redundant dial.
func (s *Syncer) fullStateFallback(client repb.SyncServiceClient, peer gossip.MemberInfo) {
	ctx, cancel := context.WithTimeout(s.ctx, s.fullStateTimeout)
	defer cancel()

	pushErr := make(chan error, 1)
	go func() {
		stream, err := client.PushEntries(ctx)
		if err != nil {
			pushErr <- err
			return
		}
		iterErr := s.node.IterateAll(func(key, field string, entry crdt.FieldEntry) error {
			return stream.Send(entryToProto(key, field, entry))
		})
		if _, closeErr := stream.CloseAndRecv(); closeErr != nil && iterErr == nil {
			iterErr = closeErr
		}
		pushErr <- iterErr
	}()

	s.pullAndApply(ctx, client, peer, nil)

	if err := <-pushErr; err != nil {
		log.Printf("[syncer] full-state push to %s: %v", peer.GRPCAddr, err)
	}
}

// PushToPeers sends entries directly to a set of replica peers (write-path fanout).
// Each peer runs in its own tracked goroutine with a per-peer pushTimeout.
// The caller's context is intentionally not forwarded — pushes must outlive the
// inbound RPC, and the syncer's own context (cancelled by Close) is the root.
func (s *Syncer) PushToPeers(_ context.Context, entries []*repb.DeltaEntry, replicas []gossip.MemberInfo, localID string) {
	for _, peer := range replicas {
		if peer.ReplicaID == localID {
			continue
		}
		peer := peer
		s.wg.Go(func() {
			ctx, cancel := context.WithTimeout(s.ctx, s.pushTimeout)
			defer cancel()
			conn, err := s.pool.Get(peer.GRPCAddr)
			if err != nil {
				log.Printf("[syncer] push dial %s: %v", peer.GRPCAddr, err)
				return
			}
			client := repb.NewSyncServiceClient(conn)
			if err := s.pushEntries(ctx, client, entries); err != nil {
				log.Printf("[syncer] push to %s: %v (IBLT sync will reconcile)", peer.ReplicaID, err)
			}
		})
	}
}

// ── helpers ──────────────────────────────────────────────────────────────────

func (s *Syncer) applyDeltaEntry(d *repb.DeltaEntry) {
	if _, err := s.node.ApplyDelta(d.GetKey(), d.GetField(), protoToEntry(d)); err != nil {
		log.Printf("[syncer] apply delta key=%s field=%s: %v", d.GetKey(), d.GetField(), err)
	}
}
