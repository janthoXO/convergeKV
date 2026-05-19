package syncer

import (
	"context"
	"io"
	"log"
	"time"

	repb "github.com/janthoXO/convergeKV/gen/replication"
	"github.com/janthoXO/convergeKV/internal/connpool"
	"github.com/janthoXO/convergeKV/internal/crdt"
	"github.com/janthoXO/convergeKV/internal/gossip"
	"github.com/janthoXO/convergeKV/internal/hlc"
	"github.com/janthoXO/convergeKV/internal/iblt"
	"github.com/janthoXO/convergeKV/internal/node"
	"github.com/janthoXO/convergeKV/internal/storage"
)

// Syncer runs the IBLT-based anti-entropy loop and handles push-on-write.
type Syncer struct {
	node      *node.Node
	gossip    *gossip.Gossip
	ibltState *IBLTState
	store     *storage.Store
	pool      *connpool.Pool
	interval  time.Duration
}

// NewSyncer constructs a Syncer backed by the given shared connection pool.
func NewSyncer(n *node.Node, g *gossip.Gossip, ibltState *IBLTState, store *storage.Store, pool *connpool.Pool, interval time.Duration) *Syncer {
	return &Syncer{
		node:      n,
		gossip:    g,
		ibltState: ibltState,
		store:     store,
		pool:      pool,
		interval:  interval,
	}
}

// Close is a no-op; the caller owns the pool's lifecycle.
func (s *Syncer) Close() {}

// Run starts the anti-entropy loop. Call in a goroutine. Stops when ctx is cancelled.
func (s *Syncer) Run(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	localID := s.node.ReplicaID()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, peer := range s.gossip.Members() {
				if peer.ReplicaID == localID {
					continue // skip self
				}
				s.SyncWithPeer(ctx, peer)
			}
		}
	}
}

// SyncWithPeer runs the three-step IBLT reconciliation protocol as the initiator.
//
//  1. Fetch peer's IBLT via GetIBLT.
//  2. Compute the symmetric difference locally.
//  3. Concurrently push what the peer is missing and pull what we are missing.
//
// If the diff is undecodable (too large), falls back to a full-state exchange:
// push our entire store and pull the peer's entire store.
func (s *Syncer) SyncWithPeer(ctx context.Context, peer gossip.MemberInfo) {
	conn, err := s.pool.Get(peer.GRPCAddr)
	if err != nil {
		log.Printf("[syncer] dial %s: %v", peer.GRPCAddr, err)
		return
	}
	client := repb.NewSyncServiceClient(conn)

	// ── Step 1: fetch peer's IBLT ────────────────────────────────────────────
	ibltResp, err := client.GetIBLT(ctx, &repb.GetIBLTRequest{ReplicaId: s.node.ReplicaID()})
	if err != nil {
		log.Printf("[syncer] GetIBLT %s: %v", peer.GRPCAddr, err)
		return
	}

	remoteIBLT, err := IBLTFromEncoded(ibltResp.GetIbltData())
	if err != nil {
		log.Printf("[syncer] decode remote IBLT from %s: %v", peer.GRPCAddr, err)
		return
	}

	// ── Step 2: compute symmetric difference locally ─────────────────────────
	localSnap := s.ibltState.Snapshot()
	diff := localSnap.SubtractUnsafe(remoteIBLT)
	onlyLocal, onlyRemote, ok := diff.Decode()

	if !ok {
		log.Printf("[syncer] IBLT diff too large with %s — falling back to full state exchange", peer.ReplicaID)
		s.fullStateFallback(ctx, client, peer)
		return
	}

	// ── Step 3a: build push batch (entries peer is missing) ─────────────────
	var pushBatch []*repb.DeltaEntry
	for _, itemBytes := range onlyLocal {
		key, field, replicaID, physMs, logical, deleted, valid := DeserialiseItem(itemBytes)
		if !valid {
			continue
		}
		entry, found, err := s.store.GetField(key, field)
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
		key, field, replicaID, physMs, logical, _, valid := DeserialiseItem(itemBytes)
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
		pushErr <- s.pushEntries(ctx, client, pushBatch)
	}()

	s.pullAndApply(ctx, client, peer, pullIDs)

	if err := <-pushErr; err != nil {
		log.Printf("[syncer] PushEntries to %s: %v", peer.GRPCAddr, err)
	}
}

// pushEntries opens a client-streaming PushEntries call and sends each entry.
// entries may be nil/empty, in which case the call is skipped.
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
	if len(ids) == 0 {
		log.Printf("[syncer] full-state pull from %s complete: received %d entries", peer.ReplicaID, received)
	}
}

// fullStateFallback exchanges complete snapshots when the IBLT diff is undecodable.
// Concurrently pushes our full store to the peer and pulls the peer's full store.
func (s *Syncer) fullStateFallback(ctx context.Context, client repb.SyncServiceClient, peer gossip.MemberInfo) {
	pushErr := make(chan error, 1)
	go func() {
		stream, err := client.PushEntries(ctx)
		if err != nil {
			pushErr <- err
			return
		}
		iterErr := s.store.IterateAll(func(key, field string, entry crdt.FieldEntry) error {
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

// PushToPeers sends entries directly to a set of replica peers (fire-and-forget).
// Called from the coordinator after a successful local write.
func (s *Syncer) PushToPeers(ctx context.Context, entries []*repb.DeltaEntry, replicas []gossip.MemberInfo, localID string) {
	for _, peer := range replicas {
		if peer.ReplicaID == localID {
			continue
		}
		peer := peer // capture for goroutine
		go func() {
			conn, err := s.pool.Get(peer.GRPCAddr)
			if err != nil {
				log.Printf("[syncer] push dial %s: %v", peer.GRPCAddr, err)
				return
			}
			client := repb.NewSyncServiceClient(conn)
			if err := s.pushEntries(ctx, client, entries); err != nil {
				log.Printf("[syncer] push to %s: %v (IBLT sync will reconcile)", peer.ReplicaID, err)
			}
		}()
	}
}

// ── helpers ──────────────────────────────────────────────────────────────────

func (s *Syncer) applyDeltaEntry(d *repb.DeltaEntry) {
	ts := hlc.Timestamp{
		PhysicalMs: d.GetTimestamp().GetPhysicalMs(),
		Logical:    d.GetTimestamp().GetLogical(),
	}
	entry := crdt.FieldEntry{
		Value:     d.GetValueJson(),
		Timestamp: ts,
		ReplicaID: d.GetReplicaId(),
		Deleted:   d.GetDeleted(),
	}
	if _, err := s.node.ApplyDelta(d.GetKey(), d.GetField(), entry); err != nil {
		log.Printf("[syncer] apply delta key=%s field=%s: %v", d.GetKey(), d.GetField(), err)
	}
}

// IBLTFromEncoded deserialises an IBLT from wire bytes.
func IBLTFromEncoded(data []byte) (*iblt.IBLT, error) {
	return iblt.DecodeIBLT(data)
}
