package syncer

import (
	"context"
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
	rf        int
	interval  time.Duration
}

// NewSyncer constructs a Syncer backed by the given shared connection pool.
func NewSyncer(n *node.Node, g *gossip.Gossip, ibltState *IBLTState, store *storage.Store, pool *connpool.Pool, rf int, interval time.Duration) *Syncer {
	return &Syncer{
		node:      n,
		gossip:    g,
		ibltState: ibltState,
		store:     store,
		pool:      pool,
		rf:        rf,
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

// SyncWithPeer runs the two-round IBLT reconciliation protocol as the initiator.
func (s *Syncer) SyncWithPeer(ctx context.Context, peer gossip.MemberInfo) {
	conn, err := s.pool.Get(peer.GRPCAddr)
	if err != nil {
		log.Printf("[syncer] dial %s: %v", peer.GRPCAddr, err)
		return
	}
	client := repb.NewSyncServiceClient(conn)

	// ── Round 1: send our IBLT, receive what they have vs what we need ─────────
	encoded := s.ibltState.t.Encode()

	resp, err := client.IBLTExchange(ctx, &repb.IBLTExchangeRequest{
		ReplicaId: s.node.ReplicaID(),
		IbltData:  encoded,
	})
	if err != nil {
		log.Printf("[syncer] IBLTExchange %s: %v", peer.GRPCAddr, err)
		return
	}

	if !resp.GetDecodable() {
		log.Printf("[syncer] IBLT diff too large with %s — falling back to full state sync", peer.ReplicaID)
		s.FullStateFallback(ctx, peer)
		return
	}

	// Apply entries the peer sent us (items they have that we don't).
	for _, d := range resp.GetItemsForInitiator() {
		s.applyDeltaEntry(d)
	}

	// ── Round 2: send entries the peer is missing ────────────────────────────

	iNeed := resp.GetINeed()
	if len(iNeed) == 0 {
		return
	}

	var toSend []*repb.DeltaEntry
	for _, id := range iNeed {
		entry, found, err := s.store.GetField(id.GetKey(), id.GetField())
		if err != nil {
			log.Printf("[syncer] GetField key=%s field=%s: %v", id.GetKey(), id.GetField(), err)
			continue
		}
		if !found {
			continue
		}
		// Verify it's the exact version the peer expects.
		if entry.Timestamp.PhysicalMs != id.GetPhysicalMs() ||
			entry.Timestamp.Logical != id.GetLogical() ||
			entry.ReplicaID != id.GetReplicaId() {
			continue
		}
		toSend = append(toSend, entryToProto(id.GetKey(), id.GetField(), entry))
	}

	if len(toSend) == 0 {
		return
	}

	if _, err := client.PushEntries(ctx, &repb.PushEntriesRequest{
		ReplicaId: s.node.ReplicaID(),
		Entries:   toSend,
	}); err != nil {
		log.Printf("[syncer] PushEntries (round 2) to %s: %v", peer.GRPCAddr, err)
	}
}

// FullStateFallback exchanges complete snapshots when IBLT decode fails.
func (s *Syncer) FullStateFallback(ctx context.Context, peer gossip.MemberInfo) {
	conn, err := s.pool.Get(peer.GRPCAddr)
	if err != nil {
		log.Printf("[syncer] fallback dial %s: %v", peer.GRPCAddr, err)
		return
	}
	client := repb.NewSyncServiceClient(conn)

	// Stream our full state directly from Badger.
	var entries []*repb.DeltaEntry
	if err := s.store.IterateAll(func(key, field string, entry crdt.FieldEntry) error {
		entries = append(entries, entryToProto(key, field, entry))
		return nil
	}); err != nil {
		log.Printf("[syncer] fallback iterate: %v", err)
		return
	}

	resp, err := client.FullStateSync(ctx, &repb.FullStateSyncRequest{
		ReplicaId: s.node.ReplicaID(),
		Entries:   entries,
	})
	if err != nil {
		log.Printf("[syncer] FullStateSync %s: %v", peer.GRPCAddr, err)
		return
	}

	for _, d := range resp.GetEntries() {
		s.applyDeltaEntry(d)
	}
	log.Printf("[syncer] full state fallback with %s complete: received %d entries", peer.ReplicaID, len(resp.GetEntries()))
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
			if _, err := client.PushEntries(ctx, &repb.PushEntriesRequest{
				ReplicaId: localID,
				Entries:   entries,
			}); err != nil {
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
