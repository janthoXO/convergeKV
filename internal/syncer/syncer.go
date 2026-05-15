package syncer

import (
	"context"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	kvpb "github.com/janthoXO/convergeKV/gen/kv"
	repb "github.com/janthoXO/convergeKV/gen/replication"
	"github.com/janthoXO/convergeKV/internal/crdt"
	"github.com/janthoXO/convergeKV/internal/gossip"
	"github.com/janthoXO/convergeKV/internal/hlc"
	"github.com/janthoXO/convergeKV/internal/iblt"
	"github.com/janthoXO/convergeKV/internal/node"
)

// connPool is a simple connection pool for gRPC connections to peers.
type connPool struct {
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

func newConnPool() *connPool { return &connPool{conns: make(map[string]*grpc.ClientConn)} }

func (p *connPool) get(addr string) (*grpc.ClientConn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.conns[addr]; ok {
		return c, nil
	}
	c, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	p.conns[addr] = c
	return c, nil
}

func (p *connPool) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		c.Close()
	}
}

// Syncer runs the IBLT-based anti-entropy loop and handles push-on-write.
type Syncer struct {
	node      *node.Node
	gossip    *gossip.Gossip
	ibltState *IBLTState
	pool      *connPool
	rf        int
	interval  time.Duration
}

// NewSyncer constructs a Syncer.
func NewSyncer(n *node.Node, g *gossip.Gossip, ibltState *IBLTState, rf int, interval time.Duration) *Syncer {
	return &Syncer{
		node:      n,
		gossip:    g,
		ibltState: ibltState,
		pool:      newConnPool(),
		rf:        rf,
		interval:  interval,
	}
}

// Close shuts down all pooled gRPC connections.
func (s *Syncer) Close() { s.pool.close() }

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
			if s.gossip == nil {
				continue
			}
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
	conn, err := s.pool.get(peer.GRPCAddr)
	if err != nil {
		log.Printf("[syncer] dial %s: %v", peer.GRPCAddr, err)
		return
	}
	client := repb.NewSyncServiceClient(conn)

	// ── Round 1: send our IBLT, receive what they have vs what we need ─────────

	localSnap := s.ibltState.Snapshot()
	encoded := localSnap.Encode()

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
		entry, found := s.lookupEntry(id.GetKey(), id.GetField(),
			id.GetPhysicalMs(), id.GetLogical(), id.GetReplicaId())
		if found {
			toSend = append(toSend, entry)
		}
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
	conn, err := s.pool.get(peer.GRPCAddr)
	if err != nil {
		log.Printf("[syncer] fallback dial %s: %v", peer.GRPCAddr, err)
		return
	}
	client := repb.NewSyncServiceClient(conn)

	// Collect our full state.
	localRecords := s.node.Snapshot()
	entries := make([]*repb.DeltaEntry, 0, len(localRecords))
	for _, r := range localRecords {
		entries = append(entries, encodeEntry(r))
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
			conn, err := s.pool.get(peer.GRPCAddr)
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

// lookupEntry finds the current entry for (key, field) and verifies that the
// (physMs, logical, replicaID) match. Returns the proto entry if found.
func (s *Syncer) lookupEntry(key, field string, physMs uint64, logical uint32, replicaID string) (*repb.DeltaEntry, bool) {
	snap := s.node.Snapshot()
	for _, r := range snap {
		if r.Key == key && r.Field == field &&
			r.Entry.Timestamp.PhysicalMs == physMs &&
			r.Entry.Timestamp.Logical == logical &&
			r.Entry.ReplicaID == replicaID {
			return encodeEntry(r), true
		}
	}
	return nil, false
}

// encodeEntry converts a KeyFieldEntryTuple into a protobuf DeltaEntry.
func encodeEntry(r node.KeyFieldEntryTuple) *repb.DeltaEntry {
	return &repb.DeltaEntry{
		Key:       r.Key,
		Field:     r.Field,
		ValueJson: r.Entry.Value,
		Timestamp: &kvpb.HLCTimestamp{
			PhysicalMs: r.Entry.Timestamp.PhysicalMs,
			Logical:    r.Entry.Timestamp.Logical,
		},
		ReplicaId: r.Entry.ReplicaID,
		Deleted:   r.Entry.Deleted,
	}
}

// IBLTFromEncoded deserialises an IBLT from wire bytes.
func IBLTFromEncoded(data []byte) (*iblt.IBLT, error) {
	return iblt.DecodeIBLT(data)
}
