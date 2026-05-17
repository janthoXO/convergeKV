package replication

import (
	"context"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	repb "github.com/janthoXO/convergeKV/gen/replication"
	"github.com/janthoXO/convergeKV/internal/crdt"
	"github.com/janthoXO/convergeKV/internal/gossip"
	"github.com/janthoXO/convergeKV/internal/hlc"
	"github.com/janthoXO/convergeKV/internal/merkle"
	"github.com/janthoXO/convergeKV/internal/node"
	"github.com/janthoXO/convergeKV/internal/partition"
)

// AntiEntropy runs a background goroutine that periodically contacts each peer,
// exchanges Merkle partition hashes only for shared slots (Phase 1), and
// merges only the delta entries for diverged shared slots (Phase 2).
//
// Shared slot calculation uses SlotMap.SharedSlots — an O(NSlots×RF) scan
// over the fixed 4096-slot map. This replaces the ring.SharedPartitions midpoint
// approximation with an exact lookup.
type AntiEntropy struct {
	node     *node.Node
	gossip   *gossip.Gossip
	slotMap  func() partition.SlotMap // returns current slot map
	context  *CausalContext
	interval time.Duration
}

// NewAntiEntropy returns an AntiEntropy runner.
// g and getSlotMap may be nil in tests (falls back to nil-safe no-ops).
func NewAntiEntropy(n *node.Node, g *gossip.Gossip, getSlotMap func() partition.SlotMap, ctx *CausalContext, interval time.Duration) *AntiEntropy {
	return &AntiEntropy{node: n, gossip: g, slotMap: getSlotMap, context: ctx, interval: interval}
}

// Run starts the anti-entropy loop. Call in a goroutine. Stops when ctx is cancelled.
func (ae *AntiEntropy) Run(ctx context.Context) {
	ticker := time.NewTicker(ae.interval)
	defer ticker.Stop()
	localID := ae.node.ReplicaID()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if ae.gossip == nil {
				continue
			}
			for _, member := range ae.gossip.Members() {
				if member.ReplicaID == localID {
					continue // skip self
				}
				ae.syncWithPeer(ctx, member.GRPCAddr, member.ReplicaID)
			}
		}
	}
}

// syncWithPeer runs the two-phase slot-aligned Merkle sync with a single peer.
func (ae *AntiEntropy) syncWithPeer(ctx context.Context, peerAddr, peerReplicaID string) {
	// Determine shared slots before opening a connection.
	var sharedSlots []int
	if ae.slotMap != nil {
		sm := ae.slotMap()
		sharedSlots = sm.SharedSlots(ae.node.ReplicaID(), peerReplicaID)
	}
	if len(sharedSlots) == 0 {
		// No shared data with this peer — nothing to sync.
		return
	}

	conn, err := grpc.NewClient(peerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("[antientropy] dial %s: %v", peerAddr, err)
		return
	}
	defer conn.Close()

	client := repb.NewReplicationServiceClient(conn)

	// ── Phase 1: exchange partition hashes for shared slots only ──────────────

	myPartitionHashes := make(map[int32][]byte, len(sharedSlots))
	for _, p := range sharedSlots {
		h := ae.node.MerkleTree().PartitionHash(p)
		b := make([]byte, 32)
		copy(b, h[:])
		myPartitionHashes[int32(p)] = b
	}

	hashResp, err := client.HashSync(ctx, &repb.HashSyncRequest{
		ReplicaId:       ae.node.ReplicaID(),
		PartitionHashes: myPartitionHashes,
	})
	if err != nil {
		log.Printf("[antientropy] HashSync %s: %v", peerAddr, err)
		return
	}

	// Partitions where the peer's hash differs from ours = we need to pull from peer.
	peerDivergent := hashResp.GetDivergentPartitions()

	// Partitions where our hash differs from the peer's = peer needs to pull from us.
	var weDivergent []int32
	for partInt32, peerHashBytes := range hashResp.GetPartitionHashes() {
		p := int(partInt32)
		myHash := ae.node.MerkleTree().PartitionHash(p)
		var peerHash merkle.Hash
		copy(peerHash[:], peerHashBytes)
		if myHash != peerHash {
			weDivergent = append(weDivergent, partInt32)
		}
	}

	if len(peerDivergent) == 0 && len(weDivergent) == 0 {
		log.Printf("[antientropy] trees match with %s on %d shared slots — skipping",
			peerReplicaID, len(sharedSlots))
		return
	}

	// ── Phase 2a: pull from peer (slots where peer has newer data) ────────────

	if len(peerDivergent) > 0 {
		deltaResp, err := client.DeltaSync(ctx, &repb.DeltaSyncRequest{
			ReplicaId:   ae.node.ReplicaID(),
			Partitions:  peerDivergent,
			RequesterId: ae.node.ReplicaID(),
		})
		if err != nil {
			log.Printf("[antientropy] DeltaSync (pull) %s: %v", peerAddr, err)
		} else {
			ae.applyDeltas(deltaResp.GetDeltas())
		}
	}

	// ── Phase 2b: peer will pull from us in its own anti-entropy tick ─────────
	_ = weDivergent
}

// applyDeltas merges a slice of incoming DeltaEntry proto messages into local state.
func (ae *AntiEntropy) applyDeltas(deltas []*repb.DeltaEntry) {
	for _, d := range deltas {
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
		if _, err := ae.node.ApplyDelta(d.GetKey(), d.GetField(), entry); err != nil {
			log.Printf("[antientropy] apply delta key=%s field=%s: %v", d.GetKey(), d.GetField(), err)
		}
		ae.context.Update(d.GetReplicaId(), ts)
	}
}
