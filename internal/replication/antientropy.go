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
	"github.com/janthoXO/convergeKV/internal/ring"
)

// AntiEntropy runs a background goroutine that periodically contacts each peer,
// exchanges Merkle partition hashes only for shared partitions (Phase 1), and
// merges only the delta entries for diverged shared partitions (Phase 2).
//
// Unlike Release 1, the peer list is resolved dynamically from gossip each tick,
// and shared-partition calculation prevents any data exchange with peers that
// share no ownership responsibility.
type AntiEntropy struct {
	node     *node.Node
	gossip   *gossip.Gossip
	ring     *ring.Ring
	context  *CausalContext
	interval time.Duration
}

// NewAntiEntropy returns an AntiEntropy runner.
// g and r may be nil in tests (falls back to nil-safe no-ops).
func NewAntiEntropy(n *node.Node, g *gossip.Gossip, r *ring.Ring, ctx *CausalContext, interval time.Duration) *AntiEntropy {
	return &AntiEntropy{node: n, gossip: g, ring: r, context: ctx, interval: interval}
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

// syncWithPeer runs the two-phase partition-aligned Merkle sync with a single peer.
func (ae *AntiEntropy) syncWithPeer(ctx context.Context, peerAddr, peerReplicaID string) {
	// Determine shared partitions before opening a connection.
	// If the ring is not yet built or we share nothing, skip entirely.
	var sharedPartitions []int
	if ae.ring != nil {
		sharedPartitions = ae.ring.SharedPartitions(ae.node.ReplicaID(), peerReplicaID)
	}
	if len(sharedPartitions) == 0 {
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

	// ── Phase 1: exchange partition hashes for shared partitions only ─────────

	myPartitionHashes := make(map[int32][]byte, len(sharedPartitions))
	for _, p := range sharedPartitions {
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
	// Compute this by comparing the peer's returned hashes against our local hashes.
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
		log.Printf("[antientropy] trees match with %s on %d shared partitions — skipping",
			peerReplicaID, len(sharedPartitions))
		return
	}

	// ── Phase 2a: pull from peer (partitions where peer has newer data) ───────

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
	// weDivergent tells us which of our partitions the peer is missing.
	// We don't push directly here; the peer's own Run() tick handles it
	// bidirectionally. (Release 3 will add an explicit push path.)
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
