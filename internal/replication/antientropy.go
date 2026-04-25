package replication

import (
	"context"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	repb "github.com/janthoXO/convergeKV/gen/replication"
	"github.com/janthoXO/convergeKV/internal/crdt"
	"github.com/janthoXO/convergeKV/internal/hlc"
	"github.com/janthoXO/convergeKV/internal/merkle"
	"github.com/janthoXO/convergeKV/internal/node"
)

// AntiEntropy runs a background goroutine that periodically contacts each peer,
// exchanges Merkle bucket hashes (Phase 1), and merges only the delta entries
// for diverged buckets (Phase 2).
type AntiEntropy struct {
	node     *node.Node
	peers    []string // host:port addresses
	context  *CausalContext
	interval time.Duration
}

// NewAntiEntropy returns an AntiEntropy runner.
func NewAntiEntropy(n *node.Node, peers []string, ctx *CausalContext, interval time.Duration) *AntiEntropy {
	return &AntiEntropy{node: n, peers: peers, context: ctx, interval: interval}
}

// Run starts the anti-entropy loop. Call in a goroutine. Stops when ctx is cancelled.
func (ae *AntiEntropy) Run(ctx context.Context) {
	ticker := time.NewTicker(ae.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, peer := range ae.peers {
				ae.syncWithPeer(ctx, peer)
			}
		}
	}
}

// syncWithPeer runs the two-phase Merkle sync with a single peer.
func (ae *AntiEntropy) syncWithPeer(ctx context.Context, addr string) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("[antientropy] dial %s: %v", addr, err)
		return
	}
	defer conn.Close()

	client := repb.NewReplicationServiceClient(conn)

	// ── Phase 1: exchange bucket hashes ──────────────────────────────────────

	myHashes := ae.node.MerkleTree().AllBucketHashes()
	myHashesBytes := make([][]byte, merkle.NumBuckets)
	for i, h := range myHashes {
		cp := make([]byte, 32)
		copy(cp, h[:])
		myHashesBytes[i] = cp
	}

	hashResp, err := client.HashSync(ctx, &repb.HashSyncRequest{
		ReplicaId:    ae.node.ReplicaID(),
		BucketHashes: myHashesBytes,
	})
	if err != nil {
		log.Printf("[antientropy] HashSync %s: %v", addr, err)
		return
	}

	// Compute which buckets WE are missing from the peer (peer's hashes vs. our tree).
	peerHashes := make([]merkle.Hash, merkle.NumBuckets)
	for i, b := range hashResp.BucketHashes {
		copy(peerHashes[i][:], b)
	}
	bucketsWeNeed := ae.node.MerkleTree().DivergentBuckets(peerHashes)

	if len(hashResp.DivergentBuckets) == 0 && len(bucketsWeNeed) == 0 {
		// Trees are identical — nothing to do this round.
		log.Printf("[antientropy] trees match with %s — skipping", addr)
		return
	}

	// ── Phase 2a: fetch entries the peer has that we don't ───────────────────

	if len(bucketsWeNeed) > 0 {
		bucketsInt32 := make([]int32, len(bucketsWeNeed))
		for i, b := range bucketsWeNeed {
			bucketsInt32[i] = int32(b)
		}

		deltaResp, err := client.DeltaSync(ctx, &repb.DeltaSyncRequest{
			ReplicaId: ae.node.ReplicaID(),
			Buckets:   bucketsInt32,
		})
		if err != nil {
			log.Printf("[antientropy] DeltaSync (pull) %s: %v", addr, err)
			// Do not return — still acknowledge Phase 2b below.
		} else {
			ae.applyDeltas(deltaResp.GetDeltas())
		}
	}

	// ── Phase 2b: peer will pull from us in its own sync round ───────────────
	// The peer's DivergentBuckets field tells us which of OUR buckets the peer
	// differs on. We acknowledge this but do not push directly; the peer will
	// pull in its own anti-entropy tick (bidirectional sync is guaranteed since
	// every node runs the loop against every peer).
	// NOTE: Phase 3 of the roadmap will add a true push path.
	_ = hashResp.DivergentBuckets // acknowledged, handled by peer's own sync round
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
