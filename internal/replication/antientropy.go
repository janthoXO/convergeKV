package replication

import (
	"context"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	kvpb "github.com/janthoXO/convergeKV/gen/kv"
	repb "github.com/janthoXO/convergeKV/gen/replication"
	"github.com/janthoXO/convergeKV/internal/crdt"
	"github.com/janthoXO/convergeKV/internal/hlc"
	"github.com/janthoXO/convergeKV/internal/node"
)

// AntiEntropy runs a background goroutine that periodically contacts each peer,
// sends the local causal context, and merges the returned deltas.
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

// syncWithPeer dials a single peer, sends this node's causal context,
// and applies all returned delta entries to local state.
func (ae *AntiEntropy) syncWithPeer(ctx context.Context, addr string) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("[antientropy] dial %s: %v", addr, err)
		return
	}
	defer conn.Close()

	client := repb.NewReplicationServiceClient(conn)

	// Build the SyncRequest with our current causal context.
	localCtx := ae.context.Snapshot()
	seenPb := make(map[string]*kvpb.HLCTimestamp, len(localCtx))
	for rid, ts := range localCtx {
		seenPb[rid] = &kvpb.HLCTimestamp{PhysicalMs: ts.PhysicalMs, Logical: ts.Logical}
	}

	resp, err := client.Sync(ctx, &repb.SyncRequest{
		ReplicaId: ae.node.ReplicaID(),
		Context:   &repb.CausalContext{Seen: seenPb},
	})
	if err != nil {
		log.Printf("[antientropy] sync %s: %v", addr, err)
		return
	}

	// Apply each received delta.
	for _, d := range resp.GetDeltas() {
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
			log.Printf("[antientropy] apply delta: %v", err)
		}

		// Update our causal context with the newly seen entry.
		ae.context.Update(d.GetReplicaId(), ts)
	}

	// Also update our context from the peer's returned context.
	for rid, pts := range resp.GetContext().GetSeen() {
		ae.context.Update(rid, hlc.Timestamp{
			PhysicalMs: pts.GetPhysicalMs(),
			Logical:    pts.GetLogical(),
		})
	}
}
