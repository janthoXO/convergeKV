// Package coordinator routes client requests using Rendezvous Hashing (HRW)
// over virtual partitions.  Any replica in the HRW set serves reads and writes
// (quorum=1). If the local node is not a replica the request is forwarded to
// the highest-scoring HRW member.
//
// The Coordinator API is domain-typed (no protobuf): callers (transport/grpc)
// map proto requests/responses to and from these signatures.
package coordinator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/janthoXO/convergeKV/internal/core/ports"
	"github.com/janthoXO/convergeKV/internal/core/replica"
	"github.com/janthoXO/convergeKV/internal/domain/crdt"
	"github.com/janthoXO/convergeKV/internal/domain/hlc"
	"github.com/janthoXO/convergeKV/internal/domain/keyspace"
)

// ErrNoReplicas is returned when a request must be forwarded but the owner
// lookup returned no peers for the key's partition — e.g. a transient gap
// during membership churn between pool eviction and ownership recomputation.
var ErrNoReplicas = errors.New("coordinator: no replicas available for partition")

// LocalReplica is the minimal interface the coordinator needs from the local replica.
type LocalReplica interface {
	Put(ctx context.Context, partitionId uint32, key, valueJSON string) (hlc.Timestamp, []crdt.FieldUpdate, error)
	Get(ctx context.Context, partitionId uint32, key string) (string, bool, error)
	Delete(ctx context.Context, partitionId uint32, key string) (hlc.Timestamp, []crdt.FieldUpdate, error)
	ReplicaID() string
	HLCNow() hlc.Timestamp
}

// Coordinator routes client requests via HRW placement.
type Coordinator struct {
	node          LocalReplica
	members       ports.MemberView
	owners        ports.OwnerLookup
	forwarder     ports.Forwarder
	sink          ports.WriteSink
	numPartitions int
}

// New returns a Coordinator. owners is the precomputed HRW lookup; members is
// retained for the Status RPC, which still needs the full cluster view. sink
// receives every successfully-applied local write for fan-out to co-replicas.
func New(node LocalReplica, members ports.MemberView, owners ports.OwnerLookup, fwd ports.Forwarder, sink ports.WriteSink, numPartitions int) *Coordinator {
	return &Coordinator{
		node:          node,
		members:       members,
		owners:        owners,
		forwarder:     fwd,
		sink:          sink,
		numPartitions: numPartitions,
	}
}

// Put writes valueJSON at key, returning the HLC timestamp assigned to the write.
func (c *Coordinator) Put(ctx context.Context, key, valueJSON string) (hlc.Timestamp, error) {
	partitionId := keyspace.Of(key, c.numPartitions)
	if c.owners.IsOwner(partitionId) {
		ts, updates, err := c.node.Put(ctx, partitionId, key, valueJSON)
		if err == nil {
			c.sink.Enqueue(updates, c.owners.Peers(partitionId))
			return ts, nil
		}
		if !errors.Is(err, replica.ErrNotOwned) {
			return hlc.Timestamp{}, fmt.Errorf("coordinator: put: %w", err)
		}
		// Ownership says we own this partition but the replica hasn't
		// finished seeding it yet (gain-side state skew). Fall through
		// to forwarding so the request still succeeds via a co-owner.
	}

	return forwardWithRetry(ctx, c.owners.Peers(partitionId), func(addr string) (hlc.Timestamp, error) {
		return c.forwarder.ForwardPut(ctx, addr, key, valueJSON)
	})
}

// getResult bundles Get's two return values so it can flow through the
// generic forwardWithRetry helper.
type getResult struct {
	value string
	found bool
}

// Get returns the value stored at key, or found=false if it does not exist.
func (c *Coordinator) Get(ctx context.Context, key string) (string, bool, error) {
	partitionId := keyspace.Of(key, c.numPartitions)
	if c.owners.IsOwner(partitionId) {
		v, found, err := c.node.Get(ctx, partitionId, key)
		if err == nil {
			return v, found, nil
		}
		if !errors.Is(err, replica.ErrNotOwned) {
			return "", false, fmt.Errorf("coordinator: get: %w", err)
		}
		// Ownership says we own this partition but the replica hasn't
		// finished seeding it yet (gain-side state skew). Fall through
		// to forwarding so the request still succeeds via a co-owner.
	}

	res, err := forwardWithRetry(ctx, c.owners.Peers(partitionId), func(addr string) (getResult, error) {
		v, found, err := c.forwarder.ForwardGet(ctx, addr, key)
		return getResult{value: v, found: found}, err
	})
	return res.value, res.found, err
}

// Delete tombstones key, returning the HLC timestamp assigned to the delete.
func (c *Coordinator) Delete(ctx context.Context, key string) (hlc.Timestamp, error) {
	partitionId := keyspace.Of(key, c.numPartitions)
	if c.owners.IsOwner(partitionId) {
		ts, updates, err := c.node.Delete(ctx, partitionId, key)
		if err == nil {
			c.sink.Enqueue(updates, c.owners.Peers(partitionId))
			return ts, nil
		}
		if !errors.Is(err, replica.ErrNotOwned) {
			return hlc.Timestamp{}, fmt.Errorf("coordinator: delete: %w", err)
		}
		// Ownership says we own this partition but the replica hasn't
		// finished seeding it yet (gain-side state skew). Fall through
		// to forwarding so the request still succeeds via a co-owner.
	}

	return forwardWithRetry(ctx, c.owners.Peers(partitionId), func(addr string) (hlc.Timestamp, error) {
		return c.forwarder.ForwardDelete(ctx, addr, key)
	})
}

// Status returns the local node's identity, current HLC, and the ReplicaIDs
// of every other known cluster member.
func (c *Coordinator) Status(_ context.Context) (replicaID string, ts hlc.Timestamp, peers []string, err error) {
	ts = c.node.HLCNow()
	all := c.members.Members()
	peers = make([]string, 0, len(all))
	for _, m := range all {
		if m.ReplicaID != c.node.ReplicaID() {
			peers = append(peers, m.ReplicaID)
		}
	}
	return c.node.ReplicaID(), ts, peers, nil
}

// forwardWithRetry tries each replica in order, returning on the first success.
// If replicas is empty (and ctx is not already done), it returns ErrNoReplicas
// rather than a zero value with a nil error.
func forwardWithRetry[R any](ctx context.Context, replicas []ports.MemberInfo, fn func(addr string) (R, error)) (R, error) {
	var lastErr error
	for _, m := range replicas {
		if ctx.Err() != nil {
			break
		}
		if result, err := fn(m.GRPCAddr); err == nil {
			return result, nil
		} else {
			slog.Warn("forward failed, trying next replica", "addr", m.GRPCAddr, "err", err)
			lastErr = err
		}
	}

	var zero R
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	if lastErr == nil {
		return zero, ErrNoReplicas
	}
	return zero, lastErr
}
