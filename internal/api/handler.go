// Package api implements the gRPC KVService handler for client-facing operations.
package api

import (
	"context"
	"strings"

	kvpb "github.com/janthoXO/convergeKV/gen/kv"
	"github.com/janthoXO/convergeKV/internal/coordinator"
	"github.com/janthoXO/convergeKV/internal/gossip"
	"github.com/janthoXO/convergeKV/internal/node"
	"github.com/janthoXO/convergeKV/internal/utils"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Handler implements kvpb.KVServiceServer.
type Handler struct {
	kvpb.UnimplementedKVServiceServer
	coord  *coordinator.Coordinator
	node   *node.Node     // used only for Status
	gossip *gossip.Gossip // queried live for Status
}

// NewHandler returns a ready-to-register Handler.
func NewHandler(coord *coordinator.Coordinator, n *node.Node, g *gossip.Gossip) *Handler {
	return &Handler{coord: coord, node: n, gossip: g}
}

// Put routes a put request through the coordinator.
func (h *Handler) Put(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	if err := validateKey(req.GetKey()); err != nil {
		return nil, err
	}
	return h.coord.Put(ctx, req)
}

// Get routes a get request through the coordinator.
func (h *Handler) Get(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	if err := validateKey(req.GetKey()); err != nil {
		return nil, err
	}
	return h.coord.Get(ctx, req)
}

// Delete routes a delete request through the coordinator.
func (h *Handler) Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	if err := validateKey(req.GetKey()); err != nil {
		return nil, err
	}
	return h.coord.Delete(ctx, req)
}

// validateKey rejects keys containing \x00, which is used as the separator
// between key and field in the storage encoding (badger.go:badgerKey).
// A \x00 in the key would corrupt the separator, causing decodeKey to
// mis-attribute fields and GetKey prefix scans to leak data from other keys.
func validateKey(key string) error {
	if strings.ContainsRune(key, 0) {
		return status.Error(codes.InvalidArgument, "key must not contain null bytes")
	}
	return nil
}

// Status returns the node's replica ID, current HLC, and peer list.
func (h *Handler) Status(_ context.Context, _ *kvpb.StatusRequest) (*kvpb.StatusResponse, error) {
	ts := h.node.HLCNow()
	return &kvpb.StatusResponse{
		ReplicaId: h.node.ReplicaID(),
		Hlc:       &kvpb.HLCTimestamp{PhysicalMs: ts.PhysicalMs, Logical: ts.Logical},
		Peers: utils.FilterSlice(
			utils.MapSlice(h.gossip.Members(), func(m gossip.MemberInfo) string { return m.ReplicaID }),
			func(s string) bool {
				return s != h.node.ReplicaID()
			},
		),
	}, nil
}
