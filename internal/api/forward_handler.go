package api

import (
	"context"

	fwdpb "github.com/janthoXO/convergeKV/gen/forward"
	kvpb "github.com/janthoXO/convergeKV/gen/kv"
	"github.com/janthoXO/convergeKV/internal/coordinator"
)

// ForwardHandler implements fwdpb.ForwardServiceServer.
// It re-uses the Coordinator so that a forwarded write also triggers
// async replication to the remaining co-replicas.
type ForwardHandler struct {
	fwdpb.UnimplementedForwardServiceServer
	coord *coordinator.Coordinator
}

// NewForwardHandler returns a ready-to-register ForwardHandler.
func NewForwardHandler(c *coordinator.Coordinator) *ForwardHandler {
	return &ForwardHandler{coord: c}
}

func (h *ForwardHandler) ForwardPut(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	return h.coord.Put(ctx, req)
}

func (h *ForwardHandler) ForwardGet(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	return h.coord.Get(ctx, req)
}

func (h *ForwardHandler) ForwardDelete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	return h.coord.Delete(ctx, req)
}
