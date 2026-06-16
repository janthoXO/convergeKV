// Package api exposes the coordinator over gRPC: the client-facing KV
// service and the node-facing Node service, plus the peer connection pool.
package api

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/janthoXO/convergeKV/internal/codec"
	"github.com/janthoXO/convergeKV/internal/coordinator"
	"github.com/janthoXO/convergeKV/internal/crdt"
	"github.com/janthoXO/convergeKV/internal/merkle"
	"github.com/janthoXO/convergeKV/internal/placement"
	"github.com/janthoXO/convergeKV/internal/storage"
	pb "github.com/janthoXO/convergeKV/pkg/proto"
)

// KVServer serves client requests on any node.
type KVServer struct {
	pb.UnimplementedKVServer
	Coord *coordinator.Coordinator
}

func (s *KVServer) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	res, err := s.Coord.Get(ctx, req.GetKey())
	if err != nil {
		return nil, toStatus(err)
	}
	return &pb.GetResponse{Found: res.Found, Document: res.Document, ContextHash: res.ContextHash}, nil
}

func (s *KVServer) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	if err := s.Coord.Put(ctx, req.GetKey(), req.GetValue()); err != nil {
		return nil, toStatus(err)
	}
	return &pb.PutResponse{}, nil
}

func (s *KVServer) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	if err := s.Coord.Delete(ctx, req.GetKey()); err != nil {
		return nil, toStatus(err)
	}
	return &pb.DeleteResponse{}, nil
}

// NodeServer serves node-to-node requests.
type NodeServer struct {
	pb.UnimplementedNodeServer
	Coord      *coordinator.Coordinator
	Store      *storage.Store
	Partitions uint16
}

func (s *NodeServer) Forward(ctx context.Context, req *pb.ForwardRequest) (*pb.ForwardResponse, error) {
	pid := placement.Partition([]byte(req.GetKey()), s.Partitions)
	switch op := req.GetOp().(type) {
	case *pb.ForwardRequest_Put:
		if err := s.Coord.CheckWriteEligible(pid); err != nil {
			return nil, toStatus(err)
		}
		fields, err := codec.SplitFields(op.Put.GetValue())
		if err != nil || len(fields) == 0 {
			return nil, status.Error(codes.InvalidArgument, "document must be a non-empty JSON object")
		}
		if err := s.Coord.ApplyPut(pid, req.GetKey(), fields); err != nil {
			return nil, toStatus(err)
		}
		return &pb.ForwardResponse{}, nil

	case *pb.ForwardRequest_Delete:
		if err := s.Coord.CheckWriteEligible(pid); err != nil {
			return nil, toStatus(err)
		}
		if err := s.Coord.ApplyDelete(pid, req.GetKey()); err != nil {
			return nil, toStatus(err)
		}
		return &pb.ForwardResponse{}, nil

	case *pb.ForwardRequest_Get:
		if err := s.Coord.CheckReadEligible(pid); err != nil {
			return nil, toStatus(err)
		}
		res, err := s.Coord.ReadLocal(pid, req.GetKey())
		if err != nil {
			return nil, toStatus(err)
		}
		return &pb.ForwardResponse{Get: &pb.GetResponse{
			Found: res.Found, Document: res.Document, ContextHash: res.ContextHash,
		}}, nil

	default:
		return nil, status.Error(codes.InvalidArgument, "forward request without op")
	}
}

func (s *NodeServer) ApplyDelta(ctx context.Context, req *pb.ApplyDeltaRequest) (*pb.ApplyDeltaResponse, error) {
	if _, err := s.Coord.MergeDelta(uint16(req.GetPartition()), req.GetKey(), req.GetDelta()); err != nil {
		return nil, toStatus(err)
	}
	return &pb.ApplyDeltaResponse{}, nil
}

func (s *NodeServer) MerkleRoot(ctx context.Context, req *pb.MerkleRootRequest) (*pb.MerkleRootResponse, error) {
	leaves, err := s.Store.MerkleLeaves(uint16(req.GetPartition()))
	if err != nil {
		return nil, toStatus(err)
	}
	root := merkle.Root(leaves)
	return &pb.MerkleRootResponse{Root: root[:]}, nil
}

func (s *NodeServer) MerkleLeaves(ctx context.Context, req *pb.MerkleLeavesRequest) (*pb.MerkleLeavesResponse, error) {
	leaves, err := s.Store.MerkleLeaves(uint16(req.GetPartition()))
	if err != nil {
		return nil, toStatus(err)
	}
	packed := make([]byte, 0, merkle.Buckets*merkle.HashSize)
	for i := range leaves {
		packed = append(packed, leaves[i][:]...)
	}
	return &pb.MerkleLeavesResponse{Leaves: packed}, nil
}

func (s *NodeServer) Snapshot(req *pb.SnapshotRequest, stream pb.Node_SnapshotServer) error {
	return s.Store.ScanPartition(uint16(req.GetPartition()), func(key []byte, doc *crdt.Document) error {
		return stream.Send(&pb.SyncDoc{Key: key, Document: doc.Canonical()})
	})
}

func (s *NodeServer) SyncBucket(req *pb.SyncBucketRequest, stream pb.Node_SyncBucketServer) error {
	pid, bucket := uint16(req.GetPartition()), uint16(req.GetBucket())
	return s.Store.ScanBucket(pid, bucket, func(key []byte, doc *crdt.Document) error {
		return stream.Send(&pb.SyncDoc{Key: key, Document: doc.Canonical()})
	})
}

func toStatus(err error) error {
	switch {
	case errors.Is(err, coordinator.ErrNoOwnerAvailable):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, coordinator.ErrNotEligible):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, coordinator.ErrInvalidDocument):
		return status.Error(codes.InvalidArgument, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
