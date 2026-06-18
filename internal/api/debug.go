package api

import (
	"context"
	"encoding/binary"
	"sort"

	"github.com/cespare/xxhash/v2"

	"github.com/janthoXO/convergeKV/internal/cluster"
	"github.com/janthoXO/convergeKV/internal/codec"
	"github.com/janthoXO/convergeKV/internal/crdt"
	"github.com/janthoXO/convergeKV/internal/hlc"
	"github.com/janthoXO/convergeKV/internal/nodeid"
	"github.com/janthoXO/convergeKV/internal/placement"
	"github.com/janthoXO/convergeKV/internal/storage"
	pb "github.com/janthoXO/convergeKV/pkg/proto"
)

// DebugServer serves read-only introspection RPCs for the debug client. It is
// not part of the data path; everything it returns is derived from state the
// node already maintains (placement view, membership, stored documents).
type DebugServer struct {
	pb.UnimplementedDebugServer
	ID         nodeid.ID
	Partitions uint16
	ClientAddr string
	View       func() *placement.View
	Cluster    *cluster.Cluster
	Store      *storage.Store
}

func (s *DebugServer) Inspect(ctx context.Context, _ *pb.InspectRequest) (*pb.InspectResponse, error) {
	self := s.Cluster.Self()
	resp := &pb.InspectResponse{
		NodeId:     s.ID[:],
		Generation: self.Generation,
		Partitions: uint32(s.Partitions),
		ClientAddr: s.ClientAddr,
		NodeAddr:   self.RPCAddr,
	}

	for _, m := range s.Cluster.Members() {
		resp.Members = append(resp.Members, memberPB(m, false))
	}
	for _, m := range s.Cluster.DeadMembers() {
		resp.Members = append(resp.Members, memberPB(m, true))
	}

	v := s.View()
	resp.PartitionsTable = make([]*pb.PartitionOwners, 0, s.Partitions)
	for pid := uint16(0); pid < s.Partitions; pid++ {
		po := &pb.PartitionOwners{Partition: uint32(pid)}
		for _, o := range v.Owners(pid) {
			po.Owners = append(po.Owners, &pb.Owner{
				Id:     o.ID[:],
				Status: uint32(o.Status),
				Dead:   o.Dead,
			})
		}
		resp.PartitionsTable = append(resp.PartitionsTable, po)
	}
	return resp, nil
}

func (s *DebugServer) DumpDocuments(_ *pb.DumpRequest, stream pb.Debug_DumpDocumentsServer) error {
	for pid := uint16(0); pid < s.Partitions; pid++ {
		err := s.Store.ScanPartition(pid, func(key []byte, doc *crdt.Document) error {
			dd := &pb.DebugDoc{
				Partition:   uint32(pid),
				Key:         key,
				ContextHash: debugContextHash(doc),
			}
			if doc.Deleted() {
				dd.Tombstone = true
			} else {
				rendered, err := codec.RenderDocument(doc)
				if err != nil {
					return err
				}
				dd.Document = rendered
				dd.Fields = debugFields(doc)
			}
			return stream.Send(dd)
		})
		if err != nil {
			return toStatus(err)
		}
	}
	return nil
}

func memberPB(m cluster.Member, dead bool) *pb.Member {
	return &pb.Member{
		Id:         m.Meta.ID[:],
		Addr:       m.Meta.RPCAddr,
		Dead:       dead,
		Generation: m.Meta.Generation,
	}
}

func debugContextHash(doc *crdt.Document) []byte {
	return binary.BigEndian.AppendUint64(nil, xxhash.Sum64(doc.Context.Canonical()))
}

// debugFields emits, per field, the current superseding register (the LWW
// winner from Document.Get) with its dot and HLC. Fields are walked in sorted
// order for deterministic output, mirroring codec.RenderDocument.
func debugFields(doc *crdt.Document) []*pb.DebugField {
	names := make([]string, 0, len(doc.Fields))
	for f := range doc.Fields {
		names = append(names, f)
	}
	sort.Strings(names)

	out := make([]*pb.DebugField, 0, len(names))
	for _, f := range names {
		reg, ok := doc.Get(f)
		if !ok {
			continue
		}
		actor := reg.Dot.Actor // copy to take a slice of the array
		out = append(out, &pb.DebugField{
			Name:       f,
			Value:      reg.Value,
			DotActor:   actor[:],
			DotSeq:     reg.Dot.Seq,
			Hlc:        reg.HLC,
			HlcPhysMs:  hlc.PhysMs(reg.HLC),
			HlcLogical: uint32(hlc.Logical(reg.HLC)),
		})
	}
	return out
}
