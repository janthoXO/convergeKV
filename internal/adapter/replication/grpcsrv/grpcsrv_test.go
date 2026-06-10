package grpcsrv_test

import (
	"context"
	"io"
	"net"
	"testing"

	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	repb "github.com/janthoXO/convergeKV/gen/replication"
	"github.com/janthoXO/convergeKV/internal/adapter/replication/grpcsrv"
	"github.com/janthoXO/convergeKV/internal/domain/crdt"
	domiblt "github.com/janthoXO/convergeKV/internal/domain/iblt"
	"github.com/janthoXO/convergeKV/internal/domain/hlc"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

const numPartitions = 4
const bufSize = 1 << 20

// fakeNode implements grpcsrv.Node.
// owned models the node's partition-ownership view; partitions not in this set
// are treated as "not mine" by Owns(pid).
type fakeNode struct {
	entries map[uint32]map[string]map[string]crdt.FieldEntry // pid->key->field->entry
	owned   map[uint32]struct{}
}

func newFakeNode() *fakeNode {
	n := &fakeNode{
		entries: make(map[uint32]map[string]map[string]crdt.FieldEntry),
		owned:   make(map[uint32]struct{}),
	}
	// Default ownership for tests: own pid range [0, numPartitions).
	for pid := uint32(0); pid < numPartitions; pid++ {
		n.owned[pid] = struct{}{}
	}
	return n
}

func (n *fakeNode) Owns(pid uint32) bool {
	_, ok := n.owned[pid]
	return ok
}

func (n *fakeNode) set(pid uint32, key, field string, e crdt.FieldEntry) {
	if n.entries[pid] == nil {
		n.entries[pid] = make(map[string]map[string]crdt.FieldEntry)
	}
	if n.entries[pid][key] == nil {
		n.entries[pid][key] = make(map[string]crdt.FieldEntry)
	}
	n.entries[pid][key][field] = e
}

func (n *fakeNode) StateOverview(pid uint32) []byte {
	return domiblt.New(32).Encode()
}

func (n *fakeNode) ApplyDelta(_ context.Context, pid uint32, key, field string, e crdt.FieldEntry) (bool, error) {
	n.set(pid, key, field, e)
	return true, nil
}

func (n *fakeNode) IteratePartition(_ context.Context, pid uint32, fn func(key, field string, entry crdt.FieldEntry) error) error {
	for key, fields := range n.entries[pid] {
		for field, entry := range fields {
			if err := fn(key, field, entry); err != nil {
				return err
			}
		}
	}
	return nil
}

func (n *fakeNode) GetField(_ context.Context, pid uint32, key, field string) (crdt.FieldEntry, bool, error) {
	e, ok := n.entries[pid][key][field]
	return e, ok, nil
}

// ── test harness ──────────────────────────────────────────────────────────────

func newTestServer(t *testing.T, node grpcsrv.Node) (repb.SyncServiceClient, func()) {
	t.Helper()
	lis := bufconn.Listen(bufSize)
	srv := grpc.NewServer()
	repb.RegisterSyncServiceServer(srv, grpcsrv.New(node))

	go srv.Serve(lis) //nolint:errcheck

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	cleanup := func() {
		conn.Close()
		srv.GracefulStop()
	}
	return repb.NewSyncServiceClient(conn), cleanup
}

// ── tests ─────────────────────────────────────────────────────────────────────

func TestGetStateOverviewValidPID(t *testing.T) {
	client, cleanup := newTestServer(t, newFakeNode())
	defer cleanup()

	_, err := client.GetStateOverview(context.Background(), &repb.StateOverviewRequest{PartitionId: 0})
	if err != nil {
		t.Fatalf("GetStateOverview: %v", err)
	}
}

func TestGetStateOverviewNotOwned(t *testing.T) {
	client, cleanup := newTestServer(t, newFakeNode())
	defer cleanup()

	// numPartitions is outside the fake's owned set.
	_, err := client.GetStateOverview(context.Background(), &repb.StateOverviewRequest{PartitionId: numPartitions})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition for unowned partition, got %v", err)
	}
}

func TestPushEntriesNoHeader(t *testing.T) {
	client, cleanup := newTestServer(t, newFakeNode())
	defer cleanup()

	stream, err := client.PushEntries(context.Background())
	if err != nil {
		t.Fatalf("PushEntries open: %v", err)
	}
	// Send an entry without a prior header.
	_ = stream.Send(&repb.PushChunk{
		Payload: &repb.PushChunk_Entry{Entry: &repb.DeltaEntry{Key: "k", Field: "f"}},
	})
	_, err = stream.CloseAndRecv()
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument for missing header, got %v", err)
	}
}

func TestPushEntriesNullByteRejected(t *testing.T) {
	node := newFakeNode()
	client, cleanup := newTestServer(t, node)
	defer cleanup()

	stream, err := client.PushEntries(context.Background())
	if err != nil {
		t.Fatalf("PushEntries open: %v", err)
	}
	_ = stream.Send(&repb.PushChunk{
		Payload: &repb.PushChunk_Header{Header: &repb.PushHeader{PartitionId: 0}},
	})
	_ = stream.Send(&repb.PushChunk{
		Payload: &repb.PushChunk_Entry{Entry: &repb.DeltaEntry{
			Key:   "key\x00poison",
			Field: "f",
		}},
	})
	ack, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatalf("CloseAndRecv: %v", err)
	}
	// Entry must be rejected (applied=0).
	if ack.GetApplied() != 0 {
		t.Errorf("expected 0 applied entries for null-byte key, got %d", ack.GetApplied())
	}
}

func TestPushEntriesApplied(t *testing.T) {
	node := newFakeNode()
	client, cleanup := newTestServer(t, node)
	defer cleanup()

	stream, err := client.PushEntries(context.Background())
	if err != nil {
		t.Fatalf("PushEntries open: %v", err)
	}
	_ = stream.Send(&repb.PushChunk{
		Payload: &repb.PushChunk_Header{Header: &repb.PushHeader{PartitionId: 0}},
	})
	_ = stream.Send(&repb.PushChunk{
		Payload: &repb.PushChunk_Entry{Entry: &repb.DeltaEntry{Key: "k", Field: "f", ValueJson: []byte(`"v"`)}},
	})
	ack, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatalf("CloseAndRecv: %v", err)
	}
	if ack.GetApplied() != 1 {
		t.Errorf("expected 1 applied, got %d", ack.GetApplied())
	}
}

func TestPushEntriesMultipleHeaders(t *testing.T) {
	node := newFakeNode()
	client, cleanup := newTestServer(t, node)
	defer cleanup()

	stream, err := client.PushEntries(context.Background())
	if err != nil {
		t.Fatalf("PushEntries open: %v", err)
	}
	// First batch: pid=0
	_ = stream.Send(&repb.PushChunk{
		Payload: &repb.PushChunk_Header{Header: &repb.PushHeader{PartitionId: 0}},
	})
	_ = stream.Send(&repb.PushChunk{
		Payload: &repb.PushChunk_Entry{Entry: &repb.DeltaEntry{Key: "k0", Field: "f", ValueJson: []byte(`0`)}},
	})
	// Second batch: pid=1 (mid-stream header)
	_ = stream.Send(&repb.PushChunk{
		Payload: &repb.PushChunk_Header{Header: &repb.PushHeader{PartitionId: 1}},
	})
	_ = stream.Send(&repb.PushChunk{
		Payload: &repb.PushChunk_Entry{Entry: &repb.DeltaEntry{Key: "k1", Field: "f", ValueJson: []byte(`1`)}},
	})

	ack, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatalf("CloseAndRecv: %v", err)
	}
	if ack.GetApplied() != 2 {
		t.Errorf("expected 2 applied across 2 partitions, got %d", ack.GetApplied())
	}
}

func TestPullEntriesFullPartition(t *testing.T) {
	node := newFakeNode()
	node.set(0, "key", "field", crdt.FieldEntry{
		Value:     crdt.NewFieldEntry([]byte(`"hello"`), 1, 0, "r1", false).Value,
		Timestamp: hlc.Timestamp{PhysicalMs: 1, Logical: 0},
		ReplicaID: "r1",
	})

	client, cleanup := newTestServer(t, node)
	defer cleanup()

	stream, err := client.PullEntries(context.Background(), &repb.PullRequest{
		PartitionId: 0,
		Identifiers: nil, // full pull
	})
	if err != nil {
		t.Fatalf("PullEntries: %v", err)
	}

	var count int
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		count++
	}
	if count != 1 {
		t.Errorf("expected 1 entry from full pull, got %d", count)
	}
}

func TestPullEntriesNotOwned(t *testing.T) {
	client, cleanup := newTestServer(t, newFakeNode())
	defer cleanup()

	stream, err := client.PullEntries(context.Background(), &repb.PullRequest{
		PartitionId: numPartitions,
	})
	if err != nil {
		t.Fatalf("PullEntries open: %v", err)
	}
	_, err = stream.Recv()
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition, got %v", err)
	}
}
