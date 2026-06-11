// Package coordinator implements the identical request-handling logic every
// node runs: route a client op to the partition's applier (the first healthy
// active owner in HRW rank order), execute local ops under the partition
// lock, and fan replicated deltas out asynchronously. No quorums: a write
// succeeds once the applier has applied and persisted locally.
package coordinator

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/cespare/xxhash/v2"
	"github.com/janthoXO/convergeKV/internal/cluster"
	"github.com/janthoXO/convergeKV/internal/codec"
	"github.com/janthoXO/convergeKV/internal/crdt"
	"github.com/janthoXO/convergeKV/internal/hlc"
	"github.com/janthoXO/convergeKV/internal/placement"
	"github.com/janthoXO/convergeKV/internal/replication"
	"github.com/janthoXO/convergeKV/internal/storage"
	pb "github.com/janthoXO/convergeKV/pkg/proto"
)

var (
	// ErrNoOwnerAvailable: no healthy owner reachable — retryable UNAVAILABLE.
	ErrNoOwnerAvailable = errors.New("coordinator: no healthy owner reachable")
	// ErrNotEligible: this node may not execute the forwarded op (stale view
	// at the sender) — FAILED_PRECONDITION, sender retries elsewhere.
	ErrNotEligible = errors.New("coordinator: node not an eligible owner")
	// ErrInvalidDocument: the client value is not a JSON object.
	ErrInvalidDocument = errors.New("coordinator: document must be a JSON object")
)

// Forwarder sends a forward request to a peer node (gRPC in production).
type Forwarder interface {
	Forward(ctx context.Context, addr string, req *pb.ForwardRequest) (*pb.ForwardResponse, error)
}

type Coordinator struct {
	self   [16]byte
	p      uint16
	store  *storage.Store
	clock  *hlc.Clock
	dots   *DotSource
	view   func() *placement.View
	peers  Forwarder
	fanout *replication.Fanout
	locks  []sync.Mutex // one per partition
	log    *slog.Logger
}

func New(self [16]byte, p uint16, store *storage.Store, clock *hlc.Clock,
	dots *DotSource, view func() *placement.View, peers Forwarder,
	fanout *replication.Fanout, log *slog.Logger) *Coordinator {
	if log == nil {
		log = slog.Default()
	}
	return &Coordinator{
		self:   self,
		p:      p,
		store:  store,
		clock:  clock,
		dots:   dots,
		view:   view,
		peers:  peers,
		fanout: fanout,
		locks:  make([]sync.Mutex, p),
		log:    log,
	}
}

// GetResult is a read response before JSON rendering at the API layer.
type GetResult struct {
	Found       bool
	Document    []byte // rendered JSON object
	ContextHash []byte
}

// --- client entry points (any node) ------------------------------------------

// Put routes a client write to the applier and returns once it has applied
// and persisted (replication to the other owners is asynchronous).
func (c *Coordinator) Put(ctx context.Context, key string, jsonDoc []byte) error {
	fields, err := codec.SplitFields(jsonDoc)
	if err != nil {
		return ErrInvalidDocument
	}
	pid := placement.Partition([]byte(key), c.p)
	req := &pb.ForwardRequest{
		Key: key,
		Op:  &pb.ForwardRequest_Put{Put: &pb.PutRequest{Key: key, Document: jsonDoc}},
	}
	return c.routeWrite(ctx, pid, req, func() error {
		return c.ApplyPut(pid, key, fields)
	})
}

// Delete routes a client delete to the applier.
func (c *Coordinator) Delete(ctx context.Context, key string) error {
	pid := placement.Partition([]byte(key), c.p)
	req := &pb.ForwardRequest{
		Key: key,
		Op:  &pb.ForwardRequest_Delete{Delete: &pb.DeleteRequest{Key: key}},
	}
	return c.routeWrite(ctx, pid, req, func() error {
		return c.ApplyDelete(pid, key)
	})
}

// Get serves a read from one active owner: locally when this node is one,
// otherwise forwarded to the first healthy active owner.
func (c *Coordinator) Get(ctx context.Context, key string) (GetResult, error) {
	pid := placement.Partition([]byte(key), c.p)
	v := c.view()
	for _, o := range v.ReadSet(pid) {
		if o.ID == c.self {
			return c.ReadLocal(pid, key)
		}
	}
	req := &pb.ForwardRequest{
		Key: key,
		Op:  &pb.ForwardRequest_Get{Get: &pb.GetRequest{Key: key}},
	}
	for _, o := range v.ReadSet(pid) {
		resp, err := c.peers.Forward(ctx, o.Addr, req)
		if err != nil {
			c.log.Debug("read forward failed, trying next owner", "peer", o.Addr, "err", err)
			continue
		}
		g := resp.GetGet()
		return GetResult{Found: g.GetFound(), Document: g.GetDocument(), ContextHash: g.GetContextHash()}, nil
	}
	return GetResult{}, ErrNoOwnerAvailable
}

// routeWrite executes the op locally if this node is the first healthy
// active owner, otherwise forwards along the rank order.
func (c *Coordinator) routeWrite(ctx context.Context, pid uint16, req *pb.ForwardRequest, local func() error) error {
	v := c.view()
	for _, o := range v.Owners(pid) {
		if o.Status != cluster.StatusActive {
			continue
		}
		if o.ID == c.self {
			return local()
		}
		if _, err := c.peers.Forward(ctx, o.Addr, req); err == nil {
			return nil
		} else { //nolint:revive // try the next-ranked owner
			c.log.Debug("write forward failed, trying next owner", "peer", o.Addr, "err", err)
		}
	}
	return ErrNoOwnerAvailable
}

// --- local execution (this node must be an eligible owner) --------------------

// CheckWriteEligible reports whether this node may act as applier for pid.
func (c *Coordinator) CheckWriteEligible(pid uint16) error {
	v := c.view()
	for _, o := range v.Owners(pid) {
		if o.ID == c.self && o.Status == cluster.StatusActive {
			return nil
		}
	}
	return ErrNotEligible
}

// CheckReadEligible reports whether this node may serve reads for pid.
func (c *Coordinator) CheckReadEligible(pid uint16) error {
	return c.CheckWriteEligible(pid) // read set == active owners
}

// ApplyPut is the applier path: mint one dot per field, apply and persist
// under the partition lock, then fan out the delta.
func (c *Coordinator) ApplyPut(pid uint16, key string, fields map[string][]byte) error {
	mint, err := c.dots.Mint(len(fields))
	if err != nil {
		return fmt.Errorf("coordinator: reserve dots: %w", err)
	}
	c.locks[pid].Lock()
	defer c.locks[pid].Unlock()

	doc, err := c.store.GetDocument(pid, []byte(key))
	if err != nil {
		return err
	}
	if doc == nil {
		doc = crdt.NewDocument()
	}
	delta := doc.PutMulti(fields, mint, c.clock.Now())
	if err := c.persist(pid, key, doc); err != nil {
		return err
	}
	c.replicate(pid, key, delta)
	return nil
}

// ApplyDelete is the applier path for document deletion.
func (c *Coordinator) ApplyDelete(pid uint16, key string) error {
	mint, err := c.dots.Mint(1)
	if err != nil {
		return fmt.Errorf("coordinator: reserve dots: %w", err)
	}
	c.locks[pid].Lock()
	defer c.locks[pid].Unlock()

	doc, err := c.store.GetDocument(pid, []byte(key))
	if err != nil {
		return err
	}
	if doc == nil {
		doc = crdt.NewDocument()
	}
	delta := doc.Delete(mint())
	if err := c.persist(pid, key, doc); err != nil {
		return err
	}
	c.replicate(pid, key, delta)
	return nil
}

// ReadLocal serves a read from local storage.
func (c *Coordinator) ReadLocal(pid uint16, key string) (GetResult, error) {
	doc, err := c.store.GetDocument(pid, []byte(key))
	if err != nil {
		return GetResult{}, err
	}
	if doc == nil || len(doc.Fields) == 0 {
		return GetResult{Found: false}, nil
	}
	rendered, err := codec.RenderDocument(doc)
	if err != nil {
		return GetResult{}, err
	}
	return GetResult{Found: true, Document: rendered, ContextHash: contextHash(doc)}, nil
}

// MergeDelta applies a replicated delta (accepted while active or
// bootstrapping — eligibility is NOT checked here; deltas must never be
// refused by an owner).
func (c *Coordinator) MergeDelta(pid uint16, key, deltaBytes []byte) error {
	delta, err := crdt.DecodeDocument(deltaBytes)
	if err != nil {
		return fmt.Errorf("coordinator: bad delta: %w", err)
	}
	// HLC receive rule: fold the delta's largest timestamp into our clock.
	var maxTS crdt.HLC
	for _, regs := range delta.Fields {
		for _, r := range regs {
			if r.HLC > maxTS {
				maxTS = r.HLC
			}
		}
	}
	if maxTS > 0 {
		c.clock.Update(maxTS)
	}

	c.locks[pid].Lock()
	defer c.locks[pid].Unlock()

	doc, err := c.store.GetDocument(pid, key)
	if err != nil {
		return err
	}
	if doc == nil {
		doc = crdt.NewDocument()
	}
	doc.Merge(delta)
	return c.persist(pid, string(key), doc)
}

// --- internals -----------------------------------------------------------------

func (c *Coordinator) persist(pid uint16, key string, doc *crdt.Document) error {
	b := c.store.NewBatch()
	b.SetDocument(pid, []byte(key), doc)
	return c.store.Commit(b)
}

// replicate enqueues the delta for every other write-set owner.
func (c *Coordinator) replicate(pid uint16, key string, delta *crdt.Document) {
	v := c.view()
	enc := delta.Canonical()
	for _, o := range v.WriteSet(pid) {
		if o.ID == c.self {
			continue
		}
		c.fanout.Enqueue(o.Addr, replication.Delta{
			Partition: pid,
			Key:       []byte(key),
			Delta:     enc,
		})
	}
}

func contextHash(doc *crdt.Document) []byte {
	return binary.BigEndian.AppendUint64(nil, xxhash.Sum64(doc.Context.Canonical()))
}
