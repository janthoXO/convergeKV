// Package coordinator implements the identical request-handling logic every
// node runs: route a client op to the partition's applier (the first healthy
// active owner in HRW rank order), execute local ops under the partition
// lock, and fan replicated deltas out asynchronously. No quorums: a write
// succeeds once the applier has applied and persisted locally.
package coordinator

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/cespare/xxhash/v2"
	"github.com/janthoXO/convergeKV/internal/codec"
	"github.com/janthoXO/convergeKV/internal/crdt"
	"github.com/janthoXO/convergeKV/internal/hlc"
	"github.com/janthoXO/convergeKV/internal/merkle"
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
	view   func() *placement.View
	peers  Forwarder
	fanout *replication.Fanout
	locks  []sync.Mutex // one per partition
	log    *slog.Logger
}

func New(self [16]byte, p uint16, store *storage.Store, clock *hlc.Clock,
	view func() *placement.View, peers Forwarder,
	fanout *replication.Fanout, log *slog.Logger) *Coordinator {
	if log == nil {
		log = slog.Default()
	}
	return &Coordinator{
		self:   self,
		p:      p,
		store:  store,
		clock:  clock,
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
	if len(fields) == 0 {
		// An empty object would persist an empty document with an empty
		// context that peers' MergeDelta absorbing rule drops — a permanent
		// divergence AE would churn on. Reject before minting anything.
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
		if o.Dead || !o.Status.Serving() {
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
		if o.ID == c.self && o.Status.Serving() {
			return nil
		}
	}
	return ErrNotEligible
}

// CheckReadEligible reports whether this node may serve reads for pid.
func (c *Coordinator) CheckReadEligible(pid uint16) error {
	return c.CheckWriteEligible(pid) // read set == active owners
}

// ApplyPut is the applier path: mint one dot per field from the document's
// own context, apply and persist under the partition lock, then fan out the
// delta.
func (c *Coordinator) ApplyPut(pid uint16, key string, fields map[string][]byte) error {
	c.locks[pid].Lock()
	defer c.locks[pid].Unlock()

	doc, err := c.store.GetDocument(pid, []byte(key))
	if err != nil {
		return err
	}
	oldHash := docHashOf(key, doc) // nil for an absent document
	if doc == nil {
		doc = crdt.NewDocument()
	}
	delta := doc.PutMulti(fields, c.minter(doc), c.clock.Now())
	if err := c.persist(pid, key, oldHash, doc); err != nil {
		return err
	}
	c.replicate(pid, key, delta)
	return nil
}

// ApplyDelete is the applier path for document deletion.
func (c *Coordinator) ApplyDelete(pid uint16, key string) error {
	c.locks[pid].Lock()
	defer c.locks[pid].Unlock()

	doc, err := c.store.GetDocument(pid, []byte(key))
	if err != nil {
		return err
	}
	oldHash := docHashOf(key, doc) // nil for an absent document
	if doc == nil {
		doc = crdt.NewDocument()
	}
	delta := doc.Delete(c.minter(doc)())
	if err := c.persist(pid, key, oldHash, doc); err != nil {
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
// refused by an owner). It reports whether local state changed, which the
// anti-entropy engine uses to count repairs.
func (c *Coordinator) MergeDelta(pid uint16, key, deltaBytes []byte) (bool, error) {
	delta, err := crdt.DecodeDocument(deltaBytes)
	if err != nil {
		return false, fmt.Errorf("coordinator: bad delta: %w", err)
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
		return false, err
	}
	if doc == nil && len(delta.Fields) == 0 {
		// Absorbing GC rule: we hold nothing and the delta carries only a
		// residual context. Recreating a residual we already garbage-
		// collected would ping-pong with peers' GC forever. The cost: a
		// node that never saw the document loses the context's protection
		// against a stale put still in flight (bounded by the retry
		// queue's max age) — anti-entropy repairs that transient.
		return false, nil
	}
	// Serialize the pre-merge document once: reuse the bytes for both the old
	// leaf hash and the idempotent-redelivery equality check below.
	var oldHash *merkle.Hash
	var before []byte
	if doc == nil {
		doc = crdt.NewDocument()
	} else {
		before = doc.Canonical()
		h := merkle.DocHash(key, before)
		oldHash = &h
	}
	doc.Merge(delta)
	after := doc.Canonical()
	if before != nil && bytes.Equal(before, after) {
		return false, nil // idempotent redelivery: nothing to persist
	}
	if err := c.persistCanonical(pid, string(key), oldHash, after); err != nil {
		return false, err
	}
	return true, nil
}

// RecomputeMerkleLeaf rebuilds one leaf from the documents themselves
// (anti-entropy self-healing after repairs: XOR leaves drift permanently if
// docs and leaves ever disagree, e.g. after corruption).
func (c *Coordinator) RecomputeMerkleLeaf(pid uint16, bucket uint16) error {
	c.locks[pid].Lock()
	defer c.locks[pid].Unlock()
	var leaf merkle.Hash
	err := c.store.ScanBucket(pid, bucket, func(key []byte, doc *crdt.Document) error {
		merkle.XOR(&leaf, merkle.DocHash(key, doc.Canonical()))
		return nil
	})
	if err != nil {
		return err
	}
	b := c.store.NewBatch()
	b.SetMerkleNode(pid, merkle.BucketPath(bucket), leaf[:])
	return c.store.Commit(b)
}

// --- garbage collection (called by internal/gc under AE certification) ----------

// GCDocument removes a residual-context document outright, with its GC
// counter and leaf contribution, iff it is still empty.
func (c *Coordinator) GCDocument(pid uint16, key []byte) error {
	c.locks[pid].Lock()
	defer c.locks[pid].Unlock()
	doc, err := c.store.GetDocument(pid, key)
	if err != nil {
		return err
	}
	if doc == nil || len(doc.Fields) > 0 {
		return nil // re-created or already gone
	}
	bucket := merkle.Bucket(key)
	leaf, err := c.store.MerkleLeaf(pid, bucket)
	if err != nil {
		return err
	}
	merkle.XOR(&leaf, merkle.DocHash(key, doc.Canonical()))
	b := c.store.NewBatch()
	b.DeleteDocument(pid, key)
	b.DeleteGCCounter(pid, key)
	b.SetMerkleNode(pid, merkle.BucketPath(bucket), leaf[:])
	return c.store.Commit(b)
}

// RetireActors drops version-vector entries of retired actors from one
// document's context, provided the actor has no live register in it. Safe
// because retirement is only certified once the actor is dead past grace and
// the owners are in sync — no event from that actor can ever arrive again.
func (c *Coordinator) RetireActors(pid uint16, key []byte, retired func(crdt.ActorID) bool) error {
	c.locks[pid].Lock()
	defer c.locks[pid].Unlock()
	doc, err := c.store.GetDocument(pid, key)
	if err != nil || doc == nil {
		return err
	}
	live := map[crdt.ActorID]bool{}
	for _, regs := range doc.Fields {
		for _, r := range regs {
			live[r.Dot.Actor] = true
		}
	}
	oldHash := docHashOf(string(key), doc)
	changed := false
	for a := range doc.Context.VV {
		if retired(a) && !live[a] {
			delete(doc.Context.VV, a)
			changed = true
		}
	}
	for d := range doc.Context.Cloud {
		if retired(d.Actor) && !live[d.Actor] {
			delete(doc.Context.Cloud, d)
			changed = true
		}
	}
	if !changed {
		return nil
	}
	return c.persist(pid, string(key), oldHash, doc)
}

// --- internals -----------------------------------------------------------------

// minter hands out this node's next dots for one document, sequential from
// the document's own context (per-document minting: uniqueness only has to
// hold within one document's context, and the VV persisted with the op is the
// crash-safe checkpoint). Callers hold the partition lock.
func (c *Coordinator) minter(doc *crdt.Document) func() crdt.Dot {
	actor := crdt.ActorID(c.self)
	seq := doc.Context.Next(actor) - 1
	return func() crdt.Dot {
		seq++
		return crdt.Dot{Actor: actor, Seq: seq}
	}
}

// persist writes the document and its incremental merkle-leaf update in one
// synced atomic batch. Callers hold the partition lock.
func (c *Coordinator) persist(pid uint16, key string, oldHash *merkle.Hash, doc *crdt.Document) error {
	return c.persistCanonical(pid, key, oldHash, doc.Canonical())
}

// persistCanonical is persist for callers that already hold the document's
// canonical bytes, so the document is serialized once (not once for the leaf
// hash and again inside SetDocument). Callers hold the partition lock.
func (c *Coordinator) persistCanonical(pid uint16, key string, oldHash *merkle.Hash, canonical []byte) error {
	keyB := []byte(key)
	bucket := merkle.Bucket(keyB)
	leaf, err := c.store.MerkleLeaf(pid, bucket)
	if err != nil {
		return err
	}
	if oldHash != nil {
		merkle.XOR(&leaf, *oldHash)
	}
	merkle.XOR(&leaf, merkle.DocHash(keyB, canonical))

	b := c.store.NewBatch()
	b.SetDocumentRaw(pid, keyB, canonical)
	b.SetMerkleNode(pid, merkle.BucketPath(bucket), leaf[:])
	return c.store.Commit(b)
}

func docHashOf(key string, doc *crdt.Document) *merkle.Hash {
	if doc == nil {
		return nil
	}
	h := merkle.DocHash([]byte(key), doc.Canonical())
	return &h
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
