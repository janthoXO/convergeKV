package replica_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"go.uber.org/goleak"

	"github.com/janthoXO/convergeKV/internal/core/replica"
	"github.com/janthoXO/convergeKV/internal/domain/crdt"
	"github.com/janthoXO/convergeKV/internal/domain/hlc"
)

const testIBLTCells = 64

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// ── fakes ──────────────────────────────────────────────────────────────────────

// fakeClock is a deterministic ports.Clock for tests.
type fakeClock struct {
	mu  sync.Mutex
	seq uint64
}

func (c *fakeClock) Now() hlc.Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()
	return hlc.Timestamp{PhysicalMs: c.seq}
}

func (c *fakeClock) Send() (hlc.Timestamp, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seq++
	return hlc.Timestamp{PhysicalMs: c.seq}, nil
}

func (c *fakeClock) Receive(remote hlc.Timestamp) (hlc.Timestamp, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if remote.PhysicalMs >= c.seq {
		c.seq = remote.PhysicalMs + 1
	} else {
		c.seq++
	}
	return hlc.Timestamp{PhysicalMs: c.seq}, nil
}

func (c *fakeClock) Seed(floor hlc.Timestamp) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if floor.PhysicalMs > c.seq {
		c.seq = floor.PhysicalMs
	}
}

// fakeStore is an in-memory ports.Store for tests.
type fakeStore struct {
	mu            sync.RWMutex
	entries       map[storeKey]crdt.FieldEntry
	checkpoint    hlc.Timestamp
	hasCheckpoint bool
}

type storeKey struct {
	pid   uint32
	key   string
	field string
}

func newFakeStore() *fakeStore { return &fakeStore{entries: make(map[storeKey]crdt.FieldEntry)} }

func (s *fakeStore) GetKey(_ context.Context, pid uint32, key string) (crdt.AWLWWMap, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m := crdt.NewAWLWWMap()
	for k, e := range s.entries {
		if k.pid == pid && k.key == key {
			m.Fields[k.field] = e
		}
	}
	return m, nil
}

func (s *fakeStore) GetField(_ context.Context, pid uint32, key, field string) (crdt.FieldEntry, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.entries[storeKey{pid, key, field}]
	return e, ok, nil
}

func (s *fakeStore) SaveBatch(_ context.Context, batch []crdt.FieldUpdate) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, u := range batch {
		s.entries[storeKey{u.PartitionID, u.Key, u.Field}] = u.Entry
	}
	return nil
}

func (s *fakeStore) IteratePartition(_ context.Context, pid uint32, fn func(string, string, crdt.FieldEntry) error) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for k, e := range s.entries {
		if k.pid == pid {
			if err := fn(k.key, k.field, e); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *fakeStore) IterateAll(_ context.Context, fn func(string, string, crdt.FieldEntry) error) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for k, e := range s.entries {
		if err := fn(k.key, k.field, e); err != nil {
			return err
		}
	}
	return nil
}

func (s *fakeStore) SaveCheckpoint(_ context.Context, ts hlc.Timestamp) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checkpoint = ts
	s.hasCheckpoint = true
	return nil
}

func (s *fakeStore) LoadCheckpoint(_ context.Context) (hlc.Timestamp, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.checkpoint, s.hasCheckpoint, nil
}

// ── helpers ────────────────────────────────────────────────────────────────────

func newTestReplica(t *testing.T) (*replica.Replica, *fakeClock, *fakeStore) {
	t.Helper()
	clk := &fakeClock{}
	store := newFakeStore()
	r := replica.New("test-node", clk, store, testIBLTCells)
	t.Cleanup(r.Close)
	return r, clk, store
}

// ── tests ──────────────────────────────────────────────────────────────────────

func TestPutStoresFieldsAndAdvancesClock(t *testing.T) {
	r, clk, store := newTestReplica(t)
	ctx := context.Background()
	const (
		pid = uint32(1)
		key = "user:1"
		val = `{"name":"Alice","age":30}`
	)
	if err := r.EnsurePartition(ctx, pid); err != nil {
		t.Fatalf("EnsurePartition: %v", err)
	}

	ts, updates, err := r.Put(ctx, pid, key, val)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if ts.PhysicalMs == 0 {
		t.Error("expected non-zero timestamp")
	}
	if len(updates) != 2 {
		t.Fatalf("expected 2 field updates, got %d", len(updates))
	}

	// Clock advanced at least once.
	if clk.Now().PhysicalMs < ts.PhysicalMs {
		t.Error("clock should be >= returned ts")
	}

	// Fields persisted in store.
	m, err := store.GetKey(ctx, pid, key)
	if err != nil {
		t.Fatalf("GetKey: %v", err)
	}
	if len(m.Fields) != 2 {
		t.Errorf("expected 2 fields in store, got %d", len(m.Fields))
	}
}

func TestDeleteProducesTombstones(t *testing.T) {
	r, _, store := newTestReplica(t)
	ctx := context.Background()
	const (
		pid = uint32(2)
		key = "doc:1"
	)
	if err := r.EnsurePartition(ctx, pid); err != nil {
		t.Fatalf("EnsurePartition: %v", err)
	}

	if _, _, err := r.Put(ctx, pid, key, `{"x":1,"y":2}`); err != nil {
		t.Fatalf("Put: %v", err)
	}
	_, updates, err := r.Delete(ctx, pid, key)
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if len(updates) != 2 {
		t.Fatalf("expected 2 tombstone updates, got %d", len(updates))
	}
	for _, u := range updates {
		if !u.Entry.Deleted {
			t.Errorf("field %q: expected Deleted=true", u.Field)
		}
	}

	// Store should reflect tombstones.
	m, err := store.GetKey(ctx, pid, key)
	if err != nil {
		t.Fatalf("GetKey: %v", err)
	}
	for _, e := range m.Fields {
		if !e.Deleted {
			t.Error("expected tombstone in store")
		}
	}
}

func TestDeleteOnMissingKeyIsNoop(t *testing.T) {
	r, _, _ := newTestReplica(t)
	ctx := context.Background()
	if err := r.EnsurePartition(ctx, 3); err != nil {
		t.Fatalf("EnsurePartition: %v", err)
	}
	_, updates, err := r.Delete(ctx, 3, "nonexistent")
	if err != nil {
		t.Fatalf("Delete on missing key: %v", err)
	}
	if len(updates) != 0 {
		t.Errorf("expected 0 updates for missing key, got %d", len(updates))
	}
}

func TestApplyDeltaAcceptsNewerEntry(t *testing.T) {
	r, clk, store := newTestReplica(t)
	ctx := context.Background()
	const (
		pid   = uint32(4)
		key   = "item:1"
		field = "v"
	)
	if err := r.EnsurePartition(ctx, pid); err != nil {
		t.Fatalf("EnsurePartition: %v", err)
	}

	// Seed with an old entry.
	if _, _, err := r.Put(ctx, pid, key, fmt.Sprintf(`{%q:1}`, field)); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Apply a newer remote entry.
	newer := crdt.FieldEntry{
		Value:     json.RawMessage(`99`),
		Timestamp: hlc.Timestamp{PhysicalMs: clk.Now().PhysicalMs + 1000},
		ReplicaID: "remote",
	}
	changed, err := r.ApplyDelta(ctx, pid, key, field, newer)
	if err != nil {
		t.Fatalf("ApplyDelta: %v", err)
	}
	if !changed {
		t.Error("expected changed=true for newer entry")
	}

	// Check store has the new value.
	e, found, err := store.GetField(ctx, pid, key, field)
	if err != nil || !found {
		t.Fatalf("GetField: found=%v err=%v", found, err)
	}
	if string(e.Value) != "99" {
		t.Errorf("got value %s, want 99", e.Value)
	}
}

func TestApplyDeltaRejectsOlderEntry(t *testing.T) {
	r, _, store := newTestReplica(t)
	ctx := context.Background()
	const (
		pid   = uint32(5)
		key   = "item:2"
		field = "v"
	)
	if err := r.EnsurePartition(ctx, pid); err != nil {
		t.Fatalf("EnsurePartition: %v", err)
	}

	if _, _, err := r.Put(ctx, pid, key, fmt.Sprintf(`{%q:10}`, field)); err != nil {
		t.Fatalf("Put: %v", err)
	}

	older := crdt.FieldEntry{
		Value:     json.RawMessage(`1`),
		Timestamp: hlc.Timestamp{PhysicalMs: 0},
		ReplicaID: "remote",
	}
	changed, err := r.ApplyDelta(ctx, pid, key, field, older)
	if err != nil {
		t.Fatalf("ApplyDelta: %v", err)
	}
	if changed {
		t.Error("expected changed=false for older entry")
	}

	// Store value unchanged.
	e, found, _ := store.GetField(ctx, pid, key, field)
	if found && string(e.Value) == "1" {
		t.Error("older entry overwrote newer value in store")
	}
}

func TestPutSerializesWithinPartition(t *testing.T) {
	r, _, _ := newTestReplica(t)
	ctx := context.Background()
	const pid = uint32(6)
	if err := r.EnsurePartition(ctx, pid); err != nil {
		t.Fatalf("EnsurePartition: %v", err)
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var tsList []hlc.Timestamp

	for i := range 10 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("k%d", i)
			ts, _, err := r.Put(ctx, pid, key, `{"v":1}`)
			if err != nil {
				t.Errorf("Put %s: %v", key, err)
				return
			}
			mu.Lock()
			tsList = append(tsList, ts)
			mu.Unlock()
		}(i)
	}
	wg.Wait()

	// All 10 puts should succeed and return unique timestamps.
	seen := make(map[hlc.Timestamp]bool)
	for _, ts := range tsList {
		if seen[ts] {
			t.Errorf("duplicate timestamp %+v", ts)
		}
		seen[ts] = true
	}
}

func TestGetReturnsJSONAfterPut(t *testing.T) {
	r, _, _ := newTestReplica(t)
	ctx := context.Background()
	const (
		pid = uint32(7)
		key = "obj:1"
	)
	if err := r.EnsurePartition(ctx, pid); err != nil {
		t.Fatalf("EnsurePartition: %v", err)
	}

	if _, _, err := r.Put(ctx, pid, key, `{"name":"Bob"}`); err != nil {
		t.Fatalf("Put: %v", err)
	}
	v, found, err := r.Get(ctx, pid, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found {
		t.Fatal("expected found=true")
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal([]byte(v), &obj); err != nil {
		t.Fatalf("Get returned invalid JSON: %v", err)
	}
	if string(obj["name"]) != `"Bob"` {
		t.Errorf("name=%s, want Bob", obj["name"])
	}
}

func TestEnsureAndDropPartition(t *testing.T) {
	r, _, _ := newTestReplica(t)
	ctx := context.Background()

	if err := r.EnsurePartition(ctx, 100); err != nil {
		t.Fatalf("EnsurePartition: %v", err)
	}
	if !r.Owns(100) {
		t.Error("expected Owns(100)=true after EnsurePartition")
	}

	r.DropPartition(100)
	if r.Owns(100) {
		t.Error("expected Owns(100)=false after DropPartition")
	}
}

func TestRecoverHLCFloor(t *testing.T) {
	store := newFakeStore()
	ctx := context.Background()
	if err := store.SaveBatch(ctx, []crdt.FieldUpdate{
		{PartitionID: 1, Key: "k", Field: "f", Entry: crdt.FieldEntry{
			Timestamp: hlc.Timestamp{PhysicalMs: 5000, Logical: 3},
			ReplicaID: "r1",
		}},
		{PartitionID: 2, Key: "k2", Field: "f2", Entry: crdt.FieldEntry{
			Timestamp: hlc.Timestamp{PhysicalMs: 3000, Logical: 0},
			ReplicaID: "r2",
		}},
	}); err != nil {
		t.Fatalf("SaveBatch: %v", err)
	}

	floor, err := replica.RecoverHLCFloor(ctx, store)
	if err != nil {
		t.Fatalf("RecoverHLCFloor: %v", err)
	}
	if floor.PhysicalMs != 5000 || floor.Logical != 3 {
		t.Errorf("got floor %+v, want {5000,3}", floor)
	}
}

func TestRecoverHLCFloorFromCheckpoint(t *testing.T) {
	store := newFakeStore()
	ctx := context.Background()

	// Stale entries below the checkpoint; RecoverHLCFloor must not need to
	// scan them — it should seed from the checkpoint alone.
	if err := store.SaveBatch(ctx, []crdt.FieldUpdate{
		{PartitionID: 1, Key: "k", Field: "f", Entry: crdt.FieldEntry{
			Timestamp: hlc.Timestamp{PhysicalMs: 100, Logical: 0},
			ReplicaID: "r1",
		}},
	}); err != nil {
		t.Fatalf("SaveBatch: %v", err)
	}

	checkpoint := hlc.Timestamp{PhysicalMs: 50000, Logical: 7}
	if err := store.SaveCheckpoint(ctx, checkpoint); err != nil {
		t.Fatalf("SaveCheckpoint: %v", err)
	}

	floor, err := replica.RecoverHLCFloor(ctx, store)
	if err != nil {
		t.Fatalf("RecoverHLCFloor: %v", err)
	}
	wantPhysicalMs := checkpoint.PhysicalMs + uint64(replica.CheckpointMargin.Milliseconds())
	if floor.PhysicalMs != wantPhysicalMs {
		t.Errorf("got floor.PhysicalMs %d, want %d", floor.PhysicalMs, wantPhysicalMs)
	}
}

func TestWithHLCFloor(t *testing.T) {
	clk := &fakeClock{}
	store := newFakeStore()
	floor := hlc.Timestamp{PhysicalMs: 9999}
	r := replica.New("node", clk, store, testIBLTCells, replica.WithHLCFloor(floor))
	defer r.Close()

	// First Send should be above the floor.
	if clk.Now().PhysicalMs < floor.PhysicalMs {
		t.Errorf("clock %d not seeded above floor %d", clk.Now().PhysicalMs, floor.PhysicalMs)
	}
}

// ── partition-lifecycle race: writes vs DropPartition ──────────────────────────

// TestDropPartitionRacesWithWrites exercises the partition lifecycle race
// under the RWMutex model: a writer that successfully resolves *partition
// before DropPartition runs completes its write against the (soon-orphaned)
// partition; a writer whose partitionFor runs after DropPartition sees
// ErrNotOwned. No panic is possible.
func TestDropPartitionRacesWithWrites(t *testing.T) {
	r, _, _ := newTestReplica(t)
	ctx := context.Background()
	const pid = uint32(42)

	if err := r.EnsurePartition(ctx, pid); err != nil {
		t.Fatalf("EnsurePartition: %v", err)
	}

	var wg sync.WaitGroup
	var notOwned, ok atomic.Int64
	for i := range 50 {
		wg.Go(func() {
			key := fmt.Sprintf("racekey-%d", i)
			_, _, err := r.Put(ctx, pid, key, `{"v":1}`)
			switch {
			case err == nil:
				ok.Add(1)
			case errors.Is(err, replica.ErrNotOwned):
				notOwned.Add(1)
			default:
				t.Errorf("unexpected Put error: %v", err)
			}
		})
	}

	r.DropPartition(pid)
	wg.Wait()

	// Every write either succeeded or got ErrNotOwned; no panic, no other error.
	if total := ok.Load() + notOwned.Load(); total != 50 {
		t.Errorf("expected 50 outcomes, got ok=%d notOwned=%d", ok.Load(), notOwned.Load())
	}
}

// TestPutAfterDropReturnsErrNotOwned confirms there is no lazy partition
// re-creation: once Ownership has dropped a partition, the Replica refuses
// writes to it until Ownership ensures it again. This is the production-first
// contract — the routing layer is the source of truth for "is this mine?".
func TestPutAfterDropReturnsErrNotOwned(t *testing.T) {
	r, _, _ := newTestReplica(t)
	ctx := context.Background()
	const pid = uint32(43)

	if err := r.EnsurePartition(ctx, pid); err != nil {
		t.Fatalf("EnsurePartition: %v", err)
	}
	r.DropPartition(pid)

	if _, _, err := r.Put(ctx, pid, "k", `{"v":1}`); !errors.Is(err, replica.ErrNotOwned) {
		t.Errorf("Put after drop: expected ErrNotOwned, got %v", err)
	}
}

// TestReadOnUnownedPartitionReturnsErrNotOwned mirrors the write contract for
// the unified read path.
func TestReadOnUnownedPartitionReturnsErrNotOwned(t *testing.T) {
	r, _, _ := newTestReplica(t)
	ctx := context.Background()

	if _, _, err := r.Get(ctx, 99, "any"); !errors.Is(err, replica.ErrNotOwned) {
		t.Errorf("Get on unowned: expected ErrNotOwned, got %v", err)
	}
	if _, _, err := r.GetField(ctx, 99, "any", "f"); !errors.Is(err, replica.ErrNotOwned) {
		t.Errorf("GetField on unowned: expected ErrNotOwned, got %v", err)
	}
}

// ── ensure concurrent writes don't data-race ───────────────────────────────────

func TestConcurrentWritesDifferentPartitions(t *testing.T) {
	r, _, _ := newTestReplica(t)
	ctx := context.Background()

	for i := range 20 {
		if err := r.EnsurePartition(ctx, uint32(i)); err != nil {
			t.Fatalf("EnsurePartition(%d): %v", i, err)
		}
	}

	var done atomic.Int64
	for i := range 20 {
		go func(i int) {
			_, _, _ = r.Put(ctx, uint32(i), fmt.Sprintf("key%d", i), `{"v":1}`)
			done.Add(1)
		}(i)
	}
	// All goroutines should complete without race detector errors.
	for done.Load() < 20 {
		// yield
	}
}
