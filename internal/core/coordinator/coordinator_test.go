package coordinator_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"go.uber.org/goleak"

	"github.com/janthoXO/convergeKV/internal/adapter/badger"
	"github.com/janthoXO/convergeKV/internal/adapter/connpool"
	"github.com/janthoXO/convergeKV/internal/adapter/forward"
	"github.com/janthoXO/convergeKV/internal/core/coordinator"
	"github.com/janthoXO/convergeKV/internal/core/ports"
	"github.com/janthoXO/convergeKV/internal/core/replica"
	"github.com/janthoXO/convergeKV/internal/domain/crdt"
	"github.com/janthoXO/convergeKV/internal/domain/hlc"
	"github.com/janthoXO/convergeKV/internal/domain/keyspace"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

const testNumPartitions = 512

// fakeSink records Enqueue calls without performing any network I/O.
// Satisfies ports.WriteSink for coordinator tests.
type fakeSink struct{ calls atomic.Int64 }

func (s *fakeSink) Enqueue(_ []crdt.FieldUpdate, _ []ports.MemberInfo) { s.calls.Add(1) }

// fakeMemberView returns a single-member view so the local node is always an HRW owner.
type fakeMemberView struct{ member ports.MemberInfo }

func (v *fakeMemberView) Members() []ports.MemberInfo { return []ports.MemberInfo{v.member} }

// fakeOwnerLookup reports the local node as owner of every partition and
// returns the configured member as the sole peer. The coordinator no longer
// recomputes HRW — it asks this lookup, so the test supplies canned answers.
type fakeOwnerLookup struct{ member ports.MemberInfo }

func (l *fakeOwnerLookup) IsOwner(_ uint32) bool             { return true }
func (l *fakeOwnerLookup) Peers(_ uint32) []ports.MemberInfo { return []ports.MemberInfo{l.member} }

// nonOwnerLookup reports the local node as never owning any partition and
// returns no peers, exercising the A3 ErrNoReplicas guard.
type nonOwnerLookup struct{}

func (nonOwnerLookup) IsOwner(_ uint32) bool             { return false }
func (nonOwnerLookup) Peers(_ uint32) []ports.MemberInfo { return nil }

// ── helpers ───────────────────────────────────────────────────────────────────

func tempReplica(t *testing.T, id string) *replica.Replica {
	t.Helper()
	store, err := badger.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open store for %s: %v", id, err)
	}
	t.Cleanup(func() { store.Close() })
	r := replica.New(id, hlc.New(), store, 512)
	t.Cleanup(r.Close)
	return r
}

// ensureKey pre-creates the partition that key will hash to. Mirrors what
// Ownership.Update does in production: the Replica is production-first and
// refuses unowned-partition access, so tests must explicitly ensure.
func ensureKey(t *testing.T, r *replica.Replica, key string) {
	t.Helper()
	pid := keyspace.Of(key, testNumPartitions)
	if err := r.EnsurePartition(t.Context(), pid); err != nil {
		t.Fatalf("EnsurePartition(%d): %v", pid, err)
	}
}

func newCoord(t *testing.T, r *replica.Replica, id string, sink *fakeSink) *coordinator.Coordinator {
	t.Helper()
	pool := connpool.New()
	t.Cleanup(pool.Close)
	fwd := forward.New(pool)
	m := ports.MemberInfo{ReplicaID: id, GRPCAddr: "127.0.0.1:0"}
	view := &fakeMemberView{member: m}
	owners := &fakeOwnerLookup{member: m}
	return coordinator.New(r, view, owners, fwd, sink, testNumPartitions)
}

// fakeForwarder records forwarded calls and returns canned success without
// any network I/O. Used to verify the coordinator forwards rather than
// returning ErrNotOwned to the client.
type fakeForwarder struct {
	puts    atomic.Int64
	gets    atomic.Int64
	deletes atomic.Int64
}

func (f *fakeForwarder) ForwardPut(_ context.Context, _, _, _ string) (hlc.Timestamp, error) {
	f.puts.Add(1)
	return hlc.Timestamp{PhysicalMs: 1}, nil
}

func (f *fakeForwarder) ForwardGet(_ context.Context, _, _ string) (string, bool, error) {
	f.gets.Add(1)
	return `{"v":1}`, true, nil
}

func (f *fakeForwarder) ForwardDelete(_ context.Context, _, _ string) (hlc.Timestamp, error) {
	f.deletes.Add(1)
	return hlc.Timestamp{PhysicalMs: 1}, nil
}

// ── tests ─────────────────────────────────────────────────────────────────────

func TestPutLocalAndReadBack(t *testing.T) {
	r := tempReplica(t, "local")
	sink := &fakeSink{}
	coord := newCoord(t, r, "local", sink)

	key := "user:1"
	pid := keyspace.Of(key, testNumPartitions)
	ensureKey(t, r, key)

	ts, err := coord.Put(t.Context(), key, `{"name":"Alice"}`)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if ts == (hlc.Timestamp{}) {
		t.Error("expected non-zero HLC timestamp")
	}

	v, found, err := r.Get(t.Context(), pid, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found || v == "" {
		t.Errorf("expected key after Put: found=%v v=%q", found, v)
	}
}

func TestDeleteProducesTombstone(t *testing.T) {
	r := tempReplica(t, "local")
	coord := newCoord(t, r, "local", &fakeSink{})

	key := "doc:1"
	pid := keyspace.Of(key, testNumPartitions)
	ensureKey(t, r, key)

	if _, err := coord.Put(t.Context(), key, `{"x":1}`); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := coord.Delete(t.Context(), key); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, found, err := r.Get(t.Context(), pid, key)
	if err != nil {
		t.Fatalf("Get after delete: %v", err)
	}
	if found {
		t.Error("expected key to be absent after Delete")
	}
}

func TestGetMissingKey(t *testing.T) {
	r := tempReplica(t, "local")
	coord := newCoord(t, r, "local", &fakeSink{})
	ensureKey(t, r, "no-such-key")

	_, found, err := coord.Get(t.Context(), "no-such-key")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if found {
		t.Error("expected Found=false for missing key")
	}
}

func TestGetLocalDirectBypass(t *testing.T) {
	r := tempReplica(t, "local")
	coord := newCoord(t, r, "local", &fakeSink{})

	key := "item:42"
	pid := keyspace.Of(key, testNumPartitions)
	ensureKey(t, r, key)

	if _, _, err := r.Put(t.Context(), pid, key, `{"v":42}`); err != nil {
		t.Fatalf("direct Put: %v", err)
	}

	_, found, err := coord.Get(t.Context(), key)
	if err != nil {
		t.Fatalf("coordinator Get: %v", err)
	}
	if !found {
		t.Error("expected Found=true")
	}
}

// TestForwardNoReplicasReturnsErrNoReplicas covers A3: when the local node
// does not own a key's partition and the owner lookup returns no peers (a
// transient gap during membership churn), the coordinator must return
// ErrNoReplicas rather than a zero value with a nil error.
// TestLocalNotOwnedFallsThroughToForward covers the gain-side ownership race:
// Ownership reports the local node as owner (IsOwner == true) but the
// replica has not yet seeded the partition (no EnsurePartition), so the
// local call returns replica.ErrNotOwned. The coordinator must fall through
// to forwarding instead of returning that error to the client.
func TestLocalNotOwnedFallsThroughToForward(t *testing.T) {
	r := tempReplica(t, "local")
	// Deliberately do NOT call ensureKey: the partition is unowned by the
	// replica even though fakeOwnerLookup.IsOwner reports true.

	fwd := &fakeForwarder{}
	m := ports.MemberInfo{ReplicaID: "peer", GRPCAddr: "127.0.0.1:0"}
	view := &fakeMemberView{member: m}
	owners := &fakeOwnerLookup{member: m}
	coord := coordinator.New(r, view, owners, fwd, &fakeSink{}, testNumPartitions)

	if _, err := coord.Put(t.Context(), "k", `{"v":1}`); err != nil {
		t.Errorf("Put: got err=%v, want nil (forwarded)", err)
	}
	if fwd.puts.Load() != 1 {
		t.Errorf("Put: forwarder called %d times, want 1", fwd.puts.Load())
	}

	if _, _, err := coord.Get(t.Context(), "k"); err != nil {
		t.Errorf("Get: got err=%v, want nil (forwarded)", err)
	}
	if fwd.gets.Load() != 1 {
		t.Errorf("Get: forwarder called %d times, want 1", fwd.gets.Load())
	}

	if _, err := coord.Delete(t.Context(), "k"); err != nil {
		t.Errorf("Delete: got err=%v, want nil (forwarded)", err)
	}
	if fwd.deletes.Load() != 1 {
		t.Errorf("Delete: forwarder called %d times, want 1", fwd.deletes.Load())
	}
}

func TestForwardNoReplicasReturnsErrNoReplicas(t *testing.T) {
	r := tempReplica(t, "local")
	pool := connpool.New()
	t.Cleanup(pool.Close)
	fwd := forward.New(pool)
	m := ports.MemberInfo{ReplicaID: "local", GRPCAddr: "127.0.0.1:0"}
	view := &fakeMemberView{member: m}
	coord := coordinator.New(r, view, nonOwnerLookup{}, fwd, &fakeSink{}, testNumPartitions)

	if _, err := coord.Put(t.Context(), "k", `{}`); !errors.Is(err, coordinator.ErrNoReplicas) {
		t.Errorf("Put: got err=%v, want ErrNoReplicas", err)
	}
	if _, _, err := coord.Get(t.Context(), "k"); !errors.Is(err, coordinator.ErrNoReplicas) {
		t.Errorf("Get: got err=%v, want ErrNoReplicas", err)
	}
	if _, err := coord.Delete(t.Context(), "k"); !errors.Is(err, coordinator.ErrNoReplicas) {
		t.Errorf("Delete: got err=%v, want ErrNoReplicas", err)
	}
}
