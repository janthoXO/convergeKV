package iblt_test

import (
	"testing"

	"github.com/janthoXO/convergeKV/internal/domain/crdt"
	"github.com/janthoXO/convergeKV/internal/domain/hlc"
	domiblt "github.com/janthoXO/convergeKV/internal/domain/iblt"
)

const reconcileTestCells = 64

// entryStore is a tiny in-memory map satisfying domiblt.GetField for tests.
type entryStore map[string]crdt.FieldEntry

func (s entryStore) get(key, field string) (crdt.FieldEntry, bool) {
	e, ok := s[key+"/"+field]
	return e, ok
}

func liveEntry(physMs uint64) crdt.FieldEntry {
	return crdt.FieldEntry{Value: []byte(`"v"`), Timestamp: hlc.Timestamp{PhysicalMs: physMs}, ReplicaID: "r"}
}

func tombstone(physMs uint64) crdt.FieldEntry {
	return crdt.FieldEntry{Timestamp: hlc.Timestamp{PhysicalMs: physMs}, ReplicaID: "r", Deleted: true}
}

// TestReconcileBasic verifies that an item only the local side has is queued
// for push, and an item only the remote side has is queued for pull.
func TestReconcileBasic(t *testing.T) {
	local := domiblt.New(reconcileTestCells)
	remote := domiblt.New(reconcileTestCells)

	store := entryStore{
		"k1/f": liveEntry(10),
	}
	local.Insert(domiblt.SerialiseItem("k1", "f", store["k1/f"]))
	remote.Insert(domiblt.SerialiseItem("k2", "f", liveEntry(20)))

	toPush, toPull, fallback, err := domiblt.Reconcile(local, remote, store.get, 0)
	if err != nil || fallback {
		t.Fatalf("Reconcile: err=%v fallback=%v", err, fallback)
	}
	if len(toPush) != 1 || toPush[0].Key != "k1" {
		t.Errorf("toPush = %+v, want [k1/f]", toPush)
	}
	if len(toPull) != 1 || toPull[0].Key != "k2" {
		t.Errorf("toPull = %+v, want [k2/f]", toPull)
	}
}

// TestReconcileFiltersExpiredTombstones verifies that tombstones older than
// cutoffMs are excluded from both toPush and toPull, while fresh tombstones
// and live entries are unaffected.
func TestReconcileFiltersExpiredTombstones(t *testing.T) {
	const cutoff = uint64(1000)

	local := domiblt.New(reconcileTestCells)
	remote := domiblt.New(reconcileTestCells)

	store := entryStore{
		"old-local/f":  tombstone(500),  // expired: < cutoff
		"new-local/f":  tombstone(1500), // fresh: >= cutoff
		"live-local/f": liveEntry(10),   // not a tombstone at all
	}
	local.Insert(domiblt.SerialiseItem("old-local", "f", store["old-local/f"]))
	local.Insert(domiblt.SerialiseItem("new-local", "f", store["new-local/f"]))
	local.Insert(domiblt.SerialiseItem("live-local", "f", store["live-local/f"]))

	remote.Insert(domiblt.SerialiseItem("old-remote", "f", tombstone(500)))
	remote.Insert(domiblt.SerialiseItem("new-remote", "f", tombstone(1500)))
	remote.Insert(domiblt.SerialiseItem("live-remote", "f", liveEntry(20)))

	toPush, toPull, fallback, err := domiblt.Reconcile(local, remote, store.get, cutoff)
	if err != nil || fallback {
		t.Fatalf("Reconcile: err=%v fallback=%v", err, fallback)
	}

	pushKeys := make(map[string]bool)
	for _, id := range toPush {
		pushKeys[id.Key] = true
	}
	if pushKeys["old-local"] {
		t.Error("expired local tombstone should be excluded from toPush")
	}
	if !pushKeys["new-local"] {
		t.Error("fresh local tombstone should be included in toPush")
	}
	if !pushKeys["live-local"] {
		t.Error("live local entry should be included in toPush")
	}

	pullKeys := make(map[string]bool)
	for _, id := range toPull {
		pullKeys[id.Key] = true
	}
	if pullKeys["old-remote"] {
		t.Error("expired remote tombstone should be excluded from toPull")
	}
	if !pullKeys["new-remote"] {
		t.Error("fresh remote tombstone should be included in toPull")
	}
	if !pullKeys["live-remote"] {
		t.Error("live remote entry should be included in toPull")
	}
}

// TestReconcileZeroCutoffDisablesFilter verifies cutoffMs=0 leaves expired
// tombstones in toPush/toPull (back-compat with the pre-GC behaviour).
func TestReconcileZeroCutoffDisablesFilter(t *testing.T) {
	local := domiblt.New(reconcileTestCells)
	remote := domiblt.New(reconcileTestCells)

	store := entryStore{"old-local/f": tombstone(500)}
	local.Insert(domiblt.SerialiseItem("old-local", "f", store["old-local/f"]))
	remote.Insert(domiblt.SerialiseItem("old-remote", "f", tombstone(500)))

	toPush, toPull, fallback, err := domiblt.Reconcile(local, remote, store.get, 0)
	if err != nil || fallback {
		t.Fatalf("Reconcile: err=%v fallback=%v", err, fallback)
	}
	if len(toPush) != 1 || toPush[0].Key != "old-local" {
		t.Errorf("toPush = %+v, want [old-local/f]", toPush)
	}
	if len(toPull) != 1 || toPull[0].Key != "old-remote" {
		t.Errorf("toPull = %+v, want [old-remote/f]", toPull)
	}
}
