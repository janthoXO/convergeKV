package partitions_test

import (
	"context"
	"slices"
	"sync"
	"testing"

	"go.uber.org/goleak"

	"github.com/janthoXO/convergeKV/internal/cluster/partitions"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// fakeManager records EnsurePartition and DropPartition calls.
type fakeManager struct {
	mu      sync.Mutex
	ensured []uint32
	dropped []uint32
}

func (f *fakeManager) EnsurePartition(_ context.Context, pid uint32) error {
	f.mu.Lock()
	f.ensured = append(f.ensured, pid)
	f.mu.Unlock()
	return nil
}

func (f *fakeManager) DropPartition(pid uint32) {
	f.mu.Lock()
	f.dropped = append(f.dropped, pid)
	f.mu.Unlock()
}

func (f *fakeManager) snapshot() (ensured, dropped []uint32) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]uint32(nil), f.ensured...), append([]uint32(nil), f.dropped...)
}

// fakeTrigger records TriggerPartitions calls.
type fakeTrigger struct {
	mu       sync.Mutex
	triggers [][]uint32
}

func (f *fakeTrigger) TriggerPartitions(pids []uint32) {
	f.mu.Lock()
	f.triggers = append(f.triggers, append([]uint32(nil), pids...))
	f.mu.Unlock()
}

func TestSyncSeedsAllOnFirstCall(t *testing.T) {
	mgr := &fakeManager{}
	ae := &fakeTrigger{}
	l := partitions.New(mgr, ae)

	l.Sync(context.Background(), []uint32{1, 2, 3})

	ensured, dropped := mgr.snapshot()
	slices.Sort(ensured)
	if !slices.Equal(ensured, []uint32{1, 2, 3}) {
		t.Errorf("ensured=%v, want [1 2 3]", ensured)
	}
	if len(dropped) != 0 {
		t.Errorf("dropped=%v, want none", dropped)
	}
	if len(ae.triggers) != 1 {
		t.Fatalf("expected one TriggerPartitions call, got %d", len(ae.triggers))
	}
}

func TestSyncDiffsAgainstPreviousCall(t *testing.T) {
	mgr := &fakeManager{}
	l := partitions.New(mgr, nil)

	l.Sync(context.Background(), []uint32{1, 2, 3})
	mgr.mu.Lock()
	mgr.ensured = mgr.ensured[:0]
	mgr.mu.Unlock()

	// Drop 1, keep 2, add 4.
	l.Sync(context.Background(), []uint32{2, 3, 4})

	ensured, dropped := mgr.snapshot()
	if !slices.Equal(ensured, []uint32{4}) {
		t.Errorf("ensured=%v, want [4]", ensured)
	}
	if !slices.Equal(dropped, []uint32{1}) {
		t.Errorf("dropped=%v, want [1]", dropped)
	}
}

func TestSyncSameSetIsNoOp(t *testing.T) {
	mgr := &fakeManager{}
	l := partitions.New(mgr, nil)

	l.Sync(context.Background(), []uint32{1, 2})
	mgr.mu.Lock()
	mgr.ensured = mgr.ensured[:0]
	mgr.mu.Unlock()

	l.Sync(context.Background(), []uint32{1, 2})

	ensured, dropped := mgr.snapshot()
	if len(ensured) != 0 || len(dropped) != 0 {
		t.Errorf("same-set Sync triggered ensured=%v dropped=%v, want none", ensured, dropped)
	}
}

func TestRunConsumesChannelUntilClosed(t *testing.T) {
	mgr := &fakeManager{}
	l := partitions.New(mgr, nil)

	ch := make(chan []uint32, 2)
	ch <- []uint32{1}
	ch <- []uint32{1, 2}
	close(ch)

	l.Run(context.Background(), ch)

	ensured, _ := mgr.snapshot()
	slices.Sort(ensured)
	if !slices.Equal(ensured, []uint32{1, 2}) {
		t.Errorf("ensured=%v, want [1 2]", ensured)
	}
}
