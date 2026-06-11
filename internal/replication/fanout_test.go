package replication

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDrainsAfterPeerOutage(t *testing.T) {
	var failing atomic.Bool
	failing.Store(true)
	var delivered atomic.Int64

	f := NewFanout(Config{
		BaseBackoff: time.Millisecond,
		MaxBackoff:  5 * time.Millisecond,
		MaxAttempts: 1000,
		MaxAge:      time.Minute,
	}, func(ctx context.Context, addr string, d Delta) error {
		if failing.Load() {
			return errors.New("peer down")
		}
		delivered.Add(1)
		return nil
	})
	defer f.Close()

	for i := 0; i < 50; i++ {
		f.Enqueue("peer1", Delta{Partition: 1, Key: []byte{byte(i)}})
	}
	time.Sleep(20 * time.Millisecond) // outage in progress
	if delivered.Load() != 0 {
		t.Fatal("nothing should be delivered during the outage")
	}
	failing.Store(false)

	deadline := time.Now().Add(5 * time.Second)
	for delivered.Load() != 50 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := delivered.Load(); got != 50 {
		t.Fatalf("delivered %d of 50 after outage ended", got)
	}
	if f.Dropped() != 0 {
		t.Fatalf("dropped %d, want 0", f.Dropped())
	}
}

func TestOverflowDropsAndCounts(t *testing.T) {
	block := make(chan struct{})
	f := NewFanout(Config{QueueSize: 4}, func(ctx context.Context, addr string, d Delta) error {
		<-block
		return nil
	})
	defer f.Close()
	defer close(block)

	for i := 0; i < 20; i++ {
		f.Enqueue("peer1", Delta{})
	}
	// 4 queued, possibly 1 in flight; the rest must be dropped and counted.
	if got := f.Dropped(); got < 15 || got > 16 {
		t.Fatalf("dropped %d, want 15 or 16", got)
	}
}

func TestGivesUpAfterMaxAttempts(t *testing.T) {
	var attempts atomic.Int64
	f := NewFanout(Config{
		MaxAttempts: 3,
		BaseBackoff: time.Millisecond,
	}, func(ctx context.Context, addr string, d Delta) error {
		attempts.Add(1)
		return errors.New("always failing")
	})
	defer f.Close()

	f.Enqueue("peer1", Delta{})
	deadline := time.Now().Add(2 * time.Second)
	for f.Dropped() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if f.Dropped() != 1 {
		t.Fatalf("dropped %d, want 1", f.Dropped())
	}
	if got := attempts.Load(); got != 3 {
		t.Fatalf("attempts %d, want 3", got)
	}
}

func TestPerPeerIsolation(t *testing.T) {
	var mu sync.Mutex
	got := map[string]int{}
	f := NewFanout(Config{}, func(ctx context.Context, addr string, d Delta) error {
		if addr == "down" {
			return errors.New("down")
		}
		mu.Lock()
		got[addr]++
		mu.Unlock()
		return nil
	})
	defer f.Close()

	for i := 0; i < 10; i++ {
		f.Enqueue("down", Delta{})
		f.Enqueue("up", Delta{})
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := got["up"]
		mu.Unlock()
		if n == 10 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("healthy peer starved by the failing one")
}
