package hlc

import (
	"sync"
	"testing"
)

func TestHLCSend(t *testing.T) {
	h := New()
	prev := h.Send()
	for i := 0; i < 100; i++ {
		next := h.Send()
		if !Less(prev, next) {
			t.Errorf("Send did not advance clock: prev=%v, next=%v", prev, next)
		}
		prev = next
	}
}

func TestHLCReceive(t *testing.T) {
	h := New()
	t1 := h.Send()

	// Receive a remote timestamp in the future
	remote := Timestamp{PhysicalMs: t1.PhysicalMs + 1000, Logical: 0}
	t2 := h.Receive(remote)

	if !Less(t1, t2) {
		t.Errorf("Receive did not advance clock over local: local=%v, result=%v", t1, t2)
	}
	if !Less(remote, t2) {
		t.Errorf("Receive did not advance clock over remote: remote=%v, result=%v", remote, t2)
	}
}

func TestHLCLessEqual(t *testing.T) {
	t1 := Timestamp{PhysicalMs: 100, Logical: 1}
	t2 := Timestamp{PhysicalMs: 100, Logical: 2}
	t3 := Timestamp{PhysicalMs: 200, Logical: 0}
	t4 := Timestamp{PhysicalMs: 100, Logical: 1}

	if !Less(t1, t2) {
		t.Errorf("Expected %v < %v", t1, t2)
	}
	if !Less(t2, t3) {
		t.Errorf("Expected %v < %v", t2, t3)
	}
	if Less(t3, t1) {
		t.Errorf("Did not expect %v < %v", t3, t1)
	}
	if !Equal(t1, t4) {
		t.Errorf("Expected %v == %v", t1, t4)
	}
}

func TestHLCConcurrency(t *testing.T) {
	h := New()
	const numGoroutines = 100
	results := make([]Timestamp, numGoroutines)
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			results[idx] = h.Send()
		}(i)
	}
	wg.Wait()

	// Verify all returned timestamps are distinct
	seen := make(map[Timestamp]bool)
	for _, ts := range results {
		if seen[ts] {
			t.Errorf("Duplicate timestamp generated: %v", ts)
		}
		seen[ts] = true
	}
}
