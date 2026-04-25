package hlc

import (
	"sync"
	"testing"
)

// TestSendMonotonicallyIncreasing verifies Send() always returns a strictly
// greater timestamp than the previous Send() result.
func TestSendMonotonicallyIncreasing(t *testing.T) {
	h := New()
	prev := h.Send()
	for i := 0; i < 1000; i++ {
		cur := h.Send()
		if !Less(prev, cur) {
			t.Fatalf("iteration %d: expected prev < cur, got prev=%+v cur=%+v", i, prev, cur)
		}
		prev = cur
	}
}

// TestReceiveAdvancesBeyondBoth verifies that Receive with a remote timestamp
// greater than local produces a result greater than both inputs.
func TestReceiveAdvancesBeyondBoth(t *testing.T) {
	h := New()
	local := h.Send()

	// Create a remote timestamp well ahead of local.
	remote := Timestamp{PhysicalMs: local.PhysicalMs + 1000, Logical: 5}
	result := h.Receive(remote)

	if !Less(local, result) {
		t.Errorf("result %+v should be > local %+v", result, local)
	}
	if !Less(remote, result) {
		t.Errorf("result %+v should be > remote %+v", result, remote)
	}
}

// TestReceiveWithOlderRemote verifies that Receive with a remote timestamp
// older than local still advances the local clock.
func TestReceiveWithOlderRemote(t *testing.T) {
	h := New()
	h.Send() // advance past zero
	h.Send()
	local := h.Now()

	// Remote is far in the past.
	remote := Timestamp{PhysicalMs: 1, Logical: 0}
	result := h.Receive(remote)

	if !Less(local, result) {
		t.Errorf("result %+v should be > local %+v after Receive with old remote", result, local)
	}
}

// TestLessAndEqual verifies Less and Equal are consistent.
func TestLessAndEqual(t *testing.T) {
	a := Timestamp{PhysicalMs: 10, Logical: 0}
	b := Timestamp{PhysicalMs: 10, Logical: 1}
	c := Timestamp{PhysicalMs: 11, Logical: 0}
	d := Timestamp{PhysicalMs: 10, Logical: 0}

	cases := []struct {
		name string
		fn   func() bool
		want bool
	}{
		{"a < b (same phys, lower logical)", func() bool { return Less(a, b) }, true},
		{"b < a (same phys, higher logical)", func() bool { return Less(b, a) }, false},
		{"a < c (lower phys)", func() bool { return Less(a, c) }, true},
		{"c < a (higher phys)", func() bool { return Less(c, a) }, false},
		{"a == d", func() bool { return Equal(a, d) }, true},
		{"a == b (should be false)", func() bool { return Equal(a, b) }, false},
		{"!Less(a, a) (irreflexive)", func() bool { return !Less(a, a) }, true},
	}

	for _, tc := range cases {
		if got := tc.fn(); got != tc.want {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestConcurrentSendAllDistinct verifies 100 concurrent goroutines each get
// a unique timestamp (race-free).
func TestConcurrentSendAllDistinct(t *testing.T) {
	h := New()
	const n = 100
	results := make([]Timestamp, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			results[i] = h.Send()
		}()
	}
	wg.Wait()

	// Check all timestamps are distinct.
	seen := make(map[Timestamp]int)
	for i, ts := range results {
		if prev, ok := seen[ts]; ok {
			t.Fatalf("duplicate timestamp %+v at indices %d and %d", ts, prev, i)
		}
		seen[ts] = i
	}
}
