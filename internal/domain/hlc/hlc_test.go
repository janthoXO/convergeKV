package hlc

import (
	"sync"
	"testing"
)

// TestSendMonotonicallyIncreasing verifies Send() always returns a strictly
// greater timestamp than the previous Send() result.
func TestSendMonotonicallyIncreasing(t *testing.T) {
	h := New()
	prev, err := h.Send()
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	for i := range 1000 {
		cur, err := h.Send()
		if err != nil {
			t.Fatalf("iteration %d: Send: %v", i, err)
		}
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
	local, err := h.Send()
	if err != nil {
		t.Fatalf("Send: %v", err)
	}

	// Create a remote timestamp well ahead of local but within drift limit.
	remote := Timestamp{PhysicalMs: local.PhysicalMs + 1000, Logical: 5}
	result, err := h.Receive(remote)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

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
	if _, err := h.Send(); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if _, err := h.Send(); err != nil {
		t.Fatalf("Send: %v", err)
	}
	local := h.Now()

	// Remote is far in the past.
	remote := Timestamp{PhysicalMs: 1, Logical: 0}
	result, err := h.Receive(remote)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !Less(local, result) {
		t.Errorf("result %+v should be > local %+v after Receive with old remote", result, local)
	}
}

// TestReceiveFutureDriftRejected verifies that a remote timestamp beyond
// MAX_CLOCK_DRIFT_MS in the future is rejected and the local clock is unchanged.
func TestReceiveFutureDriftRejected(t *testing.T) {
	h := New()
	before, err := h.Send()
	if err != nil {
		t.Fatalf("Send: %v", err)
	}

	// Remote timestamp is way in the future (simulates a broken/malicious peer).
	remote := Timestamp{PhysicalMs: before.PhysicalMs + MAX_CLOCK_DRIFT_MS + 1, Logical: 0}
	result, err := h.Receive(remote)
	if err != ErrClockDrift {
		t.Fatalf("expected ErrClockDrift, got err=%v result=%+v", err, result)
	}
	// Local clock must be unchanged.
	if !Equal(result, before) {
		t.Errorf("clock advanced despite drift rejection: before=%+v after=%+v", before, result)
	}
}

// TestSendHLCStuckInFutureRejected verifies that Send returns ErrClockDrift
// when the HLC's PhysicalMs is more than MAX_CLOCK_DRIFT_MS ahead of the
// current wall time, and that the clock is left unchanged.
// The stuck value is seeded 1 s beyond the drift limit so scheduler jitter
// during the test cannot push wall time past the boundary.
func TestSendHLCStuckInFutureRejected(t *testing.T) {
	h := New()
	stuck := Timestamp{PhysicalMs: wallNow() + MAX_CLOCK_DRIFT_MS + 1_000}
	h.Seed(stuck)
	before := h.Now()

	result, err := h.Send()
	if err != ErrClockDrift {
		t.Fatalf("expected ErrClockDrift, got err=%v result=%+v", err, result)
	}
	if !Equal(result, before) {
		t.Errorf("clock advanced despite drift rejection: before=%+v after=%+v", before, result)
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
	for i := range n {
		go func() {
			defer wg.Done()
			ts, err := h.Send()
			if err != nil {
				t.Errorf("goroutine %d: Send: %v", i, err)
				return
			}
			results[i] = ts
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
