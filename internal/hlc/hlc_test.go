package hlc

import (
	"math/rand"
	"testing"
	"time"
)

// fakeWall is a controllable wall clock.
type fakeWall struct{ ms int64 }

func (f *fakeWall) now() time.Time { return time.UnixMilli(f.ms) }

func TestPackUnpack(t *testing.T) {
	ts := Pack(0x123456789ABC, 0xDEF0)
	if PhysMs(ts) != 0x123456789ABC {
		t.Fatalf("PhysMs = %x", PhysMs(ts))
	}
	if Logical(ts) != 0xDEF0 {
		t.Fatalf("Logical = %x", Logical(ts))
	}
}

func TestNowMonotonicWithFrozenWall(t *testing.T) {
	w := &fakeWall{ms: 1000}
	c := NewWithClock(w.now)
	prev := c.Now()
	for i := 0; i < 100; i++ {
		ts := c.Now()
		if ts <= prev {
			t.Fatalf("not strictly monotonic: %d <= %d", ts, prev)
		}
		if PhysMs(ts) != 1000 {
			t.Fatalf("physical advanced unexpectedly: %d", PhysMs(ts))
		}
		prev = ts
	}
	if Logical(prev) != 100 {
		t.Fatalf("logical = %d, want 100", Logical(prev))
	}
}

func TestNowMonotonicWithBackwardsWall(t *testing.T) {
	w := &fakeWall{ms: 5000}
	c := NewWithClock(w.now)
	prev := c.Now()
	w.ms = 0 // wall clock jumps back 5s
	for i := 0; i < 10; i++ {
		ts := c.Now()
		if ts <= prev {
			t.Fatalf("not monotonic under backwards wall: %d <= %d", ts, prev)
		}
		prev = ts
	}
}

func TestNowTracksAdvancingWall(t *testing.T) {
	w := &fakeWall{ms: 1000}
	c := NewWithClock(w.now)
	c.Now()
	w.ms = 2000
	ts := c.Now()
	if PhysMs(ts) != 2000 || Logical(ts) != 0 {
		t.Fatalf("got phys=%d logical=%d, want 2000/0", PhysMs(ts), Logical(ts))
	}
}

func TestUpdateExceedsRemoteAndLocal(t *testing.T) {
	w := &fakeWall{ms: 1000}
	c := NewWithClock(w.now)
	local := c.Now()
	remote := Pack(6000, 7) // remote 5s ahead (allowed skew)
	ts := c.Update(remote)
	if ts <= remote || ts <= local {
		t.Fatalf("Update result %d not greater than remote %d and local %d", ts, remote, local)
	}
	// Next local event must stay above the inherited remote time.
	if next := c.Now(); next <= ts {
		t.Fatalf("Now after Update not monotonic: %d <= %d", next, ts)
	}
}

func TestUpdateWithStaleRemoteUsesWall(t *testing.T) {
	w := &fakeWall{ms: 10_000}
	c := NewWithClock(w.now)
	ts := c.Update(Pack(2000, 9))
	if PhysMs(ts) != 10_000 || Logical(ts) != 0 {
		t.Fatalf("got phys=%d logical=%d, want 10000/0", PhysMs(ts), Logical(ts))
	}
}

func TestSetAtLeast(t *testing.T) {
	w := &fakeWall{ms: 1000}
	c := NewWithClock(w.now)
	c.SetAtLeast(Pack(9000, 3))
	if c.Last() != Pack(9000, 3) {
		t.Fatalf("Last = %d", c.Last())
	}
	c.SetAtLeast(Pack(1, 0)) // must not lower
	if c.Last() != Pack(9000, 3) {
		t.Fatalf("SetAtLeast lowered the clock")
	}
}

// Random interleaving of Now/Update with ±5s wall skew must stay monotonic
// per clock and Update must always dominate the remote timestamp.
func TestRandomSkewMonotonic(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	w := &fakeWall{ms: 1_000_000}
	c := NewWithClock(w.now)
	prev := c.Now()
	for i := 0; i < 10_000; i++ {
		w.ms += rng.Int63n(10_001) - 5000 // ±5s jitter
		var ts Timestamp
		if rng.Intn(2) == 0 {
			ts = c.Now()
		} else {
			remote := Pack(uint64(w.ms+rng.Int63n(10_001)-5000), uint16(rng.Intn(8)))
			ts = c.Update(remote)
			if ts <= remote {
				t.Fatalf("Update %d <= remote %d", ts, remote)
			}
		}
		if ts <= prev {
			t.Fatalf("monotonicity violated at step %d: %d <= %d", i, ts, prev)
		}
		prev = ts
	}
}
