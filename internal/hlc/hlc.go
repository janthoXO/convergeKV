// Package hlc implements a hybrid logical clock packed into a uint64:
// 48 bits of physical milliseconds followed by a 16-bit logical counter.
// Comparing two timestamps as plain integers therefore orders them
// physical-first, logical-second.
package hlc

import (
	"sync"
	"time"
)

// Timestamp is a packed HLC value: physical ms << 16 | logical.
type Timestamp = uint64

const logicalBits = 16

func Pack(physMs uint64, logical uint16) Timestamp {
	return physMs<<logicalBits | uint64(logical)
}

func PhysMs(t Timestamp) uint64 { return t >> logicalBits }

func Logical(t Timestamp) uint16 { return uint16(t & (1<<logicalBits - 1)) }

// Clock is a thread-safe HLC. The zero value is not usable; construct with
// New (or NewWithClock in tests to inject a wall clock).
type Clock struct {
	mu      sync.Mutex
	last    Timestamp
	wallNow func() time.Time
}

func New() *Clock { return NewWithClock(time.Now) }

func NewWithClock(now func() time.Time) *Clock {
	return &Clock{wallNow: now}
}

// Now advances the clock for a local or send event and returns the new
// timestamp. Strictly monotonic: if the wall clock has not advanced (or went
// backwards) the logical counter increments instead; logical overflow carries
// into the physical part, which keeps ordering correct.
func (c *Clock) Now() Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()
	if pt := c.wallTS(); pt > c.last {
		c.last = pt
	} else {
		c.last++
	}
	return c.last
}

// Update applies the HLC receive rule for a remote timestamp and returns the
// new local timestamp, which is strictly greater than both the previous local
// value and the remote value unless the wall clock is already ahead of both.
func (c *Clock) Update(remote Timestamp) Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()
	pt := c.wallTS()
	m := max(c.last, remote)
	if pt > m {
		c.last = pt
	} else {
		c.last = m + 1
	}
	return c.last
}

// Last returns the current timestamp without advancing the clock
// (for checkpointing).
func (c *Clock) Last() Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.last
}

// SetAtLeast raises the clock to at least t (for restart recovery from a
// checkpoint). It never lowers the clock.
func (c *Clock) SetAtLeast(t Timestamp) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if t > c.last {
		c.last = t
	}
}

func (c *Clock) wallTS() Timestamp {
	return Pack(uint64(c.wallNow().UnixMilli()), 0)
}
