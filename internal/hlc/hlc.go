package hlc

import (
    "sync"
    "time"
)

// Timestamp is a Hybrid Logical Clock value.
// It is totally ordered: compare PhysicalMs first, then Logical.
type Timestamp struct {
    PhysicalMs uint64
    Logical    uint32
}

// HLC is a thread-safe Hybrid Logical Clock.
type HLC struct {
    mu      sync.Mutex
    current Timestamp
}

// New returns an HLC initialised to the current wall time.
func New() *HLC { return &HLC{} }

// Send is called before originating a local event (a write).
// It advances the clock and returns the new timestamp.
func (h *HLC) Send() Timestamp {
    h.mu.Lock()
    defer h.mu.Unlock()
    pt := wallMs()
    if pt > h.current.PhysicalMs {
        h.current = Timestamp{PhysicalMs: pt, Logical: 0}
    } else {
        h.current.Logical++
    }
    return h.current
}

// Receive is called when a message carrying remote timestamp r arrives.
// It advances the local clock to be strictly greater than both the local
// state and the remote timestamp, then returns the new timestamp.
func (h *HLC) Receive(r Timestamp) Timestamp {
    h.mu.Lock()
    defer h.mu.Unlock()
    pt := wallMs()
    maxPhys := max3(pt, h.current.PhysicalMs, r.PhysicalMs)
    switch {
    case maxPhys > h.current.PhysicalMs && maxPhys > r.PhysicalMs:
        h.current = Timestamp{PhysicalMs: maxPhys, Logical: 0}
    case maxPhys == h.current.PhysicalMs && maxPhys > r.PhysicalMs:
        h.current.Logical++
    case maxPhys == r.PhysicalMs && maxPhys > h.current.PhysicalMs:
        h.current = Timestamp{PhysicalMs: maxPhys, Logical: r.Logical + 1}
    default: // maxPhys == both physical parts
        if h.current.Logical >= r.Logical {
            h.current.Logical++
        } else {
            h.current = Timestamp{PhysicalMs: maxPhys, Logical: r.Logical + 1}
        }
    }
    return h.current
}

// Now returns the current clock value without advancing it.
func (h *HLC) Now() Timestamp {
    h.mu.Lock()
    defer h.mu.Unlock()
    return h.current
}

// Less returns true if a is strictly less than b.
func Less(a, b Timestamp) bool {
    if a.PhysicalMs != b.PhysicalMs {
        return a.PhysicalMs < b.PhysicalMs
    }
    return a.Logical < b.Logical
}

// Equal returns true if a and b represent the same instant.
func Equal(a, b Timestamp) bool {
    return a.PhysicalMs == b.PhysicalMs && a.Logical == b.Logical
}

func wallMs() uint64 { return uint64(time.Now().UnixMilli()) }

func max3(a, b, c uint64) uint64 {
    m := a
    if b > m { m = b }
    if c > m { m = c }
    return m
}
