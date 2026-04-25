// Package hlc implements a Hybrid Logical Clock for distributed timestamping.
// It provides total ordering of events across nodes by combining physical
// wall-clock time with a logical counter for tie-breaking.
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
	currentTimestamp Timestamp
}

// New returns an HLC initialised to the current wall time.
func New() *HLC { return &HLC{} }

// Send is called before originating a local event (a write).
// It advances the clock and returns the new timestamp.
func (h *HLC) Send() Timestamp {
	h.mu.Lock()
	defer h.mu.Unlock()
	pt := wallMs()
	if pt > h.currentTimestamp.PhysicalMs {
		h.currentTimestamp = Timestamp{PhysicalMs: pt, Logical: 0}
	} else {
		h.currentTimestamp.Logical++
	}

	return h.currentTimestamp
}

// Receive is called when a message carrying remote timestamp r arrives.
// It advances the local clock to be strictly greater than both the local
// state and the remote timestamp, then returns the new timestamp.
func (h *HLC) Receive(remoteTimestamp Timestamp) Timestamp {
	h.mu.Lock()
	defer h.mu.Unlock()
	pt := wallMs()
	maxPhys := max(pt, max(h.currentTimestamp.PhysicalMs, remoteTimestamp.PhysicalMs))
	switch {
	case maxPhys > h.currentTimestamp.PhysicalMs && maxPhys > remoteTimestamp.PhysicalMs:
		h.currentTimestamp = Timestamp{PhysicalMs: maxPhys, Logical: 0}
	case maxPhys == h.currentTimestamp.PhysicalMs && maxPhys > remoteTimestamp.PhysicalMs:
		h.currentTimestamp.Logical++
	case maxPhys == remoteTimestamp.PhysicalMs && maxPhys > h.currentTimestamp.PhysicalMs:
		h.currentTimestamp = Timestamp{PhysicalMs: maxPhys, Logical: remoteTimestamp.Logical + 1}
	default: // maxPhys == both physical parts
		if h.currentTimestamp.Logical >= remoteTimestamp.Logical {
			h.currentTimestamp.Logical++
		} else {
			h.currentTimestamp = Timestamp{PhysicalMs: maxPhys, Logical: remoteTimestamp.Logical + 1}
		}
	}
	
	return h.currentTimestamp
}

// Now returns the current clock value without advancing it.
func (h *HLC) Now() Timestamp {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.currentTimestamp
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
