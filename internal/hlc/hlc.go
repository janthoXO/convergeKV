// Package hlc implements a Hybrid Logical Clock for distributed timestamping.
// It provides total ordering of events across nodes by combining physical
// wall-clock time with a logical counter for tie-breaking.
package hlc

import (
	"errors"
	"sync"
	"time"
)

// ErrClockDrift is returned when a clock value is more than MAX_CLOCK_DRIFT_MS
// from local wall time: by Send when the HLC is stuck that far in the future,
// or by Receive when a remote timestamp is that far in the future.
var ErrClockDrift = errors.New("hlc: clock value exceeds max drift from wall time")

// Timestamp is a Hybrid Logical Clock value.
// It is totally ordered: compare PhysicalMs first, then Logical.
type Timestamp struct {
	PhysicalMs uint64
	Logical    uint32
}

// HLC is a thread-safe Hybrid Logical Clock.
type HLC struct {
	mu               sync.Mutex
	currentTimestamp Timestamp
}

// New returns an HLC initialised to the current wall time.
func New() *HLC { return &HLC{} }

// Send is called before originating a local event (a write).
// It advances the clock and returns the new timestamp.
// Returns ErrClockDrift if the HLC's physical time is more than
// MAX_CLOCK_DRIFT_MS ahead of local wall time — meaning the clock is stuck in
// the future after a forward wall-clock jump that was later corrected. The
// clock is left unchanged so the caller can retry or surface the error.
func (h *HLC) Send() (Timestamp, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	pt := wallNow()

	if h.currentTimestamp.PhysicalMs > pt+MAX_CLOCK_DRIFT_MS {
		return h.currentTimestamp, ErrClockDrift
	}

	if pt > h.currentTimestamp.PhysicalMs {
		h.currentTimestamp = Timestamp{PhysicalMs: pt, Logical: 0}
	} else {
		h.currentTimestamp.Logical++
	}
	return h.currentTimestamp, nil
}

const MAX_CLOCK_DRIFT_MS = 10 * 60 * 1000 // 10 minutes

// Receive is called when a message carrying remote timestamp r arrives.
// It advances the local clock to be strictly greater than both the local
// state and the remote timestamp, then returns the new timestamp.
// Returns ErrClockDrift if the remote physical time is more than
// MAX_CLOCK_DRIFT_MS ahead of local wall time.
func (h *HLC) Receive(remoteTimestamp Timestamp) (Timestamp, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	pt := wallNow()

	if remoteTimestamp.PhysicalMs > pt+MAX_CLOCK_DRIFT_MS {
		return h.currentTimestamp, ErrClockDrift
	}

	maxPhys := max(pt, h.currentTimestamp.PhysicalMs, remoteTimestamp.PhysicalMs)

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

	return h.currentTimestamp, nil
}

// Seed advances the clock so that future Send calls will never return a
// timestamp below floor. Call once at startup with the highest timestamp
// found in durable storage to restore monotonicity after a crash or a
// backwards NTP correction.
func (h *HLC) Seed(floor Timestamp) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if Less(h.currentTimestamp, floor) {
		h.currentTimestamp = floor
	}
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

// wallNow returns the current wall time in milliseconds.
func wallNow() uint64 { return uint64(time.Now().UnixMilli()) }
