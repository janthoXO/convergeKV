package replication

import (
    "sync"
    "github.com/janthoXO/convergeKV/internal/hlc"
)

// CausalContext tracks the highest HLC timestamp seen FROM each known replica.
// It is used to compute which delta entries a peer still needs.
type CausalContext struct {
    mu   sync.RWMutex
    seen map[string]hlc.Timestamp // replicaID -> max timestamp seen from that replica
}

// NewCausalContext returns an empty CausalContext.
func NewCausalContext() *CausalContext {
    return &CausalContext{seen: make(map[string]hlc.Timestamp)}
}

// Update advances the recorded high-watermark for replicaID if ts is greater.
func (c *CausalContext) Update(replicaID string, ts hlc.Timestamp) {
    c.mu.Lock()
    defer c.mu.Unlock()
    if cur, ok := c.seen[replicaID]; !ok || hlc.Less(cur, ts) {
        c.seen[replicaID] = ts
    }
}

// Snapshot returns a copy of the current seen map.
func (c *CausalContext) Snapshot() map[string]hlc.Timestamp {
    c.mu.RLock()
    defer c.mu.RUnlock()
    out := make(map[string]hlc.Timestamp, len(c.seen))
    for k, v := range c.seen {
        out[k] = v
    }
    return out
}

// NeedsEntry returns true if the given (replicaID, ts) entry is NOT yet
// recorded in this context, meaning the holder of this context hasn't seen it.
func (c *CausalContext) NeedsEntry(replicaID string, ts hlc.Timestamp) bool {
    c.mu.RLock()
    defer c.mu.RUnlock()
    cur, ok := c.seen[replicaID]
    if !ok {
        return true
    }
    return hlc.Less(cur, ts)
}
