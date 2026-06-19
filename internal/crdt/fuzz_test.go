package crdt

import (
	"bytes"
	"testing"
)

// FuzzMerge interprets the fuzz input as an op/delivery script across three
// replicas and asserts merge never panics and the replicas converge.
func FuzzMerge(f *testing.F) {
	f.Add([]byte{0})
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8})
	f.Add([]byte{0xFF, 0x00, 0xAB, 0x10, 0x42, 0x99, 0x7F, 0x80, 0x01, 0x02})

	f.Fuzz(func(t *testing.T, script []byte) {
		h := newHistory(3)
		for i := 0; i+3 < len(script) && i < 64; i += 4 {
			h.step(
				int(script[i])%3,
				int(script[i+1])%3,
				int(script[i+2])%len(fieldPool),
				[]byte{script[i+3]},
				uint64(script[i+3]%3)<<16,
			)
		}
		// Deterministic but input-dependent delivery orders.
		for ri, r := range h.replicas {
			for j := range h.deltas {
				idx := (j*7 + ri*13 + len(script)) % len(h.deltas)
				r.Merge(h.deltas[idx])
			}
			for j := range h.deltas {
				r.Merge(h.deltas[j])
			}
		}
		want := h.replicas[0].Canonical()
		for _, r := range h.replicas[1:] {
			if !bytes.Equal(r.Canonical(), want) {
				t.Fatal("replicas diverged")
			}
		}
	})
}
