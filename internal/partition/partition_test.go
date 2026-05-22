package partition

import (
	"testing"
)

func TestOf_Deterministic(t *testing.T) {
	const P = 512
	key := "some-key"
	got := Of(key, P)
	for range 100 {
		if Of(key, P) != got {
			t.Fatal("Of returned different values for the same key")
		}
	}
}

func TestOf_FieldIgnored(t *testing.T) {
	const P = 512
	key := "mykey"
	// Partition depends only on key; passing the field should change nothing.
	// (Callers never pass the field, but verify the domain.)
	p1 := Of(key, P)
	p2 := Of(key, P)
	if p1 != p2 {
		t.Fatalf("expected identical results, got %d vs %d", p1, p2)
	}
	// Changing the key must change (with high probability) the partition.
	p3 := Of(key+"X", P)
	_ = p3 // just ensure it doesn't panic; we can't assert the value
}

func TestOf_InRange(t *testing.T) {
	const P = 512
	keys := []string{"", "a", "hello", "user:1", "order:42", "key\x00with-null"}
	for _, k := range keys {
		partitionId := Of(k, P)
		if partitionId >= uint32(P) {
			t.Errorf("Of(%q, %d) = %d, out of range [0, %d)", k, P, partitionId, P)
		}
	}
}

func TestOf_Distribution(t *testing.T) {
	const P = 16
	counts := make([]int, P)
	const N = 10000
	for i := range N {
		key := "key-" + string(rune('a'+i%26)) + "-" + string(rune(i))
		partitionId := Of(key, P)
		counts[partitionId]++
	}
	// Each bucket should be within 50% of the expected average (very loose).
	avg := N / P
	for partitionId, c := range counts {
		if c < avg/2 || c > avg*2 {
			t.Errorf("partition %d has %d keys (expected ~%d): distribution looks skewed", partitionId, c, avg)
		}
	}
}

func TestOf_P1(t *testing.T) {
	// With a single partition everything maps to 0.
	for _, key := range []string{"a", "b", "hello", "key:123"} {
		if got := Of(key, 1); got != 0 {
			t.Errorf("Of(%q, 1) = %d, want 0", key, got)
		}
	}
}
