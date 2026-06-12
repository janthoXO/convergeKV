package keyspace_test

import (
	"testing"

	"github.com/janthoXO/convergeKV/internal/domain/keyspace"
)

func TestOf_Deterministic(t *testing.T) {
	const P = 512
	key := "some-key"
	got := keyspace.Of(key, P)
	for range 100 {
		if keyspace.Of(key, P) != got {
			t.Fatal("Of returned different values for the same key")
		}
	}
}

func TestOf_InRange(t *testing.T) {
	const P = 512
	keys := []string{"", "a", "hello", "user:1", "order:42", "key\x00with-null"}
	for _, k := range keys {
		pid := keyspace.Of(k, P)
		if pid >= uint32(P) {
			t.Errorf("Of(%q, %d) = %d, out of range [0, %d)", k, P, pid, P)
		}
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	cases := []struct{ pid uint32; key, field string }{
		{0, "hello", "name"},
		{255, "user:1", "age"},
		{512, "some-key", "field-with-\x00-byte"}, // null bytes in field name are allowed
		{0, "", ""},
	}
	for _, tc := range cases {
		b := keyspace.EncodeKey(tc.pid, tc.key, tc.field)
		pid, key, field, err := keyspace.DecodeKey(b)
		if err != nil {
			t.Errorf("DecodeKey(%q/%q): %v", tc.key, tc.field, err)
			continue
		}
		if pid != tc.pid || key != tc.key || field != tc.field {
			t.Errorf("round-trip fail: got (%d,%q,%q) want (%d,%q,%q)", pid, key, field, tc.pid, tc.key, tc.field)
		}
	}
}

func TestRejectNullBytes(t *testing.T) {
	if err := keyspace.RejectNullBytes("valid"); err != nil {
		t.Errorf("expected nil, got %v", err)
	}
	if err := keyspace.RejectNullBytes("key\x00bad"); err == nil {
		t.Error("expected error for null-byte key")
	}
}
