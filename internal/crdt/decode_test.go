package crdt

import (
	"bytes"
	"testing"
)

func TestDocumentEncodeDecodeRoundTrip(t *testing.T) {
	src := NewDocument()
	m := &Minter{Actor: actorA}
	src.Put("a", []byte("hello"), m.Next(), 100)
	src.Put("b", nil, m.Next(), 200)
	src.RemoveField("a", m.Next())
	src.Context.Add(dot(actorB, 5)) // cloud entry

	enc := src.Canonical()
	got, err := DecodeDocument(enc)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Canonical(), enc) {
		t.Fatalf("round trip mismatch:\n src: %+v\n got: %+v", src, got)
	}
}

func TestDecodeRejectsTruncation(t *testing.T) {
	src := NewDocument()
	src.Put("field", []byte("value"), dot(actorA, 1), 100)
	enc := src.Canonical()
	for cut := 0; cut < len(enc); cut++ {
		if _, err := DecodeDocument(enc[:cut]); err == nil {
			t.Fatalf("truncation at %d/%d not detected", cut, len(enc))
		}
	}
}

func TestDecodeRejectsTrailingBytes(t *testing.T) {
	src := NewDocument()
	src.Put("f", []byte("v"), dot(actorA, 1), 100)
	if _, err := DecodeDocument(append(src.Canonical(), 0x00)); err == nil {
		t.Fatal("trailing bytes not detected")
	}
}

func FuzzDecodeDocument(f *testing.F) {
	seed := NewDocument()
	seed.Put("f", []byte("v"), dot(actorA, 1), 100)
	f.Add(seed.Canonical())
	f.Fuzz(func(t *testing.T, b []byte) {
		doc, err := DecodeDocument(b)
		if err != nil {
			return
		}
		// Anything that decodes must re-encode decodably (not necessarily
		// byte-identical: map-encoded VV entries and unsorted register sets
		// from adversarial input may re-serialize in canonical order).
		if _, err := DecodeDocument(doc.Canonical()); err != nil {
			t.Fatalf("re-decode failed: %v", err)
		}
	})
}
