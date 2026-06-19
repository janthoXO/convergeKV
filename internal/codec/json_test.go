package codec

import (
	"bytes"
	"testing"

	"github.com/janthoXO/convergeKV/internal/crdt"
)

func TestSplitFieldsKeepsValuesOpaque(t *testing.T) {
	in := []byte(`{"s": "str", "n": 4.25, "arr": [1, {"x": 2}], "obj": {"a": {"b": 1}}, "z": null}`)
	fields, err := SplitFields(in)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"s":   `"str"`,
		"n":   `4.25`,
		"arr": `[1, {"x": 2}]`, // verbatim, inner whitespace preserved
		"obj": `{"a": {"b": 1}}`,
		"z":   `null`,
	}
	if len(fields) != len(want) {
		t.Fatalf("got %d fields, want %d", len(fields), len(want))
	}
	for f, w := range want {
		if string(fields[f]) != w {
			t.Errorf("field %q = %q, want %q", f, fields[f], w)
		}
	}
}

func TestSplitFieldsRejectsNonObjects(t *testing.T) {
	for _, in := range []string{`[1,2]`, `"str"`, `42`, `null`, `not json`, ``} {
		if _, err := SplitFields([]byte(in)); err == nil {
			t.Errorf("input %q: expected error", in)
		}
	}
}

func TestRenderDocumentRoundTrip(t *testing.T) {
	fields, err := SplitFields([]byte(`{"b": {"nested": [1,2]}, "a": "v"}`))
	if err != nil {
		t.Fatal(err)
	}
	doc := crdt.NewDocument()
	m := &crdt.Minter{Actor: crdt.ActorID{1}}
	doc.PutMulti(fields, m.Next, 100)

	out, err := RenderDocument(doc)
	if err != nil {
		t.Fatal(err)
	}
	// Stored value bytes stay verbatim; rendering compacts whitespace.
	want := `{"a":"v","b":{"nested":[1,2]}}`
	if string(out) != want {
		t.Fatalf("got %s, want %s", out, want)
	}
}

func TestRenderDeletedDocumentIsNil(t *testing.T) {
	doc := crdt.NewDocument()
	m := &crdt.Minter{Actor: crdt.ActorID{1}}
	doc.Put("f", []byte(`1`), m.Next(), 100)
	doc.Delete(m.Next())
	out, err := RenderDocument(doc)
	if err != nil || out != nil {
		t.Fatalf("deleted doc must render nil, got %s err %v", out, err)
	}
}

func TestRenderPicksLWWWinnerWholesale(t *testing.T) {
	// Two concurrent writes of nested objects: exactly one side's whole
	// object must render — no partial mixing.
	a := crdt.NewDocument()
	ma := &crdt.Minter{Actor: crdt.ActorID{0xAA}}
	a.Put("cfg", []byte(`{"x":1,"y":1}`), ma.Next(), 200)

	b := crdt.NewDocument()
	mb := &crdt.Minter{Actor: crdt.ActorID{0xBB}}
	b.Put("cfg", []byte(`{"x":2,"z":2}`), mb.Next(), 100)

	a.Merge(b)
	out, err := RenderDocument(a)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out, []byte(`{"cfg":{"x":1,"y":1}}`)) {
		t.Fatalf("got %s", out)
	}
}
