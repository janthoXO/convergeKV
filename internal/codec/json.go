// Package codec sits at the system boundaries: it splits client JSON into
// opaque top-level field values (the CRDT merge granularity) and renders the
// merged document back to JSON. Values — scalars, arrays, nested objects —
// are kept verbatim and never inspected: nested values are opaque LWW
// wholes; deep merge would later become a recursive register variant, so this
// package must not flatten that possibility away.
package codec

import (
	"encoding/json"
	"errors"

	"github.com/janthoXO/convergeKV/internal/crdt"
)

var ErrNotObject = errors.New("codec: value must be a JSON object")

// SplitFields parses one JSON object into its top-level fields, each value
// kept as raw bytes.
func SplitFields(data []byte) (map[string][]byte, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, ErrNotObject
	}
	if m == nil { // JSON `null`
		return nil, ErrNotObject
	}
	out := make(map[string][]byte, len(m))
	for f, v := range m {
		out[f] = []byte(v)
	}
	return out, nil
}

// RenderDocument renders the document's read view (LWW winner per field,
// values verbatim) as a JSON object with deterministically ordered keys.
// A deleted/unknown document renders as nil.
func RenderDocument(doc *crdt.Document) ([]byte, error) {
	if len(doc.Fields) == 0 {
		return nil, nil
	}
	obj := make(map[string]json.RawMessage, len(doc.Fields))
	for f := range doc.Fields {
		reg, ok := doc.Get(f)
		if !ok {
			continue
		}
		obj[f] = json.RawMessage(reg.Value)
	}
	return json.Marshal(obj) // map keys marshal in sorted order
}
