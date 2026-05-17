package gossip

import "encoding/json"

// NodeMeta is the metadata broadcast by each node via memberlist.
// It is encoded as JSON and stored in memberlist.Node.Meta.
type NodeMeta struct {
	ReplicaID string `json:"id"`
	GRPCPort  int    `json:"grpc"`
}

// EncodeMeta serialises m to JSON. Panics if encoding fails (fields are always valid).
func EncodeMeta(m NodeMeta) []byte {
	b, err := json.Marshal(m)
	if err != nil {
		panic("gossip: EncodeMeta: " + err.Error())
	}
	return b
}

// DecodeMeta deserialises a NodeMeta from raw bytes. Returns an error on malformed input.
func DecodeMeta(b []byte) (NodeMeta, error) {
	var m NodeMeta
	return m, json.Unmarshal(b, &m)
}
