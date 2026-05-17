package node

import (
	"fmt"

	"github.com/janthoXO/convergeKV/internal/crdt"
)

// Get returns the current JSON representation of key.
// Returns ("", false, nil) if the key does not exist or all fields are tombstones.
func (n *Node) Get(key string) (string, bool, error) {
	m, err := n.store.GetKey(key)
	if err != nil {
		return "", false, fmt.Errorf("get: storage error: %w", err)
	}

	b, ok := crdt.ToJSON(m)
	if !ok {
		return "", false, nil
	}

	return string(b), true, nil
}
