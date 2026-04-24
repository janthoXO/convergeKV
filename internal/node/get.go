package node

import "github.com/janthoXO/convergeKV/internal/crdt"

// Get returns the current JSON representation of key.
// Returns ("", false) if the key does not exist or all fields are tombstones.
func (n *Node) Get(key string) (string, bool) {
    m := n.getMap(key)
    b, ok := crdt.ToJSON(m)
    if !ok {
        return "", false
    }
    return string(b), true
}
