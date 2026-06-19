// Package nodeid defines the node-identity type shared across the membership,
// placement, and request layers: the 16 raw bytes of a node's UUID, used as a
// comparable map key and gossiped/persisted verbatim.
//
// It deliberately has no dependencies. ID is an alias (not a named type), so
// values stay interchangeable with [16]byte and with uuid.UUID — whose
// underlying type is also [16]byte — without explicit conversions at call
// sites. (internal/crdt keeps its own ActorID type instead of importing this:
// that package is stdlib-only, and an actor is its own domain concept that
// merely happens to share the node's bytes.)
package nodeid

// ID is a node's permanent identity: the 16 raw bytes of its UUID.
type ID = [16]byte
