package ports

import "github.com/janthoXO/convergeKV/internal/domain/crdt"

// WriteSink is the port the coordinator uses to broadcast completed writes to
// co-replicas. The concrete implementation is the streampush Fanout. Replaces
// the previous eventbus.Topic[WriteEvent] indirection so write events are
// never silently coalesced/dropped under back-pressure.
type WriteSink interface {
	Enqueue(updates []crdt.FieldUpdate, peers []MemberInfo)
}
