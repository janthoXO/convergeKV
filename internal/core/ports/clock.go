package ports

import "github.com/janthoXO/convergeKV/internal/domain/hlc"

// Clock is the hybrid logical clock port used by the replica.
type Clock interface {
	Now() hlc.Timestamp
	Send() (hlc.Timestamp, error)
	Receive(remote hlc.Timestamp) (hlc.Timestamp, error)
	Seed(floor hlc.Timestamp)
}
