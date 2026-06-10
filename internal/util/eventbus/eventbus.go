// Package eventbus provides a generic typed publish-subscribe topic.
// Subscriptions are bound to a context; the channel is closed when the context
// is cancelled, eliminating the subscriber goroutine leak that plain
// channel-based subscriptions produce.
package eventbus

import (
	"context"
	"sync"
)

// Topic is a typed broadcast channel. Publish is non-blocking: if a
// subscriber's buffer is full the event is dropped.
// All methods are safe for concurrent use.
type Topic[T any] struct {
	mu   sync.Mutex
	subs []*sub[T]
}

type sub[T any] struct {
	ch     chan T
	ctx    context.Context
	cancel context.CancelFunc

	// closeMu serialises close(ch) in AfterFunc vs ch<-v in Publish.
	closeMu sync.Mutex
	closed  bool
}

// Subscribe returns a receive-only channel that delivers every published event
// while ctx is live. The channel is buffered (cap 1, coalescing).
// When ctx is cancelled the channel is closed and the subscription is removed.
func (t *Topic[T]) Subscribe(ctx context.Context) <-chan T {
	subCtx, cancel := context.WithCancel(ctx)
	s := &sub[T]{
		ch:     make(chan T, 1),
		ctx:    subCtx,
		cancel: cancel,
	}
	t.mu.Lock()
	t.subs = append(t.subs, s)
	t.mu.Unlock()

	context.AfterFunc(subCtx, func() {
		// Remove from the subscription list first.
		t.mu.Lock()
		for i, existing := range t.subs {
			if existing == s {
				t.subs = append(t.subs[:i], t.subs[i+1:]...)
				break
			}
		}
		t.mu.Unlock()

		// Close the channel under s.closeMu so Publish cannot send to a
		// closed channel (send and close both hold s.closeMu).
		s.closeMu.Lock()
		if !s.closed {
			s.closed = true
			close(s.ch)
		}
		s.closeMu.Unlock()
	})

	return s.ch
}

// Publish delivers v to all active subscribers. Slow subscribers are skipped
// (the coalescing buffer ensures they receive the latest value on the next call).
func (t *Topic[T]) Publish(v T) {
	t.mu.Lock()
	subs := make([]*sub[T], len(t.subs))
	copy(subs, t.subs)
	t.mu.Unlock()

	for _, s := range subs {
		if s.ctx.Err() != nil {
			continue
		}
		// Hold s.closeMu while sending so we never send to a closed channel.
		s.closeMu.Lock()
		if !s.closed {
			select {
			case s.ch <- v:
			default: // slow consumer; latest will arrive on next Publish
			}
		}
		s.closeMu.Unlock()
	}
}
