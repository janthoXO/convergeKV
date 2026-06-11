// Package replication implements the applier's asynchronous delta fan-out:
// fire-and-forget through one bounded in-memory queue per peer, with
// exponential backoff and capped attempts/age. Anything dropped here is owned
// by the Merkle anti-entropy backstop — there are no acks and no persistence.
package replication

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// Delta is one replicated unit: a delta document for one key.
type Delta struct {
	Partition uint16
	Key       []byte
	Delta     []byte // canonical encoding of the delta document
	enqueued  time.Time
	attempts  int
}

// SendFunc delivers one delta to a peer address (an ApplyDelta RPC).
type SendFunc func(ctx context.Context, addr string, d Delta) error

type Config struct {
	QueueSize   int           // per-peer queue capacity
	MaxAttempts int           // delivery attempts per delta
	MaxAge      time.Duration // drop deltas older than this
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
	Logger      *slog.Logger
}

func (c *Config) defaults() {
	if c.QueueSize == 0 {
		c.QueueSize = 1024
	}
	if c.MaxAttempts == 0 {
		c.MaxAttempts = 8
	}
	if c.MaxAge == 0 {
		c.MaxAge = 30 * time.Second
	}
	if c.BaseBackoff == 0 {
		c.BaseBackoff = 50 * time.Millisecond
	}
	if c.MaxBackoff == 0 {
		c.MaxBackoff = 5 * time.Second
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

type Fanout struct {
	cfg  Config
	send SendFunc

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu     sync.Mutex
	queues map[string]chan Delta

	dropped atomic.Uint64
}

func NewFanout(cfg Config, send SendFunc) *Fanout {
	cfg.defaults()
	ctx, cancel := context.WithCancel(context.Background())
	return &Fanout{
		cfg:    cfg,
		send:   send,
		ctx:    ctx,
		cancel: cancel,
		queues: make(map[string]chan Delta),
	}
}

// Enqueue queues a delta for one peer. It never blocks: when the peer's
// queue is full the delta is dropped and counted — the Merkle backstop will
// repair it.
func (f *Fanout) Enqueue(addr string, d Delta) {
	d.enqueued = time.Now()
	f.mu.Lock()
	q, ok := f.queues[addr]
	if !ok {
		q = make(chan Delta, f.cfg.QueueSize)
		f.queues[addr] = q
		f.wg.Add(1)
		go f.worker(addr, q)
	}
	f.mu.Unlock()

	select {
	case q <- d:
	default:
		f.dropped.Add(1)
		f.cfg.Logger.Warn("replication queue overflow, delta dropped",
			"peer", addr, "partition", d.Partition)
	}
}

// Dropped returns the number of deltas dropped (overflow, attempts, or age).
func (f *Fanout) Dropped() uint64 { return f.dropped.Load() }

// QueueDepth returns the current depth of one peer's queue.
func (f *Fanout) QueueDepth(addr string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.queues[addr])
}

// Close stops all workers; queued deltas are abandoned (AE owns them).
func (f *Fanout) Close() {
	f.cancel()
	f.wg.Wait()
}

func (f *Fanout) worker(addr string, q chan Delta) {
	defer f.wg.Done()
	for {
		select {
		case <-f.ctx.Done():
			return
		case d := <-q:
			f.deliver(addr, d)
		}
	}
}

// deliver retries one delta with exponential backoff until success, attempt
// cap, age cap, or shutdown.
func (f *Fanout) deliver(addr string, d Delta) {
	backoff := f.cfg.BaseBackoff
	for {
		ctx, cancel := context.WithTimeout(f.ctx, 5*time.Second)
		err := f.send(ctx, addr, d)
		cancel()
		if err == nil {
			return
		}
		d.attempts++
		if d.attempts >= f.cfg.MaxAttempts || time.Since(d.enqueued) > f.cfg.MaxAge {
			f.dropped.Add(1)
			f.cfg.Logger.Warn("delta dropped after retries",
				"peer", addr, "attempts", d.attempts, "err", err)
			return
		}
		select {
		case <-f.ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, f.cfg.MaxBackoff)
	}
}
