// Package lifecycle provides a Supervisor that starts services in dependency
// order and stops them in reverse, draining cleanly on context cancellation.
package lifecycle

import (
	"context"
	"errors"
	"log/slog"
	"sync"
)

// Service is a named, startable/stoppable component.
// Start must call ready() once the service is fully initialised and then block
// until ctx is cancelled or a fatal error occurs. The supervisor waits for
// ready() before launching the next service, so sequential dependency is
// implicit from registration order.
type Service struct {
	Name  string
	Start func(ctx context.Context, ready func()) error // must call ready() then block
	Close func() error                                  // called after Start returns; must not block
}

// Supervisor starts services in the order they were registered and stops them
// in reverse on shutdown. If a service's Start returns an error before calling
// ready(), all previously started services are stopped in reverse order and the
// error is returned. Runtime errors (after ready()) are logged but do not
// trigger an immediate unwind.
type Supervisor struct {
	services []Service
}

// New constructs a Supervisor with the given services in startup order.
func New(services ...Service) *Supervisor {
	return &Supervisor{services: services}
}

// Run starts all services sequentially (each must call ready() before the next
// is started), blocks until ctx is cancelled or a service fails before ready(),
// then stops all started services in reverse order.
//
// The supervisor wraps ctx with its own cancel so it can signal all running
// Start goroutines to exit when unwinding on failure.
func (s *Supervisor) Run(ctx context.Context) error {
	// Wrap so we can cancel on error-unwind without the caller's ctx.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	started := make([]Service, 0, len(s.services))
	// earlyErr collects failures that occur before a service calls ready().
	// Capacity covers all services so goroutines never block on send.
	earlyErr := make(chan error, len(s.services))

	var wg sync.WaitGroup
	defer wg.Wait() // wait for all Start goroutines before returning

	for _, svc := range s.services {
		readyCh := make(chan struct{})

		wg.Go(func() {
			err := svc.Start(ctx, func() { close(readyCh) })
			if err != nil && ctx.Err() == nil {
				slog.Error("service error", "name", svc.Name, "err", err)
				// Send the error before closing readyCh so the supervisor's
				// select sees the error in earlyErr after unblocking on readyCh.
				select {
				case earlyErr <- err:
				default:
				}
			}
			// Guarantee readyCh is closed so the supervisor loop never hangs.
			select {
			case <-readyCh: // already closed by the ready() call
			default:
				close(readyCh)
			}
		})

		slog.Info("starting service", "name", svc.Name)
		select {
		case <-readyCh:
			// Check for an immediate start error that occurred before ready().
			// earlyErr is sent before readyCh is closed, so it's visible here.
			select {
			case err := <-earlyErr:
				cancel() // signal all running Start goroutines to exit
				_ = s.stopAll(started)
				return err
			default:
				started = append(started, svc)
			}
		case <-ctx.Done():
			return s.stopAll(started)
		}
	}

	slog.Info("all services started")

	// Wait for shutdown signal or a runtime failure from a running service.
	select {
	case err := <-earlyErr:
		cancel() // signal all running Start goroutines to exit
		_ = s.stopAll(started)
		return err
	case <-ctx.Done():
	}

	return s.stopAll(started)
}

func (s *Supervisor) stopAll(started []Service) error {
	var errs []error
	for i := len(started) - 1; i >= 0; i-- {
		svc := started[i]
		if svc.Close == nil {
			continue
		}
		slog.Info("stopping service", "name", svc.Name)
		if err := svc.Close(); err != nil {
			slog.Error("service stop error", "name", svc.Name, "err", err)
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
