package lifecycle_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/janthoXO/convergeKV/internal/lifecycle"
)

// waitRunning blocks until n services have sent on ch, then sleeps 5 ms to
// give the supervisor goroutine time to process the last ready() signal and
// append the service to its started slice.
func waitRunning(n int, ch <-chan struct{}) {
	for i := 0; i < n; i++ {
		<-ch
	}
	time.Sleep(5 * time.Millisecond)
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// svc builds a Service that calls ready(), then blocks until ctx is cancelled.
func svc(name string) lifecycle.Service {
	return lifecycle.Service{
		Name: name,
		Start: func(ctx context.Context, ready func()) error {
			ready()
			<-ctx.Done()
			return nil
		},
		Close: func() error { return nil },
	}
}

func TestRunCleanShutdown(t *testing.T) {
	sup := lifecycle.New(svc("a"), svc("b"))
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() { done <- sup.Run(ctx) }()

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected nil, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("supervisor did not shut down in time")
	}
}

func TestReverseOrderShutdown(t *testing.T) {
	order := make(chan string, 4)
	allRunning := make(chan struct{}, 3)

	mkSvc := func(name string) lifecycle.Service {
		return lifecycle.Service{
			Name: name,
			Start: func(ctx context.Context, ready func()) error {
				ready()
				allRunning <- struct{}{} // tell the test we're blocking
				<-ctx.Done()
				return nil
			},
			Close: func() error {
				order <- name
				return nil
			},
		}
	}

	sup := lifecycle.New(mkSvc("first"), mkSvc("second"), mkSvc("third"))
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() { done <- sup.Run(ctx) }()

	waitRunning(3, allRunning)
	cancel()
	<-done
	close(order)

	got := make([]string, 0, 3)
	for n := range order {
		got = append(got, n)
	}

	want := []string{"third", "second", "first"}
	if len(got) != len(want) {
		t.Fatalf("expected %d Close calls, got %d: %v", len(want), len(got), got)
	}
	for i, w := range want {
		if got[i] != w {
			t.Errorf("stop order[%d]: got %q, want %q (full: %v)", i, got[i], w, got)
		}
	}
}

func TestStartFailureUnwindsStartedServices(t *testing.T) {
	closed := make(chan string, 4)

	boom := errors.New("boom")

	good := lifecycle.Service{
		Name: "good",
		Start: func(ctx context.Context, ready func()) error {
			ready()
			<-ctx.Done()
			return nil
		},
		Close: func() error { closed <- "good"; return nil },
	}
	bad := lifecycle.Service{
		Name: "bad",
		Start: func(_ context.Context, _ func()) error {
			return boom // never calls ready
		},
		Close: func() error { closed <- "bad"; return nil },
	}
	after := lifecycle.Service{
		Name: "after",
		Start: func(ctx context.Context, ready func()) error {
			t.Error("after.Start should never run")
			ready()
			<-ctx.Done()
			return nil
		},
		Close: func() error { closed <- "after"; return nil },
	}

	sup := lifecycle.New(good, bad, after)
	err := sup.Run(context.Background())
	if !errors.Is(err, boom) {
		t.Fatalf("expected boom error, got %v", err)
	}

	// good should be closed; after should not.
	close(closed)
	got := make(map[string]bool)
	for n := range closed {
		got[n] = true
	}
	if !got["good"] {
		t.Error("expected 'good' to be closed on failure rollback")
	}
	if got["after"] {
		t.Error("expected 'after' NOT to be closed (it never started)")
	}
	if got["bad"] {
		t.Error("expected 'bad' NOT to be closed (its Start returned the error)")
	}
}

func TestCloseErrorsJoined(t *testing.T) {
	e1 := errors.New("e1")
	e2 := errors.New("e2")

	allRunning := make(chan struct{}, 2)

	mkFailing := func(name string, e error) lifecycle.Service {
		return lifecycle.Service{
			Name: name,
			Start: func(ctx context.Context, ready func()) error {
				ready()
				allRunning <- struct{}{}
				<-ctx.Done()
				return nil
			},
			Close: func() error { return e },
		}
	}

	sup := lifecycle.New(mkFailing("a", e1), mkFailing("b", e2))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- sup.Run(ctx) }()

	waitRunning(2, allRunning)
	cancel()

	err := <-done
	if !errors.Is(err, e1) {
		t.Errorf("expected e1 in joined error, got %v", err)
	}
	if !errors.Is(err, e2) {
		t.Errorf("expected e2 in joined error, got %v", err)
	}
}
