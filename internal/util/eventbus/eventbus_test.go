package eventbus_test

import (
	"context"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/janthoXO/convergeKV/internal/util/eventbus"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestPublishWithNoSubscribers(t *testing.T) {
	var topic eventbus.Topic[int]
	topic.Publish(42) // must not panic
}

func TestSubscribeReceivesPublished(t *testing.T) {
	var topic eventbus.Topic[int]
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := topic.Subscribe(ctx)
	topic.Publish(7)

	select {
	case got := <-ch:
		if got != 7 {
			t.Fatalf("got %d, want 7", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for published value")
	}
}

func TestCancelledSubChannelCloses(t *testing.T) {
	var topic eventbus.Topic[int]
	ctx, cancel := context.WithCancel(context.Background())

	ch := topic.Subscribe(ctx)
	cancel()

	select {
	case _, open := <-ch:
		if open {
			t.Fatal("expected channel to be closed after ctx cancel")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for channel to close")
	}
}

func TestPublishAfterCancelDoesNotPanic(t *testing.T) {
	var topic eventbus.Topic[int]
	ctx, cancel := context.WithCancel(context.Background())

	ch := topic.Subscribe(ctx)
	cancel()

	// Wait for the channel to close to ensure AfterFunc has run.
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("channel not closed")
	}

	// Publish after subscriber is gone must not panic (send on closed channel).
	topic.Publish(99)
}

func TestMultipleSubscribersReceive(t *testing.T) {
	var topic eventbus.Topic[string]
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch1 := topic.Subscribe(ctx)
	ch2 := topic.Subscribe(ctx)

	topic.Publish("hello")

	recv := func(ch <-chan string, name string) {
		select {
		case got := <-ch:
			if got != "hello" {
				t.Errorf("%s: got %q, want \"hello\"", name, got)
			}
		case <-time.After(time.Second):
			t.Errorf("%s: timed out", name)
		}
	}
	recv(ch1, "ch1")
	recv(ch2, "ch2")
}

func TestSlowSubscriberCoalesces(t *testing.T) {
	var topic eventbus.Topic[int]
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := topic.Subscribe(ctx)

	// Publish twice rapidly; the cap-1 buffer coalesces: subscriber sees at most 1.
	topic.Publish(1)
	topic.Publish(2)

	// Drain whatever arrived — must be exactly one value.
	got := 0
	select {
	case <-ch:
		got++
	case <-time.After(200 * time.Millisecond):
	}
	select {
	case <-ch:
		got++
	case <-time.After(50 * time.Millisecond):
	}

	if got > 1 {
		t.Errorf("slow subscriber got %d events, expected at most 1 (coalescing)", got)
	}
}
