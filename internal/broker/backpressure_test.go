package broker

import (
	"sync"
	"testing"
	"time"
)

// fakeConn is a minimal Connection for exercising Topic.publish directly.
type fakeConn struct {
	send       chan ServerMessage
	closeDelay time.Duration
	mu         sync.Mutex
	closed     bool
}

func newFakeConn(cap int) *fakeConn {
	return &fakeConn{send: make(chan ServerMessage, cap)}
}

func (c *fakeConn) SendCh() chan ServerMessage { return c.send }

func (c *fakeConn) Enqueue(msg ServerMessage) bool {
	select {
	case c.send <- msg:
		return true
	default:
		return false
	}
}

func (c *fakeConn) Close() {
	if c.closeDelay > 0 {
		time.Sleep(c.closeDelay)
	}
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
}

func (c *fakeConn) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

func TestPublish_DropPolicy_EvictsOldestKeepsNewestWhenFull(t *testing.T) {
	topic := newTopic("t", 0)
	conn := newFakeConn(1)
	topic.addSubscriber(&Subscriber{clientID: "c1", conn: conn, send: conn.SendCh()})

	topic.publish(Message{ID: "m1"}, PolicyDrop)
	topic.publish(Message{ID: "m2"}, PolicyDrop) // queue full: evict m1, keep m2
	topic.publish(Message{ID: "m3"}, PolicyDrop) // queue full: evict m2, keep m3

	select {
	case got := <-conn.send:
		if got.Message.ID != "m3" {
			t.Fatalf("expected newest message m3 retained, got %s", got.Message.ID)
		}
	default:
		t.Fatal("expected one retained message in queue")
	}

	select {
	case extra := <-conn.send:
		t.Fatalf("expected queue to hold exactly one message, found extra: %v", extra)
	default:
	}
}

func TestPublish_DisconnectPolicy_DoesNotHoldTopicLockDuringClose(t *testing.T) {
	topic := newTopic("t", 0)

	slow := newFakeConn(1)
	slow.closeDelay = 200 * time.Millisecond
	topic.addSubscriber(&Subscriber{clientID: "slow", conn: slow, send: slow.SendCh()})

	// Fill the queue so the next publish trips PolicyDisconnect.
	topic.mu.Lock()
	topic.publish(Message{ID: "m1"}, PolicyDisconnect)
	topic.mu.Unlock()

	done := make(chan struct{})
	go func() {
		topic.mu.Lock()
		toClose := topic.publish(Message{ID: "m2"}, PolicyDisconnect)
		topic.mu.Unlock()
		for _, c := range toClose {
			c.Close()
		}
		close(done)
	}()

	// Let the publish goroutine acquire the lock and enter the slow Close().
	time.Sleep(20 * time.Millisecond)

	start := time.Now()
	topic.removeSubscriber(newFakeConn(1)) // unrelated conn; only needs t.mu
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("removeSubscriber blocked for %v waiting on topic lock; Close() must run after unlock", elapsed)
	}

	<-done
	if !slow.isClosed() {
		t.Fatal("expected slow connection to be closed under disconnect policy")
	}
}
